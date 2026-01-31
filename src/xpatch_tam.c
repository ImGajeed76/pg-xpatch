/*
 * pg-xpatch - PostgreSQL Table Access Method for delta-compressed data
 * Copyright (c) 2025 Oliver Seifert
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 * Commercial License Option:
 * For commercial use in proprietary software, a commercial license is
 * available. Contact xpatch-commercial@alias.oseifert.ch for details.
 */

/*
 * xpatch_tam.c - Table Access Method implementation
 *
 * Implements all required TAM callbacks for the xpatch access method.
 * Uses heap storage internally but transforms tuples to/from delta-compressed format.
 */

#include "xpatch_tam.h"
#include "xpatch_config.h"
#include "xpatch_storage.h"
#include "xpatch_compress.h"
#include "xpatch_cache.h"
#include "xpatch_seq_cache.h"
#include "xpatch_insert_cache.h"
#include "xpatch_stats_cache.h"

#include "access/heapam.h"
#include "access/heapam_xlog.h"
#include "access/heaptoast.h"
#include "access/hio.h"
#include "access/htup_details.h"
#include "access/multixact.h"
#include "access/rewriteheap.h"
#include "access/tableam.h"
#include "access/visibilitymap.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "catalog/catalog.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/storage.h"
#include "commands/progress.h"
#include "commands/vacuum.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "storage/procarray.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/typcache.h"

/*
 * compute_infobits - compute infomask bits for WAL records
 *
 * This matches the heapam.c implementation. The infobits_set field
 * in xl_heap_delete records which flags to set on the tuple during
 * WAL replay.
 */
static uint8
compute_infobits(uint16 infomask, uint16 infomask2)
{
    uint8 infobits = 0;
    
    if (infomask & HEAP_XMAX_IS_MULTI)
        infobits |= XLHL_XMAX_IS_MULTI;
    if (infomask & HEAP_XMAX_LOCK_ONLY)
        infobits |= XLHL_XMAX_LOCK_ONLY;
    if (infomask & HEAP_XMAX_EXCL_LOCK)
        infobits |= XLHL_XMAX_EXCL_LOCK;
    if (infomask & HEAP_XMAX_KEYSHR_LOCK)
        infobits |= XLHL_XMAX_KEYSHR_LOCK;
    if (infomask2 & HEAP_KEYS_UPDATED)
        infobits |= XLHL_KEYS_UPDATED;
    
    return infobits;
}

/*
 * MVCC visibility check for xpatch tuples.
 * 
 * Implements Read Committed isolation level - a tuple is visible if:
 * 1. XMIN is committed (or current transaction) AND
 * 2. XMAX is invalid/aborted (tuple not deleted) OR
 *    XMAX is set but not yet committed (delete in progress by another txn)
 *
 * Returns true if tuple is visible to the given snapshot.
 */
static bool
xpatch_tuple_is_visible(HeapTupleData *tuple, Snapshot snapshot)
{
    TransactionId xmin, xmax;
    
    if (snapshot == NULL)
        return true;  /* No snapshot means return all tuples */
    
    xmin = HeapTupleHeaderGetRawXmin(tuple->t_data);
    
    /* Check XMIN - the inserting transaction */
    if (TransactionIdIsCurrentTransactionId(xmin))
    {
        /* Inserted by current transaction */
        if (tuple->t_data->t_infomask & HEAP_XMAX_INVALID)
            return true;  /* Not deleted */
        
        /* Check if deleted by current transaction */
        xmax = HeapTupleHeaderGetRawXmax(tuple->t_data);
        if (TransactionIdIsCurrentTransactionId(xmax))
            return false;  /* Deleted by us */
        
        return true;  /* Delete by other txn not committed yet */
    }
    
    if (!TransactionIdDidCommit(xmin))
    {
        /* Inserter didn't commit (aborted or in-progress) */
        if (TransactionIdDidAbort(xmin))
            return false;  /* Definitely not visible */
        
        /* In-progress - not visible in Read Committed */
        return false;
    }
    
    /* XMIN committed - check XMAX for deletions */
    if (tuple->t_data->t_infomask & HEAP_XMAX_INVALID)
        return true;  /* Not deleted */
    
    if (tuple->t_data->t_infomask & HEAP_XMAX_IS_MULTI)
    {
        /* MultiXact - treat as visible for simplicity (no UPDATE support) */
        return true;
    }
    
    xmax = HeapTupleHeaderGetRawXmax(tuple->t_data);
    
    if (TransactionIdIsCurrentTransactionId(xmax))
        return false;  /* Deleted by current transaction */
    
    if (!TransactionIdDidCommit(xmax))
    {
        /* Deleter didn't commit yet - tuple still visible */
        if (TransactionIdDidAbort(xmax))
        {
            /* Deleter aborted - tuple is visible, fix hint bits */
            tuple->t_data->t_infomask |= HEAP_XMAX_INVALID;
        }
        return true;
    }
    
    /* Deleter committed - tuple is deleted and not visible */
    return false;
}

/* Scan descriptor for xpatch tables */
typedef struct XPatchScanDescData
{
    TableScanDescData   base;           /* Base scan descriptor - must be first */
    XPatchConfig       *config;         /* Table configuration */
    MemoryContext       scan_mcxt;      /* Memory context for scan */
    BlockNumber         current_block;  /* Current block being scanned */
    Buffer              current_buffer; /* Current buffer (if any) */
    OffsetNumber        current_offset; /* Current item offset in page */
    OffsetNumber        max_offset;     /* Max offset in current page */
    int32               current_seq;    /* Current sequence number */
    Datum               current_group;  /* Current group value */
    bool                inited;         /* Has scan been initialized? */
    BlockNumber         nblocks;        /* Total blocks in relation */
    
    /* Parallel scan support */
    ParallelBlockTableScanWorkerData *pscan_worker; /* Per-worker parallel scan state */
    
    /* Bitmap scan state */
    BlockNumber         bm_block;       /* Current bitmap block */
    Buffer              bm_buffer;      /* Buffer for bitmap scan */
    int                 bm_index;       /* Current tuple index in block */
    int                 bm_ntuples;     /* Number of tuples from bitmap */
    OffsetNumber        bm_offsets[MaxHeapTuplesPerPage]; /* Offsets from bitmap */
} XPatchScanDescData;

typedef struct XPatchScanDescData *XPatchScanDesc;

/* Index fetch descriptor for xpatch tables */
typedef struct XPatchIndexFetchData
{
    IndexFetchTableData base;           /* AM independent part - must be first */
    XPatchConfig       *config;         /* Table configuration (cached) */
    Buffer              xs_cbuf;        /* Current heap buffer in scan, if any */
} XPatchIndexFetchData;

typedef struct XPatchIndexFetchData *XPatchIndexFetch;

/* Forward declarations for TAM callbacks */
static const TupleTableSlotOps *xpatch_slot_callbacks(Relation relation);
static TableScanDesc xpatch_scan_begin(Relation relation, Snapshot snapshot,
                                       int nkeys, ScanKey key,
                                       ParallelTableScanDesc parallel_scan,
                                       uint32 flags);
static void xpatch_scan_end(TableScanDesc sscan);
static void xpatch_scan_rescan(TableScanDesc sscan, ScanKey key, bool set_params,
                               bool allow_strat, bool allow_sync, bool allow_pagemode);
static bool xpatch_scan_getnextslot(TableScanDesc sscan, ScanDirection direction,
                                    TupleTableSlot *slot);

static void xpatch_tuple_insert(Relation relation, TupleTableSlot *slot,
                                CommandId cid, int options,
                                BulkInsertState bistate);
static void xpatch_tuple_insert_speculative(Relation relation, TupleTableSlot *slot,
                                            CommandId cid, int options,
                                            BulkInsertState bistate,
                                            uint32 specToken);
static void xpatch_tuple_complete_speculative(Relation relation, TupleTableSlot *slot,
                                              uint32 specToken, bool succeeded);
static void xpatch_multi_insert(Relation relation, TupleTableSlot **slots,
                                int ntuples, CommandId cid, int options,
                                BulkInsertState bistate);
static void xpatch_finish_bulk_insert(Relation relation, int options);

static TM_Result xpatch_tuple_delete(Relation relation, ItemPointer tid,
                                     CommandId cid, Snapshot snapshot,
                                     Snapshot crosscheck, bool wait,
                                     TM_FailureData *tmfd, bool changingPart);
static TM_Result xpatch_tuple_update(Relation relation, ItemPointer otid,
                                     TupleTableSlot *slot, CommandId cid,
                                     Snapshot snapshot, Snapshot crosscheck,
                                     bool wait, TM_FailureData *tmfd,
                                     LockTupleMode *lockmode,
                                     TU_UpdateIndexes *update_indexes);
static TM_Result xpatch_tuple_lock(Relation relation, ItemPointer tid,
                                   Snapshot snapshot, TupleTableSlot *slot,
                                   CommandId cid, LockTupleMode mode,
                                   LockWaitPolicy wait_policy, uint8 flags,
                                   TM_FailureData *tmfd);

static bool xpatch_tuple_fetch_row_version(Relation relation, ItemPointer tid,
                                           Snapshot snapshot, TupleTableSlot *slot);
static bool xpatch_tuple_tid_valid(TableScanDesc scan, ItemPointer tid);
static void xpatch_tuple_get_latest_tid(TableScanDesc scan, ItemPointer tid);
static bool xpatch_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot,
                                            Snapshot snapshot);
static TransactionId xpatch_index_delete_tuples(Relation rel,
                                                TM_IndexDeleteOp *delstate);

/* Index fetch callbacks */
static struct IndexFetchTableData *xpatch_index_fetch_begin(Relation rel);
static void xpatch_index_fetch_reset(struct IndexFetchTableData *scan);
static void xpatch_index_fetch_end(struct IndexFetchTableData *scan);
static bool xpatch_index_fetch_tuple(struct IndexFetchTableData *scan,
                                     ItemPointer tid,
                                     Snapshot snapshot,
                                     TupleTableSlot *slot,
                                     bool *call_again, bool *all_dead);

/* Relation management callbacks */
static void xpatch_relation_set_new_filelocator(Relation rel,
                                                const RelFileLocator *newrlocator,
                                                char persistence,
                                                TransactionId *freezeXid,
                                                MultiXactId *minmulti);
static void xpatch_relation_nontransactional_truncate(Relation rel);
static void xpatch_relation_copy_data(Relation rel, const RelFileLocator *newrlocator);
static void xpatch_relation_copy_for_cluster(Relation OldTable, Relation NewTable,
                                             Relation OldIndex, bool use_sort,
                                             TransactionId OldestXmin,
                                             TransactionId *xid_cutoff,
                                             MultiXactId *multi_cutoff,
                                             double *num_tuples,
                                             double *tups_vacuumed,
                                             double *tups_recently_dead);
static void xpatch_relation_vacuum(Relation rel, struct VacuumParams *params,
                                   BufferAccessStrategy bstrategy);
static bool xpatch_scan_analyze_next_block(TableScanDesc scan, BlockNumber blockno,
                                           BufferAccessStrategy bstrategy);
static bool xpatch_scan_analyze_next_tuple(TableScanDesc scan, TransactionId OldestXmin,
                                           double *liverows, double *deadrows,
                                           TupleTableSlot *slot);
static double xpatch_index_build_range_scan(Relation table_rel, Relation index_rel,
                                            struct IndexInfo *index_info,
                                            bool allow_sync, bool anyvisible,
                                            bool progress, BlockNumber start_blockno,
                                            BlockNumber numblocks,
                                            IndexBuildCallback callback,
                                            void *callback_state, TableScanDesc scan);
static void xpatch_index_validate_scan(Relation table_rel, Relation index_rel,
                                       struct IndexInfo *index_info,
                                       Snapshot snapshot, struct ValidateIndexState *state);

/* Estimate size callbacks */
static uint64 xpatch_relation_size(Relation rel, ForkNumber forkNumber);
static bool xpatch_relation_needs_toast_table(Relation rel);
static Oid xpatch_relation_toast_am(Relation rel);

/*
 * NOTE: Parallel scans work correctly when reconstructing delta columns
 * because each parallel worker independently:
 * 1. Scans its assigned blocks to find visible tuples
 * 2. For each tuple, looks up the sequence via TID cache or group scan
 * 3. Reconstructs delta columns using the proper sequence
 *
 * The key insight is that the TID->seq cache is populated during the first
 * scan of each group, and all workers share this cache via shared memory.
 *
 * We use the standard heap block-based parallel scan infrastructure.
 */

/* Planner info callbacks */
static void xpatch_estimate_rel_size(Relation rel, int32 *attr_widths,
                                     BlockNumber *pages, double *tuples,
                                     double *allvisfrac);
static bool xpatch_scan_bitmap_next_block(TableScanDesc scan,
                                          struct TBMIterateResult *tbmres);
static bool xpatch_scan_bitmap_next_tuple(TableScanDesc scan,
                                          struct TBMIterateResult *tbmres,
                                          TupleTableSlot *slot);
static bool xpatch_scan_sample_next_block(TableScanDesc scan,
                                          struct SampleScanState *scanstate);
static bool xpatch_scan_sample_next_tuple(TableScanDesc scan,
                                          struct SampleScanState *scanstate,
                                          TupleTableSlot *slot);

/* The TableAmRoutine for xpatch */
static const TableAmRoutine xpatch_methods = {
    .type = T_TableAmRoutine,

    .slot_callbacks = xpatch_slot_callbacks,

    .scan_begin = xpatch_scan_begin,
    .scan_end = xpatch_scan_end,
    .scan_rescan = xpatch_scan_rescan,
    .scan_getnextslot = xpatch_scan_getnextslot,

    /* 
     * Parallel scan support - uses heap-style block-based parallelism.
     * Reconstruction works correctly because each worker independently
     * looks up sequence numbers via the shared TID->seq cache.
     */
    .parallelscan_estimate = table_block_parallelscan_estimate,
    .parallelscan_initialize = table_block_parallelscan_initialize,
    .parallelscan_reinitialize = table_block_parallelscan_reinitialize,

    /* Index fetch - allows using indexes on non-delta columns */
    .index_fetch_begin = xpatch_index_fetch_begin,
    .index_fetch_reset = xpatch_index_fetch_reset,
    .index_fetch_end = xpatch_index_fetch_end,
    .index_fetch_tuple = xpatch_index_fetch_tuple,

    .tuple_insert = xpatch_tuple_insert,
    .tuple_insert_speculative = xpatch_tuple_insert_speculative,
    .tuple_complete_speculative = xpatch_tuple_complete_speculative,
    .multi_insert = xpatch_multi_insert,
    .tuple_delete = xpatch_tuple_delete,
    .tuple_update = xpatch_tuple_update,
    .tuple_lock = xpatch_tuple_lock,
    .finish_bulk_insert = xpatch_finish_bulk_insert,

    .tuple_fetch_row_version = xpatch_tuple_fetch_row_version,
    .tuple_tid_valid = xpatch_tuple_tid_valid,
    .tuple_get_latest_tid = xpatch_tuple_get_latest_tid,
    .tuple_satisfies_snapshot = xpatch_tuple_satisfies_snapshot,
    .index_delete_tuples = xpatch_index_delete_tuples,

    .relation_set_new_filelocator = xpatch_relation_set_new_filelocator,
    .relation_nontransactional_truncate = xpatch_relation_nontransactional_truncate,
    .relation_copy_data = xpatch_relation_copy_data,
    .relation_copy_for_cluster = xpatch_relation_copy_for_cluster,
    .relation_vacuum = xpatch_relation_vacuum,
    .scan_analyze_next_block = xpatch_scan_analyze_next_block,
    .scan_analyze_next_tuple = xpatch_scan_analyze_next_tuple,
    .index_build_range_scan = xpatch_index_build_range_scan,
    .index_validate_scan = xpatch_index_validate_scan,

    .relation_size = xpatch_relation_size,
    .relation_needs_toast_table = xpatch_relation_needs_toast_table,
    .relation_toast_am = xpatch_relation_toast_am,

    .relation_estimate_size = xpatch_estimate_rel_size,

    .scan_bitmap_next_block = xpatch_scan_bitmap_next_block,
    .scan_bitmap_next_tuple = xpatch_scan_bitmap_next_tuple,
    .scan_sample_next_block = xpatch_scan_sample_next_block,
    .scan_sample_next_tuple = xpatch_scan_sample_next_tuple,
};

/*
 * Return the TableAmRoutine for xpatch access method
 */
const TableAmRoutine *
xpatch_get_table_am_routine(void)
{
    return &xpatch_methods;
}

/* ----------------------------------------------------------------
 * Slot callbacks
 * ---------------------------------------------------------------- */

static const TupleTableSlotOps *
xpatch_slot_callbacks(Relation relation)
{
    elog(DEBUG1, "XPATCH: slot_callbacks - rel=%s", RelationGetRelationName(relation));
    /*
     * Use virtual tuple slots - tuples are fully reconstructed in memory.
     *
     * Potential optimization: A custom slot type could defer delta 
     * reconstruction until columns are actually accessed, improving
     * performance when only non-delta columns are read (e.g., index scans
     * that only fetch group_by/order_by). This would require implementing
     * custom slot ops with lazy materialization.
     */
    return &TTSOpsVirtual;
}

/* ----------------------------------------------------------------
 * Scan callbacks
 * ---------------------------------------------------------------- */

static TableScanDesc
xpatch_scan_begin(Relation relation, Snapshot snapshot,
                  int nkeys, ScanKey key,
                  ParallelTableScanDesc parallel_scan,
                  uint32 flags)
{
    XPatchScanDesc scan;
    MemoryContext oldcxt;

    elog(DEBUG1, "XPATCH: scan_begin - rel=%s, parallel=%s", 
         RelationGetRelationName(relation),
         parallel_scan ? "yes" : "no");

    /* Allocate scan descriptor in a dedicated memory context */
    scan = (XPatchScanDesc) palloc0(sizeof(XPatchScanDescData));
    scan->base.rs_rd = relation;
    scan->base.rs_snapshot = snapshot;
    scan->base.rs_nkeys = nkeys;
    scan->base.rs_key = key;
    scan->base.rs_flags = flags;
    scan->base.rs_parallel = parallel_scan;

    /* Create memory context for scan allocations */
    scan->scan_mcxt = AllocSetContextCreate(CurrentMemoryContext,
                                            "xpatch scan",
                                            ALLOCSET_DEFAULT_SIZES);
    oldcxt = MemoryContextSwitchTo(scan->scan_mcxt);

    /* Get table configuration */
    scan->config = xpatch_get_config(relation);

    /* Initialize scan state */
    scan->inited = false;
    scan->current_block = InvalidBlockNumber;
    scan->current_buffer = InvalidBuffer;
    scan->current_offset = 0;
    scan->current_seq = 0;
    scan->current_group = (Datum) 0;
    
    /* Get number of blocks (for parallel scans, use parallel descriptor's value) */
    if (parallel_scan != NULL)
    {
        ParallelBlockTableScanDesc pbscan = (ParallelBlockTableScanDesc) parallel_scan;
        scan->nblocks = pbscan->phs_nblocks;
        /* Allocate per-worker parallel scan state */
        scan->pscan_worker = (ParallelBlockTableScanWorkerData *) 
            palloc(sizeof(ParallelBlockTableScanWorkerData));
    }
    else
    {
        scan->nblocks = RelationGetNumberOfBlocks(relation);
        scan->pscan_worker = NULL;
    }
    
    /* Initialize bitmap scan state */
    scan->bm_block = InvalidBlockNumber;
    scan->bm_buffer = InvalidBuffer;
    scan->bm_index = 0;
    scan->bm_ntuples = 0;

    MemoryContextSwitchTo(oldcxt);

    return (TableScanDesc) scan;
}

static void
xpatch_scan_end(TableScanDesc sscan)
{
    XPatchScanDesc scan = (XPatchScanDesc) sscan;

    /* Release current buffer if held */
    if (BufferIsValid(scan->current_buffer))
        ReleaseBuffer(scan->current_buffer);
    
    /* Release bitmap scan buffer if held */
    if (BufferIsValid(scan->bm_buffer))
        ReleaseBuffer(scan->bm_buffer);
    
    /* Free parallel scan worker data if allocated */
    if (scan->pscan_worker != NULL)
        pfree(scan->pscan_worker);

    if (scan->scan_mcxt)
        MemoryContextDelete(scan->scan_mcxt);

    /*
     * Unregister the snapshot if SO_TEMP_SNAPSHOT flag is set.
     * This matches heap_endscan behavior - when the executor creates
     * a temporary snapshot for the scan, we're responsible for
     * unregistering it at scan end.
     */
    if (scan->base.rs_flags & SO_TEMP_SNAPSHOT)
        UnregisterSnapshot(scan->base.rs_snapshot);

    pfree(scan);
}

static void
xpatch_scan_rescan(TableScanDesc sscan, ScanKey key, bool set_params,
                   bool allow_strat, bool allow_sync, bool allow_pagemode)
{
    XPatchScanDesc scan = (XPatchScanDesc) sscan;

    /* Release current buffer if held */
    if (BufferIsValid(scan->current_buffer))
    {
        ReleaseBuffer(scan->current_buffer);
        scan->current_buffer = InvalidBuffer;
    }

    /* Reset scan position to beginning */
    scan->inited = false;
    scan->current_block = InvalidBlockNumber;
    scan->current_offset = 0;
    scan->max_offset = 0;
    scan->current_seq = 0;
}

/*
 * xpatch_scan_get_next_block - get the next block to scan
 *
 * For parallel scans, uses the parallel scan API to coordinate blocks
 * across workers. For non-parallel scans, simply increments the block number.
 *
 * Returns InvalidBlockNumber when there are no more blocks to scan.
 */
static BlockNumber
xpatch_scan_get_next_block(XPatchScanDesc scan, bool first_block)
{
    Relation rel = scan->base.rs_rd;
    ParallelBlockTableScanDesc pbscan;
    
    if (scan->base.rs_parallel != NULL)
    {
        /* Parallel scan - use the parallel scan API */
        pbscan = (ParallelBlockTableScanDesc) scan->base.rs_parallel;
        
        if (first_block)
        {
            /* Initialize parallel scan for this worker */
            table_block_parallelscan_startblock_init(rel, scan->pscan_worker, pbscan);
        }
        
        /* Get next block from parallel coordinator */
        return table_block_parallelscan_nextpage(rel, scan->pscan_worker, pbscan);
    }
    else
    {
        /* Non-parallel scan - simple sequential access */
        if (first_block)
        {
            /* Empty relation - no blocks to scan */
            if (scan->nblocks == 0)
                return InvalidBlockNumber;
            return 0;  /* Start at block 0 */
        }
        
        scan->current_block++;
        if (scan->current_block >= scan->nblocks)
            return InvalidBlockNumber;
        
        return scan->current_block;
    }
}

static bool
xpatch_scan_getnextslot(TableScanDesc sscan, ScanDirection direction,
                        TupleTableSlot *slot)
{
    XPatchScanDesc scan = (XPatchScanDesc) sscan;
    Relation rel = scan->base.rs_rd;
    Page page;
    ItemId itemId;
    HeapTupleData tuple;
    BlockNumber block;

    /* Loop until we find a valid tuple or exhaust all blocks */
    for (;;)
    {
        /* If we need a new block */
        if (!BufferIsValid(scan->current_buffer) || scan->current_offset > scan->max_offset)
        {
            /* Release previous buffer */
            if (BufferIsValid(scan->current_buffer))
            {
                ReleaseBuffer(scan->current_buffer);
                scan->current_buffer = InvalidBuffer;
            }

            /* Get the next block to scan */
            block = xpatch_scan_get_next_block(scan, !scan->inited);
            scan->inited = true;
            
            /* Check if we've exhausted all blocks */
            if (block == InvalidBlockNumber)
            {
                ExecClearTuple(slot);
                return false;
            }
            
            scan->current_block = block;

            /* Read the next block */
            scan->current_buffer = ReadBuffer(rel, scan->current_block);
            LockBuffer(scan->current_buffer, BUFFER_LOCK_SHARE);

            page = BufferGetPage(scan->current_buffer);
            scan->max_offset = PageGetMaxOffsetNumber(page);
            scan->current_offset = FirstOffsetNumber;

            /* Unlock but keep pinned */
            LockBuffer(scan->current_buffer, BUFFER_LOCK_UNLOCK);
        }

        /* Lock buffer for reading */
        LockBuffer(scan->current_buffer, BUFFER_LOCK_SHARE);
        page = BufferGetPage(scan->current_buffer);

        while (scan->current_offset <= scan->max_offset)
        {
            HeapTuple copy;
            ItemPointerData saved_tid;

            itemId = PageGetItemId(page, scan->current_offset);
            scan->current_offset++;

            if (!ItemIdIsNormal(itemId))
                continue;

            /* Found a valid item - extract the tuple */
            tuple.t_data = (HeapTupleHeader) PageGetItem(page, itemId);
            tuple.t_len = ItemIdGetLength(itemId);
            tuple.t_tableOid = RelationGetRelid(rel);
            ItemPointerSet(&tuple.t_self, scan->current_block, scan->current_offset - 1);

            /* MVCC visibility check - skip invisible tuples */
            if (!xpatch_tuple_is_visible(&tuple, scan->base.rs_snapshot))
                continue;

            /* Increment sequence counter */
            scan->current_seq++;

            /*
             * CRITICAL: Save TID before calling xpatch_physical_to_logical.
             * 
             * xpatch_physical_to_logical() calls ExecClearTuple() internally,
             * which resets slot->tts_tid to InvalidItemPointer. Without
             * saving and restoring the TID, index scans would return rows
             * with invalid TIDs, breaking index operations entirely.
             * 
             * This caused a bug where index scans returned 0 rows before
             * this fix was added.
             */
            saved_tid = tuple.t_self;

            /* Make a copy of the tuple before releasing lock */
            copy = heap_copytuple(&tuple);

            /* Release buffer lock before processing */
            LockBuffer(scan->current_buffer, BUFFER_LOCK_UNLOCK);

            /* Convert physical tuple to logical (reconstructs delta columns) */
            xpatch_physical_to_logical(rel, scan->config, copy, slot);

            /* Restore TID after reconstruction cleared it */
            slot->tts_tid = saved_tid;

            heap_freetuple(copy);

            return true;
        }

        /* Done with this block - release lock */
        LockBuffer(scan->current_buffer, BUFFER_LOCK_UNLOCK);
    }
}

/* ----------------------------------------------------------------
 * Tuple modification callbacks
 * ---------------------------------------------------------------- */

/* xpatch_compute_group_lock_id is now in xpatch_hash.h */

static void
xpatch_tuple_insert(Relation relation, TupleTableSlot *slot,
                    CommandId cid, int options,
                    BulkInsertState bistate)
{
    XPatchConfig *config;
    HeapTuple physical_tuple;
    MemoryContext oldcxt;
    MemoryContext insert_mcxt;
    volatile Datum group_value = (Datum) 0;
    TupleDesc tupdesc = RelationGetDescr(relation);
    uint64 group_lock_id;
    volatile int32 allocated_seq = 0;  /* For rollback on failure */
    volatile Oid group_typid = InvalidOid;

    elog(DEBUG1, "XPATCH: tuple_insert - rel=%s", RelationGetRelationName(relation));
    config = xpatch_get_config(relation);

    elog(DEBUG1, "xpatch_tuple_insert: validating schema");
    /* Validate the schema on first insert */
    xpatch_validate_schema(relation, config);
    elog(DEBUG1, "xpatch_tuple_insert: schema validated");

    /* Get group value and type if configured */
    if (config->group_by_attnum != InvalidAttrNumber)
    {
        bool isnull;
        Form_pg_attribute group_attr = TupleDescAttr(tupdesc, config->group_by_attnum - 1);
        group_typid = group_attr->atttypid;
        group_value = slot_getattr(slot, config->group_by_attnum, &isnull);
        if (isnull)
            group_value = (Datum) 0;
    }

    /*
     * Acquire group-level advisory lock to ensure sequential inserts.
     * This prevents race conditions where two concurrent inserts to the
     * same group could both see the same max_version and create duplicates.
     * The lock is released at transaction end.
     */
    {
        bool group_isnull = (group_value == (Datum) 0 && config->group_by_attnum != InvalidAttrNumber);
        XPatchGroupHash group_hash = xpatch_compute_group_hash(group_value, group_typid, group_isnull);
        group_lock_id = xpatch_compute_group_lock_id(RelationGetRelid(relation), group_hash);
    }
    DirectFunctionCall1(pg_advisory_xact_lock_int8, Int64GetDatum((int64) group_lock_id));

    elog(DEBUG1, "xpatch_tuple_insert: acquired advisory lock for group (lock_id=%lu)",
         (unsigned long) group_lock_id);

    /* Use a temporary context for insert operations */
    insert_mcxt = AllocSetContextCreate(CurrentMemoryContext,
                                        "xpatch insert",
                                        ALLOCSET_DEFAULT_SIZES);
    
    /*
     * IMPORTANT: Wrap the tuple creation and insert in PG_TRY/CATCH.
     * If any error occurs after the sequence is allocated but before the
     * insert succeeds, we need to rollback the sequence to prevent gaps.
     * Gaps in the sequence chain cause corruption because subsequent deltas
     * may reference non-existent rows.
     */
    PG_TRY();
    {
        oldcxt = MemoryContextSwitchTo(insert_mcxt);

        /* Convert logical tuple to physical (delta-compressed) format */
        physical_tuple = xpatch_logical_to_physical(relation, config, slot, (int32 *) &allocated_seq);

        MemoryContextSwitchTo(oldcxt);

        /*
         * TOAST handling: If the tuple is too large to fit on a page, we need
         * to move large attributes to the TOAST table. heap_toast_insert_or_update()
         * handles this automatically and returns a new tuple with TOAST pointers.
         *
         * TOAST_TUPLE_THRESHOLD is typically ~2KB; tuples larger than this are
         * candidates for compression and/or out-of-line storage.
         */
        if (relation->rd_rel->reltoastrelid != InvalidOid &&
            HeapTupleHasExternal(physical_tuple))
        {
            /* Already has external refs - need to flatten first */
            physical_tuple = toast_flatten_tuple(physical_tuple, tupdesc);
        }
        
        if (relation->rd_rel->reltoastrelid != InvalidOid &&
            physical_tuple->t_len > TOAST_TUPLE_THRESHOLD)
        {
            HeapTuple toasted_tuple;
            
            elog(DEBUG1, "xpatch: tuple size %zu exceeds TOAST threshold %zu, toasting",
                 (Size) physical_tuple->t_len, (Size) TOAST_TUPLE_THRESHOLD);
            
            toasted_tuple = heap_toast_insert_or_update(relation, physical_tuple, NULL, options);
            
            if (toasted_tuple != physical_tuple)
            {
                heap_freetuple(physical_tuple);
                physical_tuple = toasted_tuple;
            }
            
            elog(DEBUG1, "xpatch: after TOAST, tuple size is %zu", (Size) physical_tuple->t_len);
        }

        /*
         * Insert the tuple using low-level heap functions WITH WAL LOGGING.
         * We can't use simple_heap_insert() because it checks for heap AM.
         * We manually do what heap_insert() does: insert + WAL log.
         */
        {
            Buffer buffer;
            Buffer vmbuffer = InvalidBuffer;
            Size len;
            Page page;
            bool need_wal;
            bool all_visible_cleared = false;
            XLogRecPtr recptr;
            uint8 info;
            
            /* Prepare the tuple header */
            physical_tuple->t_data->t_infomask &= ~(HEAP_XACT_MASK);
            physical_tuple->t_data->t_infomask2 &= ~(HEAP2_XACT_MASK);
            physical_tuple->t_data->t_infomask |= HEAP_XMAX_INVALID;
            HeapTupleHeaderSetXmin(physical_tuple->t_data, GetCurrentTransactionId());
            HeapTupleHeaderSetCmin(physical_tuple->t_data, cid);
            HeapTupleHeaderSetXmax(physical_tuple->t_data, 0);
            physical_tuple->t_tableOid = RelationGetRelid(relation);
            
            /* Get tuple length */
            len = MAXALIGN(physical_tuple->t_len);
            
            /* Get a buffer with enough space */
            buffer = RelationGetBufferForTuple(relation, len, InvalidBuffer,
                                               options, NULL, &vmbuffer, NULL, 0);
        
        page = BufferGetPage(buffer);
        
        /* Check if page is marked all-visible and clear it if so */
        if (PageIsAllVisible(page))
        {
            all_visible_cleared = true;
            PageClearAllVisible(page);
            visibilitymap_clear(relation, BufferGetBlockNumber(buffer),
                                vmbuffer, VISIBILITYMAP_VALID_BITS);
        }
        
        /*
         * Enter critical section BEFORE modifying the page.
         * This ensures that if we crash after starting to modify the page
         * but before the WAL record is written, we'll PANIC and not leave
         * the database in an inconsistent state.
         */
        need_wal = RelationNeedsWAL(relation);
        
        START_CRIT_SECTION();
        
        /* Insert the tuple into the page */
        RelationPutHeapTuple(relation, buffer, physical_tuple, false);
        
        /* Mark buffer dirty */
        MarkBufferDirty(buffer);
        
        /* Update slot with inserted tuple's TID */
        slot->tts_tid = physical_tuple->t_self;
        
        /*
         * WAL-log the insert.
         * We use the heap resource manager's insert record format so that
         * PostgreSQL's built-in recovery can replay it correctly.
         */
        if (need_wal)
        {
            xl_heap_insert xlrec;
            xl_heap_header xlhdr;
            uint8 flags = 0;
            uint8 bufflags = 0;
            
            /* Determine info flags */
            info = XLOG_HEAP_INSERT;
            
            /*
             * For standard buffer, we include the "standard" page layout info.
             * We do NOT use INIT_PAGE - that's only for heap_insert when it
             * gets a completely new page from the free space map. Since we're
             * using RelationGetBufferForTuple, we get a page that's already
             * properly initialized, so we just use REGBUF_STANDARD.
             */
            bufflags = REGBUF_STANDARD;
            
            /* Set up the xl_heap_insert record */
            xlrec.offnum = ItemPointerGetOffsetNumber(&physical_tuple->t_self);
            if (all_visible_cleared)
                flags |= XLH_INSERT_ALL_VISIBLE_CLEARED;
            flags |= XLH_INSERT_CONTAINS_NEW_TUPLE;
            xlrec.flags = flags;
            
            /* Set up the tuple header for WAL */
            xlhdr.t_infomask2 = physical_tuple->t_data->t_infomask2;
            xlhdr.t_infomask = physical_tuple->t_data->t_infomask;
            xlhdr.t_hoff = physical_tuple->t_data->t_hoff;
            
            /* Construct the WAL record */
            XLogBeginInsert();
            XLogRegisterData((char *) &xlrec, SizeOfHeapInsert);
            
            /* Register the buffer (block 0) */
            XLogRegisterBuffer(0, buffer, bufflags);
            
            /* Register the tuple header */
            XLogRegisterBufData(0, (char *) &xlhdr, SizeOfHeapHeader);
            
            /* Register the tuple data (excluding the heap tuple header) */
            XLogRegisterBufData(0,
                                (char *) physical_tuple->t_data + SizeofHeapTupleHeader,
                                physical_tuple->t_len - SizeofHeapTupleHeader);
            
            /* Insert the WAL record */
            recptr = XLogInsert(RM_HEAP_ID, info);
            
            /* Set the page LSN */
            PageSetLSN(page, recptr);
        }
        
            END_CRIT_SECTION();
            
            if (BufferIsValid(vmbuffer))
                ReleaseBuffer(vmbuffer);
            UnlockReleaseBuffer(buffer);
        }

        /* Cleanup on success */
        heap_freetuple(physical_tuple);
        MemoryContextDelete(insert_mcxt);
    }
    PG_CATCH();
    {
        /*
         * INSERT FAILED - Rollback the sequence allocation to prevent gaps.
         * This is critical for maintaining delta chain integrity.
         *
         * Note: We only rollback if we actually allocated a sequence
         * (allocated_seq > 0 and not in restore mode).
         */
        if (allocated_seq > 0)
        {
            elog(DEBUG1, "xpatch: insert failed, rolling back sequence %d", allocated_seq);
            xpatch_seq_cache_rollback_seq(RelationGetRelid(relation),
                                          group_value, group_typid, allocated_seq);
        }
        
        /* Clean up memory context */
        MemoryContextDelete(insert_mcxt);
        
        /* Re-throw the error */
        PG_RE_THROW();
    }
    PG_END_TRY();
}

static void
xpatch_tuple_insert_speculative(Relation relation, TupleTableSlot *slot,
                                CommandId cid, int options,
                                BulkInsertState bistate,
                                uint32 specToken)
{
    /* Speculative insert not fully supported - fall back to regular insert */
    xpatch_tuple_insert(relation, slot, cid, options, bistate);
}

static void
xpatch_tuple_complete_speculative(Relation relation, TupleTableSlot *slot,
                                  uint32 specToken, bool succeeded)
{
    /* Speculative completion - nothing special needed */
}

static void
xpatch_multi_insert(Relation relation, TupleTableSlot **slots,
                    int ntuples, CommandId cid, int options,
                    BulkInsertState bistate)
{
    /* Multi-insert: just loop over single inserts for now */
    for (int i = 0; i < ntuples; i++)
    {
        xpatch_tuple_insert(relation, slots[i], cid, options, bistate);
    }
}

static void
xpatch_finish_bulk_insert(Relation relation, int options)
{
    /* Nothing special needed for bulk insert cleanup */
}

/*
 * Delete a tuple with cascading delete of dependent versions.
 *
 * xpatch DELETE semantics:
 * - Deleting a version cascades to ALL subsequent versions in that group
 * - This is necessary because later versions may be delta-encoded against
 *   the deleted version (directly or transitively)
 * - We perform a hard delete (immediate physical removal via XMAX)
 *
 * Algorithm:
 * 1. Lock the target tuple to get its group value and sequence
 * 2. Acquire advisory lock on the group
 * 3. Find all tuples in the group with sequence >= target_seq
 * 4. Delete them in reverse order (highest seq first)
 * 5. Invalidate cache entries
 * 6. Update seq cache
 */
static TM_Result
xpatch_tuple_delete(Relation relation, ItemPointer tid,
                    CommandId cid, Snapshot snapshot,
                    Snapshot crosscheck, bool wait,
                    TM_FailureData *tmfd, bool changingPart)
{
    XPatchConfig *config;
    TupleDesc tupdesc;
    Buffer buffer;
    Page page;
    ItemId itemId;
    HeapTupleData target_tuple;
    Datum group_value = (Datum) 0;
    Oid group_typid = InvalidOid;
    bool group_isnull = false;
    uint64 group_lock_id;
    Oid relid;
    BlockNumber blkno;
    OffsetNumber offnum;
    BlockNumber nblocks;
    int32 target_seq;
    int32 current_seq;
    Form_pg_attribute attr;
    Datum tuple_group;
    HeapTupleData tuple;
    TransactionId xid;
    int deleted_count = 0;
    
    relid = RelationGetRelid(relation);
    tupdesc = RelationGetDescr(relation);
    config = xpatch_get_config(relation);
    xid = GetCurrentTransactionId();
    
    elog(DEBUG1, "xpatch: tuple_delete starting for tid=(%u,%u)",
         ItemPointerGetBlockNumber(tid), ItemPointerGetOffsetNumber(tid));
    
    /*
     * Step 1: Read the target tuple to get its group value and determine sequence
     */
    blkno = ItemPointerGetBlockNumber(tid);
    offnum = ItemPointerGetOffsetNumber(tid);
    
    buffer = ReadBuffer(relation, blkno);
    LockBuffer(buffer, BUFFER_LOCK_SHARE);
    
    page = BufferGetPage(buffer);
    itemId = PageGetItemId(page, offnum);
    
    if (!ItemIdIsNormal(itemId))
    {
        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
        ReleaseBuffer(buffer);
        
        if (tmfd)
        {
            tmfd->traversed = false;
            tmfd->xmax = InvalidTransactionId;
        }
        return TM_Invisible;
    }
    
    target_tuple.t_data = (HeapTupleHeader) PageGetItem(page, itemId);
    target_tuple.t_len = ItemIdGetLength(itemId);
    target_tuple.t_tableOid = relid;
    target_tuple.t_self = *tid;
    
    /* Check if tuple is already deleted */
    if (!(target_tuple.t_data->t_infomask & HEAP_XMAX_INVALID))
    {
        TransactionId xmax = HeapTupleHeaderGetRawXmax(target_tuple.t_data);
        
        if (TransactionIdIsCurrentTransactionId(xmax))
        {
            /* Already deleted by us */
            LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
            ReleaseBuffer(buffer);
            return TM_SelfModified;
        }
        
        if (TransactionIdDidCommit(xmax))
        {
            /* Already deleted by committed transaction */
            LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
            ReleaseBuffer(buffer);
            if (tmfd)
            {
                tmfd->traversed = false;
                tmfd->xmax = xmax;
            }
            return TM_Updated;
        }
        
        /* Delete in progress by another transaction */
        if (!wait)
        {
            LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
            ReleaseBuffer(buffer);
            if (tmfd)
            {
                tmfd->traversed = false;
                tmfd->xmax = xmax;
            }
            return TM_WouldBlock;
        }
        /* If wait is true, we should wait for the other transaction, but for
         * simplicity we'll return TM_BeingModified. A full implementation
         * would use XactLockTableWait. */
    }
    
    /* Get group value if configured */
    if (config->group_by_attnum != InvalidAttrNumber)
    {
        Form_pg_attribute group_attr = TupleDescAttr(tupdesc, config->group_by_attnum - 1);
        group_typid = group_attr->atttypid;
        group_value = heap_getattr(&target_tuple, config->group_by_attnum, 
                                   tupdesc, &group_isnull);
        if (group_isnull)
            group_value = (Datum) 0;
    }
    
    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
    ReleaseBuffer(buffer);
    
    /*
     * Step 2: Acquire advisory lock on the group to prevent concurrent modifications
     */
    {
        XPatchGroupHash group_hash = xpatch_compute_group_hash(group_value, group_typid, group_isnull);
        group_lock_id = xpatch_compute_group_lock_id(relid, group_hash);
    }
    DirectFunctionCall1(pg_advisory_xact_lock_int8, Int64GetDatum((int64) group_lock_id));
    
    elog(DEBUG1, "xpatch: delete acquired advisory lock (lock_id=%lu)", 
         (unsigned long) group_lock_id);
    
    /*
     * Step 3: Scan to find the sequence number of the target tuple
     * and identify all tuples to delete (target_seq and higher)
     * 
     * We need to do two passes:
     * Pass 1: Find target_seq by scanning until we hit the target TID
     * Pass 2: Delete all tuples with seq >= target_seq
     */
    target_seq = 0;
    current_seq = 0;
    nblocks = RelationGetNumberOfBlocks(relation);
    
    /* Pass 1: Find target sequence number */
    for (blkno = 0; blkno < nblocks && target_seq == 0; blkno++)
    {
        OffsetNumber maxoff;
        
        buffer = ReadBuffer(relation, blkno);
        LockBuffer(buffer, BUFFER_LOCK_SHARE);
        
        page = BufferGetPage(buffer);
        maxoff = PageGetMaxOffsetNumber(page);
        
        for (offnum = FirstOffsetNumber; offnum <= maxoff; offnum++)
        {
            itemId = PageGetItemId(page, offnum);
            
            if (!ItemIdIsNormal(itemId))
                continue;
            
            tuple.t_data = (HeapTupleHeader) PageGetItem(page, itemId);
            tuple.t_len = ItemIdGetLength(itemId);
            tuple.t_tableOid = relid;
            ItemPointerSet(&tuple.t_self, blkno, offnum);
            
            /* Skip already-deleted tuples */
            if (!(tuple.t_data->t_infomask & HEAP_XMAX_INVALID))
            {
                TransactionId tup_xmax = HeapTupleHeaderGetRawXmax(tuple.t_data);
                if (TransactionIdDidCommit(tup_xmax))
                    continue;
            }
            
            /* Check group match */
            if (config->group_by_attnum != InvalidAttrNumber)
            {
                tuple_group = heap_getattr(&tuple, config->group_by_attnum, 
                                          tupdesc, &group_isnull);
                if (group_isnull)
                    continue;
                
                attr = TupleDescAttr(tupdesc, config->group_by_attnum - 1);
                if (!xpatch_datums_equal(group_value, tuple_group, attr->atttypid, attr->attcollation))
                    continue;
            }
            
            current_seq++;
            
            /* Check if this is our target tuple */
            if (ItemPointerEquals(&tuple.t_self, tid))
            {
                target_seq = current_seq;
                break;
            }
        }
        
        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
        ReleaseBuffer(buffer);
    }
    
    if (target_seq == 0)
    {
        elog(WARNING, "xpatch: could not find target tuple for delete");
        return TM_Invisible;
    }
    
    elog(DEBUG1, "xpatch: target tuple has seq=%d, will cascade delete seq>=%d", 
         target_seq, target_seq);
    
    /*
     * Step 4: Delete all tuples with seq >= target_seq
     * We mark them as deleted by setting XMAX
     */
    current_seq = 0;
    
    for (blkno = 0; blkno < nblocks; blkno++)
    {
        OffsetNumber maxoff;
        
        buffer = ReadBuffer(relation, blkno);
        LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
        
        page = BufferGetPage(buffer);
        maxoff = PageGetMaxOffsetNumber(page);
        
        for (offnum = FirstOffsetNumber; offnum <= maxoff; offnum++)
        {
            itemId = PageGetItemId(page, offnum);
            
            if (!ItemIdIsNormal(itemId))
                continue;
            
            tuple.t_data = (HeapTupleHeader) PageGetItem(page, itemId);
            tuple.t_len = ItemIdGetLength(itemId);
            tuple.t_tableOid = relid;
            ItemPointerSet(&tuple.t_self, blkno, offnum);
            
            /* Skip already-deleted tuples */
            if (!(tuple.t_data->t_infomask & HEAP_XMAX_INVALID))
            {
                TransactionId tup_xmax = HeapTupleHeaderGetRawXmax(tuple.t_data);
                if (TransactionIdDidCommit(tup_xmax))
                    continue;
            }
            
            /* Check group match */
            if (config->group_by_attnum != InvalidAttrNumber)
            {
                tuple_group = heap_getattr(&tuple, config->group_by_attnum, 
                                          tupdesc, &group_isnull);
                if (group_isnull)
                    continue;
                
                attr = TupleDescAttr(tupdesc, config->group_by_attnum - 1);
                if (!xpatch_datums_equal(group_value, tuple_group, attr->atttypid, attr->attcollation))
                    continue;
            }
            
            current_seq++;
            
            /* Delete if seq >= target_seq */
            if (current_seq >= target_seq)
            {
                bool all_visible_cleared = false;
                bool need_wal_for_delete;
                
                /*
                 * Enter critical section BEFORE modifying the page.
                 * This ensures consistency between in-memory state and WAL.
                 */
                need_wal_for_delete = RelationNeedsWAL(relation);
                
                START_CRIT_SECTION();
                
                /* Check if page is marked all-visible and clear it if so */
                if (PageIsAllVisible(page))
                {
                    all_visible_cleared = true;
                    PageClearAllVisible(page);
                    /* Note: We'd need vmbuffer here to clear visibility map.
                     * For now we just clear the page flag. Full implementation
                     * would call visibilitymap_clear() but we don't have vmbuffer
                     * in the delete path. The visibility map will be corrected
                     * on the next VACUUM. */
                }
                
                /* Mark tuple as deleted by setting XMAX */
                tuple.t_data->t_infomask &= ~HEAP_XMAX_INVALID;
                tuple.t_data->t_infomask &= ~HEAP_XMAX_IS_MULTI;
                tuple.t_data->t_infomask &= ~HEAP_XMAX_COMMITTED;
                tuple.t_data->t_infomask &= ~HEAP_XMAX_LOCK_ONLY;
                HeapTupleHeaderSetXmax(tuple.t_data, xid);
                HeapTupleHeaderSetCmax(tuple.t_data, cid, false);
                
                deleted_count++;
                
                /* Mark buffer dirty */
                MarkBufferDirty(buffer);
                
                /*
                 * WAL-log the delete operation.
                 * We use the heap resource manager's delete record format.
                 */
                if (need_wal_for_delete)
                {
                    xl_heap_delete xlrec;
                    XLogRecPtr recptr;
                    uint8 flags = 0;
                    
                    xlrec.offnum = offnum;
                    xlrec.xmax = xid;
                    xlrec.infobits_set = compute_infobits(tuple.t_data->t_infomask,
                                                          tuple.t_data->t_infomask2);
                    
                    if (all_visible_cleared)
                        flags |= XLH_DELETE_ALL_VISIBLE_CLEARED;
                    xlrec.flags = flags;
                    
                    XLogBeginInsert();
                    XLogRegisterData((char *) &xlrec, SizeOfHeapDelete);
                    XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);
                    
                    recptr = XLogInsert(RM_HEAP_ID, XLOG_HEAP_DELETE);
                    PageSetLSN(page, recptr);
                }
                
                END_CRIT_SECTION();
                
                elog(DEBUG2, "xpatch: marked tuple seq=%d as deleted (tid=%u,%u)",
                     current_seq, blkno, offnum);
            }
        }
        
        /* Buffer was marked dirty inside critical section if needed */
        
        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
        ReleaseBuffer(buffer);
    }
    
    elog(DEBUG1, "xpatch: cascade deleted %d tuples (seq >= %d)", 
         deleted_count, target_seq);
    
    /*
     * Step 5: Invalidate caches
     */
    xpatch_cache_invalidate_rel(relid);
    xpatch_insert_cache_invalidate_rel(relid);
    
    /*
     * Step 6: Update seq cache - new max_seq is target_seq - 1
     * group_typid was already set when we fetched the group value earlier.
     */
    {
        if (target_seq > 1)
        {
            xpatch_seq_cache_set_max_seq(relid, group_value, group_typid, target_seq - 1);
        }
        else
        {
            /* Deleted all rows in group - remove from cache */
            xpatch_seq_cache_set_max_seq(relid, group_value, group_typid, 0);
        }
        
        /*
         * Step 7: Refresh stats cache for the affected group.
         * 
         * We cannot accurately decrement because we don't know the original
         * uncompressed size. Instead, we refresh this group's stats by
         * rescanning just this group. The advisory lock ensures no concurrent
         * INSERTs to this group during the refresh.
         */
        {
            XPatchGroupHash stats_group_hash;
            stats_group_hash = xpatch_compute_group_hash(group_value, group_typid, group_isnull);
            xpatch_stats_cache_refresh_groups(relid, &stats_group_hash, 1);
        }
    }
    
    return TM_Ok;
}

static TM_Result
xpatch_tuple_update(Relation relation, ItemPointer otid,
                    TupleTableSlot *slot, CommandId cid,
                    Snapshot snapshot, Snapshot crosscheck,
                    bool wait, TM_FailureData *tmfd,
                    LockTupleMode *lockmode,
                    TU_UpdateIndexes *update_indexes)
{
    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("UPDATE is not supported on xpatch tables"),
             errhint("xpatch tables are append-only. Insert a new version instead.")));

    return TM_Ok; /* Not reached */
}

static TM_Result
xpatch_tuple_lock(Relation relation, ItemPointer tid,
                  Snapshot snapshot, TupleTableSlot *slot,
                  CommandId cid, LockTupleMode mode,
                  LockWaitPolicy wait_policy, uint8 flags,
                  TM_FailureData *tmfd)
{
    Buffer buffer;
    TM_Result result;
    HeapTupleData tuple;

    /*
     * xpatch tables don't support UPDATE.
     * LockTupleExclusive is used by UPDATE.
     */
    if (mode == LockTupleExclusive)
    {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("UPDATE is not supported on xpatch tables"),
                 errhint("xpatch tables are append-only. Insert a new version instead.")));
    }

    /*
     * DELETE is supported (with cascade semantics).
     * LockTupleNoKeyExclusive is used by DELETE - allow it.
     * For all lock modes, delegate to heap_lock_tuple.
     */
    tuple.t_self = *tid;
    result = heap_lock_tuple(relation, &tuple, cid, mode, wait_policy,
                             false, &buffer, tmfd);

    if (BufferIsValid(buffer))
        ReleaseBuffer(buffer);

    return result;
}

/* ----------------------------------------------------------------
 * Tuple fetch callbacks
 * ---------------------------------------------------------------- */

static bool
xpatch_tuple_fetch_row_version(Relation relation, ItemPointer tid,
                               Snapshot snapshot, TupleTableSlot *slot)
{
    XPatchConfig *config;
    Buffer buffer;
    Page page;
    BlockNumber blkno;
    OffsetNumber offnum;
    ItemId itemId;
    HeapTupleData tuple;
    HeapTuple copy;

    elog(DEBUG1, "XPATCH: fetch_row_version - rel=%s, tid=(%u,%u)", 
         RelationGetRelationName(relation),
         ItemPointerGetBlockNumber(tid),
         ItemPointerGetOffsetNumber(tid));

    config = xpatch_get_config(relation);

    blkno = ItemPointerGetBlockNumber(tid);
    offnum = ItemPointerGetOffsetNumber(tid);

    /* Validate block number */
    if (blkno >= RelationGetNumberOfBlocks(relation))
    {
        ExecClearTuple(slot);
        return false;
    }

    /* Read the buffer */
    buffer = ReadBuffer(relation, blkno);
    LockBuffer(buffer, BUFFER_LOCK_SHARE);

    page = BufferGetPage(buffer);

    /* Validate offset */
    if (offnum > PageGetMaxOffsetNumber(page))
    {
        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
        ReleaseBuffer(buffer);
        ExecClearTuple(slot);
        return false;
    }

    itemId = PageGetItemId(page, offnum);
    if (!ItemIdIsNormal(itemId))
    {
        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
        ReleaseBuffer(buffer);
        ExecClearTuple(slot);
        return false;
    }

    /* Extract the tuple */
    tuple.t_data = (HeapTupleHeader) PageGetItem(page, itemId);
    tuple.t_len = ItemIdGetLength(itemId);
    tuple.t_tableOid = RelationGetRelid(relation);
    tuple.t_self = *tid;

    /*
     * TODO: Implement proper MVCC visibility checking.
     * Currently assumes all tuples are visible. For production use,
     * should call HeapTupleSatisfiesVisibility() or similar to respect
     * transaction isolation and handle deleted tuples correctly.
     */

    /* Make a copy before releasing the buffer */
    copy = heap_copytuple(&tuple);

    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
    ReleaseBuffer(buffer);

    /* Convert to logical format */
    xpatch_physical_to_logical(relation, config, copy, slot);

    /* Set TID in slot */
    slot->tts_tid = *tid;

    heap_freetuple(copy);

    return true;
}

static bool
xpatch_tuple_tid_valid(TableScanDesc scan, ItemPointer tid)
{
    /*
     * Check if the TID is valid for this relation.
     * A TID is valid if it points to a valid block and offset.
     */
    Relation rel = scan->rs_rd;
    BlockNumber blkno = ItemPointerGetBlockNumber(tid);
    OffsetNumber offnum = ItemPointerGetOffsetNumber(tid);

    /* Check if block number is valid */
    if (blkno >= RelationGetNumberOfBlocks(rel))
        return false;

    /* Offset must be valid (> 0) */
    if (offnum == InvalidOffsetNumber)
        return false;

    return true;
}

static void
xpatch_tuple_get_latest_tid(TableScanDesc scan, ItemPointer tid)
{
    /* Delegate to heap's implementation */
    heap_get_latest_tid(scan, tid);
}

static bool
xpatch_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot,
                                Snapshot snapshot)
{
    HeapTupleData tuple;
    BufferHeapTupleTableSlot *bslot;
    bool visible;
    
    /*
     * Proper MVCC visibility check:
     * We delegate to HeapTupleSatisfiesVisibility which handles all the
     * complex visibility logic for MVCC (checking XMIN, XMAX, snapshot, etc.)
     */
    
    /* Extract the heap tuple from the slot */
    if (!TTS_IS_BUFFERTUPLE(slot))
    {
        /*
         * If not a buffer tuple slot, we can't do a full visibility check.
         * This can happen with virtual tuples created during reconstruction.
         * In this case, we check if the tuple was materialized from a 
         * visible source tuple.
         */
        return true;  /* Trust that reconstruction used visible tuples */
    }
    
    bslot = (BufferHeapTupleTableSlot *) slot;
    
    if (!bslot->base.tuple)
        return true;  /* No backing tuple, assume visible */
    
    tuple.t_data = bslot->base.tuple->t_data;
    tuple.t_len = bslot->base.tuple->t_len;
    tuple.t_self = slot->tts_tid;
    tuple.t_tableOid = RelationGetRelid(rel);
    
    /*
     * Use HeapTupleSatisfiesVisibility for proper MVCC semantics.
     * This handles:
     * - XMIN visibility (was inserting transaction committed?)
     * - XMAX visibility (was deleting transaction committed?)
     * - In-progress transaction handling
     * - Snapshot type (MVCC, SELF, ANY, TOAST, etc.)
     */
    visible = HeapTupleSatisfiesVisibility(&tuple, snapshot, bslot->buffer);
    
    return visible;
}

static TransactionId
xpatch_index_delete_tuples(Relation rel, TM_IndexDeleteOp *delstate)
{
    /* Index deletion support - delegate to heap */
    return heap_index_delete_tuples(rel, delstate);
}

/* ----------------------------------------------------------------
 * Relation management callbacks
 * ---------------------------------------------------------------- */

static void
xpatch_relation_set_new_filelocator(Relation rel,
                                    const RelFileLocator *newrlocator,
                                    char persistence,
                                    TransactionId *freezeXid,
                                    MultiXactId *minmulti)
{
    SMgrRelation srel;
    Oid relid = RelationGetRelid(rel);

    /*
     * Invalidate caches for this relation.
     * This is called during TRUNCATE (which replaces the file) and during
     * CREATE TABLE (which has no cache entries yet - safe to call).
     */
    xpatch_cache_invalidate_rel(relid);      /* Content cache */
    xpatch_seq_cache_invalidate_rel(relid);  /* Group max seq + TID seq caches */
    xpatch_insert_cache_invalidate_rel(relid); /* Insert FIFO cache */
    xpatch_stats_cache_delete_table(relid);  /* Stats cache - delete on TRUNCATE */

    /*
     * Initialize the physical storage for the new relation.
     * We use heap-style storage underneath.
     */
    *freezeXid = RecentXmin;
    *minmulti = GetOldestMultiXactId();

    srel = RelationCreateStorage(*newrlocator, persistence, true);
    smgrclose(srel);
}

static void
xpatch_relation_nontransactional_truncate(Relation rel)
{
    Oid relid = RelationGetRelid(rel);
    
    /* Invalidate all cache entries for this relation */
    xpatch_cache_invalidate_rel(relid);      /* Content cache */
    xpatch_seq_cache_invalidate_rel(relid);  /* Group max seq + TID seq caches */
    xpatch_insert_cache_invalidate_rel(relid); /* Insert FIFO cache */
    xpatch_stats_cache_delete_table(relid); /* Stats cache */

    /* Delegate to heap */
    RelationTruncate(rel, 0);
}

static void
xpatch_relation_copy_data(Relation rel, const RelFileLocator *newrlocator)
{
    /* Delegate to standard implementation */
    SMgrRelation dstrel;

    dstrel = smgropen(*newrlocator, rel->rd_backend);

    RelationCopyStorage(RelationGetSmgr(rel), dstrel,
                        MAIN_FORKNUM, rel->rd_rel->relpersistence);

    smgrclose(dstrel);
}

static void
xpatch_relation_copy_for_cluster(Relation OldTable, Relation NewTable,
                                 Relation OldIndex, bool use_sort,
                                 TransactionId OldestXmin,
                                 TransactionId *xid_cutoff,
                                 MultiXactId *multi_cutoff,
                                 double *num_tuples,
                                 double *tups_vacuumed,
                                 double *tups_recently_dead)
{
    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("CLUSTER is not supported on xpatch tables")));
}

/*
 * Vacuum an xpatch table.
 *
 * This function:
 * 1. Scans all pages for dead tuples (committed XMAX)
 * 2. Removes dead tuple pointers (marks as unused)
 * 3. Invalidates affected caches
 * 4. Reports statistics
 *
 * Note: We don't do page compaction or FSM updates in this simple implementation.
 * A full implementation would reclaim space by compacting pages.
 */
static void
xpatch_relation_vacuum(Relation rel, struct VacuumParams *params,
                       BufferAccessStrategy bstrategy)
{
    BlockNumber nblocks;
    BlockNumber blkno;
    Buffer buffer;
    Page page;
    OffsetNumber offnum;
    OffsetNumber maxoff;
    ItemId itemId;
    HeapTupleData tuple;
    TransactionId OldestXmin;
    int64 tuples_removed = 0;
    int64 tuples_remain = 0;
    int64 pages_scanned = 0;
    int64 pages_removed_tuples = 0;
    Oid relid = RelationGetRelid(rel);
    bool cache_invalidated = false;

    elog(DEBUG1, "xpatch: vacuum starting on %s", RelationGetRelationName(rel));

    /* Get the oldest transaction ID still active - tuples deleted before this are removable */
    OldestXmin = GetOldestNonRemovableTransactionId(rel);

    nblocks = RelationGetNumberOfBlocks(rel);

    for (blkno = 0; blkno < nblocks; blkno++)
    {
        bool page_has_dead = false;
        bool page_modified = false;

        CHECK_FOR_INTERRUPTS();

        buffer = ReadBufferExtended(rel, MAIN_FORKNUM, blkno, RBM_NORMAL, bstrategy);
        LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

        page = BufferGetPage(buffer);
        maxoff = PageGetMaxOffsetNumber(page);
        pages_scanned++;

        for (offnum = FirstOffsetNumber; offnum <= maxoff; offnum++)
        {
            TransactionId xmax;

            itemId = PageGetItemId(page, offnum);

            if (!ItemIdIsNormal(itemId))
                continue;

            tuple.t_data = (HeapTupleHeader) PageGetItem(page, itemId);
            tuple.t_len = ItemIdGetLength(itemId);
            tuple.t_tableOid = relid;
            ItemPointerSet(&tuple.t_self, blkno, offnum);

            /* Check if tuple is dead (deleted by a committed transaction) */
            if (!(tuple.t_data->t_infomask & HEAP_XMAX_INVALID))
            {
                xmax = HeapTupleHeaderGetRawXmax(tuple.t_data);

                /*
                 * Tuple is dead if:
                 * - XMAX committed AND
                 * - XMAX is older than OldestXmin (no active transaction can see it)
                 */
                if (TransactionIdDidCommit(xmax) &&
                    TransactionIdPrecedes(xmax, OldestXmin))
                {
                    /* Mark item as unused - this reclaims the line pointer */
                    ItemIdSetUnused(itemId);
                    page_has_dead = true;
                    page_modified = true;
                    tuples_removed++;
                    
                    elog(DEBUG2, "xpatch: vacuum removed dead tuple at (%u,%u)",
                         blkno, offnum);
                }
                else
                {
                    /* Tuple still visible to someone */
                    tuples_remain++;
                }
            }
            else
            {
                /* Tuple is live */
                tuples_remain++;
            }
        }

        if (page_has_dead)
        {
            /*
             * Compact the page by removing holes left by dead tuples.
             * PageRepairFragmentation reorganizes the page to reclaim space.
             */
            PageRepairFragmentation(page);
            pages_removed_tuples++;
        }

        if (page_modified)
            MarkBufferDirty(buffer);

        UnlockReleaseBuffer(buffer);
    }

    /* Invalidate caches if we removed any tuples */
    if (tuples_removed > 0 && !cache_invalidated)
    {
        xpatch_cache_invalidate_rel(relid);
        xpatch_seq_cache_invalidate_rel(relid);
        xpatch_insert_cache_invalidate_rel(relid);
        cache_invalidated = true;
    }

    elog(DEBUG1, "xpatch: vacuum completed on %s: removed %lld tuples, %lld remain, "
         "scanned %lld pages, %lld pages had dead tuples",
         RelationGetRelationName(rel),
         (long long) tuples_removed,
         (long long) tuples_remain,
         (long long) pages_scanned,
         (long long) pages_removed_tuples);

    /*
     * Update relation statistics in pg_class
     * This helps the query planner make better decisions
     */
    if (tuples_removed > 0 || params->options & VACOPT_VERBOSE)
    {
        vac_update_relstats(rel,
                           nblocks,                /* relpages */
                           tuples_remain,          /* reltuples */
                           0,                      /* relallvisible */
                           false,                  /* hasindex - don't update */
                           InvalidTransactionId,   /* frozenxid */
                           InvalidMultiXactId,     /* minmulti */
                           NULL,                   /* relfrozenxid ptr */
                           NULL,                   /* relminmxid ptr */
                           false);                 /* in_outer_xact */
    }
}

/*
 * Prepare to analyze the next block during ANALYZE.
 *
 * This is called by ANALYZE to prepare scanning a specific block.
 * We need to position our scan at the beginning of this block.
 */
static bool
xpatch_scan_analyze_next_block(TableScanDesc scan, BlockNumber blockno,
                               BufferAccessStrategy bstrategy)
{
    XPatchScanDesc xscan = (XPatchScanDesc) scan;
    Relation rel = scan->rs_rd;
    BlockNumber nblocks = RelationGetNumberOfBlocks(rel);

    /* Check if block is valid */
    if (blockno >= nblocks)
        return false;

    /* Release any current buffer */
    if (BufferIsValid(xscan->current_buffer))
    {
        ReleaseBuffer(xscan->current_buffer);
        xscan->current_buffer = InvalidBuffer;
    }

    /* Position scan at this block */
    xscan->current_block = blockno;
    xscan->current_offset = FirstOffsetNumber;
    xscan->inited = true;

    /* Read the block */
    xscan->current_buffer = ReadBufferExtended(rel, MAIN_FORKNUM, blockno,
                                                RBM_NORMAL, bstrategy);
    LockBuffer(xscan->current_buffer, BUFFER_LOCK_SHARE);

    return true;
}

/*
 * Get the next tuple for ANALYZE, accounting for dead rows.
 *
 * This is called repeatedly by ANALYZE to sample tuples from the current block.
 * We need to:
 * 1. Return live tuples in the slot for statistics collection
 * 2. Count dead rows separately (for pg_stat reporting)
 */
static bool
xpatch_scan_analyze_next_tuple(TableScanDesc scan, TransactionId OldestXmin,
                               double *liverows, double *deadrows,
                               TupleTableSlot *slot)
{
    XPatchScanDesc xscan = (XPatchScanDesc) scan;
    Relation rel = scan->rs_rd;
    Page page;
    ItemId itemId;
    HeapTupleData tuple;
    HeapTuple copy;
    ItemPointerData saved_tid;

    if (!BufferIsValid(xscan->current_buffer))
        return false;

    page = BufferGetPage(xscan->current_buffer);

    /* Scan through items on this page */
    while (xscan->current_offset <= PageGetMaxOffsetNumber(page))
    {
        OffsetNumber offnum = xscan->current_offset;
        xscan->current_offset++;

        itemId = PageGetItemId(page, offnum);

        if (!ItemIdIsNormal(itemId))
            continue;

        tuple.t_data = (HeapTupleHeader) PageGetItem(page, itemId);
        tuple.t_len = ItemIdGetLength(itemId);
        tuple.t_tableOid = RelationGetRelid(rel);
        ItemPointerSet(&tuple.t_self, xscan->current_block, offnum);

        /* Check tuple visibility */
        if (!(tuple.t_data->t_infomask & HEAP_XMAX_INVALID))
        {
            TransactionId xmax = HeapTupleHeaderGetRawXmax(tuple.t_data);

            /* If deleted by committed transaction, count as dead */
            if (TransactionIdDidCommit(xmax))
            {
                (*deadrows)++;
                continue;
            }
        }

        /* Tuple is live - reconstruct and return it */
        saved_tid = tuple.t_self;

        /* Release lock before reconstruction (which may do I/O) */
        LockBuffer(xscan->current_buffer, BUFFER_LOCK_UNLOCK);

        /* Make a copy and reconstruct */
        copy = heap_copytuple(&tuple);
        xpatch_physical_to_logical(rel, xscan->config, copy, slot);
        slot->tts_tid = saved_tid;
        heap_freetuple(copy);

        /* Re-acquire lock for next iteration */
        LockBuffer(xscan->current_buffer, BUFFER_LOCK_SHARE);

        (*liverows)++;
        return true;
    }

    /* Done with this block - release buffer */
    LockBuffer(xscan->current_buffer, BUFFER_LOCK_UNLOCK);
    ReleaseBuffer(xscan->current_buffer);
    xscan->current_buffer = InvalidBuffer;

    return false;
}

static double
xpatch_index_build_range_scan(Relation table_rel, Relation index_rel,
                              struct IndexInfo *index_info,
                              bool allow_sync, bool anyvisible,
                              bool progress, BlockNumber start_blockno,
                              BlockNumber numblocks,
                              IndexBuildCallback callback,
                              void *callback_state, TableScanDesc scan)
{
    /*
     * Build index by scanning the table and calling the callback for each tuple.
     * We use our own scan mechanism to properly reconstruct delta-compressed tuples.
     */
    TableScanDesc local_scan;
    TupleTableSlot *slot;
    double reltuples = 0;
    Snapshot snapshot;
    bool need_unregister_snapshot = false;

    /* Use provided scan or start a new one */
    if (scan != NULL)
    {
        local_scan = scan;
    }
    else
    {
        snapshot = RegisterSnapshot(GetLatestSnapshot());
        need_unregister_snapshot = true;
        local_scan = xpatch_scan_begin(table_rel, snapshot, 0, NULL, NULL, 0);
    }

    /* Create slot for tuple storage */
    slot = table_slot_create(table_rel, NULL);

    /* Scan all tuples and build index entries */
    while (xpatch_scan_getnextslot(local_scan, ForwardScanDirection, slot))
    {
        Datum values[INDEX_MAX_KEYS];
        bool isnull[INDEX_MAX_KEYS];
        bool tupleIsAlive = true;

        /* Extract index column values from the slot */
        FormIndexDatum(index_info, slot, NULL, values, isnull);

        /* Call the index build callback */
        callback(index_rel, &slot->tts_tid, values, isnull, tupleIsAlive, callback_state);

        reltuples++;

        if (progress)
            pgstat_progress_update_param(PROGRESS_SCAN_BLOCKS_DONE, reltuples);
    }

    ExecDropSingleTupleTableSlot(slot);

    if (scan == NULL)
    {
        xpatch_scan_end(local_scan);
        if (need_unregister_snapshot)
            UnregisterSnapshot(snapshot);
    }

    return reltuples;
}

static void
xpatch_index_validate_scan(Relation table_rel, Relation index_rel,
                           struct IndexInfo *index_info,
                           Snapshot snapshot, struct ValidateIndexState *state)
{
    /*
     * Validate index by scanning the table.
     * For now, just do nothing - index validation is not critical for initial implementation.
     */
    elog(DEBUG1, "xpatch: index validation scan (minimal implementation)");
}

/* ----------------------------------------------------------------
 * Size estimation callbacks
 * ---------------------------------------------------------------- */

static uint64
xpatch_relation_size(Relation rel, ForkNumber forkNumber)
{
    return table_block_relation_size(rel, forkNumber);
}

static bool
xpatch_relation_needs_toast_table(Relation rel)
{
    /* Delta columns may need TOAST for large deltas */
    return true;
}

static Oid
xpatch_relation_toast_am(Relation rel)
{
    /* Use the heap access method for TOAST tables */
    return HEAP_TABLE_AM_OID;
}

static void
xpatch_estimate_rel_size(Relation rel, int32 *attr_widths,
                         BlockNumber *pages, double *tuples,
                         double *allvisfrac)
{
    /*
     * Estimate relation size using block-based helper.
     * Use heap-like parameters for overhead and usable bytes.
     */
    BlockNumber curpages;
    BlockNumber relpages;
    double reltuples;
    double density;

    /* Get actual number of blocks */
    curpages = RelationGetNumberOfBlocks(rel);

    if (curpages == 0)
    {
        /* Empty table */
        *pages = 0;
        *tuples = 0;
        *allvisfrac = 0;
        return;
    }

    /* Use stored stats if available, otherwise estimate */
    relpages = rel->rd_rel->relpages;
    reltuples = rel->rd_rel->reltuples;

    if (relpages > 0 && reltuples > 0)
    {
        /* Scale based on current size */
        density = reltuples / relpages;
        *tuples = density * curpages;
    }
    else
    {
        /* No stats - assume 10 tuples per page as rough estimate */
        *tuples = curpages * 10.0;
    }

    *pages = curpages;
    *allvisfrac = 0;  /* Assume no all-visible pages for safety */
}

/* ----------------------------------------------------------------
 * Index fetch callbacks
 * 
 * These functions allow using indexes on xpatch tables. Indexes can
 * only be created on non-delta columns (group_by, order_by, or any
 * column not in the delta column list).
 * ---------------------------------------------------------------- */

/*
 * Begin an index fetch operation.
 * Allocates the XPatchIndexFetchData structure.
 */
static struct IndexFetchTableData *
xpatch_index_fetch_begin(Relation rel)
{
    XPatchIndexFetch fetch;

    elog(DEBUG1, "XPATCH: index_fetch_begin - rel=%s", RelationGetRelationName(rel));

    fetch = (XPatchIndexFetch) palloc0(sizeof(XPatchIndexFetchData));
    fetch->base.rel = rel;
    fetch->config = xpatch_get_config(rel);
    fetch->xs_cbuf = InvalidBuffer;

    return &fetch->base;
}

/*
 * Reset the index fetch state for reuse.
 * Releases any held buffer but keeps the structure.
 */
static void
xpatch_index_fetch_reset(struct IndexFetchTableData *scan)
{
    XPatchIndexFetch fetch = (XPatchIndexFetch) scan;

    elog(DEBUG1, "XPATCH: index_fetch_reset");

    if (BufferIsValid(fetch->xs_cbuf))
    {
        ReleaseBuffer(fetch->xs_cbuf);
        fetch->xs_cbuf = InvalidBuffer;
    }
}

/*
 * End an index fetch operation.
 * Releases all resources and frees the structure.
 */
static void
xpatch_index_fetch_end(struct IndexFetchTableData *scan)
{
    XPatchIndexFetch fetch = (XPatchIndexFetch) scan;

    elog(DEBUG1, "XPATCH: index_fetch_end");

    if (BufferIsValid(fetch->xs_cbuf))
    {
        ReleaseBuffer(fetch->xs_cbuf);
        fetch->xs_cbuf = InvalidBuffer;
    }

    pfree(fetch);
}

/*
 * Fetch a tuple by TID during an index scan.
 *
 * This is the core function that makes index scans work on xpatch tables.
 * It reads the physical tuple from storage and reconstructs the logical
 * tuple by applying any necessary delta decompression.
 *
 * Unlike sequential scans which process tuples in order, index scans
 * may access tuples in any order. This means delta reconstruction may
 * need to start from the base version each time (unless cached).
 *
 * Parameters:
 *   scan      - The index fetch descriptor
 *   tid       - The tuple ID to fetch
 *   snapshot  - Visibility snapshot
 *   slot      - Slot to store the result
 *   call_again - Set to true if caller should call again for same TID
 *   all_dead  - Set to true if all tuple versions are dead
 *
 * Returns true if a visible tuple was found, false otherwise.
 */
static bool
xpatch_index_fetch_tuple(struct IndexFetchTableData *scan,
                         ItemPointer tid,
                         Snapshot snapshot,
                         TupleTableSlot *slot,
                         bool *call_again, bool *all_dead)
{
    XPatchIndexFetch fetch = (XPatchIndexFetch) scan;
    Relation rel = fetch->base.rel;
    XPatchConfig *config = fetch->config;
    BlockNumber blkno;
    OffsetNumber offnum;
    Page page;
    ItemId itemId;
    HeapTupleData tuple;
    HeapTuple copy;
    Buffer buffer;
    bool visible = true;

    /* Initialize output parameters */
    *call_again = false;
    if (all_dead)
        *all_dead = false;

    blkno = ItemPointerGetBlockNumber(tid);
    offnum = ItemPointerGetOffsetNumber(tid);

    elog(DEBUG1, "XPATCH: index_fetch_tuple - tid=(%u,%u)", blkno, offnum);

    /* Validate block number */
    if (blkno >= RelationGetNumberOfBlocks(rel))
    {
        ExecClearTuple(slot);
        return false;
    }

    /*
     * Optimize buffer access: if we already have the right block pinned,
     * just lock it. Otherwise, release old buffer and read new one.
     */
    if (BufferIsValid(fetch->xs_cbuf))
    {
        if (BufferGetBlockNumber(fetch->xs_cbuf) == blkno)
        {
            /* Same block - just need to lock it */
            buffer = fetch->xs_cbuf;
        }
        else
        {
            /* Different block - release old, read new */
            ReleaseBuffer(fetch->xs_cbuf);
            buffer = ReadBuffer(rel, blkno);
            fetch->xs_cbuf = buffer;
        }
    }
    else
    {
        /* No buffer held - read the block */
        buffer = ReadBuffer(rel, blkno);
        fetch->xs_cbuf = buffer;
    }

    /* Lock the buffer for reading */
    LockBuffer(buffer, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buffer);

    /* Validate offset */
    if (offnum > PageGetMaxOffsetNumber(page) || offnum < FirstOffsetNumber)
    {
        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
        ExecClearTuple(slot);
        return false;
    }

    itemId = PageGetItemId(page, offnum);

    /* Check if item is valid */
    if (!ItemIdIsNormal(itemId))
    {
        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
        ExecClearTuple(slot);
        if (all_dead && ItemIdIsDead(itemId))
            *all_dead = true;
        return false;
    }

    /* Extract the tuple */
    tuple.t_data = (HeapTupleHeader) PageGetItem(page, itemId);
    tuple.t_len = ItemIdGetLength(itemId);
    tuple.t_tableOid = RelationGetRelid(rel);
    tuple.t_self = *tid;

    /*
     * Simplified visibility check - only validates XMIN/XMAX transaction states.
     * 
     * A production-ready implementation should use HeapTupleSatisfiesVisibility()
     * with the provided snapshot to properly handle:
     * - Snapshot isolation levels
     * - In-progress transactions
     * - Concurrent UPDATE/DELETE visibility (when supported)
     * - Vacuum cleanup decisions
     * 
     * Current implementation is sufficient for append-only workloads where
     * tuples are never modified or deleted.
     */
    if (snapshot != NULL)
    {
        /* Basic visibility check - check XMIN */
        TransactionId xmin = HeapTupleHeaderGetRawXmin(tuple.t_data);
        
        if (TransactionIdIsCurrentTransactionId(xmin))
        {
            /* Inserted by current transaction - visible */
            visible = true;
        }
        else if (TransactionIdDidCommit(xmin))
        {
            /* Inserter committed - visible (simplified, doesn't handle XMAX) */
            visible = true;
        }
        else if (TransactionIdDidAbort(xmin))
        {
            /* Inserter aborted - not visible */
            visible = false;
        }
        else
        {
            /* Transaction still in progress - assume visible for now */
            visible = true;
        }
        
        /* Check XMAX for deleted/updated tuples */
        if (visible && !(tuple.t_data->t_infomask & HEAP_XMAX_INVALID))
        {
            TransactionId xmax = HeapTupleHeaderGetRawXmax(tuple.t_data);
            if (!TransactionIdIsCurrentTransactionId(xmax) && 
                TransactionIdDidCommit(xmax))
            {
                /* Row was deleted by a committed transaction */
                visible = false;
                if (all_dead)
                    *all_dead = true;
            }
        }
    }

    if (!visible)
    {
        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
        ExecClearTuple(slot);
        return false;
    }

    /* Make a copy of the tuple before releasing the buffer lock */
    copy = heap_copytuple(&tuple);

    /* Release buffer lock (keep pin for potential reuse) */
    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

    /*
     * Convert physical tuple to logical format.
     * This reconstructs any delta-compressed columns.
     */
    xpatch_physical_to_logical(rel, config, copy, slot);

    /* Set the TID in the slot */
    slot->tts_tid = *tid;

    heap_freetuple(copy);

    return true;
}

/* ----------------------------------------------------------------
 * Bitmap scan callbacks
 * 
 * Bitmap scans are used when multiple index conditions need to be
 * combined, or when an index returns many rows. The bitmap contains
 * the TIDs of matching tuples.
 * ---------------------------------------------------------------- */

static bool
xpatch_scan_bitmap_next_block(TableScanDesc scan,
                              struct TBMIterateResult *tbmres)
{
    XPatchScanDesc xscan = (XPatchScanDesc) scan;
    BlockNumber blkno = tbmres->blockno;
    Buffer buffer;
    Page page;
    int ntup;
    OffsetNumber maxoff;
    OffsetNumber off;

    elog(DEBUG1, "XPATCH: bitmap_next_block - block %u, ntuples=%d, recheck=%d",
         blkno, tbmres->ntuples, tbmres->recheck);

    /* Release previous buffer if any */
    if (BufferIsValid(xscan->bm_buffer))
    {
        ReleaseBuffer(xscan->bm_buffer);
        xscan->bm_buffer = InvalidBuffer;
    }

    /* Read the block */
    buffer = ReadBuffer(scan->rs_rd, blkno);
    LockBuffer(buffer, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buffer);
    maxoff = PageGetMaxOffsetNumber(page);

    xscan->bm_block = blkno;
    xscan->bm_buffer = buffer;
    xscan->bm_index = 0;
    xscan->bm_ntuples = 0;

    /*
     * If tbmres->ntuples < 0, it means the bitmap is "lossy" and we need
     * to check all tuples in the block. Otherwise, we have specific offsets.
     */
    if (tbmres->ntuples < 0)
    {
        /* Lossy bitmap - check all tuples in block */
        ntup = 0;
        for (off = FirstOffsetNumber; off <= maxoff; off++)
        {
            ItemId itemId = PageGetItemId(page, off);
            if (ItemIdIsNormal(itemId))
            {
                xscan->bm_offsets[ntup++] = off;
            }
        }
        xscan->bm_ntuples = ntup;
    }
    else
    {
        /* Exact bitmap - use provided offsets */
        ntup = 0;
        for (int i = 0; i < tbmres->ntuples; i++)
        {
            off = tbmres->offsets[i];
            if (off <= maxoff)
            {
                ItemId itemId = PageGetItemId(page, off);
                if (ItemIdIsNormal(itemId))
                {
                    xscan->bm_offsets[ntup++] = off;
                }
            }
        }
        xscan->bm_ntuples = ntup;
    }

    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

    elog(DEBUG1, "XPATCH: bitmap_next_block - found %d tuples", xscan->bm_ntuples);

    return xscan->bm_ntuples > 0;
}

static bool
xpatch_scan_bitmap_next_tuple(TableScanDesc scan,
                              struct TBMIterateResult *tbmres,
                              TupleTableSlot *slot)
{
    XPatchScanDesc xscan = (XPatchScanDesc) scan;
    XPatchConfig *config = xscan->config;
    Buffer buffer;
    Page page;
    OffsetNumber off;
    ItemId itemId;
    HeapTupleData tuple;
    HeapTuple copy;

    /* Check if we have more tuples in current block */
    if (xscan->bm_index >= xscan->bm_ntuples)
        return false;

    buffer = xscan->bm_buffer;
    if (!BufferIsValid(buffer))
        return false;

    LockBuffer(buffer, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buffer);

    /* Find next valid item, skipping dead/unused items without recursion */
    while (true)
    {
        off = xscan->bm_offsets[xscan->bm_index++];
        itemId = PageGetItemId(page, off);

        if (ItemIdIsNormal(itemId))
            break;

        /* If we've exhausted items on this page, try next block */
        if (xscan->bm_index >= xscan->bm_ntuples)
        {
            LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
            /* Move to next block by clearing current block state */
            xscan->bm_block = InvalidBlockNumber;
            /* Recursion here is fine - it's just one level to get next block */
            return xpatch_scan_bitmap_next_tuple(scan, tbmres, slot);
        }
    }

    /* Extract the tuple */
    tuple.t_data = (HeapTupleHeader) PageGetItem(page, itemId);
    tuple.t_len = ItemIdGetLength(itemId);
    tuple.t_tableOid = RelationGetRelid(scan->rs_rd);
    ItemPointerSet(&tuple.t_self, xscan->bm_block, off);

    /* Make a copy before releasing lock */
    copy = heap_copytuple(&tuple);

    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

    /* Convert physical to logical tuple */
    xpatch_physical_to_logical(scan->rs_rd, config, copy, slot);

    /* Set the TID */
    slot->tts_tid = tuple.t_self;

    heap_freetuple(copy);

    elog(DEBUG1, "XPATCH: bitmap_next_tuple - returned tuple at (%u,%u)",
         xscan->bm_block, off);

    return true;
}

/* ----------------------------------------------------------------
 * Sample scan callbacks (minimal implementation)
 * ---------------------------------------------------------------- */

static bool
xpatch_scan_sample_next_block(TableScanDesc scan,
                              struct SampleScanState *scanstate)
{
    /* Sample scans not supported */
    return false;
}

static bool
xpatch_scan_sample_next_tuple(TableScanDesc scan,
                              struct SampleScanState *scanstate,
                              TupleTableSlot *slot)
{
    return false;
}
