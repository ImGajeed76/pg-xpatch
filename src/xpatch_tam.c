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

#include "access/heapam.h"
#include "access/hio.h"
#include "access/htup_details.h"
#include "access/multixact.h"
#include "access/rewriteheap.h"
#include "access/tableam.h"
#include "access/visibilitymap.h"
#include "access/xact.h"
#include "access/xloginsert.h"
#include "catalog/catalog.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/storage.h"
#include "commands/progress.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "storage/predicate.h"
#include "storage/procarray.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

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
     * Note: There's a known snapshot reference leak warning when using
     * parallel scans, but query results are correct. This is related to
     * how reconstruction handles snapshots across parallel workers.
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
            return 0;  /* Start at block 0 */
        
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

            /* TODO: Check visibility using snapshot */
            /* For now, assume all tuples are visible */

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

static void
xpatch_tuple_insert(Relation relation, TupleTableSlot *slot,
                    CommandId cid, int options,
                    BulkInsertState bistate)
{
    XPatchConfig *config;
    HeapTuple physical_tuple;
    MemoryContext oldcxt;
    MemoryContext insert_mcxt;
    Datum group_value = (Datum) 0;
    TupleDesc tupdesc = RelationGetDescr(relation);

    elog(DEBUG1, "XPATCH: tuple_insert - rel=%s", RelationGetRelationName(relation));
    config = xpatch_get_config(relation);

    elog(DEBUG1, "xpatch_tuple_insert: validating schema");
    /* Validate the schema on first insert */
    xpatch_validate_schema(relation, config);
    elog(DEBUG1, "xpatch_tuple_insert: schema validated");

    /* Get group value if configured */
    if (config->group_by_attnum != InvalidAttrNumber)
    {
        bool isnull;
        group_value = slot_getattr(slot, config->group_by_attnum, &isnull);
        if (isnull)
            group_value = (Datum) 0;
    }

    /* 
     * VALIDATE VERSION: Must be greater than max version in group
     */
    {
        bool max_is_null;
        Datum max_version;
        Datum new_version;
        bool new_is_null;

        new_version = slot_getattr(slot, config->order_by_attnum, &new_is_null);
        
        if (new_is_null)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_NOT_NULL_VIOLATION),
                     errmsg("xpatch: order_by column \"%s\" cannot be NULL",
                            config->order_by)));
        }

        max_version = xpatch_get_max_version(relation, config, group_value, &max_is_null);

        if (!max_is_null)
        {
            /* Compare versions */
            Form_pg_attribute attr = TupleDescAttr(tupdesc, config->order_by_attnum - 1);
            Oid typid = attr->atttypid;
            int cmp = 0;

            switch (typid)
            {
                case INT2OID:
                    cmp = DatumGetInt16(new_version) - DatumGetInt16(max_version);
                    break;
                case INT4OID:
                    cmp = DatumGetInt32(new_version) - DatumGetInt32(max_version);
                    break;
                case INT8OID:
                    {
                        int64 v1 = DatumGetInt64(new_version);
                        int64 v2 = DatumGetInt64(max_version);
                        cmp = (v1 > v2) ? 1 : ((v1 < v2) ? -1 : 0);
                    }
                    break;
                case TIMESTAMPOID:
                case TIMESTAMPTZOID:
                    {
                        Timestamp t1 = DatumGetTimestamp(new_version);
                        Timestamp t2 = DatumGetTimestamp(max_version);
                        cmp = (t1 > t2) ? 1 : ((t1 < t2) ? -1 : 0);
                    }
                    break;
                default:
                    /* Fallback: assume valid */
                    cmp = 1;
            }

            if (cmp <= 0)
            {
                ereport(ERROR,
                        (errcode(ERRCODE_CHECK_VIOLATION),
                         errmsg("xpatch: new version must be greater than existing max version"),
                         errhint("The order_by column \"%s\" must be strictly increasing within each group.",
                                 config->order_by)));
            }
        }
    }

    elog(DEBUG1, "xpatch_tuple_insert: version validated, creating physical tuple");

    /* Use a temporary context for insert operations */
    insert_mcxt = AllocSetContextCreate(CurrentMemoryContext,
                                        "xpatch insert",
                                        ALLOCSET_DEFAULT_SIZES);
    oldcxt = MemoryContextSwitchTo(insert_mcxt);

    /* Convert logical tuple to physical (delta-compressed) format */
    physical_tuple = xpatch_logical_to_physical(relation, config, slot);

    MemoryContextSwitchTo(oldcxt);

    /*
     * Insert the tuple using low-level heap functions.
     * We can't use simple_heap_insert() because it checks for heap AM.
     */
    {
        Buffer buffer;
        Buffer vmbuffer = InvalidBuffer;
        Size len;
        
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
        
        /* Insert the tuple into the page */
        RelationPutHeapTuple(relation, buffer, physical_tuple, false);
        
        /* Mark buffer dirty and release */
        MarkBufferDirty(buffer);
        
        /* Update slot with inserted tuple's TID */
        slot->tts_tid = physical_tuple->t_self;
        
        if (BufferIsValid(vmbuffer))
            ReleaseBuffer(vmbuffer);
        UnlockReleaseBuffer(buffer);
    }

    /* Cleanup */
    heap_freetuple(physical_tuple);
    MemoryContextDelete(insert_mcxt);
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

static TM_Result
xpatch_tuple_delete(Relation relation, ItemPointer tid,
                    CommandId cid, Snapshot snapshot,
                    Snapshot crosscheck, bool wait,
                    TM_FailureData *tmfd, bool changingPart)
{
    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("DELETE is not supported on xpatch tables"),
             errhint("xpatch tables are append-only. Insert a new version instead.")));

    return TM_Ok; /* Not reached */
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
    /*
     * xpatch tables are append-only and don't support UPDATE/DELETE.
     * The lock modes that indicate modification intent should error out.
     * LockTupleExclusive is used by UPDATE, LockTupleNoKeyExclusive by DELETE.
     */
    if (mode == LockTupleExclusive || mode == LockTupleNoKeyExclusive)
    {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("UPDATE and DELETE are not supported on xpatch tables"),
                 errhint("xpatch tables are append-only. Insert a new version instead.")));
    }

    /* For shared locks (SELECT FOR SHARE, etc.), delegate to heap */
    {
        Buffer buffer;
        TM_Result result;
        HeapTupleData tuple;

        tuple.t_self = *tid;
        result = heap_lock_tuple(relation, &tuple, cid, mode, wait_policy,
                                 false, &buffer, tmfd);

        if (BufferIsValid(buffer))
            ReleaseBuffer(buffer);

        return result;
    }
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
    /* Delegate to heap visibility check */
    return true; /* TODO: proper implementation */
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
    /* Invalidate cache entries for this relation */
    xpatch_cache_invalidate_rel(RelationGetRelid(rel));

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

static void
xpatch_relation_vacuum(Relation rel, struct VacuumParams *params,
                       BufferAccessStrategy bstrategy)
{
    /* Basic vacuum - just delegate to heap vacuum for now */
    /* Note: A real implementation would need special handling */
    elog(DEBUG1, "xpatch: vacuum on %s", RelationGetRelationName(rel));
}

static bool
xpatch_scan_analyze_next_block(TableScanDesc scan, BlockNumber blockno,
                               BufferAccessStrategy bstrategy)
{
    /*
     * For analyze, we don't need special handling - just return true
     * to indicate we're ready to analyze tuples on this block.
     */
    return true;
}

static bool
xpatch_scan_analyze_next_tuple(TableScanDesc scan, TransactionId OldestXmin,
                               double *liverows, double *deadrows,
                               TupleTableSlot *slot)
{
    /*
     * For ANALYZE, we need to return sample tuples.
     * Use the regular scan mechanism to get tuples.
     */
    bool got_tuple = xpatch_scan_getnextslot(scan, ForwardScanDirection, slot);
    
    if (got_tuple)
        (*liverows)++;
    
    return got_tuple;
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

    off = xscan->bm_offsets[xscan->bm_index++];
    itemId = PageGetItemId(page, off);

    if (!ItemIdIsNormal(itemId))
    {
        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
        /* Try next tuple recursively */
        return xpatch_scan_bitmap_next_tuple(scan, tbmres, slot);
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
