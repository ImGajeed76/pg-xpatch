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
 * xpatch_storage.c - Physical tuple handling with delta compression
 *
 * This file implements the core compression and reconstruction logic.
 * 
 * Physical Storage Format:
 * - We store tuples with delta-compressed content in the delta columns
 * - Keyframes are encoded with tag=0 (XPATCH_KEYFRAME_TAG) against empty base
 * - Deltas reference previous versions via tag: tag=1 means previous row,
 *   tag=2 means 2 rows back, etc.
 * 
 * IMPORTANT: We cannot use heap_beginscan on xpatch tables because that
 * requires heap AM. Instead, we use direct buffer access.
 */

#include "xpatch_storage.h"
#include "xpatch_compress.h"
#include "xpatch_cache.h"
#include "xpatch_seq_cache.h"
#include "xpatch_insert_cache.h"
#include "xpatch_encode_pool.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/tableam.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "executor/tuptable.h"
#include "storage/bufmgr.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/typcache.h"
#include "funcapi.h"

/* Forward declarations */
static bytea *datum_to_bytea(Datum value, Oid typid, bool isnull);
static Oid get_group_column_typid(Relation rel, XPatchConfig *config);

/*
 * Compare two Datums for equality using the type's equality operator.
 * This handles collation-sensitive types like TEXT correctly.
 *
 * Uses TypeCacheEntry to get the proper equality function for the type,
 * and FunctionCall2Coll to invoke it with the correct collation.
 *
 * This is the correct way to compare datums - datumIsEqual only works
 * for pass-by-value types or simple byte-wise comparison of fixed-length
 * types, and fails for varlena types like TEXT where it just compares pointers.
 */
bool
xpatch_datums_equal(Datum d1, Datum d2, Oid typid, Oid collation)
{
    TypeCacheEntry *typcache;

    /* Get equality function from type cache */
    typcache = lookup_type_cache(typid, TYPECACHE_EQ_OPR_FINFO);

    if (!OidIsValid(typcache->eq_opr_finfo.fn_oid))
    {
        /* Fallback for types without equality operator - byte-wise compare */
        int16 typlen;
        bool typbyval;

        get_typlenbyval(typid, &typlen, &typbyval);
        return datumIsEqual(d1, d2, typbyval, typlen);
    }

    /* Use the type's equality function with proper collation */
    return DatumGetBool(FunctionCall2Coll(&typcache->eq_opr_finfo,
                                          collation, d1, d2));
}

/*
 * Convert a Datum of various varlena types to bytea for compression.
 * 
 * All supported delta column types (TEXT, VARCHAR, BYTEA, JSON, JSONB) are
 * varlena types - they have a varlena header followed by raw bytes. We simply
 * treat them all as raw bytes for delta encoding. This is simpler and faster
 * than type-specific conversions.
 * 
 * The xpatch library doesn't care about the semantic meaning of the bytes -
 * it just compresses/decompresses byte arrays. We preserve the exact binary
 * representation so reconstruction is lossless.
 */
static bytea *
datum_to_bytea(Datum value, Oid typid, bool isnull)
{
    struct varlena *src;
    bytea *result;
    Size len;
    
    if (isnull)
        return NULL;
    
    /* 
     * All our supported types are varlena. Get the detoasted value
     * to ensure we have the full data (not compressed or out-of-line).
     */
    switch (typid)
    {
        case BYTEAOID:
        case TEXTOID:
        case VARCHAROID:
        case JSONOID:
        case JSONBOID:
            /* All varlena types - treat uniformly as raw bytes */
            src = (struct varlena *) PG_DETOAST_DATUM(value);
            len = VARSIZE_ANY(src);
            result = (bytea *) palloc(len);
            memcpy(result, src, len);
            
            /* Free detoasted copy if it was created */
            if ((Pointer) src != DatumGetPointer(value))
                pfree(src);
            
            return result;
            
        default:
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("unsupported delta column type: %u", typid)));
            return NULL;
    }
}

/*
 * Get the type OID of the group_by column.
 * Returns InvalidOid if no group_by column is configured.
 */
static Oid
get_group_column_typid(Relation rel, XPatchConfig *config)
{
    TupleDesc tupdesc;
    Form_pg_attribute attr;
    
    if (config->group_by_attnum == InvalidAttrNumber)
        return InvalidOid;
    
    tupdesc = RelationGetDescr(rel);
    attr = TupleDescAttr(tupdesc, config->group_by_attnum - 1);
    return attr->atttypid;
}

/*
 * Convert bytea back to original Datum type.
 * 
 * Since we stored the raw varlena bytes in datum_to_bytea, we just need to
 * return a copy of those bytes. The data already has the correct varlena
 * header and binary format for its type.
 * 
 * We always return a fresh copy to ensure the caller owns the memory and
 * it won't be unexpectedly freed (important for tuple materialization in Sort).
 * 
 * Exported for use by xpatch_tam.c for lazy reconstruction.
 */
Datum
bytea_to_datum(bytea *data, Oid typid)
{
    Size len;
    void *copy;
    
    if (data == NULL)
        return (Datum) 0;
    
    switch (typid)
    {
        case BYTEAOID:
        case TEXTOID:
        case VARCHAROID:
        case JSONOID:
        case JSONBOID:
            /* 
             * All varlena types - the data already contains the complete
             * varlena representation (header + content). Just copy it.
             */
            len = VARSIZE_ANY(data);
            copy = palloc(len);
            memcpy(copy, data, len);
            return PointerGetDatum(copy);
            
        default:
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("unsupported delta column type: %u", typid)));
            return (Datum) 0;
    }
}

/*
 * Scan the table using direct buffer access to find the maximum sequence number.
 * Returns 0 if the group is empty.
 * 
 * Reads MAX(_xp_seq) from tuples directly - O(n) scan but only reads one int per tuple.
 * 
 * OPTIMIZATION: First checks the seq cache for O(1) lookup via hash table.
 * On cache miss, performs a full table scan and populates the cache.
 * 
 * This function is called during INSERT to:
 * 1. Determine the next sequence number for a new version
 * 2. Find base versions for delta compression
 * 
 * After the first call for a group, subsequent calls hit the cache and return
 * immediately without scanning.
 */
int32
xpatch_get_max_seq(Relation rel, XPatchConfig *config, Datum group_value)
{
    int32 max_seq = 0;
    bool found;
    TupleDesc tupdesc;
    BlockNumber nblocks;
    BlockNumber blkno;
    Buffer buffer;
    Page page;
    OffsetNumber offnum;
    OffsetNumber maxoff;
    ItemId itemId;
    HeapTupleData tuple;
    Form_pg_attribute attr;
    Datum tuple_group;
    bool group_isnull;
    Oid group_typid = InvalidOid;
    
    tupdesc = RelationGetDescr(rel);
    
    /* _xp_seq column is required */
    if (config->xp_seq_attnum == InvalidAttrNumber)
    {
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("xpatch: table \"%s\" is missing required _xp_seq column",
                        RelationGetRelationName(rel)),
                 errhint("Recreate the table or run: ALTER TABLE %s ADD COLUMN _xp_seq INT",
                         RelationGetRelationName(rel))));
    }
    
    /* Get group column type OID for proper hash computation */
    if (config->group_by_attnum != InvalidAttrNumber)
    {
        Form_pg_attribute group_attr = TupleDescAttr(tupdesc, config->group_by_attnum - 1);
        group_typid = group_attr->atttypid;
    }
    
    /* Try cache first - O(1) lookup */
    max_seq = xpatch_seq_cache_get_max_seq(RelationGetRelid(rel), group_value, group_typid, &found);
    if (found)
    {
        elog(DEBUG1, "xpatch: get_max_seq cache hit for group, max_seq=%d", max_seq);
        return max_seq;
    }
    
    elog(DEBUG1, "xpatch: get_max_seq cache miss, scanning table");
    
    /* Cache miss - scan the table */
    max_seq = 0;
    nblocks = RelationGetNumberOfBlocks(rel);
    
    /* Scan all blocks */
    for (blkno = 0; blkno < nblocks; blkno++)
    {
        buffer = ReadBuffer(rel, blkno);
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
            tuple.t_tableOid = RelationGetRelid(rel);
            ItemPointerSet(&tuple.t_self, blkno, offnum);
            
            /*
             * MVCC visibility check: skip tuples that are not visible.
             */
            {
                TransactionId xmin = HeapTupleHeaderGetRawXmin(tuple.t_data);
                
                /* Skip if inserter hasn't committed and isn't us */
                if (!TransactionIdIsCurrentTransactionId(xmin) &&
                    !TransactionIdDidCommit(xmin))
                    continue;
                
                /* Skip if tuple is deleted by a committed transaction */
                if (!(tuple.t_data->t_infomask & HEAP_XMAX_INVALID))
                {
                    TransactionId xmax = HeapTupleHeaderGetRawXmax(tuple.t_data);
                    if (TransactionIdDidCommit(xmax))
                        continue;
                }
            }
            
            /* If we have group_by, check if this tuple matches */
            if (config->group_by_attnum != InvalidAttrNumber)
            {
                tuple_group = heap_getattr(&tuple, config->group_by_attnum, tupdesc, &group_isnull);
                
                if (!group_isnull)
                {
                    attr = TupleDescAttr(tupdesc, config->group_by_attnum - 1);
                    if (!xpatch_datums_equal(group_value, tuple_group, attr->atttypid, attr->attcollation))
                        continue;
                }
            }
            
            /* Get sequence number from _xp_seq column */
            {
                bool seq_isnull;
                Datum seq_datum = heap_getattr(&tuple, config->xp_seq_attnum, tupdesc, &seq_isnull);
                if (!seq_isnull)
                {
                    int32 tuple_seq = DatumGetInt32(seq_datum);
                    if (tuple_seq > max_seq)
                        max_seq = tuple_seq;
                }
            }
        }
        
        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
        ReleaseBuffer(buffer);
    }
    
    /* Populate the cache for future lookups */
    xpatch_seq_cache_set_max_seq(RelationGetRelid(rel), group_value, group_typid, max_seq);
    
    return max_seq;
}

/*
 * Get the maximum version value for a group using direct buffer access.
 */
Datum
xpatch_get_max_version(Relation rel, XPatchConfig *config,
                       Datum group_value, bool *is_null)
{
    Datum max_version = (Datum) 0;
    bool found = false;
    TupleDesc tupdesc;
    BlockNumber nblocks;
    BlockNumber blkno;
    Buffer buffer;
    Page page;
    OffsetNumber offnum;
    OffsetNumber maxoff;
    ItemId itemId;
    HeapTupleData tuple;
    bool isnull;
    Datum version_datum;
    Datum tuple_group;
    bool group_isnull;
    Form_pg_attribute attr;
    Form_pg_attribute order_attr;
    Oid typid;
    int cmp;
    int64 v1, v2;
    Timestamp t1, t2;
    
    tupdesc = RelationGetDescr(rel);
    nblocks = RelationGetNumberOfBlocks(rel);
    
    /* Scan all blocks */
    for (blkno = 0; blkno < nblocks; blkno++)
    {
        buffer = ReadBuffer(rel, blkno);
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
            tuple.t_tableOid = RelationGetRelid(rel);
            ItemPointerSet(&tuple.t_self, blkno, offnum);
            
            /*
             * MVCC visibility check: skip tuples that are not visible.
             * A tuple is visible if:
             * 1. XMIN is committed (or current txn) AND
             * 2. Not deleted (XMAX invalid) OR delete not yet committed
             */
            {
                TransactionId xmin = HeapTupleHeaderGetRawXmin(tuple.t_data);
                
                /* Skip if inserter hasn't committed and isn't us */
                if (!TransactionIdIsCurrentTransactionId(xmin) &&
                    !TransactionIdDidCommit(xmin))
                    continue;
                
                /* Skip if tuple is deleted by a committed transaction */
                if (!(tuple.t_data->t_infomask & HEAP_XMAX_INVALID))
                {
                    TransactionId xmax = HeapTupleHeaderGetRawXmax(tuple.t_data);
                    if (TransactionIdDidCommit(xmax))
                        continue;
                }
            }
            
            version_datum = heap_getattr(&tuple, config->order_by_attnum, tupdesc, &isnull);
            
            if (isnull)
                continue;
            
            /* If group_by is specified, check if this tuple matches */
            if (config->group_by_attnum != InvalidAttrNumber)
            {
                tuple_group = heap_getattr(&tuple, config->group_by_attnum, tupdesc, &group_isnull);
                
                if (group_isnull)
                    continue;
                
                attr = TupleDescAttr(tupdesc, config->group_by_attnum - 1);
                if (!xpatch_datums_equal(group_value, tuple_group, attr->atttypid, attr->attcollation))
                    continue;
            }
            
            if (!found)
            {
                max_version = version_datum;
                found = true;
            }
            else
            {
                /* Compare versions based on type */
                order_attr = TupleDescAttr(tupdesc, config->order_by_attnum - 1);
                typid = order_attr->atttypid;
                cmp = 0;
                
                switch (typid)
                {
                    case INT2OID:
                        {
                            int16 a = DatumGetInt16(version_datum);
                            int16 b = DatumGetInt16(max_version);
                            cmp = (a > b) ? 1 : ((a < b) ? -1 : 0);
                        }
                        break;
                    case INT4OID:
                        {
                            int32 a = DatumGetInt32(version_datum);
                            int32 b = DatumGetInt32(max_version);
                            cmp = (a > b) ? 1 : ((a < b) ? -1 : 0);
                        }
                        break;
                    case INT8OID:
                        v1 = DatumGetInt64(version_datum);
                        v2 = DatumGetInt64(max_version);
                        cmp = (v1 > v2) ? 1 : ((v1 < v2) ? -1 : 0);
                        break;
                    case TIMESTAMPOID:
                    case TIMESTAMPTZOID:
                        t1 = DatumGetTimestamp(version_datum);
                        t2 = DatumGetTimestamp(max_version);
                        cmp = (t1 > t2) ? 1 : ((t1 < t2) ? -1 : 0);
                        break;
                    default:
                        cmp = 0;
                }
                
                if (cmp > 0)
                    max_version = version_datum;
            }
        }
        
        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
        ReleaseBuffer(buffer);
    }
    
    *is_null = !found;
    return max_version;
}

/*
 * Helper: Fetch tuple by TID with visibility check.
 * Returns a palloc'd HeapTuple if the tuple at the given TID is valid and visible,
 * NULL otherwise.
 */
static HeapTuple
fetch_tuple_by_tid(Relation rel, ItemPointer tid)
{
    Buffer buffer;
    Page page;
    ItemId itemId;
    HeapTupleData tuple;
    HeapTuple result = NULL;
    BlockNumber blkno;
    OffsetNumber offnum;
    
    blkno = ItemPointerGetBlockNumber(tid);
    offnum = ItemPointerGetOffsetNumber(tid);
    
    /* Check block number is valid */
    if (blkno >= RelationGetNumberOfBlocks(rel))
        return NULL;
    
    buffer = ReadBuffer(rel, blkno);
    LockBuffer(buffer, BUFFER_LOCK_SHARE);
    
    page = BufferGetPage(buffer);
    
    /* Check offset is valid */
    if (offnum > PageGetMaxOffsetNumber(page))
    {
        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
        ReleaseBuffer(buffer);
        return NULL;
    }
    
    itemId = PageGetItemId(page, offnum);
    
    if (ItemIdIsNormal(itemId))
    {
        tuple.t_data = (HeapTupleHeader) PageGetItem(page, itemId);
        tuple.t_len = ItemIdGetLength(itemId);
        tuple.t_tableOid = RelationGetRelid(rel);
        ItemPointerCopy(tid, &tuple.t_self);
        
        /* MVCC visibility check */
        {
            TransactionId xmin = HeapTupleHeaderGetRawXmin(tuple.t_data);
            
            /* Skip if inserter hasn't committed and isn't us */
            if (TransactionIdIsCurrentTransactionId(xmin) ||
                TransactionIdDidCommit(xmin))
            {
                /* Check if tuple is deleted */
                if ((tuple.t_data->t_infomask & HEAP_XMAX_INVALID) ||
                    !TransactionIdDidCommit(HeapTupleHeaderGetRawXmax(tuple.t_data)))
                {
                    result = heap_copytuple(&tuple);
                }
            }
        }
    }
    
    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
    ReleaseBuffer(buffer);
    
    return result;
}

/*
 * Helper: Find index OID for the _xp_seq index on a table.
 * Returns the OID of the index, or InvalidOid if not found.
 * 
 * Looks for either:
 * - <tablename>_xp_seq_idx (basic index on _xp_seq)
 * - <tablename>_xp_group_seq_idx (composite index on (group_by, _xp_seq))
 */
static Oid
find_xp_seq_index(Relation rel, XPatchConfig *config)
{
    List *indexList;
    ListCell *lc;
    Oid result = InvalidOid;
    const char *relname = RelationGetRelationName(rel);
    char basic_idx_name[NAMEDATALEN];
    char composite_idx_name[NAMEDATALEN];
    
    snprintf(basic_idx_name, NAMEDATALEN, "%s_xp_seq_idx", relname);
    snprintf(composite_idx_name, NAMEDATALEN, "%s_xp_group_seq_idx", relname);
    
    indexList = RelationGetIndexList(rel);
    
    foreach(lc, indexList)
    {
        Oid indexOid = lfirst_oid(lc);
        Relation indexRel = index_open(indexOid, AccessShareLock);
        const char *indexName = RelationGetRelationName(indexRel);
        
        /* Prefer composite index if group_by is configured */
        if (config->group_by_attnum != InvalidAttrNumber &&
            strcmp(indexName, composite_idx_name) == 0)
        {
            result = indexOid;
            index_close(indexRel, AccessShareLock);
            break;
        }
        
        /* Fall back to basic index */
        if (strcmp(indexName, basic_idx_name) == 0)
        {
            result = indexOid;
        }
        
        index_close(indexRel, AccessShareLock);
    }
    
    list_free(indexList);
    return result;
}

/*
 * Helper: Fetch tuple using index scan on _xp_seq.
 * This is O(log n) compared to O(n) for full table scan.
 */
static HeapTuple
fetch_by_seq_using_index(Relation rel, XPatchConfig *config,
                         Datum group_value, int32 target_seq, ItemPointer out_tid)
{
    Oid indexOid;
    Relation indexRel;
    IndexScanDesc scan;
    ScanKeyData scankeys[2];
    int nkeys;
    HeapTuple result = NULL;
    TupleDesc tupdesc;
    
    indexOid = find_xp_seq_index(rel, config);
    if (!OidIsValid(indexOid))
    {
        elog(DEBUG1, "xpatch: no _xp_seq index found, falling back to sequential scan");
        return NULL;  /* No index - caller will fall back to seq scan */
    }
    
    tupdesc = RelationGetDescr(rel);
    indexRel = index_open(indexOid, AccessShareLock);
    
    /* Set up scan keys */
    nkeys = 0;
    
    /* If we have a composite index (group_by, _xp_seq), use both keys */
    if (config->group_by_attnum != InvalidAttrNumber)
    {
        Form_pg_attribute group_attr = TupleDescAttr(tupdesc, config->group_by_attnum - 1);
        Oid eqop = InvalidOid;
        
        /* Get equality operator for the group column type */
        eqop = OpernameGetOprid(list_make1(makeString("=")), 
                                group_attr->atttypid, group_attr->atttypid);
        
        if (OidIsValid(eqop))
        {
            ScanKeyInit(&scankeys[nkeys],
                        1,  /* First column in composite index is group_by */
                        BTEqualStrategyNumber,
                        get_opcode(eqop),
                        group_value);
            nkeys++;
            
            ScanKeyInit(&scankeys[nkeys],
                        2,  /* Second column is _xp_seq */
                        BTEqualStrategyNumber,
                        F_INT4EQ,
                        Int32GetDatum(target_seq));
            nkeys++;
        }
    }
    else
    {
        /* Basic index on just _xp_seq */
        ScanKeyInit(&scankeys[nkeys],
                    1,  /* First (and only) column is _xp_seq */
                    BTEqualStrategyNumber,
                    F_INT4EQ,
                    Int32GetDatum(target_seq));
        nkeys++;
    }
    
    /* Start index scan */
    scan = index_beginscan(rel, indexRel, GetActiveSnapshot(), nkeys, 0);
    index_rescan(scan, scankeys, nkeys, NULL, 0);
    
    /* Fetch the tuple using index_getnext_tid + heap fetch */
    while (true)
    {
        ItemPointer tid;
        HeapTupleData tuple;
        Buffer buffer;
        bool found;
        
        tid = index_getnext_tid(scan, ForwardScanDirection);
        if (tid == NULL)
            break;
        
        /* Fetch the heap tuple */
        tuple.t_self = *tid;
        found = heap_fetch(rel, GetActiveSnapshot(), &tuple, &buffer, false);
        
        if (!found)
            continue;
        
        /* For composite index, we've already filtered by group, so this is our tuple */
        /* For basic index, we need to verify the group matches */
        if (config->group_by_attnum != InvalidAttrNumber && nkeys == 1)
        {
            /* Basic index only - need to check group manually */
            bool group_isnull;
            Datum tuple_group;
            Form_pg_attribute attr;
            
            tuple_group = heap_getattr(&tuple, config->group_by_attnum, tupdesc, &group_isnull);
            
            if (group_isnull)
            {
                ReleaseBuffer(buffer);
                continue;
            }
            
            attr = TupleDescAttr(tupdesc, config->group_by_attnum - 1);
            if (!xpatch_datums_equal(group_value, tuple_group, attr->atttypid, attr->attcollation))
            {
                ReleaseBuffer(buffer);
                continue;
            }
        }
        
        /* Found it! Copy the TID for caching */
        if (out_tid)
            ItemPointerCopy(tid, out_tid);
        
        result = heap_copytuple(&tuple);
        ReleaseBuffer(buffer);
        break;
    }
    
    index_endscan(scan);
    index_close(indexRel, AccessShareLock);
    
    return result;
}

/*
 * Fetch a physical row by group and sequence number.
 * 
 * OPTIMIZED VERSION - uses three strategies in order:
 * 1. Seq-to-TID cache lookup - O(1) hash table lookup
 * 2. Index scan on _xp_seq - O(log n) B-tree lookup
 * 3. Sequential scan as fallback - O(n) if no index exists
 * 
 * The seq-to-TID cache is populated on successful lookups to speed up
 * subsequent requests for the same (group, seq) pair.
 */
HeapTuple
xpatch_fetch_by_seq(Relation rel, XPatchConfig *config,
                    Datum group_value, int32 target_seq)
{
    HeapTuple result = NULL;
    TupleDesc tupdesc;
    Oid group_typid = InvalidOid;
    ItemPointerData cached_tid;
    ItemPointerData found_tid;
    
    tupdesc = RelationGetDescr(rel);
    
    /* Get group column type for cache operations */
    if (config->group_by_attnum != InvalidAttrNumber)
    {
        Form_pg_attribute group_attr = TupleDescAttr(tupdesc, config->group_by_attnum - 1);
        group_typid = group_attr->atttypid;
    }
    
    /* Strategy 1: Check seq-to-TID cache for O(1) lookup */
    if (xpatch_seq_cache_get_seq_tid(RelationGetRelid(rel), group_value, group_typid,
                                      target_seq, &cached_tid))
    {
        elog(DEBUG2, "xpatch: fetch_by_seq cache HIT for seq=%d", target_seq);
        
        /* Fetch tuple by cached TID */
        result = fetch_tuple_by_tid(rel, &cached_tid);
        
        if (result != NULL)
        {
            /* Verify the tuple still has the right seq (in case of VACUUM/UPDATE) */
            bool seq_isnull;
            Datum seq_datum = heap_getattr(result, config->xp_seq_attnum, tupdesc, &seq_isnull);
            
            if (!seq_isnull && DatumGetInt32(seq_datum) == target_seq)
            {
                /* Cache hit valid - return the tuple */
                return result;
            }
            
            /* Cache stale - TID no longer points to our seq. Free and continue. */
            heap_freetuple(result);
            result = NULL;
            elog(DEBUG2, "xpatch: fetch_by_seq cache STALE for seq=%d", target_seq);
        }
    }
    
    /* Strategy 2: Use index scan - O(log n) */
    ItemPointerSetInvalid(&found_tid);
    result = fetch_by_seq_using_index(rel, config, group_value, target_seq, &found_tid);
    
    if (result != NULL)
    {
        /* Populate the cache for future lookups */
        xpatch_seq_cache_set_seq_tid(RelationGetRelid(rel), group_value, group_typid,
                                      target_seq, &found_tid);
        return result;
    }
    
    /* Strategy 3: Fall back to sequential scan - O(n) */
    elog(DEBUG1, "xpatch: fetch_by_seq falling back to sequential scan for seq=%d", target_seq);
    {
        BlockNumber nblocks;
        BlockNumber blkno;
        Buffer buffer;
        Page page;
        OffsetNumber offnum;
        OffsetNumber maxoff;
        ItemId itemId;
        HeapTupleData tuple;
        Datum tuple_group;
        bool group_isnull;
        Form_pg_attribute attr;
        
        nblocks = RelationGetNumberOfBlocks(rel);
        
        /* Scan all blocks */
        for (blkno = 0; blkno < nblocks && result == NULL; blkno++)
        {
            buffer = ReadBuffer(rel, blkno);
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
                tuple.t_tableOid = RelationGetRelid(rel);
                ItemPointerSet(&tuple.t_self, blkno, offnum);
                
                /* Check group if specified */
                if (config->group_by_attnum != InvalidAttrNumber)
                {
                    tuple_group = heap_getattr(&tuple, config->group_by_attnum, tupdesc, &group_isnull);
                    
                    if (group_isnull)
                        continue;
                    
                    attr = TupleDescAttr(tupdesc, config->group_by_attnum - 1);
                    if (!xpatch_datums_equal(group_value, tuple_group, attr->atttypid, attr->attcollation))
                        continue;
                }
                
                /* Check sequence number from _xp_seq column */
                {
                    bool seq_isnull;
                    Datum seq_datum = heap_getattr(&tuple, config->xp_seq_attnum, tupdesc, &seq_isnull);
                    if (!seq_isnull && DatumGetInt32(seq_datum) == target_seq)
                    {
                        result = heap_copytuple(&tuple);
                        
                        /* Cache the TID for future lookups */
                        xpatch_seq_cache_set_seq_tid(RelationGetRelid(rel), group_value, group_typid,
                                                      target_seq, &tuple.t_self);
                        break;
                    }
                }
            }
            
            LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
            ReleaseBuffer(buffer);
        }
    }
    
    return result;
}

/*
 * Internal: Reconstruct a delta column given its compressed bytea directly.
 * This avoids re-fetching the tuple when we already have the compressed data.
 * 
 * This is a helper function used by both reconstruction paths:
 * 1. xpatch_reconstruct_column_with_tuple() - scan fast path (already has tuple)
 * 2. xpatch_reconstruct_column() - recursive base lookup (fetches by seq)
 * 
 * The function handles two cases:
 * - Keyframe (tag=0xFF...): Decode against empty base (full content stored)
 * - Delta (tag=1,2,3...): Recursively fetch base version and decode against it
 * 
 * Results are cached in the LRU content cache to avoid redundant decompression.
 */
static bytea *
xpatch_reconstruct_from_delta(Relation rel, XPatchConfig *config,
                              Datum group_value, int32 seq,
                              int delta_col_index, bytea *delta_bytea)
{
    bytea *result;
    size_t tag;
    const char *err;
    bytea *base_content;
    int32 base_seq;
    AttrNumber attnum;
    
    attnum = config->delta_attnums[delta_col_index];
    
    /* Extract tag to determine if this is a keyframe or delta */
    err = xpatch_get_delta_tag((uint8 *) VARDATA_ANY(delta_bytea),
                               VARSIZE_ANY_EXHDR(delta_bytea),
                               &tag);
    if (err != NULL)
    {
        ereport(ERROR,
                (errcode(ERRCODE_DATA_CORRUPTED),
                 errmsg("xpatch: failed to extract tag: %s", err)));
    }
    
    if (tag == XPATCH_KEYFRAME_TAG)
    {
        /* Keyframe: decode against empty base */
        result = xpatch_decode_delta(NULL, 0,
                                     (uint8 *) VARDATA_ANY(delta_bytea),
                                     VARSIZE_ANY_EXHDR(delta_bytea));
    }
    else
    {
        /* Delta: calculate base sequence from tag */
        base_seq = seq - tag;
        
        if (base_seq < 1)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_DATA_CORRUPTED),
                     errmsg("xpatch: invalid base sequence %d (tag=%zu, seq=%d)",
                            base_seq, tag, seq)));
        }
        
        /* Recursively get base content (will hit cache if previously decoded) */
        base_content = xpatch_reconstruct_column(rel, config, group_value,
                                                  base_seq, delta_col_index);
        
        /* Decode this delta against the base */
        if (base_content == NULL)
        {
            result = xpatch_decode_delta(NULL, 0,
                                         (uint8 *) VARDATA_ANY(delta_bytea),
                                         VARSIZE_ANY_EXHDR(delta_bytea));
        }
        else
        {
            result = xpatch_decode_delta((uint8 *) VARDATA_ANY(base_content),
                                         VARSIZE_ANY_EXHDR(base_content),
                                         (uint8 *) VARDATA_ANY(delta_bytea),
                                         VARSIZE_ANY_EXHDR(delta_bytea));
            pfree(base_content);
        }
    }
    
    /* Cache the result */
    if (result != NULL)
    {
        Oid group_typid = get_group_column_typid(rel, config);
        xpatch_cache_put(RelationGetRelid(rel), group_value, group_typid, seq, attnum, result);
    }
    
    return result;
}

/*
 * Reconstruct the content of a delta column for a specific version.
 * 
 * This version fetches the tuple by sequence number - used for:
 * 1. Recursive base lookups during delta reconstruction
 * 2. INSERT operations that need to compare with previous versions
 * 
 * For scan operations, use xpatch_reconstruct_column_with_tuple() instead,
 * which avoids re-fetching the tuple you already have (12x faster).
 * 
 * Reconstruction flow:
 * 1. Check LRU cache - O(1) hash lookup
 * 2. On miss: Fetch physical tuple by sequence - O(n) scan
 * 3. Extract compressed data from tuple
 * 4. If delta: recursively get base (may hit cache)
 * 5. Decode and cache result
 */
bytea *
xpatch_reconstruct_column(Relation rel, XPatchConfig *config,
                          Datum group_value, int32 seq,
                          int delta_col_index)
{
    HeapTuple physical_tuple;
    TupleDesc tupdesc;
    AttrNumber attnum;
    Form_pg_attribute attr;
    Oid typid;
    bool isnull;
    Datum delta_datum;
    bytea *delta_bytea;
    bytea *result;
    
    tupdesc = RelationGetDescr(rel);
    attnum = config->delta_attnums[delta_col_index];
    attr = TupleDescAttr(tupdesc, attnum - 1);
    typid = attr->atttypid;
    
    /* 1. Check LRU cache first - O(1) hash lookup */
    {
        Oid group_typid = get_group_column_typid(rel, config);
        result = xpatch_cache_get(RelationGetRelid(rel), group_value, group_typid, seq, attnum);
        if (result != NULL)
            return result;
    }
    
    /* 2. Fetch the physical tuple for this sequence */
    physical_tuple = xpatch_fetch_by_seq(rel, config, group_value, seq);
    if (physical_tuple == NULL)
    {
        /*
         * Row not found - this could happen if:
         * 1. A previous INSERT failed after allocating a sequence number
         * 2. The row was deleted (which shouldn't normally happen in xpatch)
         * 3. Data corruption
         *
         * We return NULL here instead of throwing an error so callers can
         * handle this gracefully (e.g., by falling back to keyframe encoding).
         */
        elog(WARNING, "xpatch: could not find row with sequence %d (gap in chain?)", seq);
        return NULL;
    }
    
    /* 3. Get the delta/compressed value from the tuple */
    delta_datum = heap_getattr(physical_tuple, attnum, tupdesc, &isnull);
    
    if (isnull)
    {
        heap_freetuple(physical_tuple);
        return NULL;
    }
    
    /* Convert to bytea and reconstruct */
    delta_bytea = datum_to_bytea(delta_datum, typid, false);
    result = xpatch_reconstruct_from_delta(rel, config, group_value, seq,
                                           delta_col_index, delta_bytea);
    
    heap_freetuple(physical_tuple);
    pfree(delta_bytea);
    
    return result;
}

/*
 * Reconstruct a delta column when we already have the physical tuple.
 * 
 * PERFORMANCE CRITICAL: This is the fast path for scan operations.
 * 
 * During sequential scans, we already have the physical tuple in memory
 * from reading the page. The naive approach would be to:
 *   1. Look up the tuple's sequence number
 *   2. Call xpatch_reconstruct_column() 
 *   3. Have it call xpatch_fetch_by_seq() to re-fetch the SAME tuple
 *   4. Do an O(n) scan to find it again
 * 
 * This function bypasses that wasteful re-fetch by accepting the tuple
 * we already have, providing a massive performance improvement for scans.
 * With this optimization, count(*) on 10k rows: 11ms vs 135ms (12x faster).
 */
bytea *
xpatch_reconstruct_column_with_tuple(Relation rel, XPatchConfig *config,
                                     HeapTuple physical_tuple,
                                     Datum group_value, int32 seq,
                                     int delta_col_index)
{
    TupleDesc tupdesc;
    AttrNumber attnum;
    Form_pg_attribute attr;
    Oid typid;
    bool isnull;
    Datum delta_datum;
    bytea *delta_bytea;
    bytea *result;
    
    tupdesc = RelationGetDescr(rel);
    attnum = config->delta_attnums[delta_col_index];
    attr = TupleDescAttr(tupdesc, attnum - 1);
    typid = attr->atttypid;
    
    /* Check LRU cache first */
    {
        Oid group_typid = get_group_column_typid(rel, config);
        result = xpatch_cache_get(RelationGetRelid(rel), group_value, group_typid, seq, attnum);
        if (result != NULL)
            return result;
    }
    
    /* Get the delta/compressed value from the tuple we already have */
    delta_datum = heap_getattr(physical_tuple, attnum, tupdesc, &isnull);
    
    if (isnull)
        return NULL;
    
    /* Convert to bytea and reconstruct */
    delta_bytea = datum_to_bytea(delta_datum, typid, false);
    result = xpatch_reconstruct_from_delta(rel, config, group_value, seq,
                                           delta_col_index, delta_bytea);
    pfree(delta_bytea);
    
    return result;
}

/*
 * Convert a logical tuple (from user INSERT) to physical format.
 * This performs delta compression on configured columns.
 *
 * Supports "restore mode" for pg_dump/pg_restore: if the user explicitly
 * provides a non-NULL _xp_seq value, that value is used instead of
 * auto-generating. This allows COPY FROM to restore data correctly.
 */
HeapTuple
xpatch_logical_to_physical(Relation rel, XPatchConfig *config,
                           TupleTableSlot *slot, int32 *out_seq)
{
    TupleDesc tupdesc;
    int natts;
    Datum *values;
    bool *nulls;
    HeapTuple result;
    int32 new_seq;
    bool is_keyframe;
    Datum group_value = (Datum) 0;
    bool isnull;
    int i, j;
    AttrNumber attnum;
    bool is_delta_col;
    int delta_col_index;
    Form_pg_attribute attr;
    Oid typid;
    bytea *raw_content;
    bytea *compressed;
    bytea *best_delta;
    Size best_size;
    int best_tag;
    int tag;
    int32 base_seq;
    bytea *base_content;
    bytea *candidate;
    Size candidate_size;
    bytea *cache_content;
    bool restore_mode = false;
    int32 user_seq = 0;
    Oid group_typid = InvalidOid;
    int insert_cache_slot = -1;
    bool insert_cache_is_new = false;
    
    /* Initialize out_seq to 0 (will be set later if allocation succeeds) */
    if (out_seq)
        *out_seq = 0;
    
    tupdesc = RelationGetDescr(rel);
    natts = tupdesc->natts;
    
    /* Ensure slot is materialized early so we can check _xp_seq */
    slot_getallattrs(slot);
    
    /* Get group value if configured */
    if (config->group_by_attnum != InvalidAttrNumber)
    {
        group_value = slot_getattr(slot, config->group_by_attnum, &isnull);
        if (isnull)
            group_value = (Datum) 0;
    }
    
    /* Get group column type OID for proper hash computation */
    if (config->group_by_attnum != InvalidAttrNumber)
    {
        Form_pg_attribute group_attr = TupleDescAttr(tupdesc, config->group_by_attnum - 1);
        group_typid = group_attr->atttypid;
    }
    
    /*
     * Restore mode: if user explicitly provides _xp_seq > 0, use it.
     * This enables pg_restore / COPY FROM with explicit sequence numbers.
     * Normal inserts (without explicit _xp_seq) auto-allocate.
     */
    if (config->xp_seq_attnum != InvalidAttrNumber)
    {
        Datum seq_datum = slot_getattr(slot, config->xp_seq_attnum, &isnull);
        if (!isnull)
        {
            user_seq = DatumGetInt32(seq_datum);
            if (user_seq > 0)
            {
                restore_mode = true;
                new_seq = user_seq;
                elog(DEBUG1, "xpatch: restore mode - using explicit _xp_seq=%d", new_seq);

                /*
                 * Update the seq cache if this seq is higher than what we have.
                 * This ensures subsequent auto-generated inserts continue correctly.
                 */
                {
                    bool cache_found;
                    int32 cached_max = xpatch_seq_cache_get_max_seq(RelationGetRelid(rel),
                                                                     group_value, group_typid,
                                                                     &cache_found);
                    if (!cache_found || new_seq > cached_max)
                    {
                        xpatch_seq_cache_set_max_seq(RelationGetRelid(rel), group_value,
                                                     group_typid, new_seq);
                    }
                }
            }
        }
    }
    
    /* 
     * Calculate sequence number using the seq cache (normal mode only).
     * xpatch_seq_cache_next_seq() atomically increments and returns the new seq.
     * If the group isn't in the cache, it returns 0 and we fall back to a scan.
     */
    if (!restore_mode)
    {
        new_seq = xpatch_seq_cache_next_seq(RelationGetRelid(rel), group_value, group_typid);
        
        if (new_seq == 0)
        {
            /* Cache miss or cache full - fall back to scan (populates cache) */
            new_seq = xpatch_get_max_seq(rel, config, group_value) + 1;
            /* Update the cache with the new seq */
            xpatch_seq_cache_set_max_seq(RelationGetRelid(rel), group_value, group_typid, new_seq);
        }
    }
    
    /* 
     * Output the allocated sequence so caller can rollback on failure.
     * This must be done BEFORE any operation that might fail, so the
     * caller knows which sequence to rollback.
     */
    if (out_seq && !restore_mode)
        *out_seq = new_seq;
    
    /* Determine if this is a keyframe */
    is_keyframe = (new_seq == 1) || (new_seq % config->keyframe_every == 1);
    
    elog(DEBUG1, "xpatch: inserting seq %d, is_keyframe=%d%s", new_seq, is_keyframe,
         restore_mode ? " (restore mode)" : "");
    
    /*
     * Acquire FIFO insert cache slot for this (table, group) pair.
     * Needed for both keyframes (to keep FIFO warm) and deltas (to read bases).
     * Not used in restore mode (bulk restore bypasses FIFO).
     * On cold start (is_new=true), populate the FIFO with reconstructed content.
     */
    if (!restore_mode && config->num_delta_columns > 0)
    {
        insert_cache_slot = xpatch_insert_cache_get_slot(
            RelationGetRelid(rel), group_value, group_typid,
            config->compress_depth, config->num_delta_columns,
            &insert_cache_is_new);
        
        if (insert_cache_is_new && insert_cache_slot >= 0 && new_seq > 1 && !is_keyframe)
        {
            /* Cold start: populate FIFO with previous rows */
            int32 max_seq_for_populate = new_seq - 1;
            xpatch_insert_cache_populate(insert_cache_slot, rel, config,
                                         group_value, max_seq_for_populate);
        }
    }

    /* Allocate arrays for physical tuple */
    values = palloc(natts * sizeof(Datum));
    nulls = palloc(natts * sizeof(bool));
    
    /* Copy all attributes, compressing delta columns and setting _xp_seq */
    for (i = 0; i < natts; i++)
    {
        attnum = i + 1;
        is_delta_col = false;
        delta_col_index = -1;
        
        /* Handle _xp_seq column - set to the new sequence number */
        if (config->xp_seq_attnum != InvalidAttrNumber && attnum == config->xp_seq_attnum)
        {
            values[i] = Int32GetDatum(new_seq);
            nulls[i] = false;
            continue;
        }
        
        /* Check if this is a delta column */
        for (j = 0; j < config->num_delta_columns; j++)
        {
            if (config->delta_attnums[j] == attnum)
            {
                is_delta_col = true;
                delta_col_index = j;
                break;
            }
        }
        
        values[i] = slot_getattr(slot, attnum, &nulls[i]);
        
        if (is_delta_col && !nulls[i])
        {
            attr = TupleDescAttr(tupdesc, i);
            typid = attr->atttypid;
            
            /* Convert to bytea */
            raw_content = datum_to_bytea(values[i], typid, false);
            
            if (is_keyframe)
            {
                /* Keyframe: encode against empty base with reserved tag */
                compressed = xpatch_encode_delta(XPATCH_KEYFRAME_TAG,
                                                  NULL, 0,
                                                  (uint8 *) VARDATA_ANY(raw_content),
                                                  VARSIZE_ANY_EXHDR(raw_content),
                                                  config->enable_zstd);
                
                elog(DEBUG1, "xpatch: keyframe col %d: raw=%zu compressed=%zu",
                     delta_col_index,
                     VARSIZE_ANY_EXHDR(raw_content),
                     compressed ? VARSIZE_ANY_EXHDR(compressed) : 0);
            }
            else
            {
                /*
                 * Delta encoding: use FIFO insert cache + parallel encoding.
                 *
                 * 1. Get bases from the FIFO insert cache (pre-materialized)
                 * 2. If FIFO is cold, fall back to reconstruction
                 * 3. Dispatch encoding to thread pool (or sequential if disabled)
                 * 4. Pick smallest result
                 *
                 * Tag convention: tag=N means delta against N rows back
                 *   tag=1: previous row
                 *   tag=2: 2 rows back
                 *   etc.
                 */
                InsertCacheBases *fifo_bases;
                EncodeBatch batch;
                int best_result_idx = -1;

                best_delta = NULL;
                best_size = SIZE_MAX;
                best_tag = 1;

                /* Allocate bases struct sized to actual compress_depth */
                fifo_bases = InsertCacheBasesAlloc(config->compress_depth);

                /* Try to get bases from FIFO cache first */
                if (insert_cache_slot >= 0)
                {
                    xpatch_insert_cache_get_bases(insert_cache_slot, new_seq,
                                                  delta_col_index, fifo_bases);
                }

                if (fifo_bases->count > 0)
                {
                    /*
                     * WARM PATH: Bases available from FIFO cache.
                     * Dispatch parallel encoding via thread pool.
                     */
                    memset(&batch, 0, sizeof(batch));
                    batch.new_data = (const uint8_t *) VARDATA_ANY(raw_content);
                    batch.new_len = VARSIZE_ANY_EXHDR(raw_content);
                    batch.enable_zstd = config->enable_zstd;
                    batch.num_tasks = fifo_bases->count;
                    batch.capacity = fifo_bases->count;
                    batch.tasks = palloc0(fifo_bases->count * sizeof(EncodeTask));
                    batch.results = palloc0(fifo_bases->count * sizeof(EncodeResult));

                    for (tag = 0; tag < fifo_bases->count; tag++)
                    {
                        batch.tasks[tag].tag = fifo_bases->bases[tag].tag;
                        batch.tasks[tag].base_data = fifo_bases->bases[tag].data;
                        batch.tasks[tag].base_len = fifo_bases->bases[tag].size;
                    }

                    /* Initialize encode pool on first use if configured */
                    if (xpatch_encode_threads > 0 && fifo_bases->count > 1)
                        xpatch_encode_pool_init();

                    /* Execute batch (parallel if pool available, sequential otherwise) */
                    xpatch_encode_pool_execute(&batch);

                    /* Find smallest result */
                    for (tag = 0; tag < batch.num_tasks; tag++)
                    {
                        if (batch.results[tag].valid && batch.results[tag].size > 0)
                        {
                            if (batch.results[tag].size < best_size)
                            {
                                best_size = batch.results[tag].size;
                                best_tag = batch.results[tag].tag;
                                best_result_idx = tag;
                            }
                        }
                    }

                    /* Copy winning result to palloc'd bytea */
                    if (best_result_idx >= 0)
                    {
                        Size total_size = batch.results[best_result_idx].size + VARHDRSZ;
                        best_delta = (bytea *) palloc(total_size);
                        SET_VARSIZE(best_delta, total_size);
                        memcpy(VARDATA(best_delta),
                               batch.results[best_result_idx].data,
                               batch.results[best_result_idx].size);
                    }

                    /* Free all encode results (Rust allocator) */
                    xpatch_encode_pool_free_results(&batch);

                    /* Free palloc'd task/result arrays */
                    pfree(batch.tasks);
                    pfree(batch.results);

                    /* Free palloc'd base copies from FIFO */
                    for (tag = 0; tag < fifo_bases->count; tag++)
                    {
                        if (fifo_bases->bases[tag].data)
                            pfree((void *) fifo_bases->bases[tag].data);
                    }
                }
                else
                {
                    /*
                     * COLD PATH: No FIFO bases available.
                     * Fall back to sequential reconstruction (same as before).
                     * This only happens on cold start before FIFO is populated.
                     */
                    for (tag = 1; tag <= config->compress_depth; tag++)
                    {
                        base_seq = new_seq - tag;

                        if (base_seq < 1)
                            break;

                        base_content = xpatch_reconstruct_column(rel, config, group_value,
                                                                  base_seq, delta_col_index);
                        if (base_content == NULL)
                            continue;

                        candidate = xpatch_encode_delta(tag,
                                                        (uint8 *) VARDATA_ANY(base_content),
                                                        VARSIZE_ANY_EXHDR(base_content),
                                                        (uint8 *) VARDATA_ANY(raw_content),
                                                        VARSIZE_ANY_EXHDR(raw_content),
                                                        config->enable_zstd);
                        pfree(base_content);

                        if (candidate != NULL)
                        {
                            candidate_size = VARSIZE(candidate);

                            if (candidate_size < best_size)
                            {
                                if (best_delta != NULL)
                                    pfree(best_delta);
                                best_delta = candidate;
                                best_size = candidate_size;
                                best_tag = tag;
                            }
                            else
                            {
                                pfree(candidate);
                            }
                        }
                    }
                }

                /* Free the dynamically allocated bases struct */
                pfree(fifo_bases);

                compressed = best_delta;

                /*
                 * If no valid delta was found (all bases missing or compression failed),
                 * fall back to keyframe encoding. This makes xpatch self-healing for
                 * gaps created by failed inserts.
                 */
                if (compressed == NULL)
                {
                    elog(DEBUG1, "xpatch: no valid base found for delta, falling back to keyframe for col %d",
                         delta_col_index);

                    compressed = xpatch_encode_delta(XPATCH_KEYFRAME_TAG,
                                                      NULL, 0,
                                                      (uint8 *) VARDATA_ANY(raw_content),
                                                      VARSIZE_ANY_EXHDR(raw_content),
                                                      config->enable_zstd);
                    best_tag = XPATCH_KEYFRAME_TAG;
                }

                elog(DEBUG1, "xpatch: delta col %d: raw=%zu compressed=%zu tag=%d",
                     delta_col_index,
                     VARSIZE_ANY_EXHDR(raw_content),
                     compressed ? VARSIZE_ANY_EXHDR(compressed) : 0,
                     best_tag);
            }
            
            if (compressed == NULL)
            {
                pfree(raw_content);
                ereport(ERROR,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                         errmsg("xpatch: compression failed for column %s",
                                config->delta_columns[delta_col_index])));
            }
            
            /* 
             * Store compressed data directly as the column value.
             * For TEXT/BYTEA/VARCHAR, the compressed bytea is compatible.
             * For JSONB, we stored it as text, so the compressed data is 
             * just compressed text bytes - we need to store as-is.
             * The type conversion happens during reconstruction in physical_to_logical.
             */
            values[i] = PointerGetDatum(compressed);
            
            /* Cache the original content for future delta encoding */
            {
                cache_content = datum_to_bytea(slot_getattr(slot, attnum, &isnull), typid, false);
                xpatch_cache_put(RelationGetRelid(rel), group_value, group_typid, new_seq, attnum, cache_content);
                
                /* Push into FIFO insert cache for future inserts */
                if (insert_cache_slot >= 0)
                {
                    xpatch_insert_cache_push(insert_cache_slot, new_seq,
                                             delta_col_index,
                                             (const uint8 *) VARDATA_ANY(cache_content),
                                             VARSIZE_ANY_EXHDR(cache_content));
                }
                
                pfree(cache_content);
            }
            
            pfree(raw_content);
        }
    }
    
    /* Commit the FIFO entry after all delta columns are written */
    if (insert_cache_slot >= 0 && !restore_mode)
    {
        xpatch_insert_cache_commit_entry(insert_cache_slot, new_seq);
    }
    
    /* Build the physical tuple */
    result = heap_form_tuple(tupdesc, values, nulls);
    
    pfree(values);
    pfree(nulls);
    
    return result;
}

/*
 * Convert a physical tuple to logical format.
 * 
 * This is the core reconstruction function called during scans and fetches.
 * It takes a physical tuple (with delta-compressed columns) and reconstructs
 * the full logical tuple (with decompressed content).
 * 
 * Process:
 * 1. Extract all attributes from physical tuple into slot
 * 2. Get sequence number from _xp_seq column (O(1) read)
 * 3. For each delta column: Reconstruct using fast path (tuple already available)
 * 
 * IMPORTANT: Uses xpatch_reconstruct_column_with_tuple() to avoid redundant
 * tuple fetches. This optimization provides 12x speedup vs naive approach.
 */
void
xpatch_physical_to_logical(Relation rel, XPatchConfig *config,
                           HeapTuple physical_tuple,
                           TupleTableSlot *slot)
{
    TupleDesc tupdesc;
    int natts;
    Datum group_value = (Datum) 0;
    int32 seq = 0;
    int i, j;
    AttrNumber attnum;
    Form_pg_attribute attr;
    Oid typid;
    bytea *reconstructed;
    
    tupdesc = RelationGetDescr(rel);
    natts = tupdesc->natts;
    
    /* Clear the slot first - this also resets tts_values/tts_isnull */
    ExecClearTuple(slot);
    
    /* 
     * Extract all attributes directly into slot's arrays.
     * For pass-by-reference types, we must copy the datum because the source
     * tuple may be freed after this function returns.
     */
    for (i = 0; i < natts; i++)
    {
        Form_pg_attribute att = TupleDescAttr(tupdesc, i);
        Datum val = heap_getattr(physical_tuple, i + 1, tupdesc, &slot->tts_isnull[i]);
        
        if (slot->tts_isnull[i])
        {
            slot->tts_values[i] = (Datum) 0;
        }
        else if (att->attbyval)
        {
            slot->tts_values[i] = val;
        }
        else
        {
            slot->tts_values[i] = datumCopy(val, att->attbyval, att->attlen);
        }
    }
    
    /* Get group value if configured */
    if (config->group_by_attnum != InvalidAttrNumber)
    {
        group_value = slot->tts_values[config->group_by_attnum - 1];
        if (slot->tts_isnull[config->group_by_attnum - 1])
            group_value = (Datum) 0;
    }
    
    /* Get sequence number from _xp_seq column */
    if (slot->tts_isnull[config->xp_seq_attnum - 1])
    {
        ereport(ERROR,
                (errcode(ERRCODE_DATA_CORRUPTED),
                 errmsg("xpatch: _xp_seq column is NULL")));
    }
    seq = DatumGetInt32(slot->tts_values[config->xp_seq_attnum - 1]);

    /* Reconstruct delta columns - replace compressed data with decompressed */
    for (j = 0; j < config->num_delta_columns; j++)
    {
        attnum = config->delta_attnums[j];
        i = attnum - 1;
        
        if (!slot->tts_isnull[i])
        {
            attr = TupleDescAttr(tupdesc, i);
            typid = attr->atttypid;
            
            /* Reconstruct using the tuple we already have - avoids re-fetch! */
            reconstructed = xpatch_reconstruct_column_with_tuple(rel, config,
                                                                  physical_tuple,
                                                                  group_value, seq, j);
            
            if (reconstructed != NULL)
            {
                /* Free old compressed value if pass-by-ref */
                if (!attr->attbyval && DatumGetPointer(slot->tts_values[i]) != NULL)
                    pfree(DatumGetPointer(slot->tts_values[i]));
                
                slot->tts_values[i] = bytea_to_datum(reconstructed, typid);
            }
            else
            {
                slot->tts_isnull[i] = true;
            }
        }
    }
    
    /* Mark slot as containing a valid virtual tuple */
    ExecStoreVirtualTuple(slot);
}
