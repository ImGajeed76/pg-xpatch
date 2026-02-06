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
 * xpatch_utils.c - Utility functions
 *
 * Implements SQL-callable utility functions for statistics and inspection.
 */

#include "pg_xpatch.h"
#include "xpatch_config.h"
#include "xpatch_cache.h"
#include "xpatch_compress.h"
#include "xpatch_insert_cache.h"
#include "xpatch_storage.h"
#include "xpatch_stats_cache.h"

#include "access/amapi.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/transam.h"
#include "catalog/pg_am.h"
#include "catalog/pg_collation_d.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

/* Forward declaration for datums_equal helper */
static bool datums_equal(Datum d1, Datum d2, FmgrInfo *eq_finfo, Oid collation);

/*
 * xpatch_stats(regclass) - Get compression statistics for a table
 *
 * Returns statistics about compression efficiency and cache usage for
 * an xpatch table. 
 *
 * Design:
 * 1. Get distinct groups from actual table data
 * 2. Check which groups are missing from stats cache
 * 3. Refresh only the missing groups
 * 4. Aggregate from cache and return
 */
PG_FUNCTION_INFO_V1(xpatch_stats);
Datum
xpatch_stats(PG_FUNCTION_ARGS)
{
    Oid relid = PG_GETARG_OID(0);
    TupleDesc tupdesc;
    Datum values[10];
    bool nulls[10];
    HeapTuple result_tuple;
    XPatchCacheStats cache_stats;
    
    int64 total_rows = 0;
    int64 total_groups = 0;
    int64 keyframe_count = 0;
    int64 raw_size = 0;
    int64 compressed_size = 0;
    double sum_avg_delta_tags = 0.0;
    
    int64 delta_count;
    double avg_compression_depth;
    double compression_ratio;

    /* Build result tuple descriptor */
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("function returning record called in context "
                        "that cannot accept type record")));

    /*
     * Read stats directly from cache.
     * 
     * The cache is kept up-to-date by:
     * - INSERT: increments group stats via xpatch_stats_cache_update_group()
     * - DELETE: refreshes affected group via xpatch_stats_cache_refresh_groups()
     * 
     * No refresh needed here - just read the aggregated stats.
     */
    xpatch_stats_cache_get_table_stats(relid,
                                       &total_rows,
                                       &total_groups,
                                       &keyframe_count,
                                       &raw_size,
                                       &compressed_size,
                                       &sum_avg_delta_tags);

    /* Get cache statistics (separate from stats cache) */
    xpatch_cache_get_stats(&cache_stats);

    /* Compute derived values */
    delta_count = total_rows - keyframe_count;
    compression_ratio = compressed_size > 0 ? (double) raw_size / compressed_size : 0.0;
    avg_compression_depth = delta_count > 0 ? sum_avg_delta_tags / delta_count : 0.0;

    /* Build result tuple */
    memset(nulls, 0, sizeof(nulls));

    values[0] = Int64GetDatum(total_rows);
    values[1] = Int64GetDatum(total_groups);
    values[2] = Int64GetDatum(keyframe_count);
    values[3] = Int64GetDatum(delta_count);
    values[4] = Int64GetDatum(raw_size);
    values[5] = Int64GetDatum(compressed_size);
    values[6] = Float8GetDatum(compression_ratio);
    values[7] = Int64GetDatum(cache_stats.hit_count);
    values[8] = Int64GetDatum(cache_stats.miss_count);
    values[9] = Float8GetDatum(avg_compression_depth);

    result_tuple = heap_form_tuple(tupdesc, values, nulls);

    PG_RETURN_DATUM(HeapTupleGetDatum(result_tuple));
}

/*
 * xpatch_inspect context - stored across SRF calls
 */
typedef struct XPatchInspectContext
{
    Relation        rel;
    XPatchConfig   *config;
    Datum           filter_group;      /* Group value to filter by */
    bool            filter_group_null; /* Is filter group value NULL? */
    BlockNumber     current_block;
    OffsetNumber    current_offset;
    int             current_delta_col; /* Which delta column in current row */
    int64           current_seq;       /* Sequence counter */
    /* Group comparison support for TEXT and other types */
    Oid             group_type;        /* Group column type OID */
    Oid             group_collation;   /* Group column collation */
    FmgrInfo        group_eq_finfo;    /* Equality function info */
    bool            group_eq_valid;    /* Is group_eq_finfo initialized? */
} XPatchInspectContext;

/*
 * xpatch_inspect(regclass, anyelement) - Inspect rows in a specific group
 *
 * Returns detailed information about each row's storage format,
 * including which rows are keyframes vs deltas, compression tags, etc.
 *
 * If group_value is NULL, inspects all rows.
 */
PG_FUNCTION_INFO_V1(xpatch_inspect);
Datum
xpatch_inspect(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx;
    XPatchInspectContext *ctx;
    MemoryContext oldcontext;

    if (SRF_IS_FIRSTCALL())
    {
        Oid relid = PG_GETARG_OID(0);
        TupleDesc tupdesc;

        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* Build result tuple descriptor */
        if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("function returning record called in context "
                            "that cannot accept type record")));

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        /* Allocate and initialize context */
        ctx = (XPatchInspectContext *) palloc0(sizeof(XPatchInspectContext));

        /* Open relation */
        ctx->rel = table_open(relid, AccessShareLock);
        ctx->config = xpatch_get_config(ctx->rel);

        /* Get group filter value (may be NULL to show all) */
        if (PG_ARGISNULL(1))
        {
            ctx->filter_group_null = true;
            ctx->filter_group = (Datum) 0;
        }
        else
        {
            ctx->filter_group_null = false;
            /* Copy the datum to our memory context */
            ctx->filter_group = PG_GETARG_DATUM(1);
        }

        /* Cache group column type, collation, and equality function */
        ctx->group_eq_valid = false;
        ctx->group_collation = DEFAULT_COLLATION_OID;
        if (ctx->config->group_by_attnum != InvalidAttrNumber)
        {
            TupleDesc rel_tupdesc = RelationGetDescr(ctx->rel);
            Form_pg_attribute attr = TupleDescAttr(rel_tupdesc,
                                                   ctx->config->group_by_attnum - 1);
            TypeCacheEntry *typcache;

            ctx->group_type = attr->atttypid;
            ctx->group_collation = attr->attcollation;

            /* Get equality function from type cache */
            typcache = lookup_type_cache(ctx->group_type, TYPECACHE_EQ_OPR_FINFO);
            if (OidIsValid(typcache->eq_opr_finfo.fn_oid))
            {
                fmgr_info_copy(&ctx->group_eq_finfo, &typcache->eq_opr_finfo,
                               funcctx->multi_call_memory_ctx);
                ctx->group_eq_valid = true;
            }
        }

        ctx->current_block = 0;
        ctx->current_offset = FirstOffsetNumber;
        ctx->current_delta_col = 0;
        ctx->current_seq = 0;

        funcctx->user_fctx = ctx;

        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();
    ctx = (XPatchInspectContext *) funcctx->user_fctx;

    /*
     * Scan through the table looking for rows to return.
     * Each row may produce multiple result rows (one per delta column).
     */
    while (ctx->current_block < RelationGetNumberOfBlocks(ctx->rel))
    {
        Buffer buffer;
        Page page;
        OffsetNumber maxoff;

        buffer = ReadBuffer(ctx->rel, ctx->current_block);
        LockBuffer(buffer, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buffer);
        maxoff = PageGetMaxOffsetNumber(page);

        while (ctx->current_offset <= maxoff)
        {
            ItemId itemId = PageGetItemId(page, ctx->current_offset);
            HeapTupleData tuple;
            TupleDesc rel_tupdesc = RelationGetDescr(ctx->rel);
            bool is_null;
            Datum group_val;
            bool match_group = true;

            if (!ItemIdIsNormal(itemId))
            {
                ctx->current_offset++;
                continue;
            }

            tuple.t_data = (HeapTupleHeader) PageGetItem(page, itemId);
            tuple.t_len = ItemIdGetLength(itemId);
            tuple.t_tableOid = RelationGetRelid(ctx->rel);

            /* Check if this row matches the group filter */
            if (!ctx->filter_group_null && ctx->config->group_by_attnum != InvalidAttrNumber)
            {
                group_val = heap_getattr(&tuple, ctx->config->group_by_attnum,
                                         rel_tupdesc, &is_null);

                if (is_null)
                    match_group = false;
                else if (ctx->group_eq_valid)
                    match_group = datums_equal(group_val, ctx->filter_group,
                                               &ctx->group_eq_finfo, ctx->group_collation);
                else
                    match_group = (group_val == ctx->filter_group);  /* Fallback for simple types */
            }

            if (match_group)
            {
                /* Get order_by value for the version column */
                Datum version_datum = heap_getattr(&tuple, ctx->config->order_by_attnum,
                                                   rel_tupdesc, &is_null);
                int64 version = is_null ? 0 : DatumGetInt64(version_datum);

                /* Process delta columns */
                while (ctx->current_delta_col < ctx->config->num_delta_columns)
                {
                    AttrNumber attnum = ctx->config->delta_attnums[ctx->current_delta_col];
                    Datum col_datum = heap_getattr(&tuple, attnum, rel_tupdesc, &is_null);

                    if (!is_null)
                    {
                        bytea *data = DatumGetByteaP(col_datum);
                        int data_len = VARSIZE(data) - VARHDRSZ;
                        size_t tag = 0;
                        bool is_keyframe = false;
                        const char *err;

                        if (data_len > 0)
                        {
                            /* Use proper tag extraction function */
                            err = xpatch_get_delta_tag((uint8 *) VARDATA(data),
                                                       data_len, &tag);
                            if (err == NULL)
                                is_keyframe = (tag == XPATCH_KEYFRAME_TAG);
                        }

                        /* Build result tuple */
                        {
                            Datum values[6];
                            bool nulls[6];
                            HeapTuple result_tuple;

                            memset(nulls, 0, sizeof(nulls));

                            values[0] = Int64GetDatum(version);
                            values[1] = Int32GetDatum(ctx->current_seq);
                            values[2] = BoolGetDatum(is_keyframe);
                            values[3] = Int32GetDatum((int32) tag);
                            values[4] = Int32GetDatum(data_len);
                            values[5] = CStringGetTextDatum(ctx->config->delta_columns[ctx->current_delta_col]);

                            result_tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);

                            ctx->current_delta_col++;

                            LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
                            ReleaseBuffer(buffer);

                            SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(result_tuple));
                        }
                    }

                    ctx->current_delta_col++;
                }

                ctx->current_seq++;
            }

            /* Move to next tuple */
            ctx->current_offset++;
            ctx->current_delta_col = 0;
        }

        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
        ReleaseBuffer(buffer);

        /* Move to next block */
        ctx->current_block++;
        ctx->current_offset = FirstOffsetNumber;
    }

    /* Done - close relation */
    table_close(ctx->rel, AccessShareLock);

    SRF_RETURN_DONE(funcctx);
}

/*
 * xpatch_cache_stats() - Get global cache statistics
 */
PG_FUNCTION_INFO_V1(xpatch_cache_stats_fn);
Datum
xpatch_cache_stats_fn(PG_FUNCTION_ARGS)
{
    TupleDesc tupdesc;
    Datum values[6];
    bool nulls[6];
    HeapTuple result_tuple;
    XPatchCacheStats stats;

    /* Build result tuple descriptor */
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("function returning record called in context "
                        "that cannot accept type record")));

    /* Get cache statistics */
    xpatch_cache_get_stats(&stats);

    /* Build result tuple */
    memset(nulls, 0, sizeof(nulls));

    values[0] = Int64GetDatum(stats.size_bytes);
    values[1] = Int64GetDatum(stats.max_bytes);
    values[2] = Int64GetDatum(stats.entries_count);
    values[3] = Int64GetDatum(stats.hit_count);
    values[4] = Int64GetDatum(stats.miss_count);
    values[5] = Int64GetDatum(stats.eviction_count);

    result_tuple = heap_form_tuple(tupdesc, values, nulls);

    PG_RETURN_DATUM(HeapTupleGetDatum(result_tuple));
}

/*
 * xpatch_insert_cache_stats() - Get insert cache statistics
 */
PG_FUNCTION_INFO_V1(xpatch_insert_cache_stats_fn);
Datum
xpatch_insert_cache_stats_fn(PG_FUNCTION_ARGS)
{
    TupleDesc tupdesc;
    Datum values[6];
    bool nulls[6];
    HeapTuple result_tuple;
    InsertCacheStats stats;

    /* Build result tuple descriptor */
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("function returning record called in context "
                        "that cannot accept type record")));

    /* Get insert cache statistics */
    xpatch_insert_cache_get_stats(&stats);

    /* Build result tuple */
    memset(nulls, 0, sizeof(nulls));

    values[0] = Int64GetDatum(stats.slots_in_use);
    values[1] = Int64GetDatum(stats.total_slots);
    values[2] = Int64GetDatum(stats.hits);
    values[3] = Int64GetDatum(stats.misses);
    values[4] = Int64GetDatum(stats.evictions);
    values[5] = Int64GetDatum(stats.eviction_misses);

    result_tuple = heap_form_tuple(tupdesc, values, nulls);

    PG_RETURN_DATUM(HeapTupleGetDatum(result_tuple));
}

/*
 * xpatch_invalidate_config(regclass) - Invalidate cached config for a table
 * Called by xpatch.configure() to ensure config changes take effect.
 */
PG_FUNCTION_INFO_V1(xpatch_invalidate_config_fn);
Datum
xpatch_invalidate_config_fn(PG_FUNCTION_ARGS)
{
    Oid relid = PG_GETARG_OID(0);
    
    xpatch_invalidate_config(relid);
    
    PG_RETURN_VOID();
}

/*
 * xpatch_physical context - stored across SRF calls
 */
typedef struct XPatchPhysicalContext
{
    Relation        rel;
    XPatchConfig   *config;
    TupleDesc       rel_tupdesc;
    Oid             group_type;        /* Type OID of group_by column */
    Oid             group_collation;   /* Collation for group_by column */
    FmgrInfo        group_eq_finfo;    /* Equality function for group type */
    bool            group_eq_valid;    /* True if group_eq_finfo is initialized */
    Datum           filter_group;      /* Group value to filter by */
    bool            filter_group_null; /* True if filtering all groups */
    int32           from_seq;          /* Filter: return rows with seq > from_seq */
    bool            from_seq_null;     /* True if no seq filtering */
    BlockNumber     current_block;
    OffsetNumber    current_offset;
    int             current_delta_col;
    int64           current_seq;       /* 0-based sequence counter */
} XPatchPhysicalContext;

/*
 * Compare two Datums for equality using the type's equality operator.
 * Uses default collation for collation-sensitive types like TEXT.
 */
static bool
datums_equal(Datum d1, Datum d2, FmgrInfo *eq_finfo, Oid collation)
{
    return DatumGetBool(FunctionCall2Coll(eq_finfo, collation, d1, d2));
}

/*
 * Convert a Datum to TEXT representation based on its type.
 * Returns a palloc'd text datum.
 */
static Datum
datum_to_text(Datum value, Oid typid)
{
    Oid         typoutput;
    bool        typIsVarlena;
    char       *str;

    getTypeOutputInfo(typid, &typoutput, &typIsVarlena);
    str = OidOutputFunctionCall(typoutput, value);
    return CStringGetTextDatum(str);
}

/*
 * Convert a Datum to int64 for numeric types.
 * Handles INT2, INT4, INT8.
 */
static int64
datum_to_int64(Datum value, Oid typid)
{
    switch (typid)
    {
        case INT2OID:
            return (int64) DatumGetInt16(value);
        case INT4OID:
            return (int64) DatumGetInt32(value);
        case INT8OID:
            return DatumGetInt64(value);
        default:
            /* For other types, try int64 and hope for the best */
            return DatumGetInt64(value);
    }
}

/*
 * xpatch_physical(regclass, anyelement, int) - Access raw physical delta storage
 *
 * Returns raw delta bytes and metadata for each row/column in a table.
 * This function directly reads physical storage pages to access the
 * compressed delta data before TAM reconstruction.
 *
 * Parameters:
 *   tbl          - Table to inspect (must use xpatch access method)
 *   group_filter - Filter by specific group value, or NULL for all groups
 *   from_seq     - Only return rows with seq > from_seq, or NULL for all
 *
 * Returns a set of rows with:
 *   group_value  - Group column value as TEXT (NULL if no grouping)
 *   version      - Order-by column value as BIGINT
 *   seq          - 1-based sequence number within group
 *   is_keyframe  - True if this row stores a keyframe (tag=0)
 *   tag          - Delta tag (0=keyframe, 1+=reference to N rows back)
 *   delta_column - Name of the delta column
 *   delta_bytes  - Raw compressed delta data
 *   delta_size   - Size of delta_bytes in bytes
 */
PG_FUNCTION_INFO_V1(xpatch_physical);
Datum
xpatch_physical(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx;
    XPatchPhysicalContext *ctx;
    MemoryContext oldcontext;

    if (SRF_IS_FIRSTCALL())
    {
        Oid         relid = PG_GETARG_OID(0);
        TupleDesc   tupdesc;
        Oid         amoid;

        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* Build result tuple descriptor */
        if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("function returning record called in context "
                            "that cannot accept type record")));

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        ctx = (XPatchPhysicalContext *) palloc0(sizeof(XPatchPhysicalContext));

        /* Open relation and verify it uses xpatch access method */
        ctx->rel = table_open(relid, AccessShareLock);

        amoid = ctx->rel->rd_rel->relam;
        if (amoid == InvalidOid || strcmp(get_am_name(amoid), "xpatch") != 0)
            ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                     errmsg("table \"%s\" does not use the xpatch access method",
                            RelationGetRelationName(ctx->rel))));

        ctx->config = xpatch_get_config(ctx->rel);
        ctx->rel_tupdesc = RelationGetDescr(ctx->rel);

        /* Cache group column type, collation, and equality function */
        ctx->group_eq_valid = false;
        ctx->group_collation = DEFAULT_COLLATION_OID;
        if (ctx->config->group_by_attnum != InvalidAttrNumber)
        {
            Form_pg_attribute attr = TupleDescAttr(ctx->rel_tupdesc,
                                                   ctx->config->group_by_attnum - 1);
            TypeCacheEntry *typcache;

            ctx->group_type = attr->atttypid;
            ctx->group_collation = attr->attcollation;

            /* Get equality function from type cache */
            typcache = lookup_type_cache(ctx->group_type, TYPECACHE_EQ_OPR_FINFO);
            if (OidIsValid(typcache->eq_opr_finfo.fn_oid))
            {
                fmgr_info_copy(&ctx->group_eq_finfo, &typcache->eq_opr_finfo,
                               funcctx->multi_call_memory_ctx);
                ctx->group_eq_valid = true;
            }
        }
        else
        {
            ctx->group_type = InvalidOid;
        }

        /* Get group filter value */
        if (PG_ARGISNULL(1))
        {
            ctx->filter_group_null = true;
            ctx->filter_group = (Datum) 0;
        }
        else
        {
            ctx->filter_group_null = false;
            ctx->filter_group = PG_GETARG_DATUM(1);
        }

        /* Get from_seq filter */
        if (PG_ARGISNULL(2))
        {
            ctx->from_seq_null = true;
            ctx->from_seq = 0;
        }
        else
        {
            ctx->from_seq_null = false;
            ctx->from_seq = PG_GETARG_INT32(2);
        }

        ctx->current_block = 0;
        ctx->current_offset = FirstOffsetNumber;
        ctx->current_delta_col = 0;
        ctx->current_seq = 0;

        funcctx->user_fctx = ctx;
        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();
    ctx = (XPatchPhysicalContext *) funcctx->user_fctx;

    /* Scan pages looking for matching rows */
    while (ctx->current_block < RelationGetNumberOfBlocks(ctx->rel))
    {
        Buffer          buffer;
        Page            page;
        OffsetNumber    maxoff;

        buffer = ReadBuffer(ctx->rel, ctx->current_block);
        LockBuffer(buffer, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buffer);
        maxoff = PageGetMaxOffsetNumber(page);

        while (ctx->current_offset <= maxoff)
        {
            ItemId          itemId;
            HeapTupleData   tuple;
            bool            is_null;
            Datum           group_val = (Datum) 0;
            bool            match_group = true;

            itemId = PageGetItemId(page, ctx->current_offset);
            if (!ItemIdIsNormal(itemId))
            {
                ctx->current_offset++;
                continue;
            }

            tuple.t_data = (HeapTupleHeader) PageGetItem(page, itemId);
            tuple.t_len = ItemIdGetLength(itemId);
            tuple.t_tableOid = RelationGetRelid(ctx->rel);

            /* Apply group filter if specified */
            if (!ctx->filter_group_null && ctx->config->group_by_attnum != InvalidAttrNumber)
            {
                group_val = heap_getattr(&tuple, ctx->config->group_by_attnum,
                                         ctx->rel_tupdesc, &is_null);
                if (is_null)
                    match_group = false;
                else if (ctx->group_eq_valid)
                    match_group = datums_equal(group_val, ctx->filter_group,
                                               &ctx->group_eq_finfo, ctx->group_collation);
                else
                    match_group = (group_val == ctx->filter_group);  /* Fallback for simple types */
            }

            if (match_group)
            {
                /* All declarations at top of block for C90 compatibility */
                Datum       group_text_datum = (Datum) 0;
                bool        group_is_null = true;
                Datum       version_datum;
                Oid         version_type;
                int64       version;
                int32       output_seq;

                /* Convert group value to text for output */
                if (ctx->config->group_by_attnum != InvalidAttrNumber)
                {
                    group_val = heap_getattr(&tuple, ctx->config->group_by_attnum,
                                             ctx->rel_tupdesc, &is_null);
                    if (!is_null)
                    {
                        group_text_datum = datum_to_text(group_val, ctx->group_type);
                        group_is_null = false;
                    }
                }

                /* Get version (order_by) value */
                {
                    Form_pg_attribute attr = TupleDescAttr(ctx->rel_tupdesc,
                                                           ctx->config->order_by_attnum - 1);
                    version_type = attr->atttypid;
                }
                version_datum = heap_getattr(&tuple, ctx->config->order_by_attnum,
                                             ctx->rel_tupdesc, &is_null);
                version = is_null ? 0 : datum_to_int64(version_datum, version_type);

                /* Apply seq filter (1-based comparison) */
                output_seq = ctx->current_seq + 1;
                if (!ctx->from_seq_null && output_seq <= ctx->from_seq)
                {
                    ctx->current_offset++;
                    ctx->current_delta_col = 0;
                    ctx->current_seq++;
                    continue;
                }

                /* Iterate through delta columns */
                while (ctx->current_delta_col < ctx->config->num_delta_columns)
                {
                    AttrNumber  attnum;
                    Datum       col_datum;

                    attnum = ctx->config->delta_attnums[ctx->current_delta_col];
                    col_datum = heap_getattr(&tuple, attnum, ctx->rel_tupdesc, &is_null);

                    if (!is_null)
                    {
                        bytea      *data;
                        int         data_len;
                        size_t      tag = 0;
                        bool        is_keyframe = false;
                        Datum       values[8];
                        bool        nulls[8];
                        HeapTuple   result_tuple;

                        data = DatumGetByteaP(col_datum);
                        data_len = VARSIZE(data) - VARHDRSZ;

                        /* Extract tag from delta header */
                        if (data_len > 0)
                        {
                            const char *err;
                            err = xpatch_get_delta_tag((uint8 *) VARDATA(data), data_len, &tag);
                            if (err == NULL)
                                is_keyframe = (tag == XPATCH_KEYFRAME_TAG);
                        }

                        /* Build result tuple */
                        memset(nulls, 0, sizeof(nulls));

                        if (group_is_null)
                        {
                            nulls[0] = true;
                            values[0] = (Datum) 0;
                        }
                        else
                        {
                            values[0] = group_text_datum;
                        }

                        values[1] = Int64GetDatum(version);
                        values[2] = Int32GetDatum(output_seq);
                        values[3] = BoolGetDatum(is_keyframe);
                        values[4] = Int32GetDatum((int32) tag);
                        values[5] = CStringGetTextDatum(ctx->config->delta_columns[ctx->current_delta_col]);
                        values[6] = PointerGetDatum(data);
                        values[7] = Int32GetDatum(data_len);

                        result_tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
                        ctx->current_delta_col++;

                        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
                        ReleaseBuffer(buffer);

                        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(result_tuple));
                    }

                    ctx->current_delta_col++;
                }

                ctx->current_seq++;
            }

            ctx->current_offset++;
            ctx->current_delta_col = 0;
        }

        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
        ReleaseBuffer(buffer);

        ctx->current_block++;
        ctx->current_offset = FirstOffsetNumber;
    }

    table_close(ctx->rel, AccessShareLock);
    SRF_RETURN_DONE(funcctx);
}
