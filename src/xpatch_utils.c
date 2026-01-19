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
#include "xpatch_storage.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

/* Structure for tracking sequence per group in xpatch_stats */
typedef struct {
    Datum group_val;
    int32 current_seq;
} GroupSeqEntry;

/*
 * xpatch_stats(regclass) - Get compression statistics for a table
 *
 * Returns statistics about compression efficiency and cache usage for
 * an xpatch table. This function scans the raw storage to collect
 * information about keyframes, deltas, and compression ratios.
 */
PG_FUNCTION_INFO_V1(xpatch_stats);
Datum
xpatch_stats(PG_FUNCTION_ARGS)
{
    Oid relid = PG_GETARG_OID(0);
    Relation rel;
    TupleDesc tupdesc;
    TupleDesc rel_tupdesc;
    Datum values[10];
    bool nulls[10];
    HeapTuple result_tuple;
    XPatchConfig *config;
    BlockNumber nblocks;
    BlockNumber blkno;
    int64 total_rows = 0;
    int64 total_groups = 0;
    int64 keyframe_count = 0;
    int64 delta_count = 0;
    int64 raw_size = 0;
    int64 compressed_size = 0;
    HTAB *groups_seen = NULL;
    HASHCTL hash_ctl;
    XPatchCacheStats cache_stats;
    HTAB *group_seqs = NULL;
    HASHCTL seq_hash_ctl;

    /* Build result tuple descriptor */
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("function returning record called in context "
                        "that cannot accept type record")));

    /* Open the relation */
    rel = table_open(relid, AccessShareLock);
    rel_tupdesc = RelationGetDescr(rel);

    /* Get configuration */
    config = xpatch_get_config(rel);

    /* Create hash table to track distinct groups */
    if (config->group_by_attnum != InvalidAttrNumber)
    {
        memset(&hash_ctl, 0, sizeof(hash_ctl));
        hash_ctl.keysize = sizeof(Datum);
        hash_ctl.entrysize = sizeof(Datum);
        hash_ctl.hcxt = CurrentMemoryContext;
        groups_seen = hash_create("xpatch_stats groups",
                                  64, &hash_ctl,
                                  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
    }

    /*
     * Scan the physical storage directly (not through the TAM)
     * to analyze raw tuple sizes and compression.
     *
     * We need to track sequence numbers per group to be able to decode
     * delta columns and calculate actual raw sizes.
     */
    nblocks = RelationGetNumberOfBlocks(rel);

    /* Create hash table to track current sequence per group */
    memset(&seq_hash_ctl, 0, sizeof(seq_hash_ctl));
    seq_hash_ctl.keysize = sizeof(Datum);
    seq_hash_ctl.entrysize = sizeof(GroupSeqEntry);
    seq_hash_ctl.hcxt = CurrentMemoryContext;
    group_seqs = hash_create("xpatch_stats group_seqs",
                             64, &seq_hash_ctl,
                             HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

    for (blkno = 0; blkno < nblocks; blkno++)
    {
        Buffer buffer;
        Page page;
        OffsetNumber maxoff;
        OffsetNumber off;

        buffer = ReadBuffer(rel, blkno);
        LockBuffer(buffer, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buffer);
        maxoff = PageGetMaxOffsetNumber(page);

        for (off = FirstOffsetNumber; off <= maxoff; off++)
        {
            ItemId itemId = PageGetItemId(page, off);
            HeapTupleData tuple;
            int i;
            bool is_keyframe_row = false;
            Datum group_val = (Datum) 0;
            bool group_is_null = true;
            int32 row_seq;
            GroupSeqEntry *seq_entry;
            bool found;

            if (!ItemIdIsNormal(itemId))
                continue;

            tuple.t_data = (HeapTupleHeader) PageGetItem(page, itemId);
            tuple.t_len = ItemIdGetLength(itemId);
            tuple.t_tableOid = RelationGetRelid(rel);
            ItemPointerSet(&tuple.t_self, blkno, off);

            total_rows++;

            /* Get group value */
            if (config->group_by_attnum != InvalidAttrNumber)
            {
                group_val = heap_getattr(&tuple, config->group_by_attnum,
                                         rel_tupdesc, &group_is_null);
            }

            /* Track distinct groups */
            if (groups_seen && !group_is_null)
            {
                hash_search(groups_seen, &group_val, HASH_ENTER, &found);
                if (!found)
                    total_groups++;
            }

            /* Get/update sequence number for this group */
            seq_entry = (GroupSeqEntry *) hash_search(group_seqs, &group_val, 
                                                       HASH_ENTER, &found);
            if (!found)
            {
                seq_entry->group_val = group_val;
                seq_entry->current_seq = 0;
            }
            seq_entry->current_seq++;
            row_seq = seq_entry->current_seq;

            /*
             * Process each delta column:
             * - Check if keyframe (tag == 0)
             * - Track compressed size
             * - Decode to get raw size
             */
            for (i = 0; i < config->num_delta_columns; i++)
            {
                AttrNumber attnum = config->delta_attnums[i];
                bool is_null;
                Datum col_datum = heap_getattr(&tuple, attnum, rel_tupdesc, &is_null);

                if (!is_null)
                {
                    bytea *data = DatumGetByteaP(col_datum);
                    int data_len = VARSIZE(data) - VARHDRSZ;

                    /* Track compressed size for this delta column */
                    compressed_size += data_len;

                    if (data_len > 0)
                    {
                        size_t tag;
                        const char *err;
                        bytea *decoded;
                        
                        /* Extract tag to check if keyframe */
                        err = xpatch_get_delta_tag((uint8 *) VARDATA(data), 
                                                   data_len, &tag);
                        if (err == NULL && tag == XPATCH_KEYFRAME_TAG)
                            is_keyframe_row = true;

                        /* 
                         * Decode to get actual raw size.
                         * Release buffer lock temporarily since reconstruct may need to read.
                         */
                        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
                        
                        decoded = xpatch_reconstruct_column(rel, config, group_val, 
                                                            row_seq, i);
                        if (decoded != NULL)
                        {
                            raw_size += VARSIZE_ANY_EXHDR(decoded);
                            pfree(decoded);
                        }
                        
                        LockBuffer(buffer, BUFFER_LOCK_SHARE);
                        
                        /* Re-get page pointer after re-locking */
                        page = BufferGetPage(buffer);
                    }
                }
            }

            if (is_keyframe_row)
                keyframe_count++;
            else
                delta_count++;
        }

        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
        ReleaseBuffer(buffer);
    }

    /* If no group_by column, all rows are in one group */
    if (config->group_by_attnum == InvalidAttrNumber)
        total_groups = total_rows > 0 ? 1 : 0;

    /* Clean up hash tables */
    if (groups_seen)
        hash_destroy(groups_seen);
    if (group_seqs)
        hash_destroy(group_seqs);

    /* Get cache statistics */
    xpatch_cache_get_stats(&cache_stats);

    /* Build result tuple */
    memset(nulls, 0, sizeof(nulls));

    values[0] = Int64GetDatum(total_rows);
    values[1] = Int64GetDatum(total_groups);
    values[2] = Int64GetDatum(keyframe_count);
    values[3] = Int64GetDatum(delta_count);
    values[4] = Int64GetDatum(raw_size);
    values[5] = Int64GetDatum(compressed_size);
    values[6] = Float8GetDatum(compressed_size > 0 ?
                               (double) raw_size / compressed_size : 0.0);
    values[7] = Int64GetDatum(cache_stats.hit_count);
    values[8] = Int64GetDatum(cache_stats.miss_count);
    values[9] = Float8GetDatum(keyframe_count > 0 ?
                               (double) delta_count / keyframe_count : 0.0);

    result_tuple = heap_form_tuple(tupdesc, values, nulls);

    table_close(rel, AccessShareLock);

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

                /* Simple comparison - works for integer types */
                if (is_null || group_val != ctx->filter_group)
                    match_group = false;
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
