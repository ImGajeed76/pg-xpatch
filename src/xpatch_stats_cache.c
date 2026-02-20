/*
 * pg-xpatch - PostgreSQL Table Access Method for delta-compressed data
 * Copyright (c) 2025 Oliver Seifert
 *
 * xpatch_stats_cache.c - Stats cache implementation
 *
 * Simple design:
 * - INSERT: UPSERT into group_stats (increment counts)
 * - DELETE: Refresh that group's stats by rescanning (caller holds advisory lock)
 * - stats(): Aggregate from group_stats (always up-to-date)
 */

#include "xpatch_stats_cache.h"
#include "xpatch_config.h"
#include "xpatch_compress.h"
#include "xpatch_storage.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "executor/spi.h"
#include "storage/bufmgr.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "funcapi.h"

/* Size of XPatchGroupHash when serialized to bytea (h1 + h2 = 16 bytes) */
#define XPATCH_GROUP_HASH_SIZE (sizeof(uint64) * 2)

/* ============================================================================
 * Pending stats accumulator
 *
 * Instead of doing SPI_connect + UPSERT + SPI_finish per row, we accumulate
 * stats in a per-backend hash table and flush once at transaction commit.
 * This reduces O(rows) SPI calls to O(groups) for COPY and multi-row
 * transactions.
 * ============================================================================ */

/* Hash key: (relid, group_hash) */
typedef struct
{
    Oid             relid;
    XPatchGroupHash group_hash;
} PendingStatsKey;

/* Hash entry: accumulated counters for one group */
typedef struct
{
    PendingStatsKey key;
    int64           row_count;
    int64           keyframe_count;
    int64           max_seq;
    int64           raw_size;
    int64           compressed_size;
    double          sum_avg_delta_tags;
} PendingStatsEntry;

/* Per-backend state */
static HTAB *pending_stats = NULL;
static bool  xact_callback_registered = false;

/* SQL statements */
static const char *DELETE_GROUP_STATS_SQL =
    "DELETE FROM xpatch.group_stats WHERE relid = $1 AND group_hash = $2";

static const char *DELETE_TABLE_STATS_SQL =
    "DELETE FROM xpatch.group_stats WHERE relid = $1";

static const char *GET_MAX_SEQ_SQL =
    "SELECT max_seq FROM xpatch.group_stats WHERE relid = $1 AND group_hash = $2";

static const char *CHECK_EXISTS_SQL =
    "SELECT EXISTS(SELECT 1 FROM xpatch.group_stats WHERE relid = $1)";

static const char *GET_TABLE_STATS_SQL =
    "SELECT "
    "  COALESCE(SUM(row_count), 0)::BIGINT, "
    "  COUNT(*)::BIGINT, "
    "  COALESCE(SUM(keyframe_count), 0)::BIGINT, "
    "  COALESCE(SUM(raw_size_bytes), 0)::BIGINT, "
    "  COALESCE(SUM(compressed_size_bytes), 0)::BIGINT, "
    "  COALESCE(SUM(sum_avg_delta_tags), 0)::FLOAT8 "
    "FROM xpatch.group_stats WHERE relid = $1 AND row_count > 0";

/*
 * Helper: Convert XPatchGroupHash to bytea
 */
static bytea *
group_hash_to_bytea(XPatchGroupHash group_hash)
{
    bytea *hash_bytea = (bytea *) palloc(VARHDRSZ + XPATCH_GROUP_HASH_SIZE);
    SET_VARSIZE(hash_bytea, VARHDRSZ + XPATCH_GROUP_HASH_SIZE);
    memcpy(VARDATA(hash_bytea), &group_hash.h1, sizeof(uint64));
    memcpy(VARDATA(hash_bytea) + sizeof(uint64), &group_hash.h2, sizeof(uint64));
    return hash_bytea;
}

/*
 * Flush all pending stats to xpatch.group_stats via a single SPI session.
 * Called at transaction commit (or explicitly).
 */
static void
xpatch_stats_cache_flush_pending(void)
{
    HASH_SEQ_STATUS status;
    PendingStatsEntry *entry;
    int ret;

    if (pending_stats == NULL)
        return;

    ret = SPI_connect();
    if (ret != SPI_OK_CONNECT)
    {
        elog(WARNING, "xpatch_stats_cache: flush SPI_connect failed: %d", ret);
        goto cleanup;
    }

    /*
     * PRE_COMMIT fires after the snapshot is released, so SPI cannot execute
     * SQL without one.  Push a transaction snapshot for the duration of the
     * flush.
     */
    PushActiveSnapshot(GetTransactionSnapshot());

    hash_seq_init(&status, pending_stats);
    while ((entry = (PendingStatsEntry *) hash_seq_search(&status)) != NULL)
    {
        Oid argtypes[8] = {OIDOID, BYTEAOID, INT8OID, INT8OID,
                            INT8OID, INT8OID, INT8OID, FLOAT8OID};
        Datum values[8];
        bytea *hash_bytea;

        hash_bytea = group_hash_to_bytea(entry->key.group_hash);

        values[0] = ObjectIdGetDatum(entry->key.relid);
        values[1] = PointerGetDatum(hash_bytea);
        values[2] = Int64GetDatum(entry->row_count);
        values[3] = Int64GetDatum(entry->keyframe_count);
        values[4] = Int64GetDatum(entry->max_seq);
        values[5] = Int64GetDatum(entry->raw_size);
        values[6] = Int64GetDatum(entry->compressed_size);
        values[7] = Float8GetDatum(entry->sum_avg_delta_tags);

        ret = SPI_execute_with_args(
            "INSERT INTO xpatch.group_stats ("
            "  relid, group_hash, row_count, keyframe_count, max_seq, "
            "  raw_size_bytes, compressed_size_bytes, sum_avg_delta_tags"
            ") VALUES ($1, $2, $3, $4, $5, $6, $7, $8) "
            "ON CONFLICT (relid, group_hash) DO UPDATE SET "
            "  row_count = xpatch.group_stats.row_count + EXCLUDED.row_count, "
            "  keyframe_count = xpatch.group_stats.keyframe_count + EXCLUDED.keyframe_count, "
            "  max_seq = GREATEST(xpatch.group_stats.max_seq, EXCLUDED.max_seq), "
            "  raw_size_bytes = xpatch.group_stats.raw_size_bytes + EXCLUDED.raw_size_bytes, "
            "  compressed_size_bytes = xpatch.group_stats.compressed_size_bytes + EXCLUDED.compressed_size_bytes, "
            "  sum_avg_delta_tags = xpatch.group_stats.sum_avg_delta_tags + EXCLUDED.sum_avg_delta_tags",
            8, argtypes, values, NULL, false, 0);

        if (ret != SPI_OK_INSERT)
            elog(WARNING, "xpatch_stats_cache: batch upsert failed: %d", ret);
    }

    PopActiveSnapshot();
    SPI_finish();

cleanup:
    hash_destroy(pending_stats);
    pending_stats = NULL;
}

/*
 * Transaction callback: flush pending stats on commit, discard on abort.
 */
static void
xpatch_stats_xact_callback(XactEvent event, void *arg)
{
    switch (event)
    {
        case XACT_EVENT_PRE_COMMIT:
            xpatch_stats_cache_flush_pending();
            break;

        case XACT_EVENT_ABORT:
            /* Discard accumulated stats — the rows were rolled back */
            if (pending_stats != NULL)
            {
                hash_destroy(pending_stats);
                pending_stats = NULL;
            }
            break;

        default:
            break;
    }
}

/*
 * Ensure the xact callback is registered (idempotent).
 */
static void
ensure_xact_callback(void)
{
    if (!xact_callback_registered)
    {
        RegisterXactCallback(xpatch_stats_xact_callback, NULL);
        xact_callback_registered = true;
    }
}

/*
 * Update group stats after INSERT.
 *
 * Accumulates stats in a per-backend hash table (no SPI).
 * The accumulated stats are flushed to xpatch.group_stats in a single
 * SPI session at transaction commit via RegisterXactCallback.
 *
 * This reduces O(rows) SPI round-trips to O(groups) for COPY and
 * multi-row transactions.
 */
void
xpatch_stats_cache_update_group(
    Oid relid,
    XPatchGroupHash group_hash,
    bool is_keyframe,
    int64 max_seq,
    int64 raw_size,
    int64 compressed_size,
    double avg_delta_tag)
{
    PendingStatsKey key;
    PendingStatsEntry *entry;
    bool found;

    ensure_xact_callback();

    /* Create hash table on first use in this transaction */
    if (pending_stats == NULL)
    {
        HASHCTL hash_ctl;

        memset(&hash_ctl, 0, sizeof(hash_ctl));
        hash_ctl.keysize = sizeof(PendingStatsKey);
        hash_ctl.entrysize = sizeof(PendingStatsEntry);
        hash_ctl.hcxt = TopTransactionContext;
        pending_stats = hash_create("pending_stats", 64, &hash_ctl,
                                     HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
    }

    /* Build key */
    memset(&key, 0, sizeof(key));
    key.relid = relid;
    key.group_hash = group_hash;

    /* Find or create entry */
    entry = (PendingStatsEntry *) hash_search(pending_stats, &key,
                                               HASH_ENTER, &found);
    if (!found)
    {
        /* New group in this transaction */
        entry->row_count = 0;
        entry->keyframe_count = 0;
        entry->max_seq = 0;
        entry->raw_size = 0;
        entry->compressed_size = 0;
        entry->sum_avg_delta_tags = 0.0;
    }

    /* Accumulate */
    entry->row_count++;
    if (is_keyframe)
        entry->keyframe_count++;
    if (max_seq > entry->max_seq)
        entry->max_seq = max_seq;
    entry->raw_size += raw_size;
    entry->compressed_size += compressed_size;
    entry->sum_avg_delta_tags += avg_delta_tag;
}

/*
 * Delete stats for a specific group.
 * Next stats() call will recompute this group on demand.
 */
void
xpatch_stats_cache_delete_group(Oid relid, XPatchGroupHash group_hash)
{
    int ret;
    Oid argtypes[2];
    Datum values[2];
    bytea *hash_bytea;

    if ((ret = SPI_connect()) != SPI_OK_CONNECT)
    {
        elog(WARNING, "xpatch_stats_cache: SPI_connect failed: %d", ret);
        return;
    }

    hash_bytea = group_hash_to_bytea(group_hash);

    argtypes[0] = OIDOID;
    argtypes[1] = BYTEAOID;
    values[0] = ObjectIdGetDatum(relid);
    values[1] = PointerGetDatum(hash_bytea);

    SPI_execute_with_args(DELETE_GROUP_STATS_SQL, 2, argtypes, values, NULL, false, 0);

    SPI_finish();
}

/*
 * Delete all stats for a table (called on TRUNCATE).
 */
void
xpatch_stats_cache_delete_table(Oid relid)
{
    int ret;
    Oid argtypes[1];
    Datum values[1];

    if ((ret = SPI_connect()) != SPI_OK_CONNECT)
    {
        elog(WARNING, "xpatch_stats_cache: SPI_connect failed: %d", ret);
        return;
    }

    argtypes[0] = OIDOID;
    values[0] = ObjectIdGetDatum(relid);

    SPI_execute_with_args(DELETE_TABLE_STATS_SQL, 1, argtypes, values, NULL, false, 0);

    SPI_finish();
}

/*
 * Get max_seq for a group from cache.
 * Returns -1 if not found.
 */
int64
xpatch_stats_cache_get_max_seq(Oid relid, XPatchGroupHash group_hash)
{
    int ret;
    int64 max_seq = -1;
    Oid argtypes[2];
    Datum values[2];
    bytea *hash_bytea;

    if ((ret = SPI_connect()) != SPI_OK_CONNECT)
        return -1;

    hash_bytea = group_hash_to_bytea(group_hash);

    argtypes[0] = OIDOID;
    argtypes[1] = BYTEAOID;
    values[0] = ObjectIdGetDatum(relid);
    values[1] = PointerGetDatum(hash_bytea);

    ret = SPI_execute_with_args(GET_MAX_SEQ_SQL, 2, argtypes, values, NULL, true, 1);
    if (ret == SPI_OK_SELECT && SPI_processed > 0)
    {
        bool isnull;
        Datum val = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
        if (!isnull)
            max_seq = DatumGetInt64(val);
    }

    SPI_finish();
    return max_seq;
}

/*
 * Check if stats exist for a table.
 */
bool
xpatch_stats_cache_exists(Oid relid)
{
    int ret;
    bool exists = false;
    Oid argtypes[1];
    Datum values[1];

    if ((ret = SPI_connect()) != SPI_OK_CONNECT)
        return false;

    argtypes[0] = OIDOID;
    values[0] = ObjectIdGetDatum(relid);

    ret = SPI_execute_with_args(CHECK_EXISTS_SQL, 1, argtypes, values, NULL, true, 1);
    if (ret == SPI_OK_SELECT && SPI_processed > 0)
    {
        bool isnull;
        Datum val = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
        if (!isnull)
            exists = DatumGetBool(val);
    }

    SPI_finish();
    return exists;
}

/*
 * Get aggregated stats for a table from cache.
 * Returns true if stats exist, false if cache miss.
 */
bool
xpatch_stats_cache_get_table_stats(
    Oid relid,
    int64 *total_rows,
    int64 *total_groups,
    int64 *keyframe_count,
    int64 *raw_size_bytes,
    int64 *compressed_size_bytes,
    double *sum_avg_delta_tags)
{
    int ret;
    bool found = false;
    Oid argtypes[1];
    Datum values[1];

    if ((ret = SPI_connect()) != SPI_OK_CONNECT)
        return false;

    argtypes[0] = OIDOID;
    values[0] = ObjectIdGetDatum(relid);

    ret = SPI_execute_with_args(GET_TABLE_STATS_SQL, 1, argtypes, values, NULL, true, 1);
    if (ret == SPI_OK_SELECT && SPI_processed > 0)
    {
        bool isnull;
        TupleDesc tupdesc = SPI_tuptable->tupdesc;
        HeapTuple tuple = SPI_tuptable->vals[0];
        Datum val;

        val = SPI_getbinval(tuple, tupdesc, 1, &isnull);
        *total_rows = isnull ? 0 : DatumGetInt64(val);

        val = SPI_getbinval(tuple, tupdesc, 2, &isnull);
        *total_groups = isnull ? 0 : DatumGetInt64(val);

        val = SPI_getbinval(tuple, tupdesc, 3, &isnull);
        *keyframe_count = isnull ? 0 : DatumGetInt64(val);

        val = SPI_getbinval(tuple, tupdesc, 4, &isnull);
        *raw_size_bytes = isnull ? 0 : DatumGetInt64(val);

        val = SPI_getbinval(tuple, tupdesc, 5, &isnull);
        *compressed_size_bytes = isnull ? 0 : DatumGetInt64(val);

        val = SPI_getbinval(tuple, tupdesc, 6, &isnull);
        *sum_avg_delta_tags = isnull ? 0.0 : DatumGetFloat8(val);

        found = (*total_groups > 0);
    }

    SPI_finish();
    return found;
}

/* ============================================================================
 * SQL-callable functions
 * ============================================================================ */

PG_FUNCTION_INFO_V1(xpatch_update_group_stats);
Datum
xpatch_update_group_stats(PG_FUNCTION_ARGS)
{
    Oid relid = PG_GETARG_OID(0);
    bytea *hash_bytea = PG_GETARG_BYTEA_PP(1);
    bool is_keyframe = PG_GETARG_BOOL(2);
    int64 max_seq = PG_GETARG_INT64(3);
    int64 raw_size = PG_GETARG_INT64(4);
    int64 compressed_size = PG_GETARG_INT64(5);
    float8 avg_delta_tag = PG_GETARG_FLOAT8(6);
    XPatchGroupHash group_hash;

    if (VARSIZE_ANY_EXHDR(hash_bytea) != XPATCH_GROUP_HASH_SIZE)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("invalid group_hash size")));
    memcpy(&group_hash.h1, VARDATA_ANY(hash_bytea), sizeof(uint64));
    memcpy(&group_hash.h2, VARDATA_ANY(hash_bytea) + sizeof(uint64), sizeof(uint64));

    xpatch_stats_cache_update_group(relid, group_hash, is_keyframe, max_seq,
                                    raw_size, compressed_size, avg_delta_tag);
    PG_RETURN_VOID();
}

/* Structure to track per-group stats during scan */
typedef struct {
    XPatchGroupHash group_hash;
    Datum group_value;          /* For advisory locking and reconstruction */
    Oid group_typid;            /* Type of group_value */
    bool group_isnull;          /* Is group_value NULL? */
    bool lock_acquired;         /* Have we acquired advisory lock for this group? */
    int64 row_count;
    int64 keyframe_count;
    int64 raw_size;
    int64 compressed_size;
    double sum_avg_tags;
    int64 max_seq;
} RefreshGroupEntry;

/*
 * Refresh stats for specific groups by scanning only their rows.
 * If group_hashes is NULL, refreshes ALL groups (full table scan).
 *
 * For single-group refresh (DELETE path): caller already holds advisory lock.
 * For full refresh: we acquire advisory lock for each group before processing.
 *
 * This function decodes delta columns to get actual uncompressed sizes,
 * ensuring stats are accurate and consistent with INSERT tracking.
 */
int64
xpatch_stats_cache_refresh_groups(Oid relid, XPatchGroupHash *group_hashes, int num_groups)
{
    Relation rel;
    XPatchConfig *config;
    TupleDesc rel_tupdesc;
    BlockNumber nblocks, blkno;
    HTAB *groups_seen = NULL;
    HTAB *target_groups = NULL;
    HASHCTL hash_ctl;
    int64 rows_scanned = 0;
    int spi_ret;
    bool scan_all = (group_hashes == NULL || num_groups == 0);

    rel = table_open(relid, AccessShareLock);
    rel_tupdesc = RelationGetDescr(rel);
    config = xpatch_get_config(rel);

    /* Hash table to collect stats for groups we're refreshing */
    memset(&hash_ctl, 0, sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(XPatchGroupHash);
    hash_ctl.entrysize = sizeof(RefreshGroupEntry);
    hash_ctl.hcxt = CurrentMemoryContext;
    groups_seen = hash_create("refresh_stats groups", 64, &hash_ctl,
                              HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

    /* If not scanning all, create hash of target groups for fast lookup */
    if (!scan_all)
    {
        int i;
        target_groups = hash_create("target_groups", num_groups, &hash_ctl,
                                    HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
        for (i = 0; i < num_groups; i++)
        {
            bool found;
            hash_search(target_groups, &group_hashes[i], HASH_ENTER, &found);
        }
    }

    nblocks = RelationGetNumberOfBlocks(rel);

    for (blkno = 0; blkno < nblocks; blkno++)
    {
        Buffer buffer;
        Page page;
        OffsetNumber maxoff, off;

        buffer = ReadBuffer(rel, blkno);
        LockBuffer(buffer, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buffer);
        maxoff = PageGetMaxOffsetNumber(page);

        for (off = FirstOffsetNumber; off <= maxoff; off++)
        {
            ItemId itemId = PageGetItemId(page, off);
            HeapTupleData tuple;
            Datum group_val = (Datum) 0;
            bool group_isnull = true;
            Oid group_typid = InvalidOid;
            XPatchGroupHash group_hash;
            RefreshGroupEntry *entry;
            bool found;
            int64 row_seq = 0;
            bool is_keyframe = false;
            int64 row_raw_size = 0;
            int64 row_compressed_size = 0;
            int total_tags = 0;
            int num_delta_cols = 0;
            int i;

            if (!ItemIdIsNormal(itemId))
                continue;

            tuple.t_data = (HeapTupleHeader) PageGetItem(page, itemId);
            tuple.t_len = ItemIdGetLength(itemId);
            tuple.t_tableOid = RelationGetRelid(rel);
            ItemPointerSet(&tuple.t_self, blkno, off);

            /*
             * Use SnapshotSelf to see our own transaction's changes.
             * This is critical for DELETE - the refresh happens after the tuples
             * are marked deleted but before commit, so GetActiveSnapshot() would
             * still see them as visible.
             */
            if (!HeapTupleSatisfiesVisibility(&tuple, SnapshotSelf, buffer))
                continue;

            /* Get group value and compute hash */
            if (config->group_by_attnum != InvalidAttrNumber)
            {
                group_val = heap_getattr(&tuple, config->group_by_attnum,
                                         rel_tupdesc, &group_isnull);
                if (!group_isnull)
                {
                    Form_pg_attribute attr = TupleDescAttr(rel_tupdesc,
                                                           config->group_by_attnum - 1);
                    group_typid = attr->atttypid;
                }
            }

            group_hash = xpatch_compute_group_hash(group_val, group_typid, group_isnull);

            /* Skip if not in target groups (when doing partial refresh) */
            if (!scan_all)
            {
                if (!hash_search(target_groups, &group_hash, HASH_FIND, NULL))
                    continue;
            }

            rows_scanned++;

            /* Get or create group entry */
            entry = (RefreshGroupEntry *) hash_search(groups_seen, &group_hash,
                                                      HASH_ENTER, &found);
            if (!found)
            {
                entry->group_hash = group_hash;
                entry->group_value = group_val;
                entry->group_typid = group_typid;
                entry->group_isnull = group_isnull;
                entry->lock_acquired = false;
                entry->row_count = 0;
                entry->keyframe_count = 0;
                entry->raw_size = 0;
                entry->compressed_size = 0;
                entry->sum_avg_tags = 0;
                entry->max_seq = 0;

                /*
                 * For full refresh, acquire advisory lock on this group.
                 * For single-group refresh, caller already holds the lock.
                 */
                if (scan_all)
                {
                    uint64 lock_id = xpatch_compute_group_lock_id(relid, group_hash);
                    DirectFunctionCall1(pg_advisory_xact_lock_int8,
                                        Int64GetDatum((int64) lock_id));
                    entry->lock_acquired = true;
                }
            }

            /* Get sequence number */
            if (config->xp_seq_attnum != InvalidAttrNumber)
            {
                bool seq_isnull;
                Datum seq_datum = heap_getattr(&tuple, config->xp_seq_attnum,
                                               rel_tupdesc, &seq_isnull);
                if (!seq_isnull)
                    row_seq = DatumGetInt64(seq_datum);
            }

            /* Process delta columns - get compressed size and tag info first */
            for (i = 0; i < config->num_delta_columns; i++)
            {
                AttrNumber attnum = config->delta_attnums[i];
                bool isnull;
                Datum delta_datum = heap_getattr(&tuple, attnum, rel_tupdesc, &isnull);

                if (!isnull)
                {
                    bytea *delta_bytea = DatumGetByteaPP(delta_datum);
                    size_t tag;
                    const char *err;

                    row_compressed_size += VARSIZE_ANY(delta_bytea);

                    err = xpatch_get_delta_tag((uint8 *) VARDATA_ANY(delta_bytea),
                                               VARSIZE_ANY_EXHDR(delta_bytea), &tag);
                    if (err == NULL)
                    {
                        if (tag == XPATCH_KEYFRAME_TAG)
                            is_keyframe = true;
                        total_tags += tag;
                        num_delta_cols++;
                    }
                }
            }

            /*
             * Release buffer lock and reconstruct to get actual raw sizes.
             * This is safe because:
             * - For single-group refresh: caller holds advisory lock
             * - For full refresh: we acquired advisory lock above
             */
            LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

            for (i = 0; i < config->num_delta_columns; i++)
            {
                bytea *reconstructed = xpatch_reconstruct_column(rel, config,
                                                                  group_val, row_seq, i);
                if (reconstructed != NULL)
                {
                    row_raw_size += VARSIZE_ANY_EXHDR(reconstructed);
                    pfree(reconstructed);
                }
            }

            /* Re-lock buffer to continue scanning */
            LockBuffer(buffer, BUFFER_LOCK_SHARE);
            /* Re-get page pointer and maxoff after re-locking.
             * The buffer pin prevents VACUUM from reclaiming tuples,
             * but re-reading maxoff is defensive against any page
             * reorganization that could occur while unlocked. */
            page = BufferGetPage(buffer);
            maxoff = PageGetMaxOffsetNumber(page);

            entry->row_count++;
            if (is_keyframe)
                entry->keyframe_count++;
            entry->raw_size += row_raw_size;
            entry->compressed_size += row_compressed_size;
            if (num_delta_cols > 0)
                entry->sum_avg_tags += (double) total_tags / num_delta_cols;
            if (row_seq > entry->max_seq)
                entry->max_seq = row_seq;
        }

        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
        ReleaseBuffer(buffer);
    }

    /* Insert/update stats in database */
    spi_ret = SPI_connect();
    if (spi_ret == SPI_OK_CONNECT)
    {
        HASH_SEQ_STATUS status;
        RefreshGroupEntry *entry;

        /* If full refresh, delete existing stats first */
        if (scan_all)
        {
            Oid argtypes[1] = {OIDOID};
            Datum vals[1];
            vals[0] = ObjectIdGetDatum(relid);
            SPI_execute_with_args(DELETE_TABLE_STATS_SQL, 1, argtypes, vals, NULL, false, 0);
        }

        /*
         * For single-group refresh: if no rows were found for a target group,
         * we need to delete that group's stats (the group is now empty).
         */
        if (!scan_all && target_groups != NULL)
        {
            int i;
            for (i = 0; i < num_groups; i++)
            {
                if (!hash_search(groups_seen, &group_hashes[i], HASH_FIND, NULL))
                {
                    /* Group had no visible rows - delete its stats */
                    bytea *hash_bytea = group_hash_to_bytea(group_hashes[i]);
                    Oid argtypes[2] = {OIDOID, BYTEAOID};
                    Datum vals[2];
                    vals[0] = ObjectIdGetDatum(relid);
                    vals[1] = PointerGetDatum(hash_bytea);
                    SPI_execute_with_args(DELETE_GROUP_STATS_SQL, 2, argtypes, vals, NULL, false, 0);
                }
            }
        }

        hash_seq_init(&status, groups_seen);
        while ((entry = (RefreshGroupEntry *) hash_seq_search(&status)) != NULL)
        {
            Oid argtypes[8] = {OIDOID, BYTEAOID, INT8OID, INT8OID, INT8OID,
                               INT8OID, INT8OID, FLOAT8OID};
            Datum vals[8];
            bytea *hash_bytea = group_hash_to_bytea(entry->group_hash);

            vals[0] = ObjectIdGetDatum(relid);
            vals[1] = PointerGetDatum(hash_bytea);
            vals[2] = Int64GetDatum(entry->row_count);
            vals[3] = Int64GetDatum(entry->keyframe_count);
            vals[4] = Int64GetDatum(entry->max_seq);
            vals[5] = Int64GetDatum(entry->raw_size);
            vals[6] = Int64GetDatum(entry->compressed_size);
            vals[7] = Float8GetDatum(entry->sum_avg_tags);

            /* UPSERT with REPLACE semantics */
            SPI_execute_with_args(
                "INSERT INTO xpatch.group_stats "
                "(relid, group_hash, row_count, keyframe_count, max_seq, "
                " raw_size_bytes, compressed_size_bytes, sum_avg_delta_tags) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8) "
                "ON CONFLICT (relid, group_hash) DO UPDATE SET "
                "  row_count = EXCLUDED.row_count, "
                "  keyframe_count = EXCLUDED.keyframe_count, "
                "  max_seq = EXCLUDED.max_seq, "
                "  raw_size_bytes = EXCLUDED.raw_size_bytes, "
                "  compressed_size_bytes = EXCLUDED.compressed_size_bytes, "
                "  sum_avg_delta_tags = EXCLUDED.sum_avg_delta_tags",
                8, argtypes, vals, NULL, false, 0);
        }
        SPI_finish();
        
        /*
         * Make the inserted/updated rows visible to subsequent queries.
         * Without this, a new SPI connection won't see the changes we just made.
         */
        CommandCounterIncrement();
    }

    hash_destroy(groups_seen);
    if (target_groups)
        hash_destroy(target_groups);
    table_close(rel, AccessShareLock);

    return rows_scanned;
}

/*
 * SQL-callable: xpatch_refresh_stats_internal
 * Full table scan, populates all groups.
 */
PG_FUNCTION_INFO_V1(xpatch_refresh_stats_internal);
Datum
xpatch_refresh_stats_internal(PG_FUNCTION_ARGS)
{
    Oid relid = PG_GETARG_OID(0);
    TupleDesc tupdesc;
    Datum values[2];
    bool nulls[2] = {false, false};
    HeapTuple result_tuple;
    int64 rows_scanned;

    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("function returning record called in context that cannot accept type record")));

    /* Full refresh - pass NULL for group_hashes */
    rows_scanned = xpatch_stats_cache_refresh_groups(relid, NULL, 0);

    /* Count groups from cache */
    {
        int ret;
        int64 groups_count = 0;
        
        /*
         * Need another CCI here to make the inserts visible to this query.
         * The refresh_groups function already did CCI, but we need to ensure
         * our new SPI connection sees them.
         */
        CommandCounterIncrement();
        
        ret = SPI_connect();
        if (ret == SPI_OK_CONNECT)
        {
            char query[128];
            snprintf(query, sizeof(query),
                     "SELECT COUNT(*) FROM xpatch.group_stats WHERE relid = %u", relid);
            /* Use read_only=false to get a fresh snapshot */
            ret = SPI_execute(query, false, 1);
            if (ret == SPI_OK_SELECT && SPI_processed > 0)
            {
                bool isnull;
                Datum val = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
                if (!isnull)
                    groups_count = DatumGetInt64(val);
            }
            SPI_finish();
        }
        
        values[0] = Int64GetDatum(groups_count);
    }
    
    values[1] = Int64GetDatum(rows_scanned);

    result_tuple = heap_form_tuple(tupdesc, values, nulls);
    PG_RETURN_DATUM(HeapTupleGetDatum(result_tuple));
}
