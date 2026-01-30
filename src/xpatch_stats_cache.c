/*
 * pg-xpatch - PostgreSQL Table Access Method for delta-compressed data
 * Copyright (c) 2025 Oliver Seifert
 *
 * xpatch_stats_cache.c - Stats cache implementation
 *
 * Uses SPI to update xpatch.group_stats and xpatch.table_stats tables.
 */

#include "xpatch_stats_cache.h"
#include "xpatch_config.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/tableam.h"
#include "catalog/namespace.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "funcapi.h"

/* Size of XPatchGroupHash when serialized to bytea (h1 + h2 = 16 bytes) */
#define XPATCH_GROUP_HASH_SIZE (sizeof(uint64) * 2)

/* Upsert group stats */
static const char *UPDATE_GROUP_STATS_SQL =
    "INSERT INTO xpatch.group_stats ("
    "  relid, group_hash, group_value_text, row_count, keyframe_count, delta_count, "
    "  max_seq, max_version_typid, max_version_data, raw_size_bytes, compressed_size_bytes, "
    "  is_valid, last_updated"
    ") VALUES ($1, $2, $3, 1, $4::int, $5::int, $6, $7, $8, $9, $10, true, now()) "
    "ON CONFLICT (relid, group_hash) DO UPDATE SET "
    "  row_count = xpatch.group_stats.row_count + 1, "
    "  keyframe_count = xpatch.group_stats.keyframe_count + EXCLUDED.keyframe_count, "
    "  delta_count = xpatch.group_stats.delta_count + EXCLUDED.delta_count, "
    "  max_seq = GREATEST(xpatch.group_stats.max_seq, EXCLUDED.max_seq), "
    "  max_version_typid = EXCLUDED.max_version_typid, "
    "  max_version_data = CASE WHEN EXCLUDED.max_seq > xpatch.group_stats.max_seq "
    "                          THEN EXCLUDED.max_version_data "
    "                          ELSE xpatch.group_stats.max_version_data END, "
    "  raw_size_bytes = xpatch.group_stats.raw_size_bytes + EXCLUDED.raw_size_bytes, "
    "  compressed_size_bytes = xpatch.group_stats.compressed_size_bytes + EXCLUDED.compressed_size_bytes, "
    "  last_updated = now()";

/* Invalidate group stats */
static const char *INVALIDATE_GROUP_SQL =
    "UPDATE xpatch.group_stats SET is_valid = false, last_updated = now() "
    "WHERE relid = $1 AND group_hash = $2";

/* Invalidate table stats */
static const char *INVALIDATE_TABLE_SQL =
    "UPDATE xpatch.table_stats SET is_valid = false, last_updated = now() "
    "WHERE relid = $1";

/* Get max_seq for a group */
static const char *GET_MAX_SEQ_SQL =
    "SELECT max_seq FROM xpatch.group_stats "
    "WHERE relid = $1 AND group_hash = $2 AND is_valid";

/* Check if table stats are valid */
static const char *CHECK_VALID_SQL =
    "SELECT NOT EXISTS(SELECT 1 FROM xpatch.group_stats WHERE relid = $1 AND NOT is_valid)";

/*
 * Update group stats after INSERT.
 */
void
xpatch_stats_cache_update_group(
    Oid relid,
    XPatchGroupHash group_hash,
    const char *group_value_text,
    bool is_keyframe,
    int32 max_seq,
    Oid max_version_typid,
    const uint8 *max_version_data,
    Size max_version_len,
    int64 raw_size,
    int64 compressed_size)
{
    int ret;
    Oid argtypes[10];
    Datum values[10];
    char nulls[10];
    bytea *hash_bytea;
    bytea *version_bytea = NULL;

    /* Connect to SPI */
    if ((ret = SPI_connect()) != SPI_OK_CONNECT)
    {
        elog(WARNING, "xpatch_stats_cache: SPI_connect failed: %d", ret);
        return;
    }

    /* Build group_hash as bytea (h1 + h2 = 16 bytes) */
    hash_bytea = (bytea *) palloc(VARHDRSZ + XPATCH_GROUP_HASH_SIZE);
    SET_VARSIZE(hash_bytea, VARHDRSZ + XPATCH_GROUP_HASH_SIZE);
    memcpy(VARDATA(hash_bytea), &group_hash.h1, sizeof(uint64));
    memcpy(VARDATA(hash_bytea) + sizeof(uint64), &group_hash.h2, sizeof(uint64));

    /* Build max_version_data as bytea if provided */
    if (max_version_data != NULL && max_version_len > 0)
    {
        version_bytea = (bytea *) palloc(VARHDRSZ + max_version_len);
        SET_VARSIZE(version_bytea, VARHDRSZ + max_version_len);
        memcpy(VARDATA(version_bytea), max_version_data, max_version_len);
    }

    /* Set up arguments */
    argtypes[0] = OIDOID;           /* relid */
    argtypes[1] = BYTEAOID;         /* group_hash */
    argtypes[2] = TEXTOID;          /* group_value_text */
    argtypes[3] = INT4OID;          /* keyframe_count (1 or 0) */
    argtypes[4] = INT4OID;          /* delta_count (0 or 1) */
    argtypes[5] = INT4OID;          /* max_seq */
    argtypes[6] = OIDOID;           /* max_version_typid */
    argtypes[7] = BYTEAOID;         /* max_version_data */
    argtypes[8] = INT8OID;          /* raw_size_bytes */
    argtypes[9] = INT8OID;          /* compressed_size_bytes */

    values[0] = ObjectIdGetDatum(relid);
    values[1] = PointerGetDatum(hash_bytea);
    values[2] = group_value_text ? CStringGetTextDatum(group_value_text) : (Datum) 0;
    values[3] = Int32GetDatum(is_keyframe ? 1 : 0);
    values[4] = Int32GetDatum(is_keyframe ? 0 : 1);
    values[5] = Int32GetDatum(max_seq);
    values[6] = ObjectIdGetDatum(max_version_typid);
    values[7] = version_bytea ? PointerGetDatum(version_bytea) : (Datum) 0;
    values[8] = Int64GetDatum(raw_size);
    values[9] = Int64GetDatum(compressed_size);

    memset(nulls, ' ', sizeof(nulls));
    if (group_value_text == NULL)
        nulls[2] = 'n';
    if (version_bytea == NULL)
        nulls[7] = 'n';

    /* Execute upsert */
    ret = SPI_execute_with_args(UPDATE_GROUP_STATS_SQL, 10, argtypes, values, nulls, false, 0);
    if (ret != SPI_OK_INSERT)
    {
        elog(WARNING, "xpatch_stats_cache: update group stats failed: %d", ret);
    }

    /* Mark table stats as invalid (they need to be recomputed from group stats) */
    argtypes[0] = OIDOID;
    values[0] = ObjectIdGetDatum(relid);
    SPI_execute_with_args(INVALIDATE_TABLE_SQL, 1, argtypes, values, NULL, false, 0);

    SPI_finish();
}

/*
 * Invalidate group stats after DELETE.
 */
void
xpatch_stats_cache_invalidate_group(Oid relid, XPatchGroupHash group_hash)
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

    hash_bytea = (bytea *) palloc(VARHDRSZ + XPATCH_GROUP_HASH_SIZE);
    SET_VARSIZE(hash_bytea, VARHDRSZ + XPATCH_GROUP_HASH_SIZE);
    memcpy(VARDATA(hash_bytea), &group_hash.h1, sizeof(uint64));
    memcpy(VARDATA(hash_bytea) + sizeof(uint64), &group_hash.h2, sizeof(uint64));

    argtypes[0] = OIDOID;
    argtypes[1] = BYTEAOID;
    values[0] = ObjectIdGetDatum(relid);
    values[1] = PointerGetDatum(hash_bytea);

    SPI_execute_with_args(INVALIDATE_GROUP_SQL, 2, argtypes, values, NULL, false, 0);
    SPI_execute_with_args(INVALIDATE_TABLE_SQL, 1, argtypes, values, NULL, false, 0);

    SPI_finish();
}

/*
 * Invalidate all stats for a table (called on TRUNCATE).
 */
void
xpatch_stats_cache_invalidate_table(Oid relid)
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

    /* Delete all stats for this table */
    SPI_execute_with_args(
        "DELETE FROM xpatch.group_stats WHERE relid = $1",
        1, argtypes, values, NULL, false, 0);
    SPI_execute_with_args(
        "DELETE FROM xpatch.table_stats WHERE relid = $1",
        1, argtypes, values, NULL, false, 0);

    SPI_finish();
}

/*
 * Get max_seq for a group from cache.
 */
int32
xpatch_stats_cache_get_max_seq(Oid relid, XPatchGroupHash group_hash)
{
    int ret;
    int32 max_seq = -1;
    Oid argtypes[2];
    Datum values[2];
    bytea *hash_bytea;

    if ((ret = SPI_connect()) != SPI_OK_CONNECT)
        return -1;

    hash_bytea = (bytea *) palloc(VARHDRSZ + XPATCH_GROUP_HASH_SIZE);
    SET_VARSIZE(hash_bytea, VARHDRSZ + XPATCH_GROUP_HASH_SIZE);
    memcpy(VARDATA(hash_bytea), &group_hash.h1, sizeof(uint64));
    memcpy(VARDATA(hash_bytea) + sizeof(uint64), &group_hash.h2, sizeof(uint64));

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
            max_seq = DatumGetInt32(val);
    }

    SPI_finish();
    return max_seq;
}

/*
 * Check if table stats are valid.
 */
bool
xpatch_stats_cache_is_valid(Oid relid)
{
    int ret;
    bool is_valid = false;
    Oid argtypes[1];
    Datum values[1];

    if ((ret = SPI_connect()) != SPI_OK_CONNECT)
        return false;

    argtypes[0] = OIDOID;
    values[0] = ObjectIdGetDatum(relid);

    ret = SPI_execute_with_args(CHECK_VALID_SQL, 1, argtypes, values, NULL, true, 1);
    if (ret == SPI_OK_SELECT && SPI_processed > 0)
    {
        bool isnull;
        Datum val = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
        if (!isnull)
            is_valid = DatumGetBool(val);
    }

    SPI_finish();
    return is_valid;
}

/*
 * SQL-callable function: xpatch_update_group_stats
 */
PG_FUNCTION_INFO_V1(xpatch_update_group_stats);
Datum
xpatch_update_group_stats(PG_FUNCTION_ARGS)
{
    Oid relid = PG_GETARG_OID(0);
    bytea *hash_bytea = PG_GETARG_BYTEA_PP(1);
    text *group_text = PG_ARGISNULL(2) ? NULL : PG_GETARG_TEXT_PP(2);
    bool is_keyframe = PG_GETARG_BOOL(3);
    int32 max_seq = PG_GETARG_INT32(4);
    Oid max_version_typid = PG_ARGISNULL(5) ? InvalidOid : PG_GETARG_OID(5);
    bytea *version_bytea = PG_ARGISNULL(6) ? NULL : PG_GETARG_BYTEA_PP(6);
    int64 raw_size = PG_GETARG_INT64(7);
    int64 compressed_size = PG_GETARG_INT64(8);

    XPatchGroupHash group_hash;
    char *group_value_text = NULL;

    /* Extract group hash */
    if (VARSIZE_ANY_EXHDR(hash_bytea) != XPATCH_GROUP_HASH_SIZE)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("invalid group_hash size")));
    memcpy(&group_hash.h1, VARDATA_ANY(hash_bytea), sizeof(uint64));
    memcpy(&group_hash.h2, VARDATA_ANY(hash_bytea) + sizeof(uint64), sizeof(uint64));

    if (group_text != NULL)
        group_value_text = text_to_cstring(group_text);

    xpatch_stats_cache_update_group(
        relid,
        group_hash,
        group_value_text,
        is_keyframe,
        max_seq,
        max_version_typid,
        version_bytea ? (uint8 *) VARDATA_ANY(version_bytea) : NULL,
        version_bytea ? VARSIZE_ANY_EXHDR(version_bytea) : 0,
        raw_size,
        compressed_size
    );

    PG_RETURN_VOID();
}

/*
 * SQL-callable function: xpatch_invalidate_group_stats
 */
PG_FUNCTION_INFO_V1(xpatch_invalidate_group_stats);
Datum
xpatch_invalidate_group_stats(PG_FUNCTION_ARGS)
{
    Oid relid = PG_GETARG_OID(0);
    bytea *hash_bytea = PG_GETARG_BYTEA_PP(1);
    XPatchGroupHash group_hash;

    if (VARSIZE_ANY_EXHDR(hash_bytea) != XPATCH_GROUP_HASH_SIZE)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("invalid group_hash size")));
    memcpy(&group_hash.h1, VARDATA_ANY(hash_bytea), sizeof(uint64));
    memcpy(&group_hash.h2, VARDATA_ANY(hash_bytea) + sizeof(uint64), sizeof(uint64));

    xpatch_stats_cache_invalidate_group(relid, group_hash);

    PG_RETURN_VOID();
}

/*
 * SQL-callable function: xpatch_refresh_stats_internal
 * Does a full table scan and populates xpatch.group_stats
 *
 * TODO: Implement full table scan to populate stats.
 * For now, returns 0,0 - caller should use the existing xpatch_stats() for initial population.
 */
PG_FUNCTION_INFO_V1(xpatch_refresh_stats_internal);
Datum
xpatch_refresh_stats_internal(PG_FUNCTION_ARGS)
{
    TupleDesc tupdesc;
    Datum values[2];
    bool nulls[2] = {false, false};
    HeapTuple result_tuple;
    int64 groups_count = 0;
    int64 rows_count = 0;

    /* Suppress unused parameter warning */
    (void) PG_GETARG_OID(0);

    /* For now, return dummy values - full implementation would scan the table */
    /* This is a placeholder that will be filled in with proper scanning logic */

    /* Build result tuple */
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("function returning record called in context that cannot accept type record")));

    values[0] = Int64GetDatum(groups_count);
    values[1] = Int64GetDatum(rows_count);

    result_tuple = heap_form_tuple(tupdesc, values, nulls);
    PG_RETURN_DATUM(HeapTupleGetDatum(result_tuple));
}
