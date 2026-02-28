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
 * xpatch_l3_cache.c — Persistent disk cache (L3) for decompressed content
 *
 * Implements SPI-backed read/write to per-table L3 cache tables in the
 * xpatch schema. Each xpatch table with L3 enabled gets a companion table:
 *
 *   xpatch.<table_name>_xp_l3
 *
 * The key is (group_hash_h1, group_hash_h2, seq, attnum) and the content
 * is stored as bytea. This is universal across all column types.
 *
 * All operations use SPI. Callers must be in a valid transaction context.
 *
 * Table creation uses an advisory lock to prevent concurrent DDL when
 * multiple backends discover the table is missing simultaneously.
 */

#include "xpatch_l3_cache.h"
#include "xpatch_chain_index.h"

#include "access/htup_details.h"
#include "catalog/namespace.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

/* ---------------------------------------------------------------------------
 * Per-backend cache of L3 enabled state per relation.
 *
 * Avoids repeated SPI calls to xpatch.table_config for every put/get.
 * Invalidated when xpatch.configure() is called (via relcache invalidation).
 * ---------------------------------------------------------------------------
 */
typedef struct L3EnabledCacheEntry
{
    Oid     relid;
    bool    enabled;
    bool    valid;
} L3EnabledCacheEntry;

#define L3_ENABLED_CACHE_SIZE   64

static L3EnabledCacheEntry l3_enabled_cache[L3_ENABLED_CACHE_SIZE];
static bool l3_enabled_cache_inited = false;

static void
l3_enabled_cache_init(void)
{
    int     i;

    for (i = 0; i < L3_ENABLED_CACHE_SIZE; i++)
    {
        l3_enabled_cache[i].relid = InvalidOid;
        l3_enabled_cache[i].enabled = false;
        l3_enabled_cache[i].valid = false;
    }
    l3_enabled_cache_inited = true;
}

static int
l3_enabled_cache_slot(Oid relid)
{
    return (int)(relid % L3_ENABLED_CACHE_SIZE);
}

/*
 * Invalidate the per-backend L3 enabled cache for a relation.
 * Called when config changes (e.g., xpatch.configure()).
 */
static void
l3_enabled_cache_invalidate(Oid relid)
{
    int     slot;

    if (!l3_enabled_cache_inited)
        return;

    if (relid == InvalidOid)
    {
        /* Invalidate all */
        l3_enabled_cache_inited = false;
        return;
    }

    slot = l3_enabled_cache_slot(relid);
    if (l3_enabled_cache[slot].relid == relid)
        l3_enabled_cache[slot].valid = false;
}

/* ---------------------------------------------------------------------------
 * Internal helpers
 * ---------------------------------------------------------------------------
 */

/*
 * Build the L3 table name for a relation: "xpatch"."<relname>_xp_l3"
 *
 * Returns a palloc'd string with the fully qualified, properly quoted name.
 * Returns NULL if the relation name can't be resolved.
 */
char *
xpatch_l3_cache_table_name(Oid relid)
{
    char   *relname;
    char   *result;

    relname = get_rel_name(relid);
    if (relname == NULL)
        return NULL;

    result = psprintf("xpatch.\"%s_xp_l3\"", relname);
    pfree(relname);
    return result;
}

/*
 * Check if the L3 table exists for a relation.
 */
static bool
l3_table_exists(Oid relid)
{
    char   *relname;
    char   *l3name;
    Oid     l3oid;
    Oid     xpatch_ns;

    relname = get_rel_name(relid);
    if (relname == NULL)
        return false;

    l3name = psprintf("%s_xp_l3", relname);
    pfree(relname);

    xpatch_ns = get_namespace_oid("xpatch", true);
    if (!OidIsValid(xpatch_ns))
    {
        pfree(l3name);
        return false;
    }

    l3oid = get_relname_relid(l3name, xpatch_ns);
    pfree(l3name);

    return OidIsValid(l3oid);
}

/* ---------------------------------------------------------------------------
 * Public API
 * ---------------------------------------------------------------------------
 */

bool
xpatch_l3_cache_is_enabled(Oid relid)
{
    int     slot;
    int     ret;
    Oid     argtypes[1] = { OIDOID };
    Datum   values[1];
    char    nulls[1] = { ' ' };
    bool    enabled = false;

    if (!l3_enabled_cache_inited)
        l3_enabled_cache_init();

    /* Check per-backend cache first */
    slot = l3_enabled_cache_slot(relid);
    if (l3_enabled_cache[slot].valid && l3_enabled_cache[slot].relid == relid)
        return l3_enabled_cache[slot].enabled;

    /* Query xpatch.table_config via SPI */
    values[0] = ObjectIdGetDatum(relid);

    if (SPI_connect() != SPI_OK_CONNECT)
        return false;

    ret = SPI_execute_with_args(
        "SELECT l3_cache_enabled FROM xpatch.table_config WHERE relid = $1",
        1, argtypes, values, nulls, true, 1);

    if (ret == SPI_OK_SELECT && SPI_processed > 0)
    {
        bool    isnull;
        Datum   datum;

        datum = SPI_getbinval(SPI_tuptable->vals[0],
                              SPI_tuptable->tupdesc, 1, &isnull);
        if (!isnull)
            enabled = DatumGetBool(datum);
    }

    SPI_finish();

    /* Update per-backend cache */
    l3_enabled_cache[slot].relid = relid;
    l3_enabled_cache[slot].enabled = enabled;
    l3_enabled_cache[slot].valid = true;

    return enabled;
}

bool
xpatch_l3_cache_ensure_table(Oid relid)
{
    char   *l3_table;
    char   *relname;
    char   *sql;
    int     ret;

    if (l3_table_exists(relid))
        return true;

    relname = get_rel_name(relid);
    if (relname == NULL)
        return false;

    l3_table = psprintf("%s_xp_l3", relname);
    pfree(relname);

    /*
     * Use advisory lock to prevent concurrent CREATE TABLE.
     * Lock key: hash of the L3 table name.
     */
    if (SPI_connect() != SPI_OK_CONNECT)
    {
        pfree(l3_table);
        return false;
    }

    /* Advisory lock on the relid to serialize table creation */
    sql = psprintf("SELECT pg_advisory_xact_lock(%d, %d)",
                   (int32)(relid & 0xFFFFFFFF),
                   (int32)0x4C33); /* 'L3' as int */
    SPI_execute(sql, true, 0);
    pfree(sql);

    /* Double-check after acquiring lock */
    if (l3_table_exists(relid))
    {
        SPI_finish();
        pfree(l3_table);
        return true;
    }

    /* Create the L3 table (IF NOT EXISTS for extra safety against races) */
    sql = psprintf(
        "CREATE TABLE IF NOT EXISTS xpatch.\"%s\" ("
        "  group_hash_h1   int8        NOT NULL,"
        "  group_hash_h2   int8        NOT NULL,"
        "  seq             int8        NOT NULL,"
        "  attnum          int2        NOT NULL,"
        "  content         bytea,"
        "  cached_at       timestamptz NOT NULL DEFAULT now(),"
        "  PRIMARY KEY (group_hash_h1, group_hash_h2, seq, attnum)"
        ")",
        l3_table);

    ret = SPI_execute(sql, false, 0);
    pfree(sql);

    if (ret != SPI_OK_UTILITY)
    {
        elog(WARNING, "xpatch L3: failed to create table xpatch.\"%s\"",
             l3_table);
        SPI_finish();
        pfree(l3_table);
        return false;
    }

    /* Create the cached_at index for eviction (IF NOT EXISTS for races) */
    sql = psprintf(
        "CREATE INDEX IF NOT EXISTS \"%s_cached_at_idx\" ON xpatch.\"%s\" (cached_at)",
        l3_table, l3_table);
    SPI_execute(sql, false, 0);
    pfree(sql);

    SPI_finish();
    pfree(l3_table);

    elog(DEBUG1, "xpatch L3: created cache table for relid %u", relid);
    return true;
}

bytea *
xpatch_l3_cache_get(Oid relid, XPatchGroupHash group_hash,
                    int64 seq, AttrNumber attnum)
{
    char       *l3_table;
    char       *sql;
    int         ret;
    Oid         argtypes[4] = { INT8OID, INT8OID, INT8OID, INT2OID };
    Datum       values[4];
    char        nulls[4] = { ' ', ' ', ' ', ' ' };
    bytea      *result = NULL;

    /* Fast check: L3 enabled? */
    if (!xpatch_l3_cache_is_enabled(relid))
        return NULL;

    /* Check table exists */
    if (!l3_table_exists(relid))
        return NULL;

    l3_table = xpatch_l3_cache_table_name(relid);
    if (l3_table == NULL)
        return NULL;

    values[0] = Int64GetDatum((int64) group_hash.h1);
    values[1] = Int64GetDatum((int64) group_hash.h2);
    values[2] = Int64GetDatum(seq);
    values[3] = Int16GetDatum(attnum);

    if (SPI_connect() != SPI_OK_CONNECT)
    {
        pfree(l3_table);
        return NULL;
    }

    sql = psprintf(
        "SELECT content FROM %s "
        "WHERE group_hash_h1 = $1 AND group_hash_h2 = $2 "
        "  AND seq = $3 AND attnum = $4",
        l3_table);

    ret = SPI_execute_with_args(sql, 4, argtypes, values, nulls, true, 1);
    pfree(sql);

    if (ret == SPI_OK_SELECT && SPI_processed > 0)
    {
        bool        isnull;
        Datum       datum;
        bytea      *spi_content;
        Size        content_size;

        datum = SPI_getbinval(SPI_tuptable->vals[0],
                              SPI_tuptable->tupdesc, 1, &isnull);
        if (!isnull)
        {
            /*
             * Copy content out of SPI memory context into caller's context.
             * SPI memory is freed on SPI_finish().
             */
            spi_content = DatumGetByteaPP(datum);
            content_size = VARSIZE(spi_content);
            result = (bytea *) palloc(content_size);
            memcpy(result, spi_content, content_size);
        }
    }

    SPI_finish();
    pfree(l3_table);

    return result;
}

void
xpatch_l3_cache_put(Oid relid, XPatchGroupHash group_hash,
                    int64 seq, AttrNumber attnum,
                    bytea *content)
{
    char       *l3_table;
    char       *sql;
    int         ret;
    Oid         argtypes[5] = { INT8OID, INT8OID, INT8OID, INT2OID, BYTEAOID };
    Datum       values[5];
    char        nulls[5];

    /* Fast check: L3 enabled? */
    if (!xpatch_l3_cache_is_enabled(relid))
        return;

    /* Ensure L3 table exists (auto-create on first write) */
    if (!xpatch_l3_cache_ensure_table(relid))
        return;

    l3_table = xpatch_l3_cache_table_name(relid);
    if (l3_table == NULL)
        return;

    values[0] = Int64GetDatum((int64) group_hash.h1);
    values[1] = Int64GetDatum((int64) group_hash.h2);
    values[2] = Int64GetDatum(seq);
    values[3] = Int16GetDatum(attnum);

    if (content != NULL)
    {
        values[4] = PointerGetDatum(content);
        nulls[0] = ' ';
        nulls[1] = ' ';
        nulls[2] = ' ';
        nulls[3] = ' ';
        nulls[4] = ' ';
    }
    else
    {
        values[4] = (Datum) 0;
        nulls[0] = ' ';
        nulls[1] = ' ';
        nulls[2] = ' ';
        nulls[3] = ' ';
        nulls[4] = 'n';
    }

    if (SPI_connect() != SPI_OK_CONNECT)
    {
        pfree(l3_table);
        return;
    }

    /*
     * INSERT ... ON CONFLICT DO UPDATE to handle concurrent puts and
     * updates to existing entries.
     */
    sql = psprintf(
        "INSERT INTO %s (group_hash_h1, group_hash_h2, seq, attnum, content, cached_at) "
        "VALUES ($1, $2, $3, $4, $5, now()) "
        "ON CONFLICT (group_hash_h1, group_hash_h2, seq, attnum) "
        "DO UPDATE SET content = EXCLUDED.content, cached_at = now()",
        l3_table);

    ret = SPI_execute_with_args(sql, 5, argtypes, values, nulls, false, 0);
    pfree(sql);

    if (ret != SPI_OK_INSERT)
    {
        elog(DEBUG1, "xpatch L3: INSERT failed for relid %u seq " INT64_FORMAT
             " (ret=%d)", relid, seq, ret);
    }

    SPI_finish();
    pfree(l3_table);

    /* Set CHAIN_BIT_L3 in chain index */
    if (xpatch_chain_index_is_ready())
    {
        xpatch_chain_index_update_bits(relid, group_hash, attnum, seq,
                                       CHAIN_BIT_L3, 0);
    }
}

void
xpatch_l3_cache_invalidate(Oid relid, XPatchGroupHash group_hash,
                           int64 seq, AttrNumber attnum)
{
    char       *l3_table;
    char       *sql;
    Oid         argtypes[4] = { INT8OID, INT8OID, INT8OID, INT2OID };
    Datum       values[4];
    char        nulls[4] = { ' ', ' ', ' ', ' ' };

    if (!l3_table_exists(relid))
        return;

    l3_table = xpatch_l3_cache_table_name(relid);
    if (l3_table == NULL)
        return;

    values[0] = Int64GetDatum((int64) group_hash.h1);
    values[1] = Int64GetDatum((int64) group_hash.h2);
    values[2] = Int64GetDatum(seq);
    values[3] = Int16GetDatum(attnum);

    if (SPI_connect() != SPI_OK_CONNECT)
    {
        pfree(l3_table);
        return;
    }

    sql = psprintf(
        "DELETE FROM %s "
        "WHERE group_hash_h1 = $1 AND group_hash_h2 = $2 "
        "  AND seq = $3 AND attnum = $4",
        l3_table);

    SPI_execute_with_args(sql, 4, argtypes, values, nulls, false, 0);
    pfree(sql);

    SPI_finish();
    pfree(l3_table);

    /* Clear CHAIN_BIT_L3 in chain index */
    if (xpatch_chain_index_is_ready())
    {
        xpatch_chain_index_update_bits(relid, group_hash, attnum, seq,
                                       0, CHAIN_BIT_L3);
    }
}

void
xpatch_l3_cache_invalidate_rel(Oid relid)
{
    char   *l3_table;
    char   *sql;

    /* Invalidate per-backend cache */
    l3_enabled_cache_invalidate(relid);

    if (!l3_table_exists(relid))
        return;

    l3_table = xpatch_l3_cache_table_name(relid);
    if (l3_table == NULL)
        return;

    if (SPI_connect() != SPI_OK_CONNECT)
    {
        pfree(l3_table);
        return;
    }

    sql = psprintf("DROP TABLE IF EXISTS %s", l3_table);
    SPI_execute(sql, false, 0);
    pfree(sql);

    SPI_finish();
    pfree(l3_table);

    elog(DEBUG1, "xpatch L3: dropped cache table for relid %u", relid);

    /*
     * Note: CHAIN_BIT_L3 bits in the chain index will be stale but harmless.
     * The path planner will try L3, get NULL from l3_cache_get (table gone),
     * and fall back to disk. The bits will be cleaned up on next startup
     * warming or the next chain index rebuild.
     */
}

bool
xpatch_l3_cache_drop(Oid relid)
{
    bool    existed;

    /* Invalidate per-backend cache */
    l3_enabled_cache_invalidate(relid);

    existed = l3_table_exists(relid);
    if (existed)
        xpatch_l3_cache_invalidate_rel(relid);

    return existed;
}
