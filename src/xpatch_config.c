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
 * xpatch_config.c - Configuration parsing and storage
 *
 * Uses auto-detection by default, with optional configuration via
 * xpatch.table_config catalog table (populated by xpatch.configure()).
 * 
 * PostgreSQL 16's table AM API doesn't support custom WITH clause options,
 * so we use a catalog table for explicit configuration when needed.
 */

#include "xpatch_config.h"

#include "access/htup_details.h"
#include "access/table.h"
#include "catalog/pg_attribute.h"
#include "executor/spi.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"

/* Static cache for configs */
static HTAB *config_cache = NULL;

typedef struct ConfigCacheEntry
{
    Oid         relid;          /* Key */
    XPatchConfig *config;       /* Cached config */
} ConfigCacheEntry;

/*
 * Initialize the config cache
 */
static void
init_config_cache(void)
{
    HASHCTL hash_ctl;

    if (config_cache != NULL)
        return;

    memset(&hash_ctl, 0, sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(Oid);
    hash_ctl.entrysize = sizeof(ConfigCacheEntry);
    hash_ctl.hcxt = TopMemoryContext;

    config_cache = hash_create("xpatch config cache",
                               32, /* initial size */
                               &hash_ctl,
                               HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

/*
 * Resolve a column name to its attribute number
 */
static AttrNumber
resolve_column_name(Relation rel, const char *colname)
{
    TupleDesc tupdesc = RelationGetDescr(rel);
    int natts = tupdesc->natts;

    for (int i = 0; i < natts; i++)
    {
        Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
        if (attr->attisdropped)
            continue;

        if (strcmp(NameStr(attr->attname), colname) == 0)
            return attr->attnum;
    }

    ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_COLUMN),
             errmsg("column \"%s\" does not exist in table \"%s\"",
                    colname, RelationGetRelationName(rel))));

    return InvalidAttrNumber; /* Not reached */
}

/*
 * Auto-detect order_by column: last INTEGER/BIGINT/TIMESTAMP column
 * (excluding the internal _xp_seq column)
 */
static void
auto_detect_order_by(Relation rel, XPatchConfig *config)
{
    TupleDesc tupdesc = RelationGetDescr(rel);
    int natts = tupdesc->natts;

    for (int i = natts - 1; i >= 0; i--)
    {
        Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
        if (attr->attisdropped)
            continue;

        /* Skip the internal _xp_seq column */
        if (strcmp(NameStr(attr->attname), "_xp_seq") == 0)
            continue;

        if (attr->atttypid == INT2OID || attr->atttypid == INT4OID ||
            attr->atttypid == INT8OID || attr->atttypid == TIMESTAMPOID ||
            attr->atttypid == TIMESTAMPTZOID)
        {
            config->order_by = MemoryContextStrdup(TopMemoryContext, NameStr(attr->attname));
            config->order_by_attnum = attr->attnum;
            elog(NOTICE, "xpatch: auto-detected order_by column: %s",
                 config->order_by);
            return;
        }
    }

    ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
             errmsg("xpatch tables require an order_by column"),
             errhint("Add an INTEGER, BIGINT, or TIMESTAMP column for versioning, "
                     "or call xpatch.configure() with explicit order_by.")));
}

/*
 * Auto-detect delta_columns: all BYTEA, TEXT, VARCHAR, JSON, JSONB columns
 */
static void
auto_detect_delta_columns(Relation rel, XPatchConfig *config)
{
    TupleDesc tupdesc = RelationGetDescr(rel);
    int natts = tupdesc->natts;
    int capacity = 8;
    int count = 0;

    config->delta_columns = MemoryContextAlloc(TopMemoryContext, capacity * sizeof(char *));
    config->delta_attnums = MemoryContextAlloc(TopMemoryContext, capacity * sizeof(AttrNumber));

    for (int i = 0; i < natts; i++)
    {
        Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
        Oid typid;

        if (attr->attisdropped)
            continue;

        typid = attr->atttypid;
        if (typid == BYTEAOID || typid == TEXTOID || typid == VARCHAROID ||
            typid == JSONOID || typid == JSONBOID)
        {
            if (count >= capacity)
            {
                capacity *= 2;
                config->delta_columns = repalloc(config->delta_columns,
                                                 capacity * sizeof(char *));
                config->delta_attnums = repalloc(config->delta_attnums,
                                                 capacity * sizeof(AttrNumber));
            }
            config->delta_columns[count] = MemoryContextStrdup(TopMemoryContext, NameStr(attr->attname));
            config->delta_attnums[count] = attr->attnum;
            count++;
        }
    }

    config->num_delta_columns = count;

    if (count > 0)
    {
        elog(NOTICE, "xpatch: auto-detected %d delta column(s): %s%s",
             count, config->delta_columns[0],
             count > 1 ? ", ..." : "");
    }
    else
    {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("xpatch tables require at least one delta column"),
                 errhint("Add at least one BYTEA, TEXT, VARCHAR, JSON, or JSONB column, "
                         "or call xpatch.configure() with explicit delta_columns.")));
    }
}

/*
 * Try to read configuration from xpatch.table_config catalog table.
 * First tries by OID, then falls back to schema.table name lookup.
 * Returns true if config was found, false otherwise.
 */
static bool
read_config_from_catalog(Oid relid, XPatchConfig *config)
{
    int ret;
    bool found = false;
    Oid argtypes[1] = { OIDOID };
    Datum values[1];
    char nulls[1] = { ' ' };
    volatile bool spi_connected = false;

    values[0] = ObjectIdGetDatum(relid);

    if (SPI_connect() != SPI_OK_CONNECT)
    {
        elog(DEBUG1, "xpatch: SPI_connect failed, using auto-detection");
        return false;
    }
    spi_connected = true;

    PG_TRY();
    {
        /* First try by OID (fastest) */
        ret = SPI_execute_with_args(
            "SELECT group_by, order_by, delta_columns, keyframe_every, compress_depth, enable_zstd "
            "FROM xpatch.table_config WHERE relid = $1",
            1, argtypes, values, nulls, true, 1);
        
        /* If not found by OID, try by schema.table name (handles pg_restore) */
        if (ret == SPI_OK_SELECT && SPI_processed == 0)
        {
            ret = SPI_execute_with_args(
                "SELECT tc.group_by, tc.order_by, tc.delta_columns, tc.keyframe_every, "
                "       tc.compress_depth, tc.enable_zstd "
                "FROM xpatch.table_config tc "
                "JOIN pg_class c ON tc.schema_name = (SELECT nspname FROM pg_namespace WHERE oid = c.relnamespace) "
                "                AND tc.table_name = c.relname "
                "WHERE c.oid = $1",
                1, argtypes, values, nulls, true, 1);
            
            if (ret == SPI_OK_SELECT && SPI_processed > 0)
            {
                SPITupleTable *saved_tuptable;
                uint64 saved_processed;
                
                /* Save the SELECT result before UPDATE invalidates SPI_tuptable */
                saved_tuptable = SPI_tuptable;
                saved_processed = SPI_processed;
                
                /* Found by name - update the relid in the config table for next time */
                SPI_execute_with_args(
                    "UPDATE xpatch.table_config SET relid = $1 "
                    "WHERE (schema_name, table_name) = ("
                    "  SELECT n.nspname, c.relname FROM pg_class c "
                    "  JOIN pg_namespace n ON c.relnamespace = n.oid WHERE c.oid = $1"
                    ")",
                    1, argtypes, values, nulls, false, 0);
                
                /* Restore the SELECT result for processing below */
                SPI_tuptable = saved_tuptable;
                SPI_processed = saved_processed;
                
                elog(DEBUG1, "xpatch: found config by table name, updated OID");
            }
        }

        if (ret == SPI_OK_SELECT && SPI_processed > 0)
        {
            HeapTuple tuple = SPI_tuptable->vals[0];
            TupleDesc tupdesc = SPI_tuptable->tupdesc;
            bool isnull;
            Datum datum;
            MemoryContext oldcxt;

            found = true;
            oldcxt = MemoryContextSwitchTo(TopMemoryContext);

            /* group_by (can be NULL) */
            datum = SPI_getbinval(tuple, tupdesc, 1, &isnull);
            if (!isnull)
                config->group_by = pstrdup(TextDatumGetCString(datum));

            /* order_by (NULL means auto-detect) */
            datum = SPI_getbinval(tuple, tupdesc, 2, &isnull);
            if (!isnull)
            {
                config->order_by = pstrdup(TextDatumGetCString(datum));
            }

            /* delta_columns (can be NULL for auto-detect) */
            datum = SPI_getbinval(tuple, tupdesc, 3, &isnull);
            if (!isnull)
            {
                ArrayType *arr = DatumGetArrayTypeP(datum);
                Datum *elems;
                bool *elem_nulls;
                int nelems;
                int16 typlen;
                bool typbyval;
                char typalign;

                get_typlenbyvalalign(TEXTOID, &typlen, &typbyval, &typalign);
                deconstruct_array(arr, TEXTOID, typlen, typbyval, typalign,
                                  &elems, &elem_nulls, &nelems);

                if (nelems > 0)
                {
                    config->delta_columns = palloc(nelems * sizeof(char *));
                    config->delta_attnums = palloc(nelems * sizeof(AttrNumber));
                    config->num_delta_columns = nelems;

                    for (int i = 0; i < nelems; i++)
                    {
                        if (!elem_nulls[i])
                            config->delta_columns[i] = pstrdup(TextDatumGetCString(elems[i]));
                        else
                            config->delta_columns[i] = NULL;
                    }
                }
            }

            /* keyframe_every */
            datum = SPI_getbinval(tuple, tupdesc, 4, &isnull);
            if (!isnull)
                config->keyframe_every = DatumGetInt32(datum);

            /* compress_depth */
            datum = SPI_getbinval(tuple, tupdesc, 5, &isnull);
            if (!isnull)
                config->compress_depth = DatumGetInt32(datum);

            /* enable_zstd */
            datum = SPI_getbinval(tuple, tupdesc, 6, &isnull);
            if (!isnull)
                config->enable_zstd = DatumGetBool(datum);

            MemoryContextSwitchTo(oldcxt);
        }
    }
    PG_CATCH();
    {
        /* Ensure SPI is disconnected even on error */
        if (spi_connected)
            SPI_finish();
        PG_RE_THROW();
    }
    PG_END_TRY();

    SPI_finish();
    return found;
}

/*
 * Parse configuration for an xpatch table.
 * First checks xpatch.table_config catalog, then auto-detects if needed.
 */
XPatchConfig *
xpatch_parse_reloptions(Relation rel)
{
    XPatchConfig *config;
    MemoryContext oldcxt;
    Oid relid = RelationGetRelid(rel);

    /* Allocate config in TopMemoryContext so it persists in cache */
    oldcxt = MemoryContextSwitchTo(TopMemoryContext);
    config = palloc0(sizeof(XPatchConfig));

    /* Set defaults */
    config->keyframe_every = XPATCH_DEFAULT_KEYFRAME_EVERY;
    config->compress_depth = XPATCH_DEFAULT_COMPRESS_DEPTH;
    config->enable_zstd = XPATCH_DEFAULT_ENABLE_ZSTD;
    config->group_by_attnum = InvalidAttrNumber;

    MemoryContextSwitchTo(oldcxt);

    /* Try to read from catalog table (if xpatch.configure() was called) */
    read_config_from_catalog(relid, config);

    /* Auto-detect order_by if not specified */
    if (config->order_by == NULL)
    {
        auto_detect_order_by(rel, config);
    }
    else
    {
        config->order_by_attnum = resolve_column_name(rel, config->order_by);
    }

    /* Auto-detect delta_columns if not specified */
    if (config->num_delta_columns == 0)
    {
        auto_detect_delta_columns(rel, config);
    }
    else
    {
        /* Resolve column names to attnums */
        oldcxt = MemoryContextSwitchTo(TopMemoryContext);
        for (int i = 0; i < config->num_delta_columns; i++)
        {
            if (config->delta_columns[i] != NULL)
                config->delta_attnums[i] = resolve_column_name(rel, config->delta_columns[i]);
        }
        MemoryContextSwitchTo(oldcxt);
    }

    /* Resolve group_by column if specified */
    if (config->group_by)
        config->group_by_attnum = resolve_column_name(rel, config->group_by);

    /* Look for _xp_seq column */
    {
        TupleDesc tupdesc = RelationGetDescr(rel);
        int natts = tupdesc->natts;
        
        config->xp_seq_attnum = InvalidAttrNumber;
        
        for (int i = 0; i < natts; i++)
        {
            Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
            if (attr->attisdropped)
                continue;
            
            if (strcmp(NameStr(attr->attname), "_xp_seq") == 0)
            {
                if (attr->atttypid != INT4OID)
                {
                    ereport(ERROR,
                            (errcode(ERRCODE_DATATYPE_MISMATCH),
                             errmsg("xpatch: _xp_seq column must be INT (int4), found type %u",
                                    attr->atttypid)));
                }
                config->xp_seq_attnum = attr->attnum;
                break;
            }
        }
    }

    elog(DEBUG1, "xpatch: config for %s - order_by=%s, group_by=%s, delta_cols=%d, keyframe=%d, xp_seq_attnum=%d",
         RelationGetRelationName(rel),
         config->order_by ? config->order_by : "(null)",
         config->group_by ? config->group_by : "(none)",
         config->num_delta_columns,
         config->keyframe_every,
         config->xp_seq_attnum);

    return config;
}

/*
 * Get the configuration for an xpatch table
 */
XPatchConfig *
xpatch_get_config(Relation rel)
{
    ConfigCacheEntry *entry;
    Oid relid = RelationGetRelid(rel);
    bool found;

    elog(DEBUG1, "XPATCH: get_config - rel=%s (oid=%u)", 
         RelationGetRelationName(rel), relid);

    init_config_cache();

    entry = hash_search(config_cache, &relid, HASH_ENTER, &found);
    if (!found)
    {
        /* Parse and cache the config */
        entry->relid = relid;
        entry->config = xpatch_parse_reloptions(rel);
    }

    elog(DEBUG1, "XPATCH: get_config RETURNING - found_in_cache=%d, num_delta_cols=%d, order_by=%s", 
         found, entry->config->num_delta_columns, 
         entry->config->order_by ? entry->config->order_by : "(null)");
    return entry->config;
}

/*
 * Free configuration structure
 */
void
xpatch_free_config(XPatchConfig *config)
{
    if (config == NULL)
        return;

    if (config->group_by)
        pfree(config->group_by);
    if (config->order_by)
        pfree(config->order_by);
    if (config->delta_columns)
    {
        for (int i = 0; i < config->num_delta_columns; i++)
            if (config->delta_columns[i])
                pfree(config->delta_columns[i]);
        pfree(config->delta_columns);
    }
    if (config->delta_attnums)
        pfree(config->delta_attnums);

    pfree(config);
}

/*
 * Validate that a table schema is compatible with xpatch
 */
void
xpatch_validate_schema(Relation rel, XPatchConfig *config)
{
    TupleDesc tupdesc = RelationGetDescr(rel);

    /* Verify delta columns are BYTEA or compatible */
    for (int i = 0; i < config->num_delta_columns; i++)
    {
        Form_pg_attribute attr = TupleDescAttr(tupdesc,
                                               config->delta_attnums[i] - 1);
        Oid typid = attr->atttypid;

        /* Accept BYTEA, TEXT, VARCHAR, JSON, JSONB */
        if (typid != BYTEAOID && typid != TEXTOID && typid != VARCHAROID &&
            typid != JSONOID && typid != JSONBOID)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_DATATYPE_MISMATCH),
                     errmsg("delta column \"%s\" must be BYTEA, TEXT, VARCHAR, JSON, or JSONB",
                            config->delta_columns[i])));
        }
    }

    /* Verify order_by column exists and is a suitable type */
    {
        Form_pg_attribute order_attr = TupleDescAttr(tupdesc,
                                                     config->order_by_attnum - 1);
        Oid order_typid = order_attr->atttypid;

        /* Accept integer types and timestamp types */
        if (order_typid != INT2OID && order_typid != INT4OID &&
            order_typid != INT8OID && order_typid != TIMESTAMPOID &&
            order_typid != TIMESTAMPTZOID)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_DATATYPE_MISMATCH),
                     errmsg("order_by column \"%s\" must be an integer or timestamp type",
                            config->order_by)));
        }
    }
}

/*
 * Initialize xpatch reloptions.
 * No-op since we use auto-detection + catalog table instead.
 */
void
xpatch_init_reloptions(void)
{
    /* 
     * We don't register reloptions because PostgreSQL 16's table AM API
     * doesn't support custom WITH clause options. Instead, we use:
     * 1. Auto-detection for simple cases
     * 2. xpatch.table_config catalog table for explicit configuration
     */
}

/*
 * Invalidate cached config for a relation.
 * Called when a table is dropped or altered.
 */
void
xpatch_invalidate_config(Oid relid)
{
    if (config_cache != NULL)
    {
        hash_search(config_cache, &relid, HASH_REMOVE, NULL);
    }
}
