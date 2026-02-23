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
 * xpatch_warm.c - Parallel cache warming via dynamic background workers
 *
 * Implements xpatch.warm_cache_parallel(), which discovers all groups and
 * keyframe sections in an xpatch table, then distributes reconstruction
 * work across N PostgreSQL dynamic background workers.
 *
 * Key design decisions:
 *
 * 1. Work queue is lock-free: workers pull tasks via pg_atomic_fetch_add_u32
 *    on a shared counter. This is the same pattern used by xpatch_encode_pool.c.
 *
 * 2. Group values are serialized into the DSM segment so workers can
 *    reconstruct the Datum needed for index lookups and cache key computation.
 *    Pass-by-value types (INT4, etc.) are stored as raw 8 bytes; varlena
 *    types (TEXT, etc.) are stored with their full varlena header.
 *
 * 3. The leader process also participates in the work queue. If we request
 *    4 workers but only 2 BGW slots are available, we still get 3 threads
 *    of execution (2 BGW + leader).
 *
 * 4. Each worker runs as a full PostgreSQL backend with its own transaction,
 *    memory context, and buffer access. This is required because reconstruction
 *    needs buffer I/O and LWLock acquisition.
 *
 * 5. Workers populate the shared LRU content cache via xpatch_cache_put(),
 *    which is already designed for concurrent access (lock-striped).
 */

#include "xpatch_warm.h"
#include "xpatch_config.h"
#include "xpatch_storage.h"
#include "xpatch_cache.h"
#include "xpatch_tam.h"

#include "access/table.h"
#include "catalog/pg_am.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/spin.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

/* GUC variable: default number of warm cache workers */
int xpatch_warm_cache_workers = XPATCH_DEFAULT_WARM_WORKERS;

/* ================================================================
 * Forward declarations (static functions)
 * ================================================================ */

/*
 * GroupInfo - Intermediate struct holding one group's metadata during
 * the discovery phase. Collected via SPI, used to build the task list.
 */
typedef struct GroupInfo
{
    Datum           group_value;    /* The group column Datum (palloc'd copy) */
    bool            group_isnull;   /* True if NULL group (no group_by) */
    XPatchGroupHash group_hash;     /* Precomputed BLAKE3 hash */
    int64           max_seq;        /* Maximum sequence number in this group */
} GroupInfo;

static GroupInfo *discover_groups(Oid relid, XPatchConfig *config,
                                  int max_groups,
                                  int *num_groups_out);

static int build_tasks_from_groups(GroupInfo *groups, int num_groups,
                                   int keyframe_every,
                                   WarmCacheTask **tasks_out);

static Size compute_data_area_size(GroupInfo *groups, int num_groups,
                                   Oid group_typid);

static void serialize_group_values(GroupInfo *groups, int num_groups,
                                   Oid group_typid,
                                   WarmCacheTask *tasks, int num_tasks,
                                   char *data_area);

static dsm_segment *setup_dsm_segment(Oid relid, XPatchConfig *config,
                                      GroupInfo *groups, int num_groups,
                                      WarmCacheTask *tasks, int num_tasks);

static int launch_workers(dsm_segment *seg, int num_workers);

static void wait_for_workers(dsm_segment *seg, int launched);

static int64 warm_one_section(Relation rel, XPatchConfig *config,
                              Datum group_value, bool group_isnull,
                              int64 section_start, int64 section_end);

static int64 warm_sequential(Relation rel, XPatchConfig *config,
                             GroupInfo *groups, int num_groups,
                             WarmCacheTask *tasks, int num_tasks);

static void leader_process_tasks(WarmCacheHeader *header, Relation rel,
                                 XPatchConfig *config);

static Datum build_result_tuple(FunctionCallInfo fcinfo,
                                int64 rows_warmed, int32 groups_warmed,
                                int32 sections_warmed, int32 workers_used,
                                double duration_ms);

/* ================================================================
 * Discovery Phase: Enumerate groups via SPI
 * ================================================================ */

/*
 * discover_groups - Query xpatch.group_stats to find all groups for a table.
 *
 * For tables with a group_by column, queries the actual table to get distinct
 * group values (since group_stats only stores hashes, not the original values).
 * Then joins with group_stats to get max_seq per group.
 *
 * For tables without group_by, creates a single GroupInfo with a NULL group
 * value and queries group_stats for the max_seq.
 *
 * Parameters:
 *   relid          - OID of the target xpatch table
 *   config         - Table configuration (group_by, keyframe_every, etc.)
 *   max_groups     - Maximum number of groups to discover (-1 = all)
 *   num_groups_out - Output: number of groups found
 *
 * Returns a palloc'd array of GroupInfo structs.
 * Returns NULL and sets *num_groups_out = 0 if no groups found.
 */
static GroupInfo *
discover_groups(Oid relid, XPatchConfig *config,
                int max_groups, int *num_groups_out)
{
    GroupInfo      *groups = NULL;
    int             num_groups = 0;
    int             ret;
    StringInfoData  sql;
    MemoryContext   caller_cxt;

    *num_groups_out = 0;

    /*
     * Save the caller's memory context. All allocations that must survive
     * SPI_finish() (groups array, datumCopy'd values, query buffers) must
     * be done in this context, not the SPI context.
     */
    caller_cxt = CurrentMemoryContext;

    /* Allocate query buffer in caller's context before SPI_connect. */
    initStringInfo(&sql);

    ret = SPI_connect();
    if (ret != SPI_OK_CONNECT)
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("xpatch warm_cache_parallel: SPI_connect failed: %d", ret)));

    if (config->group_by == NULL)
    {
        /*
         * No group_by column: single group with NULL value.
         * Query group_stats for the max_seq of the NULL group hash.
         */
        appendStringInfo(&sql,
            "SELECT max_seq FROM xpatch.group_stats "
            "WHERE relid = %u AND group_hash = E'\\\\x00000000000000000000000000000000'",
            relid);

        ret = SPI_execute(sql.data, true, 1);
        if (ret != SPI_OK_SELECT)
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("xpatch warm_cache_parallel: SPI query failed: %d", ret)));

        if (SPI_processed > 0)
        {
            MemoryContext old_cxt;
            bool    isnull;
            Datum   d;

            d = SPI_getbinval(SPI_tuptable->vals[0],
                              SPI_tuptable->tupdesc, 1, &isnull);

            /* Allocate groups in caller's context so it survives SPI_finish */
            old_cxt = MemoryContextSwitchTo(caller_cxt);
            groups = palloc(sizeof(GroupInfo));
            MemoryContextSwitchTo(old_cxt);

            num_groups = 1;
            groups[0].group_value = (Datum) 0;
            groups[0].group_isnull = true;
            groups[0].group_hash.h1 = 0;
            groups[0].group_hash.h2 = 0;
            groups[0].max_seq = isnull ? 0 : DatumGetInt64(d);
        }
        else
        {
            /*
             * No entry in group_stats. Table might be empty or stats might
             * be stale. Fall through with num_groups = 0.
             */
        }
    }
    else
    {
        /*
         * Has group_by column. We need to get distinct group values AND their
         * max_seq. Strategy:
         *
         * Query the group_stats table joined with distinct group values from
         * the actual table. We need the real Datum values (not just hashes)
         * because workers need them for index lookups.
         *
         * We do a two-step approach:
         *   1. SELECT DISTINCT group_col FROM table [LIMIT max_groups]
         *   2. For each group, look up max_seq from group_stats by hash
         *
         * This is simpler and avoids complex join syntax with bytea hashes.
         */
        char   *schema_name;
        char   *table_name;

        schema_name = get_namespace_name(get_rel_namespace(relid));
        table_name = get_rel_name(relid);

        if (schema_name == NULL || table_name == NULL)
            ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_TABLE),
                     errmsg("xpatch warm_cache_parallel: table OID %u not found", relid)));

        appendStringInfo(&sql,
            "SELECT DISTINCT %s FROM %s.%s ORDER BY %s",
            quote_identifier(config->group_by),
            quote_identifier(schema_name),
            quote_identifier(table_name),
            quote_identifier(config->group_by));

        if (max_groups > 0)
            appendStringInfo(&sql, " LIMIT %d", max_groups);

        ret = SPI_execute(sql.data, true, 0);
        if (ret != SPI_OK_SELECT)
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("xpatch warm_cache_parallel: SPI query failed: %d", ret)));

        if (SPI_processed > 0)
        {
            MemoryContext old_cxt;
            uint64  i;
            Oid     group_typid;
            bool    typbyval;
            int16   typlen;

            num_groups = (int) SPI_processed;

            group_typid = SPI_gettypeid(SPI_tuptable->tupdesc, 1);
            get_typlenbyval(group_typid, &typlen, &typbyval);

            /*
             * Allocate the groups array and copy Datum values in the caller's
             * memory context so they survive SPI_finish().
             */
            old_cxt = MemoryContextSwitchTo(caller_cxt);
            groups = palloc(sizeof(GroupInfo) * num_groups);

            for (i = 0; i < SPI_processed; i++)
            {
                bool    isnull;
                Datum   val;

                val = SPI_getbinval(SPI_tuptable->vals[i],
                                    SPI_tuptable->tupdesc, 1, &isnull);

                /*
                 * Copy the Datum into caller's context — SPI_finish will
                 * destroy the SPI memory context and all its allocations.
                 */
                if (!isnull && !typbyval)
                    val = datumCopy(val, typbyval, typlen);

                groups[i].group_value = val;
                groups[i].group_isnull = isnull;
                groups[i].group_hash = xpatch_compute_group_hash(val, group_typid, isnull);
                groups[i].max_seq = 0;  /* filled below */
            }
            MemoryContextSwitchTo(old_cxt);

            /*
             * Now look up max_seq for each group from group_stats.
             * We query all at once for efficiency.
             */
            {
                StringInfoData  sql2;
                initStringInfo(&sql2);

                appendStringInfo(&sql2,
                    "SELECT group_hash, max_seq FROM xpatch.group_stats "
                    "WHERE relid = %u", relid);

                ret = SPI_execute(sql2.data, true, 0);
                if (ret != SPI_OK_SELECT)
                    ereport(ERROR,
                            (errcode(ERRCODE_INTERNAL_ERROR),
                             errmsg("xpatch warm_cache_parallel: group_stats query failed")));

                /*
                 * Build a lookup: for each row in group_stats, find the
                 * matching GroupInfo by comparing hashes.
                 */
                for (i = 0; i < SPI_processed; i++)
                {
                    bool    hash_null, seq_null;
                    Datum   hash_datum, seq_datum;
                    bytea  *hash_bytea;
                    XPatchGroupHash stats_hash;
                    int     j;

                    hash_datum = SPI_getbinval(SPI_tuptable->vals[i],
                                               SPI_tuptable->tupdesc, 1, &hash_null);
                    seq_datum = SPI_getbinval(SPI_tuptable->vals[i],
                                              SPI_tuptable->tupdesc, 2, &seq_null);

                    if (hash_null || seq_null)
                        continue;

                    hash_bytea = DatumGetByteaPP(hash_datum);
                    if (VARSIZE_ANY_EXHDR(hash_bytea) != sizeof(XPatchGroupHash))
                        continue;  /* Corrupted hash, skip */

                    memcpy(&stats_hash, VARDATA_ANY(hash_bytea), sizeof(XPatchGroupHash));

                    /* Find matching group by hash */
                    for (j = 0; j < num_groups; j++)
                    {
                        if (xpatch_group_hash_equals(groups[j].group_hash, stats_hash))
                        {
                            groups[j].max_seq = DatumGetInt64(seq_datum);
                            break;
                        }
                    }
                }

                pfree(sql2.data);
            }
        }
    }

    SPI_finish();
    pfree(sql.data);

    /* Remove groups with max_seq <= 0 (empty or not in stats) */
    if (groups != NULL)
    {
        int write_idx = 0;
        int i;

        for (i = 0; i < num_groups; i++)
        {
            if (groups[i].max_seq > 0)
            {
                if (write_idx != i)
                    groups[write_idx] = groups[i];
                write_idx++;
            }
        }
        num_groups = write_idx;
    }

    *num_groups_out = num_groups;
    return groups;
}

/* ================================================================
 * Task List Construction
 * ================================================================ */

/*
 * build_tasks_from_groups - Compute keyframe sections for each group and
 * build the flat array of WarmCacheTask structs.
 *
 * For a group with max_seq=S and keyframe_every=K:
 *   Section 0: [1, min(K, S)]
 *   Section 1: [K+1, min(2K, S)]
 *   ...
 *   Last section: [floor((S-1)/K)*K + 1, S]
 *
 * The group_value_off and group_value_len fields are NOT set here.
 * They are filled in by serialize_group_values() after the DSM data area
 * is allocated.
 *
 * Parameters:
 *   groups          - Array of GroupInfo structs
 *   num_groups      - Number of groups
 *   keyframe_every  - Keyframe interval from table config
 *   tasks_out       - Output: palloc'd array of WarmCacheTask
 *
 * Returns total number of tasks.
 */
static int
build_tasks_from_groups(GroupInfo *groups, int num_groups,
                        int keyframe_every, WarmCacheTask **tasks_out)
{
    int64   total_tasks_64 = 0;
    int     total_tasks;
    int     i;
    WarmCacheTask *tasks;

    /* First pass: count total tasks (use int64 to detect overflow) */
    for (i = 0; i < num_groups; i++)
    {
        int64   max_seq = groups[i].max_seq;
        int64   sections;

        Assert(max_seq > 0);
        sections = (max_seq + keyframe_every - 1) / keyframe_every;
        total_tasks_64 += sections;
    }

    if (total_tasks_64 > (int64) INT_MAX)
        ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                 errmsg("xpatch warm_cache_parallel: task count exceeds INT_MAX (%lld)",
                        (long long) total_tasks_64)));

    total_tasks = (int) total_tasks_64;

    if (total_tasks == 0)
    {
        *tasks_out = NULL;
        return 0;
    }

    tasks = palloc(sizeof(WarmCacheTask) * total_tasks);

    /* Second pass: fill in tasks */
    {
        int task_idx = 0;

        for (i = 0; i < num_groups; i++)
        {
            int64   max_seq = groups[i].max_seq;
            int64   section_start;

            for (section_start = 1; section_start <= max_seq; section_start += keyframe_every)
            {
                int64 section_end = section_start + keyframe_every - 1;
                if (section_end > max_seq)
                    section_end = max_seq;

                Assert(task_idx < total_tasks);

                tasks[task_idx].group_hash = groups[i].group_hash;
                tasks[task_idx].section_start = section_start;
                tasks[task_idx].section_end = section_end;
                tasks[task_idx].group_index = i;
                tasks[task_idx].group_value_len = 0;  /* filled by serialize_group_values */
                tasks[task_idx].group_value_off = 0;  /* filled by serialize_group_values */

                task_idx++;
            }
        }

        Assert(task_idx == total_tasks);
    }

    *tasks_out = tasks;
    return total_tasks;
}

/* ================================================================
 * Group Value Serialization into DSM
 * ================================================================ */

/*
 * compute_data_area_size - Calculate the total bytes needed in the DSM
 * data area to store serialized group values.
 *
 * Each unique group value is stored once. Multiple tasks referencing
 * the same group share the same offset.
 *
 * For pass-by-value types: sizeof(Datum) = 8 bytes per group.
 * For varlena types: VARSIZE(datum) bytes per group (MAXALIGN'd).
 * For NULL groups: 0 bytes (offset unused).
 */
static Size
compute_data_area_size(GroupInfo *groups, int num_groups, Oid group_typid)
{
    Size    total = 0;
    int     i;
    bool    typbyval;
    int16   typlen;

    if (num_groups == 0)
        return 0;

    if (groups[0].group_isnull)
        return 0;  /* NULL group, no data needed */

    get_typlenbyval(group_typid, &typlen, &typbyval);

    for (i = 0; i < num_groups; i++)
    {
        if (groups[i].group_isnull)
            continue;

        if (typbyval)
        {
            total += MAXALIGN(sizeof(Datum));
        }
        else if (typlen == -1)
        {
            /* Varlena type (TEXT, BYTEA, etc.) */
            struct varlena *val = (struct varlena *) DatumGetPointer(groups[i].group_value);
            Size    vsize = VARSIZE_ANY(val);

            total += MAXALIGN(vsize);
        }
        else if (typlen == -2)
        {
            /* C-string type — compute length from null terminator */
            char *cstr = DatumGetCString(groups[i].group_value);
            Size  slen = strlen(cstr) + 1;
            total += MAXALIGN(slen);
        }
        else
        {
            /* Fixed-length pass-by-reference (UUID, POINT, MACADDR, etc.) */
            Assert(typlen > 0);
            total += MAXALIGN(typlen);
        }
    }

    return total;
}

/*
 * serialize_group_values - Write group Datum values into the DSM data area
 * and update each WarmCacheTask's group_value_off and group_value_len.
 *
 * Each group's value is written once at a MAXALIGN'd offset. All tasks
 * belonging to the same group (same group_index) share the same offset.
 *
 * Parameters:
 *   groups      - Array of GroupInfo structs
 *   num_groups  - Number of groups
 *   group_typid - Type OID of the group column
 *   tasks       - Array of WarmCacheTask (modified in place)
 *   num_tasks   - Number of tasks
 *   data_area   - Pointer to the DSM data area
 */
static void
serialize_group_values(GroupInfo *groups, int num_groups,
                       Oid group_typid,
                       WarmCacheTask *tasks, int num_tasks,
                       char *data_area)
{
    int    *group_offsets;  /* Offset for each group in the data area */
    int    *group_lens;     /* Length sentinel for each group */
    int     write_pos = 0;
    int     i;
    bool    typbyval;
    int16   typlen;

    group_offsets = palloc(sizeof(int) * num_groups);
    group_lens = palloc(sizeof(int) * num_groups);

    if (num_groups > 0 && !groups[0].group_isnull)
        get_typlenbyval(group_typid, &typlen, &typbyval);
    else
    {
        typbyval = false;
        typlen = 0;
    }

    /* Serialize each group value */
    for (i = 0; i < num_groups; i++)
    {
        if (groups[i].group_isnull)
        {
            group_offsets[i] = 0;
            group_lens[i] = WARM_GROUP_VALUE_NULL;
            continue;
        }

        if (typbyval)
        {
            group_offsets[i] = write_pos;
            group_lens[i] = WARM_GROUP_VALUE_BYVAL;
            memcpy(data_area + write_pos, &groups[i].group_value, sizeof(Datum));
            write_pos += MAXALIGN(sizeof(Datum));
        }
        else if (typlen == -1)
        {
            /* Varlena type (TEXT, BYTEA, etc.) */
            struct varlena *val = (struct varlena *) DatumGetPointer(groups[i].group_value);
            Size    vsize = VARSIZE_ANY(val);

            group_offsets[i] = write_pos;
            group_lens[i] = (int32) vsize;
            memcpy(data_area + write_pos, val, vsize);
            write_pos += MAXALIGN(vsize);
        }
        else if (typlen == -2)
        {
            /* C-string type */
            char *cstr = DatumGetCString(groups[i].group_value);
            Size  slen = strlen(cstr) + 1;

            group_offsets[i] = write_pos;
            group_lens[i] = WARM_GROUP_VALUE_FIXEDLEN;
            memcpy(data_area + write_pos, cstr, slen);
            write_pos += MAXALIGN(slen);
        }
        else
        {
            /* Fixed-length pass-by-reference (UUID, POINT, MACADDR, etc.) */
            Assert(typlen > 0);
            group_offsets[i] = write_pos;
            group_lens[i] = WARM_GROUP_VALUE_FIXEDLEN;
            memcpy(data_area + write_pos,
                   DatumGetPointer(groups[i].group_value),
                   typlen);
            write_pos += MAXALIGN(typlen);
        }
    }

    /* Update tasks with their group's offset and length */
    for (i = 0; i < num_tasks; i++)
    {
        int gi = tasks[i].group_index;
        Assert(gi >= 0 && gi < num_groups);

        tasks[i].group_value_off = group_offsets[gi];
        tasks[i].group_value_len = group_lens[gi];
    }

    pfree(group_offsets);
    pfree(group_lens);
}

/* ================================================================
 * DSM Segment Setup
 * ================================================================ */

/*
 * setup_dsm_segment - Create and populate the DSM segment for the work queue.
 *
 * Allocates a DSM segment containing:
 *   1. WarmCacheHeader (with atomic counters initialized)
 *   2. WarmCacheTask[num_tasks] (task array)
 *   3. Group value data area (serialized Datums)
 *
 * All offsets are MAXALIGN'd for proper alignment.
 *
 * Parameters:
 *   relid       - Target table OID
 *   config      - Table configuration
 *   groups      - Array of GroupInfo structs
 *   num_groups  - Number of groups
 *   tasks       - Array of WarmCacheTask (offsets are filled in by this function)
 *   num_tasks   - Number of tasks
 *
 * Returns the DSM segment. Caller is responsible for dsm_detach().
 */
static dsm_segment *
setup_dsm_segment(Oid relid, XPatchConfig *config,
                  GroupInfo *groups, int num_groups,
                  WarmCacheTask *tasks, int num_tasks)
{
    dsm_segment    *seg;
    Size            header_size;
    Size            tasks_size;
    Size            data_area_size;
    Size            total_size;
    char           *base;
    WarmCacheHeader *header;
    WarmCacheTask  *dsm_tasks;
    char           *dsm_data_area;
    Oid             group_typid = InvalidOid;

    /* Determine group type OID */
    if (config->group_by != NULL)
    {
        /* Get group type from the first non-null group */
        if (num_groups > 0 && !groups[0].group_isnull)
        {
            /*
             * We need the actual type OID. Since we don't store it in GroupInfo,
             * look it up from the config's group_by_attnum and the relation.
             */
            Relation rel = table_open(relid, AccessShareLock);
            TupleDesc tupdesc = RelationGetDescr(rel);
            AttrNumber attnum = config->group_by_attnum;

            if (attnum != InvalidAttrNumber && attnum > 0 && attnum <= tupdesc->natts)
            {
                Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);
                group_typid = attr->atttypid;
            }
            table_close(rel, AccessShareLock);
        }
    }

    /* Calculate sizes */
    header_size = MAXALIGN(sizeof(WarmCacheHeader));
    tasks_size = MAXALIGN(sizeof(WarmCacheTask) * num_tasks);
    data_area_size = compute_data_area_size(groups, num_groups, group_typid);
    total_size = header_size + tasks_size + MAXALIGN(data_area_size);

    /* Create DSM segment */
    seg = dsm_create(total_size, 0);
    base = dsm_segment_address(seg);
    memset(base, 0, total_size);

    /* Initialize header */
    header = (WarmCacheHeader *) base;
    header->dboid = MyDatabaseId;
    header->relid = relid;
    header->userid = GetUserId();
    header->group_typid = group_typid;
    if (group_typid != InvalidOid)
        get_typlenbyval(group_typid, &header->group_typlen, &header->group_typbyval);
    else
    {
        header->group_typlen = 0;
        header->group_typbyval = false;
    }
    header->num_tasks = num_tasks;
    header->num_groups = num_groups;
    header->keyframe_every = config->keyframe_every;
    header->num_delta_columns = config->num_delta_columns;
    header->compress_depth = config->compress_depth;
    header->enable_zstd = config->enable_zstd;
    header->has_group_by = (config->group_by != NULL);
    header->tasks_offset = (int32) header_size;
    header->data_area_offset = (int32) (header_size + tasks_size);
    header->data_area_size = (int32) data_area_size;

    /* Initialize atomic counters */
    pg_atomic_init_u32(&header->next_task, 0);
    pg_atomic_init_u64(&header->total_rows_warmed, 0);
    pg_atomic_init_u32(&header->workers_done, 0);
    pg_atomic_init_u32(&header->has_error, 0);
    SpinLockInit(&header->error_lock);

    /* Serialize group values into data area */
    dsm_data_area = base + header->data_area_offset;
    serialize_group_values(groups, num_groups, group_typid,
                           tasks, num_tasks, dsm_data_area);

    /* Copy task array into DSM */
    dsm_tasks = (WarmCacheTask *) (base + header->tasks_offset);
    memcpy(dsm_tasks, tasks, sizeof(WarmCacheTask) * num_tasks);

    return seg;
}

/* ================================================================
 * Section Warming: The Hot Loop
 * ================================================================ */

/*
 * warm_one_section - Reconstruct all rows in a keyframe section and
 * populate the shared LRU content cache.
 *
 * For each sequence number from section_start to section_end:
 *   1. For each delta column, call xpatch_reconstruct_column().
 *   2. xpatch_reconstruct_column() checks the cache first (O(1) hit).
 *      On miss, it fetches the tuple, walks the delta chain back to the
 *      nearest keyframe, reconstructs, and calls xpatch_cache_put().
 *
 * Because we process rows in sequence order starting from the keyframe,
 * each row's base is already in cache from the previous iteration.
 * This means reconstruction is O(1) per row after the keyframe.
 *
 * Memory management: Uses a per-row temporary memory context to prevent
 * unbounded growth. Each row's reconstructed content is palloc'd in this
 * context and freed after xpatch_cache_put() copies it to shared memory.
 *
 * Parameters:
 *   rel           - Open relation (with at least AccessShareLock)
 *   config        - Table configuration
 *   group_value   - Group column Datum (or 0 if no group_by)
 *   group_isnull  - True if group value is NULL
 *   section_start - First sequence number in this section
 *   section_end   - Last sequence number in this section (inclusive)
 *
 * Returns the number of rows successfully warmed.
 */
static int64
warm_one_section(Relation rel, XPatchConfig *config,
                 Datum group_value, bool group_isnull,
                 int64 section_start, int64 section_end)
{
    int64           rows_warmed = 0;
    int64           seq;
    int             j;
    MemoryContext    section_ctx;
    MemoryContext    old_ctx;

    section_ctx = AllocSetContextCreate(CurrentMemoryContext,
                                        "xpatch warm section",
                                        ALLOCSET_DEFAULT_SIZES);

    for (seq = section_start; seq <= section_end; seq++)
    {
        MemoryContext row_ctx;

        /* Per-row temp context to avoid memory bloat */
        row_ctx = AllocSetContextCreate(section_ctx,
                                        "xpatch warm row",
                                        ALLOCSET_SMALL_SIZES);
        old_ctx = MemoryContextSwitchTo(row_ctx);

        /*
         * Reconstruct each delta column for this seq.
         *
         * xpatch_reconstruct_column() handles everything:
         *   - Cache check (returns immediately if already cached)
         *   - Tuple fetch (via seq-to-TID cache, index, or scan)
         *   - Delta chain walk back to nearest keyframe
         *   - Cache population via xpatch_cache_put()
         *
         * We call it for every delta column to ensure the full row
         * is cached, not just some columns.
         */
        for (j = 0; j < config->num_delta_columns; j++)
        {
            bytea *result;

            result = xpatch_reconstruct_column(rel, config,
                                                group_value, seq, j);

            /*
             * Result may be NULL if the row doesn't exist (e.g., deleted
             * between discovery and warming). This is not an error — just
             * skip and move on.
             */
            if (result != NULL)
                pfree(result);  /* Content is now in shared cache */
        }

        MemoryContextSwitchTo(old_ctx);
        MemoryContextDelete(row_ctx);

        rows_warmed++;
    }

    MemoryContextDelete(section_ctx);
    return rows_warmed;
}

/* ================================================================
 * Sequential Fallback Path
 * ================================================================ */

/*
 * warm_sequential - Process all tasks sequentially in the leader process.
 *
 * Used when max_workers=0 or no BGW slots are available.
 * Still much faster than PL/pgSQL because we call the C reconstruction
 * functions directly without executor/SPI overhead.
 *
 * Calls CHECK_FOR_INTERRUPTS() between tasks to allow cancellation.
 *
 * Parameters:
 *   rel         - Open relation
 *   config      - Table configuration
 *   groups      - Array of GroupInfo structs
 *   num_groups  - Number of groups
 *   tasks       - Array of WarmCacheTask
 *   num_tasks   - Number of tasks
 *
 * Returns total number of rows warmed.
 */
static int64
warm_sequential(Relation rel, XPatchConfig *config,
                GroupInfo *groups, int num_groups,
                WarmCacheTask *tasks, int num_tasks)
{
    int64   total_rows = 0;
    int     i;

    for (i = 0; i < num_tasks; i++)
    {
        WarmCacheTask  *task = &tasks[i];
        int             gi = task->group_index;

        Assert(gi >= 0 && gi < num_groups);

        total_rows += warm_one_section(rel, config,
                                        groups[gi].group_value,
                                        groups[gi].group_isnull,
                                        task->section_start,
                                        task->section_end);

        CHECK_FOR_INTERRUPTS();
    }

    return total_rows;
}

/* ================================================================
 * Background Worker Management
 * ================================================================ */

/*
 * launch_workers - Register and start N dynamic background workers.
 *
 * Each worker is configured to:
 *   - Attach to the DSM segment (via bgw_main_arg)
 *   - Connect to the same database as the leader
 *   - Run xpatch_warm_worker_main() as its entry point
 *   - Never restart on exit (one-shot)
 *   - Notify the leader (via SIGUSR1) on exit
 *
 * If a worker cannot be registered (e.g., max_worker_processes exhausted),
 * we stop registering and return however many we launched. The leader can
 * then participate in the work queue itself to compensate.
 *
 * Parameters:
 *   seg         - DSM segment containing the work queue
 *   num_workers - Desired number of workers
 *
 * Returns the actual number of workers successfully launched.
 */
static int
launch_workers(dsm_segment *seg, int num_workers)
{
    int     launched = 0;
    int     i;

    for (i = 0; i < num_workers; i++)
    {
        BackgroundWorker        worker;
        BackgroundWorkerHandle *handle;

        memset(&worker, 0, sizeof(worker));

        snprintf(worker.bgw_name, BGW_MAXLEN,
                 "xpatch warm cache worker %d", i);
        snprintf(worker.bgw_type, BGW_MAXLEN,
                 "xpatch warm cache worker");
        snprintf(worker.bgw_function_name, BGW_MAXLEN,
                 "xpatch_warm_worker_main");
        snprintf(worker.bgw_library_name, BGW_MAXLEN,
                 "pg_xpatch");

        worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
                           BGWORKER_BACKEND_DATABASE_CONNECTION;
        worker.bgw_start_time = BgWorkerStart_ConsistentState;
        worker.bgw_restart_time = BGW_NEVER_RESTART;
        worker.bgw_main_arg = UInt32GetDatum(dsm_segment_handle(seg));
        worker.bgw_notify_pid = MyProcPid;

        if (!RegisterDynamicBackgroundWorker(&worker, &handle))
        {
            elog(DEBUG1, "xpatch warm_cache_parallel: could not register "
                         "worker %d (max_worker_processes may be exhausted)", i);
            break;
        }

        /*
         * Wait for worker to start. If the postmaster refuses to start it
         * (e.g., too many workers), we get BGWH_STOPPED immediately.
         */
        {
            BgwHandleStatus status;
            pid_t           pid;

            status = WaitForBackgroundWorkerStartup(handle, &pid);
            if (status != BGWH_STARTED)
            {
                elog(DEBUG1, "xpatch warm_cache_parallel: worker %d did not start "
                             "(status %d)", i, status);
                break;
            }
        }

        launched++;
    }

    if (launched < num_workers)
    {
        elog(NOTICE, "xpatch warm_cache_parallel: launched %d of %d requested workers "
                     "(max_worker_processes may need increasing)", launched, num_workers);
    }

    return launched;
}

/*
 * wait_for_workers - Wait for all launched background workers to finish.
 *
 * Uses the DSM segment's workers_done counter as the primary completion
 * signal. We poll with WaitLatch() to be responsive to interrupts.
 *
 * If the postmaster dies while we're waiting, we detect it and error out.
 *
 * Parameters:
 *   seg      - DSM segment containing the work queue
 *   launched - Number of workers that were launched
 */
static void
wait_for_workers(dsm_segment *seg, int launched)
{
    WarmCacheHeader *header = (WarmCacheHeader *) dsm_segment_address(seg);

    for (;;)
    {
        uint32  done;

        CHECK_FOR_INTERRUPTS();

        done = pg_atomic_read_u32(&header->workers_done);
        if ((int) done >= launched)
            break;

        /*
         * Wait on our latch with a timeout. Workers send SIGUSR1 to
         * MyProcPid (via bgw_notify_pid) when they exit, which sets
         * our latch.
         */
        (void) WaitLatch(MyLatch,
                         WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                         100,  /* 100ms timeout for responsiveness */
                         WAIT_EVENT_BGWORKER_SHUTDOWN);
        ResetLatch(MyLatch);
    }
}

/* ================================================================
 * Leader Task Processing
 * ================================================================ */

/*
 * leader_process_tasks - The leader participates in the work queue,
 * grabbing and processing tasks just like the BGW workers.
 *
 * This maximizes parallelism: if we have 3 BGW workers + leader,
 * we effectively have 4 threads of execution.
 *
 * The leader uses the same atomic counter as the workers.
 * Calls CHECK_FOR_INTERRUPTS() between tasks for cancel support.
 *
 * Parameters:
 *   header  - DSM header (for atomic counter and task array)
 *   rel     - Open relation
 *   config  - Table configuration
 */
static void
leader_process_tasks(WarmCacheHeader *header, Relation rel,
                     XPatchConfig *config)
{
    WarmCacheTask  *tasks = warm_get_tasks(header);

    for (;;)
    {
        uint32      task_id;
        WarmCacheTask *task;
        Datum       group_value;
        bool        group_isnull;
        int64       rows;

        task_id = pg_atomic_fetch_add_u32(&header->next_task, 1);
        if (task_id >= (uint32) header->num_tasks)
            break;  /* No more work */

        task = &tasks[task_id];
        group_value = warm_get_group_datum(header, task, &group_isnull);

        rows = warm_one_section(rel, config,
                                group_value, group_isnull,
                                task->section_start, task->section_end);

        pg_atomic_fetch_add_u64(&header->total_rows_warmed, (uint64) rows);

        CHECK_FOR_INTERRUPTS();
    }
}

/* ================================================================
 * Result Tuple Construction
 * ================================================================ */

/*
 * build_result_tuple - Construct the return tuple for warm_cache_parallel().
 *
 * Returns a single row with columns:
 *   rows_warmed     BIGINT
 *   groups_warmed   BIGINT
 *   sections_warmed BIGINT
 *   workers_used    INT
 *   duration_ms     FLOAT8
 */
static Datum
build_result_tuple(FunctionCallInfo fcinfo,
                   int64 rows_warmed, int32 groups_warmed,
                   int32 sections_warmed, int32 workers_used,
                   double duration_ms)
{
    TupleDesc       tupdesc;
    Datum           values[5];
    bool            nulls[5];
    HeapTuple       tuple;

    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("function returning record called in context "
                        "that cannot accept type record")));

    tupdesc = BlessTupleDesc(tupdesc);

    memset(nulls, 0, sizeof(nulls));

    values[0] = Int64GetDatum(rows_warmed);
    values[1] = Int64GetDatum((int64) groups_warmed);
    values[2] = Int64GetDatum((int64) sections_warmed);
    values[3] = Int32GetDatum(workers_used);
    values[4] = Float8GetDatum(duration_ms);

    tuple = heap_form_tuple(tupdesc, values, nulls);
    return HeapTupleGetDatum(tuple);
}

/* ================================================================
 * SQL-Callable Function: xpatch.warm_cache_parallel()
 * ================================================================ */

PG_FUNCTION_INFO_V1(xpatch_warm_cache_parallel);

/*
 * xpatch_warm_cache_parallel - Parallel cache warming using background workers.
 *
 * SQL signature:
 *   xpatch.warm_cache_parallel(
 *       table_name  REGCLASS,
 *       max_workers INT DEFAULT NULL,   -- NULL = use GUC default
 *       max_groups  INT DEFAULT NULL    -- NULL = all groups
 *   ) RETURNS TABLE (
 *       rows_warmed     BIGINT,
 *       groups_warmed   BIGINT,
 *       sections_warmed BIGINT,
 *       workers_used    INT,
 *       duration_ms     FLOAT8
 *   )
 *
 * Algorithm:
 *   1. Validate inputs (privileges, xpatch AM, parameter ranges).
 *   2. Load table config and discover groups via SPI.
 *   3. Build task list (one task per keyframe section per group).
 *   4. If max_workers=0 or only 1 task: run sequentially in C.
 *   5. Otherwise: create DSM, launch BGW workers, leader participates,
 *      wait for completion, collect stats.
 *   6. Return result tuple.
 */
Datum
xpatch_warm_cache_parallel(PG_FUNCTION_ARGS)
{
    Oid             relid;
    int             max_workers;
    int             max_groups;
    TimestampTz     start_time;
    Relation        rel;
    XPatchConfig   *config;
    GroupInfo      *groups;
    int             num_groups;
    WarmCacheTask  *tasks;
    int             num_tasks;
    int64           total_rows;
    int32           workers_used = 0;
    double          duration_ms;
    AclResult       aclresult;

    /* --- Parse arguments --- */

    if (PG_ARGISNULL(0))
        ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                 errmsg("table_name must not be NULL")));

    relid = PG_GETARG_OID(0);

    if (PG_ARGISNULL(1))
        max_workers = xpatch_warm_cache_workers;
    else
        max_workers = PG_GETARG_INT32(1);

    if (PG_ARGISNULL(2))
        max_groups = -1;  /* -1 = no limit */
    else
    {
        max_groups = PG_GETARG_INT32(2);
        if (max_groups < 0)
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("max_groups must be non-negative, got %d", max_groups)));
    }

    start_time = GetCurrentTimestamp();

    /* --- Input validation --- */

    if (max_workers < 0)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("max_workers must be non-negative, got %d", max_workers)));

    /* No artificial cap — PostgreSQL's max_worker_processes is the real limit */

    /* --- Privilege check --- */

    aclresult = pg_class_aclcheck(relid, GetUserId(), ACL_SELECT);
    if (aclresult != ACLCHECK_OK)
        aclcheck_error(aclresult, OBJECT_TABLE,
                       get_rel_name(relid));

    /* --- Open relation and verify it uses xpatch AM --- */

    rel = table_open(relid, AccessShareLock);

    if (rel->rd_tableam != xpatch_get_table_am_routine())
    {
        table_close(rel, AccessShareLock);
        ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                 errmsg("table \"%s\" is not using the xpatch access method",
                        RelationGetRelationName(rel))));
    }

    config = xpatch_get_config(rel);

    /* --- Early exit for max_groups=0 --- */

    if (max_groups == 0)
    {
        table_close(rel, AccessShareLock);
        duration_ms = (double) (GetCurrentTimestamp() - start_time) / 1000.0;
        PG_RETURN_DATUM(build_result_tuple(fcinfo, 0, 0, 0, 0, duration_ms));
    }

    /* --- Discovery: enumerate groups and their max_seq --- */

    groups = discover_groups(relid, config, max_groups, &num_groups);

    if (num_groups == 0)
    {
        /* No groups found — empty table or stale stats */
        table_close(rel, AccessShareLock);
        duration_ms = (double) (GetCurrentTimestamp() - start_time) / 1000.0;
        PG_RETURN_DATUM(build_result_tuple(fcinfo, 0, 0, 0, 0, duration_ms));
    }

    /* --- Build task list from groups --- */

    num_tasks = build_tasks_from_groups(groups, num_groups,
                                        config->keyframe_every, &tasks);

    if (num_tasks == 0)
    {
        table_close(rel, AccessShareLock);
        duration_ms = (double) (GetCurrentTimestamp() - start_time) / 1000.0;
        PG_RETURN_DATUM(build_result_tuple(fcinfo, 0, num_groups, 0, 0, duration_ms));
    }

    /* --- Execute: parallel or sequential --- */

    if (max_workers == 0 || num_tasks <= 1)
    {
        /*
         * Sequential path: no background workers.
         * Still much faster than PL/pgSQL — direct C reconstruction calls.
         */
        total_rows = warm_sequential(rel, config, groups, num_groups,
                                      tasks, num_tasks);
        workers_used = 0;
    }
    else
    {
        /* Parallel path: create DSM, launch workers, participate, wait */
        dsm_segment    *seg;
        int             launched;
        WarmCacheHeader *header;

        seg = setup_dsm_segment(relid, config, groups, num_groups,
                                tasks, num_tasks);
        header = (WarmCacheHeader *) dsm_segment_address(seg);

        /* Cap workers at task count (no point having idle workers) */
        launched = launch_workers(seg, Min(max_workers, num_tasks));

        if (launched == 0)
        {
            /*
             * No BGW slots available. Fall back to sequential C warming.
             * The DSM is wasted but we clean it up below.
             */
            elog(NOTICE, "xpatch warm_cache_parallel: no background workers available, "
                         "falling back to sequential warming");
            total_rows = warm_sequential(rel, config, groups, num_groups,
                                          tasks, num_tasks);
            workers_used = 0;
        }
        else
        {
            /*
             * Leader participates in the work queue alongside the BGWs.
             * This means with N launched workers, we have N+1 effective
             * threads of execution.
             */
            leader_process_tasks(header, rel, config);

            /* Wait for all BGWs to finish */
            wait_for_workers(seg, launched);

            /* Collect stats from DSM */
            total_rows = (int64) pg_atomic_read_u64(&header->total_rows_warmed);
            workers_used = launched;

            /* Check for worker errors */
            if (pg_atomic_read_u32(&header->has_error) != 0)
            {
                ereport(WARNING,
                        (errmsg("xpatch warm_cache_parallel: worker error: %s",
                                header->error_message)));
            }
        }

        dsm_detach(seg);
    }

    table_close(rel, AccessShareLock);

    /* --- Build and return result --- */

    duration_ms = (double) (GetCurrentTimestamp() - start_time) / 1000.0;

    PG_RETURN_DATUM(build_result_tuple(fcinfo,
                                       total_rows,
                                       num_groups,
                                       num_tasks,
                                       workers_used,
                                       duration_ms));
}

/* ================================================================
 * Background Worker Entry Point
 * ================================================================ */

/*
 * xpatch_warm_worker_main - Entry point for dynamic background workers.
 *
 * Each worker:
 *   1. Attaches to the DSM segment (handle passed via bgw_main_arg).
 *   2. Connects to the database using the leader's DB OID and user OID.
 *   3. Starts a read-only transaction with a fresh snapshot.
 *   4. Opens the target relation with AccessShareLock.
 *   5. Loads the table configuration.
 *   6. Pulls tasks from the shared work queue via atomic counter.
 *   7. For each task: warms all rows in the keyframe section.
 *   8. Cleans up: closes relation, commits transaction, detaches DSM.
 *   9. Signals completion via the workers_done counter.
 *
 * Error handling:
 *   - All work is wrapped in PG_TRY/PG_CATCH.
 *   - On error, the worker writes the error message to the DSM header
 *     (under spinlock) and sets the has_error flag.
 *   - The worker then exits with proc_exit(1).
 *   - The leader detects the error via has_error and reports it as WARNING.
 *
 * This function is marked PGDLLEXPORT so it can be found by name in
 * the shared library when the postmaster launches the worker.
 */
void
xpatch_warm_worker_main(Datum main_arg)
{
    dsm_segment    *seg;
    WarmCacheHeader *header;
    WarmCacheTask  *tasks;
    Relation        rel;
    XPatchConfig   *config;

    /* Attach to DSM segment */
    seg = dsm_attach(DatumGetUInt32(main_arg));
    if (seg == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("xpatch warm worker: could not attach to DSM segment")));

    /*
     * Pin the DSM mapping so that AbortCurrentTransaction() won't detach
     * it automatically via resource owner cleanup. We manage the lifecycle
     * explicitly with dsm_detach() in both success and error paths.
     */
    dsm_pin_mapping(seg);

    header = (WarmCacheHeader *) dsm_segment_address(seg);
    tasks = warm_get_tasks(header);

    /* Connect to the database */
    BackgroundWorkerInitializeConnectionByOid(header->dboid, header->userid, 0);

    /* Start transaction (needed for buffer access, snapshot, relation open) */
    StartTransactionCommand();
    PushActiveSnapshot(GetTransactionSnapshot());

    PG_TRY();
    {
        /* Open relation */
        rel = table_open(header->relid, AccessShareLock);
        config = xpatch_get_config(rel);

        /* Process tasks from the shared work queue */
        for (;;)
        {
            uint32          task_id;
            WarmCacheTask  *task;
            Datum           group_value;
            bool            group_isnull;
            int64           rows;

            CHECK_FOR_INTERRUPTS();

            task_id = pg_atomic_fetch_add_u32(&header->next_task, 1);
            if (task_id >= (uint32) header->num_tasks)
                break;  /* No more work */

            task = &tasks[task_id];
            group_value = warm_get_group_datum(header, task, &group_isnull);

            rows = warm_one_section(rel, config,
                                    group_value, group_isnull,
                                    task->section_start, task->section_end);

            pg_atomic_fetch_add_u64(&header->total_rows_warmed, (uint64) rows);
        }

        /* Cleanup: close relation */
        table_close(rel, AccessShareLock);
    }
    PG_CATCH();
    {
        /*
         * On error: record the error message in DSM for the leader to report.
         * Use spinlock because multiple workers could error simultaneously.
         */
        ErrorData *edata = CopyErrorData();

        SpinLockAcquire(&header->error_lock);
        if (pg_atomic_read_u32(&header->has_error) == 0)
        {
            pg_atomic_write_u32(&header->has_error, 1);
            strlcpy(header->error_message,
                    edata->message ? edata->message : "unknown error",
                    sizeof(header->error_message));
        }
        SpinLockRelease(&header->error_lock);

        FreeErrorData(edata);
        FlushErrorState();

        /* Still need to signal completion */
        pg_atomic_fetch_add_u32(&header->workers_done, 1);

        PopActiveSnapshot();
        AbortCurrentTransaction();
        dsm_detach(seg);
        proc_exit(1);
    }
    PG_END_TRY();

    /* Normal completion */
    PopActiveSnapshot();
    CommitTransactionCommand();

    /* Signal completion */
    pg_atomic_fetch_add_u32(&header->workers_done, 1);

    dsm_detach(seg);
    proc_exit(0);
}
