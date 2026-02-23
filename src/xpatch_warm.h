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
 * xpatch_warm.h - Parallel cache warming via dynamic background workers
 *
 * Provides xpatch.warm_cache_parallel(), a C implementation of cache warming
 * that discovers groups and keyframe sections, then distributes reconstruction
 * work across N PostgreSQL dynamic background workers.
 *
 * Architecture:
 *   1. Leader (caller's backend) queries xpatch.group_stats to discover all
 *      groups and their max_seq values.
 *   2. For each group, keyframe sections are computed:
 *      [1..kf], [kf+1..2*kf], ... up to max_seq.
 *   3. A DSM (Dynamic Shared Memory) segment is created containing a header
 *      and an array of WarmCacheTask structs.
 *   4. N dynamic background workers are launched. Each worker attaches to
 *      the DSM, connects to the database, opens the relation, and pulls
 *      tasks from a shared work queue via atomic_fetch_add (lock-free).
 *   5. Each task = one keyframe section in one group. The worker reconstructs
 *      every row in the section, populating the shared LRU content cache.
 *   6. The leader also participates in the work queue (pulling tasks just
 *      like the workers) for maximum utilization.
 *   7. After the queue is drained and all workers exit, the leader collects
 *      stats from the DSM and returns results to the caller.
 *
 * Fallback:
 *   If no BGW slots are available (max_worker_processes exhausted), the
 *   leader falls back to sequential C warming. This is still much faster
 *   than the PL/pgSQL warm_cache() because it avoids executor overhead.
 */

#ifndef XPATCH_WARM_H
#define XPATCH_WARM_H

#include "pg_xpatch.h"
#include "xpatch_hash.h"

#include "fmgr.h"
#include "storage/dsm.h"

/* --- Constants --- */

/* Default number of background workers for warm_cache_parallel */
#define XPATCH_DEFAULT_WARM_WORKERS     4

/* --- DSM Structures --- */

/*
 * WarmCacheTask - One unit of work in the shared work queue.
 *
 * Represents a single keyframe section within a single group.
 * Workers grab these via atomic_fetch_add on the header's next_task counter.
 *
 * Each task is self-contained: it carries the group hash, group Datum
 * serialization info, and the seq range to warm.
 */
typedef struct WarmCacheTask
{
    XPatchGroupHash group_hash;         /* 128-bit BLAKE3 hash of group value */
    int64           section_start;      /* First seq in this keyframe section */
    int64           section_end;        /* Last seq in this keyframe section (inclusive) */
    int32           group_index;        /* Index of this group (0-based, for counting) */

    /*
     * Group value serialization.
     *
     * For pass-by-value types (INT2/4/8, OID, BOOL, etc.):
     *   group_value_len = -2 (sentinel)
     *   group_value_off points to 8 bytes containing the raw Datum.
     *
     * For varlena types (TEXT, BYTEA, etc.):
     *   group_value_len = VARSIZE(datum) (includes varlena header)
     *   group_value_off points to the full varlena bytes.
     *
     * For NULL groups (no group_by column):
     *   group_value_len = -1
     *   group_value_off = 0 (unused)
     */
    int32           group_value_len;    /* Serialized group value length */
    int32           group_value_off;    /* Offset into DSM data area */
} WarmCacheTask;

/*
 * Sentinel values for group_value_len in WarmCacheTask.
 *
 * group_value_len >= 0 : varlena type, len = VARSIZE (includes header)
 * WARM_GROUP_VALUE_NULL   : NULL group (no group_by or null value)
 * WARM_GROUP_VALUE_BYVAL  : pass-by-value type (INT2/4/8, OID, BOOL, etc.)
 * WARM_GROUP_VALUE_FIXEDLEN: fixed-length pass-by-reference type (UUID, etc.)
 *                           The actual length is stored in the header's
 *                           group_typlen field.
 */
#define WARM_GROUP_VALUE_NULL       (-1)
#define WARM_GROUP_VALUE_BYVAL      (-2)
#define WARM_GROUP_VALUE_FIXEDLEN   (-3)

/*
 * WarmCacheHeader - Header at the start of the DSM segment.
 *
 * Contains all metadata needed by workers, plus atomic counters for
 * lock-free task dispatch and stats accumulation.
 *
 * Memory layout of the DSM segment:
 *   [WarmCacheHeader]                              -- MAXALIGN'd
 *   [WarmCacheTask[num_tasks]]                     -- MAXALIGN'd
 *   [Group Value Data Area]                        -- variable length
 *
 * Workers locate the task array and data area using the helper functions
 * warm_get_tasks() and warm_get_data_area().
 */
typedef struct WarmCacheHeader
{
    /* --- Relation and database identity --- */
    Oid             dboid;              /* Database OID (workers connect here) */
    Oid             relid;              /* Target table OID */
    Oid             userid;             /* Launching user's OID (for privilege check) */
    Oid             group_typid;        /* Type OID of the group_by column */
    int16           group_typlen;       /* Type length of group_by column */
    bool            group_typbyval;     /* True if group_by type is pass-by-value */

    /* --- Table configuration (copied from XPatchConfig) --- */
    int32           num_tasks;          /* Total number of tasks in the queue */
    int32           num_groups;         /* Total distinct groups discovered */
    int32           keyframe_every;     /* From table config */
    int32           num_delta_columns;  /* Number of delta-compressed columns */
    int32           compress_depth;     /* From table config */
    bool            enable_zstd;        /* zstd enabled in config */
    bool            has_group_by;       /* True if table has a group_by column */

    /* --- DSM layout offsets --- */
    int32           tasks_offset;       /* Byte offset to WarmCacheTask array */
    int32           data_area_offset;   /* Byte offset to group value data area */
    int32           data_area_size;     /* Size of the group value data area */

    /* --- Lock-free work dispatch --- */
    pg_atomic_uint32 next_task;         /* Next task index to grab (0-based) */

    /* --- Aggregated stats (atomically updated by workers) --- */
    pg_atomic_uint64 total_rows_warmed; /* Total rows reconstructed + cached */
    pg_atomic_uint32 workers_done;      /* Number of workers that have finished */

    /* --- Error reporting --- */
    pg_atomic_uint32 has_error;         /* 1 if any worker encountered an error */
    char            error_message[256]; /* First error message (best-effort) */
    slock_t         error_lock;         /* Spinlock protecting error fields */
} WarmCacheHeader;

/* --- DSM accessor helpers --- */

/*
 * Get a pointer to the task array within the DSM segment.
 * The task array starts at header->tasks_offset bytes from the header.
 */
static inline WarmCacheTask *
warm_get_tasks(WarmCacheHeader *header)
{
    return (WarmCacheTask *) ((char *) header + header->tasks_offset);
}

/*
 * Get a pointer to the group value data area within the DSM segment.
 * The data area starts at header->data_area_offset bytes from the header.
 */
static inline char *
warm_get_data_area(WarmCacheHeader *header)
{
    return (char *) header + header->data_area_offset;
}

/*
 * Deserialize a group Datum from the DSM segment.
 *
 * For pass-by-value types: reads 8 bytes as a raw Datum.
 * For varlena types: returns a pointer into the DSM (valid for worker lifetime).
 * For NULL groups: returns (Datum) 0 and sets *isnull = true.
 *
 * Parameters:
 *   header  - DSM header (for data area base pointer)
 *   task    - Task containing serialization info
 *   isnull  - Output: true if group value is NULL
 *
 * Returns the deserialized Datum.
 */
static inline Datum
warm_get_group_datum(WarmCacheHeader *header, WarmCacheTask *task, bool *isnull)
{
    char *data_area = warm_get_data_area(header);

    if (task->group_value_len == WARM_GROUP_VALUE_NULL)
    {
        *isnull = true;
        return (Datum) 0;
    }

    *isnull = false;

    if (task->group_value_len == WARM_GROUP_VALUE_BYVAL)
    {
        /* Pass-by-value: stored as raw 8 bytes */
        Datum d;
        memcpy(&d, data_area + task->group_value_off, sizeof(Datum));
        return d;
    }
    else if (task->group_value_len == WARM_GROUP_VALUE_FIXEDLEN)
    {
        /*
         * Fixed-length pass-by-reference (UUID, POINT, MACADDR, etc.):
         * return pointer directly into DSM. The actual length is
         * header->group_typlen, but the caller doesn't need it — it's
         * a direct pointer to the raw bytes, just like PG stores them.
         */
        return PointerGetDatum(data_area + task->group_value_off);
    }
    else
    {
        /*
         * Varlena (TEXT, BYTEA, etc.): return pointer directly into DSM.
         * This is safe because the DSM segment outlives the worker's
         * processing of this task. The pointer is valid until dsm_detach().
         */
        return PointerGetDatum(data_area + task->group_value_off);
    }
}

/* --- SQL-callable function --- */

extern Datum xpatch_warm_cache_parallel(PG_FUNCTION_ARGS);

/* --- Background worker entry point --- */

extern PGDLLEXPORT void xpatch_warm_worker_main(Datum main_arg);

#endif /* XPATCH_WARM_H */
