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
 * pg_xpatch.c - Extension entry point
 *
 * Registers the xpatch table access method.
 *
 * IMPORTANT: For shared memory cache to work, pg_xpatch must be loaded via
 * shared_preload_libraries in postgresql.conf:
 *
 *     shared_preload_libraries = 'pg_xpatch'
 *
 * Then restart PostgreSQL and CREATE EXTENSION pg_xpatch.
 * Without shared_preload_libraries, caching is disabled.
 */

#include "pg_xpatch.h"
#include "xpatch_tam.h"
#include "xpatch_cache.h"
#include "xpatch_seq_cache.h"
#include "xpatch_insert_cache.h"
#include "xpatch_encode_pool.h"
#include "xpatch_compress.h"
#include "xpatch_config.h"
#include "xpatch_warm.h"
#include "xpatch_chain_index.h"
#include "xpatch_l2_cache.h"
#include "xpatch_l3_eviction.h"
#include "xpatch_startup_warm.h"

#include "fmgr.h"
#include "miscadmin.h"
#include "utils/guc.h"

PG_MODULE_MAGIC;

/* GUC variables */
int xpatch_cache_size_mb = XPATCH_DEFAULT_CACHE_SIZE_MB;
int xpatch_cache_max_entry_kb = XPATCH_DEFAULT_MAX_ENTRY_KB;

/* GUC variables for seq caches (defined in xpatch_seq_cache.c) */
extern int xpatch_group_cache_size_mb;
extern int xpatch_tid_cache_size_mb;
extern int xpatch_seq_tid_cache_size_mb;

/* GUC variables for insert cache (defined in xpatch_insert_cache.c) */
extern int xpatch_max_delta_columns;

/* Track if we were loaded via shared_preload_libraries */
static bool loaded_via_shared_preload = false;

void _PG_init(void);

/*
 * Extension initialization
 *
 * Called either:
 * 1. At postmaster startup (if in shared_preload_libraries) - process_shared_preload_libraries_in_progress=true
 * 2. At backend startup (when CREATE EXTENSION is called) - process_shared_preload_libraries_in_progress=false
 *
 * Shared memory can only be requested in case #1.
 */
void
_PG_init(void)
{
    /* Register xpatch reloptions - MUST be done first */
    xpatch_init_reloptions();

    /*
     * Check if we're being loaded during shared_preload_libraries.
     * Only then can we register shared memory hooks and PGC_POSTMASTER GUCs.
     */
    if (process_shared_preload_libraries_in_progress)
    {
        loaded_via_shared_preload = true;

        /* Define GUC for cache size - only valid at postmaster startup */
        DefineCustomIntVariable(
            "pg_xpatch.cache_size_mb",
            "Size of the shared LRU cache in megabytes",
            "Controls shared memory allocated for caching decoded content across all backends",
            &xpatch_cache_size_mb,
            XPATCH_DEFAULT_CACHE_SIZE_MB,  /* default 256 */
            1,                              /* min */
            INT_MAX,                        /* max */
            PGC_POSTMASTER,                 /* context - requires restart */
            GUC_UNIT_MB,
            NULL,                           /* check_hook */
            NULL,                           /* assign_hook */
            NULL                            /* show_hook */
        );

        DefineCustomIntVariable(
            "pg_xpatch.cache_max_entries",
            "Maximum number of entries in the shared LRU cache",
            "Controls how many decoded content entries the cache can hold simultaneously",
            &xpatch_cache_max_entries,
            65536,                           /* default */
            1000,                            /* min */
            INT_MAX,                         /* max */
            PGC_POSTMASTER,
            0,
            NULL, NULL, NULL
        );

        DefineCustomIntVariable(
            "pg_xpatch.cache_max_entry_kb",
            "Maximum size of a single cache entry in kilobytes",
            "Entries larger than this are not cached. Increase for workloads with large files.",
            &xpatch_cache_max_entry_kb,
            XPATCH_DEFAULT_MAX_ENTRY_KB,    /* default 256 KB */
            16,                              /* min 16 KB */
            INT_MAX,                         /* max */
            PGC_SUSET,                       /* superuser can change at runtime */
            GUC_UNIT_KB,
            NULL, NULL, NULL
        );

        DefineCustomIntVariable(
            "pg_xpatch.cache_slot_size_kb",
            "Size of each content slot in the shared cache",
            "Controls the granularity of content storage in the shared memory cache",
            &xpatch_cache_slot_size_kb,
            4,                               /* default 4 KB */
            1,                               /* min 1 KB */
            64,                              /* max 64 KB */
            PGC_POSTMASTER,
            GUC_UNIT_KB,
            NULL, NULL, NULL
        );

        DefineCustomIntVariable(
            "pg_xpatch.cache_partitions",
            "Number of lock partitions for the shared cache",
            "Controls concurrency by striping the cache into independent partitions with separate locks",
            &xpatch_cache_partitions,
            32,                              /* default */
            1,                               /* min */
            256,                             /* max */
            PGC_POSTMASTER,
            0,
            NULL, NULL, NULL
        );

        /*
         * L1 cache GUC aliases — new preferred names.
         * Point to the SAME C variables as the old pg_xpatch.cache_* names.
         * Both names work; old names kept for one version cycle.
         */
        DefineCustomIntVariable(
            "pg_xpatch.l1_cache_size_mb",
            "Size of the L1 decompressed content cache in megabytes (alias for cache_size_mb)",
            NULL,
            &xpatch_cache_size_mb,
            XPATCH_DEFAULT_CACHE_SIZE_MB, 1, INT_MAX,
            PGC_POSTMASTER, GUC_UNIT_MB,
            NULL, NULL, NULL
        );

        DefineCustomIntVariable(
            "pg_xpatch.l1_cache_max_entries",
            "Maximum number of L1 cache entries (alias for cache_max_entries)",
            NULL,
            &xpatch_cache_max_entries,
            65536, 1000, INT_MAX,
            PGC_POSTMASTER, 0,
            NULL, NULL, NULL
        );

        DefineCustomIntVariable(
            "pg_xpatch.l1_cache_max_entry_kb",
            "Maximum size of a single L1 cache entry (alias for cache_max_entry_kb)",
            NULL,
            &xpatch_cache_max_entry_kb,
            XPATCH_DEFAULT_MAX_ENTRY_KB, 16, INT_MAX,
            PGC_SUSET, GUC_UNIT_KB,
            NULL, NULL, NULL
        );

        DefineCustomIntVariable(
            "pg_xpatch.l1_cache_slot_size_kb",
            "Size of each L1 content slot (alias for cache_slot_size_kb)",
            NULL,
            &xpatch_cache_slot_size_kb,
            4, 1, 64,
            PGC_POSTMASTER, GUC_UNIT_KB,
            NULL, NULL, NULL
        );

        DefineCustomIntVariable(
            "pg_xpatch.l1_cache_partitions",
            "Number of L1 lock partitions (alias for cache_partitions)",
            NULL,
            &xpatch_cache_partitions,
            32, 1, 256,
            PGC_POSTMASTER, 0,
            NULL, NULL, NULL
        );

        DefineCustomIntVariable(
            "pg_xpatch.group_cache_size_mb",
            "Size of the group max seq cache in megabytes",
            "Controls shared memory for caching max sequence numbers per group (optimizes INSERT)",
            &xpatch_group_cache_size_mb,
            16,                              /* default 16MB */
            1,                               /* min */
            INT_MAX,                         /* max */
            PGC_POSTMASTER,
            GUC_UNIT_MB,
            NULL, NULL, NULL
        );

        DefineCustomIntVariable(
            "pg_xpatch.tid_cache_size_mb",
            "Size of the TID seq cache in megabytes",
            "Controls shared memory for caching TID to seq mappings (optimizes READ)",
            &xpatch_tid_cache_size_mb,
            16,                              /* default 16MB */
            1,                               /* min */
            INT_MAX,                         /* max */
            PGC_POSTMASTER,
            GUC_UNIT_MB,
            NULL, NULL, NULL
        );

        DefineCustomIntVariable(
            "pg_xpatch.seq_tid_cache_size_mb",
            "Size of the seq-to-TID cache in megabytes",
            "Controls shared memory for caching seq to TID reverse mappings",
            &xpatch_seq_tid_cache_size_mb,
            16,                              /* default 16MB */
            1,                               /* min */
            INT_MAX,                         /* max */
            PGC_POSTMASTER,
            GUC_UNIT_MB,
            NULL, NULL, NULL
        );

        DefineCustomIntVariable(
            "pg_xpatch.insert_cache_slots",
            "Number of FIFO insert cache slots",
            "Controls how many (table, group) pairs can have active insert caches simultaneously",
            &xpatch_insert_cache_slots,
            XPATCH_DEFAULT_INSERT_CACHE_SLOTS,  /* default 16 */
            1,                                   /* min */
            INT_MAX,                             /* max */
            PGC_POSTMASTER,
            0,
            NULL, NULL, NULL
        );

        DefineCustomIntVariable(
            "pg_xpatch.max_delta_columns",
            "Maximum number of delta-compressed columns per table",
            "Controls the maximum number of columns that can use delta compression in a single table",
            &xpatch_max_delta_columns,
            32,                              /* default */
            1,                               /* min */
            INT_MAX,                         /* max */
            PGC_POSTMASTER,
            0,
            NULL, NULL, NULL
        );

        DefineCustomIntVariable(
            "pg_xpatch.encode_threads",
            "Number of worker threads for parallel delta encoding",
            "Controls the thread pool size for parallel encoding during INSERT (0 = sequential)",
            &xpatch_encode_threads,
            XPATCH_DEFAULT_ENCODE_THREADS,  /* default 0 (disabled) */
            0,                               /* min */
            XPATCH_MAX_ENCODE_THREADS,       /* max 64 */
            PGC_USERSET,                     /* can be changed per-session */
            0,
            NULL, NULL, NULL
        );

        DefineCustomIntVariable(
            "pg_xpatch.warm_cache_workers",
            "Default number of background workers for warm_cache_parallel()",
            "Controls how many dynamic background workers are launched for "
            "parallel cache warming (0 = sequential, overridable per-call)",
            &xpatch_warm_cache_workers,
            XPATCH_DEFAULT_WARM_WORKERS,     /* default 4 */
            0,                                /* min */
            INT_MAX,                          /* max (PG limits via max_worker_processes) */
            PGC_USERSET,                      /* can be changed per-session */
            0,
            NULL, NULL, NULL
        );

        DefineCustomIntVariable(
            "pg_xpatch.chain_index_initial_capacity",
            "Initial per-group array capacity for the chain index",
            "Controls the initial number of entries allocated per group "
            "in the chain index. Arrays grow automatically by 2x when full.",
            &xpatch_chain_index_initial_capacity,
            64,                              /* default */
            8,                               /* min */
            INT_MAX,                         /* max */
            PGC_POSTMASTER,
            0,
            NULL, NULL, NULL
        );

        DefineCustomIntVariable(
            "pg_xpatch.chain_index_dir_slots",
            "Number of directory hash slots in the chain index",
            "Controls the maximum number of unique (table, column, group) "
            "combinations the chain index can track. Each xpatch table "
            "with N groups and M delta columns uses N*M slots. Increase "
            "this for large datasets with many groups.",
            &xpatch_chain_index_dir_slots,
            4096,                            /* default */
            256,                             /* min */
            INT_MAX,                         /* max */
            PGC_POSTMASTER,
            0,
            NULL, NULL, NULL
        );

        /* --- L2 cache GUCs --- */

        DefineCustomIntVariable(
            "pg_xpatch.l2_cache_size_mb",
            "Size of the L2 compressed delta cache in megabytes",
            "Controls shared memory allocated for L2 compressed delta cache",
            &xpatch_l2_cache_size_mb,
            XPATCH_L2_DEFAULT_SIZE_MB,       /* default 1024 */
            1,                                /* min */
            INT_MAX,                          /* max */
            PGC_POSTMASTER,
            GUC_UNIT_MB,
            NULL, NULL, NULL
        );

        DefineCustomIntVariable(
            "pg_xpatch.l2_cache_max_entries",
            "Maximum number of entries in the L2 cache",
            "Controls how many compressed delta entries the L2 cache can hold",
            &xpatch_l2_cache_max_entries,
            XPATCH_L2_DEFAULT_MAX_ENTRIES,   /* default 4M */
            1000,                             /* min */
            INT_MAX,                          /* max */
            PGC_POSTMASTER,
            0,
            NULL, NULL, NULL
        );

        DefineCustomIntVariable(
            "pg_xpatch.l2_cache_slot_size",
            "Size of each content slot in the L2 cache (bytes)",
            "Controls the granularity of content storage in the L2 cache",
            &xpatch_l2_cache_slot_size,
            XPATCH_L2_DEFAULT_SLOT_SIZE,     /* default 512 */
            64,                               /* min */
            65536,                            /* max */
            PGC_POSTMASTER,
            0,
            NULL, NULL, NULL
        );

        DefineCustomIntVariable(
            "pg_xpatch.l2_cache_partitions",
            "Number of lock partitions for the L2 cache",
            "Controls concurrency by striping the L2 cache into independent partitions",
            &xpatch_l2_cache_partitions,
            XPATCH_L2_DEFAULT_PARTITIONS,    /* default 16 */
            1,                                /* min */
            256,                              /* max */
            PGC_POSTMASTER,
            0,
            NULL, NULL, NULL
        );

        DefineCustomIntVariable(
            "pg_xpatch.l2_cache_max_entry_kb",
            "Maximum size of a single L2 cache entry in kilobytes",
            "Compressed deltas larger than this are not cached in L2",
            &xpatch_l2_cache_max_entry_kb,
            XPATCH_L2_DEFAULT_MAX_ENTRY_KB,  /* default 64 KB */
            1,                                /* min */
            INT_MAX,                          /* max */
            PGC_SUSET,
            GUC_UNIT_KB,
            NULL, NULL, NULL
        );

        /* --- L3 eviction GUCs --- */

        DefineCustomIntVariable(
            "pg_xpatch.l3_eviction_interval_s",
            "L3 eviction worker cycle interval in seconds",
            "How often the background worker flushes access records and "
            "checks L3 table sizes for eviction",
            &xpatch_l3_eviction_interval_s,
            XPATCH_L3_DEFAULT_EVICTION_INTERVAL_S,  /* default 60 */
            1,                                        /* min */
            3600,                                     /* max 1 hour */
            PGC_SIGHUP,
            0,
            NULL, NULL, NULL
        );

        DefineCustomIntVariable(
            "pg_xpatch.l3_access_buffer_size",
            "Number of entries in the L3 access time ring buffer",
            "Controls the shared memory ring buffer for tracking L3 cache "
            "reads. The eviction worker drains this buffer periodically.",
            &xpatch_l3_access_buffer_size,
            XPATCH_L3_DEFAULT_ACCESS_BUFFER_SIZE,  /* default 8192 */
            64,                                     /* min */
            1048576,                                /* max 1M */
            PGC_POSTMASTER,
            0,
            NULL, NULL, NULL
        );

        /* Request shared memory for caches - hooks into shmem_request_hook */
        xpatch_cache_request_shmem();
        xpatch_seq_cache_request_shmem();
        xpatch_insert_cache_request_shmem();
        xpatch_chain_index_request_shmem();
        xpatch_l2_cache_request_shmem();
        xpatch_l3_eviction_request_shmem();

        /* Register L3 eviction background worker */
        xpatch_l3_eviction_register_bgw();

        /* Register startup warming background worker (one-shot) */
        xpatch_startup_warm_register_bgw();

        elog(LOG, "pg_xpatch %s loaded via shared_preload_libraries "
             "(xpatch library %s, L1 %d MB, L2 %d MB, "
             "group_cache %d MB, tid_cache %d MB, "
             "insert_cache_slots %d, encode_threads %d, warm_workers %d)",
             PG_XPATCH_VERSION, xpatch_lib_version(),
             xpatch_cache_size_mb, xpatch_l2_cache_size_mb,
             xpatch_group_cache_size_mb, xpatch_tid_cache_size_mb,
             xpatch_insert_cache_slots, xpatch_encode_threads,
             xpatch_warm_cache_workers);
    }
    else
    {
        /*
         * Not loaded via shared_preload_libraries.
         * The extension will work but without shared memory caching.
         * This is fine for testing but not recommended for production.
         */
        elog(LOG, "pg_xpatch %s loaded (xpatch library %s) - WARNING: not in shared_preload_libraries, caching disabled",
             PG_XPATCH_VERSION, xpatch_lib_version());
    }
}

/*
 * Table access method handler - returns the TableAmRoutine
 */
PG_FUNCTION_INFO_V1(xpatch_tam_handler);
Datum
xpatch_tam_handler(PG_FUNCTION_ARGS)
{
    PG_RETURN_POINTER(xpatch_get_table_am_routine());
}

/*
 * SQL-callable function to get library version
 */
PG_FUNCTION_INFO_V1(pg_xpatch_version);
Datum
pg_xpatch_version(PG_FUNCTION_ARGS)
{
    char version[128];
    snprintf(version, sizeof(version), "pg_xpatch %s (xpatch %s)",
             PG_XPATCH_VERSION, xpatch_lib_version());
    PG_RETURN_TEXT_P(cstring_to_text(version));
}
