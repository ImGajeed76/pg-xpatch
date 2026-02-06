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

#include "fmgr.h"
#include "miscadmin.h"
#include "utils/guc.h"

PG_MODULE_MAGIC;

/* GUC variables */
int xpatch_cache_size_mb = XPATCH_DEFAULT_CACHE_SIZE_MB;

/* GUC variables for seq caches (defined in xpatch_seq_cache.c) */
extern int xpatch_group_cache_size_mb;
extern int xpatch_tid_cache_size_mb;

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
            XPATCH_DEFAULT_CACHE_SIZE_MB,  /* default */
            1,                              /* min */
            1024,                           /* max */
            PGC_POSTMASTER,                 /* context - requires restart */
            GUC_UNIT_MB,
            NULL,                           /* check_hook */
            NULL,                           /* assign_hook */
            NULL                            /* show_hook */
        );

        DefineCustomIntVariable(
            "pg_xpatch.group_cache_size_mb",
            "Size of the group max seq cache in megabytes",
            "Controls shared memory for caching max sequence numbers per group (optimizes INSERT)",
            &xpatch_group_cache_size_mb,
            8,                              /* default 8MB */
            1,                              /* min */
            256,                            /* max */
            PGC_POSTMASTER,
            GUC_UNIT_MB,
            NULL, NULL, NULL
        );

        DefineCustomIntVariable(
            "pg_xpatch.tid_cache_size_mb",
            "Size of the TID seq cache in megabytes",
            "Controls shared memory for caching TID to seq mappings (optimizes READ)",
            &xpatch_tid_cache_size_mb,
            8,                              /* default 8MB */
            1,                              /* min */
            256,                            /* max */
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
            256,                                 /* max */
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

        /* Request shared memory for caches - hooks into shmem_request_hook */
        xpatch_cache_request_shmem();
        xpatch_seq_cache_request_shmem();
        xpatch_insert_cache_request_shmem();

        elog(LOG, "pg_xpatch %s loaded via shared_preload_libraries (xpatch library %s, cache %d MB, group_cache %d MB, tid_cache %d MB, insert_cache_slots %d, encode_threads %d)",
             PG_XPATCH_VERSION, xpatch_lib_version(), xpatch_cache_size_mb,
             xpatch_group_cache_size_mb, xpatch_tid_cache_size_mb,
             xpatch_insert_cache_slots, xpatch_encode_threads);
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
