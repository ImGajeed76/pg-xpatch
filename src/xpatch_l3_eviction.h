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
 * xpatch_l3_eviction.h — L3 access tracking ring buffer + eviction BGW
 *
 * The L3 eviction system has two components:
 *
 * 1. **Access time ring buffer** (shared memory)
 *    - Fixed-size circular buffer of access records
 *    - Appended to on every L3 cache read (fast shmem write)
 *    - Protected by a single LWLock (low contention: writes are tiny)
 *
 * 2. **Eviction background worker** (static BGW, registered at _PG_init)
 *    - Wakes every l3_eviction_interval_s seconds
 *    - Flushes ring buffer → batch UPDATE cached_at on L3 tables
 *    - Checks L3 table sizes vs l3_cache_max_size_mb
 *    - DELETEs oldest entries when over limit, clears CHAIN_BIT_L3
 *
 * Lock ordering: ring buffer LWLock is independent of L1/L2/chain index
 * locks (never held simultaneously).
 *
 * GUCs (global, PGC_SIGHUP):
 *   pg_xpatch.l3_eviction_interval_s  — cycle interval (default 60)
 *   pg_xpatch.l3_access_buffer_size   — ring buffer entries (default 8192)
 */

#ifndef XPATCH_L3_EVICTION_H
#define XPATCH_L3_EVICTION_H

#include "pg_xpatch.h"
#include "xpatch_hash.h"

/* GUC defaults */
#define XPATCH_L3_DEFAULT_EVICTION_INTERVAL_S   60
#define XPATCH_L3_DEFAULT_ACCESS_BUFFER_SIZE    8192

/* GUC variables (defined in xpatch_l3_eviction.c) */
extern int xpatch_l3_eviction_interval_s;
extern int xpatch_l3_access_buffer_size;

/*
 * Record an L3 access in the shared ring buffer.
 *
 * Called from xpatch_l3_cache_get() on cache hit. Fast shmem write
 * under a lightweight exclusive lock. If the buffer is full, the
 * oldest entry is silently overwritten (ring semantics).
 *
 * No-op if shmem was not initialized (extension not in
 * shared_preload_libraries).
 */
void xpatch_l3_access_record(Oid relid, XPatchGroupHash group_hash,
                             int64 seq, AttrNumber attnum);

/*
 * Request shared memory for the L3 access ring buffer.
 *
 * Must be called from _PG_init() during shared_preload_libraries.
 * Chains into shmem_request_hook / shmem_startup_hook.
 */
void xpatch_l3_eviction_request_shmem(void);

/*
 * Register the L3 eviction background worker.
 *
 * Must be called from _PG_init() during shared_preload_libraries,
 * AFTER xpatch_l3_eviction_request_shmem().
 */
void xpatch_l3_eviction_register_bgw(void);

/*
 * Background worker entry point.
 * Exported so PostgreSQL can call it via bgw_function_name.
 */
PGDLLEXPORT void xpatch_l3_eviction_worker_main(Datum main_arg);

/*
 * Run one eviction cycle synchronously (flush buffer + eviction pass).
 *
 * Exposed as SQL function xpatch.l3_eviction_pass() for testing and
 * manual maintenance. Returns the number of access records flushed.
 *
 * Must be called within a transaction (standard for SQL functions).
 */
int32 xpatch_l3_eviction_run_once(void);

#endif /* XPATCH_L3_EVICTION_H */
