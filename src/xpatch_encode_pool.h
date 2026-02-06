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
 * xpatch_encode_pool.h - Thread pool for parallel delta encoding
 *
 * Provides a persistent pthread pool for parallelizing xpatch_encode() calls
 * during INSERT. Worker threads call xpatch_encode() directly (the Rust FFI
 * function), which is pure computation with no PostgreSQL dependencies.
 *
 * SAFETY CONSTRAINTS:
 * - Worker threads MUST NOT call any PostgreSQL functions (no palloc, elog, etc.)
 * - Worker threads only call xpatch_encode() from libxpatch_c
 * - All signals are blocked in worker threads (only main thread handles signals)
 * - Results are collected by the main thread and copied to palloc'd memory
 *
 * Architecture:
 * - Pool is created on first INSERT where compress_depth > 1 && encode_threads > 0
 * - Pool persists for the lifetime of the backend process
 * - Destroyed on backend exit via on_proc_exit() hook
 * - Main thread participates as leader (runs one encode task itself)
 * - Tasks dispatched via shared work queue + condition variable
 */

#ifndef XPATCH_ENCODE_POOL_H
#define XPATCH_ENCODE_POOL_H

#include "postgres.h"
#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

/* Default number of encode threads (0 = disabled, sequential encoding) */
#define XPATCH_DEFAULT_ENCODE_THREADS   0

/* Maximum encode threads */
#define XPATCH_MAX_ENCODE_THREADS       64

/* GUC variable (defined in xpatch_encode_pool.c) */
extern int xpatch_encode_threads;

/*
 * One encode task input.
 */
typedef struct EncodeTask
{
    int             tag;            /* Delta tag for this task */
    const uint8_t  *base_data;     /* Base content for this task */
    size_t          base_len;       /* Base content length */
} EncodeTask;

/*
 * Result of a single encode task.
 * Allocated by the Rust allocator (via xpatch_encode), freed by xpatch_free_buffer.
 * Padded to 64 bytes (cache line) to prevent false sharing between threads.
 */
typedef struct EncodeResult
{
    uint8_t    *data;           /* Encoded data (from xpatch_encode) */
    size_t      size;           /* Encoded data size */
    int         tag;            /* Tag used for this encoding */
    bool        valid;          /* Result is valid (encode succeeded) */
    char        _pad[64 - sizeof(uint8_t *) - sizeof(size_t) - sizeof(int) - sizeof(bool)];
} EncodeResult;

_Static_assert(sizeof(EncodeResult) == 64,
               "EncodeResult must be exactly 64 bytes (one cache line)");

/*
 * A batch of encode tasks to be dispatched to the pool.
 * tasks[] and results[] are dynamically allocated (palloc'd) to actual depth.
 * Use EncodeBatchInit() to allocate, EncodeBatchFree() to destroy.
 */
typedef struct EncodeBatch
{
    /* Inputs (set by caller, read-only for workers) */
    const uint8_t  *new_data;       /* New content to encode (shared across all tasks) */
    size_t          new_len;        /* Length of new content */
    bool            enable_zstd;    /* Enable zstd compression */

    int             num_tasks;      /* Number of encode tasks */
    int             capacity;       /* Allocated capacity */

    EncodeTask     *tasks;          /* palloc'd array of inputs */
    EncodeResult   *results;        /* palloc'd array of outputs (cache-line padded) */
} EncodeBatch;

/*
 * Initialize the encode pool (if not already initialized).
 * Called on first INSERT where parallel encoding is beneficial.
 * No-op if xpatch_encode_threads == 0 or pool already exists.
 */
void xpatch_encode_pool_init(void);

/*
 * Execute a batch of encode tasks in parallel.
 * The main thread participates as leader (runs one task itself).
 *
 * If the pool is not initialized (encode_threads == 0), runs all tasks
 * sequentially on the calling thread.
 *
 * Parameters:
 *   batch - Batch descriptor with inputs filled in. Results are written
 *           to batch->results[] upon return.
 *
 * After return:
 * - Each batch->results[i].valid indicates success
 * - Valid results have data/size set (allocated by Rust allocator)
 * - Caller must call xpatch_encode_pool_free_results() when done
 */
void xpatch_encode_pool_execute(EncodeBatch *batch);

/*
 * Free all result buffers in a batch.
 * Must be called after the caller has copied the winning result.
 */
void xpatch_encode_pool_free_results(EncodeBatch *batch);

/*
 * Shutdown the encode pool.
 * Called automatically on backend exit. Can also be called explicitly.
 */
void xpatch_encode_pool_shutdown(void);

/*
 * Check if the pool is available and has threads.
 */
bool xpatch_encode_pool_available(void);

#endif /* XPATCH_ENCODE_POOL_H */
