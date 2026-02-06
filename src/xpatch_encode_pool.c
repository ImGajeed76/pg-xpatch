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
 * xpatch_encode_pool.c - Lock-free pthread pool for parallel delta encoding
 *
 * Architecture:
 * - Persistent pthread pool (created on first use, destroyed on backend exit)
 * - Lock-free task dispatch via atomic fetch-add on a task counter
 * - No mutex in the hot encoding path — workers spin on the atomic counter
 * - Workers only call xpatch_encode() (Rust FFI, pure computation, thread-safe)
 * - Results written to cache-line-padded slots to prevent false sharing
 * - Condvar used only for idle-wait (workers sleep between batches)
 *
 * SAFETY INVARIANTS:
 * 1. Worker threads NEVER call any PG function (no palloc, elog, ereport, etc.)
 * 2. Worker threads block all signals via pthread_sigmask()
 * 3. Only the main thread calls PG functions after collecting results
 */

#include "xpatch_encode_pool.h"

#include "miscadmin.h"
#include "storage/ipc.h"

#include "xpatch.h"    /* libxpatch_c FFI: xpatch_encode, xpatch_free_buffer */

#include <pthread.h>
#include <signal.h>
#include <string.h>
#include <stdatomic.h>
#include <sched.h>
#include <stdlib.h>

/* GUC variable */
int xpatch_encode_threads = XPATCH_DEFAULT_ENCODE_THREADS;

/*
 * Per-task descriptor for workers.
 * Contains all inputs + output slot. Workers read inputs, write outputs.
 * Padded to 128 bytes (2 cache lines) to eliminate false sharing.
 */
typedef struct WorkerTask
{
    /* Inputs (read-only for worker) */
    uintptr_t       tag;
    const uint8_t  *base_data;
    uintptr_t       base_len;
    const uint8_t  *new_data;
    uintptr_t       new_len;
    bool            enable_zstd;

    /* Output (written by worker) */
    uint8_t        *result_data;
    size_t          result_size;
    bool            result_valid;

    /*
     * Padding to 128 bytes (2 cache lines) to avoid false sharing.
     * Without _pad, the struct layout with internal alignment padding is 72 bytes
     * (65 bytes of data + 7 bytes compiler alignment padding, rounded to 8-byte boundary).
     * We use a union trick to get the correct padding regardless of platform.
     */
    char            _pad[128 - 72];
} WorkerTask;

_Static_assert(sizeof(WorkerTask) == 128,
               "WorkerTask must be exactly 128 bytes (two cache lines)");

/*
 * Pool state - per-backend.
 */
typedef struct EncodePool
{
    pthread_t       threads[XPATCH_MAX_ENCODE_THREADS];
    int             num_threads;
    bool            initialized;

    /* Batch signaling (condvar for idle→work transition only) */
    pthread_mutex_t batch_mutex;
    pthread_cond_t  batch_ready;    /* Workers: "new batch available" */

    /* Current batch — set by leader before waking workers */
    WorkerTask     *tasks;          /* malloc'd task array */
    int             num_tasks;

    /* Lock-free task dispatch */
    atomic_int      next_task;      /* Workers atomically increment to grab tasks */
    atomic_int      completed;      /* Workers atomically increment on completion */

    /* Shutdown flag */
    atomic_bool     shutdown;

    /* Batch sequence number — workers compare to detect new batch */
    atomic_int      batch_seq;
} EncodePool;

static EncodePool pool = {
    .initialized = false,
};

/*
 * Worker thread function.
 * Waits for batch_ready signal, then spin-grabs tasks via atomic fetch-add.
 */
static void *
worker_func(void *arg)
{
    sigset_t mask;
    int my_batch_seq = 0;

    (void) arg;

    /* Block ALL signals in worker thread */
    sigfillset(&mask);
    pthread_sigmask(SIG_BLOCK, &mask, NULL);

    while (1)
    {
        int task_idx;
        int current_batch_seq;

        /* Wait for a new batch */
        pthread_mutex_lock(&pool.batch_mutex);
        while (!atomic_load(&pool.shutdown))
        {
            current_batch_seq = atomic_load(&pool.batch_seq);
            if (current_batch_seq != my_batch_seq)
                break;
            pthread_cond_wait(&pool.batch_ready, &pool.batch_mutex);
        }
        pthread_mutex_unlock(&pool.batch_mutex);

        if (atomic_load(&pool.shutdown))
            break;

        my_batch_seq = atomic_load(&pool.batch_seq);

        /* Spin-grab tasks via atomic fetch-add — no mutex in hot path */
        while (1)
        {
            task_idx = atomic_fetch_add(&pool.next_task, 1);
            if (task_idx >= pool.num_tasks)
                break;

            /* Execute encode — pure computation, no PG calls */
            {
                WorkerTask *task = &pool.tasks[task_idx];
                struct xpatch_XPatchBuffer result;

                result = xpatch_encode(task->tag,
                                       task->base_data, task->base_len,
                                       task->new_data, task->new_len,
                                       task->enable_zstd);

                if (result.data != NULL && result.len > 0)
                {
                    task->result_data = result.data;
                    task->result_size = result.len;
                    task->result_valid = true;
                }
                else
                {
                    task->result_data = NULL;
                    task->result_size = 0;
                    task->result_valid = false;
                    if (result.data != NULL)
                        xpatch_free_buffer(result);
                }
            }

            atomic_fetch_add(&pool.completed, 1);
        }
    }

    return NULL;
}

/*
 * on_proc_exit callback to shutdown pool before backend exits.
 */
static void
encode_pool_exit_callback(int code, Datum arg)
{
    xpatch_encode_pool_shutdown();
}

/*
 * Initialize the encode pool.
 */
void
xpatch_encode_pool_init(void)
{
    int i;
    int num_threads;

    if (pool.initialized)
        return;

    num_threads = xpatch_encode_threads;
    if (num_threads <= 0)
        return;
    if (num_threads > XPATCH_MAX_ENCODE_THREADS)
        num_threads = XPATCH_MAX_ENCODE_THREADS;

    pthread_mutex_init(&pool.batch_mutex, NULL);
    pthread_cond_init(&pool.batch_ready, NULL);

    pool.tasks = NULL;
    pool.num_tasks = 0;
    atomic_store(&pool.next_task, 0);
    atomic_store(&pool.completed, 0);
    atomic_store(&pool.shutdown, false);
    atomic_store(&pool.batch_seq, 0);
    pool.num_threads = num_threads;

    /* Create worker threads */
    for (i = 0; i < num_threads; i++)
    {
        int ret = pthread_create(&pool.threads[i], NULL, worker_func, NULL);
        if (ret != 0)
        {
            pool.num_threads = i;
            break;
        }
    }

    if (pool.num_threads == 0)
    {
        pthread_mutex_destroy(&pool.batch_mutex);
        pthread_cond_destroy(&pool.batch_ready);
        return;
    }

    pool.initialized = true;

    on_proc_exit(encode_pool_exit_callback, (Datum) 0);

    elog(DEBUG1, "xpatch: encode pool initialized with %d threads (lock-free)", pool.num_threads);
}

/*
 * Execute a batch of encode tasks sequentially on the calling thread.
 * Used when the pool is not available, when there's only 1 task, or as
 * a fallback when malloc fails for the parallel path.
 */
static void
execute_sequential(EncodeBatch *batch)
{
    int i;

    for (i = 0; i < batch->num_tasks; i++)
    {
        struct xpatch_XPatchBuffer result;

        result = xpatch_encode((uintptr_t) batch->tasks[i].tag,
                               batch->tasks[i].base_data,
                               (uintptr_t) batch->tasks[i].base_len,
                               batch->new_data,
                               (uintptr_t) batch->new_len,
                               batch->enable_zstd);

        if (result.data != NULL && result.len > 0)
        {
            batch->results[i].data = result.data;
            batch->results[i].size = result.len;
            batch->results[i].tag = batch->tasks[i].tag;
            batch->results[i].valid = true;
        }
        else
        {
            batch->results[i].data = NULL;
            batch->results[i].size = 0;
            batch->results[i].tag = batch->tasks[i].tag;
            batch->results[i].valid = false;
            if (result.data != NULL)
                xpatch_free_buffer(result);
        }
    }
}

/*
 * Execute a batch of encode tasks.
 *
 * Parallel path:
 * 1. Build WorkerTask array (malloc'd — safe for threads)
 * 2. Set up atomic counters
 * 3. Wake workers via condvar (one-time per batch)
 * 4. Leader participates: spin-grabs tasks via same atomic counter
 * 5. Spin-wait for all tasks to complete
 * 6. Copy results back to batch->results[]
 */
void
xpatch_encode_pool_execute(EncodeBatch *batch)
{
    int i;
    WorkerTask *tasks;

    if (batch->num_tasks <= 0)
        return;

    /*
     * Sequential path: no pool, or only 1 task (no parallelism benefit).
     */
    if (!pool.initialized || batch->num_tasks == 1)
    {
        execute_sequential(batch);
        return;
    }

    /*
     * Parallel path: lock-free dispatch.
     */

    /* Build WorkerTask array (malloc'd — not palloc, safe for threads) */
    tasks = malloc(sizeof(WorkerTask) * batch->num_tasks);
    if (!tasks)
    {
        /* Fallback to sequential on alloc failure */
        execute_sequential(batch);
        return;
    }

    for (i = 0; i < batch->num_tasks; i++)
    {
        tasks[i].tag = (uintptr_t) batch->tasks[i].tag;
        tasks[i].base_data = batch->tasks[i].base_data;
        tasks[i].base_len = (uintptr_t) batch->tasks[i].base_len;
        tasks[i].new_data = batch->new_data;
        tasks[i].new_len = (uintptr_t) batch->new_len;
        tasks[i].enable_zstd = batch->enable_zstd;
        tasks[i].result_data = NULL;
        tasks[i].result_size = 0;
        tasks[i].result_valid = false;
    }

    /* Set up batch for workers */
    pool.tasks = tasks;
    pool.num_tasks = batch->num_tasks;
    atomic_store(&pool.next_task, 0);
    atomic_store(&pool.completed, 0);

    /* Wake workers — one broadcast per batch */
    pthread_mutex_lock(&pool.batch_mutex);
    atomic_fetch_add(&pool.batch_seq, 1);
    pthread_cond_broadcast(&pool.batch_ready);
    pthread_mutex_unlock(&pool.batch_mutex);

    /* Leader participates: grab tasks via same atomic counter */
    while (1)
    {
        int task_idx = atomic_fetch_add(&pool.next_task, 1);
        if (task_idx >= batch->num_tasks)
            break;

        {
            WorkerTask *task = &tasks[task_idx];
            struct xpatch_XPatchBuffer result;

            result = xpatch_encode(task->tag,
                                   task->base_data, task->base_len,
                                   task->new_data, task->new_len,
                                   task->enable_zstd);

            if (result.data != NULL && result.len > 0)
            {
                task->result_data = result.data;
                task->result_size = result.len;
                task->result_valid = true;
            }
            else
            {
                task->result_data = NULL;
                task->result_size = 0;
                task->result_valid = false;
                if (result.data != NULL)
                    xpatch_free_buffer(result);
            }
        }

        atomic_fetch_add(&pool.completed, 1);
    }

    /* Spin-wait for all tasks to complete */
    while (atomic_load(&pool.completed) < batch->num_tasks)
    {
        /* Yield to avoid burning CPU while waiting for workers */
        #ifdef __x86_64__
        __builtin_ia32_pause();
        #else
        sched_yield();
        #endif
    }

    /*
     * Acquire fence: ensures all worker writes to WorkerTask result fields
     * are visible to us before we read them. The atomic_load(completed) above
     * uses seq_cst by default on most compilers, but the task array fields
     * (result_data, result_size, result_valid) are plain stores in workers,
     * so an explicit acquire fence guarantees visibility.
     */
    atomic_thread_fence(memory_order_acquire);

    /* Copy results back to batch */
    for (i = 0; i < batch->num_tasks; i++)
    {
        batch->results[i].data = tasks[i].result_data;
        batch->results[i].size = tasks[i].result_size;
        batch->results[i].tag = batch->tasks[i].tag;
        batch->results[i].valid = tasks[i].result_valid;
    }

    /* Clear tasks pointer so workers don't touch stale data */
    pool.tasks = NULL;
    pool.num_tasks = 0;

    free(tasks);
}

/*
 * Free all result buffers in a batch.
 */
void
xpatch_encode_pool_free_results(EncodeBatch *batch)
{
    int i;

    for (i = 0; i < batch->num_tasks; i++)
    {
        if (batch->results[i].valid && batch->results[i].data != NULL)
        {
            struct xpatch_XPatchBuffer buf;
            buf.data = batch->results[i].data;
            buf.len = batch->results[i].size;
            xpatch_free_buffer(buf);
            batch->results[i].data = NULL;
            batch->results[i].valid = false;
        }
    }
}

/*
 * Shutdown the encode pool.
 */
void
xpatch_encode_pool_shutdown(void)
{
    int i;

    if (!pool.initialized)
        return;

    /* Signal shutdown */
    atomic_store(&pool.shutdown, true);

    /* Wake all workers so they see the shutdown flag */
    pthread_mutex_lock(&pool.batch_mutex);
    atomic_fetch_add(&pool.batch_seq, 1);
    pthread_cond_broadcast(&pool.batch_ready);
    pthread_mutex_unlock(&pool.batch_mutex);

    /* Join all threads */
    for (i = 0; i < pool.num_threads; i++)
    {
        pthread_join(pool.threads[i], NULL);
    }

    pthread_mutex_destroy(&pool.batch_mutex);
    pthread_cond_destroy(&pool.batch_ready);

    pool.initialized = false;
    atomic_store(&pool.shutdown, false);
    pool.num_threads = 0;
}

/*
 * Check if the pool is available.
 */
bool
xpatch_encode_pool_available(void)
{
    return pool.initialized && pool.num_threads > 0;
}
