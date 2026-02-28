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
 * xpatch_l3_eviction.c — L3 access tracking ring buffer + eviction BGW
 *
 * Two components:
 *
 * 1. Shared-memory ring buffer for access time tracking.
 *    On every L3 cache hit, a small record is appended. The buffer
 *    is a fixed-size circular array protected by a single LWLock.
 *    When the buffer wraps, oldest entries are silently overwritten.
 *
 * 2. Static background worker that runs every l3_eviction_interval_s:
 *    a) Drain the ring buffer → batch UPDATE cached_at on L3 tables
 *    b) For each table with L3 enabled, check size vs max
 *    c) DELETE oldest rows when over limit, clear CHAIN_BIT_L3
 *
 * Lock ordering: the ring buffer LWLock is independent of L1/L2/chain
 * index locks. It is never held while acquiring any other xpatch lock.
 */

#include "xpatch_l3_eviction.h"
#include "xpatch_l3_cache.h"
#include "xpatch_chain_index.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/timeout.h"

/* GUC variables */
int xpatch_l3_eviction_interval_s = XPATCH_L3_DEFAULT_EVICTION_INTERVAL_S;
int xpatch_l3_access_buffer_size = XPATCH_L3_DEFAULT_ACCESS_BUFFER_SIZE;

/* ---------------------------------------------------------------------------
 * Ring buffer data structures
 * ---------------------------------------------------------------------------
 */

/*
 * Single access record in the ring buffer.
 * 40 bytes, naturally aligned.
 */
typedef struct L3AccessRecord
{
    Oid             relid;          /* 4 bytes */
    AttrNumber      attnum;         /* 2 bytes */
    int16           padding;        /* 2 bytes */
    XPatchGroupHash group_hash;     /* 16 bytes */
    int64           seq;            /* 8 bytes */
    TimestampTz     access_time;    /* 8 bytes */
} L3AccessRecord;                   /* 40 bytes total */

/*
 * Shared-memory ring buffer header.
 * Followed by the L3AccessRecord[] array.
 */
typedef struct L3AccessBuffer
{
    LWLock      lock;               /* Protects head/count */
    int32       capacity;           /* Max entries (from GUC) */
    int32       head;               /* Next write position */
    int32       count;              /* Entries since last flush (may exceed capacity) */
    /* L3AccessRecord entries[FLEXIBLE_ARRAY_MEMBER]; -- follows in memory */
} L3AccessBuffer;

/* Pointer to shared memory buffer (set during startup) */
static L3AccessBuffer *l3_access_buf = NULL;

/* Shmem hook chaining */
static shmem_request_hook_type l3ev_prev_shmem_request_hook = NULL;
static shmem_startup_hook_type l3ev_prev_shmem_startup_hook = NULL;

/* BGW signal flags */
static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;

/* ---------------------------------------------------------------------------
 * Inline helpers
 * ---------------------------------------------------------------------------
 */

static inline L3AccessRecord *
buf_entries(L3AccessBuffer *buf)
{
    return (L3AccessRecord *) ((char *) buf + MAXALIGN(sizeof(L3AccessBuffer)));
}

/* ---------------------------------------------------------------------------
 * Public API: record an access
 * ---------------------------------------------------------------------------
 */

void
xpatch_l3_access_record(Oid relid, XPatchGroupHash group_hash,
                         int64 seq, AttrNumber attnum)
{
    L3AccessRecord *rec;
    int             slot;

    if (l3_access_buf == NULL)
        return;

    LWLockAcquire(&l3_access_buf->lock, LW_EXCLUSIVE);

    slot = l3_access_buf->head;
    rec = &buf_entries(l3_access_buf)[slot];

    rec->relid = relid;
    rec->attnum = attnum;
    rec->padding = 0;
    rec->group_hash = group_hash;
    rec->seq = seq;
    rec->access_time = GetCurrentTimestamp();

    l3_access_buf->head = (slot + 1) % l3_access_buf->capacity;
    l3_access_buf->count++;

    LWLockRelease(&l3_access_buf->lock);
}

/* ---------------------------------------------------------------------------
 * Shared memory hooks
 * ---------------------------------------------------------------------------
 */

static Size
l3_access_buffer_shmem_size(void)
{
    Size    size;

    size = MAXALIGN(sizeof(L3AccessBuffer));
    size = add_size(size, mul_size(sizeof(L3AccessRecord),
                                   xpatch_l3_access_buffer_size));
    return size;
}

static void
l3ev_shmem_request(void)
{
    if (l3ev_prev_shmem_request_hook)
        l3ev_prev_shmem_request_hook();

    RequestAddinShmemSpace(l3_access_buffer_shmem_size());
    RequestNamedLWLockTranche("xpatch_l3_access", 1);
}

static void
l3ev_shmem_startup(void)
{
    bool    found;
    Size    total_size;

    if (l3ev_prev_shmem_startup_hook)
        l3ev_prev_shmem_startup_hook();

    LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

    total_size = l3_access_buffer_shmem_size();
    l3_access_buf = (L3AccessBuffer *) ShmemInitStruct("xpatch_l3_access_buffer",
                                                        total_size, &found);

    if (!found)
    {
        int tranche_id = LWLockNewTrancheId();
        LWLockRegisterTranche(tranche_id, "xpatch_l3_access");
        LWLockInitialize(&l3_access_buf->lock, tranche_id);
        l3_access_buf->capacity = xpatch_l3_access_buffer_size;
        l3_access_buf->head = 0;
        l3_access_buf->count = 0;

        memset(buf_entries(l3_access_buf), 0,
               sizeof(L3AccessRecord) * xpatch_l3_access_buffer_size);
    }

    LWLockRelease(AddinShmemInitLock);
}

void
xpatch_l3_eviction_request_shmem(void)
{
    l3ev_prev_shmem_request_hook = shmem_request_hook;
    shmem_request_hook = l3ev_shmem_request;

    l3ev_prev_shmem_startup_hook = shmem_startup_hook;
    shmem_startup_hook = l3ev_shmem_startup;
}

/* ---------------------------------------------------------------------------
 * Background worker: signal handlers
 * ---------------------------------------------------------------------------
 */

static void
l3ev_sigterm_handler(SIGNAL_ARGS)
{
    int save_errno = errno;

    got_sigterm = true;
    SetLatch(MyLatch);

    errno = save_errno;
}

static void
l3ev_sighup_handler(SIGNAL_ARGS)
{
    int save_errno = errno;

    got_sighup = true;
    SetLatch(MyLatch);

    errno = save_errno;
}

/* ---------------------------------------------------------------------------
 * Background worker: flush ring buffer → batch UPDATE cached_at
 * ---------------------------------------------------------------------------
 */

/*
 * Drain the ring buffer into a local array, then batch UPDATE the
 * cached_at column in each affected L3 table.
 *
 * Returns the number of records flushed.
 */
static int
flush_access_buffer(void)
{
    L3AccessRecord *local_buf;
    int             n_records;
    int             capacity;
    int             start;
    int             i;

    if (l3_access_buf == NULL)
        return 0;

    /* Snapshot the buffer under lock */
    LWLockAcquire(&l3_access_buf->lock, LW_EXCLUSIVE);

    capacity = l3_access_buf->capacity;
    n_records = l3_access_buf->count;

    if (n_records == 0)
    {
        LWLockRelease(&l3_access_buf->lock);
        return 0;
    }

    /* Clamp to capacity (ring may have wrapped) */
    if (n_records > capacity)
        n_records = capacity;

    /* Copy records to local memory */
    local_buf = (L3AccessRecord *) palloc(sizeof(L3AccessRecord) * n_records);

    /*
     * Read backwards from head. The most recent entry is at head-1,
     * the oldest kept entry is at head - n_records.
     */
    start = (l3_access_buf->head - n_records + capacity) % capacity;
    for (i = 0; i < n_records; i++)
    {
        int idx = (start + i) % capacity;
        local_buf[i] = buf_entries(l3_access_buf)[idx];
    }

    /* Reset the buffer */
    l3_access_buf->count = 0;
    /* head stays where it is — new writes continue from there */

    LWLockRelease(&l3_access_buf->lock);

    /*
     * Batch UPDATE cached_at for each record.
     *
     * We build per-table UPDATE batches using:
     *   UPDATE <l3_table> SET cached_at = $5
     *   WHERE group_hash_h1 = $1 AND group_hash_h2 = $2
     *     AND seq = $3 AND attnum = $4
     *
     * For simplicity, we issue one UPDATE per record. With typical
     * buffer sizes (~8K entries) and 60s intervals, this is fine.
     * If profiling shows this is a bottleneck, we can batch into
     * a single CTE or temp-table join.
     */
    if (SPI_connect() != SPI_OK_CONNECT)
    {
        pfree(local_buf);
        return 0;
    }

    for (i = 0; i < n_records; i++)
    {
        L3AccessRecord *rec = &local_buf[i];
        char           *l3_table;
        char           *sql;
        Oid             argtypes[5] = { INT8OID, INT8OID, INT8OID, INT2OID, TIMESTAMPTZOID };
        Datum           values[5];
        char            nulls[5] = { ' ', ' ', ' ', ' ', ' ' };

        /* Skip if L3 not enabled (table may have been disabled) */
        if (!xpatch_l3_cache_is_enabled(rec->relid))
            continue;

        l3_table = xpatch_l3_cache_table_name(rec->relid);
        if (l3_table == NULL)
            continue;

        values[0] = Int64GetDatum((int64) rec->group_hash.h1);
        values[1] = Int64GetDatum((int64) rec->group_hash.h2);
        values[2] = Int64GetDatum(rec->seq);
        values[3] = Int16GetDatum(rec->attnum);
        values[4] = TimestampTzGetDatum(rec->access_time);

        sql = psprintf(
            "UPDATE %s SET cached_at = $5 "
            "WHERE group_hash_h1 = $1 AND group_hash_h2 = $2 "
            "  AND seq = $3 AND attnum = $4",
            l3_table);

        SPI_execute_with_args(sql, 5, argtypes, values, nulls, false, 0);
        pfree(sql);
        pfree(l3_table);
    }

    SPI_finish();
    pfree(local_buf);

    return n_records;
}

/* ---------------------------------------------------------------------------
 * Background worker: eviction pass
 * ---------------------------------------------------------------------------
 */

/*
 * For each xpatch table with L3 enabled:
 *   1. Check L3 table size (pg_total_relation_size)
 *   2. If over l3_cache_max_size_mb: DELETE oldest rows by cached_at
 *   3. Clear CHAIN_BIT_L3 for evicted entries
 *
 * We discover L3-enabled tables by scanning xpatch.table_config.
 */
static void
eviction_pass(void)
{
    int     ret;
    int     ntables;
    int     i;
    Oid    *relids = NULL;
    int    *max_sizes = NULL;

    if (SPI_connect() != SPI_OK_CONNECT)
        return;

    /*
     * Find all tables with L3 enabled and their max size config.
     */
    ret = SPI_execute(
        "SELECT relid, l3_cache_max_size_mb "
        "FROM xpatch.table_config "
        "WHERE l3_cache_enabled = true AND relid IS NOT NULL",
        true, 0);

    if (ret != SPI_OK_SELECT || SPI_processed == 0)
    {
        SPI_finish();
        return;
    }

    ntables = (int) SPI_processed;

    /*
     * Allocate in CurTransactionContext so they survive SPI_finish().
     * SPI_finish() only resets the SPI procedure context, not
     * the transaction context.
     */
    {
        MemoryContext old_ctx = MemoryContextSwitchTo(CurTransactionContext);

        relids = (Oid *) palloc(sizeof(Oid) * ntables);
        max_sizes = (int *) palloc(sizeof(int) * ntables);

        MemoryContextSwitchTo(old_ctx);
    }

    for (i = 0; i < ntables; i++)
    {
        bool    isnull;
        Datum   datum;

        datum = SPI_getbinval(SPI_tuptable->vals[i],
                              SPI_tuptable->tupdesc, 1, &isnull);
        relids[i] = isnull ? InvalidOid : DatumGetObjectId(datum);

        datum = SPI_getbinval(SPI_tuptable->vals[i],
                              SPI_tuptable->tupdesc, 2, &isnull);
        max_sizes[i] = isnull ? 1024 : DatumGetInt32(datum);
    }

    SPI_finish();

    /*
     * For each L3-enabled table, check size and evict if needed.
     */
    for (i = 0; i < ntables; i++)
    {
        char   *l3_table;
        char   *sql;
        int64   size_bytes;
        int64   max_bytes;

        if (!OidIsValid(relids[i]))
            continue;

        l3_table = xpatch_l3_cache_table_name(relids[i]);
        if (l3_table == NULL)
            continue;

        if (SPI_connect() != SPI_OK_CONNECT)
        {
            pfree(l3_table);
            continue;
        }

        /* Get current L3 table size */
        sql = psprintf(
            "SELECT pg_total_relation_size('%s'::regclass)",
            l3_table);

        ret = SPI_execute(sql, true, 1);
        pfree(sql);

        if (ret != SPI_OK_SELECT || SPI_processed == 0)
        {
            SPI_finish();
            pfree(l3_table);
            continue;
        }

        {
            bool    isnull;
            Datum   datum;

            datum = SPI_getbinval(SPI_tuptable->vals[0],
                                  SPI_tuptable->tupdesc, 1, &isnull);
            size_bytes = isnull ? 0 : DatumGetInt64(datum);
        }

        max_bytes = (int64) max_sizes[i] * 1024LL * 1024LL;

        if (size_bytes <= max_bytes)
        {
            SPI_finish();
            pfree(l3_table);
            continue;
        }

        elog(LOG, "xpatch L3 eviction: table %s is " INT64_FORMAT " bytes "
             "(limit " INT64_FORMAT "), evicting oldest entries",
             l3_table, size_bytes, max_bytes);

        /*
         * Delete oldest entries until we're under the limit.
         *
         * Strategy: delete in batches using cached_at ordering.
         * We target 80% of max to avoid thrashing on the boundary.
         *
         * We use RETURNING to capture the evicted keys so we can
         * clear CHAIN_BIT_L3 in the chain index.
         */
        {
            int64   target_bytes = max_bytes * 80 / 100;  /* target 80% of max */
            int64   bytes_to_free = size_bytes - target_bytes;
            int     batch_size;
            int64   row_count;
            int64   avg_row_bytes;

            /*
             * Get actual row count to compute average row size.
             * This gives us a much better estimate than a fixed constant.
             */
            sql = psprintf("SELECT COUNT(*) FROM %s", l3_table);
            ret = SPI_execute(sql, true, 1);
            pfree(sql);

            if (ret == SPI_OK_SELECT && SPI_processed > 0)
            {
                bool    isnull;
                Datum   datum;

                datum = SPI_getbinval(SPI_tuptable->vals[0],
                                      SPI_tuptable->tupdesc, 1, &isnull);
                row_count = isnull ? 0 : DatumGetInt64(datum);
            }
            else
                row_count = 0;

            if (row_count > 0)
                avg_row_bytes = size_bytes / row_count;
            else
                avg_row_bytes = 500;    /* fallback if table is empty */

            if (avg_row_bytes < 1)
                avg_row_bytes = 1;

            batch_size = (int) (bytes_to_free / avg_row_bytes);
            if (batch_size < 1)
                batch_size = 1;
            if (batch_size > 100000)
                batch_size = 100000;

            sql = psprintf(
                "DELETE FROM %s WHERE ctid IN ("
                "  SELECT ctid FROM %s ORDER BY cached_at ASC LIMIT %d"
                ") RETURNING group_hash_h1, group_hash_h2, seq, attnum",
                l3_table, l3_table, batch_size);

            ret = SPI_execute(sql, false, 0);
            pfree(sql);

            if (ret == SPI_OK_DELETE_RETURNING && SPI_processed > 0)
            {
                uint64  evicted = SPI_processed;
                uint64  j;

                elog(LOG, "xpatch L3 eviction: evicted " UINT64_FORMAT
                     " entries from %s", evicted, l3_table);

                /* Clear CHAIN_BIT_L3 for each evicted entry */
                if (xpatch_chain_index_is_ready())
                {
                    for (j = 0; j < evicted; j++)
                    {
                        bool        isnull;
                        Datum       d;
                        XPatchGroupHash gh;
                        int64       ev_seq;
                        AttrNumber  ev_attnum;

                        d = SPI_getbinval(SPI_tuptable->vals[j],
                                          SPI_tuptable->tupdesc, 1, &isnull);
                        gh.h1 = isnull ? 0 : (uint64) DatumGetInt64(d);

                        d = SPI_getbinval(SPI_tuptable->vals[j],
                                          SPI_tuptable->tupdesc, 2, &isnull);
                        gh.h2 = isnull ? 0 : (uint64) DatumGetInt64(d);

                        d = SPI_getbinval(SPI_tuptable->vals[j],
                                          SPI_tuptable->tupdesc, 3, &isnull);
                        ev_seq = isnull ? 0 : DatumGetInt64(d);

                        d = SPI_getbinval(SPI_tuptable->vals[j],
                                          SPI_tuptable->tupdesc, 4, &isnull);
                        ev_attnum = isnull ? 0 : DatumGetInt16(d);

                        xpatch_chain_index_update_bits(relids[i], gh,
                                                       ev_attnum, ev_seq,
                                                       0, CHAIN_BIT_L3);
                    }
                }
            }
        }

        SPI_finish();
        pfree(l3_table);
    }

    if (relids)
        pfree(relids);
    if (max_sizes)
        pfree(max_sizes);
}

/* ---------------------------------------------------------------------------
 * Background worker: main loop
 * ---------------------------------------------------------------------------
 */

PGDLLEXPORT void
xpatch_l3_eviction_worker_main(Datum main_arg)
{
    /* Set up signal handlers */
    pqsignal(SIGHUP, l3ev_sighup_handler);
    pqsignal(SIGTERM, l3ev_sigterm_handler);

    /* Ready to receive signals */
    BackgroundWorkerUnblockSignals();

    /* Connect to the default database */
    BackgroundWorkerInitializeConnection("postgres", NULL, 0);

    elog(LOG, "xpatch L3 eviction worker started (interval=%ds, buffer=%d)",
         xpatch_l3_eviction_interval_s, xpatch_l3_access_buffer_size);

    while (!got_sigterm)
    {
        int     rc;
        int     wait_ms;

        /* Reload config on SIGHUP */
        if (got_sighup)
        {
            got_sighup = false;
            ProcessConfigFile(PGC_SIGHUP);
        }

        wait_ms = xpatch_l3_eviction_interval_s * 1000;
        if (wait_ms < 1000)
            wait_ms = 1000;

        rc = WaitLatch(MyLatch,
                       WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                       wait_ms,
                       PG_WAIT_EXTENSION);
        ResetLatch(MyLatch);

        /* Process any pending barrier events (required for DROP DATABASE etc.) */
        CHECK_FOR_INTERRUPTS();

        if (rc & WL_POSTMASTER_DEATH)
            proc_exit(1);

        if (got_sigterm)
            break;

        /* Do the work in a transaction */
        SetCurrentStatementStartTimestamp();
        StartTransactionCommand();
        PushActiveSnapshot(GetTransactionSnapshot());

        PG_TRY();
        {
            int flushed;

            flushed = flush_access_buffer();
            if (flushed > 0)
            {
                elog(DEBUG1, "xpatch L3 eviction: flushed %d access records",
                     flushed);
            }

            eviction_pass();

            PopActiveSnapshot();
            CommitTransactionCommand();
        }
        PG_CATCH();
        {
            /* Log the error but don't crash the worker */
            EmitErrorReport();
            FlushErrorState();

            /* Abort the failed transaction cleanly */
            AbortCurrentTransaction();
        }
        PG_END_TRY();

        pgstat_report_activity(STATE_IDLE, NULL);
    }

    elog(LOG, "xpatch L3 eviction worker shutting down");
    proc_exit(0);
}

/* ---------------------------------------------------------------------------
 * Synchronous eviction (for SQL function / testing)
 * ---------------------------------------------------------------------------
 */

int32
xpatch_l3_eviction_run_once(void)
{
    int flushed;

    flushed = flush_access_buffer();
    eviction_pass();

    return (int32) flushed;
}

/* ---------------------------------------------------------------------------
 * BGW registration
 * ---------------------------------------------------------------------------
 */

void
xpatch_l3_eviction_register_bgw(void)
{
    BackgroundWorker worker;

    memset(&worker, 0, sizeof(worker));

    snprintf(worker.bgw_name, BGW_MAXLEN, "xpatch L3 eviction worker");
    snprintf(worker.bgw_type, BGW_MAXLEN, "xpatch L3 eviction worker");
    snprintf(worker.bgw_function_name, BGW_MAXLEN,
             "xpatch_l3_eviction_worker_main");
    snprintf(worker.bgw_library_name, BGW_MAXLEN, "pg_xpatch");

    worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
                       BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = 10;  /* Restart after 10s on crash */
    worker.bgw_main_arg = (Datum) 0;
    worker.bgw_notify_pid = 0;

    RegisterBackgroundWorker(&worker);
}
