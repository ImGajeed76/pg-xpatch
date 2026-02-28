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
 * xpatch_startup_warm.c -- Multi-database startup warming background workers
 *
 * Two-tier architecture:
 *
 *   Coordinator (static BGW, connects to "postgres"):
 *     1. Enumerate connectable databases via pg_database.
 *     2. For each database, RegisterDynamicBackgroundWorker with db OID.
 *     3. Wait for all per-DB workers to finish.
 *     4. Exit (BGW_NEVER_RESTART).
 *
 *   Per-DB worker (dynamic BGW, connects to target database):
 *     1. BackgroundWorkerInitializeConnectionByOid(db_oid).
 *     2. Discover xpatch tables via pg_class (relam = xpatch AM oid).
 *     3. For each table: single-pass direct-buffer scan:
 *        - Build chain index entries (CHAIN_BIT_DISK)
 *        - Populate L2 cache (CHAIN_BIT_L2)
 *     4. For tables with L3 enabled: scan L3 PKs via SPI (CHAIN_BIT_L3).
 *     5. Exit.
 *
 * Lock ordering: no L1/L2/chain index stripe locks are held while
 * calling SPI (which may acquire its own locks).
 */

#include "xpatch_startup_warm.h"
#include "xpatch_chain_index.h"
#include "xpatch_l2_cache.h"
#include "xpatch_l3_cache.h"
#include "xpatch_config.h"
#include "xpatch_compress.h"
#include "xpatch_hash.h"

#include "access/htup_details.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/pg_am.h"
#include "catalog/pg_class.h"
#include "catalog/pg_database.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "storage/bufmgr.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

/* BGW signal flags */
static volatile sig_atomic_t sw_got_sigterm = false;

/* ---------------------------------------------------------------------------
 * Signal handlers
 * ---------------------------------------------------------------------------
 */

static void
sw_sigterm_handler(SIGNAL_ARGS)
{
    int save_errno = errno;

    sw_got_sigterm = true;
    SetLatch(MyLatch);

    errno = save_errno;
}

/* ---------------------------------------------------------------------------
 * Discover xpatch tables in the current database
 * ---------------------------------------------------------------------------
 * Returns a palloc'd array of Oid.  Caller must pfree.
 * Must be called inside an SPI connection.
 */

static Oid *
discover_xpatch_tables(int *ntables_out)
{
    Oid    *relids = NULL;
    int     ret;
    int     ntables;
    int     i;

    *ntables_out = 0;

    ret = SPI_execute(
        "SELECT c.oid AS relid "
        "FROM pg_class c "
        "JOIN pg_am a ON c.relam = a.oid "
        "WHERE a.amname = 'xpatch' "
        "  AND c.relkind = 'r'",
        true, 0);

    if (ret != SPI_OK_SELECT || SPI_processed == 0)
        return NULL;

    ntables = (int) SPI_processed;

    {
        MemoryContext old_ctx = MemoryContextSwitchTo(CurTransactionContext);
        relids = (Oid *) palloc(sizeof(Oid) * ntables);
        MemoryContextSwitchTo(old_ctx);
    }

    for (i = 0; i < ntables; i++)
    {
        bool    isnull;
        Datum   d = SPI_getbinval(SPI_tuptable->vals[i],
                                  SPI_tuptable->tupdesc, 1, &isnull);
        relids[i] = DatumGetObjectId(d);
    }

    *ntables_out = ntables;
    return relids;
}

/* ---------------------------------------------------------------------------
 * Warm a single xpatch table: build chain index + populate L2
 * ---------------------------------------------------------------------------
 * Uses direct buffer access — cannot use heap_beginscan because xpatch
 * is not the heap AM.
 *
 * Returns the number of entries warmed, or -1 on error.
 */

static int64
warm_single_table(Oid relid)
{
    Relation        rel;
    XPatchConfig   *config;
    TupleDesc       tupdesc;
    BlockNumber     nblocks;
    BlockNumber     blkno;
    Oid             group_typid = InvalidOid;
    int64           warmed = 0;
    int             j;

    /* Open the relation with AccessShareLock */
    rel = table_open(relid, AccessShareLock);
    config = xpatch_get_config(rel);
    tupdesc = RelationGetDescr(rel);

    if (config->xp_seq_attnum == InvalidAttrNumber)
    {
        elog(WARNING, "xpatch startup warm: table \"%s\" missing _xp_seq column, skipping",
             RelationGetRelationName(rel));
        table_close(rel, AccessShareLock);
        return -1;
    }

    if (config->num_delta_columns == 0)
    {
        elog(DEBUG1, "xpatch startup warm: table \"%s\" has no delta columns, skipping",
             RelationGetRelationName(rel));
        table_close(rel, AccessShareLock);
        return 0;
    }

    /* Get group column type OID for hash computation */
    if (config->group_by_attnum != InvalidAttrNumber)
    {
        Form_pg_attribute group_attr = TupleDescAttr(tupdesc,
                                                     config->group_by_attnum - 1);
        group_typid = group_attr->atttypid;
    }

    nblocks = RelationGetNumberOfBlocks(rel);

    elog(LOG, "xpatch startup warm: scanning \"%s\" (%u blocks, %d delta columns)",
         RelationGetRelationName(rel), nblocks, config->num_delta_columns);

    /* Sequential scan of all blocks */
    for (blkno = 0; blkno < nblocks; blkno++)
    {
        Buffer          buffer;
        Page            page;
        OffsetNumber    maxoff;
        OffsetNumber    offnum;

        /* Interruptible: check for SIGTERM between blocks */
        CHECK_FOR_INTERRUPTS();

        if (sw_got_sigterm)
        {
            elog(LOG, "xpatch startup warm: interrupted at block %u/%u of \"%s\"",
                 blkno, nblocks, RelationGetRelationName(rel));
            break;
        }

        buffer = ReadBuffer(rel, blkno);
        LockBuffer(buffer, BUFFER_LOCK_SHARE);

        page = BufferGetPage(buffer);
        maxoff = PageGetMaxOffsetNumber(page);

        for (offnum = FirstOffsetNumber; offnum <= maxoff; offnum++)
        {
            ItemId          itemId;
            HeapTupleData   tuple;
            bool            seq_isnull;
            Datum           seq_datum;
            int64           seq;
            Datum           group_datum = (Datum) 0;
            bool            group_isnull = true;
            uint64          gh1, gh2;

            itemId = PageGetItemId(page, offnum);
            if (!ItemIdIsNormal(itemId))
                continue;

            tuple.t_data = (HeapTupleHeader) PageGetItem(page, itemId);
            tuple.t_len = ItemIdGetLength(itemId);
            tuple.t_tableOid = relid;
            ItemPointerSet(&tuple.t_self, blkno, offnum);

            /*
             * MVCC visibility check: skip uncommitted/deleted tuples.
             *
             * Handles HEAP_XMAX_IS_MULTI (MultiXactId in xmax) and
             * HEAP_XMAX_LOCK_ONLY (row-locked but not deleted).
             */
            {
                TransactionId xmin = HeapTupleHeaderGetRawXmin(tuple.t_data);

                if (!TransactionIdIsCurrentTransactionId(xmin) &&
                    !TransactionIdDidCommit(xmin))
                    continue;

                if (!(tuple.t_data->t_infomask & HEAP_XMAX_INVALID))
                {
                    /* Lock-only xmax means the row is NOT deleted */
                    if (tuple.t_data->t_infomask & HEAP_XMAX_LOCK_ONLY)
                        ; /* visible — fall through */
                    else if (tuple.t_data->t_infomask & HEAP_XMAX_IS_MULTI)
                    {
                        /*
                         * MultiXactId in xmax — conservatively treat
                         * as visible (the row may be locked, not deleted).
                         * A proper check would use MultiXactIdIsRunning(),
                         * but for warming, false positives (including a
                         * deleted tuple) are harmless — the entry just
                         * gets a stale chain index entry that will be
                         * overwritten on next INSERT.
                         */
                    }
                    else
                    {
                        TransactionId xmax = HeapTupleHeaderGetRawXmax(tuple.t_data);
                        if (TransactionIdDidCommit(xmax) ||
                            TransactionIdIsCurrentTransactionId(xmax))
                            continue;
                    }
                }
            }

            /* Read _xp_seq */
            seq_datum = heap_getattr(&tuple, config->xp_seq_attnum,
                                     tupdesc, &seq_isnull);
            if (seq_isnull)
                continue;
            seq = DatumGetInt64(seq_datum);

            /* Read group column and compute hash */
            if (config->group_by_attnum != InvalidAttrNumber)
            {
                group_datum = heap_getattr(&tuple, config->group_by_attnum,
                                           tupdesc, &group_isnull);
            }

            /*
             * Compute group hash using separate h1/h2 variables to avoid
             * -Wclobbered warnings when called from PG_TRY(2) context.
             * The XPatchGroupHash struct is constructed in each inner
             * block scope where it is needed.
             */
            {
                XPatchGroupHash tmp_hash;
                tmp_hash = xpatch_compute_group_hash(group_datum, group_typid,
                                                     group_isnull);
                gh1 = tmp_hash.h1;
                gh2 = tmp_hash.h2;
            }

            /* Process each delta column */
            for (j = 0; j < config->num_delta_columns; j++)
            {
                AttrNumber  attnum = config->delta_attnums[j];
                bool        blob_isnull;
                Datum       blob_datum;
                bytea      *blob;
                size_t      tag;
                const char *err;
                uint32      base_offset;

                blob_datum = heap_getattr(&tuple, attnum, tupdesc,
                                          &blob_isnull);
                if (blob_isnull)
                    continue;

                /*
                 * Detoast the compressed blob.  Use PG_DETOAST_DATUM to
                 * guarantee a flat 4-byte-header copy that's safe to pass
                 * to l2_cache_put (which uses VARSIZE_ANY) and survives
                 * independently of the buffer page.
                 */
                blob = (bytea *) PG_DETOAST_DATUM(blob_datum);

                /* Extract delta tag */
                err = xpatch_get_delta_tag(
                    (const uint8 *) VARDATA_ANY(blob),
                    VARSIZE_ANY_EXHDR(blob),
                    &tag);

                if (err != NULL)
                {
                    elog(DEBUG1, "xpatch startup warm: bad tag in \"%s\" "
                         "seq=" INT64_FORMAT " attnum=%d: %s",
                         RelationGetRelationName(rel), seq, attnum, err);
                    continue;
                }

                base_offset = (uint32) tag;

                /* Insert into chain index: CHAIN_BIT_DISK */
                {
                    XPatchGroupHash group_hash;
                    group_hash.h1 = gh1;
                    group_hash.h2 = gh2;
                    xpatch_chain_index_insert(relid, group_hash, attnum,
                                              seq, base_offset,
                                              CHAIN_BIT_DISK);
                }

                /*
                 * Insert into L2 cache (also sets CHAIN_BIT_L2).
                 * L2 put takes a bytea with varlena header.
                 */
                if (xpatch_l2_cache_is_ready())
                {
                    XPatchGroupHash group_hash;
                    group_hash.h1 = gh1;
                    group_hash.h2 = gh2;
                    xpatch_l2_cache_put(relid, group_hash, seq, attnum, blob);
                }

                /*
                 * Free the detoasted copy.  PG_DETOAST_DATUM returns
                 * a palloc'd copy when the datum was toasted; when not
                 * toasted it returns the original pointer (into the
                 * buffer page).  Only free if it differs from the
                 * original datum pointer.
                 */
                if ((Pointer) blob != DatumGetPointer(blob_datum))
                    pfree(blob);

                warmed++;
            }
        }

        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
        ReleaseBuffer(buffer);
    }

    table_close(rel, AccessShareLock);

    return warmed;
}

/* ---------------------------------------------------------------------------
 * Scan L3 table PKs and set CHAIN_BIT_L3 in chain index
 * ---------------------------------------------------------------------------
 * For tables with L3 enabled, scan the L3 table primary key columns
 * (group_hash_h1, group_hash_h2, seq, attnum) and set the L3 bit.
 */

static int64
warm_l3_bits(Oid relid)
{
    char   *l3_table;
    char   *sql;
    int     ret;
    int64   count = 0;
    uint64  nrows;
    uint64  i;

    if (!xpatch_l3_cache_is_enabled(relid))
        return 0;

    l3_table = xpatch_l3_cache_table_name(relid);
    if (l3_table == NULL)
        return 0;

    /* Check if L3 table exists */
    sql = psprintf(
        "SELECT group_hash_h1, group_hash_h2, seq, attnum "
        "FROM %s", l3_table);

    ret = SPI_execute(sql, true, 0);
    pfree(sql);

    if (ret != SPI_OK_SELECT)
    {
        /* L3 table might not exist yet — that's fine */
        pfree(l3_table);
        return 0;
    }

    nrows = SPI_processed;

    for (i = 0; i < nrows; i++)
    {
        bool            isnull;
        int64           seq;
        AttrNumber      attnum;
        uint64          h1, h2;

        /* Check for interrupts periodically */
        if (i > 0 && (i % 10000) == 0)
        {
            CHECK_FOR_INTERRUPTS();
            if (sw_got_sigterm)
                break;
        }

        h1 = DatumGetInt64(
            SPI_getbinval(SPI_tuptable->vals[i],
                          SPI_tuptable->tupdesc, 1, &isnull));
        h2 = DatumGetInt64(
            SPI_getbinval(SPI_tuptable->vals[i],
                          SPI_tuptable->tupdesc, 2, &isnull));
        seq = DatumGetInt64(
            SPI_getbinval(SPI_tuptable->vals[i],
                          SPI_tuptable->tupdesc, 3, &isnull));
        attnum = DatumGetInt16(
            SPI_getbinval(SPI_tuptable->vals[i],
                          SPI_tuptable->tupdesc, 4, &isnull));

        {
            XPatchGroupHash group_hash;
            group_hash.h1 = h1;
            group_hash.h2 = h2;
            xpatch_chain_index_update_bits(relid, group_hash, attnum, seq,
                                           CHAIN_BIT_L3, 0);
        }
        count++;
    }

    pfree(l3_table);
    return count;
}

/* ---------------------------------------------------------------------------
 * Warm all xpatch tables in the current database
 * ---------------------------------------------------------------------------
 * This is the core logic shared by the per-DB worker.  It runs inside an
 * already-established database connection.
 *
 * Returns total entries warmed (chain index + L2) and L3 bits set.
 */

static void
warm_current_database(volatile int64 *out_warmed, volatile int64 *out_l3_bits)
{
    Oid    *relids = NULL;
    volatile int     ntables = 0;
    volatile int     i;

    *out_warmed = 0;
    *out_l3_bits = 0;

    /* Wait for chain index and L2 to be initialized (shmem startup) */
    if (!xpatch_chain_index_is_ready())
    {
        elog(WARNING, "xpatch startup warm: chain index not ready, skipping");
        return;
    }

    /* All work happens in one transaction */
    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    PushActiveSnapshot(GetTransactionSnapshot());

    PG_TRY();
    {
        if (SPI_connect() != SPI_OK_CONNECT)
        {
            elog(WARNING, "xpatch startup warm: SPI_connect failed");
            PopActiveSnapshot();
            CommitTransactionCommand();
            return;
        }

        /* Discover xpatch tables */
        {
            int nt;
            relids = discover_xpatch_tables(&nt);
            ntables = nt;
        }

        SPI_finish();

        if (ntables == 0 || relids == NULL)
        {
            elog(DEBUG1, "xpatch startup warm: no xpatch tables in this database");
            PopActiveSnapshot();
            CommitTransactionCommand();
            return;
        }

        elog(LOG, "xpatch startup warm: found %d xpatch table(s)", (int) ntables);

        /*
         * Phase 1: Single-pass scan — chain index + L2.
         *
         * Each table is warmed in its own subtransaction so that a
         * failure (e.g., old schema missing l3_cache_enabled column)
         * doesn't abort warming for other tables.
         */
        for (i = 0; i < (int) ntables && !sw_got_sigterm; i++)
        {
            MemoryContext per_table_ctx = CurrentMemoryContext;

            BeginInternalSubTransaction(NULL);

            PG_TRY(2);
            {
                int64 n = warm_single_table(relids[i]);
                if (n > 0)
                    *out_warmed += n;

                ReleaseCurrentSubTransaction();
            }
            PG_CATCH(2);
            {
                MemoryContextSwitchTo(per_table_ctx);
                FlushErrorState();
                RollbackAndReleaseCurrentSubTransaction();

                elog(LOG, "xpatch startup warm: skipping table OID %u "
                     "(error during warming)", relids[i]);
            }
            PG_END_TRY(2);
        }

        /* Phase 2: L3 bit scanning */
        if (!sw_got_sigterm)
        {
            /*
             * Need SPI again for L3 table queries.
             * L3 bit scanning uses SPI to read L3 table PKs.
             */
            if (SPI_connect() == SPI_OK_CONNECT)
            {
                for (i = 0; i < (int) ntables && !sw_got_sigterm; i++)
                {
                    MemoryContext l3_ctx = CurrentMemoryContext;

                    BeginInternalSubTransaction(NULL);

                    PG_TRY(2);
                    {
                        int64 n = warm_l3_bits(relids[i]);
                        if (n > 0)
                            *out_l3_bits += n;

                        ReleaseCurrentSubTransaction();
                    }
                    PG_CATCH(2);
                    {
                        MemoryContextSwitchTo(l3_ctx);
                        FlushErrorState();
                        RollbackAndReleaseCurrentSubTransaction();

                        elog(LOG, "xpatch startup warm: skipping L3 bits "
                             "for table OID %u (error)", relids[i]);
                    }
                    PG_END_TRY(2);
                }

                SPI_finish();
            }
        }

        if (relids)
            pfree(relids);

        PopActiveSnapshot();
        CommitTransactionCommand();
    }
    PG_CATCH();
    {
        EmitErrorReport();
        FlushErrorState();
        AbortCurrentTransaction();
    }
    PG_END_TRY();
}

/* ---------------------------------------------------------------------------
 * Per-database worker entry point (dynamic BGW)
 * ---------------------------------------------------------------------------
 * main_arg contains the database OID to connect to.
 */

PGDLLEXPORT void
xpatch_startup_warm_db_worker_main(Datum main_arg)
{
    Oid             dboid = DatumGetObjectId(main_arg);
    volatile int64  total_warmed = 0;
    volatile int64  total_l3_bits = 0;

    /* Set up signal handlers */
    pqsignal(SIGTERM, sw_sigterm_handler);
    pqsignal(SIGHUP, SIG_IGN);

    BackgroundWorkerUnblockSignals();

    /* Connect to the target database */
    BackgroundWorkerInitializeConnectionByOid(dboid, InvalidOid, 0);

    elog(LOG, "xpatch startup warm: per-DB worker started for database OID %u",
         dboid);

    warm_current_database(&total_warmed, &total_l3_bits);

    if (sw_got_sigterm)
        elog(LOG, "xpatch startup warm: per-DB worker interrupted (db OID %u, "
             "warmed " INT64_FORMAT " entries, " INT64_FORMAT " L3 bits)",
             dboid, total_warmed, total_l3_bits);
    else
        elog(LOG, "xpatch startup warm: per-DB worker complete (db OID %u, "
             INT64_FORMAT " chain+L2 entries, " INT64_FORMAT " L3 bits)",
             dboid, total_warmed, total_l3_bits);

    proc_exit(0);
}

/* ---------------------------------------------------------------------------
 * Coordinator: enumerate databases and launch per-DB workers
 * ---------------------------------------------------------------------------
 * Connects to "postgres", queries pg_database for all connectable
 * databases, launches a dynamic BGW for each, waits for them all.
 */

/*
 * Maximum number of databases we'll warm concurrently.  In practice
 * most deployments have 1-5 databases.  We launch them sequentially
 * (wait for each to finish before launching the next) to avoid
 * overwhelming shared memory with concurrent scans.
 */
#define SW_MAX_DATABASES 256

PGDLLEXPORT void
xpatch_startup_warm_worker_main(Datum main_arg)
{
    Oid         db_oids[SW_MAX_DATABASES];
    int         ndb = 0;
    int         ret;
    int         i;

    /* Set up signal handlers */
    pqsignal(SIGTERM, sw_sigterm_handler);
    pqsignal(SIGHUP, SIG_IGN);

    BackgroundWorkerUnblockSignals();

    /* Connect to postgres to enumerate databases */
    BackgroundWorkerInitializeConnection("postgres", NULL, 0);

    elog(LOG, "xpatch startup warming coordinator started");

    /* Enumerate connectable databases */
    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    PushActiveSnapshot(GetTransactionSnapshot());

    PG_TRY();
    {
        if (SPI_connect() != SPI_OK_CONNECT)
        {
            elog(WARNING, "xpatch startup warm: SPI_connect failed");
            PopActiveSnapshot();
            CommitTransactionCommand();
            proc_exit(0);
        }

        ret = SPI_execute(
            "SELECT oid FROM pg_database "
            "WHERE datallowconn AND NOT datistemplate "
            "ORDER BY oid",
            true, SW_MAX_DATABASES);

        if (ret == SPI_OK_SELECT && SPI_processed > 0)
        {
            int count = (int) SPI_processed;
            if (count > SW_MAX_DATABASES)
                count = SW_MAX_DATABASES;

            for (i = 0; i < count; i++)
            {
                bool    isnull;
                Datum   d = SPI_getbinval(SPI_tuptable->vals[i],
                                          SPI_tuptable->tupdesc, 1, &isnull);
                if (!isnull)
                    db_oids[ndb++] = DatumGetObjectId(d);
            }
        }

        SPI_finish();
        PopActiveSnapshot();
        CommitTransactionCommand();
    }
    PG_CATCH();
    {
        EmitErrorReport();
        FlushErrorState();
        AbortCurrentTransaction();
        proc_exit(1);
    }
    PG_END_TRY();

    if (ndb == 0)
    {
        elog(LOG, "xpatch startup warm: no connectable databases found");
        proc_exit(0);
    }

    elog(LOG, "xpatch startup warm: found %d connectable database(s)", ndb);

    /*
     * Launch a per-DB worker for each database, sequentially.
     * We wait for each worker to finish before starting the next to
     * avoid overwhelming shared memory with concurrent bulk scans.
     */
    for (i = 0; i < ndb && !sw_got_sigterm; i++)
    {
        BackgroundWorker        worker;
        BackgroundWorkerHandle *handle;
        BgwHandleStatus         status;

        memset(&worker, 0, sizeof(worker));

        snprintf(worker.bgw_name, BGW_MAXLEN,
                 "xpatch startup warm (db %u)", db_oids[i]);
        snprintf(worker.bgw_type, BGW_MAXLEN,
                 "xpatch startup warming worker");
        snprintf(worker.bgw_function_name, BGW_MAXLEN,
                 "xpatch_startup_warm_db_worker_main");
        snprintf(worker.bgw_library_name, BGW_MAXLEN, "pg_xpatch");

        worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
                           BGWORKER_BACKEND_DATABASE_CONNECTION;
        worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
        worker.bgw_restart_time = BGW_NEVER_RESTART;
        worker.bgw_main_arg = ObjectIdGetDatum(db_oids[i]);
        worker.bgw_notify_pid = MyProcPid;

        if (!RegisterDynamicBackgroundWorker(&worker, &handle))
        {
            elog(LOG, "xpatch startup warm: could not register worker "
                 "for database OID %u (max_worker_processes reached?)",
                 db_oids[i]);
            continue;
        }

        /* Wait for the per-DB worker to finish */
        status = WaitForBackgroundWorkerShutdown(handle);
        if (status == BGWH_POSTMASTER_DIED)
        {
            elog(LOG, "xpatch startup warm: postmaster died, aborting");
            proc_exit(1);
        }

        pfree(handle);
    }

    if (sw_got_sigterm)
        elog(LOG, "xpatch startup warming coordinator interrupted by SIGTERM");
    else
        elog(LOG, "xpatch startup warming complete: warmed %d database(s)", ndb);

    proc_exit(0);
}

/* ---------------------------------------------------------------------------
 * BGW registration (coordinator only — static worker)
 * ---------------------------------------------------------------------------
 */

void
xpatch_startup_warm_register_bgw(void)
{
    BackgroundWorker worker;

    memset(&worker, 0, sizeof(worker));

    snprintf(worker.bgw_name, BGW_MAXLEN,
             "xpatch startup warming coordinator");
    snprintf(worker.bgw_type, BGW_MAXLEN,
             "xpatch startup warming worker");
    snprintf(worker.bgw_function_name, BGW_MAXLEN,
             "xpatch_startup_warm_worker_main");
    snprintf(worker.bgw_library_name, BGW_MAXLEN, "pg_xpatch");

    worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
                       BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = BGW_NEVER_RESTART;
    worker.bgw_main_arg = (Datum) 0;
    worker.bgw_notify_pid = 0;

    RegisterBackgroundWorker(&worker);
}
