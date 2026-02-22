"""
Regression test for the encode pool race condition (permanent hang).

Fixed in 0.6.2 by adding atomic_int workers_in_flight to the EncodePool struct.
Workers increment it when entering the task loop and decrement when exiting.
The main thread drains all stragglers (workers_in_flight == 0) before resetting
next_task/completed for the next batch.  Also added CHECK_FOR_INTERRUPTS() to
the completion spin-wait for cancellability.

Original bug (pre-0.6.2):
  In xpatch_encode_pool.c, function xpatch_encode_pool_execute(), there was a
  race window between resetting next_task and completed counters.  A straggler
  worker from the previous batch could grab a task from the new batch (after
  next_task was reset to 0), complete it, and increment completed.  Then the
  completed = 0 reset wiped that increment, causing the spin-wait
  (while completed < num_tasks) to hang forever.

Test strategy (mirrors the original bug report):
  - 5 delta columns: 1 large (message, ~300 bytes) + 4 tiny (names/emails, ~15 bytes)
  - The large-to-small column transition is critical: stragglers from the slow
    message encode are still in the inner loop when the fast author_name batch
    is set up
  - compress_depth=50, keyframe_every=100 -> ~50 tasks per batch, 99% delta rows
  - encode_threads = min(cpu_count, 32) -- more threads = more stragglers
  - COPY path for rapid back-to-back dispatches (~5000 dispatches per second)
  - Hang detected via separate thread + join timeout

See: tmp/BUG_REPORT_encode_pool_race_condition.md
"""

import os
import threading

import psycopg
import pytest
from psycopg import sql

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Minimum cores required to have a realistic chance of triggering the race.
# With fewer cores, stragglers are unlikely to be scheduled on a separate core
# during the ~5-20ns window between the two atomic stores.
MIN_CORES = 4

# How long to wait for a single COPY batch before declaring it hung (seconds).
# A 5000-row COPY with 5 delta columns typically completes in <10s on 16 cores.
# If it takes >45s, the backend is almost certainly in a spin-wait.
HANG_TIMEOUT = 45

# Number of rows per COPY batch.  With 5 delta columns, compress_depth=50,
# and keyframe_every=100, this generates:
#   5000 * 0.99 * 5 = ~24,750 pool dispatches per batch
N_ROWS = 5000

# Number of COPY batches (each to a fresh table).  Each batch is an independent
# chance to trigger the race.  Total dispatches: N_BATCHES * ~24,750.
N_BATCHES = 10


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_conninfo(db: psycopg.Connection) -> str:
    """Extract connection string from fixture connection."""
    return db.info.dsn


def _copy_worker(
    conninfo: str,
    table: str,
    n_rows: int,
    encode_threads: int,
) -> str | None:
    """
    Run a COPY into *table* on a fresh connection.

    Returns None on success, or an error message string on failure.
    This runs in a daemon thread so the main thread can detect hangs.
    """
    try:
        conn = psycopg.connect(
            conninfo,
            autocommit=True,
            options="-c statement_timeout=0",  # disable — useless for this bug
        )
        try:
            conn.execute(f"SET pg_xpatch.encode_threads = {encode_threads}")
            with conn.cursor() as cur:
                with cur.copy(
                    sql.SQL(
                        "COPY {} (group_id, version, message, "
                        "author_name, author_email, "
                        "committer_name, committer_email) "
                        "FROM STDIN"
                    ).format(sql.Identifier(table))
                ) as copy:
                    for v in range(1, n_rows + 1):
                        copy.write_row((
                            1,
                            v,
                            # Large message (~300 bytes) — slow delta encode.
                            # Stragglers from this column are still in the inner
                            # while(1) loop when the next column's batch starts.
                            f"commit {v}: Merge branch 'feature-{v % 100}' into main "
                            + "x" * 250,
                            # Tiny columns (~10-20 bytes) — encode in nanoseconds.
                            # A straggler grabs task 0, encodes it before
                            # completed is reset to 0, and the increment is lost.
                            f"Author {v % 100}",
                            f"a{v % 100}@example.com",
                            f"Committer {v % 50}",
                            f"c{v % 50}@example.com",
                        ))
        finally:
            conn.close()
    except Exception as e:
        return f"{type(e).__name__}: {e}"
    return None


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestEncodePoolRaceCondition:
    """
    Regression tests for the next_task/completed reset race condition.

    Fixed in 0.6.2 by adding an ``atomic_int workers_in_flight`` counter:
    workers increment it when entering the task loop and decrement when
    exiting.  The main thread drains all stragglers before resetting
    next_task/completed for the next batch.

    These tests verify the fix holds under aggressive conditions:
    ~250,000 pool dispatches with large→small column transitions, high
    compress_depth, and max encode_threads.

    On machines with < MIN_CORES CPUs the test is skipped entirely, since
    the race is virtually impossible to trigger without parallel core
    scheduling.
    """

    @pytest.mark.timeout(0)  # disable pytest timeout — we handle it ourselves
    def test_copy_high_threads_multi_column_hang_detection(
        self, db: psycopg.Connection, make_table,
    ):
        """
        Primary hang-detection test.

        Mirrors the exact reproduction from the bug report:
        - 5 delta columns (1 large message + 4 tiny name/email)
        - compress_depth=50, keyframe_every=100
        - encode_threads = min(cpu_count, 32)
        - COPY of N_ROWS rows, repeated N_BATCHES times

        The COPY runs in a daemon thread on a separate connection.  If the
        thread doesn't complete within HANG_TIMEOUT seconds, the backend is
        stuck in the spin-wait at line 404 and we fail the test.
        """
        cpu_count = os.cpu_count() or 1
        if cpu_count < MIN_CORES:
            pytest.skip(
                f"Need >= {MIN_CORES} CPU cores to trigger the race, have {cpu_count}"
            )

        encode_threads = min(cpu_count, 32)
        conninfo = _get_conninfo(db)
        hung_batch = None

        for batch_idx in range(N_BATCHES):
            t = make_table(
                "group_id INT, version INT, "
                "message TEXT NOT NULL, "
                "author_name TEXT NOT NULL, "
                "author_email TEXT NOT NULL, "
                "committer_name TEXT NOT NULL, "
                "committer_email TEXT NOT NULL",
                group_by="group_id",
                order_by="version",
                delta_columns=[
                    "message", "author_name", "author_email",
                    "committer_name", "committer_email",
                ],
                compress_depth=50,
                keyframe_every=100,
            )

            error_box: list[str | None] = [None]

            def target(tbl=t) -> None:
                error_box[0] = _copy_worker(conninfo, tbl, N_ROWS, encode_threads)

            thr = threading.Thread(target=target, daemon=True)
            thr.start()
            thr.join(timeout=HANG_TIMEOUT)

            if thr.is_alive():
                hung_batch = batch_idx
                # The backend is hung.  We can't cleanly kill it (no
                # CHECK_FOR_INTERRUPTS), but we can report the failure.
                # The daemon thread will be abandoned when the test process exits.
                break

            if error_box[0] is not None:
                pytest.fail(
                    f"Batch {batch_idx}: COPY failed with error: {error_box[0]}"
                )

        if hung_batch is not None:
            dispatches_before_hang = (hung_batch + 1) * int(N_ROWS * 0.99) * 5
            pytest.fail(
                f"COPY hung on batch {hung_batch}/{N_BATCHES} after ~{dispatches_before_hang} "
                f"pool dispatches — encode pool race condition triggered.\n"
                f"Backend is in spin-wait at xpatch_encode_pool.c "
                f"(while completed < num_tasks).\n"
                f"encode_threads={encode_threads}, compress_depth=50, "
                f"keyframe_every=100, {N_ROWS} rows/batch.\n"
                f"See BUG_REPORT_encode_pool_race_condition.md"
            )

        # All batches completed — verify data integrity of the last table.
        # Even if the race didn't cause a hang, a straggler may have written
        # results to the wrong task slot, corrupting output silently.
        rows = db.execute(
            sql.SQL(
                "SELECT version, message, author_name, author_email, "
                "committer_name, committer_email "
                "FROM {} WHERE group_id = 1 ORDER BY version"
            ).format(sql.Identifier(t))
        ).fetchall()

        assert len(rows) == N_ROWS, f"Expected {N_ROWS} rows, got {len(rows)}"

        for row in rows:
            v = row["version"]
            expected_msg = (
                f"commit {v}: Merge branch 'feature-{v % 100}' into main " + "x" * 250
            )
            assert row["message"] == expected_msg, f"v{v}: message corrupted"
            assert row["author_name"] == f"Author {v % 100}", f"v{v}: author_name corrupted"
            assert row["author_email"] == f"a{v % 100}@example.com", f"v{v}: author_email corrupted"
            assert row["committer_name"] == f"Committer {v % 50}", f"v{v}: committer_name corrupted"
            assert row["committer_email"] == f"c{v % 50}@example.com", f"v{v}: committer_email corrupted"

    @pytest.mark.timeout(0)  # disable pytest timeout — we handle it ourselves
    def test_rapid_insert_multi_column_hang_detection(
        self, db: psycopg.Connection, make_table,
    ):
        """
        Sequential INSERT variant (not COPY).

        Each INSERT is a separate execute() call, creating a clean batch
        boundary.  The pool must reset between every INSERT — this is where
        the straggler race fires.  Uses 2 delta columns (large + small) and
        compress_depth=50 for ~50 tasks per batch.
        """
        cpu_count = os.cpu_count() or 1
        if cpu_count < MIN_CORES:
            pytest.skip(
                f"Need >= {MIN_CORES} CPU cores to trigger the race, have {cpu_count}"
            )

        encode_threads = min(cpu_count, 32)
        conninfo = _get_conninfo(db)

        t = make_table(
            "group_id INT, version INT, "
            "content TEXT NOT NULL, metadata TEXT NOT NULL",
            group_by="group_id",
            order_by="version",
            delta_columns=["content", "metadata"],
            compress_depth=50,
            keyframe_every=100,
        )

        n_rows = 2000
        error_box: list[str | None] = [None]

        def target() -> None:
            try:
                conn = psycopg.connect(
                    conninfo,
                    autocommit=True,
                    options="-c statement_timeout=0",
                )
                try:
                    conn.execute(f"SET pg_xpatch.encode_threads = {encode_threads}")
                    for v in range(1, n_rows + 1):
                        conn.execute(
                            sql.SQL(
                                "INSERT INTO {} (group_id, version, content, metadata) "
                                "VALUES (%s, %s, %s, %s)"
                            ).format(sql.Identifier(t)),
                            (
                                1, v,
                                # Large content (~200 bytes)
                                f"Content version {v} " + "y" * 180,
                                # Tiny metadata (~15 bytes)
                                f"meta-{v % 50}",
                            ),
                        )
                finally:
                    conn.close()
            except Exception as e:
                error_box[0] = f"{type(e).__name__}: {e}"

        thr = threading.Thread(target=target, daemon=True)
        thr.start()
        thr.join(timeout=HANG_TIMEOUT * 2)  # INSERTs are slower than COPY

        if thr.is_alive():
            pytest.fail(
                f"INSERT sequence hung after {HANG_TIMEOUT * 2}s — encode pool race "
                f"condition triggered during individual INSERT path.\n"
                f"encode_threads={encode_threads}, compress_depth=50, {n_rows} rows."
            )

        if error_box[0] is not None:
            pytest.fail(f"INSERT sequence failed: {error_box[0]}")

        # Verify integrity
        rows = db.execute(
            sql.SQL(
                "SELECT version, content, metadata FROM {} "
                "WHERE group_id = 1 ORDER BY version"
            ).format(sql.Identifier(t))
        ).fetchall()

        assert len(rows) == n_rows, f"Expected {n_rows} rows, got {len(rows)}"
        for row in rows:
            v = row["version"]
            assert row["content"] == f"Content version {v} " + "y" * 180, (
                f"v{v}: content corrupted"
            )
            assert row["metadata"] == f"meta-{v % 50}", f"v{v}: metadata corrupted"

    @pytest.mark.timeout(0)  # disable pytest timeout — we handle it ourselves
    def test_parallel_vs_sequential_correctness(
        self, db: psycopg.Connection, make_table,
    ):
        """
        Correctness comparison: COPY identical data with encode_threads=0
        (sequential, known-good) vs encode_threads=N (parallel, under test).

        Even if the race doesn't cause a hang, a straggler writing to the
        wrong task slot can silently corrupt delta output.  This catches
        that case by byte-comparing all reconstructed content.
        """
        cpu_count = os.cpu_count() or 1
        if cpu_count < MIN_CORES:
            pytest.skip(
                f"Need >= {MIN_CORES} CPU cores to trigger the race, have {cpu_count}"
            )

        encode_threads = min(cpu_count, 32)
        conninfo = _get_conninfo(db)
        n_rows = 3000

        def make_and_copy(threads: int) -> str:
            t = make_table(
                "group_id INT, version INT, "
                "message TEXT NOT NULL, author TEXT NOT NULL, email TEXT NOT NULL",
                group_by="group_id",
                order_by="version",
                delta_columns=["message", "author", "email"],
                compress_depth=50,
                keyframe_every=100,
            )

            error_box: list[str | None] = [None]

            def target() -> None:
                error_box[0] = _copy_worker_3col(
                    conninfo, t, n_rows, threads,
                )

            thr = threading.Thread(target=target, daemon=True)
            thr.start()
            thr.join(timeout=HANG_TIMEOUT)

            if thr.is_alive():
                pytest.fail(
                    f"COPY hung with encode_threads={threads} — race condition triggered"
                )
            if error_box[0] is not None:
                pytest.fail(f"COPY failed (threads={threads}): {error_box[0]}")

            return t

        t_seq = make_and_copy(0)
        t_par = make_and_copy(encode_threads)

        rows_seq = db.execute(
            sql.SQL(
                "SELECT version, message, author, email FROM {} "
                "WHERE group_id = 1 ORDER BY version"
            ).format(sql.Identifier(t_seq))
        ).fetchall()

        rows_par = db.execute(
            sql.SQL(
                "SELECT version, message, author, email FROM {} "
                "WHERE group_id = 1 ORDER BY version"
            ).format(sql.Identifier(t_par))
        ).fetchall()

        assert len(rows_seq) == len(rows_par) == n_rows

        for r_seq, r_par in zip(rows_seq, rows_par):
            v = r_seq["version"]
            assert r_seq["message"] == r_par["message"], (
                f"v{v}: message mismatch (sequential vs parallel, {encode_threads} threads)"
            )
            assert r_seq["author"] == r_par["author"], (
                f"v{v}: author mismatch (sequential vs parallel, {encode_threads} threads)"
            )
            assert r_seq["email"] == r_par["email"], (
                f"v{v}: email mismatch (sequential vs parallel, {encode_threads} threads)"
            )


def _copy_worker_3col(
    conninfo: str,
    table: str,
    n_rows: int,
    encode_threads: int,
) -> str | None:
    """COPY worker for the 3-column (message, author, email) schema."""
    try:
        conn = psycopg.connect(
            conninfo,
            autocommit=True,
            options="-c statement_timeout=0",
        )
        try:
            conn.execute(f"SET pg_xpatch.encode_threads = {encode_threads}")
            with conn.cursor() as cur:
                with cur.copy(
                    sql.SQL(
                        "COPY {} (group_id, version, message, author, email) "
                        "FROM STDIN"
                    ).format(sql.Identifier(table))
                ) as copy:
                    for v in range(1, n_rows + 1):
                        copy.write_row((
                            1, v,
                            f"commit {v}: " + "z" * 250,
                            f"Dev {v % 80}",
                            f"dev{v % 80}@co.com",
                        ))
        finally:
            conn.close()
    except Exception as e:
        return f"{type(e).__name__}: {e}"
    return None
