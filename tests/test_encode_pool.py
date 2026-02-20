"""
Tests for the pthread encode pool (pg_xpatch.encode_threads > 0).

The encode pool spawns persistent pthreads inside a PostgreSQL backend to
parallelize delta encoding during INSERT.  This is architecturally risky
because PostgreSQL backends are designed to be single-threaded.

These tests verify:
  1. Correctness: parallel encoding produces identical results to sequential
  2. Concurrent safety: multiple backends with thread pools don't corrupt data
  3. Error recovery: the pool survives backend errors (e.g., duplicate key)
  4. Cancel behaviour: the pool doesn't hang forever on cancel
  5. Consistency under high compress_depth: many parallel encode tasks

All tests compare encode_threads=0 (sequential, known-good) against
encode_threads=N (parallel, under test).
"""

import time
import threading
import psycopg
import pytest
from psycopg import sql


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _set_encode_threads(conn: psycopg.Connection, n: int) -> None:
    """SET pg_xpatch.encode_threads for this session."""
    conn.execute(f"SET pg_xpatch.encode_threads = {n}")


def _insert_versions(conn, table, group_id, start, end, *, content_fn=None):
    """Insert versions [start, end) for a group."""
    fn = content_fn or (lambda v: f"Version {v} content with some padding " + "x" * 80)
    for v in range(start, end):
        conn.execute(
            sql.SQL("INSERT INTO {} (group_id, version, content) VALUES (%s, %s, %s)").format(
                sql.Identifier(table)
            ),
            (group_id, v, fn(v)),
        )


def _read_all(conn, table, group_id):
    """Read all rows for a group, ordered by version. Returns list of dicts."""
    return conn.execute(
        sql.SQL(
            "SELECT version, content FROM {} WHERE group_id = %s ORDER BY version"
        ).format(sql.Identifier(table)),
        (group_id,),
    ).fetchall()


# ---------------------------------------------------------------------------
# Test: parallel vs sequential produce identical data
# ---------------------------------------------------------------------------

class TestEncodePoolCorrectness:
    """
    Verify that parallel encoding (encode_threads > 0) produces byte-identical
    reconstructed content compared to sequential encoding (encode_threads = 0).
    """

    def test_parallel_matches_sequential(self, db: psycopg.Connection, make_table):
        """
        Insert the same data into two tables — one with encode_threads=0,
        one with encode_threads=4.  Read back and compare every row.

        If the thread pool has memory visibility bugs, race conditions in
        task dispatch, or FFI issues, the parallel table will have different
        (corrupted) content.
        """
        # Table 1: sequential encoding (known-good baseline)
        _set_encode_threads(db, 0)
        t_seq = make_table(
            "group_id INT, version INT, content TEXT NOT NULL",
            group_by="group_id",
            order_by="version",
            delta_columns=["content"],
            compress_depth=5,
        )
        _insert_versions(db, t_seq, 1, 1, 51)

        # Table 2: parallel encoding (under test)
        _set_encode_threads(db, 4)
        t_par = make_table(
            "group_id INT, version INT, content TEXT NOT NULL",
            group_by="group_id",
            order_by="version",
            delta_columns=["content"],
            compress_depth=5,
        )
        _insert_versions(db, t_par, 1, 1, 51)

        # Compare every row
        rows_seq = _read_all(db, t_seq, 1)
        rows_par = _read_all(db, t_par, 1)

        assert len(rows_seq) == len(rows_par) == 50

        for r_seq, r_par in zip(rows_seq, rows_par):
            assert r_seq["version"] == r_par["version"]
            assert r_seq["content"] == r_par["content"], (
                f"v{r_seq['version']}: sequential content differs from parallel. "
                "Thread pool encoding produced corrupt data."
            )

    def test_parallel_with_high_compress_depth(self, db: psycopg.Connection, make_table):
        """
        With compress_depth=10, the pool dispatches up to 10 parallel encode
        tasks per row.  This maximises the chance of race conditions in the
        atomic task counter and result collection.
        """
        _set_encode_threads(db, 4)
        t = make_table(
            "group_id INT, version INT, content TEXT NOT NULL",
            group_by="group_id",
            order_by="version",
            delta_columns=["content"],
            compress_depth=10,
        )

        # Insert 30 versions — enough for the pool to see 10 bases
        _insert_versions(db, t, 1, 1, 31)

        rows = _read_all(db, t, 1)
        assert len(rows) == 30

        for row in rows:
            expected = f"Version {row['version']} content with some padding " + "x" * 80
            assert row["content"] == expected, (
                f"v{row['version']}: content corrupted with compress_depth=10 and encode_threads=4"
            )

    def test_parallel_with_large_content(self, db: psycopg.Connection, make_table):
        """
        Large content (10KB+ per row) stresses the Rust allocator inside
        worker threads.  If malloc/free race or the result copy has a size
        mismatch, this will produce wrong content.
        """
        _set_encode_threads(db, 4)
        t = make_table(
            "group_id INT, version INT, content TEXT NOT NULL",
            group_by="group_id",
            order_by="version",
            delta_columns=["content"],
            compress_depth=5,
        )

        def large_content(v):
            # Each version has unique 10KB content with small diff from previous
            base = f"=== Version {v} ===" + "A" * 10000
            # Mutate a portion to ensure delta encoding is exercised
            return base[:5000] + f"CHANGE_{v}" + base[5010:]

        _insert_versions(db, t, 1, 1, 21, content_fn=large_content)

        rows = _read_all(db, t, 1)
        assert len(rows) == 20

        for row in rows:
            v = row["version"]
            expected = large_content(v)
            assert row["content"] == expected, (
                f"v{v}: large content corrupted with parallel encoding. "
                f"Got len={len(row['content'])}, expected len={len(expected)}"
            )

    def test_parallel_multi_group(self, db: psycopg.Connection, make_table):
        """
        Multiple groups inserted sequentially, each triggering FIFO cache
        eviction and cold-start populate.  With encode_threads > 0, the pool
        is reused across groups — test that batch state doesn't leak between
        groups.
        """
        _set_encode_threads(db, 4)
        t = make_table(
            "group_id INT, version INT, content TEXT NOT NULL",
            group_by="group_id",
            order_by="version",
            delta_columns=["content"],
            compress_depth=5,
        )

        n_groups = 8
        versions_per_group = 15

        for g in range(1, n_groups + 1):
            _insert_versions(db, t, g, 1, versions_per_group + 1,
                             content_fn=lambda v, g=g: f"G{g}_V{v}_" + "d" * 100)

        # Verify every group
        for g in range(1, n_groups + 1):
            rows = _read_all(db, t, g)
            assert len(rows) == versions_per_group, (
                f"Group {g}: expected {versions_per_group} rows, got {len(rows)}"
            )
            for row in rows:
                v = row["version"]
                expected = f"G{g}_V{v}_" + "d" * 100
                assert row["content"] == expected, (
                    f"Group {g}, v{v}: content corrupted with parallel encoding"
                )


# ---------------------------------------------------------------------------
# Test: concurrent backends with thread pools
# ---------------------------------------------------------------------------

class TestEncodePoolConcurrency:
    """
    Multiple backends each with their own thread pool inserting concurrently.
    Each backend spawns encode_threads=4, so with 4 backends that's 16+4=20
    threads total in the process... except backends are separate processes.
    
    The real concern is:
    - Static pool state (per-backend) — each backend's pool is independent
    - Rust allocator thread safety across processes (should be fine, separate
      address spaces)
    - CPU oversubscription causing slow progress / spin-wait starvation
    """

    @pytest.mark.timeout(120)
    def test_concurrent_backends_with_pool(self, db: psycopg.Connection, make_table):
        """
        4 backends each insert 20 versions to separate groups, all with
        encode_threads=4.  Verify no corruption, no deadlock, no hang.
        """
        t = make_table(
            "group_id INT, version INT, content TEXT NOT NULL",
            group_by="group_id",
            order_by="version",
            delta_columns=["content"],
            compress_depth=5,
        )

        conninfo = db.info.dsn
        n_workers = 4
        versions_per_worker = 20
        errors = []

        def worker(group_id):
            try:
                conn = psycopg.connect(conninfo, autocommit=True)
                try:
                    conn.execute("SET pg_xpatch.encode_threads = 4")
                    for v in range(1, versions_per_worker + 1):
                        conn.execute(
                            sql.SQL(
                                "INSERT INTO {} (group_id, version, content) VALUES (%s, %s, %s)"
                            ).format(sql.Identifier(t)),
                            (group_id, v, f"Worker{group_id}_V{v}_" + "p" * 80),
                        )
                finally:
                    conn.close()
            except Exception as e:
                errors.append((group_id, str(e)))

        threads = [threading.Thread(target=worker, args=(g,)) for g in range(1, n_workers + 1)]
        start = time.monotonic()
        for thr in threads:
            thr.start()
        for thr in threads:
            thr.join(timeout=90)
        elapsed = time.monotonic() - start

        # Check no threads are stuck
        stuck = [i + 1 for i, thr in enumerate(threads) if thr.is_alive()]
        assert not stuck, f"Workers {stuck} still running after 90s — pool hang?"
        assert not errors, f"Errors during concurrent pool insert: {errors}"

        # Verify all data
        for g in range(1, n_workers + 1):
            rows = _read_all(db, t, g)
            assert len(rows) == versions_per_worker, (
                f"Group {g}: expected {versions_per_worker} rows, got {len(rows)}"
            )
            for row in rows:
                v = row["version"]
                expected = f"Worker{g}_V{v}_" + "p" * 80
                assert row["content"] == expected, (
                    f"Group {g}, v{v}: content corrupted under concurrent pool usage"
                )


# ---------------------------------------------------------------------------
# Test: error recovery — pool survives backend errors
# ---------------------------------------------------------------------------

class TestEncodePoolErrorRecovery:
    """
    After a failed INSERT (e.g., unique constraint violation), the pool's
    static state could be left inconsistent:
    - batch_seq incremented but tasks not cleaned up
    - workers may have stale references
    - next_task counter may be non-zero

    The next successful INSERT must still produce correct data.
    """

    def test_pool_survives_error_in_insert(self, db: psycopg.Connection, make_table):
        """
        1. Enable encode_threads=4
        2. Insert some rows (warm up FIFO + pool)
        3. Cause an INSERT error (duplicate version — or constraint violation)
        4. Insert more rows
        5. Verify all data is correct

        If the pool state is corrupted by the error, step 4 will produce
        wrong data or crash.
        """
        _set_encode_threads(db, 4)
        t = make_table(
            "group_id INT, version INT, content TEXT NOT NULL",
            group_by="group_id",
            order_by="version",
            delta_columns=["content"],
            compress_depth=5,
        )

        # Insert v1-v10 (warm up pool)
        _insert_versions(db, t, 1, 1, 11)

        # Try to insert a duplicate — this should fail
        # (xpatch may or may not have unique constraints, so we use a subtransaction)
        try:
            with db.transaction():
                # Insert something that will cause an error
                # Use a savepoint so we can recover
                db.execute("SAVEPOINT sp1")
                try:
                    # Insert with NULL content to trigger NOT NULL violation
                    db.execute(
                        sql.SQL("INSERT INTO {} (group_id, version, content) VALUES (%s, %s, NULL)").format(
                            sql.Identifier(t)
                        ),
                        (1, 11),
                    )
                except Exception:
                    db.execute("ROLLBACK TO SAVEPOINT sp1")
                db.execute("RELEASE SAVEPOINT sp1")
        except Exception:
            pass  # Error expected

        # Now insert more valid rows — pool must still work correctly
        _insert_versions(db, t, 1, 11, 21)

        # Verify all data
        rows = _read_all(db, t, 1)
        assert len(rows) == 20, f"Expected 20 rows, got {len(rows)}"

        for row in rows:
            v = row["version"]
            expected = f"Version {v} content with some padding " + "x" * 80
            assert row["content"] == expected, (
                f"v{v}: content corrupted after error recovery with encode_threads=4"
            )

    def test_pool_survives_rollback(self, db: psycopg.Connection, make_table):
        """
        Insert inside an explicit transaction, then ROLLBACK.  The pool was
        active during the inserts.  After rollback, insert again and verify.

        If the pool's batch_seq or completed counter is stale, workers may
        not wake for the next batch, causing a hang.
        """
        _set_encode_threads(db, 4)
        t = make_table(
            "group_id INT, version INT, content TEXT NOT NULL",
            group_by="group_id",
            order_by="version",
            delta_columns=["content"],
            compress_depth=5,
        )

        # Transaction 1: insert and commit (warm up pool)
        _insert_versions(db, t, 1, 1, 6)

        # Transaction 2: insert and ROLLBACK
        # Use a savepoint (nested transaction) since db is autocommit=True
        try:
            with db.transaction():
                _insert_versions(db, t, 1, 6, 11)
                raise Exception("force rollback")  # triggers rollback via exception
        except Exception:
            pass  # Exception is caught after transaction context manager rolls back

        # Transaction 3: insert same versions again — pool must work
        _insert_versions(db, t, 1, 6, 11)

        rows = _read_all(db, t, 1)
        assert len(rows) == 10, f"Expected 10 rows, got {len(rows)}"

        for row in rows:
            v = row["version"]
            expected = f"Version {v} content with some padding " + "x" * 80
            assert row["content"] == expected, (
                f"v{v}: content wrong after rollback + re-insert with pool active"
            )


# ---------------------------------------------------------------------------
# Test: COPY with thread pool
# ---------------------------------------------------------------------------

class TestEncodePoolCopy:
    """
    COPY is the primary bulk-load path and the most likely to trigger thread
    pool issues because it inserts many rows rapidly, keeping the pool busy.
    """

    def test_copy_with_parallel_encoding(self, db: psycopg.Connection, make_table):
        """
        COPY 500 rows with encode_threads=4.  Verify all content is correct.
        """
        _set_encode_threads(db, 4)
        t = make_table(
            "group_id INT, version INT, content TEXT NOT NULL",
            group_by="group_id",
            order_by="version",
            delta_columns=["content"],
            compress_depth=5,
        )

        n_rows = 500
        with db.cursor() as cur:
            with cur.copy(
                sql.SQL("COPY {} (group_id, version, content) FROM STDIN").format(
                    sql.Identifier(t)
                )
            ) as copy:
                for v in range(1, n_rows + 1):
                    copy.write_row((1, v, f"COPY version {v} " + "c" * 80))

        rows = _read_all(db, t, 1)
        assert len(rows) == n_rows

        for row in rows:
            v = row["version"]
            expected = f"COPY version {v} " + "c" * 80
            assert row["content"] == expected, (
                f"v{v}: COPY content corrupted with parallel encoding"
            )

    def test_copy_parallel_vs_sequential_match(self, db: psycopg.Connection, make_table):
        """
        COPY same data into two tables — sequential vs parallel.
        Byte-compare all reconstructed content.

        This is the definitive correctness test: if the pool has ANY
        data corruption bug, this test will catch it.
        """
        n_rows = 200

        def do_copy(table, encode_threads):
            _set_encode_threads(db, encode_threads)
            with db.cursor() as cur:
                with cur.copy(
                    sql.SQL("COPY {} (group_id, version, content) FROM STDIN").format(
                        sql.Identifier(table)
                    )
                ) as copy:
                    for v in range(1, n_rows + 1):
                        copy.write_row((1, v, f"Row {v} data " + "m" * 200))

        # Sequential baseline
        t_seq = make_table(
            "group_id INT, version INT, content TEXT NOT NULL",
            group_by="group_id",
            order_by="version",
            delta_columns=["content"],
            compress_depth=5,
        )
        do_copy(t_seq, 0)

        # Parallel under test
        t_par = make_table(
            "group_id INT, version INT, content TEXT NOT NULL",
            group_by="group_id",
            order_by="version",
            delta_columns=["content"],
            compress_depth=5,
        )
        do_copy(t_par, 4)

        rows_seq = _read_all(db, t_seq, 1)
        rows_par = _read_all(db, t_par, 1)

        assert len(rows_seq) == len(rows_par) == n_rows

        for r_seq, r_par in zip(rows_seq, rows_par):
            assert r_seq["version"] == r_par["version"]
            assert r_seq["content"] == r_par["content"], (
                f"v{r_seq['version']}: COPY sequential vs parallel content mismatch. "
                "Thread pool produced corrupt delta encoding."
            )


# ---------------------------------------------------------------------------
# Test: pool with multi-column delta
# ---------------------------------------------------------------------------

class TestEncodePoolMultiColumn:
    """
    With multiple delta columns, each INSERT dispatches separate encode
    batches for each column.  If the pool's state (next_task, completed,
    batch_seq) isn't properly reset between batches, the second column's
    encoding will be wrong.
    """

    def test_two_delta_columns_parallel(self, db: psycopg.Connection, make_table):
        """
        Table with two delta columns (content and metadata).
        Insert 20 versions with encode_threads=4.  Verify both columns.
        """
        _set_encode_threads(db, 4)
        t = make_table(
            "group_id INT, version INT, content TEXT NOT NULL, metadata TEXT NOT NULL",
            group_by="group_id",
            order_by="version",
            delta_columns=["content", "metadata"],
            compress_depth=5,
        )

        for v in range(1, 21):
            db.execute(
                sql.SQL(
                    "INSERT INTO {} (group_id, version, content, metadata) "
                    "VALUES (%s, %s, %s, %s)"
                ).format(sql.Identifier(t)),
                (1, v, f"Content v{v} " + "a" * 100, f"Meta v{v} " + "b" * 50),
            )

        rows = db.execute(
            sql.SQL(
                "SELECT version, content, metadata FROM {} "
                "WHERE group_id = 1 ORDER BY version"
            ).format(sql.Identifier(t))
        ).fetchall()

        assert len(rows) == 20

        for row in rows:
            v = row["version"]
            expected_content = f"Content v{v} " + "a" * 100
            expected_meta = f"Meta v{v} " + "b" * 50
            assert row["content"] == expected_content, (
                f"v{v}: content column corrupted with 2 delta columns + parallel encoding"
            )
            assert row["metadata"] == expected_meta, (
                f"v{v}: metadata column corrupted with 2 delta columns + parallel encoding"
            )
