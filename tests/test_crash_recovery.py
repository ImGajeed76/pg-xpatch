"""
Test crash recovery: INSERT/DELETE survive PostgreSQL kill + restart.

All tests here are marked @pytest.mark.crash_test and run LAST, sequentially.
They use pg_ctl.kill() to simulate an unclean crash (SIGKILL to postmaster),
then pg_ctl.start() to bring PG back up, and verify data integrity.

IMPORTANT: These tests affect the shared PostgreSQL server. They must NOT
run in parallel with other tests. Run with:
    pytest -m crash_test -p no:xdist

Covers:
- Committed INSERT survives crash
- Committed DELETE survives crash
- CHECKPOINT + crash preserves data
- Uncommitted transaction lost after crash
- Data integrity after recovery (delta chain across keyframes)
- Multi-group crash recovery
- Config metadata survives crash
"""

from __future__ import annotations

import time

import psycopg
import pytest

from conftest import _connect, row_count, insert_rows


pytestmark = pytest.mark.crash_test


def _wait_for_pg(pg_ctl, timeout: int = 30) -> None:
    """Wait for PostgreSQL to become ready after start."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pg_ctl.is_ready():
            return
        time.sleep(0.5)
    raise TimeoutError(f"PostgreSQL not ready after {timeout}s")


def _reconnect(dbname: str, retries: int = 10, delay: float = 1.0) -> psycopg.Connection:
    """Reconnect to a database with retries (PG may still be recovering)."""
    for attempt in range(retries):
        try:
            return _connect(dbname)
        except psycopg.OperationalError:
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                raise
    raise RuntimeError("unreachable")  # guard for retries=0


def _crash_and_recover(pg_ctl, timeout: int = 30) -> None:
    """Kill PostgreSQL, verify it's down, then restart and wait."""
    pg_ctl.kill()
    time.sleep(0.5)
    assert not pg_ctl.is_ready(), "PostgreSQL should be down after SIGKILL"
    pg_ctl.start()
    _wait_for_pg(pg_ctl, timeout=timeout)


class TestInsertSurvivesCrash:
    """Committed INSERT survives kill + restart."""

    def test_insert_survives_crash(self, db: psycopg.Connection, make_table, pg_ctl):
        """Committed rows survive an unclean crash."""
        t = make_table()
        dbname = db.info.dbname

        # Insert and commit (autocommit=True: each INSERT flushes WAL at commit)
        for v in range(1, 6):
            insert_rows(db, t, [(1, v, f"crash-test-v{v}")])

        db.close()
        _crash_and_recover(pg_ctl)

        conn = _reconnect(dbname)
        try:
            cnt = row_count(conn, t)
            assert cnt == 5

            rows = conn.execute(
                f"SELECT version, content FROM {t} ORDER BY version"
            ).fetchall()
            for row in rows:
                assert row["content"] == f"crash-test-v{row['version']}"
        finally:
            conn.close()


class TestDeleteSurvivesCrash:
    """Committed DELETE survives kill + restart."""

    def test_delete_survives_crash(self, db: psycopg.Connection, make_table, pg_ctl):
        """Committed DELETE (cascade) persists after crash."""
        t = make_table()
        dbname = db.info.dbname

        # Insert 10 rows, delete from v6 (cascade removes v6..v10)
        for v in range(1, 11):
            insert_rows(db, t, [(1, v, f"v{v}")])

        db.execute(f"DELETE FROM {t} WHERE group_id = 1 AND version = 6")

        db.close()
        _crash_and_recover(pg_ctl)

        conn = _reconnect(dbname)
        try:
            cnt = row_count(conn, t)
            assert cnt == 5

            rows = conn.execute(
                f"SELECT version FROM {t} ORDER BY version"
            ).fetchall()
            assert [r["version"] for r in rows] == [1, 2, 3, 4, 5]
        finally:
            conn.close()


class TestCheckpointCrash:
    """CHECKPOINT followed by crash — data fully on disk."""

    def test_checkpoint_then_crash(self, db: psycopg.Connection, make_table, pg_ctl):
        """Data inserted before CHECKPOINT survives crash."""
        t = make_table()
        dbname = db.info.dbname

        for v in range(1, 4):
            insert_rows(db, t, [(1, v, f"pre-checkpoint-{v}")])

        db.execute("CHECKPOINT")

        db.close()
        _crash_and_recover(pg_ctl)

        conn = _reconnect(dbname)
        try:
            cnt = row_count(conn, t)
            assert cnt == 3

            rows = conn.execute(
                f"SELECT version, content FROM {t} ORDER BY version"
            ).fetchall()
            for row in rows:
                assert row["content"] == f"pre-checkpoint-{row['version']}"
        finally:
            conn.close()


class TestUncommittedLostAfterCrash:
    """Uncommitted transaction is lost after crash."""

    def test_uncommitted_lost(self, db: psycopg.Connection, make_table, pg_ctl):
        """Rows in an uncommitted transaction are gone after crash.

        NOTE: db.close() before kill sends a clean termination that triggers
        a server-side ROLLBACK. In practice this tests "rolled back data is
        gone after recovery" which is the same end result — the uncommitted
        data doesn't survive. A true mid-transaction crash would require
        killing PG while the connection is still open, but that leaves the
        test client socket in a bad state, making it harder to test reliably.
        """
        t = make_table()
        dbname = db.info.dbname

        # Insert committed rows
        insert_rows(db, t, [(1, 1, "committed")])

        # Start a transaction but don't commit
        db.execute("BEGIN")
        insert_rows(db, t, [(1, 2, "uncommitted")])
        # Don't commit — just crash

        db.close()
        _crash_and_recover(pg_ctl)

        conn = _reconnect(dbname)
        try:
            cnt = row_count(conn, t)
            assert cnt == 1  # Only the committed row

            row = conn.execute(f"SELECT content FROM {t}").fetchone()
            assert row["content"] == "committed"
        finally:
            conn.close()


class TestDataIntegrityAfterCrash:
    """Full data integrity verification after crash."""

    def test_content_correct_after_recovery(self, db: psycopg.Connection, make_table, pg_ctl):
        """Delta-compressed content reconstructs correctly after recovery."""
        t = make_table(keyframe_every=3)
        dbname = db.info.dbname

        # Insert enough rows to span multiple keyframes (keyframes at 1, 4, 7, 10)
        for v in range(1, 11):
            insert_rows(db, t, [(1, v, f"Recovery test version {v}: {'x' * v * 50}")])

        db.close()
        _crash_and_recover(pg_ctl)

        conn = _reconnect(dbname)
        try:
            rows = conn.execute(
                f"SELECT version, content FROM {t} ORDER BY version"
            ).fetchall()
            assert len(rows) == 10
            for row in rows:
                v = row["version"]
                expected = f"Recovery test version {v}: {'x' * v * 50}"
                assert row["content"] == expected, f"Content mismatch at v{v}"
        finally:
            conn.close()

    def test_multi_group_survives_crash(self, db: psycopg.Connection, make_table, pg_ctl):
        """Data from multiple groups is intact after crash recovery."""
        t = make_table()
        dbname = db.info.dbname

        for gid in range(1, 4):
            for v in range(1, 6):
                insert_rows(db, t, [(gid, v, f"g{gid}-v{v}")])

        db.close()
        _crash_and_recover(pg_ctl)

        conn = _reconnect(dbname)
        try:
            total = row_count(conn, t)
            assert total == 15

            for gid in range(1, 4):
                rows = conn.execute(
                    f"SELECT version, content FROM {t} "
                    f"WHERE group_id = {gid} ORDER BY version"
                ).fetchall()
                assert len(rows) == 5, f"Group {gid}: expected 5 rows, got {len(rows)}"
                for row in rows:
                    v = row["version"]
                    assert row["content"] == f"g{gid}-v{v}"
        finally:
            conn.close()

    def test_config_survives_crash(self, db: psycopg.Connection, make_table, pg_ctl):
        """xpatch.get_config() returns correct config after crash recovery."""
        t = make_table(keyframe_every=7, compress_depth=2)
        dbname = db.info.dbname

        insert_rows(db, t, [(1, 1, "config-test")])
        db.close()
        _crash_and_recover(pg_ctl)

        conn = _reconnect(dbname)
        try:
            config = conn.execute(
                f"SELECT * FROM xpatch.get_config('{t}'::regclass)"
            ).fetchone()
            assert config is not None
            assert config["group_by"] == "group_id"
            assert config["order_by"] == "version"
            assert config["keyframe_every"] == 7
            assert config["compress_depth"] == 2
        finally:
            conn.close()
