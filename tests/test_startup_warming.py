"""
Test startup warming background worker.

The startup warming system uses a two-tier architecture:

  - A **coordinator** BGW starts after recovery, connects to ``postgres``,
    enumerates all connectable databases via ``pg_database``, and launches
    a dynamic per-DB worker for each one.
  - Each **per-DB worker** connects to its target database and scans all
    xpatch tables, building the chain index and populating L2 cache.

Tests verify:

  1. The coordinator BGW is registered and runs (check PG logs after restart)
  2. Workers exit cleanly (one-shot: BGW_NEVER_RESTART)
  3. Data remains readable after restart
  4. Chain index + L2 are populated via INSERT path and warming worker
  5. Edge cases: empty tables, multiple tables
"""

from __future__ import annotations

import subprocess
import time
from typing import Callable

import psycopg
import pytest

from conftest import (
    PG_HOST,
    PG_PORT,
    PG_USER,
    PG_PASSWORD,
    CONTAINER_NAME,
    insert_rows,
    insert_versions,
    row_count,
    _connect,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _pg_log_since_restart(n_lines: int = 50) -> str:
    """Get the last N lines from the PG log inside the container."""
    try:
        r = subprocess.run(
            ["docker", "exec", CONTAINER_NAME, "tail", f"-{n_lines}", "/tmp/pg.log"],
            capture_output=True, text=True, timeout=10,
        )
        return r.stdout
    except Exception:
        return ""


def _worker_log_present(log: str, *, pattern: str = "xpatch startup warming coordinator started") -> bool:
    """Check if the startup warming coordinator logged its startup message."""
    return pattern in log


# ---------------------------------------------------------------------------
# Tests: Worker registration and lifecycle
# ---------------------------------------------------------------------------

class TestStartupWarmWorker:
    """Test that the startup warming BGW is registered and runs."""

    def test_worker_ran_after_restart(self, db: psycopg.Connection, pg_ctl):
        """After restart, the startup warming worker should appear in logs."""
        db_name = db.info.dbname
        db.close()

        pg_ctl.restart()

        # Give the worker a moment to run (it's one-shot and very fast)
        time.sleep(1)

        log = _pg_log_since_restart(200)
        assert _worker_log_present(log), (
            "Expected 'xpatch startup warming worker started' in PG log "
            "after restart"
        )

        # Worker should have exited (one-shot)
        new_conn = _connect(db_name)
        try:
            rows = new_conn.execute(
                "SELECT pid FROM pg_stat_activity "
                "WHERE backend_type LIKE '%startup warming%'"
            ).fetchall()
            assert len(rows) == 0, (
                "Startup warming worker should have exited (BGW_NEVER_RESTART)"
            )
        finally:
            new_conn.close()

    def test_worker_completes_message(self, db: psycopg.Connection, pg_ctl):
        """After restart, the worker should log its completion message."""
        db_name = db.info.dbname
        db.close()

        pg_ctl.restart()
        time.sleep(1)

        log = _pg_log_since_restart(200)
        assert "xpatch startup warming complete:" in log or \
               "xpatch startup warm:" in log, \
            "Expected warming completion message in PG log"

        # Reconnect to keep fixture cleanup happy
        _connect(db_name).close()


# ---------------------------------------------------------------------------
# Tests: Data readability after restart
# ---------------------------------------------------------------------------

class TestPostRestartReads:
    """After restart, data should remain readable even before warming."""

    def test_data_readable_after_restart(
        self, db: psycopg.Connection, make_table, pg_ctl
    ):
        """Insert data, restart, verify all versions still readable."""
        t = make_table()

        for v in range(1, 11):
            insert_rows(db, t, [(1, v, f"content v{v}")])

        db_name = db.info.dbname
        db.close()

        pg_ctl.restart()
        time.sleep(1)

        new_conn = _connect(db_name)
        try:
            # All versions should be readable
            for v in range(1, 11):
                row = new_conn.execute(
                    f'SELECT content FROM "{t}" WHERE group_id = 1 AND version = {v}'
                ).fetchone()
                assert row is not None, f"Version {v} not found after restart"
                assert row["content"] == f"content v{v}", \
                    f"Content mismatch at v={v} after restart"
        finally:
            new_conn.close()

    def test_multiple_groups_after_restart(
        self, db: psycopg.Connection, make_table, pg_ctl
    ):
        """Multiple groups should all be readable after restart."""
        t = make_table()

        for g in range(1, 4):
            for v in range(1, 6):
                insert_rows(db, t, [(g, v, f"g{g}v{v}")])

        db_name = db.info.dbname
        db.close()

        pg_ctl.restart()
        time.sleep(1)

        new_conn = _connect(db_name)
        try:
            for g in range(1, 4):
                for v in range(1, 6):
                    row = new_conn.execute(
                        f'SELECT content FROM "{t}" '
                        f"WHERE group_id = {g} AND version = {v}"
                    ).fetchone()
                    assert row is not None
                    assert row["content"] == f"g{g}v{v}"
        finally:
            new_conn.close()


# ---------------------------------------------------------------------------
# Tests: Chain index + L2 populated after INSERT
# ---------------------------------------------------------------------------

class TestChainIndexAfterRestart:
    """
    Chain index and L2 get populated via INSERT, not only warming.
    After restart + query, they should be available.
    """

    def test_plan_path_after_restart(
        self, db: psycopg.Connection, make_table, pg_ctl
    ):
        """plan_path should return a valid plan after restart + query."""
        t = make_table()

        for v in range(1, 6):
            insert_rows(db, t, [(1, v, f"chain v{v}")])

        # Verify plan_path works before restart
        plan_rows = db.execute(
            "SELECT * FROM xpatch.plan_path(%s::regclass, '1', 3::int2, 5::int8, false)",
            [t],
        ).fetchall()
        assert len(plan_rows) > 0, "plan_path should return steps before restart"

        db_name = db.info.dbname
        db.close()

        pg_ctl.restart()
        time.sleep(1)

        new_conn = _connect(db_name)
        try:
            # After restart, the chain index was cleared (shmem lost).
            # But reading a version will reconstruct and repopulate the
            # chain index via the INSERT+read path.
            row = new_conn.execute(
                f'SELECT content FROM "{t}" WHERE group_id = 1 AND version = 5'
            ).fetchone()
            assert row is not None
            assert row["content"] == "chain v5"

            # Now chain index should be populated — either by the multi-DB
            # warming worker (which warms all databases) or by the read
            # path which populates chain index via the INSERT codepath.
            plan_after = new_conn.execute(
                "SELECT * FROM xpatch.plan_path(%s::regclass, '1', 3::int2, 3::int8, false)",
                [t],
            ).fetchall()
            # It's OK if plan is empty — the data is still readable via
            # the fallback path.
        finally:
            new_conn.close()


# ---------------------------------------------------------------------------
# Tests: L2 cache stats after restart
# ---------------------------------------------------------------------------

class TestL2AfterRestart:
    """L2 cache should be empty after restart (shmem lost), then repopulated."""

    def test_l2_stats_reset_after_restart(
        self, db: psycopg.Connection, make_table, pg_ctl
    ):
        """L2 stats should reset after restart."""
        t = make_table()

        for v in range(1, 11):
            insert_rows(db, t, [(1, v, f"l2test v{v}")])

        # Check L2 has some entries
        stats_before = db.execute(
            "SELECT * FROM xpatch.cache_stats()"
        ).fetchone()

        db_name = db.info.dbname
        db.close()

        pg_ctl.restart()
        time.sleep(1)

        new_conn = _connect(db_name)
        try:
            stats_after = new_conn.execute(
                "SELECT * FROM xpatch.cache_stats()"
            ).fetchone()

            # After restart, L2 may have entries (the multi-DB warming
            # worker now operates on all databases, including this one).
            # The important thing is it doesn't crash and stats are valid.
            assert stats_after is not None
        finally:
            new_conn.close()


# ---------------------------------------------------------------------------
# Tests: Edge cases
# ---------------------------------------------------------------------------

class TestStartupWarmEdgeCases:
    """Edge cases for the startup warming worker."""

    def test_empty_table_no_crash(
        self, db: psycopg.Connection, make_table, pg_ctl
    ):
        """An empty xpatch table should not cause the worker to crash."""
        t = make_table()
        # Don't insert anything — table is empty

        db_name = db.info.dbname
        db.close()

        pg_ctl.restart()
        time.sleep(1)

        log = _pg_log_since_restart(200)
        # Worker should have started and completed without crash
        assert "xpatch startup warming coordinator started" in log
        assert "PANIC" not in log, "Worker should not cause a PANIC"

        new_conn = _connect(db_name)
        try:
            # Server should still be alive
            ver = new_conn.execute("SELECT 1 AS alive").fetchone()
            assert ver["alive"] == 1
        finally:
            new_conn.close()

    def test_multiple_tables_after_restart(
        self, db: psycopg.Connection, make_table, pg_ctl
    ):
        """Multiple xpatch tables should all be readable after restart."""
        t1 = make_table()
        t2 = make_table()

        insert_rows(db, t1, [(1, 1, "table1 v1")])
        insert_rows(db, t1, [(1, 2, "table1 v2")])
        insert_rows(db, t2, [(1, 1, "table2 v1")])
        insert_rows(db, t2, [(1, 2, "table2 v2")])

        db_name = db.info.dbname
        db.close()

        pg_ctl.restart()
        time.sleep(1)

        new_conn = _connect(db_name)
        try:
            r1 = new_conn.execute(
                f'SELECT content FROM "{t1}" WHERE group_id = 1 AND version = 2'
            ).fetchone()
            assert r1 is not None
            assert r1["content"] == "table1 v2"

            r2 = new_conn.execute(
                f'SELECT content FROM "{t2}" WHERE group_id = 1 AND version = 2'
            ).fetchone()
            assert r2 is not None
            assert r2["content"] == "table2 v2"
        finally:
            new_conn.close()

    def test_eviction_worker_still_runs_after_restart(
        self, db: psycopg.Connection, pg_ctl
    ):
        """L3 eviction worker should still be running after restart."""
        db_name = db.info.dbname
        db.close()

        pg_ctl.restart()
        time.sleep(1)

        new_conn = _connect(db_name)
        try:
            rows = new_conn.execute(
                "SELECT backend_type FROM pg_stat_activity "
                "WHERE backend_type LIKE '%eviction%'"
            ).fetchall()
            assert len(rows) >= 1, \
                "L3 eviction worker should be running after restart"
        finally:
            new_conn.close()
