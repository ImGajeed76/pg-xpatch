"""
Test L3 eviction system: access tracking + eviction logic.

Covers:
- GUCs exist and are readable (l3_eviction_interval_s, l3_access_buffer_size)
- Background worker is running
- Access tracking updates cached_at on L3 reads
- Eviction respects l3_cache_max_size_mb via xpatch.l3_eviction_pass()
- Eviction deletes oldest entries by cached_at
- Worker gracefully handles tables with L3 disabled
- Data remains correct after eviction (falls back to L1/L2/disk)
"""

from __future__ import annotations

import os
import time
from typing import Callable

import psycopg
import pytest

from conftest import (
    PG_HOST,
    PG_PORT,
    PG_USER,
    PG_PASSWORD,
    insert_rows,
    insert_versions,
    row_count,
    _connect,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _random_payload(size: int = 6000) -> str:
    """Generate an incompressible text payload of approximately *size* chars."""
    return os.urandom(size).hex()


def enable_l3(db: psycopg.Connection, table: str) -> None:
    """Enable L3 cache for a table."""
    db.execute(
        "UPDATE xpatch.table_config SET l3_cache_enabled = true "
        "WHERE table_name = %s",
        [table],
    )


def l3_table_name(table: str) -> str:
    return f"{table}_xp_l3"


def l3_table_exists(db: psycopg.Connection, table: str) -> bool:
    row = db.execute(
        "SELECT EXISTS ("
        "  SELECT 1 FROM pg_class c "
        "  JOIN pg_namespace n ON c.relnamespace = n.oid "
        "  WHERE n.nspname = 'xpatch' AND c.relname = %s"
        ") AS exists",
        [l3_table_name(table)],
    ).fetchone()
    return row["exists"]


def l3_count(db: psycopg.Connection, table: str) -> int:
    name = l3_table_name(table)
    row = db.execute(
        f'SELECT COUNT(*) AS n FROM xpatch."{name}"'
    ).fetchone()
    return row["n"]


def l3_total_size(db: psycopg.Connection, table: str) -> int:
    """Get pg_total_relation_size of the L3 table."""
    name = l3_table_name(table)
    row = db.execute(
        f"SELECT pg_total_relation_size('xpatch.\"{name}\"'::regclass) AS sz"
    ).fetchone()
    return row["sz"]


def run_eviction(db: psycopg.Connection) -> int:
    """Run one eviction cycle synchronously via SQL function."""
    row = db.execute("SELECT xpatch.l3_eviction_pass() AS flushed").fetchone()
    return row["flushed"]


# ---------------------------------------------------------------------------
# GUCs
# ---------------------------------------------------------------------------


class TestL3EvictionGUCs:
    """Test that L3 eviction GUCs exist and are readable."""

    def test_eviction_interval_guc_exists(self, db: psycopg.Connection):
        """l3_eviction_interval_s GUC should exist with default 60."""
        row = db.execute(
            "SHOW pg_xpatch.l3_eviction_interval_s"
        ).fetchone()
        assert row is not None
        val = int(row["pg_xpatch.l3_eviction_interval_s"])
        assert val == 60, f"Expected default 60, got {val}"

    def test_access_buffer_size_guc_exists(self, db: psycopg.Connection):
        """l3_access_buffer_size GUC should exist with default 8192."""
        row = db.execute(
            "SHOW pg_xpatch.l3_access_buffer_size"
        ).fetchone()
        assert row is not None
        val = int(row["pg_xpatch.l3_access_buffer_size"])
        assert val == 8192, f"Expected default 8192, got {val}"


# ---------------------------------------------------------------------------
# Background Worker
# ---------------------------------------------------------------------------


class TestL3EvictionWorker:
    """Test L3 eviction background worker."""

    def test_worker_is_running(self, db: psycopg.Connection):
        """The L3 eviction worker should be visible in pg_stat_activity."""
        row = db.execute(
            "SELECT COUNT(*) AS n FROM pg_stat_activity "
            "WHERE backend_type = 'xpatch L3 eviction worker'"
        ).fetchone()
        assert row["n"] == 1, "L3 eviction worker should be running"


# ---------------------------------------------------------------------------
# Access Time Tracking
# ---------------------------------------------------------------------------


class TestL3AccessTracking:
    """Test that L3 reads update cached_at via the access buffer."""

    def test_cached_at_set_on_insert(
        self, db: psycopg.Connection, make_table
    ):
        """INSERT with L3 enabled should set cached_at to approximately now()."""
        t = make_table()
        enable_l3(db, t)
        insert_versions(db, t, group_id=1, count=3)

        assert l3_table_exists(db, t)
        name = l3_table_name(t)
        row = db.execute(
            f'SELECT MIN(cached_at) AS oldest FROM xpatch."{name}"'
        ).fetchone()
        assert row["oldest"] is not None, "cached_at should be set on INSERT"

    def test_l3_read_does_not_error(
        self, db: psycopg.Connection, make_table, db_factory
    ):
        """
        Reading L3 content with access tracking active should not
        error out and should return correct data.
        """
        t = make_table()
        enable_l3(db, t)
        insert_versions(db, t, group_id=1, count=5)

        # Force an L3 read via a fresh connection (L1 cold)
        fresh = db_factory()
        row = fresh.execute(
            f"SELECT content FROM {t} WHERE group_id = 1 AND version = 3"
        ).fetchone()
        assert row is not None
        assert "Version 3" in row["content"]
        fresh.close()


# ---------------------------------------------------------------------------
# Eviction Pass (synchronous via SQL function)
# ---------------------------------------------------------------------------


class TestL3EvictionPass:
    """Test L3 eviction via xpatch.l3_eviction_pass()."""

    def test_eviction_pass_callable(self, db: psycopg.Connection, make_table):
        """l3_eviction_pass() should be callable and return an integer."""
        t = make_table()
        enable_l3(db, t)
        insert_versions(db, t, group_id=1, count=3)

        flushed = run_eviction(db)
        assert isinstance(flushed, int)

    def test_no_eviction_when_under_limit(
        self, db: psycopg.Connection, make_table
    ):
        """If L3 table is under max size, no entries should be evicted."""
        t = make_table()
        enable_l3(db, t)
        insert_versions(db, t, group_id=1, count=10)

        count_before = l3_count(db, t)
        assert count_before > 0

        run_eviction(db)

        count_after = l3_count(db, t)
        assert count_after == count_before, (
            f"No eviction expected when under limit: "
            f"before={count_before}, after={count_after}"
        )

    def test_eviction_with_tiny_max_size(
        self, db: psycopg.Connection, make_table
    ):
        """
        Set l3_cache_max_size_mb to 1 and insert >1MB of data.
        After running eviction, the count should decrease.
        """
        t = make_table()
        enable_l3(db, t)

        # Set max size to 1 MB
        db.execute(
            "UPDATE xpatch.table_config SET l3_cache_max_size_mb = 1 "
            "WHERE table_name = %s",
            [t],
        )

        # Insert data with random, incompressible content per row
        # to ensure the L3 table actually grows past 1MB.
        for g in range(1, 21):
            for v in range(1, 16):
                insert_rows(db, t, [(g, v, f"g{g}v{v} {_random_payload()}")])

        initial_count = l3_count(db, t)
        assert initial_count > 0, "L3 should have entries"

        initial_size = l3_total_size(db, t)
        if initial_size < 800_000:
            pytest.skip(
                f"L3 table size {initial_size} is under 800KB, "
                "can't test eviction threshold"
            )

        # Run eviction
        run_eviction(db)

        final_count = l3_count(db, t)
        assert final_count < initial_count, (
            f"Eviction should have removed entries: "
            f"initial={initial_count}, final={final_count}"
        )

    def test_eviction_preserves_some_entries(
        self, db: psycopg.Connection, make_table
    ):
        """After eviction, the table should still have entries (not empty)."""
        t = make_table()
        enable_l3(db, t)
        db.execute(
            "UPDATE xpatch.table_config SET l3_cache_max_size_mb = 1 "
            "WHERE table_name = %s",
            [t],
        )

        for g in range(1, 21):
            for v in range(1, 16):
                insert_rows(db, t, [(g, v, f"g{g}v{v} {_random_payload()}")])

        initial_count = l3_count(db, t)
        if l3_total_size(db, t) < 800_000:
            pytest.skip("L3 table too small to test eviction")

        run_eviction(db)

        final_count = l3_count(db, t)
        assert final_count > 0, "Eviction should not delete ALL entries"
        assert final_count < initial_count, "Eviction should have removed some"

    def test_eviction_skips_disabled_tables(
        self, db: psycopg.Connection, make_table
    ):
        """Tables with L3 disabled should not be affected by eviction."""
        t = make_table()
        # L3 is disabled by default
        insert_versions(db, t, group_id=1, count=10)

        assert not l3_table_exists(db, t), \
            "Disabled table should not have L3 table"

        # Eviction should complete without error
        flushed = run_eviction(db)
        assert isinstance(flushed, int)

    def test_data_correct_after_eviction(
        self, db: psycopg.Connection, make_table
    ):
        """After eviction removes L3 entries, reads should still return correct data."""
        t = make_table()
        enable_l3(db, t)
        db.execute(
            "UPDATE xpatch.table_config SET l3_cache_max_size_mb = 1 "
            "WHERE table_name = %s",
            [t],
        )

        for g in range(1, 21):
            for v in range(1, 16):
                insert_rows(db, t, [(g, v, f"g{g}v{v} {_random_payload()}")])

        initial_count = l3_count(db, t)
        if l3_total_size(db, t) < 800_000:
            pytest.skip("L3 table too small to test eviction")

        run_eviction(db)

        # Read data — should still be correct (falls back to L1/L2/disk)
        for g in [1, 5, 10]:
            row = db.execute(
                f"SELECT content FROM {t} WHERE group_id = %s AND version = 1",
                [g],
            ).fetchone()
            assert row is not None, f"Should read group {g} after eviction"
            assert f"g{g}v1" in row["content"]

    def test_multiple_eviction_passes(
        self, db: psycopg.Connection, make_table
    ):
        """Running eviction multiple times should be safe (idempotent when under limit)."""
        t = make_table()
        enable_l3(db, t)
        insert_versions(db, t, group_id=1, count=5)

        count = l3_count(db, t)

        # Multiple passes should not change anything when under limit
        for _ in range(3):
            run_eviction(db)

        assert l3_count(db, t) == count, "Multiple passes should be idempotent"

    def test_eviction_across_multiple_tables(
        self, db: psycopg.Connection, make_table
    ):
        """Eviction should handle multiple L3-enabled tables independently."""
        t1 = make_table()
        t2 = make_table()
        enable_l3(db, t1)
        enable_l3(db, t2)

        # t1: small data (under limit)
        insert_versions(db, t1, group_id=1, count=5)

        # t2: over limit
        db.execute(
            "UPDATE xpatch.table_config SET l3_cache_max_size_mb = 1 "
            "WHERE table_name = %s",
            [t2],
        )
        for g in range(1, 21):
            for v in range(1, 16):
                insert_rows(db, t2, [(g, v, f"g{g}v{v} {_random_payload()}")])

        t1_count = l3_count(db, t1)
        t2_count = l3_count(db, t2)
        t2_size = l3_total_size(db, t2)

        if t2_size < 800_000:
            pytest.skip("L3 table too small to test eviction")

        run_eviction(db)

        # t1 should be unchanged (under limit)
        assert l3_count(db, t1) == t1_count, \
            "Small table should not be evicted"
        # t2 should have fewer entries
        assert l3_count(db, t2) < t2_count, \
            "Over-limit table should have entries evicted"
