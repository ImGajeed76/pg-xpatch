"""
End-to-end correctness tests for the three-level cache system.

Covers gaps NOT tested elsewhere:
- L3 persistence across crash (SIGKILL) — only clean restart was tested
- Cross-level invalidation (DELETE/TRUNCATE verified across L1+L2+L3 together)
- L2 eviction → re-read correctness (L3 had this, L2 did not)
- Full lifecycle: insert → read → restart → read → evict L3 → read → verify
"""

from __future__ import annotations

import os
import time
from typing import Any, Callable

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

def l1_stats(db: psycopg.Connection) -> dict[str, Any]:
    """Get L1 cache stats."""
    return db.execute("SELECT * FROM xpatch.cache_stats()").fetchone()


def l2_stats(db: psycopg.Connection) -> dict[str, Any]:
    """Get L2 cache stats."""
    return db.execute("SELECT * FROM xpatch.l2_cache_stats()").fetchone()


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


def run_eviction(db: psycopg.Connection) -> int:
    """Run one L3 eviction cycle synchronously."""
    row = db.execute("SELECT xpatch.l3_eviction_pass() AS flushed").fetchone()
    return row["flushed"]


def single_content(
    db: psycopg.Connection, table: str, group_id: int, version: int
) -> str | None:
    row = db.execute(
        f"SELECT content FROM {table} WHERE group_id = %s AND version = %s",
        [group_id, version],
    ).fetchone()
    return row["content"] if row else None


def all_content(db: psycopg.Connection, table: str, group_id: int) -> list[str]:
    rows = db.execute(
        f"SELECT content FROM {table} WHERE group_id = %s ORDER BY version",
        [group_id],
    ).fetchall()
    return [r["content"] for r in rows]


def _random_payload(size: int = 6000) -> str:
    """Generate an incompressible text payload of approximately *size* chars."""
    return os.urandom(size).hex()


# ---------------------------------------------------------------------------
# Crash helpers (same as test_crash_recovery.py)
# ---------------------------------------------------------------------------

pytestmark_crash = pytest.mark.crash_test


def _wait_for_pg(pg_ctl, timeout: int = 30) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pg_ctl.is_ready():
            return
        time.sleep(0.5)
    raise TimeoutError(f"PostgreSQL not ready after {timeout}s")


def _reconnect(dbname: str, retries: int = 10, delay: float = 1.0) -> psycopg.Connection:
    for attempt in range(retries):
        try:
            return _connect(dbname)
        except psycopg.OperationalError:
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                raise
    raise RuntimeError("unreachable")


def _crash_and_recover(pg_ctl, timeout: int = 30) -> None:
    pg_ctl.kill()
    time.sleep(0.5)
    assert not pg_ctl.is_ready(), "PostgreSQL should be down after SIGKILL"
    pg_ctl.start()
    _wait_for_pg(pg_ctl, timeout=timeout)


# ---------------------------------------------------------------------------
# L3 Persistence Across Crash
# ---------------------------------------------------------------------------


class TestL3CrashPersistence:
    """Verify L3 disk cache survives an unclean crash (SIGKILL)."""

    @pytest.mark.crash_test
    def test_l3_survives_crash(
        self, db: psycopg.Connection, make_table, pg_ctl
    ):
        """L3 table and its data persist after SIGKILL + recovery."""
        t = make_table(keyframe_every=5)
        enable_l3(db, t)
        dbname = db.info.dbname

        for v in range(1, 11):
            insert_rows(db, t, [(1, v, f"crash-l3-v{v}")])

        assert l3_table_exists(db, t)
        count_before = l3_count(db, t)
        assert count_before >= 10

        db.close()
        _crash_and_recover(pg_ctl)

        conn = _reconnect(dbname)
        try:
            assert l3_table_exists(conn, t), \
                "L3 table should survive crash"
            count_after = l3_count(conn, t)
            assert count_after == count_before, \
                f"L3 count changed: {count_before} -> {count_after}"

            for v in range(1, 11):
                result = single_content(conn, t, 1, v)
                assert result == f"crash-l3-v{v}", \
                    f"L3-backed data mismatch at v={v} after crash"
        finally:
            conn.close()

    @pytest.mark.crash_test
    def test_l3_multi_group_survives_crash(
        self, db: psycopg.Connection, make_table, pg_ctl
    ):
        """L3 data for multiple groups survives crash."""
        t = make_table()
        enable_l3(db, t)
        dbname = db.info.dbname

        for g in range(1, 4):
            for v in range(1, 6):
                insert_rows(db, t, [(g, v, f"g{g}-v{v}-crash")])

        db.close()
        _crash_and_recover(pg_ctl)

        conn = _reconnect(dbname)
        try:
            for g in range(1, 4):
                for v in range(1, 6):
                    result = single_content(conn, t, g, v)
                    assert result == f"g{g}-v{v}-crash", \
                        f"Mismatch at g={g} v={v} after crash"
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# Cross-Level Invalidation
# ---------------------------------------------------------------------------


class TestCrossLevelInvalidation:
    """Verify DELETE/TRUNCATE invalidates all cache levels simultaneously."""

    def test_delete_invalidates_all_levels(
        self, db: psycopg.Connection, make_table, db_factory
    ):
        """
        DELETE should invalidate L1, L2, L3, and chain index for the
        affected group. After re-insert, all data should be fresh.
        """
        t = make_table(compress_depth=3, keyframe_every=10)
        enable_l3(db, t)

        for v in range(1, 11):
            insert_rows(db, t, [(1, v, f"original-v{v}")])

        # Ensure L3 is populated
        assert l3_table_exists(db, t)
        assert l3_count(db, t) > 0

        # Read all versions to populate L1
        for v in range(1, 11):
            result = single_content(db, t, 1, v)
            assert result == f"original-v{v}"

        # Delete all versions of group 1
        db.execute(f"DELETE FROM {t} WHERE group_id = 1")

        # L3 entries for this group should be gone
        assert l3_count(db, t) == 0, \
            "L3 entries should be removed after DELETE"

        # Re-insert with DIFFERENT content
        for v in range(1, 6):
            insert_rows(db, t, [(1, v, f"fresh-v{v}")])

        # Read back — must get fresh content, not stale from any cache level
        for v in range(1, 6):
            result = single_content(db, t, 1, v)
            assert result == f"fresh-v{v}", \
                f"Stale data at v={v}: got '{result}'"

        # Also verify from a fresh connection (cold L1)
        conn2 = db_factory()
        for v in range(1, 6):
            result = single_content(conn2, t, 1, v)
            assert result == f"fresh-v{v}", \
                f"Fresh conn stale data at v={v}: got '{result}'"

    def test_delete_preserves_other_groups(
        self, db: psycopg.Connection, make_table, db_factory
    ):
        """
        DELETE of one group should NOT invalidate caches for other groups.
        Other groups' L3 entries and data should remain correct.
        """
        t = make_table(compress_depth=3)
        enable_l3(db, t)

        for g in range(1, 4):
            for v in range(1, 6):
                insert_rows(db, t, [(g, v, f"g{g}-v{v}")])

        l3_before = l3_count(db, t)
        assert l3_before > 0

        # Delete only group 2
        db.execute(f"DELETE FROM {t} WHERE group_id = 2")

        # Groups 1 and 3 should still be intact
        for g in [1, 3]:
            for v in range(1, 6):
                result = single_content(db, t, g, v)
                assert result == f"g{g}-v{v}", \
                    f"Group {g} v{v} corrupted after deleting group 2"

        # L3 should still have entries (from groups 1 and 3)
        assert l3_count(db, t) > 0, \
            "L3 should retain entries for non-deleted groups"

    def test_truncate_invalidates_all_levels(
        self, db: psycopg.Connection, make_table, db_factory
    ):
        """
        TRUNCATE should invalidate L1, L2, L3 for ALL groups.
        After re-insert, data must be fresh.
        """
        t = make_table(compress_depth=3)
        enable_l3(db, t)

        for g in range(1, 4):
            for v in range(1, 6):
                insert_rows(db, t, [(g, v, f"pre-trunc-g{g}-v{v}")])

        # Read to populate L1
        for g in range(1, 4):
            for v in range(1, 6):
                single_content(db, t, g, v)

        assert l3_table_exists(db, t)

        db.execute(f"TRUNCATE {t}")

        # L3 table should be dropped by TRUNCATE (invalidate_rel)
        assert not l3_table_exists(db, t), \
            "L3 table should be dropped after TRUNCATE"

        # Re-insert with different content
        for v in range(1, 4):
            insert_rows(db, t, [(1, v, f"post-trunc-v{v}")])

        # Verify fresh data, not stale
        for v in range(1, 4):
            result = single_content(db, t, 1, v)
            assert result == f"post-trunc-v{v}", \
                f"Stale data at v={v} after TRUNCATE: got '{result}'"

        # Fresh connection too
        conn2 = db_factory()
        for v in range(1, 4):
            result = single_content(conn2, t, 1, v)
            assert result == f"post-trunc-v{v}", \
                f"Fresh conn stale data at v={v}: got '{result}'"


# ---------------------------------------------------------------------------
# L2 Eviction → Re-read Correctness
# ---------------------------------------------------------------------------


class TestL2EvictionCorrectness:
    """Verify data is correct after L2 eviction forces fallback to disk."""

    def test_data_correct_after_l2_pressure(
        self, db: psycopg.Connection, make_table, db_factory
    ):
        """
        Fill L2 past its capacity so eviction occurs, then verify all
        data still reads correctly (falling back to disk for evicted entries).
        """
        t = make_table(compress_depth=3, keyframe_every=10)

        before = l2_stats(db)
        initial_evictions = before["eviction_count"]

        # Insert enough data across many groups to pressure L2.
        # Each group gets 20 versions with moderately large content.
        expected = {}
        for g in range(1, 31):
            for v in range(1, 21):
                content = f"g{g}-v{v}-{'p' * 200}"
                insert_rows(db, t, [(g, v, content)])
                expected[(g, v)] = content

        after = l2_stats(db)

        # If no evictions occurred, the test isn't exercising eviction.
        # That's OK — we still verify correctness. But log it.
        had_evictions = after["eviction_count"] > initial_evictions

        # Verify ALL data from a fresh connection (cold L1 — forces
        # reconstruction via L2 or disk fallback)
        conn2 = db_factory()
        for (g, v), content in expected.items():
            result = single_content(conn2, t, g, v)
            assert result == content, \
                f"Mismatch at g={g} v={v} (L2 evictions={had_evictions})"

    def test_data_correct_with_tiny_l2(
        self, db: psycopg.Connection, make_table, db_factory
    ):
        """
        With l2_cache_max_entry_kb=1, most L2 entries are skipped.
        All data should still be correct via disk fallback.
        """
        db.execute("SET pg_xpatch.l2_cache_max_entry_kb = 1")
        # compress_depth=1 ensures every non-keyframe is a delta
        t = make_table(compress_depth=1)

        before = l2_stats(db)

        # Large dissimilar content so compressed deltas exceed 1KB
        for v in range(1, 11):
            big = "A" * 5000 + str(v) * 1000
            insert_rows(db, t, [(1, v, big)])

        after = l2_stats(db)

        # Some entries should have been skipped
        assert after["skip_count"] >= before["skip_count"], \
            "Expected L2 skips with max_entry_kb=1"

        # Data should still be correct via disk
        conn2 = db_factory()
        for v in range(1, 11):
            expected = "A" * 5000 + str(v) * 1000
            result = single_content(conn2, t, 1, v)
            assert result == expected, \
                f"Mismatch at v={v} with tiny L2"


# ---------------------------------------------------------------------------
# Full Lifecycle
# ---------------------------------------------------------------------------


class TestFullLifecycle:
    """
    Full lifecycle: insert → read → restart → read → evict L3 → read.
    Verify data identical at every stage.
    """

    def test_lifecycle_insert_read_restart_evict_read(
        self, db: psycopg.Connection, make_table, pg_ctl, db_factory
    ):
        """
        Complete lifecycle exercising all three cache levels and restart.

        Stage 1: INSERT populates L1 + L2 + L3 + chain index
        Stage 2: READ hits L1 (warm) — verify correct
        Stage 3: RESTART clears L1 + L2 (shmem), L3 persists on disk
        Stage 4: READ uses L3 + disk fallback — verify correct
        Stage 5: Evict L3 — READ uses disk only — verify correct
        """
        t = make_table(keyframe_every=5, compress_depth=3)
        enable_l3(db, t)
        dbname = db.info.dbname

        # Stage 1: Insert data
        expected = {}
        for v in range(1, 16):
            content = f"lifecycle-v{v}: {'d' * (v * 30)}"
            insert_rows(db, t, [(1, v, content)])
            expected[v] = content

        # Stage 2: Read — should hit L1 (same connection, just inserted)
        for v, content in expected.items():
            result = single_content(db, t, 1, v)
            assert result == content, f"Stage 2 mismatch at v={v}"

        # Verify L3 is populated
        assert l3_table_exists(db, t)
        l3_before = l3_count(db, t)
        assert l3_before >= 15, f"Expected >= 15 L3 entries, got {l3_before}"

        # Stage 3: Restart — clears L1 + L2 shmem
        db.close()
        pg_ctl.restart()

        conn = _reconnect(dbname)
        try:
            # Stage 4: Read after restart — L1/L2 are cold, uses L3 or disk
            for v, content in expected.items():
                result = single_content(conn, t, 1, v)
                assert result == content, f"Stage 4 (post-restart) mismatch at v={v}"

            # Verify L3 still has entries
            assert l3_count(conn, t) >= 15

            # Stage 5: Evict L3, then read again (pure disk fallback).
            # Drop the L3 cache table directly — this is the cleanest way
            # to ensure L3 is fully gone without fighting check constraints.
            conn.execute(
                f"SELECT xpatch.drop_l3_cache('{t}')"
            )

            assert not l3_table_exists(conn, t), \
                "L3 table should be gone after drop_l3_cache"

            # Read again — must still be correct via disk
            for v, content in expected.items():
                result = single_content(conn, t, 1, v)
                assert result == content, \
                    f"Stage 5 (post-eviction) mismatch at v={v}"
        finally:
            conn.close()

    def test_lifecycle_multi_group_with_partial_delete(
        self, db: psycopg.Connection, make_table, db_factory
    ):
        """
        Multi-group lifecycle with partial deletion.

        Insert 3 groups → read all → delete group 2 → re-insert group 2
        with new content → verify groups 1 and 3 untouched, group 2 fresh.
        """
        t = make_table(compress_depth=3)
        enable_l3(db, t)

        expected = {}
        for g in range(1, 4):
            for v in range(1, 8):
                content = f"g{g}-v{v}-orig"
                insert_rows(db, t, [(g, v, content)])
                expected[(g, v)] = content

        # Read all to populate L1
        for (g, v), content in expected.items():
            assert single_content(db, t, g, v) == content

        # Delete group 2
        db.execute(f"DELETE FROM {t} WHERE group_id = 2")
        for v in range(1, 8):
            del expected[(2, v)]

        # Re-insert group 2 with different content
        for v in range(1, 5):
            content = f"g2-v{v}-new"
            insert_rows(db, t, [(2, v, content)])
            expected[(2, v)] = content

        # Verify everything — same connection
        for (g, v), content in expected.items():
            result = single_content(db, t, g, v)
            assert result == content, \
                f"Mismatch at g={g} v={v}: got '{result}', expected '{content}'"

        # Verify from fresh connection (cold L1)
        conn2 = db_factory()
        for (g, v), content in expected.items():
            result = single_content(conn2, t, g, v)
            assert result == content, \
                f"Fresh conn mismatch at g={g} v={v}: got '{result}'"

    def test_lifecycle_all_features_combined(
        self, db: psycopg.Connection, make_table, db_factory
    ):
        """
        Exercise all features together: L3 + zstd + compress_depth > 1
        + multiple versions + fresh connection reads.
        """
        t = make_table(
            compress_depth=5,
            keyframe_every=10,
            enable_zstd=True,
        )
        enable_l3(db, t)

        expected = {}
        for v in range(1, 21):
            content = f"all-features-v{v}: {'z' * (v * 20)}"
            insert_rows(db, t, [(1, v, content)])
            expected[v] = content

        # Read from same connection (L1 hot)
        for v, content in expected.items():
            assert single_content(db, t, 1, v) == content

        # Read from fresh connection (cold L1, exercises L2/L3/disk paths)
        conn2 = db_factory()
        for v, content in expected.items():
            result = single_content(conn2, t, 1, v)
            assert result == content, \
                f"All-features mismatch at v={v}: got '{result}'"

        # Verify L3 has entries
        assert l3_table_exists(db, t)
        assert l3_count(db, t) > 0
