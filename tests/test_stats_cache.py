"""
Test the stats cache (v0.4.0 feature): xpatch.group_stats table,
incremental updates on INSERT/DELETE, refresh, truncate clears.

Covers:
- Stats auto-populated on INSERT
- Stats updated correctly on DELETE (cascade)
- Stats cleared after TRUNCATE
- refresh_stats() regenerates from scan
- Stats match actual data (cross-validated against COUNT(*))
- Multi-group stats tracked independently
- Stats correct after multiple insert/delete cycles
- Truncate-then-reinsert
- Stats invariant: keyframe_count + delta_count == total_rows
"""

from __future__ import annotations

import psycopg
import pytest

from conftest import insert_rows, insert_versions, row_count


class TestStatsAutoPopulate:
    """Stats auto-populated on INSERT."""

    def test_stats_populated_after_insert(self, db: psycopg.Connection, make_table):
        """group_stats has entries after INSERT."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=5)

        oid_row = db.execute(f"SELECT '{t}'::regclass::oid AS oid").fetchone()
        gs = db.execute(
            "SELECT * FROM xpatch.group_stats WHERE relid = %s",
            [oid_row["oid"]],
        ).fetchall()
        assert len(gs) == 1, f"Expected 1 group_stats row, got {len(gs)}"

    def test_row_count_matches(self, db: psycopg.Connection, make_table):
        """Stats row_count matches actual rows per group."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=10)
        insert_versions(db, t, group_id=2, count=5)

        stats = db.execute(f"SELECT * FROM xpatch.stats('{t}'::regclass)").fetchone()
        assert stats["total_rows"] == 15
        assert stats["total_groups"] == 2

    def test_keyframe_count_tracked(self, db: psycopg.Connection, make_table):
        """Stats track keyframe counts correctly."""
        t = make_table(keyframe_every=3)
        insert_versions(db, t, group_id=1, count=7)
        # keyframes at seq 1, 4, 7 => 3 keyframes

        stats = db.execute(f"SELECT * FROM xpatch.stats('{t}'::regclass)").fetchone()
        assert stats["keyframe_count"] == 3

    def test_sizes_positive(self, db: psycopg.Connection, make_table):
        """raw_size_bytes and compressed_size_bytes are positive."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=5)

        stats = db.execute(f"SELECT * FROM xpatch.stats('{t}'::regclass)").fetchone()
        assert stats["raw_size_bytes"] > 0
        assert stats["compressed_size_bytes"] > 0


class TestStatsOnDelete:
    """Stats updated on DELETE."""

    def test_stats_decrease_after_delete(self, db: psycopg.Connection, make_table):
        """Stats row count decreases after DELETE."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=10)

        before = db.execute(f"SELECT * FROM xpatch.stats('{t}'::regclass)").fetchone()
        assert before["total_rows"] == 10

        # Delete last 5 (cascade from v6)
        db.execute(f"DELETE FROM {t} WHERE group_id = 1 AND version = 6")

        after = db.execute(f"SELECT * FROM xpatch.stats('{t}'::regclass)").fetchone()
        assert after["total_rows"] == 5

    def test_group_removed_from_stats_on_full_delete(self, db: psycopg.Connection, make_table):
        """When all versions of a group are deleted, group count decreases."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=5)
        insert_versions(db, t, group_id=2, count=5)

        db.execute(f"DELETE FROM {t} WHERE group_id = 1 AND version = 1")

        stats = db.execute(f"SELECT * FROM xpatch.stats('{t}'::regclass)").fetchone()
        assert stats["total_rows"] == 5
        assert stats["total_groups"] == 1


class TestStatsTruncate:
    """TRUNCATE clears stats."""

    def test_stats_cleared_after_truncate(self, db: psycopg.Connection, make_table):
        """TRUNCATE clears group_stats for the table."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=10)

        db.execute(f"TRUNCATE {t}")

        stats = db.execute(f"SELECT * FROM xpatch.stats('{t}'::regclass)").fetchone()
        assert stats["total_rows"] == 0
        assert stats["total_groups"] == 0


class TestStatsRefresh:
    """refresh_stats() regenerates stats from full scan."""

    def test_refresh_matches_actual(self, db: psycopg.Connection, make_table):
        """refresh_stats() produces stats matching actual table contents."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=10)
        insert_versions(db, t, group_id=2, count=5)

        # Clear stats manually
        oid_row = db.execute(f"SELECT '{t}'::regclass::oid AS oid").fetchone()
        db.execute("DELETE FROM xpatch.group_stats WHERE relid = %s", [oid_row["oid"]])

        # Refresh
        result = db.execute(
            f"SELECT * FROM xpatch.refresh_stats('{t}'::regclass)"
        ).fetchone()
        assert result["groups_scanned"] == 2
        assert result["rows_scanned"] == 15

        # Verify
        stats = db.execute(f"SELECT * FROM xpatch.stats('{t}'::regclass)").fetchone()
        assert stats["total_rows"] == 15
        assert stats["total_groups"] == 2

    def test_refresh_after_delete_matches(self, db: psycopg.Connection, make_table):
        """refresh_stats() after delete matches actual data."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=10)
        db.execute(f"DELETE FROM {t} WHERE group_id = 1 AND version = 6")
        # 5 rows remain

        db.execute(f"SELECT * FROM xpatch.refresh_stats('{t}'::regclass)")
        stats = db.execute(f"SELECT * FROM xpatch.stats('{t}'::regclass)").fetchone()
        assert stats["total_rows"] == 5


class TestStatsMultiGroup:
    """Multi-group stats tracking."""

    def test_many_groups_tracked(self, db: psycopg.Connection, make_table):
        """Stats track many groups independently."""
        t = make_table()
        for g in range(1, 21):
            insert_versions(db, t, group_id=g, count=3)

        stats = db.execute(f"SELECT * FROM xpatch.stats('{t}'::regclass)").fetchone()
        assert stats["total_groups"] == 20
        assert stats["total_rows"] == 60

    def test_per_group_stats_in_group_stats_table(self, db: psycopg.Connection, make_table):
        """xpatch.group_stats has one entry per group."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=5)
        insert_versions(db, t, group_id=2, count=10)

        oid_row = db.execute(f"SELECT '{t}'::regclass::oid AS oid").fetchone()
        groups = db.execute(
            "SELECT row_count FROM xpatch.group_stats WHERE relid = %s ORDER BY row_count",
            [oid_row["oid"]],
        ).fetchall()
        assert len(groups) == 2
        counts = sorted([g["row_count"] for g in groups])
        assert counts == [5, 10]


class TestStatsInsertDeleteCycles:
    """Stats correct after multiple insert/delete cycles."""

    def test_multiple_cycles(self, db: psycopg.Connection, make_table):
        """Stats remain accurate through insert/delete cycles."""
        t = make_table()

        # Cycle 1: insert 10
        insert_versions(db, t, group_id=1, count=10)
        stats = db.execute(f"SELECT * FROM xpatch.stats('{t}'::regclass)").fetchone()
        assert stats["total_rows"] == 10

        # Cycle 2: delete last 5
        db.execute(f"DELETE FROM {t} WHERE group_id = 1 AND version = 6")
        stats = db.execute(f"SELECT * FROM xpatch.stats('{t}'::regclass)").fetchone()
        assert stats["total_rows"] == 5

        # Cycle 3: insert more
        insert_versions(db, t, group_id=1, count=3, start=20)
        stats = db.execute(f"SELECT * FROM xpatch.stats('{t}'::regclass)").fetchone()
        assert stats["total_rows"] == 8

        # Cycle 4: delete all (cascade from v1)
        db.execute(f"DELETE FROM {t} WHERE group_id = 1 AND version = 1")
        stats = db.execute(f"SELECT * FROM xpatch.stats('{t}'::regclass)").fetchone()
        assert stats["total_rows"] == 0
        assert stats["total_groups"] == 0


# ---------------------------------------------------------------------------
# Cross-validation and edge cases
# ---------------------------------------------------------------------------


def _assert_stats_match_reality(db: psycopg.Connection, table: str) -> None:
    """Assert that xpatch.stats() matches actual table data."""
    actual = row_count(db, table)
    stats = db.execute(f"SELECT * FROM xpatch.stats('{table}'::regclass)").fetchone()
    assert stats["total_rows"] == actual, (
        f"Stats total_rows={stats['total_rows']} != actual COUNT(*)={actual}"
    )
    assert stats["keyframe_count"] + stats["delta_count"] == stats["total_rows"], (
        f"keyframe_count({stats['keyframe_count']}) + delta_count({stats['delta_count']}) "
        f"!= total_rows({stats['total_rows']})"
    )
    if actual > 0:
        assert stats["raw_size_bytes"] > 0
        assert stats["compressed_size_bytes"] > 0


class TestStatsCrossValidation:
    """Cross-validate stats against actual table data."""

    def test_stats_match_count_after_insert(self, db: psycopg.Connection, make_table):
        """Stats total_rows matches SELECT COUNT(*) after inserts."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=10)
        insert_versions(db, t, group_id=2, count=5)
        _assert_stats_match_reality(db, t)

    def test_stats_match_count_after_delete(self, db: psycopg.Connection, make_table):
        """Stats total_rows matches SELECT COUNT(*) after cascade delete."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=10)
        db.execute(f"DELETE FROM {t} WHERE group_id = 1 AND version = 6")
        _assert_stats_match_reality(db, t)

    def test_stats_match_count_after_full_delete(self, db: psycopg.Connection, make_table):
        """Stats match after deleting all rows via cascade."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=5)
        db.execute(f"DELETE FROM {t} WHERE group_id = 1 AND version = 1")
        stats = db.execute(f"SELECT * FROM xpatch.stats('{t}'::regclass)").fetchone()
        assert stats["total_rows"] == 0
        assert stats["total_groups"] == 0
        assert row_count(db, t) == 0

    def test_delta_count_invariant(self, db: psycopg.Connection, make_table):
        """keyframe_count + delta_count == total_rows always holds."""
        t = make_table(keyframe_every=5)
        insert_versions(db, t, group_id=1, count=12)
        stats = db.execute(f"SELECT * FROM xpatch.stats('{t}'::regclass)").fetchone()
        assert stats["keyframe_count"] + stats["delta_count"] == stats["total_rows"]


class TestStatsEdgeCases:
    """Edge cases for stats tracking."""

    def test_single_row_group_stats(self, db: psycopg.Connection, make_table):
        """Single row: total_rows=1, keyframe_count=1, delta_count=0."""
        t = make_table()
        insert_rows(db, t, [(1, 1, "only row")])
        stats = db.execute(f"SELECT * FROM xpatch.stats('{t}'::regclass)").fetchone()
        assert stats["total_rows"] == 1
        assert stats["total_groups"] == 1
        assert stats["keyframe_count"] == 1
        assert stats["delta_count"] == 0

    def test_truncate_then_reinsert(self, db: psycopg.Connection, make_table):
        """Stats are correct after TRUNCATE followed by new inserts."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=10)
        db.execute(f"TRUNCATE {t}")
        insert_versions(db, t, group_id=1, count=3)

        stats = db.execute(f"SELECT * FROM xpatch.stats('{t}'::regclass)").fetchone()
        assert stats["total_rows"] == 3
        assert stats["total_groups"] == 1
        assert stats["keyframe_count"] >= 1
        _assert_stats_match_reality(db, t)

    def test_refresh_stats_idempotent(self, db: psycopg.Connection, make_table):
        """Calling refresh_stats() twice produces identical results."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=10)

        db.execute(f"SELECT * FROM xpatch.refresh_stats('{t}'::regclass)")
        stats1 = db.execute(f"SELECT * FROM xpatch.stats('{t}'::regclass)").fetchone()

        db.execute(f"SELECT * FROM xpatch.refresh_stats('{t}'::regclass)")
        stats2 = db.execute(f"SELECT * FROM xpatch.stats('{t}'::regclass)").fetchone()

        for key in ("total_rows", "total_groups", "keyframe_count",
                     "raw_size_bytes", "compressed_size_bytes"):
            assert stats1[key] == stats2[key], (
                f"Mismatch on {key} after double refresh: {stats1[key]} != {stats2[key]}"
            )

    def test_stats_delete_then_cross_validate(self, db: psycopg.Connection, make_table):
        """After partial delete, stats and row_count agree for each group."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=10)
        insert_versions(db, t, group_id=2, count=10)
        # Cascade delete: removes v6-v10 from group 1
        db.execute(f"DELETE FROM {t} WHERE group_id = 1 AND version = 6")
        _assert_stats_match_reality(db, t)
        assert row_count(db, t, "group_id = 1") == 5
        assert row_count(db, t, "group_id = 2") == 10
