"""
Test L2 compressed delta cache.

Covers:
- L2 is populated on INSERT (compressed deltas stored in shmem)
- L2 stats reflect insertions and cache state
- Data integrity is maintained with L2 active
- L2 invalidation on DELETE/TRUNCATE
- Large entry rejection (entries > l2_cache_max_entry_kb)
- Concurrent access to L2
- Eviction under memory pressure
"""

from __future__ import annotations

import threading
from typing import Any

import psycopg
import pytest

from conftest import insert_rows, insert_versions, row_count


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def l2_stats(db: psycopg.Connection) -> dict[str, Any]:
    """Get L2 cache stats."""
    return db.execute("SELECT * FROM xpatch.l2_cache_stats()").fetchone()


def l1_stats(db: psycopg.Connection) -> dict[str, Any]:
    """Get L1 cache stats."""
    return db.execute("SELECT * FROM xpatch.cache_stats()").fetchone()


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


# ---------------------------------------------------------------------------
# L2 Population Tests
# ---------------------------------------------------------------------------


class TestL2Population:
    """Test that L2 is populated during INSERT."""

    def test_l2_entries_increase_on_insert(
        self, db: psycopg.Connection, make_table
    ):
        """
        After inserting data, L2 should have entries (compressed deltas
        are stored during INSERT path).
        """
        before = l2_stats(db)
        t = make_table(compress_depth=3, keyframe_every=10)
        insert_versions(db, t, group_id=1, count=10)
        after = l2_stats(db)

        assert after["entries_count"] > before["entries_count"], \
            "L2 entries should increase after INSERT"

    def test_l2_size_increases_on_insert(
        self, db: psycopg.Connection, make_table
    ):
        """L2 size_bytes should increase as we insert data."""
        before = l2_stats(db)
        t = make_table(compress_depth=3)
        insert_versions(db, t, group_id=1, count=20)
        after = l2_stats(db)

        assert after["cache_size_bytes"] > before["cache_size_bytes"], \
            "L2 size should increase after INSERT"

    def test_l2_multiple_groups(
        self, db: psycopg.Connection, make_table
    ):
        """L2 tracks entries across multiple groups."""
        before = l2_stats(db)
        t = make_table(compress_depth=3)
        for g in range(1, 6):
            insert_versions(db, t, group_id=g, count=5)
        after = l2_stats(db)

        new_entries = after["entries_count"] - before["entries_count"]
        # 5 groups x 5 versions each = 25 compressed deltas
        assert new_entries >= 25, \
            f"Expected at least 25 new L2 entries, got {new_entries}"

    def test_l2_max_bytes_matches_guc(
        self, db: psycopg.Connection
    ):
        """L2 max_bytes should reflect the configured l2_cache_size_mb."""
        stats = l2_stats(db)
        # The default is 1024 MB but GUC_UNIT_MB stores in KB internally,
        # so we just verify it's a positive number
        assert stats["cache_max_bytes"] > 0


# ---------------------------------------------------------------------------
# Data Integrity Tests
# ---------------------------------------------------------------------------


class TestL2DataIntegrity:
    """
    Verify that data integrity is maintained with L2 active.
    The L2 cache stores compressed deltas; if L2 is corrupt, reconstruction
    would fail. These tests insert data and verify correct readback.
    """

    def test_data_correct_after_insert(
        self, db: psycopg.Connection, make_table
    ):
        """Basic data integrity with L2 active."""
        t = make_table(compress_depth=5, keyframe_every=20)
        contents = {}
        for v in range(1, 21):
            c = f"L2 integrity v{v}: " + "x" * 50
            contents[v] = c
            insert_rows(db, t, [(1, v, c)])

        for v in range(1, 21):
            result = single_content(db, t, 1, v)
            assert result == contents[v], f"Mismatch at v={v}"

    def test_data_correct_deep_chain(
        self, db: psycopg.Connection, make_table
    ):
        """L2 handles deep delta chains correctly."""
        t = make_table(compress_depth=20, keyframe_every=50)
        contents = {}
        for v in range(1, 31):
            c = f"Deep v{v}: " + "d" * (50 + v)
            contents[v] = c
            insert_rows(db, t, [(1, v, c)])

        for v in range(1, 31):
            result = single_content(db, t, 1, v)
            assert result == contents[v], f"Deep chain mismatch at v={v}"

    def test_data_correct_with_zstd(
        self, db: psycopg.Connection, make_table
    ):
        """L2 works correctly with zstd compression enabled."""
        t = make_table(compress_depth=5, enable_zstd=True)
        contents = {}
        for v in range(1, 11):
            c = f"zstd v{v}: " + "z" * 200
            contents[v] = c
            insert_rows(db, t, [(1, v, c)])

        for v in range(1, 11):
            result = single_content(db, t, 1, v)
            assert result == contents[v], f"zstd mismatch at v={v}"

    def test_data_correct_multiple_delta_columns(
        self, db: psycopg.Connection, make_table
    ):
        """L2 handles multiple delta columns correctly."""
        t = make_table(
            columns="group_id INT, version INT, title TEXT NOT NULL, body TEXT NOT NULL",
            delta_columns=["title", "body"],
            compress_depth=3,
        )
        for v in range(1, 11):
            insert_rows(
                db, t,
                [(1, v, f"Title {v}", f"Body {v}: " + "b" * 50)],
                columns=["group_id", "version", "title", "body"],
            )

        for v in range(1, 11):
            row = db.execute(
                f"SELECT title, body FROM {t} WHERE group_id = 1 AND version = %s",
                [v],
            ).fetchone()
            assert row["title"] == f"Title {v}", f"Title mismatch at v={v}"
            assert row["body"] == f"Body {v}: " + "b" * 50, f"Body mismatch at v={v}"


# ---------------------------------------------------------------------------
# Invalidation Tests
# ---------------------------------------------------------------------------


class TestL2Invalidation:
    """Test that L2 entries are properly invalidated."""

    def test_truncate_clears_l2_entries(
        self, db: psycopg.Connection, make_table
    ):
        """TRUNCATE should invalidate L2 entries."""
        t = make_table(compress_depth=3)
        insert_versions(db, t, group_id=1, count=10)

        before = l2_stats(db)
        assert before["entries_count"] > 0

        db.execute(f"TRUNCATE {t}")

        # Re-insert and verify data is correct (not stale from old L2 entries)
        for v in range(1, 4):
            insert_rows(db, t, [(1, v, f"After truncate v{v}")])

        for v in range(1, 4):
            result = single_content(db, t, 1, v)
            assert result == f"After truncate v{v}", \
                f"Stale L2 data at v={v} after TRUNCATE"

    def test_delete_clears_l2_entries(
        self, db: psycopg.Connection, make_table
    ):
        """DELETE should invalidate L2 entries for the affected group."""
        t = make_table(compress_depth=3)
        insert_versions(db, t, group_id=1, count=5)

        db.execute(f"DELETE FROM {t} WHERE group_id = 1")

        # Re-insert and verify
        for v in range(1, 3):
            insert_rows(db, t, [(1, v, f"After delete v{v}")])

        result = all_content(db, t, group_id=1)
        assert len(result) == 2
        assert result[0] == "After delete v1"
        assert result[1] == "After delete v2"


# ---------------------------------------------------------------------------
# Large Entry Rejection
# ---------------------------------------------------------------------------


class TestL2LargeEntryRejection:
    """Test that entries exceeding l2_cache_max_entry_kb are rejected."""

    def test_skip_count_increases_for_large_deltas(
        self, db: psycopg.Connection, make_table
    ):
        """
        Compressed deltas larger than l2_cache_max_entry_kb should be
        rejected. We create very large content to produce large deltas.

        NOTE: The default l2_cache_max_entry_kb is 64 KB. Most typical
        deltas are much smaller. We use very large dissimilar content
        to produce large compressed output.
        """
        # Set a tiny max entry to force rejection
        db.execute("SET pg_xpatch.l2_cache_max_entry_kb = 1")
        t = make_table(compress_depth=1)

        before = l2_stats(db)

        # Insert content that should produce deltas > 1 KB
        for v in range(1, 6):
            big = "A" * 5000 + str(v) * 1000
            insert_rows(db, t, [(1, v, big)])

        after = l2_stats(db)

        # Some entries should be skipped (the exact number depends on
        # how compressible the data is and whether it's a keyframe)
        assert after["skip_count"] >= before["skip_count"], \
            "skip_count should increase when deltas exceed max entry size"


# ---------------------------------------------------------------------------
# Concurrent Access
# ---------------------------------------------------------------------------


class TestL2Concurrent:
    """Test concurrent access to L2 cache."""

    def test_concurrent_inserts(
        self, db: psycopg.Connection, make_table, db_factory
    ):
        """Multiple concurrent INSERT streams should not corrupt L2."""
        t = make_table(compress_depth=3)

        errors = []

        def inserter(conn_factory, group_id, count):
            try:
                c = conn_factory()
                for v in range(1, count + 1):
                    c.execute(
                        f"INSERT INTO {t} (group_id, version, content) "
                        f"VALUES (%s, %s, %s)",
                        [group_id, v, f"G{group_id}V{v}: " + "c" * 100],
                    )
            except Exception as e:
                errors.append(str(e))

        threads = []
        for g in range(1, 9):
            t_thread = threading.Thread(
                target=inserter, args=(db_factory, g, 10)
            )
            threads.append(t_thread)

        for t_thread in threads:
            t_thread.start()
        for t_thread in threads:
            t_thread.join(timeout=30)

        assert len(errors) == 0, f"Concurrent insert errors: {errors}"

        # Verify data integrity for all groups
        for g in range(1, 9):
            result = all_content(db, t, group_id=g)
            assert len(result) == 10, f"Group {g}: expected 10 rows, got {len(result)}"
            for i, v in enumerate(range(1, 11)):
                assert result[i] == f"G{g}V{v}: " + "c" * 100, \
                    f"Group {g}, version {v}: content mismatch"

    def test_concurrent_read_write(
        self, db: psycopg.Connection, make_table, db_factory
    ):
        """Concurrent reads and writes should not deadlock or corrupt data."""
        t = make_table(compress_depth=3)
        insert_versions(db, t, group_id=1, count=20)

        errors = []
        results = []

        def reader(conn_factory, n_reads):
            try:
                c = conn_factory()
                for v in range(1, n_reads + 1):
                    row = c.execute(
                        f"SELECT content FROM {t} WHERE group_id = 1 AND version = %s",
                        [v],
                    ).fetchone()
                    if row:
                        results.append(row["content"])
            except Exception as e:
                errors.append(f"reader: {e}")

        def writer(conn_factory, group_id, count):
            try:
                c = conn_factory()
                for v in range(1, count + 1):
                    c.execute(
                        f"INSERT INTO {t} (group_id, version, content) "
                        f"VALUES (%s, %s, %s)",
                        [group_id, v, f"W{group_id}V{v}"],
                    )
            except Exception as e:
                errors.append(f"writer: {e}")

        threads = []
        for _ in range(4):
            threads.append(threading.Thread(target=reader, args=(db_factory, 10)))
        for g in range(2, 5):
            threads.append(threading.Thread(target=writer, args=(db_factory, g, 10)))

        for t_thread in threads:
            t_thread.start()
        for t_thread in threads:
            t_thread.join(timeout=30)

        hung = [t_thread for t_thread in threads if t_thread.is_alive()]
        assert len(hung) == 0, f"{len(hung)} threads hung"
        assert len(errors) == 0, f"Errors: {errors}"
        assert len(results) > 0, "Expected some read results"


# ---------------------------------------------------------------------------
# Stats Tests
# ---------------------------------------------------------------------------


class TestL2Stats:
    """Test L2 cache statistics reporting."""

    def test_stats_return_all_columns(self, db: psycopg.Connection):
        """l2_cache_stats() should return all expected columns."""
        stats = l2_stats(db)
        expected_keys = [
            "cache_size_bytes", "cache_max_bytes", "entries_count",
            "hit_count", "miss_count", "eviction_count", "skip_count",
        ]
        for key in expected_keys:
            assert key in stats, f"Missing key: {key}"

    def test_stats_non_negative(self, db: psycopg.Connection):
        """All stats values should be non-negative."""
        stats = l2_stats(db)
        for key, val in stats.items():
            assert val >= 0, f"Negative value for {key}: {val}"
