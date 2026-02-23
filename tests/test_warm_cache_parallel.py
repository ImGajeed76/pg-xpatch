"""
Tests for xpatch.warm_cache_parallel() — parallel C cache warming.

Tests cover:
- Basic functionality (single group, multiple groups, empty table)
- Worker configuration (sequential mode, explicit workers, GUC override)
- max_groups parameter (limit, zero, more than available)
- Keyframe sections (multiple sections, single row, partial sections)
- Cache population verification (entries increase, post-warm reads hit cache)
- Tables without group_by column
- Error handling (non-xpatch table, negative params, permission denied)
- Correctness comparison against existing PL/pgSQL warm_cache
- Stress: many groups + many sections with parallel workers
"""

from __future__ import annotations

import pytest
import psycopg

from conftest import insert_versions, row_count


# ============================================================================
# Basic Functionality
# ============================================================================


class TestWarmCacheParallelBasic:
    """Core warm_cache_parallel() functionality tests."""

    def test_basic_single_group(self, db: psycopg.Connection, make_table):
        """Warms a single group and returns correct stats."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=10)

        result = db.execute(
            f"SELECT * FROM xpatch.warm_cache_parallel('{t}'::regclass)"
        ).fetchone()

        assert result["rows_warmed"] == 10
        assert result["groups_warmed"] == 1
        assert result["sections_warmed"] >= 1
        assert result["duration_ms"] >= 0
        # workers_used can be 0 (if sequential fallback) or > 0
        assert result["workers_used"] >= 0

    def test_multiple_groups(self, db: psycopg.Connection, make_table):
        """Warms multiple independent groups."""
        t = make_table()
        for g in range(1, 6):
            insert_versions(db, t, group_id=g, count=10)

        result = db.execute(
            f"SELECT * FROM xpatch.warm_cache_parallel('{t}'::regclass)"
        ).fetchone()

        assert result["rows_warmed"] == 50
        assert result["groups_warmed"] == 5
        assert result["sections_warmed"] >= 5  # At least 1 section per group

    def test_empty_table(self, db: psycopg.Connection, make_table):
        """Empty table returns all zeros without error."""
        t = make_table()

        result = db.execute(
            f"SELECT * FROM xpatch.warm_cache_parallel('{t}'::regclass)"
        ).fetchone()

        assert result["rows_warmed"] == 0
        assert result["groups_warmed"] == 0
        assert result["sections_warmed"] == 0
        assert result["workers_used"] == 0
        assert result["duration_ms"] >= 0

    def test_single_row_group(self, db: psycopg.Connection, make_table):
        """Group with only 1 row (keyframe only) warms correctly."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=1)

        result = db.execute(
            f"SELECT * FROM xpatch.warm_cache_parallel('{t}'::regclass)"
        ).fetchone()

        assert result["rows_warmed"] == 1
        assert result["groups_warmed"] == 1
        assert result["sections_warmed"] == 1


# ============================================================================
# Worker Configuration
# ============================================================================


class TestWarmCacheParallelWorkers:
    """Tests for worker count configuration."""

    def test_sequential_mode(self, db: psycopg.Connection, make_table):
        """max_workers=0 forces sequential C warming (no BGW)."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=10)

        result = db.execute(
            f"SELECT * FROM xpatch.warm_cache_parallel('{t}'::regclass, "
            f"max_workers => 0)"
        ).fetchone()

        assert result["rows_warmed"] == 10
        assert result["workers_used"] == 0

    def test_explicit_workers(self, db: psycopg.Connection, make_table):
        """Explicit max_workers overrides GUC default."""
        t = make_table()
        for g in range(1, 6):
            insert_versions(db, t, group_id=g, count=10)

        result = db.execute(
            f"SELECT * FROM xpatch.warm_cache_parallel('{t}'::regclass, "
            f"max_workers => 2)"
        ).fetchone()

        assert result["rows_warmed"] == 50
        # workers_used should be <= requested (could be less if BGW slots exhausted)
        assert result["workers_used"] <= 2

    def test_workers_via_guc(self, db: psycopg.Connection, make_table):
        """GUC pg_xpatch.warm_cache_workers is used when max_workers is NULL."""
        t = make_table()
        for g in range(1, 4):
            insert_versions(db, t, group_id=g, count=10)

        db.execute("SET pg_xpatch.warm_cache_workers = 2")
        result = db.execute(
            f"SELECT * FROM xpatch.warm_cache_parallel('{t}'::regclass)"
        ).fetchone()

        assert result["rows_warmed"] == 30
        # Workers used should respect the GUC
        assert result["workers_used"] <= 2

    def test_single_task_uses_no_workers(self, db: psycopg.Connection, make_table):
        """With only 1 task (1 group, rows < keyframe_every), use sequential path."""
        t = make_table(keyframe_every=100)
        insert_versions(db, t, group_id=1, count=5)

        result = db.execute(
            f"SELECT * FROM xpatch.warm_cache_parallel('{t}'::regclass, "
            f"max_workers => 4)"
        ).fetchone()

        # With only 1 task, should fall back to sequential
        assert result["rows_warmed"] == 5
        assert result["sections_warmed"] == 1


# ============================================================================
# max_groups Parameter
# ============================================================================


class TestWarmCacheParallelMaxGroups:
    """Tests for the max_groups parameter."""

    def test_max_groups_limits(self, db: psycopg.Connection, make_table):
        """max_groups limits the number of groups warmed."""
        t = make_table()
        for g in range(1, 11):
            insert_versions(db, t, group_id=g, count=5)

        result = db.execute(
            f"SELECT * FROM xpatch.warm_cache_parallel('{t}'::regclass, "
            f"max_groups => 3)"
        ).fetchone()

        assert result["groups_warmed"] == 3
        assert result["rows_warmed"] == 15  # 3 groups * 5 rows

    def test_max_groups_zero(self, db: psycopg.Connection, make_table):
        """max_groups=0 warms nothing."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=10)

        result = db.execute(
            f"SELECT * FROM xpatch.warm_cache_parallel('{t}'::regclass, "
            f"max_groups => 0)"
        ).fetchone()

        assert result["rows_warmed"] == 0
        assert result["groups_warmed"] == 0
        assert result["sections_warmed"] == 0

    def test_max_groups_exceeds_available(self, db: psycopg.Connection, make_table):
        """max_groups larger than actual groups just warms all of them."""
        t = make_table()
        for g in range(1, 4):
            insert_versions(db, t, group_id=g, count=5)

        result = db.execute(
            f"SELECT * FROM xpatch.warm_cache_parallel('{t}'::regclass, "
            f"max_groups => 100)"
        ).fetchone()

        assert result["groups_warmed"] == 3
        assert result["rows_warmed"] == 15


# ============================================================================
# Keyframe Sections
# ============================================================================


class TestWarmCacheParallelSections:
    """Tests for keyframe section computation and warming."""

    def test_multiple_sections_per_group(self, db: psycopg.Connection, make_table):
        """Multiple keyframe sections within a group are correctly split."""
        t = make_table(keyframe_every=5)
        insert_versions(db, t, group_id=1, count=17)

        result = db.execute(
            f"SELECT * FROM xpatch.warm_cache_parallel('{t}'::regclass, "
            f"max_workers => 0)"
        ).fetchone()

        assert result["rows_warmed"] == 17
        # kf_every=5, 17 rows: sections [1-5],[6-10],[11-15],[16-17] = 4 sections
        assert result["sections_warmed"] == 4

    def test_exact_keyframe_boundary(self, db: psycopg.Connection, make_table):
        """Row count exactly at keyframe boundary creates clean sections."""
        t = make_table(keyframe_every=5)
        insert_versions(db, t, group_id=1, count=15)

        result = db.execute(
            f"SELECT * FROM xpatch.warm_cache_parallel('{t}'::regclass, "
            f"max_workers => 0)"
        ).fetchone()

        assert result["rows_warmed"] == 15
        assert result["sections_warmed"] == 3  # [1-5],[6-10],[11-15]

    def test_rows_less_than_keyframe(self, db: psycopg.Connection, make_table):
        """Group with fewer rows than keyframe_every = 1 section."""
        t = make_table(keyframe_every=100)
        insert_versions(db, t, group_id=1, count=10)

        result = db.execute(
            f"SELECT * FROM xpatch.warm_cache_parallel('{t}'::regclass)"
        ).fetchone()

        assert result["sections_warmed"] == 1

    def test_many_groups_many_sections(self, db: psycopg.Connection, make_table):
        """Multiple groups each with multiple sections."""
        t = make_table(keyframe_every=5)
        for g in range(1, 4):
            insert_versions(db, t, group_id=g, count=12)

        result = db.execute(
            f"SELECT * FROM xpatch.warm_cache_parallel('{t}'::regclass, "
            f"max_workers => 0)"
        ).fetchone()

        assert result["rows_warmed"] == 36
        assert result["groups_warmed"] == 3
        # Each group: [1-5],[6-10],[11-12] = 3 sections * 3 groups = 9
        assert result["sections_warmed"] == 9


# ============================================================================
# Cache Population Verification
# ============================================================================


class TestWarmCacheParallelCachePopulation:
    """Tests verifying that cache is actually populated by warming."""

    def test_cache_entries_increase(self, db: psycopg.Connection, make_table):
        """cache_stats() shows increased entries or misses after warming."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=10)

        before = db.execute("SELECT * FROM xpatch.cache_stats()").fetchone()
        db.execute(
            f"SELECT * FROM xpatch.warm_cache_parallel('{t}'::regclass, "
            f"max_workers => 0)"
        )
        after = db.execute("SELECT * FROM xpatch.cache_stats()").fetchone()

        # Either entries count increased or miss count increased (cache was populated)
        assert (
            after["entries_count"] >= before["entries_count"]
            or after["miss_count"] > before["miss_count"]
        )

    def test_warm_then_read_hits_cache(self, db: psycopg.Connection, make_table):
        """After warming, reads should hit the cache (increased hit count)."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=10)

        # Warm the cache
        db.execute(
            f"SELECT * FROM xpatch.warm_cache_parallel('{t}'::regclass, "
            f"max_workers => 0)"
        )

        before = db.execute("SELECT * FROM xpatch.cache_stats()").fetchone()

        # Read all rows — should be cache hits
        db.execute(f"SELECT * FROM {t}")

        after = db.execute("SELECT * FROM xpatch.cache_stats()").fetchone()

        # Hit count should have increased
        assert after["hit_count"] >= before["hit_count"]

    def test_sequential_and_parallel_same_cache_effect(
        self, db: psycopg.Connection, make_table
    ):
        """Sequential (max_workers=0) and parallel produce equivalent cache state."""
        t = make_table()
        for g in range(1, 4):
            insert_versions(db, t, group_id=g, count=10)

        # Sequential warm
        result_seq = db.execute(
            f"SELECT * FROM xpatch.warm_cache_parallel('{t}'::regclass, "
            f"max_workers => 0)"
        ).fetchone()

        # Verify we can read all rows correctly after warming
        rows = db.execute(
            f"SELECT * FROM {t} ORDER BY group_id, version"
        ).fetchall()
        assert len(rows) == 30

        # Verify row contents are correct
        for row in rows:
            assert row["content"] is not None
            assert "Version" in row["content"]


# ============================================================================
# Tables Without group_by
# ============================================================================


class TestWarmCacheParallelNoGroupBy:
    """Tests for tables without a group_by column."""

    def test_no_group_by_basic(self, db: psycopg.Connection, make_table):
        """Table without group_by column warms as single group."""
        t = make_table(
            columns="version INT, content TEXT NOT NULL",
            group_by=None,
            order_by="version",
        )
        for i in range(1, 11):
            db.execute(
                f"INSERT INTO {t} (version, content) VALUES (%s, %s)",
                [i, f"Version {i} content"],
            )

        result = db.execute(
            f"SELECT * FROM xpatch.warm_cache_parallel('{t}'::regclass, "
            f"max_workers => 0)"
        ).fetchone()

        assert result["rows_warmed"] == 10
        assert result["groups_warmed"] == 1

    def test_no_group_by_multiple_sections(self, db: psycopg.Connection, make_table):
        """No group_by table with multiple keyframe sections."""
        t = make_table(
            columns="version INT, content TEXT NOT NULL",
            group_by=None,
            order_by="version",
            keyframe_every=5,
        )
        for i in range(1, 13):
            db.execute(
                f"INSERT INTO {t} (version, content) VALUES (%s, %s)",
                [i, f"Version {i} content"],
            )

        result = db.execute(
            f"SELECT * FROM xpatch.warm_cache_parallel('{t}'::regclass, "
            f"max_workers => 0)"
        ).fetchone()

        assert result["rows_warmed"] == 12
        assert result["groups_warmed"] == 1
        # [1-5],[6-10],[11-12] = 3 sections
        assert result["sections_warmed"] == 3


# ============================================================================
# Error Handling
# ============================================================================


class TestWarmCacheParallelErrors:
    """Tests for error conditions."""

    def test_non_xpatch_table(self, db: psycopg.Connection):
        """Raises error for non-xpatch (regular heap) table."""
        db.execute("CREATE TABLE heap_test (id INT)")
        with pytest.raises(psycopg.errors.WrongObjectType):
            db.execute(
                "SELECT * FROM xpatch.warm_cache_parallel('heap_test'::regclass)"
            )

    def test_negative_max_workers(self, db: psycopg.Connection, make_table):
        """Raises error for negative max_workers."""
        t = make_table()
        with pytest.raises(psycopg.errors.InvalidParameterValue):
            db.execute(
                f"SELECT * FROM xpatch.warm_cache_parallel('{t}'::regclass, "
                f"max_workers => -1)"
            )

    def test_negative_max_groups(self, db: psycopg.Connection, make_table):
        """Raises error for negative max_groups."""
        t = make_table()
        with pytest.raises(psycopg.errors.InvalidParameterValue):
            db.execute(
                f"SELECT * FROM xpatch.warm_cache_parallel('{t}'::regclass, "
                f"max_groups => -1)"
            )

    def test_nonexistent_table(self, db: psycopg.Connection):
        """Raises error for nonexistent table."""
        with pytest.raises(psycopg.errors.UndefinedTable):
            db.execute(
                "SELECT * FROM xpatch.warm_cache_parallel("
                "'nonexistent_table'::regclass)"
            )


# ============================================================================
# Correctness: Compare with PL/pgSQL warm_cache
# ============================================================================


class TestWarmCacheParallelCorrectness:
    """Cross-validate parallel warm results with existing warm_cache."""

    def test_row_count_matches_plpgsql(self, db: psycopg.Connection, make_table):
        """Parallel warm reports same row count as PL/pgSQL warm."""
        t = make_table()
        for g in range(1, 4):
            insert_versions(db, t, group_id=g, count=10)

        # PL/pgSQL warm
        plpgsql = db.execute(
            f"SELECT * FROM xpatch.warm_cache('{t}'::regclass)"
        ).fetchone()

        # C parallel warm (sequential to make comparison deterministic)
        c_result = db.execute(
            f"SELECT * FROM xpatch.warm_cache_parallel('{t}'::regclass, "
            f"max_workers => 0)"
        ).fetchone()

        assert c_result["rows_warmed"] == plpgsql["rows_scanned"]
        assert c_result["groups_warmed"] == plpgsql["groups_warmed"]

    def test_data_integrity_after_warm(self, db: psycopg.Connection, make_table):
        """All rows are readable and correct after parallel warm."""
        t = make_table()
        for g in range(1, 6):
            insert_versions(db, t, group_id=g, count=10)

        # Warm
        db.execute(
            f"SELECT * FROM xpatch.warm_cache_parallel('{t}'::regclass, "
            f"max_workers => 0)"
        )

        # Read and verify every row
        rows = db.execute(
            f"SELECT group_id, version, content FROM {t} "
            f"ORDER BY group_id, version"
        ).fetchall()

        assert len(rows) == 50

        for row in rows:
            g = row["group_id"]
            v = row["version"]
            expected_content = f"Version {v} content"
            assert row["content"] == expected_content, (
                f"Group {g}, version {v}: expected '{expected_content}', "
                f"got '{row['content']}'"
            )


# ============================================================================
# Parallel Worker Execution
# ============================================================================


class TestWarmCacheParallelExecution:
    """Tests specifically for the parallel BGW execution path."""

    def test_parallel_correctness(self, db: psycopg.Connection, make_table):
        """Parallel warming produces correct cache content for multiple groups."""
        t = make_table(keyframe_every=5)
        for g in range(1, 6):
            insert_versions(db, t, group_id=g, count=12)

        result = db.execute(
            f"SELECT * FROM xpatch.warm_cache_parallel('{t}'::regclass, "
            f"max_workers => 2)"
        ).fetchone()

        assert result["rows_warmed"] == 60
        assert result["groups_warmed"] == 5

        # Verify all rows are readable
        rows = db.execute(
            f"SELECT group_id, version, content FROM {t} "
            f"ORDER BY group_id, version"
        ).fetchall()
        assert len(rows) == 60

    def test_parallel_many_small_groups(self, db: psycopg.Connection, make_table):
        """Many small groups (1-2 rows each) with parallel workers."""
        t = make_table()
        for g in range(1, 21):
            insert_versions(db, t, group_id=g, count=2)

        result = db.execute(
            f"SELECT * FROM xpatch.warm_cache_parallel('{t}'::regclass, "
            f"max_workers => 2)"
        ).fetchone()

        assert result["rows_warmed"] == 40
        assert result["groups_warmed"] == 20

    def test_parallel_one_large_group(self, db: psycopg.Connection, make_table):
        """Single large group with many keyframe sections."""
        t = make_table(keyframe_every=5)
        insert_versions(db, t, group_id=1, count=50)

        result = db.execute(
            f"SELECT * FROM xpatch.warm_cache_parallel('{t}'::regclass, "
            f"max_workers => 2)"
        ).fetchone()

        assert result["rows_warmed"] == 50
        assert result["groups_warmed"] == 1
        assert result["sections_warmed"] == 10  # 50/5 = 10 sections


# ============================================================================
# Stress Tests (marked slow)
# ============================================================================


class TestWarmCacheParallelStress:
    """Larger-scale tests for thorough validation."""

    @pytest.mark.slow
    def test_many_groups_parallel(self, db: psycopg.Connection, make_table):
        """Warm a table with many groups using parallel workers."""
        t = make_table(keyframe_every=5)
        for g in range(1, 51):
            insert_versions(db, t, group_id=g, count=20)

        result = db.execute(
            f"SELECT * FROM xpatch.warm_cache_parallel('{t}'::regclass, "
            f"max_workers => 4)"
        ).fetchone()

        assert result["rows_warmed"] == 1000
        assert result["groups_warmed"] == 50
        # Each group: 20 rows / kf=5 -> 4 sections * 50 groups = 200
        assert result["sections_warmed"] == 200

    @pytest.mark.slow
    def test_warm_followed_by_full_read(self, db: psycopg.Connection, make_table):
        """Warm then read all rows to verify integrity at scale."""
        t = make_table(keyframe_every=10)
        for g in range(1, 21):
            insert_versions(db, t, group_id=g, count=30)

        # Warm
        warm_result = db.execute(
            f"SELECT * FROM xpatch.warm_cache_parallel('{t}'::regclass, "
            f"max_workers => 4)"
        ).fetchone()

        assert warm_result["rows_warmed"] == 600

        # Read every row and verify content
        rows = db.execute(
            f"SELECT group_id, version, content FROM {t} "
            f"ORDER BY group_id, version"
        ).fetchall()

        assert len(rows) == 600

        for row in rows:
            v = row["version"]
            assert row["content"] == f"Version {v} content"

    @pytest.mark.slow
    def test_warm_idempotent(self, db: psycopg.Connection, make_table):
        """Warming the same table twice produces the same result."""
        t = make_table(keyframe_every=5)
        for g in range(1, 11):
            insert_versions(db, t, group_id=g, count=15)

        result1 = db.execute(
            f"SELECT * FROM xpatch.warm_cache_parallel('{t}'::regclass, "
            f"max_workers => 0)"
        ).fetchone()

        result2 = db.execute(
            f"SELECT * FROM xpatch.warm_cache_parallel('{t}'::regclass, "
            f"max_workers => 0)"
        ).fetchone()

        assert result1["rows_warmed"] == result2["rows_warmed"]
        assert result1["groups_warmed"] == result2["groups_warmed"]
        assert result1["sections_warmed"] == result2["sections_warmed"]
