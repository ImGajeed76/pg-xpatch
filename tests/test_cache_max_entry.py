"""
Tests for the cache max entry size fix (v0.5.1).

Covers:
- GUC pg_xpatch.cache_max_entry_kb existence, default value, runtime tunability
- skip_count column in xpatch_cache_stats() and xpatch.cache_stats()
- Entries below the limit are cached normally
- Entries above the limit are skipped and skip_count increments
- Changing the GUC at runtime affects caching behavior
- The unqualified C function xpatch_cache_stats() also returns skip_count
"""

from __future__ import annotations

import psycopg
import pytest

from conftest import insert_rows, insert_versions, row_count


# ---------------------------------------------------------------------------
# GUC: pg_xpatch.cache_max_entry_kb
# ---------------------------------------------------------------------------


class TestCacheMaxEntryGUC:
    """pg_xpatch.cache_max_entry_kb GUC is registered and configurable."""

    def test_guc_exists(self, db: psycopg.Connection):
        """The GUC is visible via SHOW."""
        row = db.execute("SHOW pg_xpatch.cache_max_entry_kb").fetchone()
        assert row is not None

    def test_guc_default_is_256kb(self, db: psycopg.Connection):
        """Default value is 256kB."""
        row = db.execute("SHOW pg_xpatch.cache_max_entry_kb").fetchone()
        val = list(row.values())[0]
        assert val == "256kB"

    def test_guc_settable_by_superuser(self, db: psycopg.Connection):
        """Superuser can change the value at runtime (PGC_SUSET)."""
        db.execute("SET pg_xpatch.cache_max_entry_kb = 512")
        row = db.execute("SHOW pg_xpatch.cache_max_entry_kb").fetchone()
        val = list(row.values())[0]
        assert val == "512kB"

    def test_guc_rejects_below_minimum(self, db: psycopg.Connection):
        """Values below minimum (16 KB) are rejected."""
        with pytest.raises(psycopg.errors.Error):
            db.execute("SET pg_xpatch.cache_max_entry_kb = 8")

    def test_guc_rejects_above_maximum(self, db: psycopg.Connection):
        """Values above INT_MAX are rejected."""
        with pytest.raises(psycopg.errors.Error):
            db.execute("SET pg_xpatch.cache_max_entry_kb = 2147483648")

    def test_guc_accepts_boundary_min(self, db: psycopg.Connection):
        """Minimum value (16 KB) is accepted."""
        db.execute("SET pg_xpatch.cache_max_entry_kb = 16")
        row = db.execute("SHOW pg_xpatch.cache_max_entry_kb").fetchone()
        val = list(row.values())[0]
        assert val == "16kB"

    def test_guc_accepts_boundary_max(self, db: psycopg.Connection):
        """Large value (e.g. 1GB = 1048576 KB) is accepted with raised max."""
        db.execute("SET pg_xpatch.cache_max_entry_kb = 1048576")
        row = db.execute("SHOW pg_xpatch.cache_max_entry_kb").fetchone()
        val = list(row.values())[0]
        # PostgreSQL may display 1048576kB as "1GB"
        assert val in ("1048576kB", "1GB", "1024MB")

    def test_guc_visible_in_pg_settings(self, db: psycopg.Connection):
        """GUC appears in pg_settings with correct metadata."""
        row = db.execute(
            "SELECT name, setting, unit, context "
            "FROM pg_settings WHERE name = 'pg_xpatch.cache_max_entry_kb'"
        ).fetchone()
        assert row is not None
        assert row["name"] == "pg_xpatch.cache_max_entry_kb"
        assert row["unit"] == "kB"
        assert row["context"] == "superuser"

    def test_guc_reset_restores_default(self, db: psycopg.Connection):
        """RESET restores the default value."""
        db.execute("SET pg_xpatch.cache_max_entry_kb = 1024")
        db.execute("RESET pg_xpatch.cache_max_entry_kb")
        row = db.execute("SHOW pg_xpatch.cache_max_entry_kb").fetchone()
        val = list(row.values())[0]
        assert val == "256kB"


# ---------------------------------------------------------------------------
# skip_count in cache_stats()
# ---------------------------------------------------------------------------


class TestCacheStatsSkipCount:
    """xpatch.cache_stats() and xpatch_cache_stats() expose skip_count."""

    def test_skip_count_column_present_qualified(self, db: psycopg.Connection):
        """xpatch.cache_stats() returns skip_count column."""
        row = db.execute("SELECT * FROM xpatch.cache_stats()").fetchone()
        assert "skip_count" in row

    def test_skip_count_column_present_unqualified(self, db: psycopg.Connection):
        """xpatch_cache_stats() C function returns skip_count column."""
        row = db.execute("SELECT * FROM xpatch_cache_stats()").fetchone()
        assert "skip_count" in row

    def test_cache_stats_has_all_seven_columns(self, db: psycopg.Connection):
        """cache_stats() returns all 7 expected columns."""
        row = db.execute("SELECT * FROM xpatch.cache_stats()").fetchone()
        expected = {
            "cache_size_bytes", "cache_max_bytes", "entries_count",
            "hit_count", "miss_count", "eviction_count", "skip_count",
        }
        assert expected == set(row.keys())

    def test_skip_count_is_non_negative(self, db: psycopg.Connection):
        """skip_count is a non-negative integer."""
        row = db.execute("SELECT * FROM xpatch.cache_stats()").fetchone()
        assert row["skip_count"] >= 0

    def test_skip_count_is_integer_type(self, db: psycopg.Connection):
        """skip_count is returned as a BIGINT (Python int)."""
        row = db.execute("SELECT skip_count FROM xpatch.cache_stats()").fetchone()
        assert isinstance(row["skip_count"], int)


# ---------------------------------------------------------------------------
# Cache skip behaviour: entries above the limit
# ---------------------------------------------------------------------------


class TestCacheSkipLargeEntries:
    """Entries exceeding cache_max_entry_kb are skipped with skip_count increment."""

    def test_small_entry_is_cached(self, db: psycopg.Connection, make_table):
        """A small entry (well under the limit) is cached normally."""
        t = make_table()
        # Insert small content (< 1 KB)
        insert_versions(db, t, group_id=1, count=5)

        before = db.execute("SELECT * FROM xpatch.cache_stats()").fetchone()

        # Read twice: first populates cache, second hits it
        db.execute(f"SELECT * FROM {t} ORDER BY version").fetchall()
        db.execute(f"SELECT * FROM {t} ORDER BY version").fetchall()

        after = db.execute("SELECT * FROM xpatch.cache_stats()").fetchone()

        # Hit count should have increased (cache is working for small entries)
        assert after["hit_count"] > before["hit_count"], (
            "Expected cache hits for small entries"
        )

    def test_large_entry_increments_skip_count(self, db: psycopg.Connection, make_table):
        """Inserting and reading content larger than the limit increments skip_count."""
        t = make_table()

        # Set limit very low so our test content exceeds it
        db.execute("SET pg_xpatch.cache_max_entry_kb = 16")

        before = db.execute("SELECT * FROM xpatch.cache_stats()").fetchone()

        # Insert content that is ~100KB (exceeds 16KB limit)
        big_content = "X" * 100_000
        insert_rows(db, t, [(1, 1, big_content)])

        # Read it back - this triggers reconstruction and a cache put attempt
        row = db.execute(f"SELECT content FROM {t} WHERE group_id = 1").fetchone()
        assert row["content"] == big_content

        after = db.execute("SELECT * FROM xpatch.cache_stats()").fetchone()

        assert after["skip_count"] > before["skip_count"], (
            f"Expected skip_count to increase. "
            f"Before: {before['skip_count']}, After: {after['skip_count']}"
        )

    def test_skip_count_does_not_increase_for_small_entries(
        self, db: psycopg.Connection, make_table
    ):
        """Small entries do not increment skip_count."""
        t = make_table()

        # Ensure default limit (256KB) - our content will be well under
        db.execute("RESET pg_xpatch.cache_max_entry_kb")

        before = db.execute("SELECT * FROM xpatch.cache_stats()").fetchone()

        # Insert and read small content (~20 bytes per version)
        insert_versions(db, t, group_id=1, count=10)
        db.execute(f"SELECT * FROM {t}").fetchall()

        after = db.execute("SELECT * FROM xpatch.cache_stats()").fetchone()

        assert after["skip_count"] == before["skip_count"], (
            f"skip_count should not increase for small entries. "
            f"Before: {before['skip_count']}, After: {after['skip_count']}"
        )

    def test_large_entry_not_cached_repeated_reads_all_miss(
        self, db: psycopg.Connection, make_table
    ):
        """When an entry exceeds the limit, repeated reads produce misses, not hits."""
        t = make_table()

        # Set limit very low
        db.execute("SET pg_xpatch.cache_max_entry_kb = 16")

        # Insert content that exceeds 16KB limit
        big_content = "Y" * 100_000
        insert_rows(db, t, [(1, 1, big_content)])

        # First read
        db.execute(f"SELECT content FROM {t} WHERE group_id = 1").fetchone()
        after_first = db.execute("SELECT * FROM xpatch.cache_stats()").fetchone()

        # Second read of the same row - should miss again since it can't be cached
        db.execute(f"SELECT content FROM {t} WHERE group_id = 1").fetchone()
        after_second = db.execute("SELECT * FROM xpatch.cache_stats()").fetchone()

        # Miss count should increase on second read (not hit)
        assert after_second["miss_count"] > after_first["miss_count"], (
            "Expected additional misses on re-read of oversized entry"
        )

    def test_raising_limit_allows_caching(self, db: psycopg.Connection, make_table):
        """Raising the limit above the entry size allows it to be cached."""
        t = make_table()

        # Start with a low limit that rejects our content
        db.execute("SET pg_xpatch.cache_max_entry_kb = 16")
        content_50kb = "Z" * 50_000
        insert_rows(db, t, [(1, 1, content_50kb)])

        # Read with low limit - should skip
        db.execute(f"SELECT content FROM {t} WHERE group_id = 1").fetchone()
        after_low = db.execute("SELECT * FROM xpatch.cache_stats()").fetchone()
        skip_after_low = after_low["skip_count"]

        # Raise the limit above our content size
        db.execute("SET pg_xpatch.cache_max_entry_kb = 256")

        # Read again - now it should be cacheable
        db.execute(f"SELECT content FROM {t} WHERE group_id = 1").fetchone()
        after_raised = db.execute("SELECT * FROM xpatch.cache_stats()").fetchone()

        # skip_count should NOT have increased further
        assert after_raised["skip_count"] == skip_after_low, (
            f"After raising limit, skip_count should stop increasing. "
            f"After low: {skip_after_low}, After raised: {after_raised['skip_count']}"
        )

    def test_lowering_limit_triggers_skips(self, db: psycopg.Connection, make_table):
        """Lowering the limit at runtime causes previously-cacheable entries to skip."""
        t = make_table()

        # Default limit (256KB) - insert 100KB content that fits
        db.execute("RESET pg_xpatch.cache_max_entry_kb")
        content_100kb = "A" * 100_000
        insert_rows(db, t, [(1, 1, content_100kb)])

        # Read with default limit - should cache fine
        db.execute(f"SELECT content FROM {t} WHERE group_id = 1").fetchone()
        before = db.execute("SELECT * FROM xpatch.cache_stats()").fetchone()

        # Now lower the limit below the content size
        db.execute("SET pg_xpatch.cache_max_entry_kb = 16")

        # Insert a second version (also ~100KB), which will need reconstruction
        content_100kb_v2 = "B" * 100_000
        insert_rows(db, t, [(1, 2, content_100kb_v2)])

        # Read the new version - reconstructed content exceeds the new limit
        db.execute(
            f"SELECT content FROM {t} WHERE group_id = 1 ORDER BY version DESC LIMIT 1"
        ).fetchone()
        after = db.execute("SELECT * FROM xpatch.cache_stats()").fetchone()

        assert after["skip_count"] > before["skip_count"], (
            f"Lowering limit should cause new skips. "
            f"Before: {before['skip_count']}, After: {after['skip_count']}"
        )


# ---------------------------------------------------------------------------
# Mixed workload: small and large entries in same table
# ---------------------------------------------------------------------------


class TestCacheMixedSizes:
    """Tables with both small and large entries cache correctly."""

    def test_small_entries_cached_large_entries_skipped(
        self, db: psycopg.Connection, make_table
    ):
        """In a table with mixed sizes, small entries are cached while large ones are skipped."""
        t = make_table()

        # Set a moderate limit
        db.execute("SET pg_xpatch.cache_max_entry_kb = 32")

        # Insert a small group (each version ~20 bytes, well under 32KB)
        insert_versions(db, t, group_id=1, count=5)

        # Insert a large group (each version ~100KB, over 32KB)
        for v in range(1, 4):
            big = "L" * 100_000
            insert_rows(db, t, [(2, v, big)])

        before = db.execute("SELECT * FROM xpatch.cache_stats()").fetchone()

        # Read small group twice - should get cache hits
        db.execute(f"SELECT * FROM {t} WHERE group_id = 1 ORDER BY version").fetchall()
        db.execute(f"SELECT * FROM {t} WHERE group_id = 1 ORDER BY version").fetchall()

        mid = db.execute("SELECT * FROM xpatch.cache_stats()").fetchone()
        hits_from_small = mid["hit_count"] - before["hit_count"]

        # Read large group - should get skips
        db.execute(f"SELECT * FROM {t} WHERE group_id = 2 ORDER BY version").fetchall()

        after = db.execute("SELECT * FROM xpatch.cache_stats()").fetchone()

        assert hits_from_small > 0, "Small entries should produce cache hits"
        assert after["skip_count"] > mid["skip_count"], (
            "Large entries should increment skip_count"
        )

    def test_multiple_large_entries_each_skipped(
        self, db: psycopg.Connection, make_table
    ):
        """Multiple large entries each contribute to skip_count."""
        t = make_table()
        db.execute("SET pg_xpatch.cache_max_entry_kb = 16")

        before = db.execute("SELECT skip_count FROM xpatch.cache_stats()").fetchone()

        # Insert 3 groups, each with one large version (~80KB)
        for g in range(1, 4):
            big = f"Group{g}_" + "X" * 80_000
            insert_rows(db, t, [(g, 1, big)])

        # Read all groups
        db.execute(f"SELECT * FROM {t}").fetchall()

        after = db.execute("SELECT skip_count FROM xpatch.cache_stats()").fetchone()

        skips = after["skip_count"] - before["skip_count"]
        # Each of the 3 groups should have at least 1 skip
        assert skips >= 3, (
            f"Expected at least 3 skips for 3 oversized groups, got {skips}"
        )


# ---------------------------------------------------------------------------
# Delta chain reconstruction with large files
# ---------------------------------------------------------------------------


class TestCacheLargeDeltaChain:
    """Delta chain walk behaviour for large files near/above the cache limit."""

    def test_versions_below_limit_use_cache_across_chain(
        self, db: psycopg.Connection, make_table
    ):
        """When content is below the limit, walking the delta chain populates the cache."""
        t = make_table(keyframe_every=5)

        # Insert 10 versions with ~1KB content (well under any limit)
        for v in range(1, 11):
            content = f"Version {v}: " + ("data_" * 200)
            insert_rows(db, t, [(1, v, content)])

        before = db.execute("SELECT * FROM xpatch.cache_stats()").fetchone()

        # Read latest version (triggers chain walk from keyframe)
        db.execute(
            f"SELECT content FROM {t} WHERE group_id = 1 ORDER BY version DESC LIMIT 1"
        ).fetchone()

        # Read again - intermediate results should be cached
        db.execute(
            f"SELECT content FROM {t} WHERE group_id = 1 ORDER BY version DESC LIMIT 1"
        ).fetchone()

        after = db.execute("SELECT * FROM xpatch.cache_stats()").fetchone()

        # Should have hits from the second read
        assert after["hit_count"] > before["hit_count"], (
            "Expected cache hits from delta chain intermediate results"
        )
        # skip_count should NOT increase
        assert after["skip_count"] == before["skip_count"], (
            "No skips expected for content under the limit"
        )

    def test_large_file_versions_all_skipped(
        self, db: psycopg.Connection, make_table
    ):
        """When all versions of a file exceed the limit, every chain walk misses."""
        t = make_table(keyframe_every=5)
        db.execute("SET pg_xpatch.cache_max_entry_kb = 16")

        # Insert 8 versions of a ~50KB file with small changes
        base = "A" * 50_000
        for v in range(1, 9):
            # Replace a slice with a fixed-length marker to keep total length consistent
            marker = f"v{v}".ljust(10, "_")
            content = base[:v * 100] + marker + base[v * 100 + 10:]
            insert_rows(db, t, [(1, v, content)])

        before = db.execute("SELECT * FROM xpatch.cache_stats()").fetchone()

        # Read the latest version
        row = db.execute(
            f"SELECT content FROM {t} WHERE group_id = 1 ORDER BY version DESC LIMIT 1"
        ).fetchone()
        assert len(row["content"]) == 50_000

        after = db.execute("SELECT * FROM xpatch.cache_stats()").fetchone()

        # skip_count should have increased (reconstructed intermediates all exceed 16KB)
        assert after["skip_count"] > before["skip_count"], (
            "Expected skips for all intermediate versions exceeding limit"
        )

    def test_data_integrity_despite_skips(self, db: psycopg.Connection, make_table):
        """Even when cache skips occur, data reconstruction is correct."""
        t = make_table(keyframe_every=10)
        db.execute("SET pg_xpatch.cache_max_entry_kb = 16")

        # Build a delta chain with known content
        contents = {}
        base = "BASE_" * 10_000  # ~50KB
        for v in range(1, 16):
            content = base[:v * 100] + f"===VERSION_{v}===" + base[v * 100 + 20:]
            contents[v] = content
            insert_rows(db, t, [(1, v, content)])

        # Read back every version and verify correctness
        rows = db.execute(
            f"SELECT version, content FROM {t} WHERE group_id = 1 ORDER BY version"
        ).fetchall()

        assert len(rows) == 15
        for row in rows:
            v = row["version"]
            assert row["content"] == contents[v], (
                f"Data mismatch at version {v} despite cache skip"
            )
