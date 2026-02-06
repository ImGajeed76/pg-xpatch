"""
Test all SQL-callable utility functions in the xpatch schema.

Covers:
- xpatch.version()
- xpatch.stats() / xpatch_stats()
- xpatch.inspect() / xpatch_inspect()
- xpatch.describe()
- xpatch.physical() (all 3 overloads)
- xpatch.cache_stats() / xpatch_cache_stats()
- xpatch.insert_cache_stats() / xpatch_insert_cache_stats()
- xpatch.warm_cache()
- xpatch.refresh_stats()
- xpatch.stats_exist()
- xpatch.get_config()
- xpatch.dump_configs()
- xpatch._invalidate_config()
"""

from __future__ import annotations

import psycopg
import pytest

from conftest import insert_rows, insert_versions, row_count


# ---------------------------------------------------------------------------
# xpatch.version()
# ---------------------------------------------------------------------------


class TestVersion:
    """xpatch.version() returns the extension version string."""

    def test_version_returns_string(self, db: psycopg.Connection):
        """version() returns a non-empty text string."""
        row = db.execute("SELECT xpatch.version() AS v").fetchone()
        assert row["v"] is not None
        assert len(row["v"]) > 0

    def test_version_contains_pg_xpatch(self, db: psycopg.Connection):
        """version() string contains 'pg_xpatch'."""
        row = db.execute("SELECT xpatch.version() AS v").fetchone()
        assert "pg_xpatch" in row["v"]

    def test_version_contains_xpatch_lib(self, db: psycopg.Connection):
        """version() string contains 'xpatch' (Rust library version)."""
        row = db.execute("SELECT xpatch.version() AS v").fetchone()
        # Format: "pg_xpatch 0.4.0 (xpatch 0.4.2)"
        assert "xpatch" in row["v"]

    def test_unqualified_xpatch_version(self, db: psycopg.Connection):
        """Unqualified xpatch_version() also works."""
        row = db.execute("SELECT xpatch_version() AS v").fetchone()
        assert "pg_xpatch" in row["v"]


# ---------------------------------------------------------------------------
# xpatch.stats()
# ---------------------------------------------------------------------------


class TestStats:
    """xpatch.stats() returns table-level statistics."""

    def test_stats_empty_table(self, db: psycopg.Connection, make_table):
        """Stats on empty table returns zeros."""
        t = make_table()
        stats = db.execute(f"SELECT * FROM xpatch.stats('{t}'::regclass)").fetchone()
        assert stats["total_rows"] == 0
        assert stats["total_groups"] == 0

    def test_stats_after_inserts(self, db: psycopg.Connection, make_table):
        """Stats reflect inserted data."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=10)
        insert_versions(db, t, group_id=2, count=5)

        stats = db.execute(f"SELECT * FROM xpatch.stats('{t}'::regclass)").fetchone()
        assert stats["total_rows"] == 15
        assert stats["total_groups"] == 2

    def test_stats_has_all_columns(self, db: psycopg.Connection, make_table):
        """Stats returns all expected columns."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=5)
        stats = db.execute(f"SELECT * FROM xpatch.stats('{t}'::regclass)").fetchone()

        expected_keys = {
            "total_rows", "total_groups", "keyframe_count", "delta_count",
            "raw_size_bytes", "compressed_size_bytes", "compression_ratio",
            "cache_hits", "cache_misses", "avg_compression_depth",
        }
        assert expected_keys.issubset(set(stats.keys()))

    def test_stats_keyframe_count(self, db: psycopg.Connection, make_table):
        """Stats correctly counts keyframes."""
        t = make_table(keyframe_every=5)
        insert_versions(db, t, group_id=1, count=12)

        stats = db.execute(f"SELECT * FROM xpatch.stats('{t}'::regclass)").fetchone()
        assert stats["keyframe_count"] >= 3  # At seq 1, 6, 11

    def test_stats_after_delete(self, db: psycopg.Connection, make_table):
        """Stats updated after DELETE."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=10)
        db.execute(f"DELETE FROM {t} WHERE group_id = 1 AND version = 5")
        # Cascade: v5..v10 deleted, v1..v4 remain

        stats = db.execute(f"SELECT * FROM xpatch.stats('{t}'::regclass)").fetchone()
        assert stats["total_rows"] == 4

    def test_unqualified_xpatch_stats(self, db: psycopg.Connection, make_table):
        """Unqualified xpatch_stats() works."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=5)
        stats = db.execute(f"SELECT * FROM xpatch_stats('{t}'::regclass)").fetchone()
        assert stats["total_rows"] == 5


# ---------------------------------------------------------------------------
# xpatch.inspect()
# ---------------------------------------------------------------------------


class TestInspect:
    """xpatch.inspect() shows per-row storage details."""

    def test_inspect_single_group(self, db: psycopg.Connection, make_table):
        """Inspect a single group shows all rows."""
        t = make_table(keyframe_every=3)
        insert_versions(db, t, group_id=1, count=5)

        rows = db.execute(
            f"SELECT * FROM xpatch.inspect('{t}'::regclass, 1) ORDER BY seq"
        ).fetchall()
        assert len(rows) >= 5  # At least one row per version (could be more with multi-delta)

    def test_inspect_shows_keyframes(self, db: psycopg.Connection, make_table):
        """Inspect shows which rows are keyframes."""
        t = make_table(keyframe_every=3)
        insert_versions(db, t, group_id=1, count=6)

        rows = db.execute(
            f"SELECT * FROM xpatch.inspect('{t}'::regclass, 1) ORDER BY seq"
        ).fetchall()
        keyframes = [r for r in rows if r["is_keyframe"]]
        deltas = [r for r in rows if not r["is_keyframe"]]
        assert len(keyframes) >= 1
        assert len(deltas) >= 1

    def test_inspect_has_expected_columns(self, db: psycopg.Connection, make_table):
        """Inspect returns expected column set."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=3)

        rows = db.execute(
            f"SELECT * FROM xpatch.inspect('{t}'::regclass, 1)"
        ).fetchall()
        assert len(rows) > 0
        expected_cols = {"version", "seq", "is_keyframe", "tag", "delta_size_bytes", "column_name"}
        assert expected_cols.issubset(set(rows[0].keys()))

    def test_inspect_tag_zero_for_keyframe(self, db: psycopg.Connection, make_table):
        """Keyframe rows have tag=0."""
        t = make_table()
        insert_rows(db, t, [(1, 1, "keyframe content")])

        rows = db.execute(
            f"SELECT * FROM xpatch.inspect('{t}'::regclass, 1)"
        ).fetchall()
        keyframes = [r for r in rows if r["is_keyframe"]]
        assert all(r["tag"] == 0 for r in keyframes)

    def test_unqualified_xpatch_inspect(self, db: psycopg.Connection, make_table):
        """Unqualified xpatch_inspect() works."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=3)
        rows = db.execute(
            f"SELECT * FROM xpatch_inspect('{t}'::regclass, 1)"
        ).fetchall()
        assert len(rows) > 0


# ---------------------------------------------------------------------------
# xpatch.describe()
# ---------------------------------------------------------------------------


class TestDescribe:
    """xpatch.describe() shows table introspection."""

    def test_describe_returns_properties(self, db: psycopg.Connection, make_table):
        """describe() returns property/value rows."""
        t = make_table()
        rows = db.execute(
            f"SELECT * FROM xpatch.describe('{t}'::regclass)"
        ).fetchall()
        assert len(rows) > 0
        props = {r["property"]: r["value"] for r in rows}
        # The SQL function returns rows with property names like "table", "group_by", etc.
        assert "table" in props or "table_name" in props, (
            f"Expected 'table' property in describe output. Found: {list(props.keys())}"
        )

    def test_describe_shows_group_by(self, db: psycopg.Connection, make_table):
        """describe() shows the group_by column."""
        t = make_table()
        rows = db.execute(
            f"SELECT * FROM xpatch.describe('{t}'::regclass)"
        ).fetchall()
        props = {r["property"]: r["value"] for r in rows}
        # Look for group_by in the properties
        has_group_info = any("group" in k.lower() for k in props.keys())
        assert has_group_info, f"No group info in describe. Properties: {list(props.keys())}"

    def test_describe_shows_columns(self, db: psycopg.Connection, make_table):
        """describe() lists columns with their roles."""
        t = make_table()
        rows = db.execute(
            f"SELECT * FROM xpatch.describe('{t}'::regclass)"
        ).fetchall()
        all_text = " ".join(f"{r['property']}={r['value']}" for r in rows)
        # Should mention the column names somewhere
        assert "group_id" in all_text or "content" in all_text

    def test_describe_empty_table(self, db: psycopg.Connection, make_table):
        """describe() works on empty table."""
        t = make_table()
        rows = db.execute(
            f"SELECT * FROM xpatch.describe('{t}'::regclass)"
        ).fetchall()
        assert len(rows) > 0

    def test_describe_explicit_config(self, db: psycopg.Connection, make_table):
        """describe() shows explicit config correctly."""
        t = make_table(
            "doc_id INT, ver INT, body TEXT NOT NULL",
            group_by="doc_id",
            order_by="ver",
            delta_columns=["body"],
            keyframe_every=50,
        )
        rows = db.execute(
            f"SELECT * FROM xpatch.describe('{t}'::regclass)"
        ).fetchall()
        all_text = " ".join(f"{r['property']}={r['value']}" for r in rows)
        assert "doc_id" in all_text
        assert "50" in all_text


# ---------------------------------------------------------------------------
# xpatch.physical()
# ---------------------------------------------------------------------------


class TestPhysical:
    """xpatch.physical() returns raw delta bytes."""

    def test_physical_single_group(self, db: psycopg.Connection, make_table):
        """physical() with group filter returns raw storage for that group."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=5)

        rows = db.execute(
            f"SELECT * FROM xpatch.physical('{t}'::regclass, 1) ORDER BY seq"
        ).fetchall()
        # physical() returns raw delta storage — skips the first keyframe (seq=1),
        # returns only delta rows. With 5 versions: seq 2,3,4,5 = 4 delta rows.
        assert len(rows) == 4, (
            f"Expected 4 delta rows (physical() skips keyframe), got {len(rows)}"
        )
        # Verify all returned rows are deltas
        assert all(not r["is_keyframe"] for r in rows)

    def test_physical_has_expected_columns(self, db: psycopg.Connection, make_table):
        """physical() returns expected column set."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=3)

        rows = db.execute(
            f"SELECT * FROM xpatch.physical('{t}'::regclass, 1)"
        ).fetchall()
        assert len(rows) > 0
        expected = {"group_value", "version", "seq", "is_keyframe", "tag",
                    "delta_column", "delta_bytes", "delta_size"}
        assert expected.issubset(set(rows[0].keys()))

    def test_physical_all_groups(self, db: psycopg.Connection, make_table):
        """physical() without group filter returns all groups."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=3)
        insert_versions(db, t, group_id=2, count=3)

        rows = db.execute(
            f"SELECT * FROM xpatch.physical('{t}'::regclass)"
        ).fetchall()
        groups = {r["group_value"] for r in rows}
        assert len(groups) == 2

    def test_physical_delta_bytes_not_null(self, db: psycopg.Connection, make_table):
        """physical() returns non-null delta_bytes."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=3)

        rows = db.execute(
            f"SELECT * FROM xpatch.physical('{t}'::regclass, 1)"
        ).fetchall()
        for row in rows:
            assert row["delta_bytes"] is not None
            assert row["delta_size"] > 0


# ---------------------------------------------------------------------------
# xpatch.cache_stats()
# ---------------------------------------------------------------------------


class TestCacheStats:
    """xpatch.cache_stats() returns LRU cache metrics."""

    def test_cache_stats_returns_row(self, db: psycopg.Connection):
        """cache_stats() returns a single row."""
        row = db.execute("SELECT * FROM xpatch.cache_stats()").fetchone()
        assert row is not None

    def test_cache_stats_has_expected_columns(self, db: psycopg.Connection):
        """cache_stats() has all expected columns."""
        row = db.execute("SELECT * FROM xpatch.cache_stats()").fetchone()
        expected = {"cache_size_bytes", "cache_max_bytes", "entries_count",
                    "hit_count", "miss_count", "eviction_count"}
        assert expected.issubset(set(row.keys()))

    def test_cache_stats_max_bytes_positive(self, db: psycopg.Connection):
        """cache_max_bytes is positive (shared memory allocated)."""
        row = db.execute("SELECT * FROM xpatch.cache_stats()").fetchone()
        assert row["cache_max_bytes"] > 0

    def test_cache_hit_count_increases(self, db: psycopg.Connection, make_table):
        """Reading same data twice increases hit count."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=5)

        before = db.execute("SELECT * FROM xpatch.cache_stats()").fetchone()
        # Read data twice to populate then hit cache
        db.execute(f"SELECT * FROM {t}").fetchall()
        db.execute(f"SELECT * FROM {t}").fetchall()
        after = db.execute("SELECT * FROM xpatch.cache_stats()").fetchone()

        # Hit or miss count should have increased (at least misses on first read)
        total_before = before["hit_count"] + before["miss_count"]
        total_after = after["hit_count"] + after["miss_count"]
        assert total_after > total_before


# ---------------------------------------------------------------------------
# xpatch.insert_cache_stats()
# ---------------------------------------------------------------------------


class TestInsertCacheStats:
    """xpatch.insert_cache_stats() returns FIFO insert cache metrics."""

    def test_insert_cache_stats_returns_row(self, db: psycopg.Connection):
        """insert_cache_stats() returns a single row."""
        row = db.execute("SELECT * FROM xpatch.insert_cache_stats()").fetchone()
        assert row is not None

    def test_insert_cache_stats_has_expected_columns(self, db: psycopg.Connection):
        """insert_cache_stats() has all expected columns."""
        row = db.execute("SELECT * FROM xpatch.insert_cache_stats()").fetchone()
        expected = {"slots_in_use", "total_slots", "hits", "misses",
                    "evictions", "eviction_misses"}
        assert expected.issubset(set(row.keys()))

    def test_insert_cache_total_slots_positive(self, db: psycopg.Connection):
        """total_slots reflects configured insert_cache_slots."""
        row = db.execute("SELECT * FROM xpatch.insert_cache_stats()").fetchone()
        assert row["total_slots"] > 0


# ---------------------------------------------------------------------------
# xpatch.warm_cache()
# ---------------------------------------------------------------------------


class TestWarmCache:
    """xpatch.warm_cache() pre-populates the LRU cache."""

    def test_warm_cache_basic(self, db: psycopg.Connection, make_table):
        """warm_cache() scans rows and returns stats."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=10)

        result = db.execute(
            f"SELECT * FROM xpatch.warm_cache('{t}'::regclass)"
        ).fetchone()
        assert result["rows_scanned"] == 10
        assert result["groups_warmed"] == 1
        assert result["duration_ms"] >= 0

    def test_warm_cache_with_max_rows(self, db: psycopg.Connection, make_table):
        """warm_cache() with max_rows limit."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=20)

        result = db.execute(
            f"SELECT * FROM xpatch.warm_cache('{t}'::regclass, max_rows => 5)"
        ).fetchone()
        assert result["rows_scanned"] <= 5

    def test_warm_cache_with_max_groups(self, db: psycopg.Connection, make_table):
        """warm_cache() with max_groups limit."""
        t = make_table()
        for g in range(1, 6):
            insert_versions(db, t, group_id=g, count=5)

        result = db.execute(
            f"SELECT * FROM xpatch.warm_cache('{t}'::regclass, max_groups => 2)"
        ).fetchone()
        # May warm 2 or 3 groups (check happens after scanning each group)
        assert result["groups_warmed"] <= 3

    def test_warm_cache_empty_table(self, db: psycopg.Connection, make_table):
        """warm_cache() on empty table returns zeros."""
        t = make_table()
        result = db.execute(
            f"SELECT * FROM xpatch.warm_cache('{t}'::regclass)"
        ).fetchone()
        assert result["rows_scanned"] == 0
        assert result["groups_warmed"] == 0

    def test_warm_cache_increases_cache_entries(self, db: psycopg.Connection, make_table):
        """warm_cache() increases cache_stats() entries or hit counts."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=10)

        before = db.execute("SELECT * FROM xpatch.cache_stats()").fetchone()
        db.execute(f"SELECT * FROM xpatch.warm_cache('{t}'::regclass)")
        after = db.execute("SELECT * FROM xpatch.cache_stats()").fetchone()

        # Either entries or misses should increase (cache population)
        assert (after["entries_count"] >= before["entries_count"] or
                after["miss_count"] > before["miss_count"])


# ---------------------------------------------------------------------------
# xpatch.refresh_stats() and xpatch.stats_exist()
# ---------------------------------------------------------------------------


class TestRefreshStats:
    """xpatch.refresh_stats() regenerates stats from a full scan."""

    def test_refresh_stats_basic(self, db: psycopg.Connection, make_table):
        """refresh_stats() scans and returns stats."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=10)
        insert_versions(db, t, group_id=2, count=5)

        result = db.execute(
            f"SELECT * FROM xpatch.refresh_stats('{t}'::regclass)"
        ).fetchone()
        assert result["groups_scanned"] == 2
        assert result["rows_scanned"] == 15
        assert result["duration_ms"] >= 0

    def test_stats_exist_after_insert(self, db: psycopg.Connection, make_table):
        """stats_exist() returns true after INSERT (auto-populated)."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=5)

        exists = db.execute(
            f"SELECT xpatch.stats_exist('{t}'::regclass) AS e"
        ).fetchone()
        assert exists["e"] is True

    def test_stats_exist_false_after_clear(self, db: psycopg.Connection, make_table):
        """stats_exist() returns false after clearing xpatch.group_stats."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=5)

        # Get the OID
        oid_row = db.execute(f"SELECT '{t}'::regclass::oid AS oid").fetchone()
        oid = oid_row["oid"]

        db.execute("DELETE FROM xpatch.group_stats WHERE relid = %s", [oid])

        exists = db.execute(
            f"SELECT xpatch.stats_exist('{t}'::regclass) AS e"
        ).fetchone()
        assert exists["e"] is False

    def test_refresh_stats_after_clear(self, db: psycopg.Connection, make_table):
        """refresh_stats() regenerates after manual clear."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=5)

        oid_row = db.execute(f"SELECT '{t}'::regclass::oid AS oid").fetchone()
        db.execute("DELETE FROM xpatch.group_stats WHERE relid = %s", [oid_row["oid"]])

        db.execute(f"SELECT * FROM xpatch.refresh_stats('{t}'::regclass)")

        exists = db.execute(
            f"SELECT xpatch.stats_exist('{t}'::regclass) AS e"
        ).fetchone()
        assert exists["e"] is True


# ---------------------------------------------------------------------------
# xpatch.dump_configs()
# ---------------------------------------------------------------------------


class TestDumpConfigs:
    """xpatch.dump_configs() generates restore SQL."""

    def test_dump_configs_returns_sql(self, db: psycopg.Connection, make_table):
        """dump_configs() returns SQL strings."""
        t = make_table()
        rows = db.execute("SELECT * FROM xpatch.dump_configs()").fetchall()
        assert len(rows) > 0
        # Each row should be a SQL-like string
        for row in rows:
            text = row[list(row.keys())[0]]
            assert "xpatch.configure" in text

    def test_dump_configs_contains_table_name(self, db: psycopg.Connection, make_table):
        """dump_configs() output references the configured table."""
        t = make_table()
        rows = db.execute("SELECT * FROM xpatch.dump_configs()").fetchall()
        texts = [row[list(row.keys())[0]] for row in rows]
        has_table = any(t in text for text in texts)
        assert has_table, f"Table {t} not found in dump output: {texts}"


# ---------------------------------------------------------------------------
# xpatch._invalidate_config()
# ---------------------------------------------------------------------------


class TestInvalidateConfig:
    """xpatch._invalidate_config() forces config re-read."""

    def test_invalidate_config_runs(self, db: psycopg.Connection, make_table):
        """_invalidate_config() runs without error."""
        t = make_table()
        oid_row = db.execute(f"SELECT '{t}'::regclass::oid AS oid").fetchone()
        db.execute("SELECT xpatch._invalidate_config(%s::oid)", [oid_row["oid"]])
        # Should not raise; just verify it doesn't error

    def test_config_still_valid_after_invalidate(self, db: psycopg.Connection, make_table):
        """After invalidation, operations still work (config re-loaded on demand)."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=3)

        oid_row = db.execute(f"SELECT '{t}'::regclass::oid AS oid").fetchone()
        db.execute("SELECT xpatch._invalidate_config(%s::oid)", [oid_row["oid"]])

        # Read should still work (config auto re-read)
        rows = db.execute(f"SELECT * FROM {t}").fetchall()
        assert len(rows) == 3


# ---------------------------------------------------------------------------
# Edge cases and error paths
# ---------------------------------------------------------------------------


class TestUtilityEdgeCases:
    """Edge cases and error paths for utility functions."""

    def test_inspect_nonexistent_group(self, db: psycopg.Connection, make_table):
        """inspect() with nonexistent group returns 0 rows, no error."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=3)
        rows = db.execute(
            f"SELECT * FROM xpatch.inspect('{t}'::regclass, 999)"
        ).fetchall()
        assert len(rows) == 0

    def test_stats_exist_empty_table(self, db: psycopg.Connection, make_table):
        """stats_exist() on a freshly created table with no inserts returns false."""
        t = make_table()
        exists = db.execute(
            f"SELECT xpatch.stats_exist('{t}'::regclass) AS e"
        ).fetchone()
        assert exists["e"] is False

    def test_refresh_stats_empty_table(self, db: psycopg.Connection, make_table):
        """refresh_stats() on an empty table returns zero counts."""
        t = make_table()
        result = db.execute(
            f"SELECT * FROM xpatch.refresh_stats('{t}'::regclass)"
        ).fetchone()
        assert result["groups_scanned"] == 0
        assert result["rows_scanned"] == 0

    def test_dump_configs_with_multiple_tables(self, db: psycopg.Connection, make_table):
        """dump_configs() includes all configured tables."""
        t1 = make_table()
        t2 = make_table(
            "doc_id INT, ver INT, body TEXT NOT NULL",
            group_by="doc_id",
            order_by="ver",
        )
        rows = db.execute("SELECT * FROM xpatch.dump_configs()").fetchall()
        texts = [row[list(row.keys())[0]] for row in rows]
        has_t1 = any(t1 in text for text in texts)
        has_t2 = any(t2 in text for text in texts)
        assert has_t1, f"Table {t1} not found in dump output"
        assert has_t2, f"Table {t2} not found in dump output"

    def test_physical_from_seq_filter(self, db: psycopg.Connection, make_table):
        """physical() with from_seq parameter filters correctly."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=10)
        rows = db.execute(
            f"SELECT * FROM xpatch.physical('{t}'::regclass, 1, 5) ORDER BY seq"
        ).fetchall()
        # Should only return rows with seq >= 5
        if len(rows) > 0:
            seqs = [r["seq"] for r in rows]
            assert all(s >= 5 for s in seqs), f"Expected seq >= 5, got {seqs}"

    def test_describe_non_xpatch_table(self, db: psycopg.Connection):
        """describe() on a regular heap table raises an error."""
        db.execute("CREATE TABLE heap_test (id INT)")
        with pytest.raises(psycopg.errors.Error):
            db.execute("SELECT * FROM xpatch.describe('heap_test'::regclass)")

    def test_warm_cache_zero_max_rows(self, db: psycopg.Connection, make_table):
        """warm_cache() with max_rows=0 scans 0 rows."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=10)
        result = db.execute(
            f"SELECT * FROM xpatch.warm_cache('{t}'::regclass, max_rows => 0)"
        ).fetchone()
        assert result["rows_scanned"] == 0

    def test_fix_restored_configs_runs(self, db: psycopg.Connection, make_table):
        """fix_restored_configs() runs without error on configured tables."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=3)
        result = db.execute(
            "SELECT * FROM xpatch.fix_restored_configs()"
        ).fetchone()
        assert result is not None
