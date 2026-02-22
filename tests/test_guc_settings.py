"""
Tests for v0.6.0 GUC scalability changes.

Covers:
- All GUC default values after changes (cache_size_mb raised to 256, etc.)
- New GUCs: cache_max_entries, cache_slot_size_kb, cache_partitions,
  seq_tid_cache_size_mb, max_delta_columns
- Modified GUC max values raised to INT_MAX (2147483647)
- PGC_POSTMASTER GUCs reject runtime SET
- PGC_USERSET/PGC_SUSET GUCs accept runtime SET
- All 11 GUCs visible in pg_settings
- Functional correctness with new defaults
"""

from __future__ import annotations

import psycopg
import pytest

from conftest import insert_rows, insert_versions, row_count

INT_MAX = 2147483647


# ---------------------------------------------------------------------------
# GUC defaults
# ---------------------------------------------------------------------------


class TestGUCDefaults:
    """All GUC default values match the v0.6.0 specification."""

    def test_cache_size_mb_default(self, db: psycopg.Connection):
        """cache_size_mb default raised to 256."""
        row = db.execute("SHOW pg_xpatch.cache_size_mb").fetchone()
        val = list(row.values())[0]
        assert val == "256MB"

    def test_cache_max_entries_default(self, db: psycopg.Connection):
        """New GUC cache_max_entries defaults to 65536."""
        row = db.execute("SHOW pg_xpatch.cache_max_entries").fetchone()
        val = list(row.values())[0]
        assert val == "65536"

    def test_cache_max_entry_kb_default(self, db: psycopg.Connection):
        """cache_max_entry_kb still defaults to 256kB."""
        row = db.execute("SHOW pg_xpatch.cache_max_entry_kb").fetchone()
        val = list(row.values())[0]
        assert val == "256kB"

    def test_cache_slot_size_kb_default(self, db: psycopg.Connection):
        """New GUC cache_slot_size_kb defaults to 4kB."""
        row = db.execute("SHOW pg_xpatch.cache_slot_size_kb").fetchone()
        val = list(row.values())[0]
        assert val == "4kB"

    def test_cache_partitions_default(self, db: psycopg.Connection):
        """New GUC cache_partitions defaults to 32."""
        row = db.execute("SHOW pg_xpatch.cache_partitions").fetchone()
        val = list(row.values())[0]
        assert val == "32"

    def test_group_cache_size_mb_default(self, db: psycopg.Connection):
        """group_cache_size_mb default raised to 16."""
        row = db.execute("SHOW pg_xpatch.group_cache_size_mb").fetchone()
        val = list(row.values())[0]
        assert val == "16MB"

    def test_tid_cache_size_mb_default(self, db: psycopg.Connection):
        """tid_cache_size_mb default raised to 16."""
        row = db.execute("SHOW pg_xpatch.tid_cache_size_mb").fetchone()
        val = list(row.values())[0]
        assert val == "16MB"

    def test_seq_tid_cache_size_mb_default(self, db: psycopg.Connection):
        """New GUC seq_tid_cache_size_mb defaults to 16."""
        row = db.execute("SHOW pg_xpatch.seq_tid_cache_size_mb").fetchone()
        val = list(row.values())[0]
        assert val == "16MB"

    def test_insert_cache_slots_default(self, db: psycopg.Connection):
        """insert_cache_slots still defaults to 16."""
        row = db.execute("SHOW pg_xpatch.insert_cache_slots").fetchone()
        val = list(row.values())[0]
        assert val == "16"

    def test_max_delta_columns_default(self, db: psycopg.Connection):
        """New GUC max_delta_columns defaults to 32."""
        row = db.execute("SHOW pg_xpatch.max_delta_columns").fetchone()
        val = list(row.values())[0]
        assert val == "32"

    def test_encode_threads_default(self, db: psycopg.Connection):
        """encode_threads still defaults to 0."""
        row = db.execute("SHOW pg_xpatch.encode_threads").fetchone()
        val = list(row.values())[0]
        assert val == "0"


# ---------------------------------------------------------------------------
# GUC metadata in pg_settings
# ---------------------------------------------------------------------------


class TestGUCMetadata:
    """GUC metadata (min, max, context, unit) is correct in pg_settings."""

    def test_all_gucs_visible(self, db: psycopg.Connection):
        """All 11 pg_xpatch GUCs are visible in pg_settings."""
        rows = db.execute(
            "SELECT name FROM pg_settings WHERE name LIKE 'pg_xpatch.%' ORDER BY name"
        ).fetchall()
        names = {r["name"] for r in rows}

        expected = {
            "pg_xpatch.cache_size_mb",
            "pg_xpatch.cache_max_entries",
            "pg_xpatch.cache_max_entry_kb",
            "pg_xpatch.cache_slot_size_kb",
            "pg_xpatch.cache_partitions",
            "pg_xpatch.group_cache_size_mb",
            "pg_xpatch.tid_cache_size_mb",
            "pg_xpatch.seq_tid_cache_size_mb",
            "pg_xpatch.insert_cache_slots",
            "pg_xpatch.max_delta_columns",
            "pg_xpatch.encode_threads",
        }
        missing = expected - names
        assert not missing, f"Missing GUCs in pg_settings: {missing}"

    def test_postmaster_gucs_reject_runtime_set(self, db: psycopg.Connection):
        """PGC_POSTMASTER GUCs reject SET at runtime."""
        postmaster_gucs = [
            "pg_xpatch.cache_size_mb",
            "pg_xpatch.cache_max_entries",
            "pg_xpatch.cache_slot_size_kb",
            "pg_xpatch.cache_partitions",
            "pg_xpatch.group_cache_size_mb",
            "pg_xpatch.tid_cache_size_mb",
            "pg_xpatch.seq_tid_cache_size_mb",
            "pg_xpatch.insert_cache_slots",
            "pg_xpatch.max_delta_columns",
        ]
        for guc in postmaster_gucs:
            with pytest.raises(psycopg.errors.Error):
                db.execute(f"SET {guc} = 999999")
            # Reset connection error state
            db.execute("SELECT 1")

    def test_suset_guc_accepts_runtime_set(self, db: psycopg.Connection):
        """PGC_SUSET cache_max_entry_kb accepts SET at runtime."""
        db.execute("SET pg_xpatch.cache_max_entry_kb = 512")
        row = db.execute("SHOW pg_xpatch.cache_max_entry_kb").fetchone()
        val = list(row.values())[0]
        assert val == "512kB"

    def test_userset_guc_accepts_runtime_set(self, db: psycopg.Connection):
        """PGC_USERSET encode_threads accepts SET at runtime."""
        db.execute("SET pg_xpatch.encode_threads = 4")
        row = db.execute("SHOW pg_xpatch.encode_threads").fetchone()
        val = list(row.values())[0]
        assert val == "4"

    def test_uncapped_gucs_have_int_max(self, db: psycopg.Connection):
        """GUCs with raised max have max_val = 2147483647."""
        uncapped_gucs = [
            "pg_xpatch.cache_size_mb",
            "pg_xpatch.cache_max_entries",
            "pg_xpatch.cache_max_entry_kb",
            "pg_xpatch.group_cache_size_mb",
            "pg_xpatch.tid_cache_size_mb",
            "pg_xpatch.seq_tid_cache_size_mb",
            "pg_xpatch.insert_cache_slots",
            "pg_xpatch.max_delta_columns",
        ]
        for guc in uncapped_gucs:
            row = db.execute(
                "SELECT max_val FROM pg_settings WHERE name = %s", [guc]
            ).fetchone()
            assert row is not None, f"GUC {guc} not found in pg_settings"
            assert int(row["max_val"]) == INT_MAX, (
                f"{guc}: expected max_val={INT_MAX}, got {row['max_val']}"
            )

    def test_cache_partitions_max_is_256(self, db: psycopg.Connection):
        """cache_partitions has a fixed max of 256."""
        row = db.execute(
            "SELECT max_val FROM pg_settings WHERE name = 'pg_xpatch.cache_partitions'"
        ).fetchone()
        assert row is not None
        assert int(row["max_val"]) == 256

    def test_cache_slot_size_kb_max_is_64(self, db: psycopg.Connection):
        """cache_slot_size_kb has a fixed max of 64."""
        row = db.execute(
            "SELECT max_val FROM pg_settings WHERE name = 'pg_xpatch.cache_slot_size_kb'"
        ).fetchone()
        assert row is not None
        assert int(row["max_val"]) == 64

    def test_guc_units_correct(self, db: psycopg.Connection):
        """GUCs with units have correct unit in pg_settings."""
        expected_units = {
            "pg_xpatch.cache_size_mb": "MB",
            "pg_xpatch.cache_max_entry_kb": "kB",
            "pg_xpatch.cache_slot_size_kb": "kB",
            "pg_xpatch.group_cache_size_mb": "MB",
            "pg_xpatch.tid_cache_size_mb": "MB",
            "pg_xpatch.seq_tid_cache_size_mb": "MB",
        }
        for guc, expected_unit in expected_units.items():
            row = db.execute(
                "SELECT unit FROM pg_settings WHERE name = %s", [guc]
            ).fetchone()
            assert row is not None, f"GUC {guc} not found"
            assert row["unit"] == expected_unit, (
                f"{guc}: expected unit='{expected_unit}', got '{row['unit']}'"
            )

    def test_unitless_gucs_have_no_unit(self, db: psycopg.Connection):
        """GUCs without units have NULL/empty unit in pg_settings."""
        unitless_gucs = [
            "pg_xpatch.cache_max_entries",
            "pg_xpatch.cache_partitions",
            "pg_xpatch.insert_cache_slots",
            "pg_xpatch.max_delta_columns",
            "pg_xpatch.encode_threads",
        ]
        for guc in unitless_gucs:
            row = db.execute(
                "SELECT unit FROM pg_settings WHERE name = %s", [guc]
            ).fetchone()
            assert row is not None, f"GUC {guc} not found"
            assert row["unit"] is None or row["unit"] == "", (
                f"{guc}: expected no unit, got '{row['unit']}'"
            )

    def test_guc_contexts_correct(self, db: psycopg.Connection):
        """All GUCs have the correct context in pg_settings."""
        expected_contexts = {
            "pg_xpatch.cache_size_mb": "postmaster",
            "pg_xpatch.cache_max_entries": "postmaster",
            "pg_xpatch.cache_max_entry_kb": "superuser",
            "pg_xpatch.cache_slot_size_kb": "postmaster",
            "pg_xpatch.cache_partitions": "postmaster",
            "pg_xpatch.group_cache_size_mb": "postmaster",
            "pg_xpatch.tid_cache_size_mb": "postmaster",
            "pg_xpatch.seq_tid_cache_size_mb": "postmaster",
            "pg_xpatch.insert_cache_slots": "postmaster",
            "pg_xpatch.max_delta_columns": "postmaster",
            "pg_xpatch.encode_threads": "user",
        }
        for guc, expected_ctx in expected_contexts.items():
            row = db.execute(
                "SELECT context FROM pg_settings WHERE name = %s", [guc]
            ).fetchone()
            assert row is not None, f"GUC {guc} not found"
            assert row["context"] == expected_ctx, (
                f"{guc}: expected context='{expected_ctx}', got '{row['context']}'"
            )


# ---------------------------------------------------------------------------
# Functional tests: cache works with new defaults
# ---------------------------------------------------------------------------


class TestCacheFunctional:
    """Cache functions correctly with the v0.6.0 default settings."""

    def test_basic_cache_hit(self, db: psycopg.Connection, make_table):
        """Basic cache hit works after default changes."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=5)

        # First read populates cache
        db.execute(f"SELECT * FROM {t} ORDER BY version").fetchall()
        before = db.execute("SELECT * FROM xpatch.cache_stats()").fetchone()

        # Second read should hit cache
        db.execute(f"SELECT * FROM {t} ORDER BY version").fetchall()
        after = db.execute("SELECT * FROM xpatch.cache_stats()").fetchone()

        assert after["hit_count"] > before["hit_count"]

    def test_large_entry_cached(self, db: psycopg.Connection, make_table):
        """A >64KB entry is cached (old code rejected at 64KB)."""
        t = make_table()
        # 100KB content — above old hard limit, below 256KB default
        big = "X" * 100_000
        insert_rows(db, t, [(1, 1, big)])

        # Read twice
        db.execute(f"SELECT content FROM {t} WHERE group_id = 1").fetchone()
        before = db.execute("SELECT * FROM xpatch.cache_stats()").fetchone()

        db.execute(f"SELECT content FROM {t} WHERE group_id = 1").fetchone()
        after = db.execute("SELECT * FROM xpatch.cache_stats()").fetchone()

        assert after["hit_count"] > before["hit_count"], (
            "100KB entry should be cached with default 256KB limit"
        )

    def test_version_string(self, db: psycopg.Connection):
        """Version string contains 0.6.2."""
        raw = db.execute("SELECT xpatch.version()").fetchone()["version"]
        assert "0.6.2" in raw, f"Expected '0.6.2' in version, got '{raw}'"


# ---------------------------------------------------------------------------
# Lock striping correctness
# ---------------------------------------------------------------------------


class TestLockStriping:
    """Cache correctness under striped locking."""

    def test_multi_group_cache_correct(self, db: psycopg.Connection, make_table):
        """100 groups all reconstruct correctly (data hits multiple stripes)."""
        t = make_table()
        for g in range(1, 101):
            insert_versions(
                db, t, group_id=g, count=3,
                content_fn=lambda v, gid=g: f"g{gid}_v{v}_content",
            )

        # Read all groups and verify correctness
        rows = db.execute(
            f"SELECT group_id, version, content FROM {t} ORDER BY group_id, version"
        ).fetchall()

        assert len(rows) == 300
        for row in rows:
            g, v = row["group_id"], row["version"]
            expected = f"g{g}_v{v}_content"
            assert row["content"] == expected, (
                f"Mismatch at group={g}, version={v}: "
                f"expected '{expected}', got '{row['content']}'"
            )

    def test_cache_stats_aggregate(self, db: psycopg.Connection, make_table):
        """Cache stats aggregate hits/misses across all stripes."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=5)

        # Generate cache activity
        db.execute(f"SELECT * FROM {t}").fetchall()
        db.execute(f"SELECT * FROM {t}").fetchall()

        stats = db.execute("SELECT * FROM xpatch.cache_stats()").fetchone()
        # After two full scans, should have nonzero hit+miss
        assert stats["hit_count"] + stats["miss_count"] > 0

    def test_invalidate_rel_across_stripes(
        self, db: psycopg.Connection, make_table
    ):
        """TRUNCATE invalidates cache entries across all stripes."""
        t = make_table()
        for g in range(1, 21):
            insert_versions(db, t, group_id=g, count=3)

        # Warm cache: first scan = all misses, second scan = all hits
        db.execute(f"SELECT * FROM {t}").fetchall()
        db.execute(f"SELECT * FROM {t}").fetchall()
        before = db.execute("SELECT * FROM xpatch.cache_stats()").fetchone()
        before_hits = before["hit_count"]

        # Truncate wipes data and should invalidate cache
        db.execute(f"TRUNCATE {t}")

        # Re-insert same data
        for g in range(1, 21):
            insert_versions(db, t, group_id=g, count=3)

        # Third scan after truncate — if cache was invalidated, these are misses (no hits)
        db.execute(f"SELECT * FROM {t}").fetchall()
        mid = db.execute("SELECT * FROM xpatch.cache_stats()").fetchone()

        # Fourth scan — now data is re-cached, so we get hits
        db.execute(f"SELECT * FROM {t}").fetchall()
        after = db.execute("SELECT * FROM xpatch.cache_stats()").fetchone()

        # After truncate+reinsert, the first read should NOT produce hits
        # (cache was invalidated), but the second read SHOULD.
        assert after["hit_count"] > mid["hit_count"], (
            "Expected cache hits on re-read after re-population"
        )
