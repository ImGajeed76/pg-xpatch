"""
Test index support on xpatch tables.

Covers:
- Auto-created _xp_seq index
- Composite (group_by, _xp_seq) index from configure()
- Manual index on non-delta column
- Manual index on delta column (indexes reconstructed values)
- Index scan plan used when seqscan disabled
- Bitmap index scan
- ANALYZE populates pg_stats
- Index survives TRUNCATE
- Index on multiple columns (composite)
"""

from __future__ import annotations

import psycopg
import pytest

from conftest import insert_rows, insert_versions, row_count


class TestAutoCreatedIndexes:
    """Indexes automatically created by event triggers and configure()."""

    def test_xp_seq_index_exists(self, db: psycopg.Connection, make_table):
        """_xp_seq btree index is auto-created on CREATE TABLE USING xpatch."""
        t = make_table()
        indexes = db.execute(
            "SELECT indexname, indexdef FROM pg_indexes WHERE tablename = %s",
            [t],
        ).fetchall()
        # After configure(), the simple _xp_seq index becomes a composite
        # (group_by, _xp_seq) index. Check the indexdef for _xp_seq.
        seq_indexes = [r for r in indexes if "_xp_seq" in r["indexdef"]]
        assert len(seq_indexes) >= 1, (
            f"No index covering _xp_seq. Indexes: "
            f"{[(r['indexname'], r['indexdef']) for r in indexes]}"
        )
        assert "btree" in seq_indexes[0]["indexdef"].lower()

    def test_composite_group_seq_index(self, db: psycopg.Connection, make_table):
        """configure() creates a composite (group_by, _xp_seq) btree index."""
        t = make_table()
        indexes = db.execute(
            "SELECT indexdef FROM pg_indexes WHERE tablename = %s",
            [t],
        ).fetchall()
        defs = [r["indexdef"] for r in indexes]
        has_composite = any(
            "group_id" in d and "_xp_seq" in d for d in defs
        )
        assert has_composite, f"No composite index. Indexes: {defs}"


class TestManualIndexes:
    """Manually created indexes on xpatch tables."""

    def test_index_on_non_delta_column(self, db: psycopg.Connection, make_table):
        """B-tree index on a non-delta column (group_id) works."""
        t = make_table()
        db.execute(f"CREATE INDEX ON {t} (group_id)")
        insert_versions(db, t, group_id=1, count=10)
        insert_versions(db, t, group_id=2, count=10)

        # Should be able to use the index
        db.execute("SET enable_seqscan = off")
        try:
            rows = db.execute(
                f"SELECT version FROM {t} WHERE group_id = 2 ORDER BY version"
            ).fetchall()
            assert len(rows) == 10
        finally:
            db.execute("SET enable_seqscan = on")

    def test_index_on_delta_column(self, db: psycopg.Connection, make_table):
        """B-tree index on a delta-compressed column (content) works.
        The index stores reconstructed values."""
        t = make_table()
        insert_rows(db, t, [
            (1, 1, "alpha"),
            (1, 2, "beta"),
            (1, 3, "gamma"),
        ])
        db.execute(f"CREATE INDEX ON {t} (content)")
        db.execute(f"ANALYZE {t}")

        db.execute("SET enable_seqscan = off")
        try:
            rows = db.execute(
                f"SELECT content FROM {t} WHERE content = 'beta'"
            ).fetchall()
            assert len(rows) == 1
            assert rows[0]["content"] == "beta"
        finally:
            db.execute("SET enable_seqscan = on")

    def test_composite_manual_index(self, db: psycopg.Connection, make_table):
        """Composite index on (group_id, version) works."""
        t = make_table()
        db.execute(f"CREATE INDEX ON {t} (group_id, version)")
        for g in range(1, 4):
            insert_versions(db, t, group_id=g, count=10)

        db.execute(f"ANALYZE {t}")
        db.execute("SET enable_seqscan = off")
        try:
            rows = db.execute(
                f"SELECT version FROM {t} WHERE group_id = 2 AND version >= 5 "
                f"ORDER BY version"
            ).fetchall()
            assert [r["version"] for r in rows] == [5, 6, 7, 8, 9, 10]
        finally:
            db.execute("SET enable_seqscan = on")


class TestIndexScanPlans:
    """Query planner uses indexes when appropriate."""

    def test_index_scan_plan(self, db: psycopg.Connection, make_table):
        """EXPLAIN shows index scan when seqscan is disabled."""
        t = make_table()
        for g in range(1, 6):
            insert_versions(db, t, group_id=g, count=20)
        db.execute(f"ANALYZE {t}")

        db.execute("SET enable_seqscan = off")
        try:
            plan = db.execute(
                f"EXPLAIN (COSTS OFF) SELECT * FROM {t} WHERE group_id = 3"
            ).fetchall()
            plan_text = "\n".join(r[list(r.keys())[0]] for r in plan)
            assert "Index" in plan_text or "Bitmap" in plan_text, (
                f"Expected index scan in plan:\n{plan_text}"
            )
        finally:
            db.execute("SET enable_seqscan = on")

    def test_index_scan_returns_correct_data(self, db: psycopg.Connection, make_table):
        """Index scan returns the same data as sequential scan."""
        t = make_table()
        for g in range(1, 4):
            for v in range(1, 6):
                insert_rows(db, t, [(g, v, f"g{g}v{v}")])
        db.execute(f"ANALYZE {t}")

        # Sequential scan
        db.execute("SET enable_seqscan = on")
        db.execute("SET enable_indexscan = off")
        db.execute("SET enable_bitmapscan = off")
        seq_rows = db.execute(
            f"SELECT group_id, version, content FROM {t} "
            f"WHERE group_id = 2 ORDER BY version"
        ).fetchall()

        # Index scan
        db.execute("SET enable_seqscan = off")
        db.execute("SET enable_indexscan = on")
        db.execute("SET enable_bitmapscan = on")
        idx_rows = db.execute(
            f"SELECT group_id, version, content FROM {t} "
            f"WHERE group_id = 2 ORDER BY version"
        ).fetchall()

        db.execute("SET enable_seqscan = on")

        assert len(seq_rows) == len(idx_rows)
        for s, i in zip(seq_rows, idx_rows):
            assert s["group_id"] == i["group_id"]
            assert s["version"] == i["version"]
            assert s["content"] == i["content"]


class TestAnalyze:
    """ANALYZE populates statistics."""

    def test_analyze_updates_pg_class(self, db: psycopg.Connection, make_table):
        """ANALYZE updates reltuples and relpages in pg_class."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=50)
        db.execute(f"ANALYZE {t}")

        row = db.execute(
            "SELECT reltuples, relpages FROM pg_class WHERE relname = %s",
            [t],
        ).fetchone()
        assert row["reltuples"] > 0
        assert row["relpages"] > 0

    def test_analyze_populates_pg_stats(self, db: psycopg.Connection, make_table):
        """ANALYZE populates pg_stats for indexed columns."""
        t = make_table()
        for g in range(1, 11):
            insert_versions(db, t, group_id=g, count=5)
        db.execute(f"ANALYZE {t}")

        stats = db.execute(
            "SELECT attname FROM pg_stats WHERE tablename = %s",
            [t],
        ).fetchall()
        att_names = {r["attname"] for r in stats}
        # At minimum group_id and version should have stats
        assert "group_id" in att_names or "version" in att_names


class TestIndexSurvivalAfterDDL:
    """Indexes survive DDL operations."""

    def test_index_survives_truncate(self, db: psycopg.Connection, make_table):
        """Indexes remain after TRUNCATE."""
        t = make_table()
        db.execute(f"CREATE INDEX idx_test_content ON {t} (content)")
        insert_versions(db, t, group_id=1, count=5)

        # Count indexes before
        before = db.execute(
            "SELECT COUNT(*) AS cnt FROM pg_indexes WHERE tablename = %s",
            [t],
        ).fetchone()["cnt"]

        db.execute(f"TRUNCATE {t}")

        after = db.execute(
            "SELECT COUNT(*) AS cnt FROM pg_indexes WHERE tablename = %s",
            [t],
        ).fetchone()["cnt"]

        assert after == before
        # Also verify table is empty
        assert row_count(db, t) == 0

    def test_index_usable_after_truncate_and_reinsert(self, db: psycopg.Connection, make_table):
        """Index works after TRUNCATE + reinsertion."""
        t = make_table()
        db.execute(f"CREATE INDEX ON {t} (group_id)")
        insert_versions(db, t, group_id=1, count=10)
        db.execute(f"TRUNCATE {t}")
        insert_versions(db, t, group_id=1, count=10)
        db.execute(f"ANALYZE {t}")

        db.execute("SET enable_seqscan = off")
        try:
            rows = db.execute(
                f"SELECT version FROM {t} WHERE group_id = 1 ORDER BY version"
            ).fetchall()
            assert len(rows) == 10
        finally:
            db.execute("SET enable_seqscan = on")
