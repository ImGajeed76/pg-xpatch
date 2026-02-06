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
- Deleted row invisible via index scan (MVCC regression)
- REINDEX CONCURRENTLY (regression test)
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
            plan_text = "\n".join(r["QUERY PLAN"] for r in plan)
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
        # ANALYZE should produce stats for user columns
        assert "group_id" in att_names, f"group_id not in pg_stats. Found: {att_names}"
        assert "version" in att_names, f"version not in pg_stats. Found: {att_names}"


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


class TestIndexEdgeCases:
    """Edge cases for index support on xpatch tables."""

    def test_index_on_xp_seq_column(self, db: psycopg.Connection, make_table):
        """Manual index on _xp_seq works for point queries."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=10)
        # The auto-created composite index already covers _xp_seq,
        # but test a direct single-column index too
        db.execute(f"CREATE INDEX ON {t} (_xp_seq)")
        db.execute(f"ANALYZE {t}")

        db.execute("SET enable_seqscan = off")
        try:
            row = db.execute(
                f"SELECT version FROM {t} WHERE _xp_seq = 5"
            ).fetchone()
            assert row is not None
            assert row["version"] == 5
        finally:
            db.execute("SET enable_seqscan = on")

    def test_index_drop_and_recreate(self, db: psycopg.Connection, make_table):
        """DROP INDEX + CREATE INDEX — index rebuild works on delta data."""
        t = make_table()
        for v in range(1, 11):
            insert_rows(db, t, [(1, v, f"content-{v}")])
        db.execute(f"CREATE INDEX idx_rebuild_test ON {t} (content)")
        db.execute(f"ANALYZE {t}")

        # Verify index works
        db.execute("SET enable_seqscan = off")
        try:
            row = db.execute(
                f"SELECT version FROM {t} WHERE content = 'content-5'"
            ).fetchone()
            assert row is not None
            assert row["version"] == 5
        finally:
            db.execute("SET enable_seqscan = on")

        # Drop and recreate
        db.execute("DROP INDEX idx_rebuild_test")
        db.execute(f"CREATE INDEX idx_rebuild_test ON {t} (content)")
        db.execute(f"ANALYZE {t}")

        # Verify rebuilt index still works
        db.execute("SET enable_seqscan = off")
        try:
            row = db.execute(
                f"SELECT version FROM {t} WHERE content = 'content-7'"
            ).fetchone()
            assert row is not None
            assert row["version"] == 7
        finally:
            db.execute("SET enable_seqscan = on")

    def test_index_survives_vacuum(self, db: psycopg.Connection, make_table):
        """Index remains usable after VACUUM on table with deleted rows."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=20)
        db.execute(f"CREATE INDEX ON {t} (version)")

        # Delete some rows
        db.execute(f"DELETE FROM {t} WHERE group_id = 1 AND version = 15")
        db.execute(f"VACUUM {t}")
        db.execute(f"ANALYZE {t}")

        db.execute("SET enable_seqscan = off")
        try:
            rows = db.execute(
                f"SELECT version FROM {t} ORDER BY version"
            ).fetchall()
            # v15-v20 deleted by cascade, v1-v14 remain
            assert len(rows) == 14
            assert [r["version"] for r in rows] == list(range(1, 15))
        finally:
            db.execute("SET enable_seqscan = on")

    def test_explain_analyze_with_index(self, db: psycopg.Connection, make_table):
        """EXPLAIN (ANALYZE) with index scan completes without error."""
        t = make_table()
        for g in range(1, 6):
            insert_versions(db, t, group_id=g, count=10)
        db.execute(f"ANALYZE {t}")

        db.execute("SET enable_seqscan = off")
        try:
            plan = db.execute(
                f"EXPLAIN (ANALYZE, COSTS OFF) "
                f"SELECT * FROM {t} WHERE group_id = 3"
            ).fetchall()
            plan_text = "\n".join(r["QUERY PLAN"] for r in plan)
            # Should show actual rows
            assert "actual" in plan_text.lower(), (
                f"EXPLAIN ANALYZE should show actual rows:\n{plan_text}"
            )
        finally:
            db.execute("SET enable_seqscan = on")

    def test_index_with_many_rows(self, db: psycopg.Connection, make_table):
        """Index correctness with enough data to span multiple heap blocks."""
        t = make_table()
        for g in range(1, 11):
            insert_versions(db, t, group_id=g, count=100)
        db.execute(f"ANALYZE {t}")

        db.execute("SET enable_seqscan = off")
        try:
            rows = db.execute(
                f"SELECT version FROM {t} WHERE group_id = 7 ORDER BY version"
            ).fetchall()
            assert len(rows) == 100
            assert [r["version"] for r in rows] == list(range(1, 101))
        finally:
            db.execute("SET enable_seqscan = on")

    def test_analyze_on_delta_columns(self, db: psycopg.Connection, make_table):
        """ANALYZE produces meaningful stats for delta-compressed columns."""
        t = make_table()
        for g in range(1, 21):
            for v in range(1, 6):
                insert_rows(db, t, [(g, v, f"group{g}-version{v}")])
        db.execute(f"ANALYZE {t}")

        stats = db.execute(
            "SELECT attname, n_distinct, null_frac "
            "FROM pg_stats WHERE tablename = %s",
            [t],
        ).fetchall()
        att_map = {r["attname"]: r for r in stats}
        # content should have stats with meaningful n_distinct
        assert "content" in att_map, (
            f"content not in pg_stats. Found: {list(att_map.keys())}"
        )
        # n_distinct should be > 0 (there are 100 distinct content values)
        assert att_map["content"]["n_distinct"] != 0

    def test_bitmap_scan_explicit(self, db: psycopg.Connection, make_table):
        """Bitmap scan returns correct data."""
        t = make_table()
        for g in range(1, 11):
            insert_versions(db, t, group_id=g, count=10)
        db.execute(f"ANALYZE {t}")

        # Force bitmap scan by disabling both seqscan and indexscan
        db.execute("SET enable_seqscan = off")
        db.execute("SET enable_indexscan = off")
        db.execute("SET enable_bitmapscan = on")
        try:
            rows = db.execute(
                f"SELECT version, content FROM {t} WHERE group_id = 5 ORDER BY version"
            ).fetchall()
            assert len(rows) == 10
            for row in rows:
                assert row["content"] == f"Version {row['version']} content"
        finally:
            db.execute("SET enable_seqscan = on")
            db.execute("SET enable_indexscan = on")


# ---------------------------------------------------------------------------
# Deleted row visibility via index scan — regression test
# ---------------------------------------------------------------------------


class TestDeletedRowVisibilityViaIndex:
    """Despite the TODO in fetch_row_version about missing MVCC checks,
    deleted-and-committed rows ARE correctly invisible via index scan.

    This works because xpatch's DELETE sets XMAX on the physical tuple,
    and the index scan path checks ItemIdIsNormal + XMAX status.
    """

    def test_deleted_row_invisible_via_index_scan(
        self, db: psycopg.Connection, make_table
    ):
        """After DELETE + COMMIT, index scan should not return deleted rows."""
        t = make_table()
        db.execute(f"CREATE UNIQUE INDEX {t}_uk ON {t} (group_id, version)")

        insert_versions(db, t, group_id=1, count=3)

        # Delete version 2 (cascades to version 3)
        db.execute(f"DELETE FROM {t} WHERE group_id = 1 AND version = 2")
        assert row_count(db, t) == 1

        # Force index scan
        db.execute("SET enable_seqscan = off")
        try:
            rows = db.execute(
                f"SELECT version, content FROM {t} "
                f"WHERE group_id = 1 AND version = 2"
            ).fetchall()
            assert len(rows) == 0, (
                f"Deleted row (version=2) visible via index scan: {rows}"
            )
        finally:
            db.execute("SET enable_seqscan = on")


# ---------------------------------------------------------------------------
# REINDEX CONCURRENTLY — regression test
# ---------------------------------------------------------------------------


class TestReindexConcurrently:
    """Despite ``xpatch_index_validate_scan`` being a stub, REINDEX
    CONCURRENTLY produces valid indexes because PostgreSQL rebuilds the
    index from scratch using the table scan path (which works correctly).
    """

    def test_reindex_concurrently_produces_valid_index(
        self, db: psycopg.Connection, make_table
    ):
        """After REINDEX CONCURRENTLY, index scans return correct results."""
        t = make_table()
        db.execute(f"CREATE INDEX {t}_ver_idx ON {t} (version)")

        insert_versions(db, t, group_id=1, count=10)
        insert_versions(db, t, group_id=2, count=10)

        db.execute(f"REINDEX INDEX CONCURRENTLY {t}_ver_idx")

        db.execute("SET enable_seqscan = off")
        try:
            rows = db.execute(
                f"SELECT group_id, version FROM {t} "
                f"WHERE version = 5 ORDER BY group_id"
            ).fetchall()
            assert len(rows) == 2
            assert rows[0]["group_id"] == 1
            assert rows[1]["group_id"] == 2
        finally:
            db.execute("SET enable_seqscan = on")
