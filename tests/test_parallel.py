"""
Test parallel sequential scan on xpatch tables.

Covers:
- Parallel scan produces correct results
- Aggregation under parallel scan
- Filter on delta column under parallel scan
- Multiple workers produce same results as serial
- Parallel scan with GROUP BY
- Edge cases: empty table, LIMIT, single-group reconstruction, concurrent scans
"""

from __future__ import annotations

import psycopg
import pytest

from conftest import insert_rows, insert_versions, row_count


def _enable_parallel(db: psycopg.Connection) -> None:
    """Force parallel scan by lowering costs and thresholds."""
    db.execute("SET max_parallel_workers_per_gather = 2")
    db.execute("SET parallel_tuple_cost = 0")
    db.execute("SET parallel_setup_cost = 0")
    db.execute("SET min_parallel_table_scan_size = 0")
    db.execute("SET min_parallel_index_scan_size = 0")


def _disable_parallel(db: psycopg.Connection) -> None:
    """Reset to default (no forced parallel)."""
    db.execute("RESET max_parallel_workers_per_gather")
    db.execute("RESET parallel_tuple_cost")
    db.execute("RESET parallel_setup_cost")
    db.execute("RESET min_parallel_table_scan_size")
    db.execute("RESET min_parallel_index_scan_size")


def _assert_parallel_plan(db: psycopg.Connection, query: str) -> None:
    """Assert that the given query uses a parallel plan."""
    plan = db.execute(
        f"EXPLAIN (COSTS OFF) {query}"
    ).fetchall()
    plan_text = "\n".join(r["QUERY PLAN"] for r in plan)
    assert "Parallel" in plan_text, (
        f"Expected parallel plan but got:\n{plan_text}"
    )


class TestParallelScan:
    """Parallel sequential scan correctness."""

    def _setup_bulk_data(self, db, make_table):
        """Create a table with enough data to trigger parallel scan."""
        t = make_table()
        # 50 groups x 10 versions = 500 rows
        for g in range(1, 51):
            insert_versions(db, t, group_id=g, count=10)
        db.execute(f"ANALYZE {t}")
        return t

    def test_parallel_plan_used(self, db: psycopg.Connection, make_table):
        """EXPLAIN shows parallel workers when settings force it."""
        t = self._setup_bulk_data(db, make_table)
        _enable_parallel(db)
        try:
            plan = db.execute(
                f"EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM {t}"
            ).fetchall()
            plan_text = "\n".join(r["QUERY PLAN"] for r in plan)
            assert "Parallel" in plan_text, (
                f"Expected parallel plan with forced-zero costs, got:\n{plan_text}"
            )
        finally:
            _disable_parallel(db)

    def test_parallel_count_correct(self, db: psycopg.Connection, make_table):
        """COUNT(*) under parallel scan returns correct result."""
        t = self._setup_bulk_data(db, make_table)
        _enable_parallel(db)
        try:
            cnt = row_count(db, t)
            assert cnt == 500
        finally:
            _disable_parallel(db)

    def test_parallel_vs_serial_same_results(self, db: psycopg.Connection, make_table):
        """Parallel and serial scans produce identical results."""
        t = self._setup_bulk_data(db, make_table)

        # Serial
        _disable_parallel(db)
        db.execute("SET max_parallel_workers_per_gather = 0")
        serial = db.execute(
            f"SELECT group_id, version, content FROM {t} ORDER BY group_id, version"
        ).fetchall()
        db.execute("RESET max_parallel_workers_per_gather")

        # Parallel
        _enable_parallel(db)
        try:
            parallel = db.execute(
                f"SELECT group_id, version, content FROM {t} ORDER BY group_id, version"
            ).fetchall()
        finally:
            _disable_parallel(db)

        assert len(serial) == len(parallel)
        for s, p in zip(serial, parallel):
            assert s["group_id"] == p["group_id"]
            assert s["version"] == p["version"]
            assert s["content"] == p["content"]

    def test_parallel_aggregate(self, db: psycopg.Connection, make_table):
        """Aggregation works correctly under parallel scan."""
        t = self._setup_bulk_data(db, make_table)
        _enable_parallel(db)
        try:
            row = db.execute(
                f"SELECT COUNT(*) as cnt, "
                f"  COUNT(DISTINCT group_id) as groups, "
                f"  MIN(version) as min_v, "
                f"  MAX(version) as max_v "
                f"FROM {t}"
            ).fetchone()
            assert row["cnt"] == 500
            assert row["groups"] == 50
            assert row["min_v"] == 1
            assert row["max_v"] == 10
        finally:
            _disable_parallel(db)

    def test_parallel_filter_on_delta_column(self, db: psycopg.Connection, make_table):
        """WHERE on delta-compressed column works under parallel scan."""
        t = self._setup_bulk_data(db, make_table)
        _enable_parallel(db)
        try:
            rows = db.execute(
                f"SELECT group_id, version FROM {t} "
                f"WHERE content = 'Version 5 content' ORDER BY group_id"
            ).fetchall()
            # Every group has a version 5
            assert len(rows) == 50
            assert all(r["version"] == 5 for r in rows)
        finally:
            _disable_parallel(db)

    def test_parallel_group_by(self, db: psycopg.Connection, make_table):
        """GROUP BY under parallel scan returns correct per-group counts."""
        t = self._setup_bulk_data(db, make_table)
        _enable_parallel(db)
        try:
            rows = db.execute(
                f"SELECT group_id, COUNT(*) as cnt FROM {t} "
                f"GROUP BY group_id ORDER BY group_id"
            ).fetchall()
            assert len(rows) == 50
            assert all(r["cnt"] == 10 for r in rows)
        finally:
            _disable_parallel(db)


class TestParallelEdgeCases:
    """Edge cases for parallel scan correctness."""

    def test_parallel_empty_table(self, db: psycopg.Connection, make_table):
        """Parallel scan on empty table returns 0 rows without error."""
        t = make_table()
        db.execute(f"ANALYZE {t}")
        _enable_parallel(db)
        try:
            cnt = row_count(db, t)
            assert cnt == 0
            rows = db.execute(f"SELECT * FROM {t}").fetchall()
            assert rows == []
        finally:
            _disable_parallel(db)

    def test_parallel_with_limit(self, db: psycopg.Connection, make_table):
        """LIMIT under parallel scan returns correct number of rows."""
        t = make_table()
        for g in range(1, 51):
            insert_versions(db, t, group_id=g, count=10)
        db.execute(f"ANALYZE {t}")

        _enable_parallel(db)
        try:
            rows = db.execute(
                f"SELECT group_id, version, content FROM {t} "
                f"ORDER BY group_id, version LIMIT 10"
            ).fetchall()
            assert len(rows) == 10
            # Verify ordering is correct
            for i in range(1, len(rows)):
                prev, curr = rows[i - 1], rows[i]
                assert (prev["group_id"], prev["version"]) <= (
                    curr["group_id"],
                    curr["version"],
                )
        finally:
            _disable_parallel(db)

    def test_parallel_single_group_many_versions(
        self, db: psycopg.Connection, make_table
    ):
        """Parallel scan with a single group exercises shared reconstruction chain."""
        t = make_table()
        # 200 versions in one group — crosses multiple keyframe boundaries
        insert_versions(db, t, group_id=1, count=200)
        db.execute(f"ANALYZE {t}")

        # Serial baseline
        _disable_parallel(db)
        db.execute("SET max_parallel_workers_per_gather = 0")
        serial = db.execute(
            f"SELECT group_id, version, content FROM {t} ORDER BY version"
        ).fetchall()
        db.execute("RESET max_parallel_workers_per_gather")

        # Parallel
        _enable_parallel(db)
        try:
            parallel = db.execute(
                f"SELECT group_id, version, content FROM {t} ORDER BY version"
            ).fetchall()
        finally:
            _disable_parallel(db)

        assert len(serial) == len(parallel) == 200
        for s, p in zip(serial, parallel):
            assert s["version"] == p["version"]
            assert s["content"] == p["content"]

    def test_parallel_filter_on_group_column(
        self, db: psycopg.Connection, make_table
    ):
        """WHERE on group_id (non-delta) column under parallel scan."""
        t = make_table()
        for g in range(1, 51):
            insert_versions(db, t, group_id=g, count=10)
        db.execute(f"ANALYZE {t}")

        _enable_parallel(db)
        try:
            rows = db.execute(
                f"SELECT group_id, version, content FROM {t} "
                f"WHERE group_id = 25 ORDER BY version"
            ).fetchall()
            assert len(rows) == 10
            assert all(r["group_id"] == 25 for r in rows)
            assert [r["version"] for r in rows] == list(range(1, 11))
        finally:
            _disable_parallel(db)

    def test_parallel_xp_seq_correctness(self, db: psycopg.Connection, make_table):
        """_xp_seq values are correct under parallel scan."""
        t = make_table()
        for g in range(1, 21):
            insert_versions(db, t, group_id=g, count=10)
        db.execute(f"ANALYZE {t}")

        # Serial baseline
        _disable_parallel(db)
        db.execute("SET max_parallel_workers_per_gather = 0")
        serial = db.execute(
            f"SELECT group_id, version, _xp_seq FROM {t} ORDER BY group_id, version"
        ).fetchall()
        db.execute("RESET max_parallel_workers_per_gather")

        # Parallel
        _enable_parallel(db)
        try:
            parallel = db.execute(
                f"SELECT group_id, version, _xp_seq FROM {t} ORDER BY group_id, version"
            ).fetchall()
        finally:
            _disable_parallel(db)

        assert len(serial) == len(parallel) == 200
        for s, p in zip(serial, parallel):
            assert s["_xp_seq"] == p["_xp_seq"], (
                f"_xp_seq mismatch for group={s['group_id']} version={s['version']}: "
                f"serial={s['_xp_seq']} parallel={p['_xp_seq']}"
            )

    def test_concurrent_parallel_scans(
        self, db: psycopg.Connection, make_table, db_factory
    ):
        """Two connections running parallel scans simultaneously."""
        t = make_table()
        for g in range(1, 51):
            insert_versions(db, t, group_id=g, count=10)
        db.execute(f"ANALYZE {t}")

        db2 = db_factory()

        for conn in (db, db2):
            _enable_parallel(conn)

        try:
            # Run parallel count on both connections
            cnt1 = row_count(db, t)
            cnt2 = row_count(db2, t)
            assert cnt1 == 500
            assert cnt2 == 500

            # Run full selects on both connections
            rows1 = db.execute(
                f"SELECT group_id, version, content FROM {t} ORDER BY group_id, version"
            ).fetchall()
            rows2 = db2.execute(
                f"SELECT group_id, version, content FROM {t} ORDER BY group_id, version"
            ).fetchall()
            assert len(rows1) == len(rows2) == 500
            for r1, r2 in zip(rows1, rows2):
                assert r1["group_id"] == r2["group_id"]
                assert r1["version"] == r2["version"]
                assert r1["content"] == r2["content"]
        finally:
            for conn in (db, db2):
                _disable_parallel(conn)
