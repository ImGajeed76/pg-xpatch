"""
Tests for stats batching: group_stats updates should be O(groups), not O(rows).

Currently, every xpatch_tuple_insert triggers xpatch_stats_cache_update_group(),
which does SPI_connect + INSERT...ON CONFLICT DO UPDATE + SPI_finish PER ROW.

With zero batching, a 200-row COPY performs 200 SPI round-trips (1 INSERT
+ 199 UPDATEs to group_stats).  This is measurable via pg_stat_all_tables:
  - n_tup_ins on xpatch.group_stats = number of new groups seen
  - n_tup_upd on xpatch.group_stats = total rows - new groups

Correct behavior: during COPY / bulk insert, stats are accumulated in memory
and flushed once at the end — O(groups) operations, not O(rows).

These tests assert the correct (batched) behavior.  They FAIL on the current
code (which does per-row SPI) and PASS after the fix.

We use pg_stat_all_tables to count the exact number of DML operations on
xpatch.group_stats, then assert they are proportional to GROUPS, not ROWS.
"""

import time
import psycopg
import pytest
from psycopg import sql


def _get_group_stats_ops(conn: psycopg.Connection) -> dict:
    """
    Get cumulative insert/update counts for xpatch.group_stats table.

    Returns {"n_tup_ins": int, "n_tup_upd": int}.
    """
    conn.execute("SELECT pg_stat_force_next_flush()")
    row = conn.execute(
        """
        SELECT n_tup_ins, n_tup_upd
        FROM pg_stat_all_tables
        WHERE schemaname = 'xpatch' AND relname = 'group_stats'
        """
    ).fetchone()
    if row is None:
        return {"n_tup_ins": 0, "n_tup_upd": 0}
    return {"n_tup_ins": row["n_tup_ins"], "n_tup_upd": row["n_tup_upd"]}


def _wait_for_stats_update(conn: psycopg.Connection):
    """Wait for pg_stat_all_tables to reflect recent DML changes."""
    conn.execute("SELECT pg_stat_force_next_flush()")
    time.sleep(0.5)


class TestStatsBatchedCopy:
    """
    Assert that stats updates during COPY are BATCHED (O(groups) operations).

    COPY goes through multi_insert -> tuple_insert per row, then
    finish_bulk_insert at the end.  The fix accumulates stats in memory
    during the per-row inserts and flushes once in finish_bulk_insert.

    These tests FAIL on current code (per-row SPI) and PASS after the fix.
    """

    def test_copy_single_group_batched(
        self, db: psycopg.Connection, make_table
    ):
        """
        COPY 200 rows to a single group.

        CORRECT (batched): total SPI ops <= 10  (single UPSERT at end)
        BUGGY (per-row): total SPI ops == 200

        This is THE primary test case: COPY is the bulk-load path and
        the most impacted by per-row SPI overhead.
        """
        t = make_table(
            "group_id INT, version INT, content TEXT NOT NULL",
            group_by="group_id",
            order_by="version",
            delta_columns=["content"],
            compress_depth=5,
        )

        db.execute("SELECT pg_stat_reset()")
        _wait_for_stats_update(db)
        before = _get_group_stats_ops(db)

        n_rows = 200
        n_groups = 1
        with db.cursor() as cur:
            with cur.copy(
                sql.SQL("COPY {} (group_id, version, content) FROM STDIN").format(
                    sql.Identifier(t)
                )
            ) as copy:
                for v in range(1, n_rows + 1):
                    copy.write_row((1, v, f"COPY v{v} " + "c" * 80))

        _wait_for_stats_update(db)
        after = _get_group_stats_ops(db)

        inserts = after["n_tup_ins"] - before["n_tup_ins"]
        updates = after["n_tup_upd"] - before["n_tup_upd"]
        total_ops = inserts + updates

        max_expected = n_groups * 10
        assert total_ops <= max_expected, (
            f"Stats not batched: {total_ops} SPI operations on group_stats "
            f"for {n_rows}-row COPY (inserts={inserts}, updates={updates}). "
            f"Expected at most {max_expected} with batched stats. "
            f"Per-row SPI overhead confirmed: ~{total_ops / n_rows:.1f} ops/row."
        )

    def test_copy_multi_group_batched(
        self, db: psycopg.Connection, make_table
    ):
        """
        COPY 200 rows across 5 groups (interleaved).

        CORRECT (batched): total SPI ops <= 50  (up to 10 per group)
        BUGGY (per-row): total SPI ops == 200
        """
        t = make_table(
            "group_id INT, version INT, content TEXT NOT NULL",
            group_by="group_id",
            order_by="version",
            delta_columns=["content"],
            compress_depth=5,
        )

        db.execute("SELECT pg_stat_reset()")
        _wait_for_stats_update(db)
        before = _get_group_stats_ops(db)

        n_groups = 5
        versions_per_group = 40
        total_rows = n_groups * versions_per_group

        with db.cursor() as cur:
            with cur.copy(
                sql.SQL("COPY {} (group_id, version, content) FROM STDIN").format(
                    sql.Identifier(t)
                )
            ) as copy:
                for g in range(1, n_groups + 1):
                    for v in range(1, versions_per_group + 1):
                        copy.write_row((g, v, f"g{g} v{v} " + "d" * 50))

        _wait_for_stats_update(db)
        after = _get_group_stats_ops(db)

        inserts = after["n_tup_ins"] - before["n_tup_ins"]
        updates = after["n_tup_upd"] - before["n_tup_upd"]
        total_ops = inserts + updates

        max_expected = n_groups * 10
        assert total_ops <= max_expected, (
            f"Stats not batched: {total_ops} SPI operations on group_stats "
            f"for {total_rows}-row COPY across {n_groups} groups "
            f"(inserts={inserts}, updates={updates}). "
            f"Expected at most {max_expected} with batched stats. "
            f"Per-row SPI overhead confirmed: ~{total_ops / total_rows:.1f} ops/row."
        )

    def test_transaction_batch_insert_batched(
        self, db: psycopg.Connection, make_table
    ):
        """
        100 INSERTs inside a single explicit transaction.

        CORRECT (batched): total SPI ops <= 10  (flush at commit)
        BUGGY (per-row): total SPI ops == 100
        """
        t = make_table(
            "group_id INT, version INT, content TEXT NOT NULL",
            group_by="group_id",
            order_by="version",
            delta_columns=["content"],
            compress_depth=5,
        )

        db.execute("SELECT pg_stat_reset()")
        _wait_for_stats_update(db)
        before = _get_group_stats_ops(db)

        n_rows = 100
        n_groups = 1
        with db.transaction():
            for v in range(1, n_rows + 1):
                db.execute(
                    sql.SQL(
                        "INSERT INTO {} (group_id, version, content) VALUES (%s, %s, %s)"
                    ).format(sql.Identifier(t)),
                    (1, v, f"Version {v} content " + "x" * 80),
                )

        _wait_for_stats_update(db)
        after = _get_group_stats_ops(db)

        inserts = after["n_tup_ins"] - before["n_tup_ins"]
        updates = after["n_tup_upd"] - before["n_tup_upd"]
        total_ops = inserts + updates

        max_expected = n_groups * 10
        assert total_ops <= max_expected, (
            f"Stats not batched: {total_ops} SPI operations on group_stats "
            f"for {n_rows} INSERTs in a single transaction "
            f"(inserts={inserts}, updates={updates}). "
            f"Expected at most {max_expected} with batched stats. "
            f"Per-row SPI overhead confirmed: ~{total_ops / n_rows:.1f} ops/row."
        )

        # Data correctness check
        count = db.execute(
            sql.SQL("SELECT COUNT(*) AS cnt FROM {}").format(sql.Identifier(t))
        ).fetchone()["cnt"]
        assert count == n_rows


class TestStatsCorrectness:
    """
    Stats correctness tests — these PASS both before and after the fix.
    The stats values must be accurate regardless of batching strategy.
    """

    def test_stats_correct_after_large_copy(
        self, db: psycopg.Connection, make_table
    ):
        """Stats total_rows matches actual row count after large COPY."""
        t = make_table(
            "group_id INT, version INT, content TEXT NOT NULL",
            group_by="group_id",
            order_by="version",
            delta_columns=["content"],
            compress_depth=5,
            keyframe_every=100,
        )

        n_rows = 200
        with db.cursor() as cur:
            with cur.copy(
                sql.SQL("COPY {} (group_id, version, content) FROM STDIN").format(
                    sql.Identifier(t)
                )
            ) as copy:
                for v in range(1, n_rows + 1):
                    copy.write_row((1, v, f"versioned content {v}"))

        stats = db.execute(
            sql.SQL("SELECT * FROM xpatch.stats({})").format(sql.Literal(t))
        ).fetchone()

        assert stats["total_rows"] == n_rows, (
            f"Stats total_rows={stats['total_rows']}, expected {n_rows}"
        )

    def test_stats_correct_multi_group(
        self, db: psycopg.Connection, make_table
    ):
        """Stats total_groups and total_rows correct after multi-group insert."""
        t = make_table(
            "group_id INT, version INT, content TEXT NOT NULL",
            group_by="group_id",
            order_by="version",
            delta_columns=["content"],
            compress_depth=5,
        )

        n_groups = 10
        versions_per_group = 20

        for g in range(1, n_groups + 1):
            for v in range(1, versions_per_group + 1):
                db.execute(
                    sql.SQL(
                        "INSERT INTO {} (group_id, version, content) VALUES (%s, %s, %s)"
                    ).format(sql.Identifier(t)),
                    (g, v, f"g{g} v{v} data"),
                )

        stats = db.execute(
            sql.SQL("SELECT * FROM xpatch.stats({})").format(sql.Literal(t))
        ).fetchone()

        assert stats["total_rows"] == n_groups * versions_per_group
        assert stats["total_groups"] == n_groups
