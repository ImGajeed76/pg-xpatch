"""
Test VACUUM, ANALYZE, and TRUNCATE on xpatch tables.

Covers:
- VACUUM runs without error
- VACUUM after DELETE reclaims dead tuples
- VACUUM ANALYZE combined
- VACUUM FULL expected error (not supported by xpatch TAM)
- ANALYZE updates pg_class stats (reltuples, relpages)
- ANALYZE on empty table
- Data integrity preserved after VACUUM
- TRUNCATE clears all data
- TRUNCATE followed by reinsertion
- TRUNCATE rollback restores data
- _xp_seq resets after TRUNCATE
- VACUUM WAL crash recovery (C2 — known bug)
- ANALYZE concurrent race (H2 — known bug)
"""

from __future__ import annotations

import threading
import time

import psycopg
import pytest

from conftest import insert_rows, insert_versions, row_count


class TestVacuum:
    """VACUUM on xpatch tables."""

    def test_vacuum_runs_without_error(self, db: psycopg.Connection, make_table):
        """VACUUM completes without error."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=10)
        db.execute(f"VACUUM {t}")

    def test_vacuum_on_empty_table(self, db: psycopg.Connection, make_table):
        """VACUUM on empty table works."""
        t = make_table()
        db.execute(f"VACUUM {t}")

    def test_vacuum_after_delete(self, db: psycopg.Connection, make_table):
        """VACUUM after DELETE runs successfully."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=20)
        db.execute(f"DELETE FROM {t} WHERE group_id = 1 AND version = 10")
        # Cascade: v10..v20 deleted
        db.execute(f"VACUUM {t}")
        assert row_count(db, t) == 9

    def test_data_intact_after_vacuum(self, db: psycopg.Connection, make_table):
        """Data is correct after VACUUM."""
        t = make_table()
        for v in range(1, 11):
            insert_rows(db, t, [(1, v, f"data-{v}")])

        db.execute(f"VACUUM {t}")

        rows = db.execute(
            f"SELECT version, content FROM {t} ORDER BY version"
        ).fetchall()
        assert len(rows) == 10
        for row in rows:
            assert row["content"] == f"data-{row['version']}"

    def test_data_intact_after_delete_and_vacuum(self, db: psycopg.Connection, make_table):
        """Remaining data is correct after DELETE + VACUUM."""
        t = make_table()
        for v in range(1, 11):
            insert_rows(db, t, [(1, v, f"data-{v}")])

        db.execute(f"DELETE FROM {t} WHERE group_id = 1 AND version = 6")
        db.execute(f"VACUUM {t}")

        rows = db.execute(
            f"SELECT version, content FROM {t} ORDER BY version"
        ).fetchall()
        assert len(rows) == 5
        for row in rows:
            assert row["content"] == f"data-{row['version']}"

    def test_vacuum_verbose(self, db: psycopg.Connection, make_table):
        """VACUUM (VERBOSE) runs without error."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=10)
        db.execute(f"VACUUM (VERBOSE) {t}")

    def test_insert_after_vacuum(self, db: psycopg.Connection, make_table):
        """INSERT works correctly after VACUUM."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=10)
        db.execute(f"DELETE FROM {t} WHERE group_id = 1 AND version = 5")
        db.execute(f"VACUUM {t}")

        # Insert new data
        insert_rows(db, t, [(1, 20, "after vacuum")])
        row = db.execute(
            f"SELECT content FROM {t} WHERE version = 20"
        ).fetchone()
        assert row["content"] == "after vacuum"


class TestAnalyze:
    """ANALYZE on xpatch tables."""

    def test_analyze_runs_without_error(self, db: psycopg.Connection, make_table):
        """ANALYZE completes without error."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=10)
        db.execute(f"ANALYZE {t}")

    def test_analyze_updates_reltuples(self, db: psycopg.Connection, make_table):
        """ANALYZE updates reltuples in pg_class."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=50)
        db.execute(f"ANALYZE {t}")

        row = db.execute(
            "SELECT reltuples FROM pg_class WHERE relname = %s",
            [t],
        ).fetchone()
        assert row["reltuples"] > 0

    def test_analyze_updates_relpages(self, db: psycopg.Connection, make_table):
        """ANALYZE updates relpages in pg_class."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=50)
        db.execute(f"ANALYZE {t}")

        row = db.execute(
            "SELECT relpages FROM pg_class WHERE relname = %s",
            [t],
        ).fetchone()
        assert row["relpages"] > 0

    def test_analyze_after_delete(self, db: psycopg.Connection, make_table):
        """ANALYZE after DELETE updates statistics correctly."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=50)
        db.execute(f"DELETE FROM {t} WHERE group_id = 1 AND version = 25")
        db.execute(f"ANALYZE {t}")

        row = db.execute(
            "SELECT reltuples FROM pg_class WHERE relname = %s",
            [t],
        ).fetchone()
        # Cascade delete removed v25-v50 (26 rows), leaving 24.
        # ANALYZE uses sampling so allow some tolerance.
        assert row["reltuples"] == pytest.approx(24, abs=5), (
            f"Expected reltuples ~24 after cascade delete, got {row['reltuples']}"
        )


class TestTruncate:
    """TRUNCATE on xpatch tables."""

    def test_truncate_removes_all_data(self, db: psycopg.Connection, make_table):
        """TRUNCATE removes all rows."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=20)
        insert_versions(db, t, group_id=2, count=10)
        assert row_count(db, t) == 30

        db.execute(f"TRUNCATE {t}")
        assert row_count(db, t) == 0

    def test_insert_after_truncate(self, db: psycopg.Connection, make_table):
        """INSERT works after TRUNCATE."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=10)
        db.execute(f"TRUNCATE {t}")

        insert_rows(db, t, [(1, 1, "after truncate")])
        assert row_count(db, t) == 1
        row = db.execute(f"SELECT content FROM {t}").fetchone()
        assert row["content"] == "after truncate"

    def test_xp_seq_resets_after_truncate(self, db: psycopg.Connection, make_table):
        """_xp_seq starts at 1 again after TRUNCATE."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=5)
        db.execute(f"TRUNCATE {t}")

        db.execute(
            f"INSERT INTO {t} (group_id, version, content) "
            f"VALUES (1, 1, 'fresh')"
        )
        row = db.execute(f"SELECT _xp_seq FROM {t}").fetchone()
        assert row["_xp_seq"] == 1

    def test_multiple_truncate_insert_cycles(self, db: psycopg.Connection, make_table):
        """Multiple TRUNCATE + INSERT cycles work correctly."""
        t = make_table()

        for cycle in range(3):
            insert_versions(db, t, group_id=1, count=5, start=cycle * 10 + 1)
            assert row_count(db, t) == 5
            db.execute(f"TRUNCATE {t}")
            assert row_count(db, t) == 0

        # Final insert
        insert_rows(db, t, [(1, 1, "final")])
        row = db.execute(f"SELECT content FROM {t}").fetchone()
        assert row["content"] == "final"

    def test_truncate_rollback_restores_data(self, db: psycopg.Connection, make_table):
        """ROLLBACK after TRUNCATE restores all data (TRUNCATE is transactional)."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=10)

        db.execute("BEGIN")
        try:
            db.execute(f"TRUNCATE {t}")
            assert row_count(db, t) == 0
        finally:
            db.execute("ROLLBACK")

        assert row_count(db, t) == 10


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestVacuumEdgeCases:
    """Edge cases for VACUUM, ANALYZE, and TRUNCATE."""

    def test_vacuum_full_not_supported(self, db: psycopg.Connection, make_table):
        """VACUUM (FULL) raises an error on xpatch tables.

        VACUUM FULL triggers relation_copy_for_cluster which xpatch does not
        support (ERRCODE_FEATURE_NOT_SUPPORTED).
        """
        t = make_table()
        insert_versions(db, t, group_id=1, count=5)
        with pytest.raises(psycopg.errors.FeatureNotSupported):
            db.execute(f"VACUUM (FULL) {t}")

    def test_vacuum_analyze_combined(self, db: psycopg.Connection, make_table):
        """VACUUM ANALYZE runs without error and updates stats."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=20)
        db.execute(f"VACUUM ANALYZE {t}")

        row = db.execute(
            "SELECT reltuples FROM pg_class WHERE relname = %s", [t]
        ).fetchone()
        assert row["reltuples"] > 0

    def test_vacuum_multi_group_partial_delete(self, db: psycopg.Connection, make_table):
        """VACUUM after deleting one group preserves the other."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=10)
        insert_versions(db, t, group_id=2, count=10)

        db.execute(f"DELETE FROM {t} WHERE group_id = 1 AND version = 1")
        db.execute(f"VACUUM {t}")

        assert row_count(db, t, "group_id = 1") == 0
        assert row_count(db, t, "group_id = 2") == 10
        # Verify group 2 data is intact
        rows = db.execute(
            f"SELECT version, content FROM {t} WHERE group_id = 2 ORDER BY version"
        ).fetchall()
        assert len(rows) == 10
        for row in rows:
            assert row["content"] == f"Version {row['version']} content"

    def test_vacuum_all_dead_tuples(self, db: psycopg.Connection, make_table):
        """VACUUM on table where all rows are deleted, then reinsert."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=10)
        db.execute(f"DELETE FROM {t} WHERE group_id = 1 AND version = 1")
        assert row_count(db, t) == 0

        db.execute(f"VACUUM {t}")
        assert row_count(db, t) == 0

        # Verify table is still usable after vacuuming all-dead state
        insert_rows(db, t, [(1, 1, "after full vacuum")])
        assert row_count(db, t) == 1
        row = db.execute(f"SELECT content FROM {t}").fetchone()
        assert row["content"] == "after full vacuum"

    def test_analyze_on_empty_table(self, db: psycopg.Connection, make_table):
        """ANALYZE on empty table works and sets reltuples to 0."""
        t = make_table()
        db.execute(f"ANALYZE {t}")

        row = db.execute(
            "SELECT reltuples FROM pg_class WHERE relname = %s", [t]
        ).fetchone()
        assert row["reltuples"] == 0

    def test_analyze_after_truncate(self, db: psycopg.Connection, make_table):
        """ANALYZE after TRUNCATE reflects empty table."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=20)
        db.execute(f"TRUNCATE {t}")
        db.execute(f"ANALYZE {t}")

        row = db.execute(
            "SELECT reltuples FROM pg_class WHERE relname = %s", [t]
        ).fetchone()
        assert row["reltuples"] == 0

    def test_vacuum_large_dataset(self, db: psycopg.Connection, make_table):
        """VACUUM with enough data to span multiple heap pages."""
        t = make_table()
        for g in range(1, 6):
            insert_versions(db, t, group_id=g, count=200)
        # Delete half from each group
        for g in range(1, 6):
            db.execute(f"DELETE FROM {t} WHERE group_id = {g} AND version = 101")

        db.execute(f"VACUUM {t}")

        for g in range(1, 6):
            assert row_count(db, t, f"group_id = {g}") == 100
        assert row_count(db, t) == 500


# ---------------------------------------------------------------------------
# C2 — VACUUM does not WAL-log tuple removals (crash = resurrection)
# ---------------------------------------------------------------------------


class TestVacuumWalCrashRecovery:
    """VACUUM calls ``ItemIdSetUnused()`` and ``PageRepairFragmentation()``
    but never enters a critical section and never writes WAL records.
    If the server crashes between the page modification and the next
    checkpoint, dead tuples reappear (tuple resurrection).

    Bug: xpatch_tam.c:1837-1977 (known bug C2)
    """

    @pytest.mark.crash_test
    @pytest.mark.xfail(
        strict=False,
        reason="C2: VACUUM does not WAL-log tuple removals — crash after "
               "VACUUM may resurrect dead tuples",
    )
    def test_vacuum_survives_crash(self, db: psycopg.Connection, make_table, pg_ctl):
        """After DELETE + VACUUM + crash, dead tuples should NOT reappear.

        If the VACUUM changes are not WAL-logged, crash recovery replays
        from the last checkpoint and the vacuum'd tuples come back.
        """
        from conftest import _connect

        t = make_table()
        insert_versions(db, t, group_id=1, count=20)

        # Delete versions 11-20 (cascade from version 11)
        db.execute(f"DELETE FROM {t} WHERE group_id = 1 AND version = 11")
        assert row_count(db, t) == 10

        # VACUUM to physically remove the dead tuples
        db.execute(f"VACUUM {t}")

        # Force a checkpoint so the vacuum'd page is on disk
        db.execute("CHECKPOINT")

        # Record the database name for reconnection
        dbname = db.info.dbname

        # Now crash the server
        db.close()
        pg_ctl.kill()
        time.sleep(1)
        pg_ctl.start()

        # Reconnect and check
        conn = _connect(dbname)
        try:
            cnt = row_count(conn, t)
            assert cnt == 10, (
                f"Expected 10 rows after crash recovery, got {cnt} — "
                f"dead tuples resurrected (C2: VACUUM not WAL-logged)"
            )
            # Verify the surviving rows are correct
            rows = conn.execute(
                f"SELECT version FROM {t} ORDER BY version"
            ).fetchall()
            versions = [r["version"] for r in rows]
            assert versions == list(range(1, 11))
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# H2 — ANALYZE releases buffer lock during iteration (race condition)
# ---------------------------------------------------------------------------


class TestAnalyzeConcurrentRace:
    """``scan_analyze_next_tuple`` (xpatch_tam.c:2076-2085) unlocks the
    buffer to do reconstruction, then re-locks to continue iterating.
    Between unlock and re-lock, another transaction could modify the page.

    Bug: xpatch_tam.c:2076-2085 (known bug H2)

    Regression guard — tests that ANALYZE + concurrent INSERT don't crash.
    """

    def test_analyze_with_concurrent_inserts_no_crash(
        self, db: psycopg.Connection, db2: psycopg.Connection, make_table
    ):
        """Running ANALYZE while inserting rows concurrently should not crash."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=50)

        results: dict[str, str] = {}

        def do_analyze(conn, name):
            try:
                for _ in range(5):
                    conn.execute(f"ANALYZE {t}")
                results[name] = "ok"
            except Exception as e:
                results[name] = f"error: {e}"

        def do_inserts(conn, name):
            try:
                for v in range(51, 151):
                    insert_rows(conn, t, [(1, v, f"concurrent-v{v}")])
                results[name] = "ok"
            except Exception as e:
                results[name] = f"error: {e}"

        t1 = threading.Thread(target=do_analyze, args=(db, "analyze"))
        t2 = threading.Thread(target=do_inserts, args=(db2, "inserts"))

        t1.start()
        t2.start()
        t1.join(timeout=30)
        t2.join(timeout=30)

        assert results.get("analyze") == "ok", f"analyze: {results.get('analyze')}"
        assert results.get("inserts") == "ok", f"inserts: {results.get('inserts')}"

        # Verify data integrity after the race
        cnt = row_count(db, t)
        assert cnt == 150, f"Expected 150 rows, got {cnt}"

    def test_analyze_with_concurrent_deletes_no_crash(
        self, db: psycopg.Connection, db2: psycopg.Connection, make_table
    ):
        """Running ANALYZE while deleting rows concurrently should not crash."""
        t = make_table(keyframe_every=10)
        for g in range(1, 11):
            insert_versions(db, t, group_id=g, count=20)

        results: dict[str, str] = {}

        def do_analyze(conn, name):
            try:
                for _ in range(3):
                    conn.execute(f"ANALYZE {t}")
                    time.sleep(0.05)
                results[name] = "ok"
            except Exception as e:
                results[name] = f"error: {e}"

        def do_deletes(conn, name):
            try:
                # Delete some groups
                for g in range(6, 11):
                    conn.execute(
                        f"DELETE FROM {t} WHERE group_id = {g} AND version = 1"
                    )
                results[name] = "ok"
            except Exception as e:
                results[name] = f"error: {e}"

        t1 = threading.Thread(target=do_analyze, args=(db, "analyze"))
        t2 = threading.Thread(target=do_deletes, args=(db2, "deletes"))

        t1.start()
        t2.start()
        t1.join(timeout=30)
        t2.join(timeout=30)

        assert results.get("analyze") == "ok", f"analyze: {results.get('analyze')}"
        assert results.get("deletes") == "ok", f"deletes: {results.get('deletes')}"
