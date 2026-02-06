"""
Test transaction behavior on xpatch tables.

Covers:
- INSERT visible within same transaction
- ROLLBACK undoes INSERT
- SAVEPOINT + ROLLBACK TO partial undo
- INSERT after ROLLBACK works (no corruption)
- Multi-statement transaction commit
- INSERT RETURNING inside transaction
- Visibility isolation between connections (db vs db2)
- Nested transactions via savepoints
- DELETE within transactions and savepoints
- Concurrent insert serialization via advisory locks
- Multi-group transaction atomicity
- MVCC visibility via index scan (H1 — fixed)
- SELECT FOR SHARE / FOR KEY SHARE (M11 — fixed)
- Concurrent DELETE serialization (H4 — fixed)
- MVCC visibility via sequential scan (C4 — regression guard)
- MVCC visibility via bitmap scan (H6 — fixed)
- Speculative INSERT orphan (C1 — known bug, xfail)
- Sequential scan nblocks not updated (H3 — regression guard)
- DELETE two-pass race with VACUUM (H5 — regression guard)
- SERIALIZABLE snapshot visibility (M5 — regression guard)
"""

from __future__ import annotations

import threading
import time
from typing import Any

import psycopg
import psycopg.errors
import pytest

from conftest import insert_rows, insert_versions, row_count


class _ForceRollback(Exception):
    """Raised intentionally to trigger transaction rollback in tests."""


class TestTransactionCommit:
    """Committed transactions are visible."""

    def test_insert_visible_in_transaction(self, db: psycopg.Connection, xpatch_table):
        """INSERT is visible within the same explicit transaction."""
        t = xpatch_table
        with db.transaction():
            insert_rows(db, t, [(1, 1, "in txn")])
            # Should be visible within the transaction
            cnt = row_count(db, t)
            assert cnt == 1
            # Verify content too
            row = db.execute(f"SELECT content FROM {t}").fetchone()
            assert row["content"] == "in txn"

    def test_multi_insert_transaction(self, db: psycopg.Connection, xpatch_table):
        """Multiple INSERTs in one transaction all committed together."""
        t = xpatch_table
        with db.transaction():
            for v in range(1, 6):
                insert_rows(db, t, [(1, v, f"v{v}")])

        assert row_count(db, t) == 5
        rows = db.execute(
            f"SELECT version, content FROM {t} ORDER BY version"
        ).fetchall()
        for row in rows:
            assert row["content"] == f"v{row['version']}"

    def test_insert_returning_in_transaction(self, db: psycopg.Connection, xpatch_table):
        """INSERT RETURNING works inside a transaction."""
        t = xpatch_table
        with db.transaction():
            row = db.execute(
                f"INSERT INTO {t} (group_id, version, content) "
                f"VALUES (1, 1, 'test') RETURNING group_id, version, content"
            ).fetchone()
            assert row["group_id"] == 1
            assert row["version"] == 1
            assert row["content"] == "test"


class TestTransactionRollback:
    """ROLLBACK undoes changes."""

    def test_rollback_undoes_insert(self, db: psycopg.Connection, xpatch_table):
        """ROLLBACK makes inserted rows disappear."""
        t = xpatch_table
        try:
            with db.transaction():
                insert_rows(db, t, [(1, 1, "will vanish")])
                assert row_count(db, t) == 1
                raise _ForceRollback()
        except _ForceRollback:
            pass

        assert row_count(db, t) == 0

    def test_rollback_multiple_inserts(self, db: psycopg.Connection, xpatch_table):
        """ROLLBACK undoes all inserts in the transaction."""
        t = xpatch_table
        try:
            with db.transaction():
                for v in range(1, 11):
                    insert_rows(db, t, [(1, v, f"v{v}")])
                assert row_count(db, t) == 10
                raise _ForceRollback()
        except _ForceRollback:
            pass

        assert row_count(db, t) == 0

    def test_insert_after_rollback_works(self, db: psycopg.Connection, xpatch_table):
        """INSERT after a rolled-back transaction works correctly (no corruption)."""
        t = xpatch_table

        # First: rollback
        try:
            with db.transaction():
                insert_rows(db, t, [(1, 1, "rolled back")])
                raise _ForceRollback()
        except _ForceRollback:
            pass

        # Then: successful insert
        insert_rows(db, t, [(1, 1, "survived")])
        assert row_count(db, t) == 1
        row = db.execute(f"SELECT content FROM {t}").fetchone()
        assert row["content"] == "survived"

    def test_multiple_rollback_cycles(self, db: psycopg.Connection, xpatch_table):
        """Multiple rollback cycles don't corrupt the table."""
        t = xpatch_table

        for i in range(5):
            try:
                with db.transaction():
                    insert_rows(db, t, [(1, i + 1, f"attempt {i}")])
                    raise _ForceRollback()
            except _ForceRollback:
                pass

        assert row_count(db, t) == 0

        # Final successful insert
        insert_rows(db, t, [(1, 1, "final")])
        assert row_count(db, t) == 1
        row = db.execute(f"SELECT content FROM {t}").fetchone()
        assert row["content"] == "final"


class TestSavepoints:
    """SAVEPOINT and ROLLBACK TO for partial undo."""

    def test_savepoint_partial_rollback(self, db: psycopg.Connection, xpatch_table):
        """ROLLBACK TO SAVEPOINT undoes only changes after the savepoint."""
        t = xpatch_table
        with db.transaction():
            insert_rows(db, t, [(1, 1, "before savepoint")])

            with pytest.raises(_ForceRollback):
                with db.transaction():  # nested = savepoint
                    insert_rows(db, t, [(1, 2, "after savepoint")])
                    raise _ForceRollback()

            # v1 should still be there, v2 should not
            cnt = row_count(db, t)
            assert cnt == 1

        assert row_count(db, t) == 1
        row = db.execute(f"SELECT content FROM {t}").fetchone()
        assert row["content"] == "before savepoint"

    def test_savepoint_insert_after_partial_rollback(self, db: psycopg.Connection, xpatch_table):
        """INSERT after partial rollback works correctly."""
        t = xpatch_table
        with db.transaction():
            insert_rows(db, t, [(1, 1, "v1")])

            with pytest.raises(_ForceRollback):
                with db.transaction():
                    insert_rows(db, t, [(1, 2, "will vanish")])
                    raise _ForceRollback()

            # Insert more after the partial rollback
            insert_rows(db, t, [(1, 3, "v3 after partial")])

        assert row_count(db, t) == 2
        rows = db.execute(
            f"SELECT version, content FROM {t} ORDER BY version"
        ).fetchall()
        assert rows[0]["content"] == "v1"
        assert rows[1]["content"] == "v3 after partial"

    def test_savepoint_delete_rollback(self, db: psycopg.Connection, xpatch_table):
        """DELETE inside rolled-back savepoint is undone."""
        t = xpatch_table
        insert_versions(db, t, group_id=1, count=3)
        with db.transaction():
            with pytest.raises(_ForceRollback):
                with db.transaction():  # savepoint
                    db.execute(f"DELETE FROM {t} WHERE group_id = 1 AND version = 2")
                    # CASCADE: v2,v3 deleted
                    assert row_count(db, t) == 1
                    raise _ForceRollback()
            # After savepoint rollback, all 3 rows should be back
            assert row_count(db, t) == 3


class TestIsolation:
    """Transaction isolation between connections."""

    def test_uncommitted_not_visible_to_other_conn(
        self, db: psycopg.Connection, db2: psycopg.Connection, xpatch_table
    ):
        """Uncommitted INSERT not visible from another connection.

        Uses raw BEGIN/ROLLBACK because db.transaction() auto-commits on exit,
        making it impossible to test uncommitted visibility mid-transaction.
        """
        t = xpatch_table

        db.execute("BEGIN")
        try:
            insert_rows(db, t, [(1, 1, "uncommitted")])
            # db sees it
            assert row_count(db, t) == 1
            # db2 does NOT see it (Read Committed default)
            assert row_count(db2, t) == 0
        finally:
            db.execute("ROLLBACK")

    def test_committed_visible_to_other_conn(
        self, db: psycopg.Connection, db2: psycopg.Connection, xpatch_table
    ):
        """Committed INSERT is visible from another connection."""
        t = xpatch_table
        insert_rows(db, t, [(1, 1, "committed")])  # autocommit
        assert row_count(db2, t) == 1

    def test_rollback_not_visible_to_other_conn(
        self, db: psycopg.Connection, db2: psycopg.Connection, xpatch_table
    ):
        """Rolled back INSERT is never visible to another connection."""
        t = xpatch_table
        db.execute("BEGIN")
        try:
            insert_rows(db, t, [(1, 1, "will rollback")])
            assert row_count(db2, t) == 0
        finally:
            db.execute("ROLLBACK")

        assert row_count(db2, t) == 0

    def test_autocommit_baseline(
        self, db: psycopg.Connection, db2: psycopg.Connection, xpatch_table
    ):
        """With autocommit=True (default), each INSERT is immediately visible."""
        t = xpatch_table
        insert_rows(db, t, [(1, 1, "auto")])
        assert row_count(db2, t) == 1
        row = db2.execute(f"SELECT content FROM {t}").fetchone()
        assert row["content"] == "auto"

    def test_delete_not_visible_until_commit(
        self, db: psycopg.Connection, db2: psycopg.Connection, xpatch_table
    ):
        """Cascade DELETE within txn not visible to other connections."""
        t = xpatch_table
        insert_versions(db, t, group_id=1, count=3)

        db.execute("BEGIN")
        try:
            db.execute(f"DELETE FROM {t} WHERE group_id = 1 AND version = 2")
            # db sees cascade (v2,v3 deleted)
            assert row_count(db, t) == 1
            # db2 still sees all 3
            assert row_count(db2, t) == 3
        finally:
            db.execute("ROLLBACK")


class TestDeleteInTransaction:
    """DELETE behavior within transactions."""

    def test_delete_rollback(self, db: psycopg.Connection, xpatch_table):
        """DELETE inside a rolled-back transaction is undone.

        Note: cascade delete is based on physical insertion order (_xp_seq),
        which matches version order here because insert_versions inserts
        in ascending version order.
        """
        t = xpatch_table
        insert_versions(db, t, group_id=1, count=5)

        try:
            with db.transaction():
                db.execute(f"DELETE FROM {t} WHERE group_id = 1 AND version = 3")
                # Cascade: v3,v4,v5 deleted within txn
                assert row_count(db, t) == 2
                raise _ForceRollback()
        except _ForceRollback:
            pass

        # All 5 rows should be back
        assert row_count(db, t) == 5
        versions = [r["version"] for r in db.execute(
            f"SELECT version FROM {t} ORDER BY version"
        ).fetchall()]
        assert versions == [1, 2, 3, 4, 5]

    def test_insert_then_delete_then_rollback(self, db: psycopg.Connection, xpatch_table):
        """INSERT + DELETE in same txn, then ROLLBACK, leaves table empty."""
        t = xpatch_table
        try:
            with db.transaction():
                insert_rows(db, t, [(1, 1, "inserted")])
                assert row_count(db, t) == 1
                db.execute(f"DELETE FROM {t} WHERE group_id = 1 AND version = 1")
                assert row_count(db, t) == 0
                raise _ForceRollback()
        except _ForceRollback:
            pass
        assert row_count(db, t) == 0


class TestMultiGroupTransaction:
    """Transaction atomicity across multiple groups."""

    def test_multi_group_commit(self, db: psycopg.Connection, xpatch_table):
        """Transaction spanning multiple groups commits atomically."""
        t = xpatch_table
        with db.transaction():
            insert_rows(db, t, [(1, 1, "g1v1")])
            insert_rows(db, t, [(2, 1, "g2v1")])
            insert_rows(db, t, [(3, 1, "g3v1")])
        assert row_count(db, t) == 3
        assert row_count(db, t, "group_id = 1") == 1
        assert row_count(db, t, "group_id = 2") == 1
        assert row_count(db, t, "group_id = 3") == 1

    def test_multi_group_rollback(self, db: psycopg.Connection, xpatch_table):
        """Transaction spanning multiple groups rolls back atomically."""
        t = xpatch_table
        try:
            with db.transaction():
                insert_rows(db, t, [(1, 1, "g1v1")])
                insert_rows(db, t, [(2, 1, "g2v1")])
                raise _ForceRollback()
        except _ForceRollback:
            pass
        assert row_count(db, t) == 0


class TestConcurrentInserts:
    """Concurrent inserts via advisory lock serialization."""

    def test_concurrent_insert_different_groups(
        self, db: psycopg.Connection, db2: psycopg.Connection, xpatch_table
    ):
        """Inserts to different groups don't contend — both succeed."""
        t = xpatch_table
        db.execute("BEGIN")
        try:
            insert_rows(db, t, [(1, 1, "group 1 from db")])
            # db2 inserts to a different group — should not block
            insert_rows(db2, t, [(2, 1, "group 2 from db2")])
            assert row_count(db2, t, "group_id = 2") == 1
            db.execute("COMMIT")
        except Exception:
            db.execute("ROLLBACK")
            raise

        assert row_count(db, t) == 2

    def test_concurrent_insert_same_group_after_commit(
        self, db: psycopg.Connection, db2: psycopg.Connection, xpatch_table
    ):
        """Two connections insert to same group sequentially."""
        t = xpatch_table
        insert_rows(db, t, [(1, 1, "from db")])
        insert_rows(db2, t, [(1, 2, "from db2")])
        assert row_count(db, t) == 2
        rows = db.execute(
            f"SELECT version, content FROM {t} ORDER BY version"
        ).fetchall()
        assert rows[0]["content"] == "from db"
        assert rows[1]["content"] == "from db2"


# ---------------------------------------------------------------------------
# H1 — MVCC visibility via index scan (known bug: uncommitted rows visible)
# ---------------------------------------------------------------------------


class TestMvccVisibilityIndexScan:
    """``xpatch_tuple_fetch_row_version`` now calls HeapTupleSatisfiesVisibility
    to respect transaction isolation on index-scan paths.

    Bug: xpatch_tam.c:1537-1542 (fixed)
    """

    def test_uncommitted_row_invisible_via_index_scan(
        self, db: psycopg.Connection, db2: psycopg.Connection, make_table
    ):
        """An uncommitted INSERT should not be visible via index scan."""
        t = make_table()
        db.execute(f"CREATE UNIQUE INDEX {t}_uk ON {t} (group_id, version)")

        db.autocommit = False
        try:
            db.execute(
                f"INSERT INTO {t} (group_id, version, content) "
                f"VALUES (1, 1, 'uncommitted')"
            )

            # From db2 with index scan forced, the uncommitted row should NOT be visible
            db2.execute("SET enable_seqscan = off")
            rows = db2.execute(
                f"SELECT * FROM {t} WHERE group_id = 1 AND version = 1"
            ).fetchall()

            assert len(rows) == 0, (
                f"Uncommitted row visible via index scan: {rows}"
            )
        finally:
            db.rollback()
            db.autocommit = True


# ---------------------------------------------------------------------------
# M11 — SELECT FOR SHARE / FOR KEY SHARE (fixed: slot now populated)
# ---------------------------------------------------------------------------


class TestSelectForLock:
    """``xpatch_tuple_lock`` now copies and converts the locked tuple into
    the TupleTableSlot via ``xpatch_physical_to_logical``.

    Bug: xpatch_tam.c:1463-1471 (fixed)
    """

    def test_select_for_share_returns_data(
        self, db: psycopg.Connection, xpatch_table
    ):
        """SELECT ... FOR SHARE should return the actual row data."""
        t = xpatch_table
        insert_rows(db, t, [(1, 1, "locked-row")])

        with db.transaction():
            rows = db.execute(
                f"SELECT group_id, version, content FROM {t} "
                f"WHERE group_id = 1 FOR SHARE"
            ).fetchall()

        assert len(rows) == 1
        assert rows[0]["content"] == "locked-row"

    def test_select_for_key_share_returns_data(
        self, db: psycopg.Connection, xpatch_table
    ):
        """SELECT ... FOR KEY SHARE should return the actual row data."""
        t = xpatch_table
        insert_rows(db, t, [(1, 1, "key-share-row")])

        with db.transaction():
            rows = db.execute(
                f"SELECT group_id, version, content FROM {t} "
                f"WHERE group_id = 1 FOR KEY SHARE"
            ).fetchall()

        assert len(rows) == 1
        assert rows[0]["content"] == "key-share-row"


# ---------------------------------------------------------------------------
# H4 — Concurrent DELETE serialization (fixed: uses XactLockTableWait)
# ---------------------------------------------------------------------------


class TestConcurrentDeleteSerialization:
    """The DELETE wait path now calls ``XactLockTableWait`` and re-checks
    the tuple after the other transaction finishes.

    Bug: xpatch_tam.c:1137-1139 (fixed)
    """

    def test_concurrent_delete_same_group_serializes(
        self, db: psycopg.Connection, db_factory, make_table
    ):
        """Two concurrent DELETEs on the same group should serialize correctly."""
        t = make_table(keyframe_every=10)
        insert_versions(db, t, group_id=1, count=5)

        db2 = db_factory()
        results: dict[str, Any] = {}

        def delete_in_txn(conn, conn_name, version):
            try:
                conn.autocommit = False
                conn.execute(
                    f"DELETE FROM {t} WHERE group_id = 1 AND version = {version}"
                )
                time.sleep(0.5)
                conn.commit()
                results[conn_name] = "ok"
            except Exception as e:
                conn.rollback()
                results[conn_name] = f"error: {e}"
            finally:
                conn.autocommit = True

        t1 = threading.Thread(target=delete_in_txn, args=(db, "conn1", 1))
        t2 = threading.Thread(target=delete_in_txn, args=(db2, "conn2", 3))

        t1.start()
        time.sleep(0.1)
        t2.start()

        t1.join(timeout=10)
        t2.join(timeout=10)

        assert results.get("conn1") == "ok", f"conn1: {results.get('conn1')}"
        assert results.get("conn2") == "ok", f"conn2: {results.get('conn2')}"

        # Verify table is in a consistent state
        rows = db.execute(
            f"SELECT version, content FROM {t} WHERE group_id = 1 ORDER BY version"
        ).fetchall()
        for r in rows:
            assert r["content"] is not None


# ---------------------------------------------------------------------------
# C4 — Sequential scan MVCC visibility (known bug: simplified check)
# ---------------------------------------------------------------------------


class TestMvccVisibilitySeqScan:
    """``xpatch_tuple_is_visible`` (xpatch_tam.c:107-170) used by sequential
    scans is a simplified MVCC check that doesn't call
    ``HeapTupleSatisfiesVisibility``. Despite missing some edge cases
    (FrozenTransactionId, CommandId, snapshot boundaries), it handles the
    common Read Committed cases correctly.

    Regression tests for C4 audit finding — these pass today but guard
    against future regressions in the visibility logic.
    """

    def test_uncommitted_row_invisible_via_seq_scan(
        self, db: psycopg.Connection, db2: psycopg.Connection, make_table
    ):
        """An uncommitted INSERT should not be visible via sequential scan.

        Forces sequential scan by disabling index and bitmap scan.
        From db2, the uncommitted row inserted by db should NOT be visible.
        """
        t = make_table()

        db.autocommit = False
        try:
            db.execute(
                f"INSERT INTO {t} (group_id, version, content) "
                f"VALUES (1, 1, 'uncommitted')"
            )

            # Force sequential scan only
            db2.execute("SET enable_indexscan = off")
            db2.execute("SET enable_bitmapscan = off")
            db2.execute("SET enable_seqscan = on")
            rows = db2.execute(
                f"SELECT * FROM {t}"
            ).fetchall()

            assert len(rows) == 0, (
                f"Uncommitted row visible via sequential scan: {rows}"
            )
        finally:
            db.rollback()
            db.autocommit = True
            db2.execute("SET enable_indexscan = on")
            db2.execute("SET enable_bitmapscan = on")

    def test_deleted_row_still_visible_to_other_txn_via_seq_scan(
        self, db: psycopg.Connection, db2: psycopg.Connection, make_table
    ):
        """A row deleted in an uncommitted txn should still be visible to other
        connections via sequential scan (Read Committed).

        This tests the XMAX-in-progress path of the simplified MVCC check.
        """
        t = make_table()
        db.execute(
            f"INSERT INTO {t} (group_id, version, content) "
            f"VALUES (1, 1, 'visible')"
        )

        # db deletes within a transaction but does not commit
        db.autocommit = False
        try:
            db.execute(f"DELETE FROM {t} WHERE group_id = 1 AND version = 1")
            # db itself should see 0 rows
            assert row_count(db, t) == 0

            # db2 should still see the row (deleter hasn't committed)
            db2.execute("SET enable_indexscan = off")
            db2.execute("SET enable_bitmapscan = off")
            db2.execute("SET enable_seqscan = on")
            rows = db2.execute(
                f"SELECT * FROM {t}"
            ).fetchall()
            assert len(rows) == 1, (
                f"Row should still be visible to db2 via seq scan while delete is uncommitted"
            )
        finally:
            db.rollback()
            db.autocommit = True
            db2.execute("SET enable_indexscan = on")
            db2.execute("SET enable_bitmapscan = on")


# ---------------------------------------------------------------------------
# H6 — Bitmap scan MVCC visibility (known bug: zero MVCC checking)
# ---------------------------------------------------------------------------


class TestMvccVisibilityBitmapScan:
    """``xpatch_scan_bitmap_next_tuple`` (xpatch_tam.c:2556-2624) now includes
    MVCC visibility checking via ``xpatch_tuple_is_visible``.

    Previously, bitmap scans returned every tuple that was ``ItemIdIsNormal``
    regardless of transaction state (bug H6, fixed).
    """

    def test_uncommitted_row_invisible_via_bitmap_scan(
        self, db: psycopg.Connection, db2: psycopg.Connection, make_table
    ):
        """An uncommitted INSERT should not be visible via bitmap scan."""
        t = make_table()
        # Insert some committed rows first so the planner has stats
        for g in range(1, 6):
            insert_versions(db, t, group_id=g, count=5)
        db.execute(f"ANALYZE {t}")

        db.autocommit = False
        try:
            # Insert an uncommitted row in group 99
            db.execute(
                f"INSERT INTO {t} (group_id, version, content) "
                f"VALUES (99, 1, 'uncommitted-bm')"
            )

            # Force bitmap scan from db2
            db2.execute("SET enable_seqscan = off")
            db2.execute("SET enable_indexscan = off")
            db2.execute("SET enable_bitmapscan = on")
            rows = db2.execute(
                f"SELECT * FROM {t} WHERE group_id = 99"
            ).fetchall()

            assert len(rows) == 0, (
                f"Uncommitted row visible via bitmap scan: {rows}"
            )
        finally:
            db.rollback()
            db.autocommit = True
            db2.execute("SET enable_seqscan = on")
            db2.execute("SET enable_indexscan = on")

    def test_deleted_row_invisible_via_bitmap_scan(
        self, db: psycopg.Connection, make_table
    ):
        """A deleted-and-committed row should not be visible via bitmap scan."""
        t = make_table()
        for g in range(1, 6):
            insert_versions(db, t, group_id=g, count=10)
        db.execute(f"ANALYZE {t}")

        # Delete group 3 entirely (cascade from version 1)
        db.execute(f"DELETE FROM {t} WHERE group_id = 3 AND version = 1")

        # Force bitmap scan
        db.execute("SET enable_seqscan = off")
        db.execute("SET enable_indexscan = off")
        db.execute("SET enable_bitmapscan = on")
        try:
            rows = db.execute(
                f"SELECT * FROM {t} WHERE group_id = 3"
            ).fetchall()
            assert len(rows) == 0, (
                f"Deleted rows visible via bitmap scan: {rows}"
            )
        finally:
            db.execute("SET enable_seqscan = on")
            db.execute("SET enable_indexscan = on")


# ---------------------------------------------------------------------------
# C1 — Speculative INSERT orphan (INSERT ON CONFLICT)
# ---------------------------------------------------------------------------


class TestSpeculativeInsertOrphan:
    """``xpatch_tuple_insert_speculative`` delegates to the regular insert
    path, so ``complete_speculative(false)`` cannot undo it — an orphaned
    tuple permanently occupies a sequence slot.

    Bug: xpatch_tam.c:982-1023 (known bug C1)
    """

    @pytest.mark.xfail(
        strict=False,
        reason="C1: speculative insert orphan — complete_speculative(false) "
               "does not remove the row; may leave COUNT=2",
    )
    def test_concurrent_on_conflict_do_nothing_no_orphan(
        self, db: psycopg.Connection, db2: psycopg.Connection, make_table
    ):
        """Two concurrent INSERT ON CONFLICT DO NOTHING for the same key
        should result in exactly 1 row.  If the speculative insert is not
        properly aborted, an orphan row leaks and COUNT becomes 2.
        """
        t = make_table()
        db.execute(f"CREATE UNIQUE INDEX ON {t} (group_id, version)")

        results: dict[str, Any] = {}

        def do_insert(conn, name):
            try:
                conn.execute(
                    f"INSERT INTO {t} (group_id, version, content) "
                    f"VALUES (1, 1, 'from-{name}') "
                    f"ON CONFLICT (group_id, version) DO NOTHING"
                )
                results[name] = "ok"
            except Exception as e:
                results[name] = f"error: {e}"

        t1 = threading.Thread(target=do_insert, args=(db, "conn1"))
        t2 = threading.Thread(target=do_insert, args=(db2, "conn2"))
        t1.start()
        t2.start()
        t1.join(timeout=10)
        t2.join(timeout=10)

        assert results.get("conn1") == "ok", f"conn1: {results.get('conn1')}"
        assert results.get("conn2") == "ok", f"conn2: {results.get('conn2')}"

        cnt = row_count(db, t)
        assert cnt == 1, (
            f"Expected exactly 1 row after concurrent ON CONFLICT DO NOTHING, "
            f"got {cnt} — orphan row leaked (C1)"
        )

    def test_sequential_on_conflict_do_nothing_integrity(
        self, db: psycopg.Connection, make_table
    ):
        """Sequential INSERT ON CONFLICT DO NOTHING — no orphan for the
        non-concurrent case (regression guard).
        """
        t = make_table()
        db.execute(f"CREATE UNIQUE INDEX ON {t} (group_id, version)")

        insert_rows(db, t, [(1, 1, "original")])
        db.execute(
            f"INSERT INTO {t} (group_id, version, content) "
            f"VALUES (1, 1, 'duplicate') ON CONFLICT (group_id, version) DO NOTHING"
        )
        assert row_count(db, t) == 1


# ---------------------------------------------------------------------------
# H3 — Sequential scan nblocks not updated during scan
# ---------------------------------------------------------------------------


class TestScanNblocksNotUpdated:
    """``xpatch_scan_getnextslot`` uses ``scan->nblocks`` set once at
    ``scan_begin``.  If rows are inserted concurrently that extend the
    relation, the scan misses the new blocks.

    Bug: xpatch_tam.c:599-709 (known bug H3)

    Regression guard — tests that a cursor-based scan at least returns
    the originally-visible rows and doesn't crash.
    """

    def test_cursor_scan_during_concurrent_insert(
        self, db: psycopg.Connection, db2: psycopg.Connection, make_table
    ):
        """A server-side cursor opened before new rows are inserted should
        return at least the original rows without crashing.
        """
        t = make_table()
        insert_versions(db, t, group_id=1, count=10)

        # Open a cursor on db — nblocks is frozen here
        db.autocommit = False
        try:
            db.execute(f"DECLARE cur CURSOR FOR SELECT version FROM {t} ORDER BY version")

            # Insert more rows from db2, extending the relation
            for v in range(11, 31):
                insert_rows(db2, t, [(1, v, f"Version {v} content")])

            # Fetch all from cursor — should get at least the original 10
            rows = db.execute("FETCH ALL FROM cur").fetchall()
            db.execute("CLOSE cur")

            assert len(rows) >= 10, (
                f"Expected at least 10 rows from cursor, got {len(rows)}"
            )
            # Verify the first 10 are correct
            versions = [r["version"] for r in rows[:10]]
            assert versions == list(range(1, 11))
        finally:
            db.rollback()
            db.autocommit = True


# ---------------------------------------------------------------------------
# H5 — DELETE two-pass race with concurrent VACUUM
# ---------------------------------------------------------------------------


class TestDeleteVacuumRace:
    """``xpatch_tuple_delete`` does two passes using a sequential counter
    instead of ``_xp_seq``.  If VACUUM modifies page layout between passes,
    the counter may mismatch, causing wrong tuples to be deleted.

    Bug: xpatch_tam.c:1329-1453 (known bug H5)

    Regression guard — hard to trigger deterministically but tests that
    DELETE + concurrent VACUUM don't corrupt data.
    """

    def test_delete_with_concurrent_vacuum_no_corruption(
        self, db: psycopg.Connection, db2: psycopg.Connection, make_table
    ):
        """DELETE and VACUUM running concurrently on the same group should
        not corrupt data or crash.
        """
        t = make_table(keyframe_every=10)
        # Insert enough data that VACUUM has work to do
        insert_versions(db, t, group_id=1, count=20)
        insert_versions(db, t, group_id=2, count=20)

        # First, delete some rows to create dead tuples for VACUUM
        db.execute(f"DELETE FROM {t} WHERE group_id = 2 AND version = 1")

        results: dict[str, Any] = {}

        def do_delete(conn, name):
            try:
                conn.execute(
                    f"DELETE FROM {t} WHERE group_id = 1 AND version = 10"
                )
                results[name] = "ok"
            except Exception as e:
                results[name] = f"error: {e}"

        def do_vacuum(conn, name):
            try:
                conn.execute(f"VACUUM {t}")
                results[name] = "ok"
            except Exception as e:
                results[name] = f"error: {e}"

        t1 = threading.Thread(target=do_delete, args=(db, "delete"))
        t2 = threading.Thread(target=do_vacuum, args=(db2, "vacuum"))

        t1.start()
        t2.start()
        t1.join(timeout=15)
        t2.join(timeout=15)

        # Both should complete without error
        assert results.get("delete") == "ok", f"delete: {results.get('delete')}"
        assert results.get("vacuum") == "ok", f"vacuum: {results.get('vacuum')}"

        # Verify remaining group 1 data is consistent
        rows = db.execute(
            f"SELECT version, content FROM {t} WHERE group_id = 1 ORDER BY version"
        ).fetchall()
        # Cascade from version 10: v10-v20 deleted, v1-v9 remain
        assert len(rows) == 9
        for row in rows:
            assert row["content"] is not None
            assert row["version"] <= 9


# ---------------------------------------------------------------------------
# M5 — SERIALIZABLE snapshot visibility always returns true for virtual tuples
# ---------------------------------------------------------------------------


class TestSerializableIsolation:
    """``xpatch_tuple_satisfies_snapshot`` always returns true for virtual
    tuples.  Despite this, PostgreSQL's predicate locking detects
    SERIALIZABLE conflicts independently of the TAM's snapshot check.

    Bug: xpatch_tam.c (known bug M5)

    Regression guards — SERIALIZABLE isolation works today because PG's
    predicate lock manager catches the conflict.
    """

    def test_serializable_write_write_conflict(
        self, db: psycopg.Connection, db_factory, make_table
    ):
        """Two SERIALIZABLE transactions writing to the same table should
        produce a serialization failure if they overlap.
        """
        t = make_table()
        insert_versions(db, t, group_id=1, count=5)

        db2 = db_factory()
        results: dict[str, Any] = {}

        def txn_insert(conn, name, version):
            try:
                conn.autocommit = False
                conn.execute(
                    "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"
                )
                # Read the table to establish a read dependency
                conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()
                time.sleep(0.2)
                # Insert new row
                conn.execute(
                    f"INSERT INTO {t} (group_id, version, content) "
                    f"VALUES (1, {version}, 'ser-{name}')"
                )
                conn.commit()
                results[name] = "ok"
            except psycopg.errors.SerializationFailure:
                conn.rollback()
                results[name] = "serialization_failure"
            except Exception as e:
                conn.rollback()
                results[name] = f"error: {e}"
            finally:
                conn.autocommit = True

        t1 = threading.Thread(target=txn_insert, args=(db, "conn1", 100))
        t2 = threading.Thread(target=txn_insert, args=(db2, "conn2", 200))
        t1.start()
        time.sleep(0.05)
        t2.start()
        t1.join(timeout=10)
        t2.join(timeout=10)

        # At least one should get serialization_failure
        outcomes = [results.get("conn1"), results.get("conn2")]
        assert "serialization_failure" in outcomes, (
            f"Expected at least one SERIALIZABLE conflict, got: {outcomes}"
        )

    def test_read_committed_concurrent_inserts_both_succeed(
        self, db: psycopg.Connection, db2: psycopg.Connection, make_table
    ):
        """Under READ COMMITTED (default), concurrent inserts to separate
        groups both succeed — regression guard for M5.
        """
        t = make_table()
        results: dict[str, Any] = {}

        def do_insert(conn, name, gid):
            try:
                insert_rows(conn, t, [(gid, 1, f"from-{name}")])
                results[name] = "ok"
            except Exception as e:
                results[name] = f"error: {e}"

        t1 = threading.Thread(target=do_insert, args=(db, "conn1", 1))
        t2 = threading.Thread(target=do_insert, args=(db2, "conn2", 2))
        t1.start()
        t2.start()
        t1.join(timeout=10)
        t2.join(timeout=10)

        assert results.get("conn1") == "ok"
        assert results.get("conn2") == "ok"
        assert row_count(db, t) == 2
