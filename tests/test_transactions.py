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
"""

from __future__ import annotations

import psycopg
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
