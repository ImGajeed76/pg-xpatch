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
"""

from __future__ import annotations

import psycopg
import pytest

from conftest import insert_rows, insert_versions, row_count


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
                raise Exception("force rollback")
        except Exception:
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
                raise Exception("force rollback")
        except Exception:
            pass

        assert row_count(db, t) == 0

    def test_insert_after_rollback_works(self, db: psycopg.Connection, xpatch_table):
        """INSERT after a rolled-back transaction works correctly (no corruption)."""
        t = xpatch_table

        # First: rollback
        try:
            with db.transaction():
                insert_rows(db, t, [(1, 1, "rolled back")])
                raise Exception("force rollback")
        except Exception:
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
                    raise Exception("rollback")
            except Exception:
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

            with pytest.raises(Exception):
                with db.transaction():  # nested = savepoint
                    insert_rows(db, t, [(1, 2, "after savepoint")])
                    raise Exception("rollback to savepoint")

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

            with pytest.raises(Exception):
                with db.transaction():
                    insert_rows(db, t, [(1, 2, "will vanish")])
                    raise Exception("partial rollback")

            # Insert more after the partial rollback
            insert_rows(db, t, [(1, 3, "v3 after partial")])

        assert row_count(db, t) == 2
        rows = db.execute(
            f"SELECT version, content FROM {t} ORDER BY version"
        ).fetchall()
        assert rows[0]["content"] == "v1"
        assert rows[1]["content"] == "v3 after partial"


class TestIsolation:
    """Transaction isolation between connections."""

    def test_uncommitted_not_visible_to_other_conn(
        self, db: psycopg.Connection, db2: psycopg.Connection, xpatch_table
    ):
        """Uncommitted INSERT not visible from another connection."""
        t = xpatch_table

        # db2 needs autocommit off to hold a transaction open on db
        # But db is autocommit=True by default, so we use explicit transaction
        # Start a transaction on db without committing
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


class TestDeleteInTransaction:
    """DELETE behavior within transactions."""

    def test_delete_rollback(self, db: psycopg.Connection, xpatch_table):
        """DELETE inside a rolled-back transaction is undone."""
        t = xpatch_table
        insert_versions(db, t, group_id=1, count=5)

        try:
            with db.transaction():
                db.execute(f"DELETE FROM {t} WHERE group_id = 1 AND version = 3")
                # Cascade: v3,v4,v5 deleted within txn
                assert row_count(db, t) == 2
                raise Exception("rollback")
        except Exception:
            pass

        # All 5 rows should be back
        assert row_count(db, t) == 5
