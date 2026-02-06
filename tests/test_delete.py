"""
Test DELETE cascade semantics on xpatch tables.

xpatch DELETE is CASCADE: deleting a row also deletes all subsequent
versions in the same group (rows with _xp_seq >= target).

Covers:
- DELETE last version: only that row removed
- DELETE middle version: cascades to subsequent versions
- DELETE first version: removes entire group
- INSERT after DELETE: new versions work correctly
- Multi-group isolation
- Row count correct after each delete
- DELETE with WHERE on delta column
- DELETE without WHERE (all rows)
- DELETE by group_id (whole group)
- _xp_seq reset after full group delete + re-insert
- Multiple sequential deletes
"""

from __future__ import annotations

import psycopg
import pytest

from conftest import insert_rows, insert_versions, row_count


class TestDeleteLastVersion:
    """Delete the last (most recent) version in a group."""

    def test_delete_last_removes_one_row(self, db: psycopg.Connection, xpatch_table):
        """Deleting the last version removes exactly one row."""
        t = xpatch_table
        insert_versions(db, t, group_id=1, count=5)
        assert row_count(db, t) == 5

        db.execute(f"DELETE FROM {t} WHERE group_id = 1 AND version = 5")
        assert row_count(db, t) == 4

    def test_delete_last_preserves_earlier(self, db: psycopg.Connection, xpatch_table):
        """Earlier versions are intact after deleting the last."""
        t = xpatch_table
        insert_versions(db, t, group_id=1, count=5)
        db.execute(f"DELETE FROM {t} WHERE group_id = 1 AND version = 5")

        rows = db.execute(
            f"SELECT version FROM {t} ORDER BY version"
        ).fetchall()
        assert [r["version"] for r in rows] == [1, 2, 3, 4]

    def test_content_intact_after_delete_last(self, db: psycopg.Connection, xpatch_table):
        """Content of remaining versions is not corrupted by delete."""
        t = xpatch_table
        for v in range(1, 6):
            insert_rows(db, t, [(1, v, f"data-{v}")])

        db.execute(f"DELETE FROM {t} WHERE group_id = 1 AND version = 5")
        rows = db.execute(
            f"SELECT version, content FROM {t} ORDER BY version"
        ).fetchall()
        for row in rows:
            assert row["content"] == f"data-{row['version']}"


class TestDeleteMiddleVersion:
    """Delete a middle version — cascades to all subsequent versions."""

    def test_cascade_removes_subsequent(self, db: psycopg.Connection, xpatch_table):
        """Deleting v3 out of [1..5] also removes v4 and v5."""
        t = xpatch_table
        insert_versions(db, t, group_id=1, count=5)
        db.execute(f"DELETE FROM {t} WHERE group_id = 1 AND version = 3")

        assert row_count(db, t) == 2
        rows = db.execute(
            f"SELECT version FROM {t} ORDER BY version"
        ).fetchall()
        assert [r["version"] for r in rows] == [1, 2]

    def test_cascade_delete_v2_of_5(self, db: psycopg.Connection, xpatch_table):
        """Deleting v2 cascades: removes v2, v3, v4, v5. Only v1 remains."""
        t = xpatch_table
        insert_versions(db, t, group_id=1, count=5)
        db.execute(f"DELETE FROM {t} WHERE group_id = 1 AND version = 2")

        assert row_count(db, t) == 1
        row = db.execute(f"SELECT version, content FROM {t}").fetchone()
        assert row["version"] == 1

    def test_cascade_content_preserved(self, db: psycopg.Connection, xpatch_table):
        """Remaining versions have correct content after cascade delete."""
        t = xpatch_table
        for v in range(1, 6):
            insert_rows(db, t, [(1, v, f"content-{v}")])

        db.execute(f"DELETE FROM {t} WHERE group_id = 1 AND version = 4")
        rows = db.execute(
            f"SELECT version, content FROM {t} ORDER BY version"
        ).fetchall()
        assert len(rows) == 3
        for row in rows:
            assert row["content"] == f"content-{row['version']}"


class TestDeleteFirstVersion:
    """Delete the first version — removes the entire group."""

    def test_delete_first_removes_all_in_group(self, db: psycopg.Connection, xpatch_table):
        """Deleting v1 cascades to all versions — group is empty."""
        t = xpatch_table
        insert_versions(db, t, group_id=1, count=5)
        db.execute(f"DELETE FROM {t} WHERE group_id = 1 AND version = 1")
        assert row_count(db, t, "group_id = 1") == 0

    def test_delete_first_other_group_unaffected(self, db: psycopg.Connection, xpatch_table):
        """Deleting first version of group 1 does not affect group 2."""
        t = xpatch_table
        insert_versions(db, t, group_id=1, count=5)
        insert_versions(db, t, group_id=2, count=3)

        db.execute(f"DELETE FROM {t} WHERE group_id = 1 AND version = 1")
        assert row_count(db, t, "group_id = 1") == 0
        assert row_count(db, t, "group_id = 2") == 3


class TestInsertAfterDelete:
    """INSERT new versions after DELETE operations."""

    def test_insert_after_delete_last(self, db: psycopg.Connection, xpatch_table):
        """Insert works after deleting the last version."""
        t = xpatch_table
        insert_versions(db, t, group_id=1, count=5)
        db.execute(f"DELETE FROM {t} WHERE group_id = 1 AND version = 5")

        # Insert a new version
        insert_rows(db, t, [(1, 6, "new after delete")])
        assert row_count(db, t) == 5
        row = db.execute(
            f"SELECT content FROM {t} WHERE version = 6"
        ).fetchone()
        assert row["content"] == "new after delete"

    def test_insert_after_cascade_delete(self, db: psycopg.Connection, xpatch_table):
        """Insert works after cascade delete removed several versions."""
        t = xpatch_table
        insert_versions(db, t, group_id=1, count=5)
        db.execute(f"DELETE FROM {t} WHERE group_id = 1 AND version = 3")
        # v3, v4, v5 deleted; v1, v2 remain
        assert row_count(db, t) == 2

        # Insert new versions
        insert_rows(db, t, [(1, 10, "fresh start")])
        assert row_count(db, t) == 3

        row = db.execute(
            f"SELECT content FROM {t} WHERE version = 10"
        ).fetchone()
        assert row["content"] == "fresh start"

    def test_insert_after_full_group_delete(self, db: psycopg.Connection, xpatch_table):
        """Insert into a group after all its versions were deleted."""
        t = xpatch_table
        insert_versions(db, t, group_id=1, count=3)
        db.execute(f"DELETE FROM {t} WHERE group_id = 1 AND version = 1")
        assert row_count(db, t) == 0

        # Re-insert into the same group
        insert_rows(db, t, [(1, 1, "rebirth")])
        assert row_count(db, t) == 1
        row = db.execute(f"SELECT content FROM {t}").fetchone()
        assert row["content"] == "rebirth"


class TestMultiGroupIsolation:
    """DELETE operations are isolated to the target group."""

    def test_delete_one_group_other_intact(self, db: psycopg.Connection, xpatch_table):
        """Deleting from group 1 leaves groups 2 and 3 untouched."""
        t = xpatch_table
        for g in range(1, 4):
            insert_versions(db, t, group_id=g, count=5)

        db.execute(f"DELETE FROM {t} WHERE group_id = 1 AND version = 1")

        assert row_count(db, t, "group_id = 1") == 0
        assert row_count(db, t, "group_id = 2") == 5
        assert row_count(db, t, "group_id = 3") == 5

    def test_cascade_delete_per_group_independence(self, db: psycopg.Connection, xpatch_table):
        """Cascade delete in group 2 doesn't affect group 1."""
        t = xpatch_table
        for v in range(1, 6):
            insert_rows(db, t, [(1, v, f"g1-v{v}")])
            insert_rows(db, t, [(2, v, f"g2-v{v}")])

        db.execute(f"DELETE FROM {t} WHERE group_id = 2 AND version = 3")

        # Group 1 fully intact
        g1 = db.execute(
            f"SELECT version, content FROM {t} WHERE group_id = 1 ORDER BY version"
        ).fetchall()
        assert len(g1) == 5
        for row in g1:
            assert row["content"] == f"g1-v{row['version']}"

        # Group 2 has only v1, v2
        g2 = db.execute(
            f"SELECT version FROM {t} WHERE group_id = 2 ORDER BY version"
        ).fetchall()
        assert [r["version"] for r in g2] == [1, 2]


class TestDeleteEdgeCases:
    """Edge cases for DELETE."""

    def test_delete_single_row_group(self, db: psycopg.Connection, xpatch_table):
        """Delete the only row in a group."""
        t = xpatch_table
        insert_rows(db, t, [(1, 1, "only")])
        db.execute(f"DELETE FROM {t} WHERE group_id = 1 AND version = 1")
        assert row_count(db, t) == 0

    def test_delete_nonexistent_row(self, db: psycopg.Connection, xpatch_table):
        """Deleting a row that doesn't exist has no effect."""
        t = xpatch_table
        insert_versions(db, t, group_id=1, count=3)
        # Delete a version that doesn't exist
        db.execute(f"DELETE FROM {t} WHERE group_id = 1 AND version = 999")
        assert row_count(db, t) == 3

    def test_delete_from_empty_table(self, db: psycopg.Connection, xpatch_table):
        """DELETE from an empty table is a no-op."""
        t = xpatch_table
        db.execute(f"DELETE FROM {t} WHERE group_id = 1")
        assert row_count(db, t) == 0

    def test_delete_by_content_filter(self, db: psycopg.Connection, xpatch_table):
        """DELETE with WHERE on delta column (content).

        Non-trivial: the scan must reconstruct delta-compressed content
        before evaluating the WHERE predicate. The matched row's TID is
        then passed to xpatch_tuple_delete for cascade.
        """
        t = xpatch_table
        insert_rows(db, t, [
            (1, 1, "keep"),
            (1, 2, "remove_me"),
            (1, 3, "also keep"),
        ])
        # This deletes the row with content='remove_me', which cascades to v3
        db.execute(f"DELETE FROM {t} WHERE content = 'remove_me'")
        remaining = db.execute(
            f"SELECT version, content FROM {t} ORDER BY version"
        ).fetchall()
        # v2 deleted => cascades to v3, only v1 remains
        assert len(remaining) == 1
        assert remaining[0]["version"] == 1
        assert remaining[0]["content"] == "keep"

    def test_delete_all_rows_no_where(self, db: psycopg.Connection, xpatch_table):
        """DELETE without WHERE removes all rows from the table."""
        t = xpatch_table
        for g in range(1, 4):
            insert_versions(db, t, group_id=g, count=3)
        assert row_count(db, t) == 9

        db.execute(f"DELETE FROM {t}")
        assert row_count(db, t) == 0

    def test_delete_whole_group_by_group_id(self, db: psycopg.Connection, xpatch_table):
        """DELETE WHERE group_id = X removes all versions in the group."""
        t = xpatch_table
        insert_versions(db, t, group_id=1, count=5)
        insert_versions(db, t, group_id=2, count=3)

        db.execute(f"DELETE FROM {t} WHERE group_id = 1")
        assert row_count(db, t, "group_id = 1") == 0
        assert row_count(db, t, "group_id = 2") == 3

    def test_xp_seq_restarts_after_full_delete(self, db: psycopg.Connection, xpatch_table):
        """_xp_seq restarts from 1 after full group deletion and re-insert."""
        t = xpatch_table
        insert_versions(db, t, group_id=1, count=3)
        db.execute(f"DELETE FROM {t} WHERE group_id = 1 AND version = 1")
        assert row_count(db, t) == 0

        insert_rows(db, t, [(1, 1, "new-v1")])
        row = db.execute(f"SELECT _xp_seq FROM {t} WHERE group_id = 1").fetchone()
        assert row["_xp_seq"] == 1

    def test_multiple_sequential_deletes(self, db: psycopg.Connection, xpatch_table):
        """Multiple DELETEs in sequence across groups work correctly."""
        t = xpatch_table
        insert_versions(db, t, group_id=1, count=5)
        insert_versions(db, t, group_id=2, count=5)

        # Delete tail of group 1
        db.execute(f"DELETE FROM {t} WHERE group_id = 1 AND version = 4")
        assert row_count(db, t, "group_id = 1") == 3

        # Delete tail of group 2
        db.execute(f"DELETE FROM {t} WHERE group_id = 2 AND version = 3")
        assert row_count(db, t, "group_id = 2") == 2

        # Delete more from group 1
        db.execute(f"DELETE FROM {t} WHERE group_id = 1 AND version = 2")
        assert row_count(db, t, "group_id = 1") == 1
        assert row_count(db, t, "group_id = 2") == 2

    def test_delete_from_empty_table_no_where(self, db: psycopg.Connection, xpatch_table):
        """DELETE without WHERE from an empty table is a no-op."""
        t = xpatch_table
        db.execute(f"DELETE FROM {t}")
        assert row_count(db, t) == 0

    def test_insert_after_partial_delete_chain_integrity(
        self, db: psycopg.Connection, xpatch_table
    ):
        """After cascade delete, new inserts form a valid delta chain."""
        t = xpatch_table
        for v in range(1, 6):
            insert_rows(db, t, [(1, v, f"original-{v}")])

        # Cascade delete v3-v5
        db.execute(f"DELETE FROM {t} WHERE group_id = 1 AND version = 3")
        assert row_count(db, t) == 2

        # Insert new versions — should form a valid chain with v1, v2
        for v in range(3, 6):
            insert_rows(db, t, [(1, v, f"new-{v}")])

        rows = db.execute(
            f"SELECT version, content FROM {t} ORDER BY version"
        ).fetchall()
        assert len(rows) == 5
        assert rows[0]["content"] == "original-1"
        assert rows[1]["content"] == "original-2"
        assert rows[2]["content"] == "new-3"
        assert rows[3]["content"] == "new-4"
        assert rows[4]["content"] == "new-5"
