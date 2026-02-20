"""
Tests for bugs found in the deep code audit.

Bug 5 (CRITICAL): Use-after-free of group_value Datum in DELETE path.
    heap_getattr returns a pointer into the buffer page for pass-by-ref types.
    The buffer is released before the Datum is used for hash computation,
    group matching in the delete loop, seq cache update, and stats refresh.

Bug 1 (CRITICAL): Custom xpatch_tuple_is_visible() ignores the snapshot.
    It uses raw TransactionIdDidCommit() instead of checking against the
    snapshot's xmin/xmax/xcip_list.  Under REPEATABLE READ, tuples committed
    after the snapshot should NOT be visible but are returned anyway.

Bug 2 (CRITICAL): Hint bit mutation under BUFFER_LOCK_SHARE.
    xpatch_tuple_is_visible writes t_infomask |= HEAP_XMAX_INVALID on a
    shared-locked buffer page.  This is a data race with concurrent readers.

Bug 8 (HIGH): xpatch_get_max_seq counts same-transaction deleted tuples.
    If the seq cache is evicted between a DELETE and a subsequent INSERT in
    the same transaction, the scan fallback sees deleted tuples as visible,
    causing sequence gaps.
"""

import threading
import time
import psycopg
import pytest


class TestDeleteWithTextGroupBy:
    """
    Bug 5: DELETE with pass-by-reference group_by column (TEXT).

    heap_getattr returns a pointer INTO the buffer page for TEXT.
    After ReleaseBuffer, that pointer is dangling.  The DELETE path
    then uses it for hash computation, datum comparison in the cascade
    loop, seq cache update, and stats refresh.

    With INT group_by columns this is safe (pass-by-value Datum).
    With TEXT/VARCHAR/BYTEA it's use-after-free.
    """

    def test_delete_text_group_single(self, db):
        """Delete a single row from a table with TEXT group_by column."""
        db.execute("""
            CREATE TABLE xp_del_text (
                grp TEXT NOT NULL,
                version INT NOT NULL,
                content TEXT NOT NULL
            ) USING xpatch
        """)
        db.execute("""
            SELECT xpatch.configure('xp_del_text',
                group_by => 'grp',
                order_by => 'version',
                delta_columns => ARRAY['content'])
        """)

        # Insert several versions
        for v in range(1, 11):
            db.execute(
                "INSERT INTO xp_del_text (grp, version, content) VALUES (%s, %s, %s)",
                ("my_group", v, f"content_v{v}"),
            )

        # Delete the last version — this triggers the use-after-free path
        db.execute(
            "DELETE FROM xp_del_text WHERE grp = 'my_group' AND version = 10"
        )

        # Verify remaining rows
        row = db.execute(
            "SELECT COUNT(*) AS cnt FROM xp_del_text WHERE grp = 'my_group'"
        ).fetchone()
        assert row["cnt"] == 9

        # Verify data integrity of remaining rows
        for v in range(1, 10):
            row = db.execute(
                "SELECT content FROM xp_del_text WHERE grp = 'my_group' AND version = %s",
                (v,),
            ).fetchone()
            assert row["content"] == f"content_v{v}", f"Version {v} corrupt after delete"

    def test_delete_long_text_group(self, db):
        """
        Use a long text group value that would be TOASTed.
        This makes the use-after-free more likely to manifest because
        the detoasted data lives in a temporary buffer that gets freed.
        """
        db.execute("""
            CREATE TABLE xp_del_longgrp (
                grp TEXT NOT NULL,
                version INT NOT NULL,
                content TEXT NOT NULL
            ) USING xpatch
        """)
        db.execute("""
            SELECT xpatch.configure('xp_del_longgrp',
                group_by => 'grp',
                order_by => 'version',
                delta_columns => ARRAY['content'])
        """)

        long_group = "G" * 500  # Long enough to exercise varlena paths
        for v in range(1, 6):
            db.execute(
                "INSERT INTO xp_del_longgrp (grp, version, content) VALUES (%s, %s, %s)",
                (long_group, v, f"content_v{v}"),
            )

        db.execute(
            "DELETE FROM xp_del_longgrp WHERE grp = %s AND version = 5",
            (long_group,),
        )

        row = db.execute(
            "SELECT COUNT(*) AS cnt FROM xp_del_longgrp WHERE grp = %s",
            (long_group,),
        ).fetchone()
        assert row["cnt"] == 4

    def test_delete_varchar_group(self, db):
        """Same bug with VARCHAR group_by column."""
        db.execute("""
            CREATE TABLE xp_del_varchar (
                grp VARCHAR(100) NOT NULL,
                version INT NOT NULL,
                content TEXT NOT NULL
            ) USING xpatch
        """)
        db.execute("""
            SELECT xpatch.configure('xp_del_varchar',
                group_by => 'grp',
                order_by => 'version',
                delta_columns => ARRAY['content'])
        """)

        for v in range(1, 11):
            db.execute(
                "INSERT INTO xp_del_varchar (grp, version, content) VALUES (%s, %s, %s)",
                ("test_group", v, f"content_v{v}"),
            )

        db.execute(
            "DELETE FROM xp_del_varchar WHERE grp = 'test_group' AND version = 10"
        )

        # Verify cascade delete didn't corrupt other groups
        row = db.execute(
            "SELECT COUNT(*) AS cnt FROM xp_del_varchar"
        ).fetchone()
        assert row["cnt"] == 9

    def test_delete_multiple_text_groups(self, db):
        """
        Delete from multiple TEXT groups — each delete does use-after-free.
        The buffer page reuse between deletes makes corruption more likely.
        """
        db.execute("""
            CREATE TABLE xp_del_multi_text (
                grp TEXT NOT NULL,
                version INT NOT NULL,
                content TEXT NOT NULL
            ) USING xpatch
        """)
        db.execute("""
            SELECT xpatch.configure('xp_del_multi_text',
                group_by => 'grp',
                order_by => 'version',
                delta_columns => ARRAY['content'])
        """)

        groups = [f"group_{i}" for i in range(10)]
        for g in groups:
            for v in range(1, 6):
                db.execute(
                    "INSERT INTO xp_del_multi_text (grp, version, content) VALUES (%s, %s, %s)",
                    (g, v, f"{g}_v{v}"),
                )

        # Delete last version from each group
        for g in groups:
            db.execute(
                "DELETE FROM xp_del_multi_text WHERE grp = %s AND version = 5",
                (g,),
            )

        # Verify all remaining data
        for g in groups:
            row = db.execute(
                "SELECT COUNT(*) AS cnt FROM xp_del_multi_text WHERE grp = %s",
                (g,),
            ).fetchone()
            assert row["cnt"] == 4, f"Group {g}: expected 4, got {row['cnt']}"

            for v in range(1, 5):
                row = db.execute(
                    "SELECT content FROM xp_del_multi_text WHERE grp = %s AND version = %s",
                    (g, v),
                ).fetchone()
                assert row["content"] == f"{g}_v{v}", f"Corrupt: {g} v{v}"

    def test_concurrent_delete_text_groups(self, db, db_factory):
        """
        Concurrent DELETEs on different TEXT groups.
        Concurrent buffer access makes use-after-free more likely to
        manifest because the buffer pool page gets reused faster.
        """
        db.execute("""
            CREATE TABLE xp_del_conc_text (
                grp TEXT NOT NULL,
                version INT NOT NULL,
                content TEXT NOT NULL
            ) USING xpatch
        """)
        db.execute("""
            SELECT xpatch.configure('xp_del_conc_text',
                group_by => 'grp',
                order_by => 'version',
                delta_columns => ARRAY['content'])
        """)

        for g in range(1, 9):
            for v in range(1, 11):
                db.execute(
                    "INSERT INTO xp_del_conc_text (grp, version, content) VALUES (%s, %s, %s)",
                    (f"group_{g}", v, f"g{g}_v{v}_" + "x" * 100),
                )

        errors = []

        def delete_group(gid):
            try:
                conn = db_factory()
                conn.execute("SET statement_timeout = '10s'")
                conn.execute(
                    "DELETE FROM xp_del_conc_text WHERE grp = %s AND version = 10",
                    (f"group_{gid}",),
                )
                conn.close()
            except Exception as e:
                errors.append((gid, e))

        threads = [threading.Thread(target=delete_group, args=(g,)) for g in range(1, 9)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=15)

        alive = [i + 1 for i, t in enumerate(threads) if t.is_alive()]
        assert not alive, f"Threads hung: {alive}"
        assert not errors, f"Errors: {errors}"

        # Verify integrity
        for g in range(1, 9):
            row = db.execute(
                "SELECT COUNT(*) AS cnt FROM xp_del_conc_text WHERE grp = %s",
                (f"group_{g}",),
            ).fetchone()
            assert row["cnt"] == 9, f"Group {g}: expected 9, got {row['cnt']}"


class TestRepeatableReadVisibility:
    """
    Bug 1: xpatch_tuple_is_visible ignores the snapshot.

    Under REPEATABLE READ, a tuple committed after the snapshot should
    not be visible.  The custom visibility function uses raw
    TransactionIdDidCommit() which returns true regardless of when
    the commit happened relative to the snapshot.
    """

    def test_repeatable_read_doesnt_see_later_inserts(self, db, db_factory, make_table):
        """
        Session A: BEGIN ISOLATION LEVEL REPEATABLE READ, count rows.
        Session B: INSERT a new row, COMMIT.
        Session A: Count rows again — should see the SAME count.
        """
        tbl = make_table()

        # Insert initial data
        for v in range(1, 6):
            db.execute(
                f"INSERT INTO {tbl} (group_id, version, content) VALUES (1, %s, %s)",
                (v, f"v{v}"),
            )

        # Session A: start REPEATABLE READ
        conn_a = db_factory()
        conn_a.autocommit = False
        conn_a.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        row = conn_a.execute(f"SELECT COUNT(*) AS cnt FROM {tbl}").fetchone()
        count_before = row["cnt"]
        assert count_before == 5

        # Session B: insert a new row and commit
        conn_b = db_factory()
        conn_b.execute(
            f"INSERT INTO {tbl} (group_id, version, content) VALUES (1, 6, 'v6_new')"
        )
        conn_b.close()

        # Session A: count again — should still be 5 under REPEATABLE READ
        row = conn_a.execute(f"SELECT COUNT(*) AS cnt FROM {tbl}").fetchone()
        count_after = row["cnt"]

        conn_a.rollback()
        conn_a.close()

        assert count_after == count_before, (
            f"REPEATABLE READ violation: saw {count_after} rows after concurrent insert "
            f"(expected {count_before}). Custom MVCC check ignores snapshot."
        )

    def test_repeatable_read_doesnt_see_later_deletes(self, db, db_factory, make_table):
        """
        Session A: BEGIN REPEATABLE READ, count rows.
        Session B: DELETE a row, COMMIT.
        Session A: Count rows again — should see the SAME count.
        """
        tbl = make_table()

        for v in range(1, 6):
            db.execute(
                f"INSERT INTO {tbl} (group_id, version, content) VALUES (1, %s, %s)",
                (v, f"v{v}"),
            )

        conn_a = db_factory()
        conn_a.autocommit = False
        conn_a.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        row = conn_a.execute(f"SELECT COUNT(*) AS cnt FROM {tbl}").fetchone()
        count_before = row["cnt"]
        assert count_before == 5

        # Session B: delete and commit
        conn_b = db_factory()
        conn_b.execute(
            f"DELETE FROM {tbl} WHERE group_id = 1 AND version = 5"
        )
        conn_b.close()

        # Session A: should still see 5
        row = conn_a.execute(f"SELECT COUNT(*) AS cnt FROM {tbl}").fetchone()
        count_after = row["cnt"]

        conn_a.rollback()
        conn_a.close()

        assert count_after == count_before, (
            f"REPEATABLE READ violation: saw {count_after} rows after concurrent delete "
            f"(expected {count_before}). Custom MVCC check ignores snapshot."
        )

    def test_read_committed_sees_committed_inserts(self, db, db_factory, make_table):
        """
        Sanity check: under READ COMMITTED, a new statement should see
        rows committed by other transactions.
        """
        tbl = make_table()

        for v in range(1, 6):
            db.execute(
                f"INSERT INTO {tbl} (group_id, version, content) VALUES (1, %s, %s)",
                (v, f"v{v}"),
            )

        # Session A: READ COMMITTED (default), first count
        conn_a = db_factory()
        conn_a.autocommit = False
        row = conn_a.execute(f"SELECT COUNT(*) AS cnt FROM {tbl}").fetchone()
        assert row["cnt"] == 5

        # Session B: insert and commit
        conn_b = db_factory()
        conn_b.execute(
            f"INSERT INTO {tbl} (group_id, version, content) VALUES (1, 6, 'v6')"
        )
        conn_b.close()

        # Session A: new statement should see the new row under READ COMMITTED
        row = conn_a.execute(f"SELECT COUNT(*) AS cnt FROM {tbl}").fetchone()

        conn_a.rollback()
        conn_a.close()

        # Under READ COMMITTED, new statement should see committed data
        assert row["cnt"] == 6, (
            f"READ COMMITTED should see committed insert: got {row['cnt']}, expected 6"
        )


class TestSameTransactionDeleteInsert:
    """
    Bug 8: xpatch_get_max_seq counts same-transaction deleted tuples.

    If the seq cache is evicted between DELETE and INSERT in the same
    transaction, get_max_seq falls back to a scan that sees the deleted
    tuples (because TransactionIdDidCommit(xmax) returns false for
    the current transaction's deletes).  This creates sequence gaps.

    Hard to trigger reliably because it requires cache eviction.
    We test the normal path (cache hit) for correctness, and attempt
    to stress cache eviction.
    """

    def test_delete_then_insert_same_transaction(self, db, make_table):
        """
        Delete last version, then insert a new one, in the same transaction.
        The new row should get the correct next sequence number.
        """
        tbl = make_table(compress_depth=3)

        for v in range(1, 6):
            db.execute(
                f"INSERT INTO {tbl} (group_id, version, content) VALUES (1, %s, %s)",
                (v, f"v{v}"),
            )

        # Same-transaction delete + insert
        db.autocommit = False
        db.execute(f"DELETE FROM {tbl} WHERE group_id = 1 AND version = 5")
        db.execute(
            f"INSERT INTO {tbl} (group_id, version, content) VALUES (1, 5, 'v5_replaced')"
        )
        db.commit()
        db.autocommit = True

        # Verify: should have 5 rows with correct content
        row = db.execute(f"SELECT COUNT(*) AS cnt FROM {tbl}").fetchone()
        assert row["cnt"] == 5

        row = db.execute(
            f"SELECT content FROM {tbl} WHERE group_id = 1 AND version = 5"
        ).fetchone()
        assert row["content"] == "v5_replaced"

    def test_delete_then_insert_many_groups_cache_pressure(self, db, make_table):
        """
        Delete + insert across many groups to try to pressure the seq cache
        into evicting entries.  If Bug 8 triggers, we get sequence gaps.
        """
        tbl = make_table(compress_depth=3)
        n_groups = 50

        # Setup
        for g in range(1, n_groups + 1):
            for v in range(1, 6):
                db.execute(
                    f"INSERT INTO {tbl} (group_id, version, content) VALUES (%s, %s, %s)",
                    (g, v, f"g{g}_v{v}"),
                )

        # Delete last version from each group, then re-insert
        db.autocommit = False
        for g in range(1, n_groups + 1):
            db.execute(
                f"DELETE FROM {tbl} WHERE group_id = %s AND version = 5", (g,)
            )
        for g in range(1, n_groups + 1):
            db.execute(
                f"INSERT INTO {tbl} (group_id, version, content) VALUES (%s, 5, %s)",
                (g, f"g{g}_v5_new"),
            )
        db.commit()
        db.autocommit = True

        # Verify all groups have correct data
        for g in range(1, n_groups + 1):
            row = db.execute(
                f"SELECT COUNT(*) AS cnt FROM {tbl} WHERE group_id = %s", (g,)
            ).fetchone()
            assert row["cnt"] == 5, f"Group {g}: expected 5, got {row['cnt']}"

            row = db.execute(
                f"SELECT content FROM {tbl} WHERE group_id = %s AND version = 5",
                (g,),
            ).fetchone()
            assert row["content"] == f"g{g}_v5_new", f"Group {g} v5 content wrong"
