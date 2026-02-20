"""
Tests for deadlock risks in the DELETE path.

Finding 1 (CRITICAL): The DELETE path acquires an advisory lock on the group
(xpatch_tam.c:1351), then calls refresh_groups which does SPI (SQL UPSERTs
to xpatch.group_stats) AND calls xpatch_reconstruct_column (index scans,
buffer locks) — all while holding the advisory lock.

A concurrent INSERT to the SAME group will try to acquire the same advisory
lock and block.  But a concurrent INSERT to a DIFFERENT group acquires a
different advisory lock — and if both then do SPI to the same group_stats
table, they can deadlock on row-level locks within group_stats.

Finding 3 (HIGH): refresh_groups in full-scan mode acquires advisory locks
for each group in page order.  Two concurrent full refreshes could acquire
advisory locks in different orders, deadlocking.

These tests attempt to trigger these deadlocks under controlled conditions.
A deadlock manifests as either:
- PostgreSQL's deadlock detector raises "deadlock detected" error
- A timeout (statement_timeout) expires
"""

import threading
import time
import psycopg
import pytest


class TestDeleteInsertConcurrency:
    """Test concurrent DELETE and INSERT on the same/different groups."""

    def test_delete_and_insert_same_group_serialized(self, db, db_factory, make_table):
        """
        DELETE and INSERT on the same group should serialize on the advisory
        lock (not deadlock).  The INSERT waits for DELETE to finish.
        """
        tbl = make_table(compress_depth=3)

        # Insert 10 versions for group 1
        for v in range(1, 11):
            db.execute(
                f"INSERT INTO {tbl} (group_id, version, content) VALUES (1, %s, %s)",
                (v, f"content_v{v}"),
            )

        errors = []
        results = {}

        def do_delete():
            try:
                conn = db_factory()
                # Use a longer timeout to allow for serialization
                conn.execute("SET statement_timeout = '10s'")
                conn.autocommit = False
                conn.execute(
                    f"DELETE FROM {tbl} WHERE group_id = 1 AND version = 10"
                )
                time.sleep(0.5)  # Hold the advisory lock briefly
                conn.commit()
                results["delete"] = "ok"
                conn.close()
            except Exception as e:
                errors.append(("delete", e))

        def do_insert():
            try:
                conn = db_factory()
                conn.execute("SET statement_timeout = '10s'")
                conn.autocommit = False
                time.sleep(0.1)  # Let DELETE acquire lock first
                conn.execute(
                    f"INSERT INTO {tbl} (group_id, version, content) VALUES (1, 11, 'new_v11')"
                )
                conn.commit()
                results["insert"] = "ok"
                conn.close()
            except Exception as e:
                errors.append(("insert", e))

        t1 = threading.Thread(target=do_delete)
        t2 = threading.Thread(target=do_insert)
        t1.start()
        t2.start()
        t1.join(timeout=15)
        t2.join(timeout=15)

        assert not t1.is_alive(), "DELETE thread hung"
        assert not t2.is_alive(), "INSERT thread hung"

        # Check for deadlock errors
        deadlocks = [e for op, e in errors if "deadlock" in str(e).lower()]
        assert not deadlocks, f"DEADLOCK detected: {deadlocks}"

        # At least one should succeed; both might succeed if serialized properly
        assert not errors or len(errors) < 2, f"Both operations failed: {errors}"

    def test_concurrent_deletes_different_groups(self, db, db_factory, make_table):
        """
        Concurrent DELETEs on different groups should not deadlock.
        Each acquires its own advisory lock.  The risk is that refresh_groups
        (called from DELETE) does SPI to group_stats, and two concurrent
        DELETEs both modify group_stats rows, potentially deadlocking on
        the index or row-level locks.
        """
        tbl = make_table(compress_depth=3)

        # Insert versions for groups 1 and 2
        for g in [1, 2]:
            for v in range(1, 11):
                db.execute(
                    f"INSERT INTO {tbl} (group_id, version, content) VALUES (%s, %s, %s)",
                    (g, v, f"g{g}_v{v}_content"),
                )

        errors = []

        def delete_from_group(group_id):
            try:
                conn = db_factory()
                conn.execute("SET statement_timeout = '10s'")
                conn.autocommit = False
                conn.execute(
                    f"DELETE FROM {tbl} WHERE group_id = %s AND version = 10",
                    (group_id,),
                )
                conn.commit()
                conn.close()
            except Exception as e:
                errors.append((group_id, e))

        t1 = threading.Thread(target=delete_from_group, args=(1,))
        t2 = threading.Thread(target=delete_from_group, args=(2,))
        t1.start()
        t2.start()
        t1.join(timeout=15)
        t2.join(timeout=15)

        assert not t1.is_alive(), "DELETE group 1 hung"
        assert not t2.is_alive(), "DELETE group 2 hung"

        deadlocks = [e for gid, e in errors if "deadlock" in str(e).lower()]
        assert not deadlocks, f"DEADLOCK between deletes on different groups: {deadlocks}"
        assert not errors, f"Errors during concurrent deletes: {errors}"

        # Verify data integrity
        for g in [1, 2]:
            row = db.execute(
                f"SELECT COUNT(*) AS cnt FROM {tbl} WHERE group_id = %s", (g,)
            ).fetchone()
            assert row["cnt"] == 9, f"Group {g}: expected 9 rows after delete, got {row['cnt']}"

    def test_concurrent_delete_and_insert_different_groups(self, db, db_factory, make_table):
        """
        DELETE on group 1 + INSERT on group 2 concurrently.
        Different advisory locks, but both do SPI to group_stats.
        
        DELETE path: advisory lock(group1) → refresh_groups → SPI(group_stats)
        INSERT path: advisory lock(group2) → stats_cache_update_group → 
                     (batched, so no SPI during insert, only at commit)
        
        With the stats batching fix, the INSERT's SPI happens at commit time
        (in the xact callback), not during the insert itself.  So the window
        for deadlock is smaller but still exists: DELETE holds advisory(group1)
        + SPI modifies group_stats while INSERT commits and flushes stats
        to group_stats.
        """
        tbl = make_table(compress_depth=3)

        # Setup: group 1 has 10 versions, group 2 has 5
        for v in range(1, 11):
            db.execute(
                f"INSERT INTO {tbl} (group_id, version, content) VALUES (1, %s, %s)",
                (v, f"g1_v{v}"),
            )
        for v in range(1, 6):
            db.execute(
                f"INSERT INTO {tbl} (group_id, version, content) VALUES (2, %s, %s)",
                (v, f"g2_v{v}"),
            )

        errors = []

        def do_delete_g1():
            try:
                conn = db_factory()
                conn.execute("SET statement_timeout = '10s'")
                conn.autocommit = False
                conn.execute(f"DELETE FROM {tbl} WHERE group_id = 1 AND version = 10")
                conn.commit()
                conn.close()
            except Exception as e:
                errors.append(("delete_g1", e))

        def do_insert_g2():
            try:
                conn = db_factory()
                conn.execute("SET statement_timeout = '10s'")
                conn.autocommit = False
                for v in range(6, 16):
                    conn.execute(
                        f"INSERT INTO {tbl} (group_id, version, content) VALUES (2, %s, %s)",
                        (v, f"g2_v{v}_new"),
                    )
                conn.commit()
                conn.close()
            except Exception as e:
                errors.append(("insert_g2", e))

        t1 = threading.Thread(target=do_delete_g1)
        t2 = threading.Thread(target=do_insert_g2)
        t1.start()
        t2.start()
        t1.join(timeout=15)
        t2.join(timeout=15)

        assert not t1.is_alive(), "DELETE thread hung"
        assert not t2.is_alive(), "INSERT thread hung"

        deadlocks = [e for op, e in errors if "deadlock" in str(e).lower()]
        assert not deadlocks, f"DEADLOCK between delete and insert: {deadlocks}"
        assert not errors, f"Errors: {errors}"

    def test_many_concurrent_deletes(self, db, db_factory, make_table):
        """
        8 concurrent DELETEs, each on a different group.
        This maximizes the chance of deadlock via group_stats contention.
        """
        tbl = make_table(compress_depth=3)
        n_groups = 8

        # Setup: each group has 10 versions
        for g in range(1, n_groups + 1):
            for v in range(1, 11):
                db.execute(
                    f"INSERT INTO {tbl} (group_id, version, content) VALUES (%s, %s, %s)",
                    (g, v, f"g{g}_v{v}_" + "x" * 50),
                )

        errors = []

        def delete_last(gid):
            try:
                conn = db_factory()
                conn.execute("SET statement_timeout = '15s'")
                conn.autocommit = False
                conn.execute(
                    f"DELETE FROM {tbl} WHERE group_id = %s AND version = 10",
                    (gid,),
                )
                conn.commit()
                conn.close()
            except Exception as e:
                errors.append((gid, e))

        threads = [threading.Thread(target=delete_last, args=(g,)) for g in range(1, n_groups + 1)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=20)

        alive = [i + 1 for i, t in enumerate(threads) if t.is_alive()]
        assert not alive, f"Threads hung for groups: {alive}"

        deadlocks = [e for gid, e in errors if "deadlock" in str(e).lower()]
        assert not deadlocks, f"DEADLOCK during concurrent deletes: {deadlocks}"
        assert not errors, f"Errors during concurrent deletes: {errors}"

        # Verify: each group should have 9 rows
        for g in range(1, n_groups + 1):
            row = db.execute(
                f"SELECT COUNT(*) AS cnt FROM {tbl} WHERE group_id = %s", (g,)
            ).fetchone()
            assert row["cnt"] == 9, f"Group {g}: expected 9, got {row['cnt']}"


class TestDeleteInsertInterleavedStress:
    """Higher-stress tests with mixed operations."""

    @pytest.mark.stress
    def test_interleaved_insert_delete_10_groups(self, db, db_factory, make_table):
        """
        10 threads each: insert 5 rows, then delete the last one, repeat 3 times.
        Each thread operates on its own group.
        
        This exercises the advisory lock → SPI path repeatedly under concurrency.
        """
        tbl = make_table(compress_depth=3)
        n_groups = 10
        errors = []

        def worker(gid):
            try:
                conn = db_factory()
                conn.execute("SET statement_timeout = '20s'")
                version = 1
                for cycle in range(3):
                    # Insert 5 rows
                    conn.autocommit = False
                    for _ in range(5):
                        conn.execute(
                            f"INSERT INTO {tbl} (group_id, version, content) VALUES (%s, %s, %s)",
                            (gid, version, f"g{gid}_v{version}_c{cycle}"),
                        )
                        version += 1
                    conn.commit()

                    # Delete the last one
                    conn.autocommit = False
                    conn.execute(
                        f"DELETE FROM {tbl} WHERE group_id = %s AND version = %s",
                        (gid, version - 1),
                    )
                    conn.commit()
                    version -= 1  # seq was rolled back by delete

                conn.close()
            except Exception as e:
                errors.append((gid, e))

        threads = [threading.Thread(target=worker, args=(g,)) for g in range(1, n_groups + 1)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        alive = [i + 1 for i, t in enumerate(threads) if t.is_alive()]
        assert not alive, f"Threads hung for groups: {alive}"

        deadlocks = [e for gid, e in errors if "deadlock" in str(e).lower()]
        assert not deadlocks, f"DEADLOCK during interleaved ops: {deadlocks}"

        timeouts = [e for gid, e in errors if "timeout" in str(e).lower()]
        assert not timeouts, f"TIMEOUT (possible deadlock): {timeouts}"

        # Allow non-deadlock errors (e.g., serialization failures) but report them
        if errors:
            non_deadlock = [(gid, e) for gid, e in errors
                           if "deadlock" not in str(e).lower() and "timeout" not in str(e).lower()]
            if non_deadlock:
                pytest.fail(f"Non-deadlock errors: {non_deadlock}")


class TestRefreshStatsDeadlock:
    """Test concurrent refresh_stats calls (Finding 3)."""

    def test_concurrent_refresh_stats_no_deadlock(self, db, db_factory, make_table):
        """
        Two concurrent xpatch.refresh_stats() calls on the same table.
        Full refresh acquires advisory locks for ALL groups in page scan order.
        If page order differs between the two calls (due to concurrent
        modifications), they could deadlock on advisory locks.
        """
        tbl = make_table(compress_depth=3)

        # Create 20 groups with 5 versions each
        for g in range(1, 21):
            for v in range(1, 6):
                db.execute(
                    f"INSERT INTO {tbl} (group_id, version, content) VALUES (%s, %s, %s)",
                    (g, v, f"g{g}_v{v}_" + "y" * 100),
                )

        errors = []

        def do_refresh():
            try:
                conn = db_factory()
                conn.execute("SET statement_timeout = '15s'")
                conn.execute(f"SELECT xpatch.refresh_stats('{tbl}')")
                conn.close()
            except Exception as e:
                errors.append(e)

        t1 = threading.Thread(target=do_refresh)
        t2 = threading.Thread(target=do_refresh)
        t1.start()
        t2.start()
        t1.join(timeout=20)
        t2.join(timeout=20)

        assert not t1.is_alive(), "Refresh 1 hung"
        assert not t2.is_alive(), "Refresh 2 hung"

        deadlocks = [e for e in errors if "deadlock" in str(e).lower()]
        assert not deadlocks, f"DEADLOCK during concurrent refresh: {deadlocks}"

    def test_refresh_during_insert_no_deadlock(self, db, db_factory, make_table):
        """
        refresh_stats() while concurrent INSERTs are happening.
        refresh acquires advisory locks for all groups;
        INSERT acquires advisory lock for one group.
        If refresh encounters the INSERT's group, it waits on the advisory lock.
        The INSERT's commit flushes stats via SPI.  Meanwhile refresh is also
        doing SPI to group_stats.  Potential for cross-lock deadlock.
        """
        tbl = make_table(compress_depth=3)

        # Setup some initial data
        for g in range(1, 6):
            for v in range(1, 6):
                db.execute(
                    f"INSERT INTO {tbl} (group_id, version, content) VALUES (%s, %s, %s)",
                    (g, v, f"g{g}_v{v}"),
                )

        errors = []

        def do_inserts():
            try:
                conn = db_factory()
                conn.execute("SET statement_timeout = '15s'")
                for g in range(1, 6):
                    conn.autocommit = False
                    for v in range(6, 11):
                        conn.execute(
                            f"INSERT INTO {tbl} (group_id, version, content) VALUES (%s, %s, %s)",
                            (g, v, f"g{g}_v{v}_new"),
                        )
                    conn.commit()
                conn.close()
            except Exception as e:
                errors.append(("insert", e))

        def do_refresh():
            try:
                conn = db_factory()
                conn.execute("SET statement_timeout = '15s'")
                time.sleep(0.1)  # Let inserts start first
                conn.execute(f"SELECT xpatch.refresh_stats('{tbl}')")
                conn.close()
            except Exception as e:
                errors.append(("refresh", e))

        t1 = threading.Thread(target=do_inserts)
        t2 = threading.Thread(target=do_refresh)
        t1.start()
        t2.start()
        t1.join(timeout=20)
        t2.join(timeout=20)

        assert not t1.is_alive(), "Insert thread hung"
        assert not t2.is_alive(), "Refresh thread hung"

        deadlocks = [e for op, e in errors if "deadlock" in str(e).lower()]
        assert not deadlocks, f"DEADLOCK during refresh+insert: {deadlocks}"

        timeouts = [e for op, e in errors if "timeout" in str(e).lower()]
        assert not timeouts, f"TIMEOUT (possible deadlock): {timeouts}"
