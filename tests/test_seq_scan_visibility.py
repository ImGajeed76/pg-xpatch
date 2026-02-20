"""
Tests for MVCC visibility in the sequential scan fallback of xpatch_fetch_by_seq.

The sequential scan fallback (xpatch_storage.c, Strategy 3) has no MVCC
visibility check.  It reads any tuple where ItemIdIsNormal(itemId) is true,
including tuples from aborted transactions and deleted-but-not-vacuumed tuples.

The fallback returns the FIRST matching tuple (scanning from block 0 forward).
If a dead/aborted tuple has a matching seq number, it could be returned instead
of the live tuple, causing corrupt reconstruction.

The fallback triggers when:
  1. The seq-to-TID cache has no entry (cold start or eviction)
  2. The _xp_seq index doesn't exist or index scan returns no visible tuple

Test strategy:
  - Create aborted tuples on the heap (rollback after insert)
  - Drop the _xp_seq index to force the sequential scan fallback
  - Evict the TID cache by inserting to many groups
  - Read back data and verify correctness (defense-in-depth)

NOTE: In practice, aborted inserts create seq GAPS (not reuse), so the
aborted tuple's seq number differs from the live tuple's. This means B6
is unlikely to produce corruption in normal operation. These tests serve
as defense-in-depth to ensure the fallback is safe even in edge cases.
"""

import psycopg
import pytest
from psycopg import sql


class TestAbortedTupleVisibility:
    """
    Core B6 tests: aborted tuples with reused seq numbers cause corruption
    when the sequential scan fallback has no visibility check.
    """

    def test_aborted_insert_reuses_seq_corruption(
        self, db: psycopg.Connection, make_table
    ):
        """
        THE DEFINITIVE B6 TEST.

        Scenario:
          1. Insert v1-v5 for group 1 (seq 1=keyframe, 2-5=deltas)
          2. In a NEW connection, begin transaction, insert v6 (gets seq=6),
             then ROLLBACK — aborted tuple with seq=6 remains on-page
          3. seq cache rolls back to max_seq=5 (PG_CATCH in xpatch_tuple_insert)
          4. Insert v6 with CORRECT content (gets seq=6 again — same seq!)
          5. Drop the _xp_seq index (kill Strategy 2)
          6. Insert to 20+ other groups (evict group 1's TID cache entries)
          7. SELECT group 1 — reconstruction of v6 calls xpatch_fetch_by_seq(6)
             which hits Strategy 3 (fallback)
          8. Strategy 3 scans from block 0 and finds the ABORTED tuple first
             (it was written first → earlier page/offset)
          9. The aborted tuple's delta data is decoded against a wrong base
             → produces garbage content or "Error decoding gdelta"

        Expected result BEFORE fix: content of v6 is wrong (aborted data) or
        an error is raised during reconstruction.

        Expected result AFTER fix: Strategy 3 skips the aborted tuple
        (visibility check), finds the live tuple, returns correct content.
        """
        t = make_table(
            "group_id INT, version INT, content TEXT NOT NULL",
            group_by="group_id",
            order_by="version",
            delta_columns=["content"],
            compress_depth=5,
            keyframe_every=100,  # only seq 1 is keyframe, rest are deltas
        )

        # Step 1: Insert v1-v5 with known content
        for v in range(1, 6):
            db.execute(
                sql.SQL(
                    "INSERT INTO {} (group_id, version, content) VALUES (%s, %s, %s)"
                ).format(sql.Identifier(t)),
                (1, v, f"GOOD content version {v} " + "A" * 100),
            )

        # Step 2: In a separate connection, insert v6 then ROLLBACK
        # This creates an aborted tuple with seq=6 on the heap page.
        # The ROLLBACK triggers PG_CATCH which rolls back the seq allocation.
        conninfo = db.info.dsn
        conn2 = psycopg.connect(conninfo, autocommit=False)
        try:
            conn2.execute(
                sql.SQL(
                    "INSERT INTO {} (group_id, version, content) VALUES (%s, %s, %s)"
                ).format(sql.Identifier(t)),
                (1, 6, f"ABORTED content version 6 " + "Z" * 100),
            )
            conn2.rollback()  # Aborted! Tuple stays on page, seq rolled back
        finally:
            conn2.close()

        # Step 3: Insert the CORRECT v6 (gets seq=6 again after rollback)
        db.execute(
            sql.SQL(
                "INSERT INTO {} (group_id, version, content) VALUES (%s, %s, %s)"
            ).format(sql.Identifier(t)),
            (1, 6, f"GOOD content version 6 " + "A" * 100),
        )

        # Step 4: Drop _xp_seq index to kill Strategy 2 (index scan)
        indexes = db.execute(
            """
            SELECT indexname FROM pg_indexes
            WHERE tablename = %s AND indexdef LIKE '%%xp_seq%%'
            """,
            (t,),
        ).fetchall()
        for idx_row in indexes:
            idx_name = idx_row["indexname"]
            db.execute(
                sql.SQL("DROP INDEX IF EXISTS {}").format(sql.Identifier(idx_name))
            )

        # Step 5: Evict group 1's TID cache entries by inserting to many other groups
        # insert_cache_slots defaults to 16, and TID cache is global shared memory.
        # We need enough groups to push group 1 entries out of the cache.
        for g in range(100, 130):
            for v in range(1, 4):
                db.execute(
                    sql.SQL(
                        "INSERT INTO {} (group_id, version, content) VALUES (%s, %s, %s)"
                    ).format(sql.Identifier(t)),
                    (g, v, f"filler group {g} v{v}"),
                )

        # Step 6: READ group 1 — this triggers reconstruction via Strategy 3
        # v6 is a delta (not keyframe). To reconstruct it, the extension
        # calls xpatch_fetch_by_seq(seq=5) for its base. BUT v6 itself was
        # fetched by the scan — the issue is when READING v6, the scan's
        # physical_to_logical → reconstruct_from_delta needs to look up the
        # base. With no index and no TID cache, it falls through to Strategy 3.
        #
        # Actually, the DIRECT issue: two tuples have seq=6 on the heap.
        # When any later delta (if we inserted v7+) needs seq=6 as base,
        # it would fetch the wrong one. Let's also insert v7 to make this
        # scenario explicit.
        db.execute(
            sql.SQL(
                "INSERT INTO {} (group_id, version, content) VALUES (%s, %s, %s)"
            ).format(sql.Identifier(t)),
            (1, 7, f"GOOD content version 7 " + "A" * 100),
        )

        # Now read everything
        rows = db.execute(
            sql.SQL(
                "SELECT version, content FROM {} WHERE group_id = 1 ORDER BY version"
            ).format(sql.Identifier(t))
        ).fetchall()

        assert len(rows) == 7, f"Expected 7 rows, got {len(rows)}"

        for row in rows:
            v = row["version"]
            content = row["content"]
            expected_prefix = f"GOOD content version {v} "
            assert content.startswith(expected_prefix), (
                f"v{v}: expected content starting with '{expected_prefix}', "
                f"got '{content[:60]}'. "
                "Sequential scan fallback likely returned an aborted "
                "tuple, causing corrupt reconstruction."
            )

    def test_aborted_insert_multiple_aborts(
        self, db: psycopg.Connection, make_table
    ):
        """
        Multiple aborted inserts stacking up dead tuples with reused seq numbers.
        Each abort leaves a dead tuple on-page. Strategy 3 scans linearly and
        returns the first match — which will be the FIRST aborted tuple.
        """
        t = make_table(
            "group_id INT, version INT, content TEXT NOT NULL",
            group_by="group_id",
            order_by="version",
            delta_columns=["content"],
            compress_depth=5,
            keyframe_every=100,
        )

        # Insert v1-v3
        for v in range(1, 4):
            db.execute(
                sql.SQL(
                    "INSERT INTO {} (group_id, version, content) VALUES (%s, %s, %s)"
                ).format(sql.Identifier(t)),
                (1, v, f"COMMITTED v{v} " + "C" * 100),
            )

        conninfo = db.info.dsn

        # Abort 3 times — each leaves a dead tuple with seq=4
        for attempt in range(3):
            conn_abort = psycopg.connect(conninfo, autocommit=False)
            try:
                conn_abort.execute(
                    sql.SQL(
                        "INSERT INTO {} (group_id, version, content) VALUES (%s, %s, %s)"
                    ).format(sql.Identifier(t)),
                    (1, 4, f"ABORT attempt {attempt} " + "X" * 100),
                )
                conn_abort.rollback()
            finally:
                conn_abort.close()

        # Now insert the real v4
        db.execute(
            sql.SQL(
                "INSERT INTO {} (group_id, version, content) VALUES (%s, %s, %s)"
            ).format(sql.Identifier(t)),
            (1, 4, f"COMMITTED v4 " + "C" * 100),
        )

        # Also insert v5 so v4 is used as a base during reconstruction
        db.execute(
            sql.SQL(
                "INSERT INTO {} (group_id, version, content) VALUES (%s, %s, %s)"
            ).format(sql.Identifier(t)),
            (1, 5, f"COMMITTED v5 " + "C" * 100),
        )

        # Drop index, evict cache
        indexes = db.execute(
            """
            SELECT indexname FROM pg_indexes
            WHERE tablename = %s AND indexdef LIKE '%%xp_seq%%'
            """,
            (t,),
        ).fetchall()
        for idx_row in indexes:
            db.execute(
                sql.SQL("DROP INDEX IF EXISTS {}").format(
                    sql.Identifier(idx_row["indexname"])
                )
            )

        for g in range(200, 230):
            db.execute(
                sql.SQL(
                    "INSERT INTO {} (group_id, version, content) VALUES (%s, %s, %s)"
                ).format(sql.Identifier(t)),
                (g, 1, f"evict group {g}"),
            )

        # Read back — if Strategy 3 returns an aborted tuple, v4/v5 are corrupt
        rows = db.execute(
            sql.SQL(
                "SELECT version, content FROM {} WHERE group_id = 1 ORDER BY version"
            ).format(sql.Identifier(t))
        ).fetchall()

        assert len(rows) == 5

        for row in rows:
            v = row["version"]
            assert row["content"].startswith(f"COMMITTED v{v} "), (
                f"v{v}: expected 'COMMITTED v{v}' prefix, got '{row['content'][:60]}'. "
                "Aborted tuple used as reconstruction base (missing visibility check)."
            )


class TestDeletedTupleVisibility:
    """
    Deleted-but-not-vacuumed tuples should also be skipped by Strategy 3.
    """

    def test_deleted_tuple_not_used_as_base(
        self, db: psycopg.Connection, make_table
    ):
        """
        1. Insert v1-v5
        2. DELETE v3-v5 (cascade)
        3. DON'T vacuum — dead tuples remain with ItemIdIsNormal
        4. Insert NEW v3-v5 with different content
        5. Drop index, evict cache
        6. Read back — dead v3's delta data must NOT be used

        NOTE: Cascade delete removes v3-v5, so the old seq numbers (3,4,5)
        are still on dead tuples. The new inserts get seq 6,7,8 (not 3,4,5).
        So this test checks a slightly different scenario: new v3-v5 have
        seq 6-8. During reconstruction, v4 (seq=7) needs v3 (seq=6) as base.
        The dead tuples have seq 3,4,5 — different seq numbers. So this test
        does NOT trigger B6 in the same way as aborted inserts.

        HOWEVER, if we delete ONLY v5 and reinsert it:
        - DELETE v5 → dead tuple with seq=5
        - INSERT v5 → new tuple with seq=6
        - The dead seq=5 is never looked up because the new v5 is seq=6
        
        The delete scenario is less dangerous for B6 because deleted rows
        keep their original seq (not reused). The aborted scenario is the
        main trigger because seq IS reused after rollback.
        """
        t = make_table(
            "group_id INT, version INT, content TEXT NOT NULL",
            group_by="group_id",
            order_by="version",
            delta_columns=["content"],
            compress_depth=5,
            keyframe_every=100,
        )

        # Insert v1-v5
        for v in range(1, 6):
            db.execute(
                sql.SQL(
                    "INSERT INTO {} (group_id, version, content) VALUES (%s, %s, %s)"
                ).format(sql.Identifier(t)),
                (1, v, f"original v{v} " + "O" * 100),
            )

        # Delete v4-v5 (cascade)
        db.execute(
            sql.SQL("DELETE FROM {} WHERE group_id = 1 AND version >= 4").format(
                sql.Identifier(t)
            )
        )

        # DO NOT VACUUM — dead tuples remain

        # Insert new v4-v6 (these get seq 4,5,6 — because delete triggers
        # stats refresh which may affect seq cache, but the important thing
        # is that the new inserts get fresh seq numbers)
        for v in range(4, 7):
            db.execute(
                sql.SQL(
                    "INSERT INTO {} (group_id, version, content) VALUES (%s, %s, %s)"
                ).format(sql.Identifier(t)),
                (1, v, f"REPLACED v{v} " + "R" * 100),
            )

        # Drop index, evict cache
        indexes = db.execute(
            """
            SELECT indexname FROM pg_indexes
            WHERE tablename = %s AND indexdef LIKE '%%xp_seq%%'
            """,
            (t,),
        ).fetchall()
        for idx_row in indexes:
            db.execute(
                sql.SQL("DROP INDEX IF EXISTS {}").format(
                    sql.Identifier(idx_row["indexname"])
                )
            )

        for g in range(300, 330):
            db.execute(
                sql.SQL(
                    "INSERT INTO {} (group_id, version, content) VALUES (%s, %s, %s)"
                ).format(sql.Identifier(t)),
                (g, 1, f"evict group {g}"),
            )

        # Read back
        rows = db.execute(
            sql.SQL(
                "SELECT version, content FROM {} WHERE group_id = 1 ORDER BY version"
            ).format(sql.Identifier(t))
        ).fetchall()

        assert len(rows) == 6, f"Expected 6 rows, got {len(rows)}"

        for row in rows:
            v = row["version"]
            if v <= 3:
                assert row["content"].startswith(f"original v{v} "), (
                    f"v{v}: expected original content, got '{row['content'][:60]}'"
                )
            else:
                assert row["content"].startswith(f"REPLACED v{v} "), (
                    f"v{v}: expected REPLACED content, got '{row['content'][:60]}'. "
                    "Deleted tuple's delta data used as reconstruction base (missing visibility check)."
                )


class TestColdStartFIFOPopulate:
    """
    The cold-start FIFO populate path (xpatch_insert_cache_populate, line 917
    of xpatch_insert_cache.c) calls xpatch_reconstruct_column which goes
    through xpatch_fetch_by_seq.  If aborted tuples are found by Strategy 3,
    the FIFO is populated with corrupt data, poisoning ALL subsequent inserts
    for that group.
    """

    def test_fifo_populate_after_abort_no_corruption(
        self, db: psycopg.Connection, make_table
    ):
        """
        1. Insert v1-v5 for group 1
        2. Abort an insert of v6 (leaves dead tuple with seq=6)
        3. Insert to 20+ other groups (evict group 1's FIFO slot)
        4. Drop index (force Strategy 3)
        5. Insert v6 correctly — this triggers cold-start FIFO populate
           which calls xpatch_reconstruct_column for bases (seq 1-5)
           Strategy 3 should NOT find the aborted seq=6 tuple when looking
           for bases, but the bases (seq 1-5) are clean.
        6. Insert v7 — during encoding, the FIFO has base from v6 populate.
           If v6's FIFO populate used corrupt data, v7 is also corrupt.
        7. Read back and verify correctness.

        This test is most likely to trigger B6 because:
        - FIFO populate explicitly calls xpatch_reconstruct_column
        - The cold start guarantees no TID cache entries
        - With no index, Strategy 3 is the only option
        """
        t = make_table(
            "group_id INT, version INT, content TEXT NOT NULL",
            group_by="group_id",
            order_by="version",
            delta_columns=["content"],
            compress_depth=5,
            keyframe_every=100,
        )

        # Insert v1-v5
        for v in range(1, 6):
            db.execute(
                sql.SQL(
                    "INSERT INTO {} (group_id, version, content) VALUES (%s, %s, %s)"
                ).format(sql.Identifier(t)),
                (1, v, f"BASE v{v} " + "B" * 100),
            )

        # Abort insert of v6
        conninfo = db.info.dsn
        conn2 = psycopg.connect(conninfo, autocommit=False)
        try:
            conn2.execute(
                sql.SQL(
                    "INSERT INTO {} (group_id, version, content) VALUES (%s, %s, %s)"
                ).format(sql.Identifier(t)),
                (1, 6, f"ABORT v6 " + "X" * 100),
            )
            conn2.rollback()
        finally:
            conn2.close()

        # Drop index BEFORE evicting groups (so new groups also don't use index)
        indexes = db.execute(
            """
            SELECT indexname FROM pg_indexes
            WHERE tablename = %s AND indexdef LIKE '%%xp_seq%%'
            """,
            (t,),
        ).fetchall()
        for idx_row in indexes:
            db.execute(
                sql.SQL("DROP INDEX IF EXISTS {}").format(
                    sql.Identifier(idx_row["indexname"])
                )
            )

        # Evict group 1's FIFO slot (insert to 20+ other groups)
        for g in range(400, 425):
            for v in range(1, 3):
                db.execute(
                    sql.SQL(
                        "INSERT INTO {} (group_id, version, content) VALUES (%s, %s, %s)"
                    ).format(sql.Identifier(t)),
                    (g, v, f"evict group {g} v{v}"),
                )

        # Insert v6 correctly — triggers cold-start FIFO populate
        db.execute(
            sql.SQL(
                "INSERT INTO {} (group_id, version, content) VALUES (%s, %s, %s)"
            ).format(sql.Identifier(t)),
            (1, 6, f"BASE v6 " + "B" * 100),
        )

        # Insert v7 — uses FIFO base from v6
        db.execute(
            sql.SQL(
                "INSERT INTO {} (group_id, version, content) VALUES (%s, %s, %s)"
            ).format(sql.Identifier(t)),
            (1, 7, f"BASE v7 " + "B" * 100),
        )

        # Read back group 1
        rows = db.execute(
            sql.SQL(
                "SELECT version, content FROM {} WHERE group_id = 1 ORDER BY version"
            ).format(sql.Identifier(t))
        ).fetchall()

        assert len(rows) == 7, f"Expected 7 rows, got {len(rows)}"

        for row in rows:
            v = row["version"]
            expected = f"BASE v{v} " + "B" * 100
            assert row["content"] == expected, (
                f"v{v}: expected '{expected[:40]}...', got '{row['content'][:40]}...'. "
                "FIFO populate used aborted tuple data via sequential scan fallback."
            )
