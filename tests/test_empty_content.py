"""
Tests for empty content handling in delta compression.

Regression tests for a bug where the insert cache (FIFO) silently drops
empty content (size=0) on push but still advances the ring head. This
causes subsequent inserts to compute deltas against stale (non-empty)
bases instead of the correct empty predecessor.

The bug chain:
  1. xpatch_insert_cache_push() returns early when size=0 (line 719)
  2. xpatch_insert_cache_commit_entry() still marks the entry valid
  3. xpatch_insert_cache_get_bases() skips entries with content_size=0
  4. Result: the next insert uses a stale base from the ring buffer

This produces incorrect deltas that may decode to wrong content or fail
on reconstruction depending on the xpatch library version.
"""

from __future__ import annotations

import psycopg
import pytest


class TestEmptyContentDelta:
    """Empty string content in delta-compressed columns."""

    def test_nonempty_empty_empty_via_copy(
        self, db: psycopg.Connection, make_table
    ):
        """
        COPY: non-empty -> empty -> empty must all reconstruct correctly.

        This is the exact pattern from the pgit bug report: 15 non-empty
        versions followed by 2 consecutive empty versions, all inserted
        in one COPY call with compress_depth=5.
        """
        t = make_table(
            "group_id INT, version_id INT, content TEXT NOT NULL",
            group_by="group_id",
            order_by="version_id",
            delta_columns=["content"],
            compress_depth=5,
        )

        data = []
        for v in range(1, 18):
            if v <= 5:
                content = "- a list\n  - of stuff\n"
            elif v <= 10:
                content = "- a list\n  - of stuff\n  "
            elif v in (11, 13, 15):
                content = "- a list\n  - of stuff\n"
            elif v in (12, 14):
                content = "- a list\n  - of stuff\n  "
            else:
                content = ""
            data.append((1, v, content))

        with db.cursor() as cur:
            with cur.copy(
                f"COPY {t} (group_id, version_id, content) FROM STDIN"
            ) as copy:
                for row in data:
                    copy.write_row(row)

        # Read all rows back and verify content
        rows = db.execute(
            f"SELECT version_id, content FROM {t} "
            "WHERE group_id = 1 ORDER BY version_id"
        ).fetchall()

        assert len(rows) == 17
        for row in rows:
            v = row["version_id"]
            if v >= 16:
                assert row["content"] == "", (
                    f"v{v}: expected empty, got {repr(row['content'])}"
                )

    def test_nonempty_empty_empty_via_insert(
        self, db: psycopg.Connection, make_table
    ):
        """
        Individual INSERTs: non-empty -> empty -> empty.

        Same pattern but via individual INSERTs (warm FIFO path).
        """
        t = make_table(
            compress_depth=5,
        )

        for v in range(1, 6):
            db.execute(
                f"INSERT INTO {t} (group_id, version, content) "
                "VALUES (%s, %s, %s)",
                (1, v, f"content version {v}"),
            )

        # Insert 2 consecutive empty versions
        db.execute(
            f"INSERT INTO {t} (group_id, version, content) "
            "VALUES (%s, %s, %s)",
            (1, 6, ""),
        )
        db.execute(
            f"INSERT INTO {t} (group_id, version, content) "
            "VALUES (%s, %s, %s)",
            (1, 7, ""),
        )

        rows = db.execute(
            f"SELECT version, content FROM {t} "
            "WHERE group_id = 1 ORDER BY version"
        ).fetchall()

        assert len(rows) == 7
        assert rows[5]["content"] == "", f"v6: {repr(rows[5]['content'])}"
        assert rows[6]["content"] == "", f"v7: {repr(rows[6]['content'])}"

    def test_empty_delta_tag_correctness(
        self, db: psycopg.Connection, make_table
    ):
        """
        Minimal reproduction of the insert cache empty content bug.

        Pattern from the original bug report (pgit group 4772):
          v1: non-empty (22 bytes)
          v2: empty string — delta against v1 (tag=1): correct
          v3: empty string — delta against v2 (tag=1): EXPECTED

        BUG: xpatch_insert_cache_push() silently drops empty content
        (size=0 early return at line 719), but commit_entry() still
        advances the ring head. get_bases() then skips the empty v2
        entry, so v3's delta is computed against the stale v1 (tag=2).

        This test uses individual INSERTs to exercise the warm FIFO
        path where the bug lives. COPY goes through a bulk/cold path
        that doesn't trigger the bug.
        """
        t = make_table(
            "group_id INT, version_id INT, content TEXT NOT NULL",
            group_by="group_id",
            order_by="version_id",
            delta_columns=["content"],
            compress_depth=5,
        )

        # Individual INSERTs to exercise the warm FIFO path
        db.execute(
            f"INSERT INTO {t} (group_id, version_id, content) "
            "VALUES (%s, %s, %s)",
            (1, 1, "This is 22 bytes text!"),
        )
        db.execute(
            f"INSERT INTO {t} (group_id, version_id, content) "
            "VALUES (%s, %s, %s)",
            (1, 2, ""),
        )
        db.execute(
            f"INSERT INTO {t} (group_id, version_id, content) "
            "VALUES (%s, %s, %s)",
            (1, 3, ""),
        )

        # Inspect physical storage for v2 and v3
        phys = db.execute(
            f"SELECT version, tag FROM xpatch.inspect('{t}', 1) "
            "WHERE version >= 2 ORDER BY version"
        ).fetchall()

        assert len(phys) == 2, f"Expected 2 rows for v2-v3, got {len(phys)}"

        v2 = phys[0]
        v3 = phys[1]

        # v2: delta against v1 (tag=1) is correct
        assert v2["tag"] == 1, (
            f"v2 tag should be 1 (delta against v1), got {v2['tag']}"
        )

        # v3: MUST be tag=1 (delta against v2).
        # BUG: insert cache drops empty v2, so v3 gets tag=2 (against v1)
        assert v3["tag"] == 1, (
            f"v3 tag should be 1 (delta against v2), got {v3['tag']}. "
            "This indicates the insert cache skipped the empty v2 entry."
        )

    def test_empty_then_nonempty_after_empty(
        self, db: psycopg.Connection, make_table
    ):
        """
        Pattern: non-empty -> empty -> empty -> non-empty.

        The non-empty version after the empty ones must reconstruct
        correctly. If the FIFO has stale bases, this could produce
        wrong delta encoding.
        """
        t = make_table(compress_depth=3)

        for v in range(1, 4):
            db.execute(
                f"INSERT INTO {t} (group_id, version, content) "
                "VALUES (%s, %s, %s)",
                (1, v, f"hello world v{v}"),
            )

        # Two empty versions
        db.execute(
            f"INSERT INTO {t} (group_id, version, content) "
            "VALUES (%s, %s, %s)",
            (1, 4, ""),
        )
        db.execute(
            f"INSERT INTO {t} (group_id, version, content) "
            "VALUES (%s, %s, %s)",
            (1, 5, ""),
        )

        # Non-empty again
        db.execute(
            f"INSERT INTO {t} (group_id, version, content) "
            "VALUES (%s, %s, %s)",
            (1, 6, "back to normal"),
        )

        rows = db.execute(
            f"SELECT version, content FROM {t} "
            "WHERE group_id = 1 ORDER BY version"
        ).fetchall()

        assert len(rows) == 6
        assert rows[3]["content"] == ""
        assert rows[4]["content"] == ""
        assert rows[5]["content"] == "back to normal"

    def test_alternating_empty_nonempty(
        self, db: psycopg.Connection, make_table
    ):
        """
        Alternating: non-empty, empty, non-empty, empty, ...

        Each empty version's delta should reference its immediate
        predecessor (tag=1), not skip to an older non-empty base.
        """
        t = make_table(compress_depth=3)

        for v in range(1, 11):
            content = f"content v{v}" if v % 2 == 1 else ""
            db.execute(
                f"INSERT INTO {t} (group_id, version, content) "
                "VALUES (%s, %s, %s)",
                (1, v, content),
            )

        rows = db.execute(
            f"SELECT version, content FROM {t} "
            "WHERE group_id = 1 ORDER BY version"
        ).fetchall()

        assert len(rows) == 10
        for row in rows:
            v = row["version"]
            expected = f"content v{v}" if v % 2 == 1 else ""
            assert row["content"] == expected, (
                f"v{v}: expected {repr(expected)}, got {repr(row['content'])}"
            )

    def test_compress_depth_2_empty_wraps_ring(
        self, db: psycopg.Connection, make_table
    ):
        """
        With compress_depth=2, the FIFO ring has only 2 slots.
        Insert pattern: A, B, '', '', C

        After '' at v3, the ring should have [v2='B', v3=''].
        After '' at v4, the ring should have [v3='', v4=''].
        After 'C' at v5, v5's delta should be against v4=''.

        If empty pushes are dropped, the ring never updates for v3/v4,
        so v5 computes delta against v2='B' (stale).
        """
        t = make_table(compress_depth=2)

        db.execute(
            f"INSERT INTO {t} (group_id, version, content) "
            "VALUES (1, 1, 'AAAA')"
        )
        db.execute(
            f"INSERT INTO {t} (group_id, version, content) "
            "VALUES (1, 2, 'BBBB')"
        )
        db.execute(
            f"INSERT INTO {t} (group_id, version, content) "
            "VALUES (1, 3, '')"
        )
        db.execute(
            f"INSERT INTO {t} (group_id, version, content) "
            "VALUES (1, 4, '')"
        )
        db.execute(
            f"INSERT INTO {t} (group_id, version, content) "
            "VALUES (1, 5, 'CCCC')"
        )

        rows = db.execute(
            f"SELECT version, content FROM {t} "
            "WHERE group_id = 1 ORDER BY version"
        ).fetchall()

        assert len(rows) == 5
        assert rows[0]["content"] == "AAAA"
        assert rows[1]["content"] == "BBBB"
        assert rows[2]["content"] == ""
        assert rows[3]["content"] == ""
        assert rows[4]["content"] == "CCCC"

        # Check physical: v5 should have tag=1 (against v4='')
        # not tag=2 (against v3='') or higher
        phys = db.execute(
            f"SELECT version, tag FROM xpatch.inspect('{t}', 1) "
            "WHERE version = 5"
        ).fetchone()
        assert phys["tag"] == 1, (
            f"v5 should be tag=1 (against v4), got tag={phys['tag']}"
        )

    def test_many_consecutive_empties(
        self, db: psycopg.Connection, make_table
    ):
        """
        10 non-empty then 10 consecutive empty versions.

        All must reconstruct to empty strings.
        """
        t = make_table(compress_depth=5)

        for v in range(1, 11):
            db.execute(
                f"INSERT INTO {t} (group_id, version, content) "
                "VALUES (%s, %s, %s)",
                (1, v, f"data-{v}" * 10),
            )
        for v in range(11, 21):
            db.execute(
                f"INSERT INTO {t} (group_id, version, content) "
                "VALUES (%s, %s, %s)",
                (1, v, ""),
            )

        rows = db.execute(
            f"SELECT version, content FROM {t} "
            "WHERE group_id = 1 AND version >= 11 ORDER BY version"
        ).fetchall()

        assert len(rows) == 10
        for row in rows:
            assert row["content"] == "", (
                f"v{row['version']}: expected empty, got {repr(row['content'])}"
            )

    def test_empty_at_keyframe_boundary(
        self, db: psycopg.Connection, make_table
    ):
        """
        Empty content right at the keyframe boundary.

        With keyframe_every=5, v5 is a keyframe. If v5 is empty,
        it should be stored as a keyframe with empty content.
        """
        t = make_table(
            compress_depth=3,
            keyframe_every=5,
        )

        for v in range(1, 5):
            db.execute(
                f"INSERT INTO {t} (group_id, version, content) "
                "VALUES (%s, %s, %s)",
                (1, v, f"version {v}"),
            )

        # v5 = keyframe, empty
        db.execute(
            f"INSERT INTO {t} (group_id, version, content) "
            "VALUES (1, 5, '')"
        )
        # v6 = delta against empty keyframe
        db.execute(
            f"INSERT INTO {t} (group_id, version, content) "
            "VALUES (1, 6, '')"
        )
        # v7 = non-empty after empty keyframe
        db.execute(
            f"INSERT INTO {t} (group_id, version, content) "
            "VALUES (1, 7, 'restored')"
        )

        rows = db.execute(
            f"SELECT version, content FROM {t} "
            "WHERE group_id = 1 ORDER BY version"
        ).fetchall()

        assert len(rows) == 7
        assert rows[4]["content"] == ""
        assert rows[5]["content"] == ""
        assert rows[6]["content"] == "restored"

    def test_multiple_groups_with_empty(
        self, db: psycopg.Connection, make_table
    ):
        """
        Multiple groups, some with empty content, some without.

        Verifies insert cache doesn't leak stale content between groups.
        """
        t = make_table(compress_depth=3)

        # Group 1: normal content
        for v in range(1, 4):
            db.execute(
                f"INSERT INTO {t} (group_id, version, content) "
                "VALUES (%s, %s, %s)",
                (1, v, f"g1v{v}"),
            )

        # Group 2: non-empty then empty
        for v in range(1, 3):
            db.execute(
                f"INSERT INTO {t} (group_id, version, content) "
                "VALUES (%s, %s, %s)",
                (2, v, f"g2v{v}"),
            )
        db.execute(
            f"INSERT INTO {t} (group_id, version, content) "
            "VALUES (2, 3, '')"
        )
        db.execute(
            f"INSERT INTO {t} (group_id, version, content) "
            "VALUES (2, 4, '')"
        )

        # Group 3: only empty
        db.execute(
            f"INSERT INTO {t} (group_id, version, content) "
            "VALUES (3, 1, '')"
        )
        db.execute(
            f"INSERT INTO {t} (group_id, version, content) "
            "VALUES (3, 2, '')"
        )

        # Verify all groups
        for gid, expected in [
            (1, ["g1v1", "g1v2", "g1v3"]),
            (2, ["g2v1", "g2v2", "", ""]),
            (3, ["", ""]),
        ]:
            rows = db.execute(
                f"SELECT content FROM {t} "
                "WHERE group_id = %s ORDER BY version",
                (gid,),
            ).fetchall()
            actual = [r["content"] for r in rows]
            assert actual == expected, (
                f"Group {gid}: expected {expected}, got {actual}"
            )
