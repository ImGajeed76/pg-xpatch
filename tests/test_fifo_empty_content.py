"""
Tests for hypothesis B1/B7: FIFO empty-content push + ring buffer boundary.

The concern is that pushing empty content (size=0) into the FIFO ring buffer
might not properly advance the head pointer or set the valid flag, causing
subsequent get_bases() to return stale data from a previous ring occupant.

The ring buffer has `compress_depth` positions.  When it wraps around, old
entries are overwritten.  If an empty-content entry doesn't properly occupy
its ring position, subsequent entries could be shifted, causing get_bases()
to return the wrong base for delta encoding.

We also test the ring boundary itself: when count == depth and head wraps
to 0, the oldest entry is overwritten.  We verify that no corruption occurs
at the exact wrap point.

Note: The code already has a fix for the empty-content early return path
(it now properly records the entry).  These tests verify the fix works.
"""

import pytest


class TestFIFOEmptyContent:
    """Test FIFO behavior with empty/zero-length content in delta columns."""

    def test_empty_content_between_nonempty(self, db):
        """
        Insert pattern: nonempty, empty, nonempty.
        The delta for version 3 should be against version 2 (empty) or
        version 1 (nonempty), not against stale data.
        """
        db.execute("""
            CREATE TABLE xp_empty_mid (
                group_id INT NOT NULL,
                version INT NOT NULL,
                content TEXT NOT NULL
            ) USING xpatch
        """)
        db.execute("""
            SELECT xpatch.configure('xp_empty_mid',
                group_by => 'group_id',
                order_by => 'version',
                delta_columns => ARRAY['content'],
                compress_depth => 5)
        """)

        db.execute("INSERT INTO xp_empty_mid VALUES (1, 1, 'hello world')")
        db.execute("INSERT INTO xp_empty_mid VALUES (1, 2, '')")  # empty
        db.execute("INSERT INTO xp_empty_mid VALUES (1, 3, 'hello world again')")
        db.execute("INSERT INTO xp_empty_mid VALUES (1, 4, '')")  # empty
        db.execute("INSERT INTO xp_empty_mid VALUES (1, 5, 'final content')")

        # Verify all versions read back correctly
        expected = {1: "hello world", 2: "", 3: "hello world again", 4: "", 5: "final content"}
        for v, exp in expected.items():
            row = db.execute(
                "SELECT content FROM xp_empty_mid WHERE group_id = 1 AND version = %s", (v,)
            ).fetchone()
            assert row["content"] == exp, f"Version {v}: expected {exp!r}, got {row['content']!r}"

    def test_all_empty_content(self, db):
        """All versions have empty content.  Should still work."""
        db.execute("""
            CREATE TABLE xp_all_empty (
                group_id INT NOT NULL,
                version INT NOT NULL,
                content TEXT NOT NULL
            ) USING xpatch
        """)
        db.execute("""
            SELECT xpatch.configure('xp_all_empty',
                group_by => 'group_id',
                order_by => 'version',
                delta_columns => ARRAY['content'],
                compress_depth => 3)
        """)

        for v in range(1, 11):
            db.execute("INSERT INTO xp_all_empty VALUES (1, %s, '')", (v,))

        for v in range(1, 11):
            row = db.execute(
                "SELECT content FROM xp_all_empty WHERE group_id = 1 AND version = %s", (v,)
            ).fetchone()
            assert row["content"] == "", f"Version {v}: expected empty, got {row['content']!r}"

    def test_empty_at_ring_wrap_point(self, db):
        """
        With compress_depth=3, the ring wraps after 3 entries.
        Put empty content at the exact wrap point (version 4 = position 0).
        """
        db.execute("""
            CREATE TABLE xp_wrap_empty (
                group_id INT NOT NULL,
                version INT NOT NULL,
                content TEXT NOT NULL
            ) USING xpatch
        """)
        db.execute("""
            SELECT xpatch.configure('xp_wrap_empty',
                group_by => 'group_id',
                order_by => 'version',
                delta_columns => ARRAY['content'],
                compress_depth => 3)
        """)

        # Positions 0,1,2 filled with versions 1,2,3
        db.execute("INSERT INTO xp_wrap_empty VALUES (1, 1, 'aaa')")
        db.execute("INSERT INTO xp_wrap_empty VALUES (1, 2, 'bbb')")
        db.execute("INSERT INTO xp_wrap_empty VALUES (1, 3, 'ccc')")
        # Version 4 wraps to position 0, and it's EMPTY
        db.execute("INSERT INTO xp_wrap_empty VALUES (1, 4, '')")
        # Version 5 at position 1 — delta against version 4 (empty) or 3
        db.execute("INSERT INTO xp_wrap_empty VALUES (1, 5, 'ddd')")
        # Version 6 at position 2
        db.execute("INSERT INTO xp_wrap_empty VALUES (1, 6, 'eee')")

        expected = {1: "aaa", 2: "bbb", 3: "ccc", 4: "", 5: "ddd", 6: "eee"}
        for v, exp in expected.items():
            row = db.execute(
                "SELECT content FROM xp_wrap_empty WHERE group_id = 1 AND version = %s", (v,)
            ).fetchone()
            assert row["content"] == exp, f"Version {v}: expected {exp!r}, got {row['content']!r}"


class TestFIFORingBoundary:
    """Test ring buffer wrap-around at various compress_depth values."""

    @pytest.mark.parametrize("depth", [1, 2, 3, 4, 5])
    def test_ring_wrap_correctness(self, db, depth):
        """
        Insert 3*depth rows to ensure multiple ring wrap-arounds.
        All versions must reconstruct correctly.
        """
        db.execute("""
            CREATE TABLE xp_ring (
                group_id INT NOT NULL,
                version INT NOT NULL,
                content TEXT NOT NULL
            ) USING xpatch
        """)
        db.execute(f"""
            SELECT xpatch.configure('xp_ring',
                group_by => 'group_id',
                order_by => 'version',
                delta_columns => ARRAY['content'],
                compress_depth => {depth})
        """)

        n = depth * 3
        base = "Ring wrap test content. " * 5
        versions = {}
        for v in range(1, n + 1):
            content = base + f" v{v}" + "=" * (v % 20)
            versions[v] = content
            db.execute(
                "INSERT INTO xp_ring (group_id, version, content) VALUES (1, %s, %s)",
                (v, content),
            )

        for v in range(1, n + 1):
            row = db.execute(
                "SELECT content FROM xp_ring WHERE group_id = 1 AND version = %s", (v,)
            ).fetchone()
            assert row["content"] == versions[v], (
                f"depth={depth}, version={v}: ring wrap corruption"
            )

        db.execute("DROP TABLE xp_ring")

    def test_exact_depth_boundary(self, db, make_table):
        """
        Insert exactly compress_depth rows, then one more.
        The (depth+1)-th row triggers the first wrap-around.
        """
        tbl = make_table(compress_depth=5)

        base = "Boundary test. " * 10
        # Fill ring exactly (5 entries for depth=5)
        for v in range(1, 6):
            db.execute(
                f"INSERT INTO {tbl} (group_id, version, content) VALUES (1, %s, %s)",
                (v, base + f"v{v}"),
            )

        # This triggers wrap — version 6 overwrites position 0
        db.execute(
            f"INSERT INTO {tbl} (group_id, version, content) VALUES (1, 6, %s)",
            (base + "v6",),
        )

        # Verify version 6 (just written) and version 1 (overwritten in ring but still on disk)
        for v in range(1, 7):
            row = db.execute(
                f"SELECT content FROM {tbl} WHERE group_id = 1 AND version = %s", (v,)
            ).fetchone()
            assert row["content"] == base + f"v{v}", f"Version {v} mismatch at ring boundary"


class TestFIFOMultiDeltaColumn:
    """Test FIFO with multiple delta columns, especially when one is empty."""

    def test_multi_column_one_empty(self, db):
        """
        Table with two delta columns.  One column is empty for some versions.
        The other column should still delta-encode correctly.
        """
        db.execute("""
            CREATE TABLE xp_multi_empty (
                group_id INT NOT NULL,
                version INT NOT NULL,
                col_a TEXT NOT NULL,
                col_b TEXT NOT NULL
            ) USING xpatch
        """)
        db.execute("""
            SELECT xpatch.configure('xp_multi_empty',
                group_by => 'group_id',
                order_by => 'version',
                delta_columns => ARRAY['col_a', 'col_b'],
                compress_depth => 3)
        """)

        data = [
            (1, "hello", "world"),
            (2, "", "still here"),        # col_a empty
            (3, "back again", ""),         # col_b empty
            (4, "", ""),                   # both empty
            (5, "final a", "final b"),     # both non-empty
            (6, "extra a", "extra b"),     # past ring wrap for depth=3
        ]

        for v, a, b in data:
            db.execute(
                "INSERT INTO xp_multi_empty VALUES (1, %s, %s, %s)", (v, a, b)
            )

        for v, exp_a, exp_b in data:
            row = db.execute(
                "SELECT col_a, col_b FROM xp_multi_empty WHERE group_id = 1 AND version = %s",
                (v,),
            ).fetchone()
            assert row["col_a"] == exp_a, f"v{v} col_a: expected {exp_a!r}, got {row['col_a']!r}"
            assert row["col_b"] == exp_b, f"v{v} col_b: expected {exp_b!r}, got {row['col_b']!r}"

    def test_multi_column_alternating_empty(self, db):
        """
        Alternate which column is empty across versions.
        This stresses the per-column FIFO tracking.
        """
        db.execute("""
            CREATE TABLE xp_multi_alt (
                group_id INT NOT NULL,
                version INT NOT NULL,
                col_a TEXT NOT NULL,
                col_b TEXT NOT NULL
            ) USING xpatch
        """)
        db.execute("""
            SELECT xpatch.configure('xp_multi_alt',
                group_by => 'group_id',
                order_by => 'version',
                delta_columns => ARRAY['col_a', 'col_b'],
                compress_depth => 5)
        """)

        expected = {}
        for v in range(1, 16):
            if v % 3 == 0:
                a, b = "", f"only_b_{v}"
            elif v % 3 == 1:
                a, b = f"only_a_{v}", ""
            else:
                a, b = f"both_a_{v}", f"both_b_{v}"
            expected[v] = (a, b)
            db.execute(
                "INSERT INTO xp_multi_alt VALUES (1, %s, %s, %s)", (v, a, b)
            )

        for v, (exp_a, exp_b) in expected.items():
            row = db.execute(
                "SELECT col_a, col_b FROM xp_multi_alt WHERE group_id = 1 AND version = %s",
                (v,),
            ).fetchone()
            assert row["col_a"] == exp_a, f"v{v} col_a mismatch"
            assert row["col_b"] == exp_b, f"v{v} col_b mismatch"
