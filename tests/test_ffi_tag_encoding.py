"""
Tests for hypothesis B4: FFI tag encoding mismatch at Rust boundary.

The tag value flows through:
  C (int/size_t) → Rust FFI (usize) → gdelta header → stored on disk →
  extracted by xpatch_get_delta_tag (Rust) → C (size_t) → used as base_seq offset

If there's a type mismatch or encoding error, deltas would be decoded against
the wrong base, producing "Error decoding gdelta" or silent corruption.

We test by:
1. Using various compress_depth values (tags 1-N)
2. Verifying reconstruction at each tag distance
3. Specifically targeting the boundary where tag values cross byte boundaries
"""

import pytest


class TestFFITagBasic:
    """Verify tag encoding/decoding for all supported compress_depth values."""

    @pytest.mark.parametrize("depth", [1, 2, 3, 4, 5])
    def test_all_tag_values_at_depth(self, db, depth):
        """
        With compress_depth=N, tags 1..N should all work correctly.
        Insert N+1 versions so that the last version has tag=N against the first.
        """
        db.execute("""
            CREATE TABLE xp_tags (
                group_id INT NOT NULL,
                version INT NOT NULL,
                content TEXT NOT NULL
            ) USING xpatch
        """)
        db.execute(f"""
            SELECT xpatch.configure('xp_tags',
                group_by => 'group_id',
                order_by => 'version',
                delta_columns => ARRAY['content'],
                compress_depth => {depth})
        """)

        base = "The base content for tag testing. " * 10
        versions = {}
        # Insert enough versions to exercise all tag values
        n = depth * 3 + 1
        for v in range(1, n + 1):
            content = base + f" v{v}: {'a' * v}"
            versions[v] = content
            db.execute(
                "INSERT INTO xp_tags (group_id, version, content) VALUES (1, %s, %s)",
                (v, content),
            )

        # Read all back
        for v in range(1, n + 1):
            row = db.execute(
                "SELECT content FROM xp_tags WHERE group_id = 1 AND version = %s", (v,)
            ).fetchone()
            assert row["content"] == versions[v], (
                f"depth={depth}, version={v}: reconstruction mismatch"
            )

    def test_tag_at_max_depth_boundary(self, db, make_table):
        """
        Specifically test that the maximum tag value (compress_depth) encodes
        and decodes correctly.  With compress_depth=5, version 6 should have
        tag=5 against version 1.
        """
        tbl = make_table(compress_depth=5)

        # Insert exactly compress_depth+1 versions
        base = "Stable base content for boundary test. " * 20
        for v in range(1, 7):
            content = base + f"\n--- version {v} ---\n" + "x" * (v * 100)
            db.execute(
                f"INSERT INTO {tbl} (group_id, version, content) VALUES (1, %s, %s)",
                (v, content),
            )

        # Read version 6 — this requires tag=5 delta against version 1
        row = db.execute(
            f"SELECT content FROM {tbl} WHERE group_id = 1 AND version = 6"
        ).fetchone()
        expected = base + "\n--- version 6 ---\n" + "x" * 600
        assert row["content"] == expected


class TestFFITagWithCopy:
    """Verify tag encoding works correctly during COPY (bulk insert path)."""

    def test_copy_long_chain(self, db, make_table):
        """
        COPY 100 rows for a single group with compress_depth=5.
        Every version beyond 5 requires delta decoding with various tags.
        """
        tbl = make_table(compress_depth=5)

        base = "Copy chain base content. " * 8
        versions = {}
        with db.cursor() as cur:
            with cur.copy(f"COPY {tbl} (group_id, version, content) FROM STDIN") as copy:
                for v in range(1, 101):
                    content = base + f" v{v}" + "#" * (v % 50)
                    versions[v] = content
                    copy.write_row((1, v, content))

        # Verify all versions
        for v in range(1, 101):
            row = db.execute(
                f"SELECT content FROM {tbl} WHERE group_id = 1 AND version = %s", (v,)
            ).fetchone()
            assert row["content"] == versions[v], (
                f"COPY chain: version {v} reconstruction mismatch"
            )

    def test_copy_multiple_groups_interleaved_tags(self, db, make_table):
        """
        COPY rows for multiple groups.  Each group has its own delta chain
        with independent tag sequences.
        """
        tbl = make_table(compress_depth=3)

        expected = {}
        with db.cursor() as cur:
            with cur.copy(f"COPY {tbl} (group_id, version, content) FROM STDIN") as copy:
                for g in range(1, 6):
                    for v in range(1, 21):
                        content = f"group{g}_v{v}_" + "data" * (g * v)
                        expected[(g, v)] = content
                        copy.write_row((g, v, content))

        for (g, v), content in expected.items():
            row = db.execute(
                f"SELECT content FROM {tbl} WHERE group_id = %s AND version = %s",
                (g, v),
            ).fetchone()
            assert row["content"] == content, (
                f"Group {g}, version {v}: mismatch"
            )


class TestFFITagKeyframeTransition:
    """Test the keyframe tag (0xFFFF / XPATCH_KEYFRAME_TAG) encoding."""

    def test_keyframe_every_setting(self, db):
        """
        With keyframe_every=5, every 5th row should be a keyframe (tag=65535).
        All rows should still reconstruct correctly.
        """
        db.execute("""
            CREATE TABLE xp_kf (
                group_id INT NOT NULL,
                version INT NOT NULL,
                content TEXT NOT NULL
            ) USING xpatch
        """)
        db.execute("""
            SELECT xpatch.configure('xp_kf',
                group_by => 'group_id',
                order_by => 'version',
                delta_columns => ARRAY['content'],
                keyframe_every => 5,
                compress_depth => 5)
        """)

        base = "Keyframe test content. " * 10
        for v in range(1, 26):
            content = base + f" VERSION_{v}_" + "q" * v
            db.execute(
                "INSERT INTO xp_kf (group_id, version, content) VALUES (1, %s, %s)",
                (v, content),
            )

        for v in range(1, 26):
            expected = base + f" VERSION_{v}_" + "q" * v
            row = db.execute(
                "SELECT content FROM xp_kf WHERE group_id = 1 AND version = %s", (v,)
            ).fetchone()
            assert row["content"] == expected, f"Version {v} mismatch with keyframe_every=5"

    def test_first_version_always_keyframe(self, db, make_table):
        """Version 1 of any group should always be stored as a keyframe."""
        tbl = make_table(compress_depth=5)

        for g in range(1, 6):
            db.execute(
                f"INSERT INTO {tbl} (group_id, version, content) VALUES (%s, 1, %s)",
                (g, f"first_version_group_{g}"),
            )

        # Read back — these are pure keyframes, no delta chain
        for g in range(1, 6):
            row = db.execute(
                f"SELECT content FROM {tbl} WHERE group_id = %s AND version = 1", (g,)
            ).fetchone()
            assert row["content"] == f"first_version_group_{g}"
