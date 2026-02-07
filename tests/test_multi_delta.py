"""
Test tables with multiple delta-compressed columns.

Covers:
- Table with 3 delta columns
- Independent column queries and column projection
- Different columns change at different versions
- Per-column inspection via xpatch.inspect()
- Mixed delta column types (TEXT + JSONB + BYTEA)
- Content integrity across multi-column delta chains
- Keyframe boundary with multi-delta
- 4+ delta columns
- Edge cases: empty strings, identical columns, ORDER BY delta
"""

from __future__ import annotations

import json

import psycopg
import pytest

from conftest import row_count


def _parse_jsonb(val):
    """Parse JSONB value that may be str or already-parsed dict."""
    if isinstance(val, str):
        return json.loads(val)
    return val


class TestThreeDeltaColumns:
    """Table with 3 delta-compressed columns."""

    def _make_3col_table(self, db, make_table, **kwargs):
        """Create table with content TEXT, summary TEXT, metadata JSONB."""
        return make_table(
            "doc_id INT, version INT, content TEXT NOT NULL, summary TEXT NOT NULL, metadata JSONB NOT NULL",
            group_by="doc_id",
            order_by="version",
            delta_columns=["content", "summary", "metadata"],
            **kwargs,
        )

    def test_insert_and_read_all_columns(self, db: psycopg.Connection, make_table):
        """All 3 delta columns store and retrieve correctly."""
        t = self._make_3col_table(db, make_table)
        db.execute(
            f"INSERT INTO {t} (doc_id, version, content, summary, metadata) "
            f"VALUES (1, 1, 'Full content v1', 'Summary v1', "
            f"'{{\"author\": \"alice\"}}'::jsonb)"
        )
        row = db.execute(f"SELECT * FROM {t}").fetchone()
        assert row["content"] == "Full content v1"
        assert row["summary"] == "Summary v1"
        meta = _parse_jsonb(row["metadata"])
        assert meta["author"] == "alice"

    def test_multi_version_all_columns(self, db: psycopg.Connection, make_table):
        """Multiple versions with all 3 delta columns reconstruct correctly."""
        t = self._make_3col_table(db, make_table)
        for v in range(1, 6):
            db.execute(
                f"INSERT INTO {t} (doc_id, version, content, summary, metadata) "
                f"VALUES (1, {v}, 'Content v{v}', 'Summary v{v}', "
                f"%s::jsonb)",
                [json.dumps({"version": v, "tags": list(range(v))})],
            )

        rows = db.execute(
            f"SELECT version, content, summary, metadata FROM {t} ORDER BY version"
        ).fetchall()
        assert len(rows) == 5
        for row in rows:
            v = row["version"]
            assert row["content"] == f"Content v{v}"
            assert row["summary"] == f"Summary v{v}"
            meta = _parse_jsonb(row["metadata"])
            assert meta["version"] == v

    def test_only_one_column_changes(self, db: psycopg.Connection, make_table):
        """When only one delta column changes, others are still correct."""
        t = self._make_3col_table(db, make_table)
        db.execute(
            f"INSERT INTO {t} (doc_id, version, content, summary, metadata) "
            f"VALUES (1, 1, 'Content stays same', 'Summary stays same', "
            f"'{{\"fixed\": true}}'::jsonb)"
        )
        # V2: only content changes
        db.execute(
            f"INSERT INTO {t} (doc_id, version, content, summary, metadata) "
            f"VALUES (1, 2, 'Content changed!', 'Summary stays same', "
            f"'{{\"fixed\": true}}'::jsonb)"
        )
        # V3: only metadata changes
        db.execute(
            f"INSERT INTO {t} (doc_id, version, content, summary, metadata) "
            f"VALUES (1, 3, 'Content changed!', 'Summary stays same', "
            f"'{{\"fixed\": false}}'::jsonb)"
        )

        rows = db.execute(
            f"SELECT * FROM {t} ORDER BY version"
        ).fetchall()

        # V1 — all original values
        assert rows[0]["content"] == "Content stays same"
        assert rows[0]["summary"] == "Summary stays same"
        meta1 = _parse_jsonb(rows[0]["metadata"])
        assert meta1["fixed"] is True

        # V2 — content changed, others unchanged
        assert rows[1]["content"] == "Content changed!"
        assert rows[1]["summary"] == "Summary stays same"
        meta2 = _parse_jsonb(rows[1]["metadata"])
        assert meta2["fixed"] is True

        # V3 — only metadata changed
        assert rows[2]["content"] == "Content changed!"
        assert rows[2]["summary"] == "Summary stays same"
        meta3 = _parse_jsonb(rows[2]["metadata"])
        assert meta3["fixed"] is False

    def test_select_single_delta_column(self, db: psycopg.Connection, make_table):
        """Selecting only one delta column works (no need to reconstruct others)."""
        t = self._make_3col_table(db, make_table)
        for v in range(1, 4):
            db.execute(
                f"INSERT INTO {t} (doc_id, version, content, summary, metadata) "
                f"VALUES (1, {v}, 'C{v}', 'S{v}', '{{\"v\": {v}}}'::jsonb)"
            )

        rows = db.execute(
            f"SELECT summary FROM {t} ORDER BY version"
        ).fetchall()
        assert [r["summary"] for r in rows] == ["S1", "S2", "S3"]

    def test_select_only_last_delta_column(self, db: psycopg.Connection, make_table):
        """Selecting only the 3rd delta column in a 3-column table works."""
        t = self._make_3col_table(db, make_table)
        for v in range(1, 4):
            db.execute(
                f"INSERT INTO {t} (doc_id, version, content, summary, metadata) "
                f"VALUES (1, {v}, 'C{v}', 'S{v}', '{{\"v\": {v}}}'::jsonb)"
            )

        rows = db.execute(
            f"SELECT metadata FROM {t} ORDER BY version"
        ).fetchall()
        for i, row in enumerate(rows, 1):
            meta = _parse_jsonb(row["metadata"])
            assert meta["v"] == i

    def test_filter_on_one_delta_column(self, db: psycopg.Connection, make_table):
        """WHERE on one delta column doesn't affect others."""
        t = self._make_3col_table(db, make_table)
        for v in range(1, 6):
            db.execute(
                f"INSERT INTO {t} (doc_id, version, content, summary, metadata) "
                f"VALUES (1, {v}, 'C{v}', 'target' || CASE WHEN {v} = 3 THEN '_match' ELSE '' END, "
                f"'{{\"v\": {v}}}'::jsonb)"
            )

        rows = db.execute(
            f"SELECT version, content FROM {t} WHERE summary = 'target_match'"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0]["version"] == 3
        assert rows[0]["content"] == "C3"


class TestInspectMultiDelta:
    """xpatch.inspect() with multiple delta columns."""

    def test_inspect_shows_per_column_entries(self, db: psycopg.Connection, make_table):
        """Inspect returns entries for each delta column with correct count."""
        t = make_table(
            "doc_id INT, version INT, content TEXT NOT NULL, summary TEXT NOT NULL",
            group_by="doc_id",
            order_by="version",
            delta_columns=["content", "summary"],
        )
        for v in range(1, 4):
            db.execute(
                f"INSERT INTO {t} (doc_id, version, content, summary) "
                f"VALUES (1, {v}, 'Content v{v}', 'Summary v{v}')"
            )

        rows = db.execute(
            f"SELECT * FROM xpatch.inspect('{t}'::regclass, 1) ORDER BY seq, column_name"
        ).fetchall()
        # 3 versions × 2 columns = 6 entries
        assert len(rows) == 6
        col_names = {r["column_name"] for r in rows}
        assert col_names == {"content", "summary"}
        # Each seq should have exactly 2 entries
        for seq in range(1, 4):
            seq_rows = [r for r in rows if r["seq"] == seq]
            assert len(seq_rows) == 2
            assert {r["column_name"] for r in seq_rows} == {"content", "summary"}

    def test_inspect_keyframe_applies_to_all_columns(
        self, db: psycopg.Connection, make_table
    ):
        """Keyframe status applies to all delta columns on the same row."""
        t = make_table(
            "gid INT, ver INT, a TEXT NOT NULL, b TEXT NOT NULL",
            group_by="gid",
            order_by="ver",
            delta_columns=["a", "b"],
            keyframe_every=3,
        )
        for v in range(1, 7):
            db.execute(
                f"INSERT INTO {t} (gid, ver, a, b) VALUES (1, {v}, 'A{v}', 'B{v}')"
            )

        rows = db.execute(
            f"SELECT seq, is_keyframe, column_name "
            f"FROM xpatch.inspect('{t}'::regclass, 1) ORDER BY seq, column_name"
        ).fetchall()
        # Keyframes at seq 1 and 4 (keyframe_every=3)
        for r in rows:
            if r["seq"] in (1, 4):
                assert r["is_keyframe"] is True, (
                    f"seq={r['seq']} col={r['column_name']} should be keyframe"
                )
            else:
                assert r["is_keyframe"] is False, (
                    f"seq={r['seq']} col={r['column_name']} should be delta"
                )


class TestMixedDeltaTypes:
    """Mixed delta column types (TEXT + BYTEA)."""

    def test_text_and_bytea_delta(self, db: psycopg.Connection, make_table):
        """Table with TEXT and BYTEA delta columns across delta chain."""
        t = make_table(
            "gid INT, ver INT, doc TEXT NOT NULL, data BYTEA NOT NULL",
            group_by="gid",
            order_by="ver",
            delta_columns=["doc", "data"],
        )
        # Insert enough versions to exercise delta chain (not just keyframe)
        for v in range(1, 6):
            db.execute(
                f"INSERT INTO {t} (gid, ver, doc, data) VALUES (1, {v}, 'doc-v{v}', %s)",
                [bytes(range(v, v + 10))],
            )

        rows = db.execute(f"SELECT ver, doc, data FROM {t} ORDER BY ver").fetchall()
        assert len(rows) == 5
        for row in rows:
            v = row["ver"]
            assert row["doc"] == f"doc-v{v}"
            assert bytes(row["data"]) == bytes(range(v, v + 10))

    def test_text_and_jsonb_delta(self, db: psycopg.Connection, make_table):
        """Table with TEXT and JSONB delta columns."""
        t = make_table(
            "gid INT, ver INT, body TEXT NOT NULL, meta JSONB NOT NULL",
            group_by="gid",
            order_by="ver",
            delta_columns=["body", "meta"],
        )
        for v in range(1, 6):
            db.execute(
                f"INSERT INTO {t} (gid, ver, body, meta) "
                f"VALUES (1, {v}, 'Body v{v}', %s::jsonb)",
                [json.dumps({"v": v})],
            )

        rows = db.execute(
            f"SELECT ver, body, meta FROM {t} ORDER BY ver"
        ).fetchall()
        for row in rows:
            v = row["ver"]
            assert row["body"] == f"Body v{v}"
            meta = _parse_jsonb(row["meta"])
            assert meta["v"] == v


class TestMultiDeltaMultiGroup:
    """Multiple delta columns with multiple groups."""

    def test_multi_group_multi_delta(self, db: psycopg.Connection, make_table):
        """3 groups x 5 versions x 2 delta columns all correct."""
        t = make_table(
            "gid INT, ver INT, content TEXT NOT NULL, summary TEXT NOT NULL",
            group_by="gid",
            order_by="ver",
            delta_columns=["content", "summary"],
        )
        for g in range(1, 4):
            for v in range(1, 6):
                db.execute(
                    f"INSERT INTO {t} (gid, ver, content, summary) "
                    f"VALUES ({g}, {v}, 'g{g}c{v}', 'g{g}s{v}')"
                )

        assert row_count(db, t) == 15

        for g in range(1, 4):
            rows = db.execute(
                f"SELECT ver, content, summary FROM {t} "
                f"WHERE gid = {g} ORDER BY ver"
            ).fetchall()
            assert len(rows) == 5
            for row in rows:
                v = row["ver"]
                assert row["content"] == f"g{g}c{v}"
                assert row["summary"] == f"g{g}s{v}"


class TestMultiDeltaKeyframeBoundary:
    """Multi-delta tables across keyframe boundaries."""

    def test_keyframe_boundary_3_columns(self, db: psycopg.Connection, make_table):
        """All delta columns reconstruct correctly across keyframe boundaries."""
        t = make_table(
            "gid INT, ver INT, a TEXT NOT NULL, b TEXT NOT NULL, c JSONB NOT NULL",
            group_by="gid",
            order_by="ver",
            delta_columns=["a", "b", "c"],
            keyframe_every=3,
        )
        for v in range(1, 10):
            db.execute(
                f"INSERT INTO {t} (gid, ver, a, b, c) "
                f"VALUES (1, {v}, 'A-v{v}', 'B-v{v}', %s::jsonb)",
                [json.dumps({"v": v})],
            )

        rows = db.execute(
            f"SELECT ver, a, b, c FROM {t} ORDER BY ver"
        ).fetchall()
        assert len(rows) == 9
        for row in rows:
            v = row["ver"]
            assert row["a"] == f"A-v{v}"
            assert row["b"] == f"B-v{v}"
            meta = _parse_jsonb(row["c"])
            assert meta["v"] == v


class TestMultiDeltaEdgeCases:
    """Edge cases specific to multi-delta columns."""

    def test_empty_string_in_one_column_only(self, db: psycopg.Connection, make_table):
        """Empty string in one delta column while others have content."""
        t = make_table(
            "gid INT, ver INT, a TEXT NOT NULL, b TEXT NOT NULL",
            group_by="gid",
            order_by="ver",
            delta_columns=["a", "b"],
        )
        db.execute(f"INSERT INTO {t} (gid, ver, a, b) VALUES (1, 1, '', 'has-content')")
        db.execute(f"INSERT INTO {t} (gid, ver, a, b) VALUES (1, 2, 'now-has-content', '')")
        db.execute(f"INSERT INTO {t} (gid, ver, a, b) VALUES (1, 3, '', '')")

        rows = db.execute(f"SELECT ver, a, b FROM {t} ORDER BY ver").fetchall()
        assert rows[0]["a"] == "" and rows[0]["b"] == "has-content"
        assert rows[1]["a"] == "now-has-content" and rows[1]["b"] == ""
        assert rows[2]["a"] == "" and rows[2]["b"] == ""

    def test_identical_content_across_versions_one_column(
        self, db: psycopg.Connection, make_table
    ):
        """One column identical across all versions, another changes."""
        t = make_table(
            "gid INT, ver INT, stable TEXT NOT NULL, changing TEXT NOT NULL",
            group_by="gid",
            order_by="ver",
            delta_columns=["stable", "changing"],
        )
        for v in range(1, 8):
            db.execute(
                f"INSERT INTO {t} (gid, ver, stable, changing) "
                f"VALUES (1, {v}, 'never changes', 'version-{v}')"
            )

        rows = db.execute(
            f"SELECT ver, stable, changing FROM {t} ORDER BY ver"
        ).fetchall()
        assert len(rows) == 7
        for row in rows:
            assert row["stable"] == "never changes"
            assert row["changing"] == f"version-{row['ver']}"

    def test_four_delta_columns(self, db: psycopg.Connection, make_table):
        """Table with 4 delta columns all reconstruct correctly."""
        t = make_table(
            "gid INT, ver INT, "
            "w TEXT NOT NULL, x TEXT NOT NULL, y TEXT NOT NULL, z TEXT NOT NULL",
            group_by="gid",
            order_by="ver",
            delta_columns=["w", "x", "y", "z"],
        )
        for v in range(1, 8):
            db.execute(
                f"INSERT INTO {t} (gid, ver, w, x, y, z) "
                f"VALUES (1, {v}, 'W{v}', 'X{v}', 'Y{v}', 'Z{v}')"
            )
        rows = db.execute(
            f"SELECT ver, w, x, y, z FROM {t} ORDER BY ver"
        ).fetchall()
        assert len(rows) == 7
        for row in rows:
            v = row["ver"]
            assert row["w"] == f"W{v}"
            assert row["x"] == f"X{v}"
            assert row["y"] == f"Y{v}"
            assert row["z"] == f"Z{v}"

    def test_insert_returning_multi_delta(self, db: psycopg.Connection, make_table):
        """INSERT RETURNING returns values for all delta columns."""
        t = make_table(
            "gid INT, ver INT, a TEXT NOT NULL, b TEXT NOT NULL",
            group_by="gid",
            order_by="ver",
            delta_columns=["a", "b"],
        )
        row = db.execute(
            f"INSERT INTO {t} (gid, ver, a, b) VALUES (1, 1, 'hello', 'world') "
            f"RETURNING gid, ver, a, b"
        ).fetchone()
        assert row["a"] == "hello"
        assert row["b"] == "world"

    def test_order_by_delta_column(self, db: psycopg.Connection, make_table):
        """ORDER BY on a delta-compressed column in multi-delta table."""
        t = make_table(
            "gid INT, ver INT, name TEXT NOT NULL, priority TEXT NOT NULL",
            group_by="gid",
            order_by="ver",
            delta_columns=["name", "priority"],
        )
        db.execute(f"INSERT INTO {t} VALUES (1, 1, 'charlie', 'low')")
        db.execute(f"INSERT INTO {t} VALUES (1, 2, 'alpha', 'high')")
        db.execute(f"INSERT INTO {t} VALUES (1, 3, 'bravo', 'medium')")

        rows = db.execute(f"SELECT name FROM {t} ORDER BY name").fetchall()
        assert [r["name"] for r in rows] == ["alpha", "bravo", "charlie"]

    def test_delete_preserves_multi_delta_chain(
        self, db: psycopg.Connection, make_table
    ):
        """Deleting rows from a multi-delta table doesn't corrupt remaining chains."""
        t = make_table(
            "gid INT, ver INT, a TEXT NOT NULL, b TEXT NOT NULL",
            group_by="gid",
            order_by="ver",
            delta_columns=["a", "b"],
        )
        for v in range(1, 8):
            db.execute(
                f"INSERT INTO {t} (gid, ver, a, b) "
                f"VALUES (1, {v}, 'A{v}', 'B{v}')"
            )

        # Delete the last 3 versions
        db.execute(f"DELETE FROM {t} WHERE ver >= 5")
        assert row_count(db, t) == 4

        rows = db.execute(
            f"SELECT ver, a, b FROM {t} ORDER BY ver"
        ).fetchall()
        for row in rows:
            v = row["ver"]
            assert row["a"] == f"A{v}"
            assert row["b"] == f"B{v}"
