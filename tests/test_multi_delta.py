"""
Test tables with multiple delta-compressed columns.

Covers:
- Table with 3 delta columns
- Independent column queries
- Different columns change at different versions
- Per-column inspection via xpatch.inspect()
- Mixed delta column types (TEXT + JSONB + BYTEA)
- Content integrity across multi-column delta chains
"""

from __future__ import annotations

import json

import psycopg
import pytest

from conftest import row_count


class TestThreeDeltaColumns:
    """Table with 3 delta-compressed columns."""

    def _make_3col_table(self, db, make_table):
        """Create table with content TEXT NOT NULL, summary TEXT NOT NULL, metadata JSONB."""
        return make_table(
            "doc_id INT, version INT, content TEXT NOT NULL, summary TEXT NOT NULL, metadata JSONB NOT NULL",
            group_by="doc_id",
            order_by="version",
            delta_columns=["content", "summary", "metadata"],
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
        meta = row["metadata"]
        if isinstance(meta, str):
            meta = json.loads(meta)
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
            meta = row["metadata"]
            if isinstance(meta, str):
                meta = json.loads(meta)
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

        # V1
        assert rows[0]["content"] == "Content stays same"
        assert rows[0]["summary"] == "Summary stays same"
        # V2
        assert rows[1]["content"] == "Content changed!"
        assert rows[1]["summary"] == "Summary stays same"
        # V3
        assert rows[2]["content"] == "Content changed!"
        meta3 = rows[2]["metadata"]
        if isinstance(meta3, str):
            meta3 = json.loads(meta3)
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
        """Inspect returns entries for each delta column."""
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
        # Should have entries for both 'content' and 'summary' columns
        col_names = {r["column_name"] for r in rows}
        assert "content" in col_names
        assert "summary" in col_names


class TestMixedDeltaTypes:
    """Mixed delta column types (TEXT + BYTEA)."""

    def test_text_and_bytea_delta(self, db: psycopg.Connection, make_table):
        """Table with TEXT and BYTEA delta columns."""
        t = make_table(
            "gid INT, ver INT, doc TEXT NOT NULL, data BYTEA NOT NULL",
            group_by="gid",
            order_by="ver",
            delta_columns=["doc", "data"],
        )
        db.execute(
            f"INSERT INTO {t} (gid, ver, doc, data) VALUES (1, 1, 'hello', %s)",
            [b"\x01\x02\x03"],
        )
        db.execute(
            f"INSERT INTO {t} (gid, ver, doc, data) VALUES (1, 2, 'world', %s)",
            [b"\x04\x05\x06"],
        )

        rows = db.execute(f"SELECT ver, doc, data FROM {t} ORDER BY ver").fetchall()
        assert rows[0]["doc"] == "hello"
        assert bytes(rows[0]["data"]) == b"\x01\x02\x03"
        assert rows[1]["doc"] == "world"
        assert bytes(rows[1]["data"]) == b"\x04\x05\x06"

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
            meta = row["meta"]
            if isinstance(meta, str):
                meta = json.loads(meta)
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
