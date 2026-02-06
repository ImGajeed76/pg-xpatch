"""
Test data type support across group columns, order columns, and delta columns.

Covers:
- Group column types: INT, BIGINT, TEXT, VARCHAR, UUID
- Order column types: INT, BIGINT, SMALLINT, TIMESTAMP, TIMESTAMPTZ
- Delta column types: TEXT, BYTEA, VARCHAR, JSON, JSONB
- Non-delta columns: BOOLEAN, NUMERIC, ARRAY, DATE, etc.
- Special characters in TEXT (newlines, tabs, unicode)
- Empty string in delta column
- NULL in non-delta, non-group columns
"""

from __future__ import annotations

import json

import psycopg
import pytest

from conftest import insert_rows, row_count


# ---------------------------------------------------------------------------
# Group column types
# ---------------------------------------------------------------------------


class TestGroupColumnTypes:
    """Different data types for the group_by column."""

    def test_int_group(self, db: psycopg.Connection, make_table):
        """INT group column with multi-version groups."""
        t = make_table()
        insert_rows(db, t, [(1, 1, "a"), (1, 2, "a2"), (2, 1, "b")])
        assert row_count(db, t) == 3
        assert row_count(db, t, "group_id = 1") == 2
        assert row_count(db, t, "group_id = 2") == 1

    def test_bigint_group(self, db: psycopg.Connection, make_table):
        """BIGINT group column with large values."""
        t = make_table(
            "gid BIGINT, ver INT, content TEXT NOT NULL",
            group_by="gid",
            order_by="ver",
        )
        big_id = 9_223_372_036_854_775_806  # near max BIGINT
        insert_rows(db, t, [
            (big_id, 1, "big group"),
            (big_id, 2, "big group v2"),
        ], columns=["gid", "ver", "content"])
        rows = db.execute(f"SELECT gid, content FROM {t} ORDER BY ver").fetchall()
        assert len(rows) == 2
        assert rows[0]["gid"] == big_id
        assert rows[1]["content"] == "big group v2"

    def test_text_group(self, db: psycopg.Connection, make_table):
        """TEXT group column."""
        t = make_table(
            "category TEXT, ver INT, content TEXT NOT NULL",
            group_by="category",
            order_by="ver",
        )
        insert_rows(db, t, [
            ("docs", 1, "first doc"),
            ("docs", 2, "second doc"),
            ("images", 1, "first image"),
        ], columns=["category", "ver", "content"])
        assert row_count(db, t, "category = 'docs'") == 2
        assert row_count(db, t, "category = 'images'") == 1

    def test_varchar_group(self, db: psycopg.Connection, make_table):
        """VARCHAR group column."""
        t = make_table(
            "tag VARCHAR(50), ver INT, content TEXT NOT NULL",
            group_by="tag",
            order_by="ver",
        )
        insert_rows(db, t, [
            ("alpha", 1, "data a"),
            ("beta", 1, "data b"),
        ], columns=["tag", "ver", "content"])
        assert row_count(db, t) == 2

    def test_uuid_group(self, db: psycopg.Connection, make_table):
        """UUID group column with delta chain across multiple versions."""
        t = make_table(
            "id UUID, ver INT, content TEXT NOT NULL",
            group_by="id",
            order_by="ver",
        )
        u1 = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
        u2 = "b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a22"
        for v in range(1, 4):
            db.execute(
                f"INSERT INTO {t} (id, ver, content) VALUES (%s::uuid, %s, %s)",
                [u1, v, f"u1-v{v}"],
            )
            db.execute(
                f"INSERT INTO {t} (id, ver, content) VALUES (%s::uuid, %s, %s)",
                [u2, v, f"u2-v{v}"],
            )
        assert row_count(db, t) == 6
        # Verify delta chain reconstruction per UUID group
        rows = db.execute(
            f"SELECT ver, content FROM {t} WHERE id = %s::uuid ORDER BY ver",
            [u1],
        ).fetchall()
        assert len(rows) == 3
        for row in rows:
            assert row["content"] == f"u1-v{row['ver']}"


# ---------------------------------------------------------------------------
# Order column types
# ---------------------------------------------------------------------------


class TestOrderColumnTypes:
    """Different data types for the order_by column."""

    def test_int_order(self, db: psycopg.Connection, make_table):
        """INT order column (default)."""
        t = make_table()
        insert_rows(db, t, [(1, 1, "a"), (1, 2, "b")])
        rows = db.execute(
            f"SELECT version FROM {t} ORDER BY version"
        ).fetchall()
        assert [r["version"] for r in rows] == [1, 2]

    def test_bigint_order(self, db: psycopg.Connection, make_table):
        """BIGINT order column."""
        t = make_table(
            "gid INT, ts BIGINT, content TEXT NOT NULL",
            group_by="gid",
            order_by="ts",
        )
        insert_rows(db, t, [
            (1, 1000000000000, "ts1"),
            (1, 2000000000000, "ts2"),
        ], columns=["gid", "ts", "content"])
        rows = db.execute(f"SELECT ts, content FROM {t} ORDER BY ts").fetchall()
        assert rows[0]["ts"] == 1000000000000
        assert rows[1]["content"] == "ts2"

    def test_smallint_order(self, db: psycopg.Connection, make_table):
        """SMALLINT order column."""
        t = make_table(
            "gid INT, ver SMALLINT, content TEXT NOT NULL",
            group_by="gid",
            order_by="ver",
        )
        insert_rows(db, t, [
            (1, 1, "v1"),
            (1, 2, "v2"),
        ], columns=["gid", "ver", "content"])
        rows = db.execute(f"SELECT ver FROM {t} ORDER BY ver").fetchall()
        assert [r["ver"] for r in rows] == [1, 2]

    def test_timestamp_order(self, db: psycopg.Connection, make_table):
        """TIMESTAMP order column."""
        t = make_table(
            "gid INT, ts TIMESTAMP, content TEXT NOT NULL",
            group_by="gid",
            order_by="ts",
        )
        db.execute(
            f"INSERT INTO {t} (gid, ts, content) VALUES "
            f"(1, '2025-01-01 00:00:00', 'new year'), "
            f"(1, '2025-06-15 12:00:00', 'mid year')"
        )
        rows = db.execute(f"SELECT ts, content FROM {t} ORDER BY ts").fetchall()
        assert len(rows) == 2
        assert rows[0]["content"] == "new year"
        assert rows[1]["content"] == "mid year"

    def test_timestamptz_order(self, db: psycopg.Connection, make_table):
        """TIMESTAMPTZ order column."""
        t = make_table(
            "gid INT, ts TIMESTAMPTZ, content TEXT NOT NULL",
            group_by="gid",
            order_by="ts",
        )
        db.execute(
            f"INSERT INTO {t} (gid, ts, content) VALUES "
            f"(1, '2025-01-01 00:00:00+00', 'utc'), "
            f"(1, '2025-01-01 08:00:00+00', 'utc later')"
        )
        rows = db.execute(f"SELECT content FROM {t} ORDER BY ts").fetchall()
        assert rows[0]["content"] == "utc"
        assert rows[1]["content"] == "utc later"


# ---------------------------------------------------------------------------
# Delta column types
# ---------------------------------------------------------------------------


class TestDeltaColumnTypes:
    """Different data types for delta-compressed columns."""

    def test_text_delta(self, db: psycopg.Connection, make_table):
        """TEXT delta column (default)."""
        t = make_table()
        insert_rows(db, t, [(1, 1, "hello"), (1, 2, "hello world")])
        row = db.execute(f"SELECT content FROM {t} WHERE version = 2").fetchone()
        assert row["content"] == "hello world"

    def test_bytea_delta(self, db: psycopg.Connection, make_table):
        """BYTEA delta column stores and retrieves binary data."""
        t = make_table(
            "gid INT, ver INT, data BYTEA NOT NULL",
            group_by="gid",
            order_by="ver",
            delta_columns=["data"],
        )
        binary1 = bytes(range(256))
        binary2 = bytes(range(256)) + b"\xff\xfe\xfd"
        db.execute(
            f"INSERT INTO {t} (gid, ver, data) VALUES (1, 1, %s)",
            [binary1],
        )
        db.execute(
            f"INSERT INTO {t} (gid, ver, data) VALUES (1, 2, %s)",
            [binary2],
        )
        rows = db.execute(f"SELECT ver, data FROM {t} ORDER BY ver").fetchall()
        assert bytes(rows[0]["data"]) == binary1
        assert bytes(rows[1]["data"]) == binary2

    def test_varchar_delta(self, db: psycopg.Connection, make_table):
        """VARCHAR delta column."""
        t = make_table(
            "gid INT, ver INT, body VARCHAR(1000) NOT NULL",
            group_by="gid",
            order_by="ver",
            delta_columns=["body"],
        )
        insert_rows(db, t, [
            (1, 1, "short"),
            (1, 2, "a longer piece of text"),
        ], columns=["gid", "ver", "body"])
        row = db.execute(f"SELECT body FROM {t} WHERE ver = 2").fetchone()
        assert row["body"] == "a longer piece of text"

    def test_json_delta(self, db: psycopg.Connection, make_table):
        """JSON delta column."""
        t = make_table(
            "gid INT, ver INT, doc JSON NOT NULL",
            group_by="gid",
            order_by="ver",
            delta_columns=["doc"],
        )
        db.execute(
            f"INSERT INTO {t} (gid, ver, doc) VALUES (1, 1, %s::json)",
            ['{"key": "value1"}'],
        )
        db.execute(
            f"INSERT INTO {t} (gid, ver, doc) VALUES (1, 2, %s::json)",
            ['{"key": "value2", "extra": true}'],
        )
        row = db.execute(f"SELECT doc FROM {t} WHERE ver = 2").fetchone()
        doc = row["doc"]
        if isinstance(doc, str):
            doc = json.loads(doc)
        assert doc["key"] == "value2"
        assert doc["extra"] is True

    def test_jsonb_delta(self, db: psycopg.Connection, make_table):
        """JSONB delta column."""
        t = make_table(
            "gid INT, ver INT, payload JSONB NOT NULL",
            group_by="gid",
            order_by="ver",
            delta_columns=["payload"],
        )
        db.execute(
            f"INSERT INTO {t} (gid, ver, payload) VALUES (1, 1, %s::jsonb)",
            [json.dumps({"a": 1})],
        )
        db.execute(
            f"INSERT INTO {t} (gid, ver, payload) VALUES (1, 2, %s::jsonb)",
            [json.dumps({"a": 2, "b": 3})],
        )
        row = db.execute(f"SELECT payload FROM {t} WHERE ver = 2").fetchone()
        p = row["payload"]
        if isinstance(p, str):
            p = json.loads(p)
        assert p["a"] == 2
        assert p["b"] == 3


# ---------------------------------------------------------------------------
# Non-delta columns
# ---------------------------------------------------------------------------


class TestNonDeltaColumns:
    """Non-delta columns can be any PostgreSQL type."""

    def test_boolean_column(self, db: psycopg.Connection, make_table):
        """BOOLEAN non-delta column."""
        t = make_table(
            "gid INT, ver INT, content TEXT NOT NULL, active BOOLEAN",
            group_by="gid",
            order_by="ver",
        )
        insert_rows(db, t, [
            (1, 1, "data", True),
            (1, 2, "data2", False),
        ], columns=["gid", "ver", "content", "active"])
        rows = db.execute(f"SELECT ver, active FROM {t} ORDER BY ver").fetchall()
        assert rows[0]["active"] is True
        assert rows[1]["active"] is False

    def test_numeric_column(self, db: psycopg.Connection, make_table):
        """NUMERIC non-delta column."""
        t = make_table(
            "gid INT, ver INT, content TEXT NOT NULL, amount NUMERIC(10,2)",
            group_by="gid",
            order_by="ver",
        )
        from decimal import Decimal
        insert_rows(db, t, [
            (1, 1, "data", Decimal("123.45")),
        ], columns=["gid", "ver", "content", "amount"])
        row = db.execute(f"SELECT amount FROM {t}").fetchone()
        assert float(row["amount"]) == pytest.approx(123.45)

    def test_array_column(self, db: psycopg.Connection, make_table):
        """ARRAY non-delta column."""
        t = make_table(
            "gid INT, ver INT, content TEXT NOT NULL, tags TEXT[]",
            group_by="gid",
            order_by="ver",
        )
        db.execute(
            f"INSERT INTO {t} (gid, ver, content, tags) "
            f"VALUES (1, 1, 'data', ARRAY['tag1', 'tag2', 'tag3'])"
        )
        row = db.execute(f"SELECT tags FROM {t}").fetchone()
        assert row["tags"] == ["tag1", "tag2", "tag3"]

    def test_date_column(self, db: psycopg.Connection, make_table):
        """DATE non-delta column."""
        t = make_table(
            "gid INT, ver INT, content TEXT NOT NULL, created DATE",
            group_by="gid",
            order_by="ver",
        )
        db.execute(
            f"INSERT INTO {t} (gid, ver, content, created) "
            f"VALUES (1, 1, 'data', '2025-06-15')"
        )
        row = db.execute(f"SELECT created FROM {t}").fetchone()
        import datetime
        assert row["created"] == datetime.date(2025, 6, 15)

    def test_null_in_non_delta_non_group(self, db: psycopg.Connection, make_table):
        """NULL in non-delta, non-group columns is allowed."""
        t = make_table(
            "gid INT, ver INT, content TEXT NOT NULL, score INT",
            group_by="gid",
            order_by="ver",
        )
        insert_rows(db, t, [
            (1, 1, "data", None),
        ], columns=["gid", "ver", "content", "score"])
        row = db.execute(f"SELECT score FROM {t}").fetchone()
        assert row["score"] is None


# ---------------------------------------------------------------------------
# Special characters
# ---------------------------------------------------------------------------


class TestSpecialCharacters:
    """Special characters in delta-compressed TEXT columns."""

    def test_newlines_and_tabs(self, db: psycopg.Connection, xpatch_table):
        """Newlines and tabs in content are preserved."""
        t = xpatch_table
        content = "Line1\nLine2\nLine3\tTabbed"
        insert_rows(db, t, [(1, 1, content)])
        row = db.execute(f"SELECT content FROM {t}").fetchone()
        assert row["content"] == content

    def test_unicode_characters(self, db: psycopg.Connection, xpatch_table):
        """Unicode characters (accented, CJK, emoji) are preserved."""
        t = xpatch_table
        content = "caf\u00e9 \u20ac100 \u3042 \U0001f600"
        insert_rows(db, t, [(1, 1, content)])
        row = db.execute(f"SELECT content FROM {t}").fetchone()
        assert row["content"] == content

    def test_empty_string(self, db: psycopg.Connection, xpatch_table):
        """Empty string in delta column is preserved (not converted to NULL)."""
        t = xpatch_table
        insert_rows(db, t, [(1, 1, "")])
        row = db.execute(f"SELECT content FROM {t}").fetchone()
        assert row["content"] == ""

    def test_very_long_unicode(self, db: psycopg.Connection, xpatch_table):
        """Long unicode text (10k chars) preserved across delta chain."""
        t = xpatch_table
        base = "\u00e9\u20ac\u3042" * 3333 + "\u00e9"  # ~10k chars
        insert_rows(db, t, [(1, 1, base)])
        modified = base[:5000] + "CHANGED" + base[5007:]
        insert_rows(db, t, [(1, 2, modified)])

        rows = db.execute(
            f"SELECT version, content FROM {t} ORDER BY version"
        ).fetchall()
        assert rows[0]["content"] == base
        assert rows[1]["content"] == modified

    def test_null_bytes_in_bytea(self, db: psycopg.Connection, make_table):
        """NULL bytes (\\x00) in BYTEA column are preserved."""
        t = make_table(
            "gid INT, ver INT, data BYTEA NOT NULL",
            group_by="gid",
            order_by="ver",
            delta_columns=["data"],
        )
        data = b"\x00\x01\x02\x00\xff\x00"
        db.execute(
            f"INSERT INTO {t} (gid, ver, data) VALUES (1, 1, %s)",
            [data],
        )
        row = db.execute(f"SELECT data FROM {t}").fetchone()
        assert bytes(row["data"]) == data

    def test_backslashes_and_quotes(self, db: psycopg.Connection, xpatch_table):
        """Backslashes and quotes in text content."""
        t = xpatch_table
        content = r'He said "hello\" and C:\path\to\file'
        insert_rows(db, t, [(1, 1, content)])
        row = db.execute(f"SELECT content FROM {t}").fetchone()
        assert row["content"] == content

    def test_sql_injection_safe(self, db: psycopg.Connection, xpatch_table):
        """SQL-injection-like content stored safely."""
        t = xpatch_table
        content = "'; DROP TABLE students; --"
        insert_rows(db, t, [(1, 1, content)])
        row = db.execute(f"SELECT content FROM {t}").fetchone()
        assert row["content"] == content


# ---------------------------------------------------------------------------
# Boundary values and edge cases
# ---------------------------------------------------------------------------


class TestBoundaryValues:
    """Type boundary values and edge cases."""

    def test_bigint_group_max_value(self, db: psycopg.Connection, make_table):
        """BIGINT group at exact INT8_MAX boundary."""
        t = make_table(
            "gid BIGINT, ver INT, content TEXT NOT NULL",
            group_by="gid",
            order_by="ver",
        )
        max_bigint = 9_223_372_036_854_775_807  # INT8_MAX
        insert_rows(db, t, [
            (max_bigint, 1, "max bigint group"),
            (max_bigint, 2, "max bigint v2"),
        ], columns=["gid", "ver", "content"])
        rows = db.execute(f"SELECT gid, content FROM {t} ORDER BY ver").fetchall()
        assert len(rows) == 2
        assert rows[0]["gid"] == max_bigint
        assert rows[1]["content"] == "max bigint v2"

    def test_int_order_negative_values(self, db: psycopg.Connection, make_table):
        """Negative order_by values work correctly."""
        t = make_table()
        insert_rows(db, t, [
            (1, -10, "neg ten"),
            (1, -1, "neg one"),
            (1, 0, "zero"),
            (1, 1, "one"),
        ])
        rows = db.execute(
            f"SELECT version, content FROM {t} ORDER BY version"
        ).fetchall()
        assert len(rows) == 4
        assert [r["version"] for r in rows] == [-10, -1, 0, 1]
        assert rows[0]["content"] == "neg ten"

    def test_smallint_order_boundary(self, db: psycopg.Connection, make_table):
        """SMALLINT order near boundary values."""
        t = make_table(
            "gid INT, ver SMALLINT, content TEXT NOT NULL",
            group_by="gid",
            order_by="ver",
        )
        insert_rows(db, t, [
            (1, 32766, "near max"),
            (1, 32767, "at max"),  # INT2_MAX
        ], columns=["gid", "ver", "content"])
        rows = db.execute(f"SELECT ver, content FROM {t} ORDER BY ver").fetchall()
        assert rows[0]["ver"] == 32766
        assert rows[1]["ver"] == 32767

    def test_timestamp_order_subsecond(self, db: psycopg.Connection, make_table):
        """Timestamps differing only by microseconds are distinguished."""
        t = make_table(
            "gid INT, ts TIMESTAMP, content TEXT NOT NULL",
            group_by="gid",
            order_by="ts",
        )
        db.execute(
            f"INSERT INTO {t} (gid, ts, content) VALUES "
            f"(1, '2025-01-01 00:00:00.000001', 'micro1'), "
            f"(1, '2025-01-01 00:00:00.000002', 'micro2'), "
            f"(1, '2025-01-01 00:00:00.000003', 'micro3')"
        )
        rows = db.execute(f"SELECT content FROM {t} ORDER BY ts").fetchall()
        assert [r["content"] for r in rows] == ["micro1", "micro2", "micro3"]

    def test_text_group_unicode_collation(self, db: psycopg.Connection, make_table):
        """TEXT group with unicode — accented chars form separate groups."""
        t = make_table(
            "category TEXT, ver INT, content TEXT NOT NULL",
            group_by="category",
            order_by="ver",
        )
        insert_rows(db, t, [
            ("cafe", 1, "plain"),
            ("caf\u00e9", 1, "accented"),
        ], columns=["category", "ver", "content"])
        assert row_count(db, t) == 2
        assert row_count(db, t, "category = 'cafe'") == 1
        assert row_count(db, t, "category = E'caf\\u00e9'") == 1

    def test_empty_bytea_delta(self, db: psycopg.Connection, make_table):
        """Empty BYTEA value in delta column."""
        t = make_table(
            "gid INT, ver INT, data BYTEA NOT NULL",
            group_by="gid",
            order_by="ver",
            delta_columns=["data"],
        )
        db.execute(
            f"INSERT INTO {t} (gid, ver, data) VALUES (1, 1, %s)",
            [b""],
        )
        db.execute(
            f"INSERT INTO {t} (gid, ver, data) VALUES (1, 2, %s)",
            [b"now has data"],
        )
        rows = db.execute(f"SELECT ver, data FROM {t} ORDER BY ver").fetchall()
        assert bytes(rows[0]["data"]) == b""
        assert bytes(rows[1]["data"]) == b"now has data"

    def test_bytea_delta_chain(self, db: psycopg.Connection, make_table):
        """BYTEA delta compression works across multiple versions."""
        t = make_table(
            "gid INT, ver INT, data BYTEA NOT NULL",
            group_by="gid",
            order_by="ver",
            delta_columns=["data"],
        )
        base = bytes(range(256)) * 10  # 2560 bytes
        for v in range(1, 6):
            modified = bytearray(base)
            modified[v * 100] = 0xFF  # small change per version
            db.execute(
                f"INSERT INTO {t} (gid, ver, data) VALUES (1, %s, %s)",
                [v, bytes(modified)],
            )
        rows = db.execute(f"SELECT ver, data FROM {t} ORDER BY ver").fetchall()
        assert len(rows) == 5
        for row in rows:
            v = row["ver"]
            expected = bytearray(base)
            expected[v * 100] = 0xFF
            assert bytes(row["data"]) == bytes(expected)

    def test_null_in_non_delta_across_versions(self, db: psycopg.Connection, make_table):
        """NULL in non-delta column survives reconstruction across versions."""
        t = make_table(
            "gid INT, ver INT, content TEXT NOT NULL, score INT",
            group_by="gid",
            order_by="ver",
        )
        insert_rows(db, t, [
            (1, 1, "v1", None),
            (1, 2, "v2", 42),
            (1, 3, "v3", None),
        ], columns=["gid", "ver", "content", "score"])
        rows = db.execute(
            f"SELECT ver, score FROM {t} ORDER BY ver"
        ).fetchall()
        assert rows[0]["score"] is None
        assert rows[1]["score"] == 42
        assert rows[2]["score"] is None
