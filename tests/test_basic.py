"""
Test basic table creation, configuration, and CRUD operations.

Covers:
- CREATE TABLE USING xpatch
- Auto-added _xp_seq column and indexes
- xpatch.configure() and xpatch.get_config()
- Auto-detection of order_by and delta_columns
- INSERT (single and multiple rows)
- SELECT with ORDER BY, WHERE, COUNT, GROUP BY, column projection
- UPDATE raises error
- NULL group_by value raises error
- _xp_seq populated automatically
- INSERT RETURNING
- COPY FROM/TO (text and binary modes)
- DROP TABLE config cleanup
- ALTER TABLE (ADD/DROP/RENAME COLUMN) regression tests
- Custom schema support (non-public schemas)
"""

from __future__ import annotations

import psycopg
import pytest

import io

from conftest import insert_rows, insert_versions, row_count


# ---------------------------------------------------------------------------
# Table creation
# ---------------------------------------------------------------------------


class TestTableCreation:
    """CREATE TABLE USING xpatch and auto-DDL event triggers."""

    def test_create_table_adds_xp_seq_column(self, db: psycopg.Connection, make_table):
        """The _add_seq_column event trigger auto-adds _xp_seq INT."""
        t = make_table()
        cols = db.execute(
            "SELECT column_name, data_type "
            "FROM information_schema.columns "
            "WHERE table_name = %s ORDER BY ordinal_position",
            [t],
        ).fetchall()
        col_map = {r["column_name"]: r["data_type"] for r in cols}
        assert "_xp_seq" in col_map, f"_xp_seq not found in columns: {list(col_map)}"
        assert col_map["_xp_seq"] == "integer"

    def test_create_table_creates_seq_index(self, db: psycopg.Connection, make_table):
        """Auto-created btree index on _xp_seq."""
        t = make_table()
        indexes = db.execute(
            "SELECT indexname, indexdef FROM pg_indexes WHERE tablename = %s",
            [t],
        ).fetchall()
        idx_names = [r["indexname"] for r in indexes]
        # Either <table>_xp_seq_idx or <table>_xp_group_seq_idx
        has_seq_idx = any("xp_seq" in name or "xp_group_seq" in name for name in idx_names)
        assert has_seq_idx, f"No _xp_seq index found. Indexes: {idx_names}"

    def test_table_uses_xpatch_am(self, db: psycopg.Connection, make_table):
        """Table's access method is xpatch."""
        t = make_table()
        row = db.execute(
            "SELECT am.amname "
            "FROM pg_class c JOIN pg_am am ON c.relam = am.oid "
            "WHERE c.relname = %s",
            [t],
        ).fetchone()
        assert row is not None
        assert row["amname"] == "xpatch"

    def test_empty_table_has_zero_rows(self, db: psycopg.Connection, xpatch_table):
        """A freshly created xpatch table has no rows."""
        assert row_count(db, xpatch_table) == 0


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


class TestConfiguration:
    """xpatch.configure() and xpatch.get_config()."""

    def test_get_config_returns_explicit_config(self, db: psycopg.Connection, make_table):
        """Explicitly set config values are returned by get_config."""
        t = make_table(
            "doc_id INT, ver INT, body TEXT NOT NULL",
            group_by="doc_id",
            order_by="ver",
            delta_columns=["body"],
            keyframe_every=50,
            enable_zstd=False,
        )
        cfg = db.execute(
            "SELECT * FROM xpatch.get_config(%s::regclass)", [t]
        ).fetchone()
        assert cfg["group_by"] == "doc_id"
        assert cfg["order_by"] == "ver"
        assert cfg["delta_columns"] == ["body"]
        assert cfg["keyframe_every"] == 50
        assert cfg["enable_zstd"] is False

    def test_get_config_default_values(self, db: psycopg.Connection, make_table):
        """Default config uses keyframe_every=100, enable_zstd=true."""
        t = make_table()
        cfg = db.execute(
            "SELECT * FROM xpatch.get_config(%s::regclass)", [t]
        ).fetchone()
        assert cfg["keyframe_every"] == 100
        assert cfg["enable_zstd"] is True

    def test_auto_detect_order_by(self, db: psycopg.Connection):
        """Auto-detects order_by — verified by describe() and INSERT/SELECT."""
        t = "test_auto_ob"
        db.execute(f"CREATE TABLE {t} (gid INT, name TEXT NOT NULL, seq INT) USING xpatch")
        # configure without explicit order_by — C code auto-detects 'seq'
        db.execute(f"SELECT xpatch.configure('{t}', group_by => 'gid')")
        # Verify auto-detection picked the right column
        desc = db.execute(
            f"SELECT * FROM xpatch.describe('{t}'::regclass)"
        ).fetchall()
        ob_row = [r for r in desc if r["property"] == "order_by"]
        assert len(ob_row) == 1
        assert ob_row[0]["value"] == "seq", (
            f"Expected auto-detected order_by='seq', got '{ob_row[0]['value']}'"
        )
        # And INSERT + SELECT should succeed
        db.execute(f"INSERT INTO {t} (gid, name, seq) VALUES (1, 'hello', 1)")
        db.execute(f"INSERT INTO {t} (gid, name, seq) VALUES (1, 'world', 2)")
        rows = db.execute(f"SELECT name FROM {t} ORDER BY seq").fetchall()
        assert len(rows) == 2
        assert rows[0]["name"] == "hello"

    def test_auto_detect_delta_columns(self, db: psycopg.Connection):
        """Auto-detects delta_columns — verified by describe()."""
        t = "test_auto_dc"
        db.execute(
            f"CREATE TABLE {t} (gid INT, ver INT, body TEXT NOT NULL, data BYTEA NOT NULL, doc JSONB NOT NULL) "
            f"USING xpatch"
        )
        db.execute(f"SELECT xpatch.configure('{t}', group_by => 'gid', order_by => 'ver')")
        # Verify auto-detection via describe — check delta_columns property specifically
        desc = db.execute(
            f"SELECT * FROM xpatch.describe('{t}'::regclass)"
        ).fetchall()
        delta_rows = [r for r in desc if r["property"] == "delta_columns"]
        assert len(delta_rows) == 1, f"Expected 1 delta_columns row, got {len(delta_rows)}"
        delta_value = delta_rows[0]["value"]
        for col in ("body", "data", "doc"):
            assert col in delta_value, f"{col} not in delta_columns: {delta_value}"

    def test_configure_creates_composite_index(self, db: psycopg.Connection, make_table):
        """configure() creates (group_by, _xp_seq) composite index."""
        t = make_table()
        indexes = db.execute(
            "SELECT indexdef FROM pg_indexes WHERE tablename = %s",
            [t],
        ).fetchall()
        defs = [r["indexdef"] for r in indexes]
        has_composite = any(
            "group_id" in d and "_xp_seq" in d for d in defs
        )
        assert has_composite, f"No composite (group_id, _xp_seq) index. Defs: {defs}"

    def test_config_stored_in_table_config(self, db: psycopg.Connection, make_table):
        """Config is persisted in xpatch.table_config."""
        t = make_table()
        row = db.execute(
            "SELECT table_name FROM xpatch.table_config WHERE table_name = %s", [t]
        ).fetchone()
        assert row is not None


# ---------------------------------------------------------------------------
# INSERT operations
# ---------------------------------------------------------------------------


class TestInsert:
    """INSERT into xpatch tables."""

    def test_insert_single_row(self, db: psycopg.Connection, xpatch_table):
        """Single row INSERT and SELECT."""
        t = xpatch_table
        insert_rows(db, t, [(1, 1, "hello")])
        rows = db.execute(f"SELECT * FROM {t}").fetchall()
        assert len(rows) == 1
        assert rows[0]["group_id"] == 1
        assert rows[0]["version"] == 1
        assert rows[0]["content"] == "hello"

    def test_insert_multiple_rows_same_group(self, db: psycopg.Connection, xpatch_table):
        """Multiple rows in the same group form a version chain."""
        t = xpatch_table
        insert_versions(db, t, group_id=1, count=5)
        assert row_count(db, t) == 5
        rows = db.execute(
            f"SELECT version, content FROM {t} WHERE group_id = 1 ORDER BY version"
        ).fetchall()
        versions = [r["version"] for r in rows]
        assert versions == [1, 2, 3, 4, 5]

    def test_insert_multiple_groups(self, db: psycopg.Connection, xpatch_table):
        """Rows in different groups are independent."""
        t = xpatch_table
        insert_versions(db, t, group_id=1, count=3)
        insert_versions(db, t, group_id=2, count=3)
        assert row_count(db, t) == 6
        assert row_count(db, t, "group_id = 1") == 3
        assert row_count(db, t, "group_id = 2") == 3

    def test_xp_seq_populated_automatically(self, db: psycopg.Connection, xpatch_table):
        """_xp_seq is auto-incremented per group starting at 1."""
        t = xpatch_table
        insert_versions(db, t, group_id=1, count=3)
        insert_versions(db, t, group_id=2, count=2)
        # Group 1: _xp_seq 1,2,3; Group 2: _xp_seq 1,2
        g1_seqs = db.execute(
            f"SELECT _xp_seq FROM {t} WHERE group_id = 1 ORDER BY _xp_seq"
        ).fetchall()
        g2_seqs = db.execute(
            f"SELECT _xp_seq FROM {t} WHERE group_id = 2 ORDER BY _xp_seq"
        ).fetchall()
        assert [r["_xp_seq"] for r in g1_seqs] == [1, 2, 3]
        assert [r["_xp_seq"] for r in g2_seqs] == [1, 2]

    def test_insert_returning(self, db: psycopg.Connection, xpatch_table):
        """INSERT ... RETURNING returns the inserted row's user columns."""
        t = xpatch_table
        row = db.execute(
            f"INSERT INTO {t} (group_id, version, content) "
            f"VALUES (1, 1, 'test') RETURNING group_id, version, content"
        ).fetchone()
        assert row["group_id"] == 1
        assert row["version"] == 1
        assert row["content"] == "test"

    def test_xp_seq_correct_via_select(self, db: psycopg.Connection, xpatch_table):
        """_xp_seq is correct when read back via SELECT."""
        t = xpatch_table
        for v in range(1, 4):
            insert_rows(db, t, [(1, v, f"v{v}")])

        rows = db.execute(
            f"SELECT version, _xp_seq FROM {t} ORDER BY _xp_seq"
        ).fetchall()
        for i, row in enumerate(rows):
            assert row["_xp_seq"] == i + 1

    def test_null_group_raises_error(self, db: psycopg.Connection, xpatch_table):
        """NULL value in group_by column raises an error."""
        t = xpatch_table
        with pytest.raises(psycopg.errors.Error, match="(?i)null.*group"):
            db.execute(
                f"INSERT INTO {t} (group_id, version, content) VALUES (NULL, 1, 'x')"
            )


# ---------------------------------------------------------------------------
# SELECT operations
# ---------------------------------------------------------------------------


class TestSelect:
    """SELECT from xpatch tables — various query patterns."""

    def test_select_order_by(self, db: psycopg.Connection, xpatch_table):
        """SELECT with ORDER BY returns rows in correct order."""
        t = xpatch_table
        insert_versions(db, t, group_id=1, count=5)
        rows = db.execute(
            f"SELECT version FROM {t} ORDER BY version"
        ).fetchall()
        assert [r["version"] for r in rows] == [1, 2, 3, 4, 5]

    def test_select_order_by_desc(self, db: psycopg.Connection, xpatch_table):
        """SELECT ORDER BY DESC returns rows in reverse order."""
        t = xpatch_table
        insert_versions(db, t, group_id=1, count=5)
        rows = db.execute(
            f"SELECT version FROM {t} ORDER BY version DESC"
        ).fetchall()
        assert [r["version"] for r in rows] == [5, 4, 3, 2, 1]

    def test_select_where_group(self, db: psycopg.Connection, xpatch_table):
        """WHERE clause on group column filters correctly."""
        t = xpatch_table
        insert_versions(db, t, group_id=1, count=3)
        insert_versions(db, t, group_id=2, count=3)
        rows = db.execute(
            f"SELECT * FROM {t} WHERE group_id = 2"
        ).fetchall()
        assert len(rows) == 3
        assert all(r["group_id"] == 2 for r in rows)

    def test_select_where_delta_column(self, db: psycopg.Connection, xpatch_table):
        """WHERE clause on delta-compressed column filters correctly."""
        t = xpatch_table
        insert_rows(db, t, [
            (1, 1, "alpha"),
            (1, 2, "beta"),
            (1, 3, "gamma"),
        ])
        rows = db.execute(
            f"SELECT content FROM {t} WHERE content = 'beta'"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0]["content"] == "beta"

    def test_select_where_like_on_delta(self, db: psycopg.Connection, xpatch_table):
        """LIKE on delta-compressed column works."""
        t = xpatch_table
        insert_rows(db, t, [
            (1, 1, "hello world"),
            (1, 2, "hello there"),
            (1, 3, "goodbye world"),
        ])
        rows = db.execute(
            f"SELECT content FROM {t} WHERE content LIKE 'hello%%' ORDER BY version"
        ).fetchall()
        assert len(rows) == 2
        assert rows[0]["content"] == "hello world"
        assert rows[1]["content"] == "hello there"

    def test_select_count(self, db: psycopg.Connection, xpatch_table):
        """COUNT(*) returns correct count."""
        t = xpatch_table
        insert_versions(db, t, group_id=1, count=10)
        cnt = row_count(db, t)
        assert cnt == 10

    def test_select_count_group_by(self, db: psycopg.Connection, xpatch_table):
        """COUNT with GROUP BY returns per-group counts."""
        t = xpatch_table
        insert_versions(db, t, group_id=1, count=5)
        insert_versions(db, t, group_id=2, count=3)
        rows = db.execute(
            f"SELECT group_id, COUNT(*) as cnt FROM {t} GROUP BY group_id ORDER BY group_id"
        ).fetchall()
        assert rows[0]["group_id"] == 1 and rows[0]["cnt"] == 5
        assert rows[1]["group_id"] == 2 and rows[1]["cnt"] == 3

    def test_select_column_projection(self, db: psycopg.Connection, xpatch_table):
        """Selecting a subset of columns works."""
        t = xpatch_table
        insert_rows(db, t, [(1, 1, "test")])
        row = db.execute(f"SELECT group_id, content FROM {t}").fetchone()
        assert set(row.keys()) == {"group_id", "content"}
        assert row["content"] == "test"

    def test_select_limit_offset(self, db: psycopg.Connection, xpatch_table):
        """LIMIT and OFFSET work correctly."""
        t = xpatch_table
        insert_versions(db, t, group_id=1, count=10)
        rows = db.execute(
            f"SELECT version FROM {t} ORDER BY version LIMIT 3 OFFSET 2"
        ).fetchall()
        assert [r["version"] for r in rows] == [3, 4, 5]

    def test_select_distinct_on_latest(self, db: psycopg.Connection, xpatch_table):
        """DISTINCT ON pattern to get latest version per group."""
        t = xpatch_table
        insert_versions(db, t, group_id=1, count=5)
        insert_versions(db, t, group_id=2, count=3)
        rows = db.execute(
            f"SELECT DISTINCT ON (group_id) group_id, version, content "
            f"FROM {t} ORDER BY group_id, version DESC"
        ).fetchall()
        assert len(rows) == 2
        assert rows[0]["group_id"] == 1 and rows[0]["version"] == 5
        assert rows[1]["group_id"] == 2 and rows[1]["version"] == 3

    def test_select_exists(self, db: psycopg.Connection, xpatch_table):
        """EXISTS subquery on xpatch table."""
        t = xpatch_table
        insert_rows(db, t, [(1, 1, "test")])
        row = db.execute(
            f"SELECT EXISTS (SELECT 1 FROM {t} WHERE group_id = 1) AS found"
        ).fetchone()
        assert row["found"] is True

        row = db.execute(
            f"SELECT EXISTS (SELECT 1 FROM {t} WHERE group_id = 999) AS found"
        ).fetchone()
        assert row["found"] is False

    def test_select_case_expression(self, db: psycopg.Connection, xpatch_table):
        """CASE expression on delta-compressed content."""
        t = xpatch_table
        insert_rows(db, t, [
            (1, 1, "short"),
            (1, 2, "a much longer piece of content here"),
        ])
        rows = db.execute(
            f"SELECT version, "
            f"  CASE WHEN length(content) > 10 THEN 'long' ELSE 'short' END AS label "
            f"FROM {t} ORDER BY version"
        ).fetchall()
        assert rows[0]["label"] == "short"
        assert rows[1]["label"] == "long"

    def test_select_aggregates_on_delta(self, db: psycopg.Connection, xpatch_table):
        """Aggregate functions work on delta-compressed columns."""
        t = xpatch_table
        insert_rows(db, t, [
            (1, 1, "a"),
            (1, 2, "bb"),
            (1, 3, "ccc"),
        ])
        row = db.execute(
            f"SELECT MIN(length(content)) AS mn, "
            f"  MAX(length(content)) AS mx, "
            f"  SUM(length(content)) AS sm "
            f"FROM {t}"
        ).fetchone()
        assert row["mn"] == 1
        assert row["mx"] == 3
        assert row["sm"] == 6

    def test_select_join_with_heap_table(self, db: psycopg.Connection, xpatch_table):
        """JOIN between xpatch table and a regular heap table."""
        t = xpatch_table
        insert_versions(db, t, group_id=1, count=3)
        insert_versions(db, t, group_id=2, count=2)

        db.execute("CREATE TABLE groups (id INT PRIMARY KEY, name TEXT)")
        db.execute("INSERT INTO groups VALUES (1, 'Alpha'), (2, 'Beta')")

        rows = db.execute(
            f"SELECT g.name, x.version, x.content "
            f"FROM {t} x JOIN groups g ON x.group_id = g.id "
            f"ORDER BY g.name, x.version"
        ).fetchall()
        assert len(rows) == 5
        assert rows[0]["name"] == "Alpha" and rows[0]["version"] == 1
        assert rows[3]["name"] == "Beta" and rows[3]["version"] == 1

    def test_select_subquery_in(self, db: psycopg.Connection, xpatch_table):
        """IN subquery works."""
        t = xpatch_table
        insert_versions(db, t, group_id=1, count=3)
        insert_versions(db, t, group_id=2, count=3)
        insert_versions(db, t, group_id=3, count=3)

        rows = db.execute(
            f"SELECT DISTINCT group_id FROM {t} "
            f"WHERE group_id IN (SELECT group_id FROM {t} WHERE version = 3) "
            f"ORDER BY group_id"
        ).fetchall()
        assert [r["group_id"] for r in rows] == [1, 2, 3]

    def test_select_correlated_subquery(self, db: psycopg.Connection, xpatch_table):
        """Correlated subquery on xpatch table."""
        t = xpatch_table
        insert_versions(db, t, group_id=1, count=5)
        insert_versions(db, t, group_id=2, count=3)

        # Get max version per group via correlated subquery
        rows = db.execute(
            f"SELECT group_id, version FROM {t} a "
            f"WHERE version = (SELECT MAX(version) FROM {t} b WHERE b.group_id = a.group_id) "
            f"ORDER BY group_id"
        ).fetchall()
        assert len(rows) == 2
        assert rows[0]["version"] == 5
        assert rows[1]["version"] == 3


# ---------------------------------------------------------------------------
# UPDATE blocked
# ---------------------------------------------------------------------------


class TestUpdate:
    """UPDATE operations should be blocked."""

    def test_update_raises_error(self, db: psycopg.Connection, xpatch_table):
        """UPDATE on xpatch table raises a clear error."""
        t = xpatch_table
        insert_rows(db, t, [(1, 1, "original")])
        with pytest.raises(psycopg.errors.FeatureNotSupported, match="(?i)update.*not supported"):
            db.execute(f"UPDATE {t} SET content = 'modified' WHERE group_id = 1")


# ---------------------------------------------------------------------------
# Version semantics (since 0.3.0: versions not required to be increasing)
# ---------------------------------------------------------------------------


class TestVersionSemantics:
    """Version column behavior (order_by column)."""

    def test_duplicate_versions_allowed(self, db: psycopg.Connection, xpatch_table):
        """Since 0.3.0: duplicate version values are allowed."""
        t = xpatch_table
        insert_rows(db, t, [
            (1, 1, "first v1"),
            (1, 1, "second v1"),
        ])
        assert row_count(db, t) == 2

    def test_non_increasing_versions_allowed(self, db: psycopg.Connection, xpatch_table):
        """Since 0.3.0: non-increasing version values are allowed."""
        t = xpatch_table
        insert_rows(db, t, [
            (1, 5, "high first"),
            (1, 2, "low second"),
        ])
        assert row_count(db, t) == 2
        rows = db.execute(
            f"SELECT version FROM {t} ORDER BY _xp_seq"
        ).fetchall()
        assert rows[0]["version"] == 5
        assert rows[1]["version"] == 2

    def test_null_version_allowed(self, db: psycopg.Connection, xpatch_table):
        """NULL in the version (order_by) column is allowed and content reconstructs."""
        t = xpatch_table
        insert_rows(db, t, [(1, None, "no version")])
        assert row_count(db, t) == 1
        row = db.execute(f"SELECT content FROM {t}").fetchone()
        assert row["content"] == "no version"


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    """Edge cases for basic operations."""

    def test_insert_empty_content(self, db: psycopg.Connection, xpatch_table):
        """Empty string in delta column round-trips correctly."""
        t = xpatch_table
        insert_rows(db, t, [(1, 1, "")])
        row = db.execute(f"SELECT content FROM {t}").fetchone()
        assert row["content"] == ""

    def test_insert_large_content_toast(self, db: psycopg.Connection, xpatch_table):
        """Content exceeding TOAST threshold (~2KB) round-trips correctly."""
        t = xpatch_table
        large = "X" * 10_000  # Well above TOAST threshold
        insert_rows(db, t, [(1, 1, large)])
        row = db.execute(f"SELECT content FROM {t}").fetchone()
        assert row["content"] == large

    def test_insert_large_content_multiple_versions(self, db: psycopg.Connection, xpatch_table):
        """Multiple TOAST-sized versions in same group reconstruct correctly."""
        t = xpatch_table
        for v in range(1, 4):
            insert_rows(db, t, [(1, v, f"{'A' * 5000}-v{v}")])
        rows = db.execute(
            f"SELECT version, content FROM {t} ORDER BY version"
        ).fetchall()
        for i, row in enumerate(rows):
            assert row["content"] == f"{'A' * 5000}-v{i + 1}"

    def test_select_star_includes_xp_seq(self, db: psycopg.Connection, xpatch_table):
        """SELECT * includes the _xp_seq column."""
        t = xpatch_table
        insert_rows(db, t, [(1, 1, "test")])
        row = db.execute(f"SELECT * FROM {t}").fetchone()
        assert "_xp_seq" in row.keys()
        assert row["_xp_seq"] is not None

    def test_select_where_on_xp_seq(self, db: psycopg.Connection, xpatch_table):
        """Filtering on _xp_seq returns the correct row."""
        t = xpatch_table
        insert_versions(db, t, group_id=1, count=5)
        row = db.execute(
            f"SELECT version FROM {t} WHERE _xp_seq = 3"
        ).fetchone()
        assert row is not None
        assert row["version"] == 3

    def test_keyframe_boundary_reconstruction(self, db: psycopg.Connection, make_table):
        """Data at and around keyframe boundaries reconstructs correctly."""
        t = make_table(keyframe_every=5)
        # With keyframe_every=5: seq 1 and 6 are keyframes
        for v in range(1, 12):
            insert_rows(db, t, [(1, v, f"content-{v}")])
        rows = db.execute(
            f"SELECT version, content FROM {t} ORDER BY version"
        ).fetchall()
        assert len(rows) == 11
        for i, row in enumerate(rows):
            assert row["content"] == f"content-{i + 1}", (
                f"Mismatch at version {i + 1}: expected 'content-{i + 1}', got '{row['content']}'"
            )

    def test_multi_row_batch_insert(self, db: psycopg.Connection, xpatch_table):
        """Multi-row INSERT VALUES (...), (...) works correctly."""
        t = xpatch_table
        db.execute(
            f"INSERT INTO {t} (group_id, version, content) VALUES "
            f"(1, 1, 'a'), (1, 2, 'b'), (1, 3, 'c')"
        )
        rows = db.execute(
            f"SELECT version, content FROM {t} ORDER BY version"
        ).fetchall()
        assert len(rows) == 3
        assert [r["content"] for r in rows] == ["a", "b", "c"]

    def test_configure_idempotent(self, db: psycopg.Connection, make_table):
        """Calling configure() twice on the same table does not corrupt data."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=3)
        # Reconfigure with same parameters
        db.execute(
            f"SELECT xpatch.configure('{t}', "
            f"group_by => 'group_id', order_by => 'version')"
        )
        # Data should still be readable
        assert row_count(db, t) == 3
        rows = db.execute(
            f"SELECT version, content FROM {t} ORDER BY version"
        ).fetchall()
        assert [r["version"] for r in rows] == [1, 2, 3]

    def test_many_versions_in_group(self, db: psycopg.Connection, xpatch_table):
        """250 rows spanning multiple keyframe intervals reconstruct correctly."""
        t = xpatch_table
        # Default keyframe_every=100, so keyframes at seq 1, 101, 201
        for v in range(1, 251):
            insert_rows(db, t, [(1, v, f"v{v}")])
        assert row_count(db, t) == 250
        # Spot-check around keyframe boundaries
        for check_v in [1, 2, 99, 100, 101, 102, 199, 200, 201, 250]:
            row = db.execute(
                f"SELECT content FROM {t} WHERE version = {check_v}"
            ).fetchone()
            assert row is not None, f"Missing version {check_v}"
            assert row["content"] == f"v{check_v}", (
                f"Version {check_v}: expected 'v{check_v}', got '{row['content']}'"
            )


# ---------------------------------------------------------------------------
# COPY FROM/TO
# ---------------------------------------------------------------------------


class TestCopyFromTo:
    """COPY exercises the multi_insert path in xpatch_tam.c."""

    def test_copy_from_stdin(self, db: psycopg.Connection, make_table):
        """COPY FROM STDIN inserts rows correctly through multi_insert."""
        t = make_table()
        # Build TSV data: group_id, version, content
        lines = []
        for v in range(1, 11):
            lines.append(f"1\t{v}\tCopy version {v}")
        tsv = "\n".join(lines) + "\n"

        with db.cursor() as cur:
            with cur.copy(
                f"COPY {t} (group_id, version, content) FROM STDIN"
            ) as copy:
                copy.write(tsv.encode())

        assert row_count(db, t) == 10
        rows = db.execute(
            f"SELECT version, content FROM {t} ORDER BY version"
        ).fetchall()
        for row in rows:
            assert row["content"] == f"Copy version {row['version']}"

    def test_copy_to_stdout(self, db: psycopg.Connection, make_table):
        """COPY TO STDOUT exports delta-reconstructed content."""
        t = make_table()
        for v in range(1, 6):
            insert_rows(db, t, [(1, v, f"Export v{v}")])

        buf = io.BytesIO()
        with db.cursor() as cur:
            with cur.copy(
                f"COPY {t} (group_id, version, content) TO STDOUT"
            ) as copy:
                for data in copy:
                    buf.write(data)

        output = buf.getvalue().decode()
        lines = [l for l in output.strip().split("\n") if l]
        assert len(lines) == 5
        for line in lines:
            parts = line.split("\t")
            assert len(parts) == 3
            gid, ver, content = parts
            assert content == f"Export v{ver}"

    def test_copy_from_multiple_groups(self, db: psycopg.Connection, make_table):
        """COPY FROM with multiple groups inserts correctly."""
        t = make_table()
        lines = []
        for g in range(1, 4):
            for v in range(1, 6):
                lines.append(f"{g}\t{v}\tg{g}v{v}")
        tsv = "\n".join(lines) + "\n"

        with db.cursor() as cur:
            with cur.copy(
                f"COPY {t} (group_id, version, content) FROM STDIN"
            ) as copy:
                copy.write(tsv.encode())

        assert row_count(db, t) == 15
        for g in range(1, 4):
            rows = db.execute(
                f"SELECT version, content FROM {t} "
                f"WHERE group_id = {g} ORDER BY version"
            ).fetchall()
            assert len(rows) == 5
            for row in rows:
                assert row["content"] == f"g{g}v{row['version']}"

    def test_copy_round_trip(self, db: psycopg.Connection, make_table):
        """COPY TO then COPY FROM produces identical data."""
        t1 = make_table()
        for v in range(1, 6):
            insert_rows(db, t1, [(1, v, f"Round trip v{v}")])

        # Export
        buf = io.BytesIO()
        with db.cursor() as cur:
            with cur.copy(
                f"COPY {t1} (group_id, version, content) TO STDOUT"
            ) as copy:
                for data in copy:
                    buf.write(data)

        # Import into a new table
        t2 = make_table()
        buf.seek(0)
        with db.cursor() as cur:
            with cur.copy(
                f"COPY {t2} (group_id, version, content) FROM STDIN"
            ) as copy:
                copy.write(buf.read())

        # Verify identical
        rows1 = db.execute(
            f"SELECT version, content FROM {t1} ORDER BY version"
        ).fetchall()
        rows2 = db.execute(
            f"SELECT version, content FROM {t2} ORDER BY version"
        ).fetchall()
        assert len(rows1) == len(rows2) == 5
        for r1, r2 in zip(rows1, rows2):
            assert r1["version"] == r2["version"]
            assert r1["content"] == r2["content"]


# ---------------------------------------------------------------------------
# DROP TABLE config cleanup
# ---------------------------------------------------------------------------


class TestDropTableCleanup:
    """DROP TABLE triggers xpatch_cleanup_on_drop event trigger."""

    def test_drop_table_removes_config(self, db: psycopg.Connection, make_table):
        """Dropping an xpatch table removes its entry from xpatch.table_config."""
        t = make_table()
        insert_rows(db, t, [(1, 1, "before drop")])

        # Verify config exists
        cfg = db.execute(
            f"SELECT * FROM xpatch.table_config WHERE table_name = '{t}'"
        ).fetchone()
        assert cfg is not None, "Config should exist before DROP"

        # Drop the table
        db.execute(f"DROP TABLE {t}")

        # Verify config is cleaned up
        cfg = db.execute(
            f"SELECT * FROM xpatch.table_config WHERE table_name = '{t}'"
        ).fetchone()
        assert cfg is None, "Config should be removed after DROP TABLE"

    def test_drop_table_cascade_removes_config(self, db: psycopg.Connection, make_table):
        """DROP TABLE CASCADE also cleans up config."""
        t = make_table()
        insert_rows(db, t, [(1, 1, "cascade")])

        db.execute(f"DROP TABLE {t} CASCADE")

        cfg = db.execute(
            f"SELECT * FROM xpatch.table_config WHERE table_name = '{t}'"
        ).fetchone()
        assert cfg is None, "Config should be removed after DROP TABLE CASCADE"

    def test_drop_multiple_tables(self, db: psycopg.Connection, make_table):
        """Dropping multiple xpatch tables cleans up all configs."""
        t1 = make_table()
        t2 = make_table()
        insert_rows(db, t1, [(1, 1, "t1")])
        insert_rows(db, t2, [(1, 1, "t2")])

        db.execute(f"DROP TABLE {t1}, {t2}")

        for t in (t1, t2):
            cfg = db.execute(
                f"SELECT * FROM xpatch.table_config WHERE table_name = '{t}'"
            ).fetchone()
            assert cfg is None, f"Config for {t} should be removed"


# ---------------------------------------------------------------------------
# ALTER TABLE — regression tests
# ---------------------------------------------------------------------------


class TestAlterTable:
    """ALTER TABLE ADD/DROP/RENAME COLUMN works correctly on xpatch tables.

    Despite storing column attribute numbers (attnums) in the config cache,
    PostgreSQL's relcache invalidation triggers config re-reads so attnums
    stay correct after ALTER TABLE.
    """

    def test_add_column_then_insert(self, db: psycopg.Connection, make_table):
        """Adding a column doesn't break subsequent INSERTs."""
        t = make_table()
        insert_rows(db, t, [(1, 1, "before-alter")])

        db.execute(f"ALTER TABLE {t} ADD COLUMN metadata TEXT DEFAULT ''")

        insert_rows(db, t, [(1, 2, "after-alter")],
                    columns=["group_id", "version", "content"])

        rows = db.execute(
            f"SELECT version, content FROM {t} WHERE group_id = 1 ORDER BY version"
        ).fetchall()
        assert len(rows) == 2
        assert rows[0]["content"] == "before-alter"
        assert rows[1]["content"] == "after-alter"

    def test_drop_non_delta_column_then_insert(
        self, db: psycopg.Connection, make_table
    ):
        """Dropping a non-delta column doesn't break the table."""
        t = make_table(
            "group_id INT, version INT, extra INT, content TEXT NOT NULL",
        )
        insert_rows(db, t, [(1, 1, 42, "v1")],
                    columns=["group_id", "version", "extra", "content"])

        db.execute(f"ALTER TABLE {t} DROP COLUMN extra")

        insert_rows(db, t, [(1, 2, "v2")],
                    columns=["group_id", "version", "content"])

        rows = db.execute(
            f"SELECT version, content FROM {t} WHERE group_id = 1 ORDER BY version"
        ).fetchall()
        assert len(rows) == 2
        assert rows[1]["content"] == "v2"

    def test_rename_delta_column_then_insert(
        self, db: psycopg.Connection, make_table
    ):
        """Renaming a delta column doesn't break inserts or reads."""
        t = make_table()
        insert_rows(db, t, [(1, 1, "v1")])

        db.execute(f"ALTER TABLE {t} RENAME COLUMN content TO body")

        db.execute(
            f"INSERT INTO {t} (group_id, version, body) VALUES (1, 2, 'v2')"
        )

        rows = db.execute(
            f"SELECT version, body FROM {t} WHERE group_id = 1 ORDER BY version"
        ).fetchall()
        assert len(rows) == 2
        assert rows[1]["body"] == "v2"


# ---------------------------------------------------------------------------
# Custom schemas — regression tests
# ---------------------------------------------------------------------------


class TestCustomSchemas:
    """Tables in non-public schemas work correctly.

    The index name lookup uses pg_class with schema filtering,
    not just name matching.
    """

    def test_table_in_custom_schema(self, db: psycopg.Connection):
        """A table in a non-public schema works correctly."""
        schema = "test_schema"
        table = "docs"

        db.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        try:
            db.execute(
                f"CREATE TABLE {schema}.{table} "
                f"(group_id INT, version INT, content TEXT NOT NULL) USING xpatch"
            )
            db.execute(
                f"SELECT xpatch.configure('{schema}.{table}', "
                f"group_by => 'group_id', order_by => 'version')"
            )

            db.execute(
                f"INSERT INTO {schema}.{table} (group_id, version, content) "
                f"VALUES (1, 1, 'schema-test')"
            )
            db.execute(
                f"INSERT INTO {schema}.{table} (group_id, version, content) "
                f"VALUES (1, 2, 'schema-test-v2')"
            )

            rows = db.execute(
                f"SELECT version, content FROM {schema}.{table} "
                f"WHERE group_id = 1 ORDER BY version"
            ).fetchall()
            assert len(rows) == 2
            assert rows[0]["content"] == "schema-test"
            assert rows[1]["content"] == "schema-test-v2"
        finally:
            db.execute(f"DROP TABLE IF EXISTS {schema}.{table}")
            db.execute(f"DROP SCHEMA IF EXISTS {schema}")

    def test_same_table_name_different_schemas(self, db: psycopg.Connection):
        """Two tables with same name in different schemas don't interfere."""
        db.execute("CREATE SCHEMA IF NOT EXISTS schema_a")
        db.execute("CREATE SCHEMA IF NOT EXISTS schema_b")
        try:
            for s in ("schema_a", "schema_b"):
                db.execute(
                    f"CREATE TABLE {s}.docs "
                    f"(group_id INT, version INT, content TEXT NOT NULL) USING xpatch"
                )
                db.execute(
                    f"SELECT xpatch.configure('{s}.docs', "
                    f"group_by => 'group_id', order_by => 'version')"
                )

            db.execute(
                "INSERT INTO schema_a.docs (group_id, version, content) "
                "VALUES (1, 1, 'from-schema-a')"
            )
            db.execute(
                "INSERT INTO schema_b.docs (group_id, version, content) "
                "VALUES (1, 1, 'from-schema-b')"
            )

            rows_a = db.execute(
                "SELECT content FROM schema_a.docs WHERE group_id = 1"
            ).fetchall()
            rows_b = db.execute(
                "SELECT content FROM schema_b.docs WHERE group_id = 1"
            ).fetchall()

            assert rows_a[0]["content"] == "from-schema-a"
            assert rows_b[0]["content"] == "from-schema-b"
        finally:
            db.execute("DROP TABLE IF EXISTS schema_a.docs")
            db.execute("DROP TABLE IF EXISTS schema_b.docs")
            db.execute("DROP SCHEMA IF EXISTS schema_a")
            db.execute("DROP SCHEMA IF EXISTS schema_b")

    def test_delta_chain_in_custom_schema(self, db: psycopg.Connection):
        """Delta compression works correctly in a non-public schema."""
        schema = "delta_schema"
        db.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        try:
            db.execute(
                f"CREATE TABLE {schema}.versioned "
                f"(group_id INT, version INT, content TEXT NOT NULL) USING xpatch"
            )
            db.execute(
                f"SELECT xpatch.configure('{schema}.versioned', "
                f"group_by => 'group_id', order_by => 'version', keyframe_every => 5)"
            )

            for v in range(1, 8):
                db.execute(
                    f"INSERT INTO {schema}.versioned (group_id, version, content) "
                    f"VALUES (1, {v}, 'Version {v} content for delta test')"
                )

            rows = db.execute(
                f"SELECT version, content FROM {schema}.versioned "
                f"WHERE group_id = 1 ORDER BY version"
            ).fetchall()
            assert len(rows) == 7
            for i, r in enumerate(rows, 1):
                assert r["content"] == f"Version {i} content for delta test"

            stats = db.execute(
                f"SELECT * FROM xpatch.stats('{schema}.versioned')"
            ).fetchone()
            assert stats["total_rows"] == 7
            assert stats["delta_count"] > 0
        finally:
            db.execute(f"DROP TABLE IF EXISTS {schema}.versioned")
            db.execute(f"DROP SCHEMA IF EXISTS {schema}")


# ---------------------------------------------------------------------------
# COPY binary mode — regression test
# ---------------------------------------------------------------------------


class TestCopyBinaryMode:
    """Binary COPY TO/FROM works correctly because the multi_insert path
    just loops over single inserts, and binary mode doesn't change the
    slot materialization path.
    """

    def test_copy_binary_roundtrip(self, db: psycopg.Connection, make_table):
        """COPY TO binary -> COPY FROM binary preserves data."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=5)

        t2 = make_table()

        buf = io.BytesIO()
        with db.cursor().copy(
            f"COPY (SELECT group_id, version, content FROM {t} ORDER BY version) "
            f"TO STDOUT WITH (FORMAT BINARY)"
        ) as copy:
            for data in copy:
                buf.write(data)

        buf.seek(0)
        with db.cursor().copy(
            f"COPY {t2} (group_id, version, content) FROM STDIN WITH (FORMAT BINARY)"
        ) as copy:
            copy.write(buf.read())

        rows_orig = db.execute(
            f"SELECT version, content FROM {t} ORDER BY version"
        ).fetchall()
        rows_copy = db.execute(
            f"SELECT version, content FROM {t2} ORDER BY version"
        ).fetchall()

        assert len(rows_copy) == len(rows_orig)
        for orig, copied in zip(rows_orig, rows_copy):
            assert orig["version"] == copied["version"]
            assert orig["content"] == copied["content"]
