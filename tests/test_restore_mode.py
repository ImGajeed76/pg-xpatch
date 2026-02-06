"""
Test pg_dump/pg_restore support (restore mode with explicit _xp_seq).

Covers:
- Explicit _xp_seq values honored on INSERT
- Auto-seq continues correctly after explicit inserts
- Multi-group restore with interleaved data
- dump_configs() output is valid SQL
- Mixed explicit and auto _xp_seq
"""

from __future__ import annotations

import psycopg
import pytest

from conftest import insert_rows, insert_versions, row_count


class TestExplicitXpSeq:
    """INSERT with explicit _xp_seq values (restore mode)."""

    def test_explicit_xp_seq_honored(self, db: psycopg.Connection, make_table):
        """Explicit _xp_seq values are stored as given."""
        t = make_table()
        db.execute(
            f"INSERT INTO {t} (group_id, version, content, _xp_seq) "
            f"VALUES (1, 1, 'first', 1)"
        )
        db.execute(
            f"INSERT INTO {t} (group_id, version, content, _xp_seq) "
            f"VALUES (1, 2, 'second', 2)"
        )
        db.execute(
            f"INSERT INTO {t} (group_id, version, content, _xp_seq) "
            f"VALUES (1, 3, 'third', 3)"
        )

        rows = db.execute(
            f"SELECT _xp_seq, version, content FROM {t} ORDER BY _xp_seq"
        ).fetchall()
        assert len(rows) == 3
        assert rows[0]["_xp_seq"] == 1 and rows[0]["content"] == "first"
        assert rows[1]["_xp_seq"] == 2 and rows[1]["content"] == "second"
        assert rows[2]["_xp_seq"] == 3 and rows[2]["content"] == "third"

    def test_explicit_xp_seq_data_correct(self, db: psycopg.Connection, make_table):
        """Data restored with explicit _xp_seq reconstructs correctly."""
        t = make_table()
        for seq in range(1, 11):
            db.execute(
                f"INSERT INTO {t} (group_id, version, content, _xp_seq) "
                f"VALUES (1, {seq}, 'Restored version {seq}', {seq})"
            )

        rows = db.execute(
            f"SELECT version, content FROM {t} ORDER BY version"
        ).fetchall()
        for row in rows:
            assert row["content"] == f"Restored version {row['version']}"


class TestAutoSeqAfterRestore:
    """Auto-seq continues correctly after explicit inserts."""

    def test_auto_seq_after_explicit(self, db: psycopg.Connection, make_table):
        """Auto-seq picks up after the max explicit _xp_seq."""
        t = make_table()
        # Restore 5 rows with explicit seq
        for seq in range(1, 6):
            db.execute(
                f"INSERT INTO {t} (group_id, version, content, _xp_seq) "
                f"VALUES (1, {seq}, 'restored-{seq}', {seq})"
            )

        # Now insert without explicit seq — should get seq=6
        db.execute(
            f"INSERT INTO {t} (group_id, version, content) "
            f"VALUES (1, 6, 'new after restore')"
        )
        row = db.execute(
            f"SELECT _xp_seq FROM {t} WHERE version = 6"
        ).fetchone()
        assert row["_xp_seq"] == 6

    def test_auto_seq_multiple_groups(self, db: psycopg.Connection, make_table):
        """Auto-seq works per-group after explicit restore."""
        t = make_table()
        # Restore: group 1 has 3 rows, group 2 has 5 rows
        for seq in range(1, 4):
            db.execute(
                f"INSERT INTO {t} (group_id, version, content, _xp_seq) "
                f"VALUES (1, {seq}, 'g1-{seq}', {seq})"
            )
        for seq in range(1, 6):
            db.execute(
                f"INSERT INTO {t} (group_id, version, content, _xp_seq) "
                f"VALUES (2, {seq}, 'g2-{seq}', {seq})"
            )

        # Auto-insert to group 1: should get seq=4
        db.execute(
            f"INSERT INTO {t} (group_id, version, content) "
            f"VALUES (1, 4, 'g1-new')"
        )
        r1 = db.execute(
            f"SELECT _xp_seq FROM {t} WHERE group_id = 1 AND version = 4"
        ).fetchone()
        assert r1["_xp_seq"] == 4

        # Auto-insert to group 2: should get seq=6
        db.execute(
            f"INSERT INTO {t} (group_id, version, content) "
            f"VALUES (2, 6, 'g2-new')"
        )
        r2 = db.execute(
            f"SELECT _xp_seq FROM {t} WHERE group_id = 2 AND version = 6"
        ).fetchone()
        assert r2["_xp_seq"] == 6


class TestMultiGroupRestore:
    """Multi-group restore with interleaved data."""

    def test_interleaved_restore(self, db: psycopg.Connection, make_table):
        """Interleaved group inserts with explicit _xp_seq work."""
        t = make_table()
        # Simulate pg_restore order (interleaved)
        db.execute(
            f"INSERT INTO {t} (group_id, version, content, _xp_seq) "
            f"VALUES (1, 1, 'g1v1', 1)"
        )
        db.execute(
            f"INSERT INTO {t} (group_id, version, content, _xp_seq) "
            f"VALUES (2, 1, 'g2v1', 1)"
        )
        db.execute(
            f"INSERT INTO {t} (group_id, version, content, _xp_seq) "
            f"VALUES (1, 2, 'g1v2', 2)"
        )
        db.execute(
            f"INSERT INTO {t} (group_id, version, content, _xp_seq) "
            f"VALUES (2, 2, 'g2v2', 2)"
        )
        db.execute(
            f"INSERT INTO {t} (group_id, version, content, _xp_seq) "
            f"VALUES (1, 3, 'g1v3', 3)"
        )

        assert row_count(db, t) == 5
        assert row_count(db, t, "group_id = 1") == 3
        assert row_count(db, t, "group_id = 2") == 2

        # Verify content
        rows = db.execute(
            f"SELECT group_id, version, content FROM {t} "
            f"WHERE group_id = 1 ORDER BY version"
        ).fetchall()
        assert rows[0]["content"] == "g1v1"
        assert rows[1]["content"] == "g1v2"
        assert rows[2]["content"] == "g1v3"


class TestDumpConfigs:
    """dump_configs() generates valid restore SQL."""

    def test_dump_configs_valid_sql(self, db: psycopg.Connection, make_table):
        """dump_configs() output is syntactically valid SQL."""
        t = make_table(
            "doc_id INT, ver INT, body TEXT NOT NULL",
            group_by="doc_id",
            order_by="ver",
            delta_columns=["body"],
            keyframe_every=50,
            enable_zstd=False,
        )

        rows = db.execute("SELECT * FROM xpatch.dump_configs()").fetchall()
        assert len(rows) > 0

        # Each row should contain xpatch.configure
        for row in rows:
            text = row[list(row.keys())[0]]
            assert "xpatch.configure" in text

    def test_dump_configs_enable_zstd_format(self, db: psycopg.Connection, make_table):
        """dump_configs() outputs 'true'/'false' not 't'/'f' for enable_zstd."""
        t = make_table(enable_zstd=False)

        rows = db.execute("SELECT * FROM xpatch.dump_configs()").fetchall()
        texts = [row[list(row.keys())[0]] for row in rows]
        matching = [txt for txt in texts if t in txt]

        if matching:
            # Should use 'false' not 'f'
            assert "false" in matching[0].lower() or "'f'" not in matching[0]

    def test_dump_configs_contains_all_tables(self, db: psycopg.Connection, make_table):
        """dump_configs() lists all configured tables."""
        t1 = make_table()
        t2 = make_table(
            "doc_id INT, ver INT, body TEXT NOT NULL",
            group_by="doc_id",
            order_by="ver",
        )

        rows = db.execute("SELECT * FROM xpatch.dump_configs()").fetchall()
        texts = [row[list(row.keys())[0]] for row in rows]
        all_text = "\n".join(texts)

        assert t1 in all_text
        assert t2 in all_text


class TestMixedExplicitAutoSeq:
    """Mixed explicit and auto _xp_seq inserts."""

    def test_explicit_then_auto(self, db: psycopg.Connection, make_table):
        """Explicit _xp_seq inserts followed by auto works."""
        t = make_table()
        # Explicit
        db.execute(
            f"INSERT INTO {t} (group_id, version, content, _xp_seq) VALUES (1, 1, 'r1', 1)"
        )
        db.execute(
            f"INSERT INTO {t} (group_id, version, content, _xp_seq) VALUES (1, 2, 'r2', 2)"
        )
        # Auto
        insert_rows(db, t, [(1, 3, "auto")])

        rows = db.execute(
            f"SELECT _xp_seq, content FROM {t} ORDER BY _xp_seq"
        ).fetchall()
        assert len(rows) == 3
        assert rows[0]["content"] == "r1"
        assert rows[1]["content"] == "r2"
        assert rows[2]["content"] == "auto"
        assert rows[2]["_xp_seq"] == 3
