"""
Test tables without a group_by column (single group = entire table).

Covers:
- Table without group_by works
- Stats show 1 group with complete fields
- Version chain covers entire table
- DELETE cascade works
- Latest-version pattern
- TRUNCATE and reinsertion
- Introspection: inspect(), physical(), describe()
- Keyframe placement without grouping
- _xp_seq behavior after TRUNCATE
"""

from __future__ import annotations

import psycopg
import pytest

from conftest import row_count


def _make_no_group_table(
    db: psycopg.Connection,
    make_table,
    *,
    keyframe_every: int | None = None,
) -> str:
    """Create a table without group_by (single group).

    Uses a two-step configure: first create via make_table (which requires
    group_by), then reconfigure without group_by.
    """
    t = make_table(
        "version INT, content TEXT NOT NULL",
        group_by="version",  # Dummy — will reconfigure below
        order_by="version",
    )
    # Reconfigure without group_by
    kfe = keyframe_every or 100
    db.execute(
        f"SELECT xpatch.configure('{t}', order_by => 'version', "
        f"delta_columns => '{{content}}', keyframe_every => {kfe})"
    )
    # Verify reconfiguration took effect
    cfg = db.execute(
        f"SELECT * FROM xpatch.get_config('{t}'::regclass)"
    ).fetchone()
    assert cfg["group_by"] is None, (
        f"Expected group_by=NULL after reconfigure, got {cfg['group_by']!r}"
    )
    return t


class TestNoGroupBasic:
    """Basic operations on a table without group_by."""

    def test_insert_and_read(self, db: psycopg.Connection, make_table):
        """INSERT and SELECT without group_by works."""
        t = _make_no_group_table(db, make_table)
        db.execute(f"INSERT INTO {t} (version, content) VALUES (1, 'first')")
        db.execute(f"INSERT INTO {t} (version, content) VALUES (2, 'second')")
        db.execute(f"INSERT INTO {t} (version, content) VALUES (3, 'third')")

        rows = db.execute(
            f"SELECT version, content FROM {t} ORDER BY version"
        ).fetchall()
        assert len(rows) == 3
        assert rows[0]["content"] == "first"
        assert rows[1]["content"] == "second"
        assert rows[2]["content"] == "third"

    def test_xp_seq_auto_increments(self, db: psycopg.Connection, make_table):
        """_xp_seq increments across the single group."""
        t = _make_no_group_table(db, make_table)
        for v in range(1, 6):
            db.execute(
                f"INSERT INTO {t} (version, content) VALUES ({v}, 'v{v}')"
            )

        rows = db.execute(
            f"SELECT _xp_seq FROM {t} ORDER BY _xp_seq"
        ).fetchall()
        assert [r["_xp_seq"] for r in rows] == [1, 2, 3, 4, 5]

    def test_count(self, db: psycopg.Connection, make_table):
        """COUNT works on ungrouped table."""
        t = _make_no_group_table(db, make_table)
        for v in range(1, 11):
            db.execute(
                f"INSERT INTO {t} (version, content) VALUES ({v}, 'v{v}')"
            )
        assert row_count(db, t) == 10

    def test_single_row_is_keyframe(self, db: psycopg.Connection, make_table):
        """A single row in a no-group table is a keyframe."""
        t = _make_no_group_table(db, make_table)
        db.execute(f"INSERT INTO {t} (version, content) VALUES (1, 'only')")

        rows = db.execute(
            f"SELECT * FROM xpatch.inspect('{t}'::regclass, NULL::int)"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0]["is_keyframe"] is True
        assert rows[0]["seq"] == 1


class TestNoGroupStats:
    """Stats on ungrouped table."""

    def test_stats_show_one_group(self, db: psycopg.Connection, make_table):
        """Stats report 1 group for ungrouped table with complete fields."""
        t = _make_no_group_table(db, make_table)
        for v in range(1, 6):
            db.execute(
                f"INSERT INTO {t} (version, content) VALUES ({v}, 'version {v} content')"
            )

        stats = db.execute(f"SELECT * FROM xpatch.stats('{t}'::regclass)").fetchone()
        assert stats["total_rows"] == 5
        assert stats["total_groups"] == 1
        assert stats["keyframe_count"] == 1
        assert stats["delta_count"] == 4
        assert stats["keyframe_count"] + stats["delta_count"] == stats["total_rows"]
        assert stats["raw_size_bytes"] > 0
        assert stats["compressed_size_bytes"] > 0


class TestNoGroupDelete:
    """DELETE cascade on ungrouped table."""

    def test_delete_last_version(self, db: psycopg.Connection, make_table):
        """Delete last version removes one row."""
        t = _make_no_group_table(db, make_table)
        for v in range(1, 6):
            db.execute(
                f"INSERT INTO {t} (version, content) VALUES ({v}, 'v{v}')"
            )

        db.execute(f"DELETE FROM {t} WHERE version = 5")
        assert row_count(db, t) == 4

    def test_delete_middle_cascades(self, db: psycopg.Connection, make_table):
        """Delete middle version cascades to subsequent versions."""
        t = _make_no_group_table(db, make_table)
        for v in range(1, 6):
            db.execute(
                f"INSERT INTO {t} (version, content) VALUES ({v}, 'v{v}')"
            )

        db.execute(f"DELETE FROM {t} WHERE version = 3")
        # Cascade: v3, v4, v5 deleted — only v1, v2 remain
        assert row_count(db, t) == 2
        rows = db.execute(
            f"SELECT version FROM {t} ORDER BY version"
        ).fetchall()
        assert [r["version"] for r in rows] == [1, 2]

    def test_delete_first_removes_all(self, db: psycopg.Connection, make_table):
        """Delete first version removes all rows (entire chain)."""
        t = _make_no_group_table(db, make_table)
        for v in range(1, 6):
            db.execute(
                f"INSERT INTO {t} (version, content) VALUES ({v}, 'v{v}')"
            )

        db.execute(f"DELETE FROM {t} WHERE version = 1")
        assert row_count(db, t) == 0


class TestNoGroupPatterns:
    """Common query patterns on ungrouped tables."""

    def test_latest_version(self, db: psycopg.Connection, make_table):
        """Get the latest version by ordering."""
        t = _make_no_group_table(db, make_table)
        for v in range(1, 6):
            db.execute(
                f"INSERT INTO {t} (version, content) VALUES ({v}, 'Latest is v{v}')"
            )

        row = db.execute(
            f"SELECT content FROM {t} ORDER BY version DESC LIMIT 1"
        ).fetchone()
        assert row["content"] == "Latest is v5"

    def test_aggregation(self, db: psycopg.Connection, make_table):
        """Aggregation on ungrouped table."""
        t = _make_no_group_table(db, make_table)
        for v in range(1, 11):
            db.execute(
                f"INSERT INTO {t} (version, content) VALUES ({v}, %s)",
                ["x" * v],
            )

        row = db.execute(
            f"SELECT MIN(version) as mn, MAX(version) as mx, "
            f"  SUM(length(content)) as total_len FROM {t}"
        ).fetchone()
        assert row["mn"] == 1
        assert row["mx"] == 10
        assert row["total_len"] == sum(range(1, 11))  # 1+2+...+10 = 55

    def test_insert_after_delete(self, db: psycopg.Connection, make_table):
        """INSERT after DELETE on ungrouped table works."""
        t = _make_no_group_table(db, make_table)
        for v in range(1, 4):
            db.execute(
                f"INSERT INTO {t} (version, content) VALUES ({v}, 'v{v}')"
            )

        db.execute(f"DELETE FROM {t} WHERE version = 1")
        # All deleted (cascade from first)

        db.execute(f"INSERT INTO {t} (version, content) VALUES (10, 'fresh')")
        assert row_count(db, t) == 1
        row = db.execute(f"SELECT content FROM {t}").fetchone()
        assert row["content"] == "fresh"

    def test_truncate_and_reinsert(self, db: psycopg.Connection, make_table):
        """TRUNCATE + reinsert on ungrouped table resets _xp_seq."""
        t = _make_no_group_table(db, make_table)
        for v in range(1, 6):
            db.execute(
                f"INSERT INTO {t} (version, content) VALUES ({v}, 'v{v}')"
            )

        db.execute(f"TRUNCATE {t}")
        assert row_count(db, t) == 0

        db.execute(f"INSERT INTO {t} (version, content) VALUES (1, 'reborn')")
        row = db.execute(f"SELECT _xp_seq, content FROM {t}").fetchone()
        assert row["content"] == "reborn"
        assert row["_xp_seq"] == 1


class TestNoGroupIntrospection:
    """Introspection functions on no-group tables."""

    def test_inspect_no_group(self, db: psycopg.Connection, make_table):
        """xpatch.inspect() works on no-group table with NULL group_value."""
        t = _make_no_group_table(db, make_table)
        for v in range(1, 6):
            db.execute(
                f"INSERT INTO {t} (version, content) VALUES ({v}, 'v{v}')"
            )

        rows = db.execute(
            f"SELECT * FROM xpatch.inspect('{t}'::regclass, NULL::int) ORDER BY seq"
        ).fetchall()
        assert len(rows) == 5
        # First row is always a keyframe
        assert rows[0]["is_keyframe"] is True
        assert rows[0]["seq"] == 1
        # Remaining are deltas
        for r in rows[1:]:
            assert r["is_keyframe"] is False

    def test_physical_no_group(self, db: psycopg.Connection, make_table):
        """xpatch.physical() works on no-group table."""
        t = _make_no_group_table(db, make_table)
        for v in range(1, 4):
            db.execute(
                f"INSERT INTO {t} (version, content) VALUES ({v}, 'content-v{v}')"
            )

        # All-rows form returns all physical rows including keyframes
        rows = db.execute(
            f"SELECT * FROM xpatch.physical('{t}'::regclass)"
        ).fetchall()
        assert len(rows) == 3
        # group_value should be NULL for no-group tables
        for r in rows:
            assert r["group_value"] is None
            assert r["delta_bytes"] is not None
            assert r["delta_size"] > 0

    def test_describe_no_group(self, db: psycopg.Connection, make_table):
        """xpatch.describe() shows no group_by for ungrouped table."""
        t = _make_no_group_table(db, make_table)
        desc = db.execute(
            f"SELECT * FROM xpatch.describe('{t}'::regclass)"
        ).fetchall()
        props = {r["property"]: r["value"] for r in desc}
        # group_by should indicate no grouping
        assert props.get("group_by") is None or "none" in props.get("group_by", "").lower()

    def test_keyframe_placement_no_group(self, db: psycopg.Connection, make_table):
        """Keyframe intervals work correctly without grouping."""
        t = _make_no_group_table(db, make_table, keyframe_every=3)
        for v in range(1, 8):
            db.execute(
                f"INSERT INTO {t} (version, content) VALUES ({v}, 'version {v} data')"
            )

        rows = db.execute(
            f"SELECT * FROM xpatch.inspect('{t}'::regclass, NULL::int) ORDER BY seq"
        ).fetchall()
        keyframe_seqs = [r["seq"] for r in rows if r["is_keyframe"]]
        # seq 1 is always keyframe; with keyframe_every=3: 1, 4, 7
        assert keyframe_seqs == [1, 4, 7]
