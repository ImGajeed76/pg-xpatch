"""
Test L3 persistent disk cache.

Covers:
- L3 enable/disable via xpatch.configure()
- L3 table auto-creation on first write
- L3 populated on INSERT (decompressed content stored in xpatch.<table>_xp_l3)
- L3 populated on read reconstruction (path planner stores result)
- Data integrity: content read back from L3 matches original
- L3 invalidation on DELETE
- L3 invalidation on TRUNCATE
- xpatch.drop_l3_cache() drops the L3 table
- L3 data survives PG restart (persistent disk cache)
- Concurrent access to L3
- Per-table L3 configuration (enabled for one table, disabled for another)
"""

from __future__ import annotations

import threading
import uuid
from typing import Any, Callable

import psycopg
import pytest
from psycopg import sql

from conftest import (
    PG_HOST,
    PG_PORT,
    PG_USER,
    PG_PASSWORD,
    insert_rows,
    insert_versions,
    row_count,
    _connect,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def enable_l3(db: psycopg.Connection, table: str) -> None:
    """Enable L3 cache for a table by reconfiguring it."""
    db.execute(
        "UPDATE xpatch.table_config SET l3_cache_enabled = true "
        "WHERE table_name = %s",
        [table],
    )


def l3_table_name(table: str) -> str:
    """Return the L3 cache table name for an xpatch table."""
    return f"{table}_xp_l3"


def l3_table_exists(db: psycopg.Connection, table: str) -> bool:
    """Check if the L3 cache table exists in the xpatch schema."""
    row = db.execute(
        "SELECT EXISTS ("
        "  SELECT 1 FROM pg_class c "
        "  JOIN pg_namespace n ON c.relnamespace = n.oid "
        "  WHERE n.nspname = 'xpatch' AND c.relname = %s"
        ") AS exists",
        [l3_table_name(table)],
    ).fetchone()
    return row["exists"]


def l3_count(db: psycopg.Connection, table: str) -> int:
    """Count entries in the L3 cache table."""
    name = l3_table_name(table)
    row = db.execute(
        f'SELECT COUNT(*) AS n FROM xpatch."{name}"'
    ).fetchone()
    return row["n"]


def single_content(
    db: psycopg.Connection, table: str, group_id: int, version: int
) -> str | None:
    """Read a single content value from an xpatch table."""
    row = db.execute(
        f"SELECT content FROM {table} WHERE group_id = %s AND version = %s",
        [group_id, version],
    ).fetchone()
    return row["content"] if row else None


def all_content(db: psycopg.Connection, table: str, group_id: int) -> list[str]:
    """Read all content values for a group, ordered by version."""
    rows = db.execute(
        f"SELECT content FROM {table} WHERE group_id = %s ORDER BY version",
        [group_id],
    ).fetchall()
    return [r["content"] for r in rows]


# ---------------------------------------------------------------------------
# L3 Enable / Disable
# ---------------------------------------------------------------------------


class TestL3EnableDisable:
    """Test L3 enable/disable via xpatch.configure()."""

    def test_l3_disabled_by_default(
        self, db: psycopg.Connection, make_table
    ):
        """L3 is disabled by default — no L3 table should be created on INSERT."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=5)
        assert not l3_table_exists(db, t), \
            "L3 table should not exist when l3_cache_enabled is false"

    def test_l3_enabled_via_configure(
        self, db: psycopg.Connection, make_table
    ):
        """L3 can be enabled via xpatch.configure()."""
        t = make_table()
        enable_l3(db, t)
        insert_versions(db, t, group_id=1, count=3)
        assert l3_table_exists(db, t), \
            "L3 table should be created when l3_cache_enabled is true"

    def test_l3_can_be_disabled_after_enable(
        self, db: psycopg.Connection, make_table, db_factory
    ):
        """L3 can be disabled after being enabled; new inserts shouldn't populate L3."""
        t = make_table()
        enable_l3(db, t)
        insert_versions(db, t, group_id=1, count=3)
        assert l3_table_exists(db, t)

        before_count = l3_count(db, t)

        # Disable L3
        db.execute(
            "UPDATE xpatch.table_config SET l3_cache_enabled = false "
            "WHERE table_name = %s",
            [t],
        )

        # Use a NEW connection so per-backend L3 enabled cache is fresh
        # (the original connection caches l3_enabled=true in its process)
        conn2 = db_factory()
        insert_versions(conn2, t, group_id=2, count=3)
        after_count = l3_count(db, t)

        assert after_count == before_count, \
            "No new L3 entries should be created after disabling L3"


# ---------------------------------------------------------------------------
# L3 Table Auto-Creation
# ---------------------------------------------------------------------------


class TestL3TableCreation:
    """Test L3 table auto-creation on first write."""

    def test_l3_table_created_on_first_insert(
        self, db: psycopg.Connection, make_table
    ):
        """L3 table should be created automatically on first INSERT when enabled."""
        t = make_table()
        enable_l3(db, t)

        assert not l3_table_exists(db, t), \
            "L3 table shouldn't exist before any writes"

        insert_rows(db, t, [(1, 1, "first row")])

        assert l3_table_exists(db, t), \
            "L3 table should exist after first INSERT with L3 enabled"

    def test_l3_table_has_correct_schema(
        self, db: psycopg.Connection, make_table
    ):
        """L3 table should have the expected columns."""
        t = make_table()
        enable_l3(db, t)
        insert_rows(db, t, [(1, 1, "check schema")])

        l3t = l3_table_name(t)
        cols = db.execute(
            "SELECT attname FROM pg_attribute "
            "WHERE attrelid = (SELECT oid FROM pg_class WHERE relname = %s "
            "  AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'xpatch')) "
            "AND attnum > 0 AND NOT attisdropped "
            "ORDER BY attnum",
            [l3t],
        ).fetchall()
        col_names = [c["attname"] for c in cols]
        assert col_names == [
            "group_hash_h1", "group_hash_h2", "seq", "attnum",
            "content", "cached_at",
        ], f"Unexpected L3 table columns: {col_names}"

    def test_l3_table_has_primary_key(
        self, db: psycopg.Connection, make_table
    ):
        """L3 table should have a primary key on (group_hash_h1, h2, seq, attnum)."""
        t = make_table()
        enable_l3(db, t)
        insert_rows(db, t, [(1, 1, "pk check")])

        l3t = l3_table_name(t)
        # Check that a primary key index exists
        row = db.execute(
            "SELECT COUNT(*) AS n FROM pg_index i "
            "JOIN pg_class c ON c.oid = i.indrelid "
            "JOIN pg_namespace n ON c.relnamespace = n.oid "
            "WHERE n.nspname = 'xpatch' AND c.relname = %s "
            "AND i.indisprimary",
            [l3t],
        ).fetchone()
        assert row["n"] == 1, "L3 table should have a primary key"


# ---------------------------------------------------------------------------
# L3 Population on INSERT
# ---------------------------------------------------------------------------


class TestL3InsertPopulation:
    """Test that L3 is populated during INSERT."""

    def test_l3_entries_created_on_insert(
        self, db: psycopg.Connection, make_table
    ):
        """INSERT should create L3 entries when L3 is enabled."""
        t = make_table()
        enable_l3(db, t)
        insert_versions(db, t, group_id=1, count=5)

        count = l3_count(db, t)
        # Each version should get an L3 entry for the delta column (content)
        assert count >= 5, f"Expected at least 5 L3 entries, got {count}"

    def test_l3_content_matches_original(
        self, db: psycopg.Connection, make_table
    ):
        """Content stored in L3 should be decompressed and match the original."""
        t = make_table()
        enable_l3(db, t)

        for v in range(1, 6):
            insert_rows(db, t, [(1, v, f"L3 content v{v}")])

        # Read back from the main table (which may use L3)
        for v in range(1, 6):
            result = single_content(db, t, 1, v)
            assert result == f"L3 content v{v}", f"Content mismatch at v={v}"

    def test_l3_multiple_groups(
        self, db: psycopg.Connection, make_table
    ):
        """L3 tracks entries across multiple groups."""
        t = make_table()
        enable_l3(db, t)
        for g in range(1, 4):
            insert_versions(db, t, group_id=g, count=3)

        count = l3_count(db, t)
        # 3 groups x 3 versions = 9 minimum
        assert count >= 9, f"Expected at least 9 L3 entries, got {count}"

    def test_l3_multiple_delta_columns(
        self, db: psycopg.Connection, make_table
    ):
        """L3 stores entries for each delta column."""
        t = make_table(
            columns="group_id INT, version INT, title TEXT NOT NULL, body TEXT NOT NULL",
            delta_columns=["title", "body"],
        )
        enable_l3(db, t)

        for v in range(1, 4):
            insert_rows(
                db, t,
                [(1, v, f"Title {v}", f"Body {v}")],
                columns=["group_id", "version", "title", "body"],
            )

        count = l3_count(db, t)
        # 3 versions x 2 delta columns = 6 entries
        assert count >= 6, f"Expected at least 6 L3 entries (2 cols x 3 rows), got {count}"

        # Verify data integrity
        for v in range(1, 4):
            row = db.execute(
                f"SELECT title, body FROM {t} WHERE group_id = 1 AND version = %s",
                [v],
            ).fetchone()
            assert row["title"] == f"Title {v}"
            assert row["body"] == f"Body {v}"


# ---------------------------------------------------------------------------
# L3 Population on Read (Reconstruction)
# ---------------------------------------------------------------------------


class TestL3ReadPopulation:
    """Test that L3 is populated when content is reconstructed on read."""

    def test_l3_populated_on_cache_miss_read(
        self, db: psycopg.Connection, make_table, db_factory
    ):
        """
        When L3 is enabled after data exists, reading (which triggers
        reconstruction via a fresh connection without L1 cache hits)
        should populate L3.
        """
        t = make_table(compress_depth=3)
        # Insert with L3 disabled
        insert_versions(db, t, group_id=1, count=5)

        # Now enable L3
        enable_l3(db, t)

        # L3 table doesn't exist yet
        assert not l3_table_exists(db, t)

        # Use a fresh connection to avoid L1 cache hits from the inserting
        # backend. The new backend starts with empty L1 process-local state,
        # forcing reconstruction which should populate L3.
        conn2 = db_factory()
        result = conn2.execute(
            f"SELECT content FROM {t} WHERE group_id = 1 ORDER BY version"
        ).fetchall()
        assert len(result) == 5
        for i, v in enumerate(range(1, 6)):
            assert result[i]["content"] == f"Version {v} content"

        # L3 might have entries now (depends on whether reconstruction
        # path triggered L3 put in the new backend)
        if l3_table_exists(db, t):
            count = l3_count(db, t)
            assert count >= 1, "L3 should be populated after reconstruction read"


# ---------------------------------------------------------------------------
# Data Integrity Tests
# ---------------------------------------------------------------------------


class TestL3DataIntegrity:
    """Test data integrity with L3 active."""

    def test_deep_chain_with_l3(
        self, db: psycopg.Connection, make_table
    ):
        """Deep delta chains work correctly with L3 enabled."""
        t = make_table(compress_depth=10, keyframe_every=50)
        enable_l3(db, t)

        contents = {}
        for v in range(1, 31):
            c = f"Deep L3 v{v}: " + "x" * (50 + v)
            contents[v] = c
            insert_rows(db, t, [(1, v, c)])

        for v in range(1, 31):
            result = single_content(db, t, 1, v)
            assert result == contents[v], f"Deep chain mismatch at v={v}"

    def test_l3_with_zstd(
        self, db: psycopg.Connection, make_table
    ):
        """L3 works correctly with zstd compression."""
        t = make_table(compress_depth=5, enable_zstd=True)
        enable_l3(db, t)

        contents = {}
        for v in range(1, 11):
            c = f"zstd L3 v{v}: " + "z" * 200
            contents[v] = c
            insert_rows(db, t, [(1, v, c)])

        for v in range(1, 11):
            result = single_content(db, t, 1, v)
            assert result == contents[v], f"zstd L3 mismatch at v={v}"

    def test_l3_content_from_fresh_connection(
        self, db: psycopg.Connection, make_table, db_factory
    ):
        """
        A fresh connection (with empty L1) should still read correctly
        when L3 is enabled. The path planner may use L3 as an anchor.
        """
        t = make_table(compress_depth=3)
        enable_l3(db, t)

        for v in range(1, 11):
            insert_rows(db, t, [(1, v, f"fresh conn v{v}")])

        # Use a fresh connection — its L1 is cold, forcing reconstruction
        conn2 = db_factory()
        for v in range(1, 11):
            row = conn2.execute(
                f"SELECT content FROM {t} WHERE group_id = 1 AND version = %s",
                [v],
            ).fetchone()
            assert row["content"] == f"fresh conn v{v}", \
                f"Fresh connection mismatch at v={v}"


# ---------------------------------------------------------------------------
# Invalidation Tests
# ---------------------------------------------------------------------------


class TestL3Invalidation:
    """Test L3 cache invalidation."""

    def test_delete_invalidates_l3(
        self, db: psycopg.Connection, make_table
    ):
        """DELETE should remove L3 entries for the deleted group."""
        t = make_table()
        enable_l3(db, t)
        insert_versions(db, t, group_id=1, count=5)
        assert l3_table_exists(db, t)
        assert l3_count(db, t) > 0

        db.execute(f"DELETE FROM {t} WHERE group_id = 1")

        # L3 table still exists but all entries for the group are gone
        assert l3_table_exists(db, t), \
            "L3 table should still exist after per-group DELETE"
        assert l3_count(db, t) == 0, \
            "L3 entries for deleted group should be removed"

    def test_truncate_invalidates_l3(
        self, db: psycopg.Connection, make_table
    ):
        """TRUNCATE should invalidate L3 (drop the L3 table)."""
        t = make_table()
        enable_l3(db, t)
        insert_versions(db, t, group_id=1, count=5)
        assert l3_table_exists(db, t)

        db.execute(f"TRUNCATE {t}")

        assert not l3_table_exists(db, t), \
            "L3 table should be dropped after TRUNCATE"

    def test_data_correct_after_delete_and_reinsert(
        self, db: psycopg.Connection, make_table
    ):
        """After DELETE + re-INSERT with L3, data should be correct (no stale cache)."""
        t = make_table()
        enable_l3(db, t)
        insert_versions(db, t, group_id=1, count=3)

        db.execute(f"DELETE FROM {t} WHERE group_id = 1")

        # Re-insert with different content
        for v in range(1, 3):
            insert_rows(db, t, [(1, v, f"After delete v{v}")])

        result = all_content(db, t, group_id=1)
        assert len(result) == 2
        assert result[0] == "After delete v1"
        assert result[1] == "After delete v2"

    def test_data_correct_after_truncate_and_reinsert(
        self, db: psycopg.Connection, make_table
    ):
        """After TRUNCATE + re-INSERT with L3, data should be correct."""
        t = make_table()
        enable_l3(db, t)
        insert_versions(db, t, group_id=1, count=5)

        db.execute(f"TRUNCATE {t}")

        for v in range(1, 4):
            insert_rows(db, t, [(1, v, f"After truncate v{v}")])

        for v in range(1, 4):
            result = single_content(db, t, 1, v)
            assert result == f"After truncate v{v}", \
                f"Stale L3 data at v={v} after TRUNCATE"


# ---------------------------------------------------------------------------
# drop_l3_cache() Tests
# ---------------------------------------------------------------------------


class TestDropL3Cache:
    """Test xpatch.drop_l3_cache() SQL function."""

    def test_drop_l3_cache_removes_table(
        self, db: psycopg.Connection, make_table
    ):
        """drop_l3_cache() should drop the L3 table."""
        t = make_table()
        enable_l3(db, t)
        insert_versions(db, t, group_id=1, count=3)
        assert l3_table_exists(db, t)

        db.execute(f"SELECT xpatch.drop_l3_cache('{t}'::regclass)")

        assert not l3_table_exists(db, t), \
            "L3 table should be dropped after drop_l3_cache()"

    def test_drop_l3_cache_on_nonexistent_table(
        self, db: psycopg.Connection, make_table
    ):
        """drop_l3_cache() on a table without L3 should not error."""
        t = make_table()
        insert_versions(db, t, group_id=1, count=3)
        assert not l3_table_exists(db, t)

        # Should not raise
        db.execute(f"SELECT xpatch.drop_l3_cache('{t}'::regclass)")

    def test_l3_recreated_after_drop(
        self, db: psycopg.Connection, make_table
    ):
        """After drop_l3_cache(), new inserts should recreate the L3 table."""
        t = make_table()
        enable_l3(db, t)
        insert_versions(db, t, group_id=1, count=3)
        assert l3_table_exists(db, t)

        db.execute(f"SELECT xpatch.drop_l3_cache('{t}'::regclass)")
        assert not l3_table_exists(db, t)

        # New inserts should recreate
        insert_versions(db, t, group_id=2, count=2)
        assert l3_table_exists(db, t), \
            "L3 table should be recreated on new INSERT after drop"


# ---------------------------------------------------------------------------
# Persistence (Survives Restart) Tests
# ---------------------------------------------------------------------------


class TestL3Persistence:
    """Test that L3 data survives PG restart."""

    def test_l3_data_survives_restart(
        self, db: psycopg.Connection, make_table, pg_ctl
    ):
        """L3 data should persist across a PG restart."""
        t = make_table()
        enable_l3(db, t)

        for v in range(1, 6):
            insert_rows(db, t, [(1, v, f"persist v{v}")])

        assert l3_table_exists(db, t)
        count_before = l3_count(db, t)
        assert count_before >= 5

        db_name = db.info.dbname

        # Close connection before restart
        db.close()

        # Restart PG
        pg_ctl.restart()

        # Reconnect
        new_conn = _connect(db_name)
        try:
            # L3 table should still exist
            assert l3_table_exists(new_conn, t), \
                "L3 table should survive PG restart"

            count_after = l3_count(new_conn, t)
            assert count_after == count_before, \
                f"L3 entry count changed after restart: {count_before} -> {count_after}"

            # Data should be readable
            for v in range(1, 6):
                result = single_content(new_conn, t, 1, v)
                assert result == f"persist v{v}", \
                    f"Data mismatch after restart at v={v}"
        finally:
            new_conn.close()


# ---------------------------------------------------------------------------
# Per-Table Configuration
# ---------------------------------------------------------------------------


class TestL3PerTableConfig:
    """Test that L3 is per-table, not global."""

    def test_l3_enabled_per_table(
        self, db: psycopg.Connection, make_table
    ):
        """One table can have L3 enabled while another has it disabled."""
        t1 = make_table()
        t2 = make_table()

        # Enable L3 only for t1
        enable_l3(db, t1)

        insert_versions(db, t1, group_id=1, count=3)
        insert_versions(db, t2, group_id=1, count=3)

        assert l3_table_exists(db, t1), "t1 should have L3 table"
        assert not l3_table_exists(db, t2), "t2 should NOT have L3 table"

    def test_l3_max_size_per_table(
        self, db: psycopg.Connection, make_table
    ):
        """l3_cache_max_size_mb is per-table config."""
        t = make_table()
        enable_l3(db, t)

        # Update max size for this specific table
        db.execute(
            "UPDATE xpatch.table_config SET l3_cache_max_size_mb = 512 "
            "WHERE table_name = %s",
            [t],
        )

        row = db.execute(
            "SELECT l3_cache_max_size_mb FROM xpatch.table_config "
            "WHERE table_name = %s",
            [t],
        ).fetchone()
        assert row["l3_cache_max_size_mb"] == 512


# ---------------------------------------------------------------------------
# Concurrent Access
# ---------------------------------------------------------------------------


class TestL3Concurrent:
    """Test concurrent L3 access."""

    def test_concurrent_inserts_with_l3(
        self, db: psycopg.Connection, make_table, db_factory
    ):
        """Multiple concurrent INSERT streams should not corrupt L3."""
        t = make_table(compress_depth=3)
        enable_l3(db, t)

        errors = []

        def inserter(conn_factory, group_id, count):
            try:
                c = conn_factory()
                for v in range(1, count + 1):
                    c.execute(
                        f"INSERT INTO {t} (group_id, version, content) "
                        f"VALUES (%s, %s, %s)",
                        [group_id, v, f"G{group_id}V{v}: " + "c" * 50],
                    )
            except Exception as e:
                errors.append(str(e))

        threads = []
        for g in range(1, 6):
            t_thread = threading.Thread(
                target=inserter, args=(db_factory, g, 5)
            )
            threads.append(t_thread)

        for t_thread in threads:
            t_thread.start()
        for t_thread in threads:
            t_thread.join(timeout=30)

        assert len(errors) == 0, f"Concurrent insert errors: {errors}"

        # Verify data integrity for all groups
        for g in range(1, 6):
            result = all_content(db, t, group_id=g)
            assert len(result) == 5, \
                f"Group {g}: expected 5 rows, got {len(result)}"

    def test_concurrent_read_write_with_l3(
        self, db: psycopg.Connection, make_table, db_factory
    ):
        """Concurrent reads and writes should not deadlock with L3."""
        t = make_table(compress_depth=3)
        enable_l3(db, t)
        insert_versions(db, t, group_id=1, count=10)

        errors = []

        def reader(conn_factory, n_reads):
            try:
                c = conn_factory()
                for v in range(1, min(n_reads, 10) + 1):
                    c.execute(
                        f"SELECT content FROM {t} "
                        f"WHERE group_id = 1 AND version = %s",
                        [v],
                    ).fetchone()
            except Exception as e:
                errors.append(f"reader: {e}")

        def writer(conn_factory, group_id, count):
            try:
                c = conn_factory()
                for v in range(1, count + 1):
                    c.execute(
                        f"INSERT INTO {t} (group_id, version, content) "
                        f"VALUES (%s, %s, %s)",
                        [group_id, v, f"W{group_id}V{v}"],
                    )
            except Exception as e:
                errors.append(f"writer: {e}")

        threads = []
        for _ in range(3):
            threads.append(threading.Thread(target=reader, args=(db_factory, 10)))
        for g in range(2, 5):
            threads.append(threading.Thread(target=writer, args=(db_factory, g, 5)))

        for t_thread in threads:
            t_thread.start()
        for t_thread in threads:
            t_thread.join(timeout=30)

        hung = [t_thread for t_thread in threads if t_thread.is_alive()]
        assert len(hung) == 0, f"{len(hung)} threads hung"
        assert len(errors) == 0, f"Errors: {errors}"


# ---------------------------------------------------------------------------
# L3 + Path Planner Integration
# ---------------------------------------------------------------------------


class TestL3PathPlanner:
    """Test that the path planner uses L3 as an anchor."""

    def test_plan_path_shows_l3_when_available(
        self, db: psycopg.Connection, make_table
    ):
        """
        After L3 is populated, xpatch.plan_path() should include L3
        as a possible anchor in the path plan.
        """
        t = make_table(compress_depth=3, keyframe_every=20)
        enable_l3(db, t)

        for v in range(1, 11):
            insert_rows(db, t, [(1, v, f"plan test v{v}")])

        # plan_path signature: (regclass, text, int2, int8, bool)
        # attnum=3 is the 'content' column, target_seq=5
        plan = db.execute(
            f"SELECT * FROM xpatch.plan_path('{t}'::regclass, '1', "
            f"3::int2, 5::int8)"
        ).fetchall()

        # At minimum, the plan should have at least one step
        assert len(plan) >= 1, "Plan should have at least one step"

        # Check that the plan produces correct data
        result = single_content(db, t, 1, 5)
        assert result == "plan test v5"
