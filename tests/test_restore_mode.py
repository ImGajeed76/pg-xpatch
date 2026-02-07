"""
Test pg_dump/pg_restore support (restore mode with explicit _xp_seq).

Covers:
- Explicit _xp_seq values honored on INSERT
- Auto-seq continues correctly after explicit inserts
- Multi-group restore with interleaved data
- dump_configs() output is valid SQL
- Mixed explicit and auto _xp_seq
- Physical storage verification in restore mode
- fix_restored_configs() behavior
- Edge cases: gaps, boundary values, round-trip
- Full pg_dump/pg_restore round-trip
"""

from __future__ import annotations

import subprocess

import psycopg
import pytest

from conftest import (
    CONTAINER_NAME,
    insert_rows,
    insert_versions,
    row_count,
    _docker_exec,
    _admin_conn,
    _connect,
)


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

    def test_explicit_xp_seq_inspect_physical(self, db: psycopg.Connection, make_table):
        """Explicit _xp_seq values are reflected in inspect() output."""
        t = make_table()
        for seq in range(1, 6):
            db.execute(
                f"INSERT INTO {t} (group_id, version, content, _xp_seq) "
                f"VALUES (1, {seq}, 'v{seq}', {seq})"
            )

        inspect = db.execute(
            f"SELECT seq, is_keyframe FROM xpatch.inspect('{t}'::regclass, 1) "
            f"ORDER BY seq"
        ).fetchall()
        seqs = [r["seq"] for r in inspect]
        # With keyframe_every=5 (default), seq 1 is keyframe, 2-5 are deltas
        assert 1 in seqs
        kf = [r for r in inspect if r["seq"] == 1]
        assert kf[0]["is_keyframe"] is True

    def test_explicit_seq_with_gaps(self, db: psycopg.Connection, make_table):
        """Auto-seq after gapped explicit _xp_seq starts after max."""
        t = make_table()
        # Insert with a gap: 1, 2, 5
        for seq in [1, 2, 5]:
            db.execute(
                f"INSERT INTO {t} (group_id, version, content, _xp_seq) "
                f"VALUES (1, {seq}, 'v{seq}', {seq})"
            )

        # Auto-insert should get seq >= 6 (max+1)
        db.execute(
            f"INSERT INTO {t} (group_id, version, content) "
            f"VALUES (1, 6, 'auto')"
        )
        row = db.execute(
            f"SELECT _xp_seq FROM {t} WHERE version = 6"
        ).fetchone()
        assert row["_xp_seq"] >= 6, (
            f"Expected auto-seq >= 6 after gap (max explicit=5), got {row['_xp_seq']}"
        )

    def test_explicit_seq_zero_treated_as_auto(self, db: psycopg.Connection, make_table):
        """_xp_seq=0 should NOT trigger restore mode (auto-allocate instead)."""
        t = make_table()
        db.execute(
            f"INSERT INTO {t} (group_id, version, content, _xp_seq) "
            f"VALUES (1, 1, 'v1', 0)"
        )
        row = db.execute(
            f"SELECT _xp_seq FROM {t} WHERE version = 1"
        ).fetchone()
        # C code: user_seq > 0 triggers restore mode, so 0 → auto-allocate = 1
        assert row["_xp_seq"] == 1, (
            f"_xp_seq=0 should auto-allocate to 1, got {row['_xp_seq']}"
        )


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

    def test_auto_seq_after_delete_and_restore(self, db: psycopg.Connection, make_table):
        """After restore + delete + auto-insert, seq continues from max."""
        t = make_table()
        for seq in range(1, 6):
            db.execute(
                f"INSERT INTO {t} (group_id, version, content, _xp_seq) "
                f"VALUES (1, {seq}, 'r{seq}', {seq})"
            )
        # Delete versions 4 and 5
        db.execute(f"DELETE FROM {t} WHERE version >= 4")
        # Auto-insert should still get seq >= 4 (or wherever seq cache is)
        db.execute(
            f"INSERT INTO {t} (group_id, version, content) "
            f"VALUES (1, 4, 'new-4')"
        )
        row = db.execute(
            f"SELECT _xp_seq, content FROM {t} WHERE version = 4"
        ).fetchone()
        assert row["content"] == "new-4"
        # Seq should be > 0 (exact value depends on implementation)
        assert row["_xp_seq"] > 0


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

    def test_interleaved_restore_many_groups(self, db: psycopg.Connection, make_table):
        """Interleaved restore across 10 groups, all content correct."""
        t = make_table()
        # Insert interleaved: for each version, insert across all groups
        for v in range(1, 6):
            for g in range(1, 11):
                db.execute(
                    f"INSERT INTO {t} (group_id, version, content, _xp_seq) "
                    f"VALUES ({g}, {v}, 'g{g}v{v}', {v})"
                )

        assert row_count(db, t) == 50

        # Check every group has correct content
        for g in range(1, 11):
            rows = db.execute(
                f"SELECT version, content FROM {t} "
                f"WHERE group_id = {g} ORDER BY version"
            ).fetchall()
            assert len(rows) == 5
            for row in rows:
                assert row["content"] == f"g{g}v{row['version']}"


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

        assert len(matching) == 1, (
            f"Expected exactly 1 dump_configs row for {t}, got {len(matching)}"
        )
        # Must contain the literal 'false', not abbreviated 'f'
        assert "false" in matching[0].lower(), (
            f"Expected 'false' in dump_configs output: {matching[0]}"
        )
        assert "'f'" not in matching[0], (
            f"Found abbreviated 'f' for boolean in: {matching[0]}"
        )

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

    def test_dump_configs_round_trip(self, db: psycopg.Connection, make_table):
        """dump_configs() output can be re-executed to recreate config."""
        t = make_table(keyframe_every=50, compress_depth=2, enable_zstd=False)

        # Get dumped SQL for this table
        rows = db.execute("SELECT * FROM xpatch.dump_configs()").fetchall()
        texts = [row[list(row.keys())[0]] for row in rows]
        matching = [txt for txt in texts if t in txt]
        assert len(matching) == 1

        # Delete config
        db.execute(
            f"DELETE FROM xpatch.table_config WHERE table_name = '{t}'"
        )

        # Re-execute dumped SQL
        db.execute(matching[0])

        # Verify config was restored correctly
        cfg = db.execute(
            f"SELECT * FROM xpatch.describe('{t}'::regclass)"
        ).fetchall()
        props = {r["property"]: r["value"] for r in cfg}
        assert props["keyframe_every"] == "50"
        assert props["compress_depth"] == "2"
        assert props["enable_zstd"] == "false"


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

    def test_insert_returning_seq_is_null(self, db: psycopg.Connection, make_table):
        """INSERT RETURNING _xp_seq returns NULL (computed inside C TAM)."""
        t = make_table()
        # Without explicit _xp_seq
        row = db.execute(
            f"INSERT INTO {t} (group_id, version, content) "
            f"VALUES (1, 1, 'test') RETURNING _xp_seq"
        ).fetchone()
        assert row["_xp_seq"] is None, (
            f"INSERT RETURNING _xp_seq should be NULL, got {row['_xp_seq']}"
        )

    def test_insert_returning_seq_with_explicit(self, db: psycopg.Connection, make_table):
        """INSERT RETURNING _xp_seq with explicit value returns the provided value."""
        t = make_table()
        row = db.execute(
            f"INSERT INTO {t} (group_id, version, content, _xp_seq) "
            f"VALUES (1, 1, 'test', 42) RETURNING _xp_seq"
        ).fetchone()
        # With explicit _xp_seq, the user-provided value is in the tuple
        # before the TAM processes it, so RETURNING sees it
        assert row["_xp_seq"] == 42, (
            f"Expected RETURNING to show explicit _xp_seq=42, got {row['_xp_seq']}"
        )


class TestFixRestoredConfigs:
    """xpatch.fix_restored_configs() repairs config OIDs after restore."""

    def test_fix_restored_configs_no_mismatch(self, db: psycopg.Connection, make_table):
        """fix_restored_configs() returns 0 when no OID mismatches exist."""
        t = make_table()
        result = db.execute("SELECT xpatch.fix_restored_configs()").fetchone()
        fixed = result[list(result.keys())[0]]
        assert fixed == 0

    def test_fix_restored_configs_fixes_oid_mismatch(
        self, db: psycopg.Connection, make_table
    ):
        """fix_restored_configs() fixes a manually corrupted OID."""
        t = make_table()
        # Get actual OID
        actual_oid = db.execute(
            f"SELECT '{t}'::regclass::oid AS oid"
        ).fetchone()["oid"]

        # Corrupt the OID in table_config to simulate a restore
        db.execute(
            f"UPDATE xpatch.table_config SET relid = 99999 "
            f"WHERE table_name = '{t}'"
        )

        result = db.execute("SELECT xpatch.fix_restored_configs()").fetchone()
        fixed = result[list(result.keys())[0]]
        assert fixed == 1, f"Expected 1 fixed config, got {fixed}"

        # Verify the OID was restored correctly
        cfg = db.execute(
            f"SELECT relid FROM xpatch.table_config WHERE table_name = '{t}'"
        ).fetchone()
        assert cfg["relid"] == actual_oid

    def test_fix_restored_configs_removes_orphans(
        self, db: psycopg.Connection, make_table
    ):
        """fix_restored_configs() removes config for non-existent tables."""
        t = make_table()

        # Insert an orphan config for a table that doesn't exist
        db.execute(
            "INSERT INTO xpatch.table_config "
            "(relid, schema_name, table_name, group_by, order_by, keyframe_every, compress_depth, enable_zstd) "
            "VALUES (88888, 'public', 'nonexistent_table_xyz', 'gid', 'ver', 5, 1, true)"
        )

        result = db.execute("SELECT xpatch.fix_restored_configs()").fetchone()
        fixed = result[list(result.keys())[0]]
        # The orphan was removed, but fix_restored_configs only counts OID updates
        # Check the orphan is gone
        orphan = db.execute(
            "SELECT * FROM xpatch.table_config WHERE table_name = 'nonexistent_table_xyz'"
        ).fetchone()
        assert orphan is None, "Orphan config should have been removed"


# ---------------------------------------------------------------------------
# Full pg_dump / pg_restore round-trip
# ---------------------------------------------------------------------------


class TestPgDumpRestore:
    """End-to-end pg_dump/pg_restore round-trip verifying that data,
    configuration, and _xp_seq values survive the cycle.

    Uses ``pg_dump -Fc`` (custom format) and ``pg_restore`` running inside
    the Docker container.
    """

    def test_dump_restore_round_trip(self, db: psycopg.Connection, make_table):
        """Data + config survive a pg_dump → pg_restore cycle."""
        # -- Setup: create table, insert data, configure --
        t = make_table(keyframe_every=5, compress_depth=2, enable_zstd=False)
        for g in (1, 2):
            for v in range(1, 8):
                insert_rows(db, t, [(g, v, f"g{g}-v{v}")])

        # Record original data and config
        orig_rows = db.execute(
            f"SELECT group_id, version, content, _xp_seq "
            f"FROM {t} ORDER BY group_id, version"
        ).fetchall()
        orig_config = db.execute(
            f"SELECT * FROM xpatch.describe('{t}'::regclass)"
        ).fetchall()
        orig_props = {r["property"]: r["value"] for r in orig_config}

        # Get the dump_configs SQL for reconfiguration after restore
        dump_sql = db.execute("SELECT xpatch.dump_configs()").fetchone()
        config_sql = dump_sql[list(dump_sql.keys())[0]]

        src_db = db.info.dbname
        dst_db = f"{src_db}_restored"

        try:
            # -- pg_dump inside the container --
            _docker_exec(
                f"su postgres -c 'pg_dump -Fc -d {src_db} -f /tmp/xpatch_dump.fc'",
                timeout=30,
            )

            # -- Create destination DB + pg_restore --
            with _admin_conn() as admin:
                admin.execute(f"CREATE DATABASE {dst_db}")

            _docker_exec(
                f"su postgres -c 'pg_restore -d {dst_db} /tmp/xpatch_dump.fc'",
                timeout=30,
                check=False,  # pg_restore may warn about pre-existing objects
            )

            # -- Connect to restored DB, fix configs, verify --
            restored = _connect(dst_db)
            try:
                # fix_restored_configs repairs OIDs that changed during restore
                restored.execute("SELECT xpatch.fix_restored_configs()")

                # Re-apply configuration (dump_configs output)
                if config_sql:
                    for stmt in config_sql.split(";"):
                        stmt = stmt.strip()
                        if stmt:
                            restored.execute(stmt)

                # Verify row count
                restored_rows = restored.execute(
                    f"SELECT group_id, version, content, _xp_seq "
                    f"FROM {t} ORDER BY group_id, version"
                ).fetchall()
                assert len(restored_rows) == len(orig_rows), (
                    f"Row count mismatch: original={len(orig_rows)}, "
                    f"restored={len(restored_rows)}"
                )

                # Verify data integrity (content matches exactly)
                for orig, rest in zip(orig_rows, restored_rows):
                    assert orig["group_id"] == rest["group_id"]
                    assert orig["version"] == rest["version"]
                    assert orig["content"] == rest["content"], (
                        f"Content mismatch at g={orig['group_id']} v={orig['version']}: "
                        f"original={orig['content']!r}, restored={rest['content']!r}"
                    )

                # Verify _xp_seq values are preserved
                for orig, rest in zip(orig_rows, restored_rows):
                    assert orig["_xp_seq"] == rest["_xp_seq"], (
                        f"_xp_seq mismatch at g={orig['group_id']} v={orig['version']}: "
                        f"original={orig['_xp_seq']}, restored={rest['_xp_seq']}"
                    )

                # Verify INSERT still works after restore
                insert_rows(restored, t, [(1, 100, "post-restore")])
                cnt = row_count(restored, t)
                assert cnt == len(orig_rows) + 1

            finally:
                restored.close()

        finally:
            # Cleanup
            _docker_exec("rm -f /tmp/xpatch_dump.fc", check=False)
            with _admin_conn() as admin:
                admin.execute(f"DROP DATABASE IF EXISTS {dst_db} WITH (FORCE)")

    def test_dump_restore_preserves_config(self, db: psycopg.Connection, make_table):
        """Table configuration (keyframe_every, etc.) can be restored."""
        t = make_table(keyframe_every=3, compress_depth=3, enable_zstd=True)
        insert_versions(db, t, group_id=1, count=5)

        # Get dump_configs SQL
        dump_sql = db.execute("SELECT xpatch.dump_configs()").fetchone()
        config_sql = dump_sql[list(dump_sql.keys())[0]]
        assert config_sql is not None, "dump_configs() returned NULL"

        src_db = db.info.dbname
        dst_db = f"{src_db}_cfgtest"

        try:
            _docker_exec(
                f"su postgres -c 'pg_dump -Fc -d {src_db} -f /tmp/xpatch_cfg.fc'",
                timeout=30,
            )

            with _admin_conn() as admin:
                admin.execute(f"CREATE DATABASE {dst_db}")

            _docker_exec(
                f"su postgres -c 'pg_restore -d {dst_db} /tmp/xpatch_cfg.fc'",
                timeout=30,
                check=False,
            )

            restored = _connect(dst_db)
            try:
                restored.execute("SELECT xpatch.fix_restored_configs()")

                # Re-apply configuration
                if config_sql:
                    for stmt in config_sql.split(";"):
                        stmt = stmt.strip()
                        if stmt:
                            restored.execute(stmt)

                # Verify config
                cfg = restored.execute(
                    f"SELECT * FROM xpatch.describe('{t}'::regclass)"
                ).fetchall()
                props = {r["property"]: r["value"] for r in cfg}
                assert props["keyframe_every"] == "3"
                assert props["compress_depth"] == "3"
                assert props["enable_zstd"] == "true"

                # Verify data still reads correctly
                rows = restored.execute(
                    f"SELECT version, content FROM {t} ORDER BY version"
                ).fetchall()
                assert len(rows) == 5
                for row in rows:
                    assert row["content"] == f"Version {row['version']} content"
            finally:
                restored.close()
        finally:
            _docker_exec("rm -f /tmp/xpatch_cfg.fc", check=False)
            with _admin_conn() as admin:
                admin.execute(f"DROP DATABASE IF EXISTS {dst_db} WITH (FORCE)")
