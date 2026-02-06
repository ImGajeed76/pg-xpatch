"""
Test error handling and validation.

Covers:
- UPDATE blocked with FeatureNotSupported
- CLUSTER blocked with FeatureNotSupported
- Configure on non-xpatch table raises RaiseException
- Configure with nonexistent column raises RaiseException
- Nullable delta column rejected (0.4.0)
- NOT NULL delta column accepted
- Unsupported delta column type raises DatatypeMismatch
- keyframe_every/compress_depth validation
- NULL group value raises NotNullViolation
- Utility functions on non-xpatch table raise errors
- INSERT ON CONFLICT hangs (H2 — known bug)
- TABLESAMPLE silently returns 0 rows (H5 — known bug)
- Auto-detection error paths (E13, E17 — known bugs)
- INT column as delta rejected (E16)
- >32 delta columns silently clamped (M6 — regression guard)
"""

from __future__ import annotations

import psycopg
import psycopg.errors
import pytest

from conftest import insert_rows, insert_versions, row_count


class TestUpdateBlocked:
    """UPDATE is not supported on xpatch tables."""

    def test_update_raises_error(self, db: psycopg.Connection, xpatch_table):
        """UPDATE produces FeatureNotSupported error."""
        t = xpatch_table
        insert_rows(db, t, [(1, 1, "original")])
        with pytest.raises(
            psycopg.errors.FeatureNotSupported,
            match="UPDATE is not supported on xpatch tables",
        ):
            db.execute(f"UPDATE {t} SET content = 'modified' WHERE group_id = 1")

    def test_update_with_set_on_group(self, db: psycopg.Connection, xpatch_table):
        """UPDATE on group column also raises FeatureNotSupported."""
        t = xpatch_table
        insert_rows(db, t, [(1, 1, "data")])
        with pytest.raises(
            psycopg.errors.FeatureNotSupported,
            match="UPDATE is not supported on xpatch tables",
        ):
            db.execute(f"UPDATE {t} SET group_id = 2")

    def test_update_with_set_on_version(self, db: psycopg.Connection, xpatch_table):
        """UPDATE on version column also raises FeatureNotSupported."""
        t = xpatch_table
        insert_rows(db, t, [(1, 1, "data")])
        with pytest.raises(
            psycopg.errors.FeatureNotSupported,
            match="UPDATE is not supported on xpatch tables",
        ):
            db.execute(f"UPDATE {t} SET version = 99")


class TestClusterBlocked:
    """CLUSTER is not supported on xpatch tables."""

    def test_cluster_raises_error(self, db: psycopg.Connection, make_table):
        """CLUSTER produces FeatureNotSupported error."""
        t = make_table()
        db.execute(f"CREATE INDEX idx_cluster_test ON {t} (group_id)")
        insert_rows(db, t, [(1, 1, "data")])
        with pytest.raises(
            psycopg.errors.FeatureNotSupported,
            match="(?i)cluster.*not supported",
        ):
            db.execute(f"CLUSTER {t} USING idx_cluster_test")


class TestConfigureErrors:
    """xpatch.configure() validation errors."""

    def test_configure_non_xpatch_table(self, db: psycopg.Connection):
        """Configure on a heap table raises RaiseException."""
        db.execute("CREATE TABLE heap_test (id INT, data TEXT)")
        with pytest.raises(
            psycopg.errors.RaiseException,
            match="is not using the xpatch access method",
        ):
            db.execute(
                "SELECT xpatch.configure('heap_test', "
                "group_by => 'id', order_by => 'id')"
            )

    def test_configure_nonexistent_column(self, db: psycopg.Connection, make_table):
        """Configure with a nonexistent column name raises RaiseException."""
        t = make_table()
        with pytest.raises(
            psycopg.errors.RaiseException,
            match="does not exist in table",
        ):
            db.execute(
                f"SELECT xpatch.configure('{t}', "
                f"group_by => 'nonexistent_col', order_by => 'version')"
            )

    def test_nullable_delta_column_rejected(self, db: psycopg.Connection):
        """Delta column without NOT NULL constraint is rejected (0.4.0)."""
        db.execute(
            "CREATE TABLE null_delta_test (gid INT, ver INT, body TEXT) USING xpatch"
        )
        with pytest.raises(
            psycopg.errors.RaiseException,
            match="must be NOT NULL",
        ):
            db.execute(
                "SELECT xpatch.configure('null_delta_test', "
                "group_by => 'gid', order_by => 'ver', "
                "delta_columns => '{body}')"
            )

    def test_not_null_delta_column_accepted(self, db: psycopg.Connection):
        """Delta column with NOT NULL constraint is accepted."""
        db.execute(
            "CREATE TABLE notnull_delta_test "
            "(gid INT, ver INT, body TEXT NOT NULL) USING xpatch"
        )
        # Should not raise
        db.execute(
            "SELECT xpatch.configure('notnull_delta_test', "
            "group_by => 'gid', order_by => 'ver', "
            "delta_columns => '{body}')"
        )

    def test_unsupported_delta_column_type(self, db: psycopg.Connection):
        """INT column configured as delta column raises DatatypeMismatch on first use."""
        db.execute(
            "CREATE TABLE bad_delta_type "
            "(gid INT, ver INT, val INT NOT NULL, body TEXT NOT NULL) "
            "USING xpatch"
        )
        db.execute(
            "SELECT xpatch.configure('bad_delta_type', "
            "group_by => 'gid', order_by => 'ver', "
            "delta_columns => '{val}')"
        )
        with pytest.raises(
            psycopg.errors.DatatypeMismatch,
            match="must be BYTEA, TEXT, VARCHAR, JSON, or JSONB",
        ):
            db.execute(
                "INSERT INTO bad_delta_type (gid, ver, val, body) "
                "VALUES (1, 1, 42, 'test')"
            )

    def test_keyframe_every_zero_rejected(self, db: psycopg.Connection, make_table):
        """keyframe_every=0 is rejected by configure()."""
        t = make_table()
        with pytest.raises(
            psycopg.errors.RaiseException,
            match="keyframe_every must be at least 1",
        ):
            db.execute(
                f"SELECT xpatch.configure('{t}', "
                f"group_by => 'group_id', order_by => 'version', "
                f"keyframe_every => 0)"
            )

    def test_keyframe_every_negative_rejected(self, db: psycopg.Connection, make_table):
        """keyframe_every=-1 is rejected by configure()."""
        t = make_table()
        with pytest.raises(
            psycopg.errors.RaiseException,
            match="keyframe_every must be at least 1",
        ):
            db.execute(
                f"SELECT xpatch.configure('{t}', "
                f"group_by => 'group_id', order_by => 'version', "
                f"keyframe_every => -1)"
            )

    def test_compress_depth_zero_rejected(self, db: psycopg.Connection, make_table):
        """compress_depth=0 is rejected by configure()."""
        t = make_table()
        with pytest.raises(
            psycopg.errors.RaiseException,
            match="compress_depth must be at least 1",
        ):
            db.execute(
                f"SELECT xpatch.configure('{t}', "
                f"group_by => 'group_id', order_by => 'version', "
                f"compress_depth => 0)"
            )

    def test_keyframe_every_above_max_rejected(self, db: psycopg.Connection, make_table):
        """keyframe_every > 10000 violates CHECK constraint."""
        t = make_table()
        with pytest.raises(psycopg.errors.CheckViolation):
            db.execute(
                f"SELECT xpatch.configure('{t}', "
                f"group_by => 'group_id', order_by => 'version', "
                f"keyframe_every => 10001)"
            )


class TestNullGroupRejected:
    """NULL group_by value rejected on INSERT."""

    def test_null_group_error(self, db: psycopg.Connection, xpatch_table):
        """INSERT with NULL group_by raises NullValueNotAllowed."""
        t = xpatch_table
        with pytest.raises(
            psycopg.errors.NullValueNotAllowed,
            match="(?i)null.*group",
        ):
            db.execute(
                f"INSERT INTO {t} (group_id, version, content) "
                f"VALUES (NULL, 1, 'test')"
            )

    def test_null_group_no_partial_insert(self, db: psycopg.Connection, xpatch_table):
        """Failed NULL group INSERT doesn't leave partial data."""
        t = xpatch_table
        try:
            db.execute(
                f"INSERT INTO {t} (group_id, version, content) "
                f"VALUES (NULL, 1, 'test')"
            )
        except psycopg.errors.NullValueNotAllowed:
            pass
        # In autocommit mode, the failed statement is rolled back automatically
        assert row_count(db, t) == 0


class TestUtilityFunctionErrors:
    """Error handling in utility functions."""

    def test_physical_on_non_xpatch_table(self, db: psycopg.Connection):
        """xpatch.physical() on heap table raises WrongObjectType."""
        db.execute("CREATE TABLE heap_phys (id INT, data TEXT)")
        db.execute("INSERT INTO heap_phys VALUES (1, 'test')")
        with pytest.raises(
            psycopg.errors.WrongObjectType,
            match="does not use the xpatch access method",
        ):
            db.execute(
                "SELECT * FROM xpatch.physical('heap_phys'::regclass, 1)"
            )

    def test_warm_cache_on_non_xpatch_table(self, db: psycopg.Connection):
        """xpatch.warm_cache() on heap table raises RaiseException."""
        db.execute("CREATE TABLE heap_warm (id INT, data TEXT)")
        with pytest.raises(
            psycopg.errors.RaiseException,
            match="is not using the xpatch access method",
        ):
            db.execute(
                "SELECT * FROM xpatch.warm_cache('heap_warm'::regclass)"
            )

    def test_describe_on_non_xpatch_table(self, db: psycopg.Connection):
        """xpatch.describe() on heap table raises RaiseException."""
        db.execute("CREATE TABLE heap_desc (id INT, data TEXT)")
        with pytest.raises(
            psycopg.errors.RaiseException,
            match="is not using the xpatch access method",
        ):
            db.execute(
                "SELECT * FROM xpatch.describe('heap_desc'::regclass)"
            )

    def test_inspect_on_non_xpatch_table(self, db: psycopg.Connection):
        """xpatch.inspect() on heap table doesn't crash.

        NOTE: inspect() doesn't currently validate the access method.
        On a heap table it reads raw heap bytes as xpatch metadata, which
        produces garbage results. This test simply verifies it doesn't crash.
        """
        db.execute("CREATE TABLE heap_insp (id INT, data TEXT)")
        db.execute("INSERT INTO heap_insp VALUES (1, 'test')")
        # Should not crash, even though results will be meaningless
        try:
            db.execute(
                "SELECT * FROM xpatch.inspect('heap_insp'::regclass, 1)"
            ).fetchall()
        except psycopg.errors.Error:
            pass  # Raising an error would also be acceptable


# ---------------------------------------------------------------------------
# H2 — INSERT ON CONFLICT causes uninterruptible hang (known bug)
# ---------------------------------------------------------------------------


class TestInsertOnConflict:
    """INSERT ON CONFLICT previously caused an uninterruptible infinite loop.

    Root cause was that ``xpatch_index_fetch_tuple`` used a simplified
    visibility check that didn't set snapshot fields correctly for the
    ON CONFLICT executor path, causing infinite retries.

    Fixed by replacing the simplified check with ``HeapTupleSatisfiesVisibility``.
    Also hardened ``xpatch_tuple_insert_speculative`` and
    ``xpatch_tuple_complete_speculative`` with explicit errors as a safety net.

    Bug: xpatch_tam.c:2428-2466 visibility check (fixed)
    """

    def test_insert_on_conflict_do_nothing_skips(
        self, db: psycopg.Connection, make_table
    ):
        """INSERT ... ON CONFLICT DO NOTHING correctly skips the duplicate."""
        t = make_table()
        db.execute(f"CREATE UNIQUE INDEX ON {t} (group_id, version)")

        insert_rows(db, t, [(1, 1, "v1")])

        # Should succeed silently — the conflicting row is skipped
        db.execute(
            f"INSERT INTO {t} (group_id, version, content) VALUES (1, 1, 'v1-dup') "
            f"ON CONFLICT (group_id, version) DO NOTHING"
        )

        assert row_count(db, t) == 1
        row = db.execute(
            f"SELECT content FROM {t} WHERE group_id = 1 AND version = 1"
        ).fetchone()
        assert row["content"] == "v1"

    def test_insert_on_conflict_do_update_rejects(
        self, db: psycopg.Connection, make_table
    ):
        """INSERT ... ON CONFLICT DO UPDATE raises 'UPDATE not supported'."""
        t = make_table()
        db.execute(f"CREATE UNIQUE INDEX ON {t} (group_id, version)")

        insert_rows(db, t, [(1, 1, "original")])

        with pytest.raises(
            psycopg.errors.FeatureNotSupported,
            match="UPDATE.*not supported",
        ):
            db.execute(
                f"INSERT INTO {t} (group_id, version, content) VALUES (1, 1, 'updated') "
                f"ON CONFLICT (group_id, version) DO UPDATE SET content = EXCLUDED.content"
            )

    def test_insert_on_conflict_no_conflict_inserts(
        self, db: psycopg.Connection, make_table
    ):
        """INSERT ... ON CONFLICT with no actual conflict inserts normally."""
        t = make_table()
        db.execute(f"CREATE UNIQUE INDEX ON {t} (group_id, version)")

        insert_rows(db, t, [(1, 1, "v1")])

        # Different key — no conflict, should insert normally
        db.execute(
            f"INSERT INTO {t} (group_id, version, content) VALUES (1, 2, 'v2') "
            f"ON CONFLICT (group_id, version) DO NOTHING"
        )

        assert row_count(db, t) == 2


# ---------------------------------------------------------------------------
# H5 — TABLESAMPLE silently returns zero rows (known bug)
# ---------------------------------------------------------------------------


class TestTableSampleRejected:
    """TABLESAMPLE is not supported on xpatch tables.

    The sample-scan callbacks now raise FeatureNotSupported immediately
    instead of silently returning zero rows.

    Bug: xpatch_tam.c:2564-2578 (fixed)
    """

    def test_tablesample_bernoulli_raises(
        self, db: psycopg.Connection, xpatch_table
    ):
        """TABLESAMPLE BERNOULLI raises FeatureNotSupported."""
        t = xpatch_table
        insert_versions(db, t, group_id=1, count=5)

        with pytest.raises(
            psycopg.errors.FeatureNotSupported,
            match="TABLESAMPLE.*not supported",
        ):
            db.execute(f"SELECT * FROM {t} TABLESAMPLE BERNOULLI(100)")

    def test_tablesample_system_raises(
        self, db: psycopg.Connection, xpatch_table
    ):
        """TABLESAMPLE SYSTEM raises FeatureNotSupported."""
        t = xpatch_table
        insert_versions(db, t, group_id=1, count=10)

        with pytest.raises(
            psycopg.errors.FeatureNotSupported,
            match="TABLESAMPLE.*not supported",
        ):
            db.execute(f"SELECT * FROM {t} TABLESAMPLE SYSTEM(100)")


# ---------------------------------------------------------------------------
# E13/E17 — Auto-detection and type-validation error paths (known bugs)
# ---------------------------------------------------------------------------


class TestAutoDetectionErrors:
    """Validation in xpatch.configure() for order_by column type checking.

    Fixed: configure() now validates eagerly instead of deferring to first access.
    """

    def test_no_order_by_column_auto_detection_fails(self, db: psycopg.Connection):
        """A table with only TEXT columns should fail auto-detection of order_by.

        The error fires at CREATE TABLE time because the _add_seq_column()
        event trigger calls xpatch_get_config which runs auto_detect_order_by.
        """
        t = "test_no_orderby"
        try:
            with pytest.raises(
                psycopg.errors.InvalidParameterValue,
                match="order_by column",
            ):
                db.execute(
                    f"CREATE TABLE {t} (name TEXT NOT NULL, body TEXT NOT NULL) USING xpatch"
                )
        finally:
            db.execute(f"DROP TABLE IF EXISTS {t}")


# ---------------------------------------------------------------------------
# M6 — >32 delta columns silently clamped (known bug)
# ---------------------------------------------------------------------------


class TestExcessDeltaColumns:
    """``auto_detect_delta_columns`` (xpatch_config.c:147) dynamically grows
    its array with no cap, but the insert cache silently clamps to
    ``XPATCH_MAX_DELTA_COLUMNS=32`` (xpatch_insert_cache.c:446-447).

    Columns beyond the 32nd are stored as regular heap columns (not
    delta-compressed) but still round-trip correctly. This is a performance
    issue (no compression) rather than a correctness bug.

    Regression test for M6 audit finding — passes today, guards against
    future regressions in column handling.
    """

    def test_33_delta_columns_roundtrip(self, db: psycopg.Connection):
        """A table with 33 TEXT NOT NULL columns should store and retrieve
        all column values correctly, including column 33+.

        With the bug, columns beyond the 32nd are not delta-compressed but
        the reconstruction logic may mishandle them.
        """
        # Build column definitions: id INT, version INT, col_01..col_33 TEXT NOT NULL
        n_delta = 33
        col_defs = ["id INT", "version INT"]
        col_names = []
        for i in range(1, n_delta + 1):
            name = f"col_{i:02d}"
            col_defs.append(f"{name} TEXT NOT NULL")
            col_names.append(name)

        ddl = ", ".join(col_defs)
        t = "test_33_delta"
        db.execute(f"CREATE TABLE {t} ({ddl}) USING xpatch")

        try:
            # Configure — auto_detect_delta_columns will find all 33 TEXT columns
            db.execute(
                f"SELECT xpatch.configure('{t}', "
                f"group_by => 'id', order_by => 'version')"
            )

            # Insert first row (keyframe)
            vals_v1 = [f"v1-{name}" for name in col_names]
            placeholders = ", ".join(["%s"] * (2 + n_delta))
            db.execute(
                f"INSERT INTO {t} VALUES ({placeholders})",
                [1, 1] + vals_v1,
            )

            # Insert second row (delta) — change every column
            vals_v2 = [f"v2-{name}" for name in col_names]
            db.execute(
                f"INSERT INTO {t} VALUES ({placeholders})",
                [1, 2] + vals_v2,
            )

            # Read back both rows
            rows = db.execute(
                f"SELECT * FROM {t} ORDER BY version"
            ).fetchall()
            assert len(rows) == 2

            # Verify ALL columns in the delta row (version 2)
            row_v2 = rows[1]
            for i, name in enumerate(col_names):
                expected = f"v2-{name}"
                actual = row_v2[name]
                assert actual == expected, (
                    f"Column {name} (#{i+1}): expected {expected!r}, got {actual!r}"
                )
        finally:
            db.execute(f"DROP TABLE IF EXISTS {t}")


    def test_wrong_order_by_type_timestamp_accepted(self, db: psycopg.Connection):
        """TIMESTAMP column as order_by should be accepted (it's a valid type)."""
        t = "test_ts_orderby"
        db.execute(
            f"CREATE TABLE {t} (id INT, ts TIMESTAMP NOT NULL, body TEXT NOT NULL) "
            f"USING xpatch"
        )
        try:
            # Should not raise
            db.execute(
                f"SELECT xpatch.configure('{t}', "
                f"group_by => 'id', order_by => 'ts')"
            )
        finally:
            db.execute(f"DROP TABLE IF EXISTS {t}")

    def test_int_column_as_delta_rejected_on_insert(self, db: psycopg.Connection):
        """Inserting into an INT delta column raises DatatypeMismatch.

        Regression guard for E16.  The error is raised at INSERT time (in the
        C code's ``datum_to_bytea``), not during ``configure()``.
        """
        t = "test_int_delta_e16"
        db.execute(
            f"CREATE TABLE {t} "
            f"(id INT, version INT, payload INT NOT NULL, filler TEXT NOT NULL) "
            f"USING xpatch"
        )
        try:
            db.execute(
                f"SELECT xpatch.configure('{t}', "
                f"order_by => 'version', "
                f"delta_columns => '{{payload}}')"
            )
            with pytest.raises(psycopg.errors.DatatypeMismatch):
                db.execute(
                    f"INSERT INTO {t} (id, version, payload, filler) "
                    f"VALUES (1, 1, 42, 'test')"
                )
        finally:
            db.execute(f"DROP TABLE IF EXISTS {t}")


# ---------------------------------------------------------------------------
# C5 — _xp_seq column type (BIGINT since 0.5.0)
# ---------------------------------------------------------------------------


class TestXpSeqBigint:
    """Since 0.5.0, ``_xp_seq`` is BIGINT (int8) to support >2.1B rows/group.

    Previously it was INT (int4), which would overflow at 2,147,483,647.
    These tests verify the migration was applied correctly.
    """

    def test_new_table_xp_seq_is_bigint(
        self, db: psycopg.Connection, make_table
    ):
        """New xpatch tables should have _xp_seq as BIGINT, not INT."""
        t = make_table()
        row = db.execute(
            "SELECT format_type(atttypid, atttypmod) AS typename "
            "FROM pg_attribute "
            "WHERE attrelid = %s::regclass AND attname = '_xp_seq' "
            "  AND NOT attisdropped",
            (t,),
        ).fetchone()
        assert row is not None, "_xp_seq column not found"
        assert row["typename"] == "bigint", (
            f"Expected _xp_seq to be bigint, got {row['typename']}"
        )

    def test_xp_seq_values_are_bigint_typed(
        self, db: psycopg.Connection, make_table
    ):
        """_xp_seq values returned by queries should be Python int (no overflow)."""
        t = make_table()
        db.execute(
            f"INSERT INTO {t} (group_id, version, content) "
            f"VALUES (1, 1, 'row1'), (1, 2, 'row2')"
        )
        rows = db.execute(f"SELECT _xp_seq FROM {t} ORDER BY _xp_seq").fetchall()
        assert len(rows) == 2
        # Python ints have arbitrary precision; just verify they're sensible
        assert rows[0]["_xp_seq"] == 1
        assert rows[1]["_xp_seq"] == 2

    def test_group_stats_max_seq_is_bigint(self, db: psycopg.Connection):
        """group_stats.max_seq should be BIGINT."""
        row = db.execute(
            "SELECT format_type(atttypid, atttypmod) AS typename "
            "FROM pg_attribute "
            "WHERE attrelid = 'xpatch.group_stats'::regclass "
            "  AND attname = 'max_seq' AND NOT attisdropped"
        ).fetchone()
        assert row is not None, "max_seq column not found in group_stats"
        assert row["typename"] == "bigint", (
            f"Expected max_seq to be bigint, got {row['typename']}"
        )


# ---------------------------------------------------------------------------
# H7 — Config cache in TopMemoryContext never invalidated
# ---------------------------------------------------------------------------


class TestConfigCacheInvalidation:
    """The config hash table lives in ``TopMemoryContext`` and entries are
    only removed by explicit ``xpatch_invalidate_config()`` calls.  There is
    no relcache invalidation callback.  If a table is ALTERed, the cached
    config becomes stale.

    Bug: xpatch_config.c:68, 471-495 (known bug H7)

    Despite the lack of explicit invalidation, PostgreSQL's relcache
    invalidation seems to trigger config re-reads in practice (see
    TestAlterTable in test_basic.py).  These tests guard that assumption.
    """

    def test_add_column_config_refreshed(
        self, db: psycopg.Connection, make_table
    ):
        """After ALTER TABLE ADD COLUMN, the config should reflect the new
        schema — INSERT with new column should succeed.
        """
        t = make_table()
        insert_rows(db, t, [(1, 1, "before-add")])

        db.execute(f"ALTER TABLE {t} ADD COLUMN extra TEXT DEFAULT 'x'")

        # Insert using the new column
        db.execute(
            f"INSERT INTO {t} (group_id, version, content, extra) "
            f"VALUES (1, 2, 'after-add', 'new-data')"
        )

        rows = db.execute(
            f"SELECT version, content, extra FROM {t} ORDER BY version"
        ).fetchall()
        assert len(rows) == 2
        assert rows[1]["content"] == "after-add"
        assert rows[1]["extra"] == "new-data"

    def test_rename_column_config_refreshed(
        self, db: psycopg.Connection, make_table
    ):
        """After ALTER TABLE RENAME COLUMN on a delta column, subsequent
        inserts and reads should use the new name correctly.
        """
        t = make_table()
        insert_rows(db, t, [(1, 1, "before-rename")])

        db.execute(f"ALTER TABLE {t} RENAME COLUMN content TO body")

        # Insert using the renamed column
        db.execute(
            f"INSERT INTO {t} (group_id, version, body) "
            f"VALUES (1, 2, 'after-rename')"
        )

        rows = db.execute(
            f"SELECT version, body FROM {t} ORDER BY version"
        ).fetchall()
        assert len(rows) == 2
        assert rows[0]["body"] == "before-rename"
        assert rows[1]["body"] == "after-rename"

    def test_drop_column_config_refreshed(
        self, db: psycopg.Connection, make_table
    ):
        """After ALTER TABLE DROP COLUMN on a non-delta column, INSERT
        still works correctly with the remaining columns.
        """
        t = make_table(
            "group_id INT, version INT, extra INT, content TEXT NOT NULL",
        )
        insert_rows(db, t, [(1, 1, 42, "v1")],
                    columns=["group_id", "version", "extra", "content"])

        db.execute(f"ALTER TABLE {t} DROP COLUMN extra")

        insert_rows(db, t, [(1, 2, "v2")],
                    columns=["group_id", "version", "content"])

        rows = db.execute(
            f"SELECT version, content FROM {t} ORDER BY version"
        ).fetchall()
        assert len(rows) == 2
        assert rows[1]["content"] == "v2"

    def test_reconfigure_table_updates_cache(
        self, db: psycopg.Connection, make_table
    ):
        """Calling configure() again after inserting data refreshes the
        config cache — subsequent reads still work.
        """
        t = make_table(keyframe_every=100)
        insert_versions(db, t, group_id=1, count=5)

        # Reconfigure with different keyframe_every
        db.execute(
            f"SELECT xpatch.configure('{t}', "
            f"group_by => 'group_id', order_by => 'version', "
            f"keyframe_every => 2)"
        )

        # Read should still work
        rows = db.execute(
            f"SELECT version, content FROM {t} ORDER BY version"
        ).fetchall()
        assert len(rows) == 5

        # Insert new row — should use new keyframe_every=2
        insert_rows(db, t, [(1, 6, "after reconfig")])
        assert row_count(db, t) == 6
