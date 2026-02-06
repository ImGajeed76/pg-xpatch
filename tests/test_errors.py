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
    """INSERT ON CONFLICT uses PostgreSQL's speculative insert protocol.

    The xpatch TAM's ``xpatch_tuple_insert_speculative`` falls back to
    a regular insert (which acquires an advisory lock on the group),
    and ``xpatch_tuple_complete_speculative`` is a no-op.

    When a conflict is detected, PostgreSQL tries to abort the speculative
    insert.  But the advisory lock is already held by this transaction,
    and the regular insert already physically wrote the row.  The result
    is an **uninterruptible hang** — even ``pg_terminate_backend`` cannot
    kill the stuck backend, requiring ``pg_ctl stop -m immediate``.

    Bug: xpatch_tam.c:982-996
    Severity: CRITICAL — requires server restart to recover
    """

    @pytest.mark.skip(
        reason="H2: INSERT ON CONFLICT causes uninterruptible hang — "
        "leaves zombie backend that requires pg_ctl stop -m immediate. "
        "Run manually with: pytest -k InsertOnConflict --no-header"
    )
    @pytest.mark.timeout(15)
    def test_insert_on_conflict_do_nothing_hangs(
        self, db: psycopg.Connection, make_table
    ):
        """INSERT ... ON CONFLICT DO NOTHING enters uninterruptible C loop.

        This test is skipped by default because:
        1. The backend enters a tight C loop that ignores CHECK_FOR_INTERRUPTS
        2. Even statement_timeout and pg_terminate_backend cannot stop it
        3. The only recovery is pg_ctl stop -m immediate (server restart)
        4. This leaves the test database orphaned
        """
        t = make_table()
        db.execute("SET statement_timeout = '3s'")
        db.execute(f"CREATE UNIQUE INDEX ON {t} (group_id, version)")

        insert_rows(db, t, [(1, 1, "v1")])

        db.execute(
            f"INSERT INTO {t} (group_id, version, content) VALUES (1, 1, 'v1-dup') "
            f"ON CONFLICT (group_id, version) DO NOTHING"
        )

        assert row_count(db, t) == 1

    @pytest.mark.skip(
        reason="H2: INSERT ON CONFLICT DO UPDATE causes same uninterruptible hang"
    )
    @pytest.mark.timeout(15)
    def test_insert_on_conflict_do_update_hangs(
        self, db: psycopg.Connection, make_table
    ):
        """INSERT ... ON CONFLICT DO UPDATE enters uninterruptible C loop."""
        t = make_table()
        db.execute("SET statement_timeout = '3s'")
        db.execute(f"CREATE UNIQUE INDEX ON {t} (group_id, version)")

        insert_rows(db, t, [(1, 1, "original")])

        db.execute(
            f"INSERT INTO {t} (group_id, version, content) VALUES (1, 1, 'updated') "
            f"ON CONFLICT (group_id, version) DO UPDATE SET content = EXCLUDED.content"
        )

        assert row_count(db, t) == 1


# ---------------------------------------------------------------------------
# H5 — TABLESAMPLE silently returns zero rows (known bug)
# ---------------------------------------------------------------------------


class TestTableSampleBroken:
    """The TABLESAMPLE callbacks are stubs that always return false.

    ``SELECT * FROM t TABLESAMPLE BERNOULLI(100)`` silently returns zero rows
    instead of either working correctly or raising FeatureNotSupported.

    Bug: xpatch_tam.c:2564-2578
    """

    @pytest.mark.xfail(
        reason="H5: TABLESAMPLE silently returns 0 rows — should either work or error",
        strict=True,
    )
    def test_tablesample_bernoulli_100_returns_all_rows(
        self, db: psycopg.Connection, xpatch_table
    ):
        """TABLESAMPLE BERNOULLI(100) should return all rows."""
        t = xpatch_table
        insert_versions(db, t, group_id=1, count=5)

        rows = db.execute(
            f"SELECT * FROM {t} TABLESAMPLE BERNOULLI(100)"
        ).fetchall()
        assert len(rows) == 5

    @pytest.mark.xfail(
        reason="H5: TABLESAMPLE silently returns 0 rows instead of raising error",
        strict=True,
    )
    def test_tablesample_system_returns_rows_or_errors(
        self, db: psycopg.Connection, xpatch_table
    ):
        """TABLESAMPLE SYSTEM(100) should return rows or raise an error."""
        t = xpatch_table
        insert_versions(db, t, group_id=1, count=10)

        try:
            rows = db.execute(
                f"SELECT * FROM {t} TABLESAMPLE SYSTEM(100)"
            ).fetchall()
            assert len(rows) > 0, "TABLESAMPLE SYSTEM(100) returned 0 rows silently"
        except psycopg.errors.FeatureNotSupported:
            pass  # Acceptable — at least it's explicit


# ---------------------------------------------------------------------------
# E13/E17 — Auto-detection and type-validation error paths (known bugs)
# ---------------------------------------------------------------------------


class TestAutoDetectionErrors:
    """Error paths in the C code that are not exercised until triggered."""

    @pytest.mark.xfail(
        reason="E13: auto_detect_order_by should fail on table with no INT/TIMESTAMP columns",
        strict=True,
    )
    def test_no_order_by_column_auto_detection_fails(self, db: psycopg.Connection):
        """A table with only TEXT columns should fail auto-detection of order_by."""
        t = "test_no_orderby"
        db.execute(
            f"CREATE TABLE {t} (name TEXT NOT NULL, body TEXT NOT NULL) USING xpatch"
        )
        try:
            with pytest.raises(
                psycopg.errors.InvalidParameterValue,
                match="order_by column",
            ):
                db.execute(f"SELECT xpatch.configure('{t}')")
        finally:
            db.execute(f"DROP TABLE IF EXISTS {t}")

    @pytest.mark.xfail(
        reason="E17: order_by column type validation should reject TEXT columns",
        strict=True,
    )
    def test_wrong_order_by_type_rejected(self, db: psycopg.Connection):
        """Explicitly setting order_by to a TEXT column should raise DatatypeMismatch."""
        t = "test_wrong_orderby"
        db.execute(
            f"CREATE TABLE {t} (id INT, name TEXT NOT NULL, body TEXT NOT NULL) "
            f"USING xpatch"
        )
        try:
            with pytest.raises(
                psycopg.errors.DatatypeMismatch,
                match="order_by column.*must be",
            ):
                db.execute(
                    f"SELECT xpatch.configure('{t}', order_by => 'name')"
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
