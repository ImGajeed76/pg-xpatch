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
