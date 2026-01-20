"""
Tests for xpatch.version() function.
"""

from xptest import pg_test


@pg_test(tags=["unit", "version"])
def test_version_returns_string(db):
    """xpatch.version() should return a non-empty string."""
    result = db.fetchval("SELECT xpatch.version()")
    assert result is not None, "version() returned NULL"
    assert isinstance(result, str), f"Expected string, got {type(result)}"
    assert len(result) > 0, "version() returned empty string"


@pg_test(tags=["unit", "version"])
def test_version_contains_pg_xpatch(db):
    """Version string should contain 'pg_xpatch'."""
    result = db.fetchval("SELECT xpatch.version()")
    assert "pg_xpatch" in result, f"Expected 'pg_xpatch' in version: {result}"


@pg_test(tags=["unit", "version"])
def test_version_contains_version_number(db):
    """Version string should contain a version number like X.Y.Z."""
    import re
    result = db.fetchval("SELECT xpatch.version()")
    # Match patterns like "0.2.0" or "1.0.0"
    pattern = r'\d+\.\d+\.\d+'
    match = re.search(pattern, result)
    assert match is not None, f"No version number found in: {result}"


@pg_test(tags=["unit", "version"])
def test_legacy_xpatch_version_function(db):
    """Legacy xpatch_version() function should still work."""
    new_version = db.fetchval("SELECT xpatch.version()")
    legacy_version = db.fetchval("SELECT xpatch_version()")
    assert new_version == legacy_version, (
        f"Version mismatch: xpatch.version()={new_version}, "
        f"xpatch_version()={legacy_version}"
    )
