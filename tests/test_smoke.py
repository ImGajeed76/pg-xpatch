"""
Smoke tests: verify the dev container and pg_xpatch extension are operational.

These run first (no special markers) and act as a gate for the rest of the suite.
"""

import psycopg


def test_pg_xpatch_version(db: psycopg.Connection, xpatch_expect_version: str | None):
    """Extension version in the container contains the expected version string."""
    assert xpatch_expect_version is not None, (
        "Could not determine expected version. "
        "Set XPATCH_EXPECT_VERSION env var or ensure pg_xpatch.control exists."
    )

    raw = db.execute("SELECT xpatch.version()").fetchone()["version"]

    # xpatch.version() returns e.g. "pg_xpatch 0.4.0 (xpatch 0.4.2)"
    assert xpatch_expect_version in raw, (
        f"Version mismatch: container reports '{raw}', "
        f"expected it to contain '{xpatch_expect_version}'. "
        f"Rebuild the dev container with the latest extension."
    )


def test_extension_loaded(db: psycopg.Connection):
    """pg_xpatch extension is installed and the access method is registered."""
    row = db.execute(
        "SELECT 1 FROM pg_am WHERE amname = 'xpatch'"
    ).fetchone()
    assert row is not None, "xpatch access method not registered"


def test_xpatch_schema_exists(db: psycopg.Connection):
    """The xpatch schema and its core functions exist."""
    functions = db.execute("""
        SELECT routine_name
        FROM information_schema.routines
        WHERE routine_schema = 'xpatch'
        ORDER BY routine_name
    """).fetchall()
    names = {r["routine_name"] for r in functions}

    required = {"configure", "version", "stats", "describe", "inspect"}
    missing = required - names
    assert not missing, f"Missing xpatch functions: {missing}"
