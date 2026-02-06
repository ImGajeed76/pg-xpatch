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
    ext = db.execute(
        "SELECT 1 FROM pg_extension WHERE extname = 'pg_xpatch'"
    ).fetchone()
    assert ext is not None, "pg_xpatch extension not installed"

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

    required = {
        "configure", "version", "stats", "describe", "inspect",
        "get_config", "physical", "cache_stats", "warm_cache",
    }
    missing = required - names
    assert not missing, f"Missing xpatch functions: {missing}"


def test_event_triggers_registered(db: psycopg.Connection):
    """Event triggers for auto-DDL are installed."""
    triggers = db.execute(
        "SELECT evtname FROM pg_event_trigger WHERE evtname LIKE 'xpatch_%'"
    ).fetchall()
    names = {r["evtname"] for r in triggers}
    assert "xpatch_add_seq_column" in names, "Missing xpatch_add_seq_column event trigger"
    assert "xpatch_cleanup_on_drop" in names, "Missing xpatch_cleanup_on_drop event trigger"


def test_can_create_xpatch_table(db: psycopg.Connection):
    """Minimal: CREATE TABLE USING xpatch succeeds and _xp_seq auto-added."""
    db.execute(
        "CREATE TABLE _smoke_probe (id INT, ver INT, body TEXT NOT NULL) USING xpatch"
    )
    row = db.execute(
        "SELECT 1 FROM information_schema.columns "
        "WHERE table_name = '_smoke_probe' AND column_name = '_xp_seq'"
    ).fetchone()
    assert row is not None, "_xp_seq column was not auto-added"


def test_can_insert_and_select(db: psycopg.Connection):
    """Minimal round-trip: INSERT then SELECT through xpatch TAM."""
    db.execute(
        "CREATE TABLE _smoke_rt (gid INT, ver INT, body TEXT NOT NULL) USING xpatch"
    )
    db.execute("SELECT xpatch.configure('_smoke_rt', group_by => 'gid', order_by => 'ver')")
    db.execute("INSERT INTO _smoke_rt (gid, ver, body) VALUES (1, 1, 'hello')")
    row = db.execute("SELECT body FROM _smoke_rt WHERE gid = 1").fetchone()
    assert row is not None, "No row returned after INSERT"
    assert row["body"] == "hello", f"Expected 'hello', got '{row['body']}'"
