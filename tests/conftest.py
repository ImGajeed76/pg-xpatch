"""
Shared pytest fixtures for pg-xpatch integration tests.

Each test gets its own isolated PostgreSQL database with pg_xpatch installed.
Databases are created/dropped automatically via the ``db`` fixture.

Configuration via environment variables:
    PGHOST                  PostgreSQL host     (default: auto-detect from pg-xpatch-dev container)
    PGPORT                  PostgreSQL port     (default: 5432)
    PGUSER                  PostgreSQL user     (default: postgres)
    PGPASSWORD              PostgreSQL password (default: None)
    XPATCH_EXPECT_VERSION   Expected pg_xpatch version (default: read from pg_xpatch.control)
    PG_XPATCH_CONTAINER     Docker container name (default: pg-xpatch-dev)

Run tests:
    pytest                          # all tests, sequential
    pytest -n auto                  # parallel via pytest-xdist (auto-detect CPUs)
    pytest -n 8                     # 8 parallel workers
    pytest -x                       # stop on first failure
    pytest -m "not slow"            # skip slow tests
    pytest -m "not stress"          # skip stress tests
    pytest -m "not crash_test"      # skip crash recovery tests
    pytest -k "test_basic"          # filter by name
"""

from __future__ import annotations

import os
import re
import subprocess
import uuid
from pathlib import Path
from typing import Any, Callable, Generator

import psycopg
import pytest
from psycopg import sql
from psycopg.rows import dict_row


# ---------------------------------------------------------------------------
# Connection settings (from environment, with sensible defaults)
# ---------------------------------------------------------------------------

CONTAINER_NAME = os.environ.get("PG_XPATCH_CONTAINER", "pg-xpatch-dev")

# Timeout settings (seconds)
CONNECT_TIMEOUT = 10
STATEMENT_TIMEOUT_MS = 30_000  # 30 s — per-statement guard


def _detect_container_ip(container: str = CONTAINER_NAME) -> str | None:
    """Try to get the IP of the dev container. Returns None on failure."""
    try:
        result = subprocess.run(
            [
                "docker", "inspect", "-f",
                "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
                container,
            ],
            capture_output=True, text=True, timeout=5,
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except Exception:
        pass
    return None


def _read_version_from_control() -> str | None:
    """Read default_version from pg_xpatch.control at repo root."""
    control = Path(__file__).resolve().parent.parent / "pg_xpatch.control"
    try:
        text = control.read_text()
        match = re.search(r"default_version\s*=\s*'([^']+)'", text)
        return match.group(1) if match else None
    except Exception:
        return None


PG_HOST = os.environ.get("PGHOST") or _detect_container_ip() or "localhost"
PG_PORT = int(os.environ.get("PGPORT", "5432"))
PG_USER = os.environ.get("PGUSER", "postgres")
PG_PASSWORD = os.environ.get("PGPASSWORD") or None  # empty string treated as unset

XPATCH_EXPECT_VERSION = (
    os.environ.get("XPATCH_EXPECT_VERSION") or _read_version_from_control()
)


# ---------------------------------------------------------------------------
# Low-level connection helpers
# ---------------------------------------------------------------------------

def _pg_kwargs(
    dbname: str = "postgres",
    *,
    autocommit: bool = True,
    row_factory: Any = None,
    statement_timeout: int | None = STATEMENT_TIMEOUT_MS,
) -> dict[str, Any]:
    """Build a kwargs dict for psycopg.connect()."""
    opts_parts = []
    if statement_timeout is not None:
        opts_parts.append(f"-c statement_timeout={statement_timeout}")

    kwargs: dict[str, Any] = {
        "host": PG_HOST,
        "port": PG_PORT,
        "user": PG_USER,
        "dbname": dbname,
        "autocommit": autocommit,
        "connect_timeout": CONNECT_TIMEOUT,
    }
    if PG_PASSWORD:
        kwargs["password"] = PG_PASSWORD
    if row_factory is not None:
        kwargs["row_factory"] = row_factory
    if opts_parts:
        kwargs["options"] = " ".join(opts_parts)
    return kwargs


def _admin_conn() -> psycopg.Connection:
    """Autocommit connection to ``postgres`` for DDL (CREATE/DROP DATABASE)."""
    return psycopg.connect(**_pg_kwargs("postgres", statement_timeout=None))


def _connect(
    dbname: str,
    *,
    autocommit: bool = True,
    statement_timeout: int | None = STATEMENT_TIMEOUT_MS,
) -> psycopg.Connection:
    """Connection to *dbname* with dict-row factory."""
    return psycopg.connect(
        **_pg_kwargs(
            dbname,
            autocommit=autocommit,
            row_factory=dict_row,
            statement_timeout=statement_timeout,
        )
    )


# ---------------------------------------------------------------------------
# Database lifecycle helpers
# ---------------------------------------------------------------------------

def _create_database(name: str) -> None:
    """Create a fresh database (fails loudly on name collision)."""
    with _admin_conn() as conn:
        ident = sql.Identifier(name)
        conn.execute(sql.SQL("CREATE DATABASE {}").format(ident))


def _drop_database(name: str) -> None:
    """Drop a database, force-terminating all connections (PG 13+)."""
    try:
        with _admin_conn() as conn:
            ident = sql.Identifier(name)
            conn.execute(
                sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(ident)
            )
    except Exception:
        pass  # best-effort — orphan cleanup will catch leftovers


def _drop_orphans() -> None:
    """Drop all ``xptest_*`` databases (leftovers from crashed runs)."""
    try:
        with _admin_conn() as conn:
            rows = conn.execute(
                "SELECT datname FROM pg_database WHERE datname LIKE 'xptest_%'"
            ).fetchall()
            for row in rows:
                name = row[0] if isinstance(row, tuple) else row["datname"]
                _drop_database(name)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Container helpers (for crash-recovery tests)
# ---------------------------------------------------------------------------

def _docker_exec(
    cmd: str,
    *,
    container: str = CONTAINER_NAME,
    timeout: int = 30,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    """Run a shell command inside the dev container."""
    return subprocess.run(
        ["docker", "exec", container, "bash", "-c", cmd],
        capture_output=True, text=True, timeout=timeout, check=check,
    )


def _pg_is_ready(container: str = CONTAINER_NAME) -> bool:
    """Check if PostgreSQL is accepting connections inside the container."""
    try:
        r = _docker_exec("su postgres -c 'pg_isready'", container=container, check=False)
        return r.returncode == 0
    except Exception:
        return False


def _pg_restart(container: str = CONTAINER_NAME, timeout: int = 30) -> None:
    """Graceful restart of PostgreSQL inside the container."""
    _docker_exec(
        "su postgres -c 'pg_ctl -D $PGDATA restart -w -t 10'",
        container=container, timeout=timeout,
    )


def _pg_kill(container: str = CONTAINER_NAME) -> None:
    """Simulate a crash by sending SIGKILL to the postgres master process."""
    _docker_exec(
        "kill -9 $(head -1 /var/lib/postgresql/data/postmaster.pid)",
        container=container, check=False,
    )
    # Clean up stale PID file so pg_ctl start won't refuse
    import time as _time
    _time.sleep(0.5)
    _docker_exec(
        "rm -f /var/lib/postgresql/data/postmaster.pid",
        container=container, check=False,
    )


def _pg_start(container: str = CONTAINER_NAME, timeout: int = 30) -> None:
    """Start PostgreSQL inside the container (after a kill/stop)."""
    # Remove stale lock files from prior SIGKILL
    _docker_exec(
        "rm -f /var/lib/postgresql/data/postmaster.pid "
        "/var/run/postgresql/.s.PGSQL.5432.lock "
        "/var/run/postgresql/.s.PGSQL.5432",
        container=container, check=False,
    )
    _docker_exec(
        "su postgres -c 'pg_ctl -D $PGDATA -l $PGDATA/logfile start -w -t 30'",
        container=container, timeout=timeout,
    )


# ---------------------------------------------------------------------------
# Test helpers (importable by tests as plain functions)
# ---------------------------------------------------------------------------

def row_count(
    conn: psycopg.Connection,
    table: str,
    where: str = "",
) -> int:
    """Return ``SELECT COUNT(*)`` for *table*, with an optional WHERE clause."""
    q = sql.SQL("SELECT COUNT(*) AS n FROM {}").format(sql.Identifier(table))
    if where:
        q = sql.SQL("{} WHERE {}").format(q, sql.SQL(where))
    return conn.execute(q).fetchone()["n"]  # type: ignore[index]


def insert_rows(
    conn: psycopg.Connection,
    table: str,
    rows: list[tuple[Any, ...]],
    columns: list[str] | None = None,
) -> None:
    """
    Insert multiple rows into *table*.

    *columns* is an optional list of column names (e.g. ``["group_id", "version", "content"]``).
    If omitted the INSERT has no column list (positional).

    Example::

        insert_rows(db, t, [(1, 1, "hello"), (1, 2, "world")])
        insert_rows(db, t, [(1, 1, "hello")], columns=["gid", "ver", "body"])
    """
    ident = sql.Identifier(table)
    if columns:
        col_list = sql.SQL(", ").join(sql.Identifier(c) for c in columns)
        placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in columns)
        q = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(ident, col_list, placeholders)
    else:
        # Assume positional — build placeholders from first row
        placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in rows[0])
        q = sql.SQL("INSERT INTO {} VALUES ({})").format(ident, placeholders)
    for row in rows:
        conn.execute(q, row)


def insert_versions(
    conn: psycopg.Connection,
    table: str,
    group_id: int,
    count: int,
    *,
    start: int = 1,
    content_fn: Callable[[int], str] | None = None,
    columns: tuple[str, str, str] = ("group_id", "version", "content"),
) -> None:
    """
    Insert *count* versioned rows into an xpatch table for a single group.

    Default content: ``"Version {v} content"``.

    *columns* lets you map to different column names::

        insert_versions(db, t, 1, 10, columns=("doc_id", "ver", "body"))
    """
    fn = content_fn or (lambda v: f"Version {v} content")
    col_list = list(columns)
    rows = [(group_id, v, fn(v)) for v in range(start, start + count)]
    insert_rows(conn, table, rows, columns=col_list)


# ---------------------------------------------------------------------------
# Session-scoped fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def xpatch_expect_version() -> str | None:
    """The expected pg_xpatch version (from pg_xpatch.control or env)."""
    return XPATCH_EXPECT_VERSION


@pytest.fixture(scope="session")
def pg_container() -> str:
    """Name of the Docker container running PostgreSQL."""
    return CONTAINER_NAME


def _get_worker_id(request: pytest.FixtureRequest) -> str:
    """Get xdist worker_id, falling back to 'master' when xdist is not loaded."""
    return getattr(request.config, "workerinput", {}).get("workerid", "master")


@pytest.fixture(scope="session", autouse=True)
def _check_postgres(request: pytest.FixtureRequest) -> None:
    """Fail fast if PostgreSQL is not reachable. Runs only on the first xdist worker."""
    worker_id = _get_worker_id(request)
    if worker_id not in ("master", "gw0"):
        return
    try:
        with _admin_conn() as conn:
            conn.execute("SELECT 1")
    except Exception as exc:
        pytest.exit(
            f"Cannot connect to PostgreSQL at {PG_HOST}:{PG_PORT} "
            f"as {PG_USER}: {exc}",
            returncode=1,
        )


@pytest.fixture(scope="session", autouse=True)
def _cleanup_orphaned_databases(request: pytest.FixtureRequest) -> Generator[None, None, None]:
    """
    Drop leftover ``xptest_*`` databases before and after the run.

    Only the controller / first xdist worker performs cleanup to avoid
    race conditions where one worker drops another worker's active database.
    """
    worker_id = _get_worker_id(request)
    if worker_id in ("master", "gw0"):
        _drop_orphans()
    yield
    if worker_id in ("master", "gw0"):
        _drop_orphans()


# ---------------------------------------------------------------------------
# Core fixture: isolated database per test
# ---------------------------------------------------------------------------

@pytest.fixture()
def db() -> Generator[psycopg.Connection, None, None]:
    """
    Fresh, isolated database with pg_xpatch installed.

    Behaviour:
    - Unique UUID-based name — safe under pytest-xdist parallelism.
    - ``autocommit=True`` — each statement is immediately visible.
      Use ``with conn.transaction():`` when you need explicit transactions.
    - ``row_factory=dict_row`` — rows come back as dicts, e.g. ``row["col"]``.
    - ``statement_timeout=30s`` — guards against infinite loops in the C extension.
    - Database is dropped (WITH FORCE) after the test regardless of outcome.
    """
    db_name = f"xptest_{uuid.uuid4().hex[:12]}"
    _create_database(db_name)

    conn = _connect(db_name)
    try:
        conn.execute("CREATE EXTENSION IF NOT EXISTS pg_xpatch")
        yield conn
    finally:
        conn.close()
        _drop_database(db_name)


@pytest.fixture()
def db2(db: psycopg.Connection) -> Generator[psycopg.Connection, None, None]:
    """
    Second connection to the **same** database as ``db``.

    Useful for testing:
    - Transaction isolation / visibility across connections
    - Advisory locking contention
    - Concurrent INSERT behaviour
    """
    conn = _connect(db.info.dbname)
    try:
        yield conn
    finally:
        conn.close()


@pytest.fixture()
def db_factory(db: psycopg.Connection) -> Generator[Callable[[], psycopg.Connection], None, None]:
    """
    Factory that creates additional connections to the same database as ``db``.

    Returns a callable: each call opens a new connection.  All connections are
    closed automatically after the test.

    Example::

        def test_concurrent(db, db_factory):
            conns = [db_factory() for _ in range(5)]
            # ... use conns for concurrent operations ...
    """
    opened: list[psycopg.Connection] = []

    def _make() -> psycopg.Connection:
        conn = _connect(db.info.dbname)
        opened.append(conn)
        return conn

    yield _make

    for conn in opened:
        try:
            conn.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Convenience fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def make_table(db: psycopg.Connection) -> Callable[..., str]:
    """
    Factory fixture to create xpatch tables with arbitrary schemas.

    Returns a callable.  Every table is cleaned up after the test
    (though the whole DB is dropped anyway).

    Example::

        def test_jsonb(db, make_table):
            t = make_table(
                "doc_id INT, version INT, payload JSONB",
                group_by="doc_id",
                order_by="version",
            )
            db.execute(f"INSERT INTO {t} VALUES (1, 1, '{{}}' ::jsonb)")

        def test_defaults(db, make_table):
            t = make_table()  # (group_id INT, version INT, content TEXT)
    """
    created: list[str] = []

    def _make(
        columns: str = "group_id INT, version INT, content TEXT NOT NULL",
        *,
        group_by: str = "group_id",
        order_by: str = "version",
        delta_columns: list[str] | None = None,
        keyframe_every: int | None = None,
        compress_depth: int | None = None,
        enable_zstd: bool | None = None,
    ) -> str:
        name = f"test_{uuid.uuid4().hex[:8]}"
        ident = sql.Identifier(name)

        db.execute(
            sql.SQL("CREATE TABLE {} ({}) USING xpatch").format(
                ident, sql.SQL(columns),
            )
        )

        # Build xpatch.configure() call
        config_parts = [
            sql.SQL("group_by => {}").format(sql.Literal(group_by)),
            sql.SQL("order_by => {}").format(sql.Literal(order_by)),
        ]
        if delta_columns is not None:
            dc_val = "{" + ",".join(delta_columns) + "}"
            config_parts.append(
                sql.SQL("delta_columns => {}").format(sql.Literal(dc_val))
            )
        if keyframe_every is not None:
            config_parts.append(
                sql.SQL("keyframe_every => {}").format(sql.Literal(keyframe_every))
            )
        if compress_depth is not None:
            config_parts.append(
                sql.SQL("compress_depth => {}").format(sql.Literal(compress_depth))
            )
        if enable_zstd is not None:
            config_parts.append(
                sql.SQL("enable_zstd => {}").format(sql.Literal(enable_zstd))
            )

        db.execute(
            sql.SQL("SELECT xpatch.configure({}, {})").format(
                sql.Literal(name),
                sql.SQL(", ").join(config_parts),
            )
        )

        created.append(name)
        return name

    return _make


@pytest.fixture()
def xpatch_table(make_table: Callable[..., str]) -> str:
    """
    Pre-configured xpatch table with the default schema.

    Schema::

        group_id  INT
        version   INT
        content   TEXT

    Configured with ``group_by='group_id'``, ``order_by='version'``.
    Shortcut for ``make_table()`` with no arguments.
    """
    return make_table()


# ---------------------------------------------------------------------------
# Crash-recovery helpers
# ---------------------------------------------------------------------------

class PgCtl:
    """
    Control the PostgreSQL process inside the dev container.

    Intended for ``@pytest.mark.crash_test`` tests.  Crash tests are
    automatically run last and sequentially (never in parallel) because
    killing PG affects the shared server.

    Example::

        @pytest.mark.crash_test
        def test_recovery(db, pg_ctl):
            db.execute("INSERT INTO ...")
            pg_ctl.kill()
            pg_ctl.start()
            assert pg_ctl.is_ready()
    """

    def __init__(self, container: str = CONTAINER_NAME) -> None:
        self.container = container

    def restart(self, timeout: int = 30) -> None:
        """Graceful restart of PostgreSQL."""
        _pg_restart(container=self.container, timeout=timeout)

    def kill(self) -> None:
        """SIGKILL the postmaster — simulates an unclean crash."""
        _pg_kill(container=self.container)

    def start(self, timeout: int = 30) -> None:
        """Start PostgreSQL (after a kill/stop)."""
        _pg_start(container=self.container, timeout=timeout)

    def is_ready(self) -> bool:
        """Return True if PostgreSQL is accepting connections."""
        return _pg_is_ready(container=self.container)

    def exec(self, cmd: str, *, timeout: int = 30, check: bool = True) -> subprocess.CompletedProcess[str]:
        """Run an arbitrary shell command inside the container."""
        return _docker_exec(cmd, container=self.container, timeout=timeout, check=check)


@pytest.fixture()
def pg_ctl() -> PgCtl:
    """PgCtl instance for controlling the PostgreSQL process in the dev container."""
    return PgCtl()


# ---------------------------------------------------------------------------
# Pytest hooks
# ---------------------------------------------------------------------------

def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """
    Reorder tests so that ``@pytest.mark.crash_test`` tests always run **last**.

    Under pytest-xdist these tests must run sequentially because they kill
    the shared PostgreSQL process.  Add ``-m crash_test -p no:xdist`` to your
    command when running crash tests, or run the full suite and they will
    execute after all other tests complete.

    Also forces ``@pytest.mark.stress`` tests to the end (but before crash).
    """
    normal: list[pytest.Item] = []
    stress: list[pytest.Item] = []
    crash: list[pytest.Item] = []

    for item in items:
        if item.get_closest_marker("crash_test"):
            crash.append(item)
        elif item.get_closest_marker("stress"):
            stress.append(item)
        else:
            normal.append(item)

    items[:] = normal + stress + crash
