"""
Database connection management for xptest.

Provides DatabaseConnection class for interacting with PostgreSQL,
and utilities for creating/dropping test databases.
"""

import subprocess
from typing import Optional, List, Dict, Any, Union
from contextlib import contextmanager
import threading

try:
    import psycopg
    from psycopg.rows import dict_row
    PSYCOPG_AVAILABLE = True
except ImportError:
    PSYCOPG_AVAILABLE = False


class DatabaseError(Exception):
    """Exception raised for database-related errors."""
    pass


class DatabaseConnection:
    """
    Wrapper around psycopg connection with helper methods.
    
    Provides a simplified interface for executing SQL and fetching results.
    Each test gets its own DatabaseConnection with a dedicated database.
    """
    
    def __init__(
        self, 
        db_name: str, 
        host: str = "localhost",
        port: int = 5432,
        user: str = "postgres",
        password: Optional[str] = None,
    ):
        self.db_name = db_name
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self._conn: Optional[Any] = None  # psycopg.Connection
        self._lock = threading.Lock()
    
    def connect(self) -> None:
        """Establish connection to the database."""
        if not PSYCOPG_AVAILABLE:
            raise DatabaseError(
                "psycopg is not installed. Install with: pip install 'psycopg[binary]'"
            )
        
        connect_kwargs: Dict[str, Any] = {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "dbname": self.db_name,
            "row_factory": dict_row,
            "autocommit": False,
        }
        if self.password:
            connect_kwargs["password"] = self.password
        
        self._conn = psycopg.connect(**connect_kwargs)
    
    def close(self) -> None:
        """Close the database connection."""
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None
    
    def execute(self, sql: str, params: Optional[tuple] = None) -> None:
        """
        Execute SQL statement(s) without returning results.
        
        Handles multi-statement SQL by splitting on semicolons when needed.
        Auto-commits after successful execution UNLESS in explicit transaction
        (i.e., after BEGIN or inside a savepoint).
        """
        if not self._conn:
            raise DatabaseError("Not connected to database")
        
        with self._lock:
            with self._conn.cursor() as cur:
                if params:
                    cur.execute(sql, params)
                else:
                    cur.execute(sql)
            # Only auto-commit if not in an explicit transaction
            if not self._in_transaction():
                self._conn.commit()
    
    def _in_transaction(self) -> bool:
        """Check if we're inside an explicit transaction (BEGIN/SAVEPOINT)."""
        if not self._conn:
            return False
        # psycopg3: check transaction status
        # IDLE = no transaction, INTRANS = in transaction, INERROR = in failed transaction
        try:
            status = self._conn.info.transaction_status
            # 0 = IDLE, 1 = ACTIVE, 2 = INTRANS, 3 = INERROR, 4 = UNKNOWN
            return status in (1, 2, 3)  # ACTIVE, INTRANS, or INERROR
        except Exception:
            return False
    
    def execute_many(self, sql: str, params_seq: List[tuple]) -> None:
        """Execute SQL with multiple parameter sets."""
        if not self._conn:
            raise DatabaseError("Not connected to database")
        
        with self._lock:
            with self._conn.cursor() as cur:
                cur.executemany(sql, params_seq)
            self._conn.commit()
    
    def fetchone(
        self, 
        sql: str, 
        params: Optional[tuple] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Execute query and return single row as dict.
        
        Returns None if no rows found.
        """
        if not self._conn:
            raise DatabaseError("Not connected to database")
        
        with self._lock:
            with self._conn.cursor() as cur:
                if params:
                    cur.execute(sql, params)
                else:
                    cur.execute(sql)
                result = cur.fetchone()
            if not self._in_transaction():
                self._conn.commit()
        return result
    
    def fetchall(
        self, 
        sql: str, 
        params: Optional[tuple] = None
    ) -> List[Dict[str, Any]]:
        """
        Execute query and return all rows as list of dicts.
        
        Returns empty list if no rows found.
        """
        if not self._conn:
            raise DatabaseError("Not connected to database")
        
        with self._lock:
            with self._conn.cursor() as cur:
                if params:
                    cur.execute(sql, params)
                else:
                    cur.execute(sql)
                result = cur.fetchall()
            if not self._in_transaction():
                self._conn.commit()
        return result
    
    def fetchval(
        self, 
        sql: str, 
        params: Optional[tuple] = None
    ) -> Any:
        """
        Execute query and return single value from first column of first row.
        
        Returns None if no rows found.
        """
        row = self.fetchone(sql, params)
        if row:
            # Get first value from dict
            return next(iter(row.values()))
        return None
    
    @contextmanager
    def transaction(self):
        """
        Context manager for explicit transaction control.
        
        Example:
            with db.transaction():
                db.execute("INSERT INTO ...")
                db.execute("UPDATE ...")
                # Auto-commits on exit, rollback on exception
        """
        if not self._conn:
            raise DatabaseError("Not connected to database")
        
        try:
            yield self
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise
    
    def rollback(self) -> None:
        """Rollback current transaction."""
        if self._conn:
            self._conn.rollback()
    
    def __enter__(self) -> "DatabaseConnection":
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


class AdminConnection:
    """
    Connection to postgres database for administrative operations.
    
    Used for creating and dropping test databases.
    """
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 5432,
        user: str = "postgres",
        password: Optional[str] = None,
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self._conn: Optional[Any] = None
    
    def connect(self) -> None:
        """Connect to postgres database with autocommit for DDL."""
        if not PSYCOPG_AVAILABLE:
            raise DatabaseError(
                "psycopg is not installed. Install with: pip install 'psycopg[binary]'"
            )
        
        connect_kwargs: Dict[str, Any] = {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "dbname": "postgres",
            "autocommit": True,
        }
        if self.password:
            connect_kwargs["password"] = self.password
        
        self._conn = psycopg.connect(**connect_kwargs)
    
    def close(self) -> None:
        """Close admin connection."""
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None
    
    def create_database(self, db_name: str) -> None:
        """Create a new database."""
        if not self._conn:
            raise DatabaseError("Not connected")
        
        with self._conn.cursor() as cur:
            # Drop if exists first
            cur.execute(f"DROP DATABASE IF EXISTS {db_name}")
            cur.execute(f"CREATE DATABASE {db_name}")
    
    def drop_database(self, db_name: str) -> None:
        """Drop a database, terminating active connections first."""
        if not self._conn:
            raise DatabaseError("Not connected")
        
        with self._conn.cursor() as cur:
            # Terminate active connections
            cur.execute("""
                SELECT pg_terminate_backend(pid) 
                FROM pg_stat_activity 
                WHERE datname = %s AND pid <> pg_backend_pid()
            """, (db_name,))
            
            # Drop the database
            cur.execute(f"DROP DATABASE IF EXISTS {db_name}")
    
    def database_exists(self, db_name: str) -> bool:
        """Check if a database exists."""
        if not self._conn:
            raise DatabaseError("Not connected")
        
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s",
                (db_name,)
            )
            return cur.fetchone() is not None
    
    def __enter__(self) -> "AdminConnection":
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


def create_test_database(
    db_name: str,
    host: str = "localhost",
    port: int = 5432,
    user: str = "postgres",
    password: Optional[str] = None,
    create_extension: bool = True,
) -> DatabaseConnection:
    """
    Create a fresh test database with pg_xpatch extension.
    
    Args:
        db_name: Name for the test database
        host: PostgreSQL host
        port: PostgreSQL port
        user: PostgreSQL user
        password: PostgreSQL password (optional)
        create_extension: Whether to create pg_xpatch extension
    
    Returns:
        Connected DatabaseConnection to the new database
    """
    # Create the database using admin connection
    with AdminConnection(host, port, user, password) as admin:
        admin.create_database(db_name)
    
    # Connect to new database
    conn = DatabaseConnection(db_name, host, port, user, password)
    conn.connect()
    
    # Create extension
    if create_extension:
        try:
            conn.execute("CREATE EXTENSION IF NOT EXISTS pg_xpatch")
        except Exception as e:
            conn.close()
            # Clean up the database if extension creation fails
            with AdminConnection(host, port, user, password) as admin:
                admin.drop_database(db_name)
            raise DatabaseError(f"Failed to create pg_xpatch extension: {e}")
    
    return conn


def drop_test_database(
    db_name: str,
    host: str = "localhost",
    port: int = 5432,
    user: str = "postgres",
    password: Optional[str] = None,
) -> None:
    """
    Drop a test database.
    
    Terminates any active connections before dropping.
    """
    with AdminConnection(host, port, user, password) as admin:
        admin.drop_database(db_name)


def check_container_running(container_name: str) -> bool:
    """
    Check if a Docker container is running.
    
    Args:
        container_name: Name of the Docker container
    
    Returns:
        True if container is running, False otherwise
    """
    try:
        result = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Running}}", container_name],
            capture_output=True,
            text=True,
            timeout=10,
        )
        return result.returncode == 0 and "true" in result.stdout.lower()
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return False


def check_postgres_connection(
    host: str = "localhost",
    port: int = 5432,
    user: str = "postgres",
    password: Optional[str] = None,
) -> bool:
    """
    Check if PostgreSQL is reachable.
    
    Returns:
        True if connection succeeds, False otherwise
    """
    try:
        with AdminConnection(host, port, user, password) as admin:
            return True
    except Exception:
        return False


def get_postgres_version(
    host: str = "localhost",
    port: int = 5432,
    user: str = "postgres",
    password: Optional[str] = None,
) -> Optional[str]:
    """
    Get PostgreSQL server version.
    
    Returns:
        Version string or None if connection fails
    """
    try:
        conn = DatabaseConnection("postgres", host, port, user, password)
        conn.connect()
        try:
            result = conn.fetchval("SHOW server_version")
            return str(result) if result else None
        finally:
            conn.close()
    except Exception:
        return None


def get_extension_version(
    db_name: str,
    host: str = "localhost",
    port: int = 5432,
    user: str = "postgres",
    password: Optional[str] = None,
) -> Optional[str]:
    """
    Get pg_xpatch extension version from a database.
    
    Returns:
        Version string or None if extension not installed
    """
    try:
        conn = DatabaseConnection(db_name, host, port, user, password)
        conn.connect()
        try:
            result = conn.fetchval("SELECT xpatch.version()")
            return str(result) if result else None
        finally:
            conn.close()
    except Exception:
        return None
