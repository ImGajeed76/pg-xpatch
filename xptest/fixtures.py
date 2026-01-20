"""
Fixture system for xptest.

Fixtures provide reusable setup/teardown logic at different scopes.
They are injected into test functions based on parameter names.

Thread Safety for 32+ Parallel Tests:
- Each test gets its own FixtureManager instance with isolated state
- The 'db' fixture creates a unique database per test (UUID-based naming)
- Built-in fixtures are NOT in the global registry - they're created per-manager
- Global fixture registry is only for user-defined fixtures
- Session/module caches are protected by locks
"""

import uuid
import inspect
import threading
from enum import Enum
from typing import (
    Callable, Generator, Dict, Any, Optional, List, 
    TypeVar, Set, Tuple
)
from contextlib import contextmanager
from dataclasses import dataclass, field

from .database import (
    DatabaseConnection, 
    create_test_database, 
    drop_test_database,
    check_container_running,
)


T = TypeVar('T')


class Scope(Enum):
    """Fixture scope determines when fixtures are created and destroyed."""
    FUNCTION = "function"    # Fresh fixture for each test function
    MODULE = "module"        # Shared within a test file/module
    SESSION = "session"      # Shared across entire test run
    DATABASE = "database"    # Same as FUNCTION (each test gets own DB)


@dataclass
class FixtureDefinition:
    """Definition of a fixture."""
    name: str
    func: Callable[..., Any]
    scope: Scope
    dependencies: List[str] = field(default_factory=list)


@dataclass
class FixtureValue:
    """Cached fixture value with cleanup function."""
    value: Any
    cleanup: Optional[Callable[[], None]] = None
    scope: Scope = Scope.FUNCTION


# Global registry for USER-DEFINED fixtures only (not built-ins)
# Protected by lock for thread-safe registration
_user_fixture_registry: Dict[str, FixtureDefinition] = {}
_user_fixture_registry_lock = threading.Lock()

# Cache for session-scoped fixtures (shared across all tests)
_session_cache: Dict[str, FixtureValue] = {}
_session_cache_lock = threading.Lock()

# Cache for module-scoped fixtures (keyed by module name)
_module_cache: Dict[str, Dict[str, FixtureValue]] = {}
_module_cache_lock = threading.Lock()

# Flag to track if built-ins were initialized (for first FixtureManager only)
_builtins_initialized = False
_builtins_init_lock = threading.Lock()


def pg_fixture(
    scope: Scope = Scope.FUNCTION,
    name: Optional[str] = None,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Decorator to mark a function as a fixture.
    
    Args:
        scope: Fixture scope (FUNCTION, MODULE, SESSION, DATABASE)
        name: Optional custom name (defaults to function name)
    
    Example:
        @pg_fixture(scope=Scope.FUNCTION)
        def my_fixture():
            yield some_value
            # cleanup
        
        @pg_fixture(scope=Scope.SESSION)
        def shared_resource():
            return expensive_setup()
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        fixture_name = name or func.__name__
        
        # Get fixture dependencies from function parameters
        sig = inspect.signature(func)
        dependencies = [
            param.name 
            for param in sig.parameters.values()
            if param.kind in (
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                inspect.Parameter.KEYWORD_ONLY,
            )
        ]
        
        fixture_def = FixtureDefinition(
            name=fixture_name,
            func=func,
            scope=scope,
            dependencies=dependencies,
        )
        
        # Thread-safe registration
        with _user_fixture_registry_lock:
            _user_fixture_registry[fixture_name] = fixture_def
        
        # Mark the function as a fixture for introspection
        func._pg_fixture_def = fixture_def  # type: ignore
        
        return func
    
    return decorator


def get_fixture_registry() -> Dict[str, FixtureDefinition]:
    """Get the user-defined fixture registry (for introspection)."""
    with _user_fixture_registry_lock:
        return dict(_user_fixture_registry)


def clear_fixture_registry() -> None:
    """Clear all fixture registrations and caches."""
    global _builtins_initialized
    with _user_fixture_registry_lock:
        _user_fixture_registry.clear()
    with _session_cache_lock:
        _session_cache.clear()
    with _module_cache_lock:
        _module_cache.clear()
    with _builtins_init_lock:
        _builtins_initialized = False


class FixtureManager:
    """
    Manages fixture resolution and lifecycle for a SINGLE TEST.
    
    Thread Safety:
    - Each test creates its own FixtureManager instance
    - Built-in fixtures (db, xpatch_table) are created per-instance, not shared
    - This ensures 32+ parallel tests can run without interference
    - Each test gets its own database with a unique UUID-based name
    
    Handles:
    - Dependency resolution
    - Scope-based caching (function, module, session)
    - Cleanup on test completion
    """
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 5432,
        user: str = "postgres",
        password: Optional[str] = None,
        container: str = "pg-xpatch-dev",
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.container = container
        
        # Per-instance fixture registry (includes built-ins + user fixtures)
        # This is the KEY change - each manager has its own registry copy
        self._local_registry: Dict[str, FixtureDefinition] = {}
        
        # Track active fixtures for cleanup (per-instance, not shared)
        self._active_fixtures: Dict[str, FixtureValue] = {}
        self._cleanup_stack: List[Tuple[str, Callable[[], None]]] = []
        
        # Build local registry with built-in fixtures
        self._init_local_registry()
    
    def _init_local_registry(self) -> None:
        """
        Initialize local registry with built-in fixtures.
        
        Each FixtureManager gets its own copy of built-in fixture definitions
        that capture THIS manager's connection settings.
        """
        # Copy user-defined fixtures
        with _user_fixture_registry_lock:
            self._local_registry = dict(_user_fixture_registry)
        
        # Add built-in fixtures that capture THIS manager's settings
        self._register_db_fixture()
        self._register_xpatch_table_fixture()
        self._register_container_fixture()
    
    def _register_db_fixture(self) -> None:
        """
        Register the 'db' fixture for THIS manager instance.
        
        Creates a fresh database per test with:
        - Unique UUID-based name (xptest_XXXXXXXX)
        - Automatic cleanup after test
        - pg_xpatch extension pre-installed
        """
        # Capture connection settings for this manager
        host = self.host
        port = self.port
        user = self.user
        password = self.password
        
        def db_fixture() -> Generator[DatabaseConnection, None, None]:
            """
            Provides a fresh, isolated database connection for each test.
            
            The database is:
            - Created with a unique name (UUID-based)
            - Has pg_xpatch extension installed
            - Dropped after test completion
            """
            # Generate unique database name for this test
            db_name = f"xptest_{uuid.uuid4().hex[:12]}"
            
            conn = create_test_database(
                db_name,
                host=host,
                port=port,
                user=user,
                password=password,
            )
            try:
                yield conn
            finally:
                # Always cleanup: close connection and drop database
                try:
                    conn.close()
                except Exception:
                    pass
                try:
                    drop_test_database(
                        db_name,
                        host=host,
                        port=port,
                        user=user,
                        password=password,
                    )
                except Exception:
                    pass  # Best effort cleanup
        
        self._local_registry["db"] = FixtureDefinition(
            name="db",
            func=db_fixture,
            scope=Scope.FUNCTION,
            dependencies=[],
        )
        
        # Also register db2 fixture for multi-connection tests
        self._register_db2_fixture()
    
    def _register_db2_fixture(self) -> None:
        """
        Register the 'db2' fixture for tests requiring multiple connections.
        
        This creates a second connection to the SAME database as 'db',
        useful for testing transaction isolation and concurrency.
        """
        host = self.host
        port = self.port
        user = self.user
        password = self.password
        
        def db2_fixture(db: DatabaseConnection) -> Generator[DatabaseConnection, None, None]:
            """
            Provides a second connection to the same database as 'db'.
            
            Useful for:
            - Testing transaction isolation (visibility across connections)
            - Testing concurrent operations
            - Testing locking behavior
            """
            # Create a new connection to the SAME database
            conn2 = DatabaseConnection(
                host=host,
                port=port,
                user=user,
                password=password,
                db_name=db.db_name,  # Same database as db
            )
            conn2.connect()
            try:
                yield conn2
            finally:
                try:
                    conn2.close()
                except Exception:
                    pass
        
        self._local_registry["db2"] = FixtureDefinition(
            name="db2",
            func=db2_fixture,
            scope=Scope.FUNCTION,
            dependencies=["db"],  # Depends on db to get the database name
        )
    
    def _register_xpatch_table_fixture(self) -> None:
        """Register the 'xpatch_table' fixture."""
        def xpatch_table_fixture(db: DatabaseConnection) -> Generator[str, None, None]:
            """
            Creates a pre-configured xpatch table for testing.
            
            Returns the table name. Table is dropped after test.
            """
            table_name = f"test_{uuid.uuid4().hex[:8]}"
            db.execute(f"""
                CREATE TABLE {table_name} (
                    group_id INT,
                    version INT,
                    content TEXT
                ) USING xpatch;
                SELECT xpatch.configure('{table_name}', 
                    group_by => 'group_id', 
                    order_by => 'version');
            """)
            try:
                yield table_name
            finally:
                try:
                    db.execute(f"DROP TABLE IF EXISTS {table_name}")
                except Exception:
                    pass  # Ignore cleanup errors
        
        self._local_registry["xpatch_table"] = FixtureDefinition(
            name="xpatch_table",
            func=xpatch_table_fixture,
            scope=Scope.FUNCTION,
            dependencies=["db"],
        )
    
    def _register_container_fixture(self) -> None:
        """Register the 'container_name' fixture."""
        container = self.container
        
        def container_name_fixture() -> str:
            """Returns the docker container name."""
            return container
        
        self._local_registry["container_name"] = FixtureDefinition(
            name="container_name",
            func=container_name_fixture,
            scope=Scope.SESSION,
            dependencies=[],
        )
    
    def resolve_fixtures(
        self, 
        fixture_names: List[str],
        module_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Resolve fixtures and their dependencies.
        
        Args:
            fixture_names: List of fixture names to resolve
            module_name: Module name for module-scoped fixtures
        
        Returns:
            Dict mapping fixture names to their values
        """
        resolved: Dict[str, Any] = {}
        
        for name in fixture_names:
            if name not in resolved:
                self._resolve_fixture(name, resolved, module_name, set())
        
        return resolved
    
    def _resolve_fixture(
        self,
        name: str,
        resolved: Dict[str, Any],
        module_name: Optional[str],
        resolving: Set[str],
    ) -> Any:
        """
        Resolve a single fixture, handling dependencies recursively.
        
        Detects circular dependencies.
        """
        # Check for circular dependency
        if name in resolving:
            cycle = " -> ".join(list(resolving) + [name])
            raise ValueError(f"Circular fixture dependency: {cycle}")
        
        # Already resolved in this context
        if name in resolved:
            return resolved[name]
        
        # Get fixture definition from LOCAL registry (not global)
        if name not in self._local_registry:
            raise ValueError(f"Unknown fixture: {name}")
        
        fixture_def = self._local_registry[name]
        
        # Check cache based on scope
        cached = self._get_cached_fixture(name, fixture_def.scope, module_name)
        if cached is not None:
            resolved[name] = cached.value
            return cached.value
        
        # Mark as being resolved (for circular dependency detection)
        resolving.add(name)
        
        try:
            # Resolve dependencies first
            dep_values = {}
            for dep_name in fixture_def.dependencies:
                dep_values[dep_name] = self._resolve_fixture(
                    dep_name, resolved, module_name, resolving
                )
            
            # Create fixture value
            fixture_value = self._create_fixture(fixture_def, dep_values)
            
            # Cache based on scope
            self._cache_fixture(name, fixture_value, fixture_def.scope, module_name)
            
            resolved[name] = fixture_value.value
            return fixture_value.value
            
        finally:
            resolving.discard(name)
    
    def _get_cached_fixture(
        self,
        name: str,
        scope: Scope,
        module_name: Optional[str],
    ) -> Optional[FixtureValue]:
        """Get cached fixture value if available."""
        if scope == Scope.SESSION:
            with _session_cache_lock:
                return _session_cache.get(name)
        
        elif scope == Scope.MODULE and module_name:
            with _module_cache_lock:
                module_fixtures = _module_cache.get(module_name, {})
                return module_fixtures.get(name)
        
        # FUNCTION and DATABASE scopes are NOT cached globally
        # They're only tracked in this manager's _active_fixtures for cleanup
        return self._active_fixtures.get(name)
    
    def _cache_fixture(
        self,
        name: str,
        fixture_value: FixtureValue,
        scope: Scope,
        module_name: Optional[str],
    ) -> None:
        """Cache fixture value based on scope."""
        fixture_value.scope = scope
        
        if scope == Scope.SESSION:
            with _session_cache_lock:
                # Only cache if not already cached (first one wins)
                if name not in _session_cache:
                    _session_cache[name] = fixture_value
        
        elif scope == Scope.MODULE and module_name:
            with _module_cache_lock:
                if module_name not in _module_cache:
                    _module_cache[module_name] = {}
                # Only cache if not already cached
                if name not in _module_cache[module_name]:
                    _module_cache[module_name][name] = fixture_value
        
        else:
            # FUNCTION/DATABASE scope - track for cleanup in THIS manager only
            self._active_fixtures[name] = fixture_value
            if fixture_value.cleanup:
                self._cleanup_stack.append((name, fixture_value.cleanup))
    
    def _create_fixture(
        self,
        fixture_def: FixtureDefinition,
        dep_values: Dict[str, Any],
    ) -> FixtureValue:
        """Create fixture value, handling generators for cleanup."""
        result = fixture_def.func(**dep_values)
        
        # Check if it's a generator (for setup/teardown pattern)
        if inspect.isgenerator(result):
            # Get the yielded value
            try:
                value = next(result)
            except StopIteration as e:
                # Generator returned without yielding
                value = e.value
                result = None
            
            # Capture generator in closure for cleanup
            gen = result
            
            def cleanup():
                if gen is not None:
                    try:
                        next(gen)
                    except StopIteration:
                        pass
                    except Exception:
                        pass  # Swallow cleanup errors
            
            return FixtureValue(value=value, cleanup=cleanup if gen else None)
        
        # Regular function return
        return FixtureValue(value=result, cleanup=None)
    
    def cleanup_function_fixtures(self) -> None:
        """
        Clean up function-scoped fixtures (called after each test).
        
        Runs cleanups in reverse order (LIFO) to handle dependencies correctly.
        """
        errors = []
        
        # Run cleanups in reverse order
        while self._cleanup_stack:
            name, cleanup_func = self._cleanup_stack.pop()
            try:
                cleanup_func()
            except Exception as e:
                # Collect errors but continue cleanup
                errors.append(f"Fixture '{name}': {e}")
            
            # Remove from active fixtures
            self._active_fixtures.pop(name, None)
        
        # Clear any remaining active fixtures
        self._active_fixtures.clear()
        
        # Log errors if any (but don't fail)
        if errors:
            import sys
            print(f"Warning: {len(errors)} fixture cleanup error(s):", file=sys.stderr)
            for err in errors:
                print(f"  - {err}", file=sys.stderr)
    
    def cleanup_module_fixtures(self, module_name: str) -> None:
        """Clean up module-scoped fixtures (called after all tests in module)."""
        with _module_cache_lock:
            module_fixtures = _module_cache.pop(module_name, {})
        
        for name, fixture_value in reversed(list(module_fixtures.items())):
            if fixture_value.cleanup:
                try:
                    fixture_value.cleanup()
                except Exception as e:
                    import sys
                    print(f"Warning: Module fixture cleanup error for '{name}': {e}", 
                          file=sys.stderr)
    
    @classmethod
    def cleanup_session_fixtures(cls) -> None:
        """Clean up session-scoped fixtures (called at end of test run)."""
        with _session_cache_lock:
            fixtures = list(_session_cache.items())
            _session_cache.clear()
        
        for name, fixture_value in reversed(fixtures):
            if fixture_value.cleanup:
                try:
                    fixture_value.cleanup()
                except Exception as e:
                    import sys
                    print(f"Warning: Session fixture cleanup error for '{name}': {e}",
                          file=sys.stderr)


def resolve_fixtures_for_test(
    func: Callable[..., Any],
    manager: FixtureManager,
    module_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Resolve all fixtures needed by a test function.
    
    Args:
        func: Test function to resolve fixtures for
        manager: FixtureManager instance (should be unique per test)
        module_name: Module name for module-scoped fixtures
    
    Returns:
        Dict mapping parameter names to fixture values
    """
    # Get required fixture names from function signature
    sig = inspect.signature(func)
    fixture_names = [
        param.name 
        for param in sig.parameters.values()
        if param.kind in (
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.KEYWORD_ONLY,
        )
    ]
    
    return manager.resolve_fixtures(fixture_names, module_name)
