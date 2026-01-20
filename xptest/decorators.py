"""
Test decorator for pg-xpatch tests.

The @pg_test() decorator marks functions as tests and configures their behavior.
"""

from typing import Callable, Optional, List, Dict, Tuple, Any
import functools
import inspect

from .models import TestConfig


# Global registry of all discovered tests
# Maps test_id -> (function, config)
_test_registry: Dict[str, Tuple[Callable[..., Any], TestConfig]] = {}


def get_test_registry() -> Dict[str, Tuple[Callable[..., Any], TestConfig]]:
    """Get the global test registry."""
    return _test_registry


def clear_test_registry() -> None:
    """Clear the test registry (useful for testing the test runner itself)."""
    _test_registry.clear()


def pg_test(
    parallel: bool = True,
    benchmark: bool = False,
    slow: bool = False,
    crash_test: bool = False,
    timeout: int = 60,
    retries: int = 0,
    tags: Optional[List[str]] = None,
    depends_on: Optional[List[str]] = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    Decorator to mark a function as a pg-xpatch test.
    
    Args:
        parallel: Whether this test can run concurrently with other parallel tests.
                  Default True. Set to False for tests that need exclusive access.
        benchmark: Whether to collect timing statistics for this test.
                   Benchmark tests are run multiple times and stats are computed.
        slow: Mark this test as slow. Can be skipped with --fast flag.
        crash_test: Mark as a destructive test that may corrupt state.
                    These tests always run last, after all other tests.
        timeout: Maximum seconds before the test is killed. Default 60.
        retries: Number of times to retry on failure. Default 0 (no retries).
        tags: List of tags for filtering tests (e.g., ["unit", "config"]).
        depends_on: List of test IDs that must pass before this test runs.
    
    Example:
        @pg_test(tags=["unit", "stats"])
        def test_stats_empty_table(db):
            result = db.fetchone("SELECT * FROM xpatch.stats('test_table')")
            assert result['total_rows'] == 0
        
        @pg_test(benchmark=True, tags=["bench"])
        def test_insert_performance(db):
            # This test will be run multiple times for statistics
            for i in range(1000):
                db.execute(f"INSERT INTO t VALUES ({i}, 1, 'data')")
        
        @pg_test(parallel=False, crash_test=True)
        def test_recovery_after_crash(db):
            # This test runs alone, at the end
            ...
    """
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        # Create test configuration
        config = TestConfig(
            parallel=parallel,
            benchmark=benchmark,
            slow=slow,
            crash_test=crash_test,
            timeout=timeout,
            retries=retries,
            tags=tags or [],
            depends_on=depends_on or [],
        )
        
        # Generate unique test ID from module and function name
        module = func.__module__
        qualname = func.__qualname__
        test_id = f"{module}.{qualname}"
        
        # Register the test
        _test_registry[test_id] = (func, config)
        
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return func(*args, **kwargs)
        
        # Attach metadata to the wrapper for introspection
        wrapper._pg_test_config = config  # type: ignore
        wrapper._pg_test_id = test_id  # type: ignore
        wrapper._pg_test_func = func  # type: ignore
        
        return wrapper
    
    return decorator


def get_test_fixtures(func: Callable[..., Any]) -> List[str]:
    """
    Get the list of fixture names required by a test function.
    
    Inspects the function signature to find parameter names,
    which correspond to fixture names.
    """
    sig = inspect.signature(func)
    return [
        param.name 
        for param in sig.parameters.values()
        if param.kind in (
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.KEYWORD_ONLY,
        )
    ]
