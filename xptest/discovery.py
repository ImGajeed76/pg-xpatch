"""
Test discovery for xptest.

Discovers tests by importing Python modules and collecting
functions decorated with @pg_test.
"""

import sys
import importlib.util
from pathlib import Path
from typing import List, Tuple, Callable, Optional, Set, Dict, Any
import fnmatch
import re

from .models import TestConfig
from .decorators import get_test_registry, clear_test_registry


# Type alias for discovered test
DiscoveredTest = Tuple[str, Callable[..., Any], TestConfig]


def discover_tests(
    test_paths: List[Path],
    pattern: str = "test_*.py",
    bench_pattern: str = "bench_*.py",
) -> List[DiscoveredTest]:
    """
    Discover all tests by importing modules and collecting
    functions decorated with @pg_test.
    
    Args:
        test_paths: List of paths to search (files or directories)
        pattern: Glob pattern for test files
        bench_pattern: Glob pattern for benchmark files
    
    Returns:
        List of tuples: (test_id, function, config)
    """
    # Clear registry before discovery to avoid duplicates
    clear_test_registry()
    
    # Collect all Python files to import
    files_to_import: Set[Path] = set()
    
    for path in test_paths:
        path = path.resolve()
        
        if path.is_file():
            if path.suffix == ".py":
                files_to_import.add(path)
        elif path.is_dir():
            # Find test files
            for py_file in path.rglob("*.py"):
                # Skip __pycache__ and hidden directories
                if any(part.startswith('.') or part == '__pycache__' 
                       for part in py_file.parts):
                    continue
                
                # Match against patterns
                if (fnmatch.fnmatch(py_file.name, pattern) or 
                    fnmatch.fnmatch(py_file.name, bench_pattern)):
                    files_to_import.add(py_file)
    
    # Import all files (this triggers @pg_test decorators to register tests)
    for file_path in sorted(files_to_import):
        _import_module_from_path(file_path)
    
    # Collect from registry
    registry = get_test_registry()
    discovered: List[DiscoveredTest] = []
    
    for test_id, (func, config) in registry.items():
        discovered.append((test_id, func, config))
    
    return discovered


def _import_module_from_path(path: Path) -> None:
    """
    Import a Python file as a module.
    
    The module is imported with a generated name based on its path,
    to avoid conflicts with existing modules.
    """
    # Generate unique module name based on path
    # Use path relative to cwd to keep it reasonable
    try:
        rel_path = path.relative_to(Path.cwd())
    except ValueError:
        rel_path = path
    
    # Convert path to module name: tests/unit/test_foo.py -> tests.unit.test_foo
    parts = list(rel_path.parts)
    if parts and parts[-1].endswith(".py"):
        parts[-1] = parts[-1][:-3]  # Remove .py
    module_name = ".".join(parts)
    
    # Avoid re-importing if already loaded
    if module_name in sys.modules:
        return
    
    try:
        spec = importlib.util.spec_from_file_location(module_name, path)
        if spec and spec.loader:
            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module
            spec.loader.exec_module(module)
    except Exception as e:
        # Print warning but continue with other files
        print(f"Warning: Failed to import {path}: {e}")


def filter_tests(
    tests: List[DiscoveredTest],
    tags: Optional[List[str]] = None,
    name_pattern: Optional[str] = None,
    exclude_slow: bool = False,
    only_benchmarks: bool = False,
    exclude_crash_tests: bool = False,
) -> List[DiscoveredTest]:
    """
    Filter tests based on various criteria.
    
    Args:
        tests: List of discovered tests to filter
        tags: Only include tests with at least one of these tags
        name_pattern: Only include tests with name matching pattern (substring or regex)
        exclude_slow: Exclude tests marked as slow=True
        only_benchmarks: Only include tests marked as benchmark=True
        exclude_crash_tests: Exclude tests marked as crash_test=True
    
    Returns:
        Filtered list of tests
    """
    filtered: List[DiscoveredTest] = []
    
    # Compile regex if pattern looks like one
    name_regex = None
    if name_pattern:
        try:
            # If it contains regex special chars, treat as regex
            if any(c in name_pattern for c in r'[](){}*+?|^$\.'):
                name_regex = re.compile(name_pattern, re.IGNORECASE)
        except re.error:
            pass  # Treat as simple substring
    
    for test_id, func, config in tests:
        # Tag filter (OR logic - match any tag)
        if tags:
            if not any(tag in config.tags for tag in tags):
                continue
        
        # Name pattern filter
        if name_pattern:
            if name_regex:
                if not name_regex.search(test_id):
                    continue
            else:
                if name_pattern.lower() not in test_id.lower():
                    continue
        
        # Slow filter
        if exclude_slow and config.slow:
            continue
        
        # Benchmark filter
        if only_benchmarks and not config.benchmark:
            continue
        
        # Crash test filter
        if exclude_crash_tests and config.crash_test:
            continue
        
        filtered.append((test_id, func, config))
    
    return filtered


def sort_tests(tests: List[DiscoveredTest]) -> List[DiscoveredTest]:
    """
    Sort tests for optimal execution order.
    
    Order:
    1. Parallel tests (can run concurrently)
    2. Sequential tests (run one at a time)
    3. Crash tests (run last, may corrupt state)
    
    Within each group, maintain original order.
    """
    parallel = []
    sequential = []
    crash = []
    
    for test in tests:
        test_id, func, config = test
        if config.crash_test:
            crash.append(test)
        elif config.parallel:
            parallel.append(test)
        else:
            sequential.append(test)
    
    return parallel + sequential + crash


def resolve_dependencies(
    tests: List[DiscoveredTest],
) -> List[DiscoveredTest]:
    """
    Resolve test dependencies and return tests in valid execution order.
    
    Tests with depends_on are moved after their dependencies.
    Raises ValueError on circular dependencies.
    
    Note: This is a simple topological sort. Dependencies are respected
    even across parallel/sequential boundaries.
    """
    # Build dependency graph
    test_map: Dict[str, DiscoveredTest] = {t[0]: t for t in tests}
    
    # Track resolved and in-progress for cycle detection
    resolved: List[str] = []
    resolving: Set[str] = set()
    
    def visit(test_id: str) -> None:
        if test_id in resolved:
            return
        if test_id in resolving:
            raise ValueError(f"Circular test dependency involving: {test_id}")
        
        if test_id not in test_map:
            # Dependency not in test set - skip
            return
        
        resolving.add(test_id)
        
        _, _, config = test_map[test_id]
        for dep_id in config.depends_on:
            visit(dep_id)
        
        resolving.remove(test_id)
        resolved.append(test_id)
    
    # Visit all tests
    for test_id in test_map:
        visit(test_id)
    
    # Return tests in resolved order
    return [test_map[test_id] for test_id in resolved]


def group_tests_by_module(
    tests: List[DiscoveredTest],
) -> Dict[str, List[DiscoveredTest]]:
    """
    Group tests by their module.
    
    Useful for module-scoped fixtures.
    """
    groups: Dict[str, List[DiscoveredTest]] = {}
    
    for test in tests:
        test_id, func, config = test
        # Module is everything before the last dot
        if '.' in test_id:
            module = test_id.rsplit('.', 1)[0]
        else:
            module = test_id
        
        if module not in groups:
            groups[module] = []
        groups[module].append(test)
    
    return groups


def get_test_summary(tests: List[DiscoveredTest]) -> Dict[str, Any]:
    """
    Get summary statistics about discovered tests.
    
    Returns:
        Dict with test counts and other info
    """
    total = len(tests)
    parallel = sum(1 for _, _, c in tests if c.parallel and not c.crash_test)
    sequential = sum(1 for _, _, c in tests if not c.parallel and not c.crash_test)
    crash = sum(1 for _, _, c in tests if c.crash_test)
    benchmarks = sum(1 for _, _, c in tests if c.benchmark)
    slow = sum(1 for _, _, c in tests if c.slow)
    
    # Collect all unique tags
    all_tags: Set[str] = set()
    for _, _, config in tests:
        all_tags.update(config.tags)
    
    # Group by tag
    by_tag: Dict[str, int] = {}
    for tag in sorted(all_tags):
        by_tag[tag] = sum(1 for _, _, c in tests if tag in c.tags)
    
    return {
        "total": total,
        "parallel": parallel,
        "sequential": sequential,
        "crash_tests": crash,
        "benchmarks": benchmarks,
        "slow": slow,
        "tags": sorted(all_tags),
        "by_tag": by_tag,
    }
