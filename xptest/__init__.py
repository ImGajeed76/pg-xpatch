"""
xptest - pg-xpatch Test Runner

A custom test runner for pg-xpatch PostgreSQL extension testing.
Designed for parallel execution, benchmarking, and clean TUI output.
"""

from .decorators import pg_test
from .fixtures import pg_fixture, Scope
from .models import TestStatus, TestResult

__version__ = "0.1.0"
__all__ = ["pg_test", "pg_fixture", "Scope", "TestStatus", "TestResult"]
