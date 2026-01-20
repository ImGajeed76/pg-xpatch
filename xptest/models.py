"""
Data models for xptest.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, List, Any


class TestStatus(str, Enum):
    """Status of a test execution."""
    PASSED = "passed"
    FAILED = "failed"
    ERROR = "error"
    SKIPPED = "skipped"
    TIMEOUT = "timeout"


@dataclass
class TestConfig:
    """Configuration for a single test, set via @pg_test() decorator."""
    parallel: bool = True
    benchmark: bool = False
    slow: bool = False
    crash_test: bool = False
    timeout: int = 60
    retries: int = 0
    tags: List[str] = field(default_factory=list)
    depends_on: List[str] = field(default_factory=list)


@dataclass
class TestResult:
    """Result of a single test execution."""
    test_id: str
    status: TestStatus
    duration_seconds: float
    error_message: Optional[str] = None
    error_traceback: Optional[str] = None
    attempts: int = 1
    config: Optional[TestConfig] = None
    
    # Benchmark data (only populated if benchmark=True)
    benchmark_runs: Optional[List[float]] = None


@dataclass 
class BenchmarkStats:
    """Statistics for benchmark tests."""
    test_id: str
    runs: List[float]
    mean: float
    stddev: float
    min_time: float
    max_time: float
    p50: float
    p95: float


@dataclass
class RunConfig:
    """Configuration for a test run."""
    max_workers: int = 32  # Support 32 parallel tests by default
    fail_fast: bool = False
    verbose: bool = False
    timeout_multiplier: float = 1.0
    benchmark_runs: int = 5  # Number of times to run benchmarks
    container: str = "pg-xpatch-dev"
