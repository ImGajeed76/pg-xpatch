"""
Test execution engine for xptest.

Handles running tests with proper parallelization, fixture management,
timeout handling, and result collection.
"""

import time
import traceback
import threading
import signal
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from dataclasses import dataclass, field
from typing import List, Tuple, Callable, Optional, Dict, Any, Set
from queue import Queue
import functools

from .models import TestConfig, TestResult, TestStatus, RunConfig
from .fixtures import FixtureManager, resolve_fixtures_for_test
from .discovery import DiscoveredTest, sort_tests, resolve_dependencies


class TestTimeoutError(Exception):
    """Raised when a test exceeds its timeout."""
    pass


class TestRunner:
    """
    Main test execution engine.
    
    Handles:
    - Parallel test execution with thread pool
    - Sequential test execution
    - Crash test handling (run last)
    - Timeout enforcement
    - Retry logic
    - Fixture management
    - Result collection and callbacks
    """
    
    def __init__(
        self,
        config: RunConfig,
        on_test_start: Optional[Callable[[str], None]] = None,
        on_test_complete: Optional[Callable[[TestResult], None]] = None,
    ):
        """
        Initialize test runner.
        
        Args:
            config: Run configuration
            on_test_start: Callback when test starts (receives test_id)
            on_test_complete: Callback when test completes (receives TestResult)
        """
        self.config = config
        self.on_test_start = on_test_start
        self.on_test_complete = on_test_complete
        
        self.results: List[TestResult] = []
        self._results_lock = threading.Lock()
        
        self._stop_requested = False
        self._completed_tests: Set[str] = set()
    
    def run_all(self, tests: List[DiscoveredTest]) -> List[TestResult]:
        """
        Execute all tests with proper ordering and parallelization.
        
        Args:
            tests: List of discovered tests to run
        
        Returns:
            List of TestResult objects
        """
        self.results = []
        self._stop_requested = False
        self._completed_tests = set()
        
        if not tests:
            return []
        
        # Resolve dependencies and sort
        try:
            tests = resolve_dependencies(tests)
        except ValueError as e:
            # Circular dependency - report as error
            error_result = TestResult(
                test_id="<dependency_resolution>",
                status=TestStatus.ERROR,
                duration_seconds=0,
                error_message=str(e),
            )
            return [error_result]
        
        tests = sort_tests(tests)
        
        # Categorize tests
        parallel_tests = []
        sequential_tests = []
        crash_tests = []
        
        for test in tests:
            test_id, func, config = test
            if config.crash_test:
                crash_tests.append(test)
            elif config.parallel:
                parallel_tests.append(test)
            else:
                sequential_tests.append(test)
        
        # 1. Run parallel tests
        if parallel_tests and not self._stop_requested:
            self._run_parallel(parallel_tests)
        
        # 2. Run sequential tests
        if sequential_tests and not self._stop_requested:
            self._run_sequential(sequential_tests)
        
        # 3. Run crash tests last
        if crash_tests and not self._stop_requested:
            self._run_sequential(crash_tests)
        
        return self.results
    
    def stop(self) -> None:
        """Request stopping test execution (for fail-fast)."""
        self._stop_requested = True
    
    def _run_parallel(self, tests: List[DiscoveredTest]) -> None:
        """Run tests in parallel using thread pool."""
        # Filter out tests whose dependencies haven't completed
        runnable = self._filter_runnable(tests)
        pending = [t for t in tests if t not in runnable]
        
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            # Submit initial batch
            futures: Dict[Future, DiscoveredTest] = {}
            for test in runnable:
                if self._stop_requested:
                    break
                future = executor.submit(self._execute_test, test)
                futures[future] = test
            
            # Process completed tests and submit more as dependencies resolve
            while futures and not self._stop_requested:
                # Wait for any test to complete
                done_futures = []
                for future in list(futures.keys()):
                    if future.done():
                        done_futures.append(future)
                
                if not done_futures:
                    # Brief sleep to avoid busy-waiting
                    time.sleep(0.01)
                    continue
                
                for future in done_futures:
                    test = futures.pop(future)
                    result = future.result()
                    self._record_result(result)
                    
                    # Check for fail-fast
                    if (self.config.fail_fast and 
                        result.status in (TestStatus.FAILED, TestStatus.ERROR)):
                        self._stop_requested = True
                        break
                    
                    # Check if any pending tests can now run
                    newly_runnable = self._filter_runnable(pending)
                    for new_test in newly_runnable:
                        pending.remove(new_test)
                        if not self._stop_requested:
                            new_future = executor.submit(self._execute_test, new_test)
                            futures[new_future] = new_test
    
    def _run_sequential(self, tests: List[DiscoveredTest]) -> None:
        """Run tests sequentially."""
        for test in tests:
            if self._stop_requested:
                break
            
            # Check dependencies
            test_id, _, config = test
            if not self._dependencies_satisfied(config.depends_on):
                result = TestResult(
                    test_id=test_id,
                    status=TestStatus.SKIPPED,
                    duration_seconds=0,
                    error_message="Dependency not satisfied",
                )
                self._record_result(result)
                continue
            
            result = self._execute_test(test)
            self._record_result(result)
            
            # Check for fail-fast
            if (self.config.fail_fast and 
                result.status in (TestStatus.FAILED, TestStatus.ERROR)):
                self._stop_requested = True
    
    def _filter_runnable(self, tests: List[DiscoveredTest]) -> List[DiscoveredTest]:
        """Filter tests to those whose dependencies are satisfied."""
        runnable = []
        for test in tests:
            _, _, config = test
            if self._dependencies_satisfied(config.depends_on):
                runnable.append(test)
        return runnable
    
    def _dependencies_satisfied(self, depends_on: List[str]) -> bool:
        """Check if all dependencies have completed successfully."""
        for dep_id in depends_on:
            if dep_id not in self._completed_tests:
                return False
            # Could also check if dependency passed, but for now just check completion
        return True
    
    def _execute_test(self, test: DiscoveredTest) -> TestResult:
        """
        Execute a single test with full setup/teardown cycle.
        
        Handles:
        - Fixture resolution and cleanup
        - Timeout enforcement
        - Retry logic
        - Exception handling
        """
        test_id, func, config = test
        
        # Notify test start
        if self.on_test_start:
            try:
                self.on_test_start(test_id)
            except Exception:
                pass
        
        start_time = time.perf_counter()
        status = TestStatus.PASSED
        error_message: Optional[str] = None
        error_traceback: Optional[str] = None
        
        # Calculate effective timeout
        timeout = config.timeout * self.config.timeout_multiplier
        
        # Retry loop
        attempts = 0
        max_attempts = config.retries + 1
        
        while attempts < max_attempts:
            attempts += 1
            
            # Create fixture manager for this test
            fixture_manager = FixtureManager(
                host="localhost",
                port=5432,
                user="postgres",
                container=self.config.container,
            )
            
            try:
                # Resolve fixtures
                fixtures = resolve_fixtures_for_test(
                    func, 
                    fixture_manager,
                    module_name=test_id.rsplit('.', 1)[0] if '.' in test_id else None,
                )
                
                # Execute test with timeout
                self._run_with_timeout(func, fixtures, timeout)
                
                # Test passed
                status = TestStatus.PASSED
                error_message = None
                error_traceback = None
                break
                
            except AssertionError as e:
                status = TestStatus.FAILED
                error_message = str(e) or "Assertion failed"
                error_traceback = traceback.format_exc()
                
            except TestTimeoutError as e:
                status = TestStatus.TIMEOUT
                error_message = str(e)
                error_traceback = None
                # Don't retry timeouts
                break
                
            except Exception as e:
                status = TestStatus.ERROR
                error_message = f"{type(e).__name__}: {e}"
                error_traceback = traceback.format_exc()
            
            finally:
                # Always cleanup fixtures
                try:
                    fixture_manager.cleanup_function_fixtures()
                except Exception as cleanup_error:
                    # Log but don't fail the test for cleanup errors
                    if error_message:
                        error_message += f" (cleanup error: {cleanup_error})"
        
        duration = time.perf_counter() - start_time
        
        return TestResult(
            test_id=test_id,
            status=status,
            duration_seconds=duration,
            error_message=error_message,
            error_traceback=error_traceback,
            attempts=attempts,
            config=config,
        )
    
    def _run_with_timeout(
        self, 
        func: Callable[..., Any], 
        kwargs: Dict[str, Any],
        timeout: float,
    ) -> Any:
        """
        Run function with timeout.
        
        Uses a separate thread to allow timeout on blocking operations.
        """
        result_container: Dict[str, Any] = {}
        exception_container: Dict[str, BaseException] = {}
        
        def target():
            try:
                result_container['result'] = func(**kwargs)
            except BaseException as e:
                exception_container['exception'] = e
        
        thread = threading.Thread(target=target, daemon=True)
        thread.start()
        thread.join(timeout=timeout)
        
        if thread.is_alive():
            # Timeout occurred - thread is still running
            # We can't forcefully kill it, but we raise an error
            raise TestTimeoutError(f"Test exceeded {timeout}s timeout")
        
        if 'exception' in exception_container:
            raise exception_container['exception']
        
        return result_container.get('result')
    
    def _record_result(self, result: TestResult) -> None:
        """Record a test result and notify callback."""
        with self._results_lock:
            self.results.append(result)
            self._completed_tests.add(result.test_id)
        
        if self.on_test_complete:
            try:
                self.on_test_complete(result)
            except Exception:
                pass


class BenchmarkRunner:
    """
    Specialized runner for benchmark tests.
    
    Runs benchmark tests multiple times and collects statistics.
    """
    
    def __init__(
        self,
        config: RunConfig,
        num_runs: int = 5,
        warmup_runs: int = 1,
    ):
        """
        Initialize benchmark runner.
        
        Args:
            config: Run configuration
            num_runs: Number of timed runs per benchmark
            warmup_runs: Number of warmup runs (not timed)
        """
        self.config = config
        self.num_runs = num_runs
        self.warmup_runs = warmup_runs
    
    def run_benchmark(self, test: DiscoveredTest) -> TestResult:
        """
        Run a benchmark test multiple times and collect timing stats.
        
        Returns TestResult with benchmark_runs populated.
        """
        test_id, func, test_config = test
        
        runs: List[float] = []
        status = TestStatus.PASSED
        error_message: Optional[str] = None
        error_traceback: Optional[str] = None
        
        total_start = time.perf_counter()
        
        try:
            # Warmup runs
            for _ in range(self.warmup_runs):
                fixture_manager = FixtureManager(
                    host="localhost",
                    port=5432,
                    user="postgres",
                    container=self.config.container,
                )
                try:
                    fixtures = resolve_fixtures_for_test(func, fixture_manager)
                    func(**fixtures)
                finally:
                    fixture_manager.cleanup_function_fixtures()
            
            # Timed runs
            for run_num in range(self.num_runs):
                fixture_manager = FixtureManager(
                    host="localhost",
                    port=5432,
                    user="postgres",
                    container=self.config.container,
                )
                try:
                    fixtures = resolve_fixtures_for_test(func, fixture_manager)
                    
                    run_start = time.perf_counter()
                    func(**fixtures)
                    run_duration = time.perf_counter() - run_start
                    
                    runs.append(run_duration)
                finally:
                    fixture_manager.cleanup_function_fixtures()
        
        except AssertionError as e:
            status = TestStatus.FAILED
            error_message = str(e) or "Assertion failed"
            error_traceback = traceback.format_exc()
        
        except Exception as e:
            status = TestStatus.ERROR
            error_message = f"{type(e).__name__}: {e}"
            error_traceback = traceback.format_exc()
        
        total_duration = time.perf_counter() - total_start
        
        return TestResult(
            test_id=test_id,
            status=status,
            duration_seconds=total_duration,
            error_message=error_message,
            error_traceback=error_traceback,
            attempts=1,
            config=test_config,
            benchmark_runs=runs if runs else None,
        )


def run_tests(
    tests: List[DiscoveredTest],
    config: Optional[RunConfig] = None,
    on_test_start: Optional[Callable[[str], None]] = None,
    on_test_complete: Optional[Callable[[TestResult], None]] = None,
) -> List[TestResult]:
    """
    Convenience function to run tests.
    
    Args:
        tests: Tests to run
        config: Run configuration (uses defaults if not provided)
        on_test_start: Callback when test starts
        on_test_complete: Callback when test completes
    
    Returns:
        List of test results
    """
    if config is None:
        config = RunConfig()
    
    runner = TestRunner(
        config=config,
        on_test_start=on_test_start,
        on_test_complete=on_test_complete,
    )
    
    return runner.run_all(tests)
