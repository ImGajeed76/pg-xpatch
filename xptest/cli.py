"""
CLI entry point for xptest.

Uses Click for argument parsing and provides a clean interface
for running tests from the command line.
"""

import sys
import time
from pathlib import Path
from typing import Optional, Tuple, List

try:
    import click
    CLICK_AVAILABLE = True
except ImportError:
    CLICK_AVAILABLE = False

from .models import RunConfig, TestStatus
from .discovery import discover_tests, filter_tests, get_test_summary
from .runner import TestRunner, BenchmarkRunner
from .reporting import TestReporter, write_json_report
from .benchmarks import BenchmarkManager
from .database import check_container_running, check_postgres_connection
from .fixtures import FixtureManager


# Default paths
DEFAULT_TEST_PATH = Path("tests")
DEFAULT_BASELINE_PATH = Path("benchmarks/baseline.json")


def _validate_environment(container: str, reporter: TestReporter) -> bool:
    """
    Validate that the test environment is ready.
    
    Checks:
    - Docker container is running
    - PostgreSQL is reachable
    """
    # Check container
    if not check_container_running(container):
        reporter.print_error(
            f"Container '{container}' is not running.\n"
            f"Start it with: docker compose -f .devcontainer/docker-compose.yml up -d"
        )
        return False
    
    # Check PostgreSQL connection
    if not check_postgres_connection():
        reporter.print_error(
            "Cannot connect to PostgreSQL.\n"
            "Make sure the container is running and port 5432 is accessible."
        )
        return False
    
    return True


if CLICK_AVAILABLE:
    @click.command()
    @click.argument(
        "paths", 
        nargs=-1, 
        type=click.Path(exists=True),
    )
    @click.option(
        "-k", "--filter", "name_filter",
        help="Filter tests by name pattern (substring or regex)",
    )
    @click.option(
        "-t", "--tag", "tags",
        multiple=True,
        help="Filter by tag (can be repeated, OR logic)",
    )
    @click.option(
        "-v", "--verbose",
        is_flag=True,
        help="Verbose output (show stack traces)",
    )
    @click.option(
        "-q", "--quiet",
        is_flag=True,
        help="Quiet mode (minimal output)",
    )
    @click.option(
        "--fast",
        is_flag=True,
        help="Skip slow tests",
    )
    @click.option(
        "--benchmarks",
        is_flag=True,
        help="Run only benchmark tests",
    )
    @click.option(
        "--parallel/--sequential",
        default=True,
        help="Force parallel or sequential execution",
    )
    @click.option(
        "-j", "--jobs",
        default=4,
        type=int,
        help="Number of parallel workers (default: 4)",
    )
    @click.option(
        "--fail-fast",
        is_flag=True,
        help="Stop on first failure",
    )
    @click.option(
        "--timeout",
        default=1.0,
        type=float,
        help="Timeout multiplier (default: 1.0)",
    )
    @click.option(
        "--json",
        "json_output",
        type=click.Path(),
        help="Write JSON report to file",
    )
    @click.option(
        "--save-baseline",
        is_flag=True,
        help="Save benchmark results as new baseline",
    )
    @click.option(
        "--compare-baseline",
        is_flag=True,
        help="Compare benchmark results against baseline",
    )
    @click.option(
        "--baseline-file",
        type=click.Path(),
        default=str(DEFAULT_BASELINE_PATH),
        help=f"Path to baseline file (default: {DEFAULT_BASELINE_PATH})",
    )
    @click.option(
        "--container",
        default="pg-xpatch-dev",
        help="Docker container name (default: pg-xpatch-dev)",
    )
    @click.option(
        "--list", "list_tests",
        is_flag=True,
        help="List tests without running them",
    )
    @click.option(
        "--no-color",
        is_flag=True,
        help="Disable colored output",
    )
    @click.option(
        "--benchmark-runs",
        default=5,
        type=int,
        help="Number of runs for benchmark tests (default: 5)",
    )
    @click.version_option(version="0.1.0", prog_name="xptest")
    def main(
        paths: Tuple[str, ...],
        name_filter: Optional[str],
        tags: Tuple[str, ...],
        verbose: bool,
        quiet: bool,
        fast: bool,
        benchmarks: bool,
        parallel: bool,
        jobs: int,
        fail_fast: bool,
        timeout: float,
        json_output: Optional[str],
        save_baseline: bool,
        compare_baseline: bool,
        baseline_file: str,
        container: str,
        list_tests: bool,
        no_color: bool,
        benchmark_runs: int,
    ):
        """
        pg-xpatch Test Runner
        
        Run tests from specified PATHS (default: tests/)
        
        \b
        Examples:
            xptest                        # Run all tests
            xptest tests/unit/            # Run unit tests only
            xptest -k "stats"             # Filter by name
            xptest -t unit -t config      # Filter by tags
            xptest --benchmarks           # Only benchmarks
            xptest --fast                 # Skip slow tests
            xptest -j 8                   # 8 parallel workers
            xptest --list                 # List tests
        """
        exit_code = run_cli(
            paths=paths,
            name_filter=name_filter,
            tags=tags,
            verbose=verbose,
            quiet=quiet,
            fast=fast,
            benchmarks=benchmarks,
            parallel=parallel,
            jobs=jobs,
            fail_fast=fail_fast,
            timeout=timeout,
            json_output=json_output,
            save_baseline=save_baseline,
            compare_baseline=compare_baseline,
            baseline_file=baseline_file,
            container=container,
            list_tests=list_tests,
            no_color=no_color,
            benchmark_runs=benchmark_runs,
        )
        sys.exit(exit_code)

else:
    # Fallback when click is not available
    def main():
        """Fallback main when click is not installed."""
        print("Error: click is required for CLI. Install with: pip install click")
        print("Or run directly: python -c 'from xptest.cli import run_cli; ...'")
        sys.exit(1)


def run_cli(
    paths: Tuple[str, ...] = (),
    name_filter: Optional[str] = None,
    tags: Tuple[str, ...] = (),
    verbose: bool = False,
    quiet: bool = False,
    fast: bool = False,
    benchmarks: bool = False,
    parallel: bool = True,
    jobs: int = 4,
    fail_fast: bool = False,
    timeout: float = 1.0,
    json_output: Optional[str] = None,
    save_baseline: bool = False,
    compare_baseline: bool = False,
    baseline_file: str = str(DEFAULT_BASELINE_PATH),
    container: str = "pg-xpatch-dev",
    list_tests: bool = False,
    no_color: bool = False,
    benchmark_runs: int = 5,
) -> int:
    """
    Main CLI logic (can be called programmatically).
    
    Returns exit code (0 for success, 1 for failures).
    """
    # Create reporter
    reporter = TestReporter(
        verbose=verbose,
        quiet=quiet,
        no_color=no_color,
    )
    
    # Validate environment (unless just listing)
    if not list_tests:
        if not _validate_environment(container, reporter):
            return 1
    
    # Determine test paths
    test_paths = [Path(p) for p in paths] if paths else [DEFAULT_TEST_PATH]
    
    # Check paths exist
    for path in test_paths:
        if not path.exists():
            reporter.print_error(f"Path does not exist: {path}")
            return 1
    
    # Discover tests
    try:
        all_tests = discover_tests(test_paths)
    except Exception as e:
        reporter.print_error(f"Failed to discover tests: {e}")
        if verbose:
            import traceback
            traceback.print_exc()
        return 1
    
    # Filter tests
    tests = filter_tests(
        all_tests,
        tags=list(tags) if tags else None,
        name_pattern=name_filter,
        exclude_slow=fast,
        only_benchmarks=benchmarks,
    )
    
    # List mode
    if list_tests:
        reporter.print_test_list(tests)
        summary = get_test_summary(tests)
        reporter.print(
            f"\nTotal: {summary['total']} tests "
            f"({summary['parallel']} parallel, {summary['sequential']} sequential, "
            f"{summary['crash_tests']} crash tests)"
        )
        if summary['benchmarks']:
            reporter.print(f"Benchmarks: {summary['benchmarks']}")
        if summary['tags']:
            reporter.print(f"Tags: {', '.join(summary['tags'])}")
        return 0
    
    # No tests found
    if not tests:
        reporter.print("No tests found matching criteria.", style="yellow")
        return 0
    
    # Create run configuration
    config = RunConfig(
        max_workers=jobs if parallel else 1,
        fail_fast=fail_fast,
        verbose=verbose,
        timeout_multiplier=timeout,
        benchmark_runs=benchmark_runs,
        container=container,
    )
    
    # Print header
    reporter.print_header(config, len(tests))
    
    # Create runner
    runner = TestRunner(
        config=config,
        on_test_complete=reporter.update_progress,
    )
    
    # Run tests
    start_time = time.perf_counter()
    
    with reporter.progress_context(len(tests)):
        results = runner.run_all(tests)
    
    total_duration = time.perf_counter() - start_time
    
    # Handle benchmark tests specially
    bench_results = [
        r for r in results 
        if r.config and r.config.benchmark
    ]
    
    if bench_results and (compare_baseline or save_baseline):
        # Run benchmarks multiple times for statistics
        bench_runner = BenchmarkRunner(
            config=config,
            num_runs=benchmark_runs,
        )
        
        # Re-run benchmark tests
        bench_tests = [
            (test_id, func, cfg) 
            for test_id, func, cfg in tests 
            if cfg.benchmark
        ]
        
        for test in bench_tests:
            test_id = test[0]
            # Find and update result
            for i, r in enumerate(results):
                if r.test_id == test_id:
                    results[i] = bench_runner.run_benchmark(test)
                    break
    
    # Print summary
    reporter.print_summary(results, total_duration)
    
    # Benchmark comparison/saving
    benchmark_manager = BenchmarkManager(Path(baseline_file))
    
    if compare_baseline:
        reporter.print_benchmark_results(results, benchmark_manager)
        
        # Check for regressions
        regressions = benchmark_manager.get_regressions(results)
        if regressions:
            reporter.print(
                f"\n[red]Warning: {len(regressions)} benchmark regression(s) detected![/red]"
            )
    
    if save_baseline:
        benchmark_manager.save_baselines(results)
        reporter.print(f"\nBaseline saved to {baseline_file}", style="green")
    
    # JSON report
    if json_output:
        write_json_report(results, Path(json_output), config, total_duration)
        reporter.print(f"\nJSON report written to {json_output}")
    
    # Determine exit code
    failed_count = sum(
        1 for r in results 
        if r.status in (TestStatus.FAILED, TestStatus.ERROR, TestStatus.TIMEOUT)
    )
    
    return 1 if failed_count > 0 else 0


if __name__ == "__main__":
    main()
