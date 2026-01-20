"""
Reporting and TUI output for xptest.

Uses Rich library for beautiful terminal output with progress bars,
colored text, tables, and panels.
"""

import json
from contextlib import contextmanager
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Any, Generator

from .models import TestResult, TestStatus, RunConfig
from .benchmarks import BenchmarkManager, format_duration, format_diff

try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich.progress import (
        Progress, 
        SpinnerColumn, 
        TextColumn, 
        BarColumn,
        TaskProgressColumn,
        TimeElapsedColumn,
        MofNCompleteColumn,
    )
    from rich.live import Live
    from rich.tree import Tree
    from rich.text import Text
    from rich.style import Style
    from rich.markup import escape
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False


# Status colors and symbols
STATUS_STYLES = {
    TestStatus.PASSED: ("green", "PASS", "[green]PASS[/green]"),
    TestStatus.FAILED: ("red", "FAIL", "[red]FAIL[/red]"),
    TestStatus.ERROR: ("red", "ERR ", "[red]ERROR[/red]"),
    TestStatus.SKIPPED: ("yellow", "SKIP", "[yellow]SKIP[/yellow]"),
    TestStatus.TIMEOUT: ("red", "TIME", "[red]TIMEOUT[/red]"),
}


class TestReporter:
    """
    Main reporter class for test output.
    
    Handles live progress updates and final summary output.
    """
    
    def __init__(
        self, 
        verbose: bool = False,
        quiet: bool = False,
        no_color: bool = False,
    ):
        """
        Initialize reporter.
        
        Args:
            verbose: Show detailed output including stack traces
            quiet: Minimal output (only errors and summary)
            no_color: Disable colored output
        """
        self.verbose = verbose
        self.quiet = quiet
        self.no_color = no_color
        
        if RICH_AVAILABLE and not no_color:
            self.console = Console()
        else:
            self.console = None
        
        self._progress: Optional[Progress] = None
        self._task_id: Optional[int] = None
        self._live: Optional[Live] = None
    
    def print(self, message: str, style: Optional[str] = None) -> None:
        """Print a message to the console."""
        if self.quiet:
            return
        
        if self.console:
            if style:
                self.console.print(message, style=style)
            else:
                self.console.print(message)
        else:
            # Strip rich markup for plain output
            plain = self._strip_markup(message)
            print(plain)
    
    def print_error(self, message: str) -> None:
        """Print an error message (always shown, even in quiet mode)."""
        if self.console:
            self.console.print(f"[red]Error:[/red] {message}")
        else:
            print(f"Error: {message}")
    
    def _strip_markup(self, text: str) -> str:
        """Strip Rich markup from text for plain output."""
        import re
        return re.sub(r'\[/?[^\]]+\]', '', text)
    
    @contextmanager
    def progress_context(
        self, 
        total_tests: int,
        description: str = "Running tests",
    ) -> Generator["TestReporter", None, None]:
        """
        Context manager for live progress display.
        
        Args:
            total_tests: Total number of tests
            description: Progress bar description
        
        Yields:
            Self for chaining
        """
        if self.quiet or not self.console:
            yield self
            return
        
        self._progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(bar_width=30),
            MofNCompleteColumn(),
            TextColumn("[cyan]{task.percentage:>3.0f}%[/cyan]"),
            TimeElapsedColumn(),
            console=self.console,
            transient=False,
        )
        
        with self._progress:
            self._task_id = self._progress.add_task(
                description,
                total=total_tests,
            )
            yield self
        
        self._progress = None
        self._task_id = None
    
    def update_progress(self, result: TestResult) -> None:
        """
        Update progress bar and show test result.
        
        Args:
            result: Completed test result
        """
        if self._progress and self._task_id is not None:
            self._progress.advance(self._task_id)
            
            if not self.quiet:
                self._print_test_result(result)
    
    def _print_test_result(self, result: TestResult) -> None:
        """Print a single test result line."""
        _, plain_status, rich_status = STATUS_STYLES.get(
            result.status, 
            ("white", "???", "[white]???[/white]")
        )
        
        duration_str = f"({result.duration_seconds:.2f}s)"
        
        if self.console:
            # Show retry info if applicable
            retry_info = ""
            if result.attempts > 1:
                retry_info = f" [dim](attempt {result.attempts})[/dim]"
            
            self.console.print(
                f"  {rich_status} {escape(result.test_id)} "
                f"[dim]{duration_str}[/dim]{retry_info}"
            )
        else:
            print(f"  {plain_status} {result.test_id} {duration_str}")
    
    def print_header(self, config: RunConfig, total_tests: int) -> None:
        """Print test run header."""
        if self.quiet:
            return
        
        if self.console:
            self.console.print()
            self.console.print(
                Panel(
                    f"[bold]pg-xpatch Test Runner[/bold]\n"
                    f"[dim]Container: {config.container}[/dim]",
                    title="xptest",
                    border_style="blue",
                )
            )
            self.console.print(f"\nFound [cyan]{total_tests}[/cyan] tests\n")
        else:
            print("\n=== pg-xpatch Test Runner ===")
            print(f"Container: {config.container}")
            print(f"\nFound {total_tests} tests\n")
    
    def print_summary(self, results: List[TestResult], duration: float) -> None:
        """
        Print final test summary.
        
        Args:
            results: All test results
            duration: Total run duration in seconds
        """
        if not results:
            self.print("No tests were run.")
            return
        
        # Count by status
        counts: Dict[TestStatus, int] = {}
        for result in results:
            counts[result.status] = counts.get(result.status, 0) + 1
        
        total = len(results)
        passed = counts.get(TestStatus.PASSED, 0)
        failed = counts.get(TestStatus.FAILED, 0)
        errors = counts.get(TestStatus.ERROR, 0)
        skipped = counts.get(TestStatus.SKIPPED, 0)
        timeouts = counts.get(TestStatus.TIMEOUT, 0)
        
        failed_total = failed + errors + timeouts
        
        if self.console:
            self.console.print()
            
            # Summary panel
            if failed_total == 0:
                summary_text = f"[green bold]All {total} tests passed![/green bold]"
                border_style = "green"
            else:
                parts = []
                if passed:
                    parts.append(f"[green]{passed} passed[/green]")
                if failed:
                    parts.append(f"[red]{failed} failed[/red]")
                if errors:
                    parts.append(f"[red]{errors} errors[/red]")
                if timeouts:
                    parts.append(f"[red]{timeouts} timeouts[/red]")
                if skipped:
                    parts.append(f"[yellow]{skipped} skipped[/yellow]")
                
                summary_text = ", ".join(parts) + f" [dim]of {total} tests[/dim]"
                border_style = "red"
            
            self.console.print(Panel(
                summary_text,
                title="Results",
                border_style=border_style,
            ))
            
            # Show failures
            failures = [
                r for r in results 
                if r.status in (TestStatus.FAILED, TestStatus.ERROR, TestStatus.TIMEOUT)
            ]
            
            if failures:
                self.console.print("\n[red bold]Failures:[/red bold]\n")
                for result in failures:
                    self._print_failure_details(result)
            
            # Timing
            self.console.print(
                f"\n[dim]Total time: {duration:.2f}s[/dim]"
            )
        else:
            # Plain text output
            print("\n" + "=" * 60)
            print("RESULTS")
            print("=" * 60)
            
            if failed_total == 0:
                print(f"\nAll {total} tests passed!")
            else:
                print(f"\nPassed: {passed}, Failed: {failed + errors}, "
                      f"Skipped: {skipped}, Timeouts: {timeouts}")
            
            failures = [
                r for r in results 
                if r.status in (TestStatus.FAILED, TestStatus.ERROR, TestStatus.TIMEOUT)
            ]
            
            if failures:
                print("\nFailures:")
                for result in failures:
                    print(f"  - {result.test_id}")
                    if result.error_message:
                        print(f"    {result.error_message}")
            
            print(f"\nTotal time: {duration:.2f}s")
    
    def _print_failure_details(self, result: TestResult) -> None:
        """Print detailed failure information."""
        if not self.console:
            return
        
        self.console.print(f"[red]{escape(result.test_id)}[/red]")
        
        if result.error_message:
            self.console.print(f"  [dim]Message:[/dim] {escape(result.error_message)}")
        
        if self.verbose and result.error_traceback:
            self.console.print(f"  [dim]Traceback:[/dim]")
            for line in result.error_traceback.split('\n'):
                if line.strip():
                    self.console.print(f"    [dim]{escape(line)}[/dim]")
        
        self.console.print()
    
    def print_benchmark_results(
        self, 
        results: List[TestResult],
        benchmark_manager: Optional[BenchmarkManager] = None,
    ) -> None:
        """
        Print benchmark results table.
        
        Args:
            results: Test results (filtered to benchmarks)
            benchmark_manager: For baseline comparison (optional)
        """
        bench_results = [
            r for r in results 
            if r.config and r.config.benchmark and r.benchmark_runs
        ]
        
        if not bench_results:
            return
        
        if self.console:
            table = Table(title="Benchmark Results")
            table.add_column("Test", style="cyan")
            table.add_column("Mean", justify="right")
            table.add_column("StdDev", justify="right")
            table.add_column("Min", justify="right")
            table.add_column("Max", justify="right")
            
            if benchmark_manager:
                table.add_column("Baseline", justify="right")
                table.add_column("Diff", justify="right")
                table.add_column("Status")
            
            for result in bench_results:
                if not result.benchmark_runs:
                    continue
                
                stats = BenchmarkManager.compute_stats(
                    result.test_id, 
                    result.benchmark_runs
                )
                
                row = [
                    result.test_id.split('.')[-1],  # Short name
                    format_duration(stats.mean),
                    format_duration(stats.stddev),
                    format_duration(stats.min_time),
                    format_duration(stats.max_time),
                ]
                
                if benchmark_manager:
                    comparison = benchmark_manager.compare(result)
                    if comparison:
                        row.extend([
                            format_duration(comparison["baseline"]["mean"]),
                            format_diff(comparison["diff_percent"]),
                            self._get_status_badge(comparison["status"]),
                        ])
                    else:
                        row.extend(["-", "-", "[yellow]NEW[/yellow]"])
                
                table.add_row(*row)
            
            self.console.print()
            self.console.print(table)
        else:
            # Plain text
            print("\nBenchmark Results:")
            print("-" * 60)
            for result in bench_results:
                if not result.benchmark_runs:
                    continue
                stats = BenchmarkManager.compute_stats(
                    result.test_id, 
                    result.benchmark_runs
                )
                print(f"  {result.test_id}: mean={format_duration(stats.mean)}, "
                      f"stddev={format_duration(stats.stddev)}")
    
    def _get_status_badge(self, status: str) -> str:
        """Get colored status badge for benchmark comparison."""
        if status == "regression":
            return "[red]SLOWER[/red]"
        elif status == "improvement":
            return "[green]FASTER[/green]"
        else:
            return "[dim]OK[/dim]"
    
    def print_test_list(
        self, 
        tests: List[tuple],
        show_tags: bool = True,
    ) -> None:
        """
        Print list of discovered tests.
        
        Useful for --list flag.
        """
        if not tests:
            self.print("No tests found.")
            return
        
        if self.console:
            tree = Tree("[bold]Tests[/bold]")
            
            # Group by module
            modules: Dict[str, list] = {}
            for test_id, func, config in tests:
                parts = test_id.rsplit('.', 1)
                module = parts[0] if len(parts) > 1 else "<root>"
                test_name = parts[-1]
                
                if module not in modules:
                    modules[module] = []
                modules[module].append((test_name, config))
            
            for module, test_list in sorted(modules.items()):
                module_branch = tree.add(f"[cyan]{escape(module)}[/cyan]")
                for test_name, config in test_list:
                    tags_str = ""
                    if show_tags and config.tags:
                        tags_str = f" [dim][{', '.join(config.tags)}][/dim]"
                    
                    flags = []
                    if config.benchmark:
                        flags.append("[magenta]bench[/magenta]")
                    if config.slow:
                        flags.append("[yellow]slow[/yellow]")
                    if config.crash_test:
                        flags.append("[red]crash[/red]")
                    if not config.parallel:
                        flags.append("[blue]seq[/blue]")
                    
                    flags_str = " " + " ".join(flags) if flags else ""
                    
                    module_branch.add(f"{escape(test_name)}{tags_str}{flags_str}")
            
            self.console.print(tree)
        else:
            print("Tests:")
            for test_id, func, config in tests:
                tags_str = f" [{', '.join(config.tags)}]" if config.tags else ""
                print(f"  {test_id}{tags_str}")


def write_json_report(
    results: List[TestResult],
    output_path: Path,
    config: Optional[RunConfig] = None,
    duration: float = 0,
) -> None:
    """
    Write test results to JSON file.
    
    Args:
        results: Test results to write
        output_path: Path to output JSON file
        config: Run configuration (optional)
        duration: Total run duration
    """
    # Count by status
    counts: Dict[str, int] = {}
    for result in results:
        status_name = result.status.value
        counts[status_name] = counts.get(status_name, 0) + 1
    
    report = {
        "timestamp": datetime.now().isoformat(),
        "duration_seconds": duration,
        "summary": {
            "total": len(results),
            **counts,
        },
        "config": asdict(config) if config else None,
        "results": [
            {
                "test_id": r.test_id,
                "status": r.status.value,
                "duration_seconds": r.duration_seconds,
                "error_message": r.error_message,
                "error_traceback": r.error_traceback if r.error_traceback else None,
                "attempts": r.attempts,
                "benchmark_runs": r.benchmark_runs,
            }
            for r in results
        ],
    }
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, default=str))
