"""
Benchmark system for xptest.

Handles benchmark timing statistics, baseline storage and comparison.
"""

import json
import statistics
import subprocess
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import List, Optional, Dict, Any
from datetime import datetime

from .models import TestResult, BenchmarkStats


@dataclass
class BaselineEntry:
    """A single benchmark baseline entry."""
    test_id: str
    mean: float
    stddev: float
    min_time: float
    max_time: float
    p50: float
    p95: float
    runs: int
    recorded_at: str
    git_commit: Optional[str] = None
    git_branch: Optional[str] = None
    notes: Optional[str] = None


class BenchmarkManager:
    """
    Manages benchmark baselines and comparisons.
    
    Stores baselines in JSON format for easy version control.
    """
    
    # Thresholds for regression/improvement detection (percentage)
    REGRESSION_THRESHOLD = 10.0  # >10% slower = regression
    IMPROVEMENT_THRESHOLD = -10.0  # >10% faster = improvement
    
    def __init__(self, baseline_path: Path):
        """
        Initialize benchmark manager.
        
        Args:
            baseline_path: Path to baseline JSON file
        """
        self.baseline_path = baseline_path
        self._baselines: Dict[str, BaselineEntry] = {}
        self._load_baselines()
    
    def _load_baselines(self) -> None:
        """Load baselines from file."""
        if not self.baseline_path.exists():
            return
        
        try:
            data = json.loads(self.baseline_path.read_text())
            for test_id, entry_data in data.get("baselines", {}).items():
                self._baselines[test_id] = BaselineEntry(**entry_data)
        except (json.JSONDecodeError, TypeError, KeyError) as e:
            print(f"Warning: Failed to load baselines from {self.baseline_path}: {e}")
    
    def save_baselines(self, results: List[TestResult]) -> None:
        """
        Save benchmark results as new baseline.
        
        Args:
            results: Test results with benchmark_runs populated
        """
        # Filter to only benchmark tests with successful runs
        benchmark_results = [
            r for r in results 
            if r.config and r.config.benchmark and r.benchmark_runs
        ]
        
        if not benchmark_results:
            return
        
        git_commit = self._get_git_commit()
        git_branch = self._get_git_branch()
        recorded_at = datetime.now().isoformat()
        
        for result in benchmark_results:
            if not result.benchmark_runs:
                continue
            
            stats = self.compute_stats(result.test_id, result.benchmark_runs)
            
            self._baselines[result.test_id] = BaselineEntry(
                test_id=result.test_id,
                mean=stats.mean,
                stddev=stats.stddev,
                min_time=stats.min_time,
                max_time=stats.max_time,
                p50=stats.p50,
                p95=stats.p95,
                runs=len(result.benchmark_runs),
                recorded_at=recorded_at,
                git_commit=git_commit,
                git_branch=git_branch,
            )
        
        # Write to file
        self._save_to_file()
    
    def _save_to_file(self) -> None:
        """Save baselines to JSON file."""
        # Ensure directory exists
        self.baseline_path.parent.mkdir(parents=True, exist_ok=True)
        
        data = {
            "version": 1,
            "updated_at": datetime.now().isoformat(),
            "baselines": {
                test_id: asdict(entry)
                for test_id, entry in self._baselines.items()
            }
        }
        
        self.baseline_path.write_text(json.dumps(data, indent=2))
    
    def get_baseline(self, test_id: str) -> Optional[BaselineEntry]:
        """Get baseline for a specific test."""
        return self._baselines.get(test_id)
    
    def compare(
        self, 
        result: TestResult,
    ) -> Optional[Dict[str, Any]]:
        """
        Compare a benchmark result against its baseline.
        
        Args:
            result: Test result with benchmark_runs
        
        Returns:
            Comparison dict with baseline, current, diff, status
            or None if no baseline exists
        """
        if not result.benchmark_runs:
            return None
        
        baseline = self._baselines.get(result.test_id)
        if not baseline:
            return None
        
        current_stats = self.compute_stats(result.test_id, result.benchmark_runs)
        
        # Calculate percentage difference
        if baseline.mean > 0:
            diff_pct = ((current_stats.mean - baseline.mean) / baseline.mean) * 100
        else:
            diff_pct = 0.0
        
        # Determine status
        if diff_pct > self.REGRESSION_THRESHOLD:
            status = "regression"
        elif diff_pct < self.IMPROVEMENT_THRESHOLD:
            status = "improvement"
        else:
            status = "stable"
        
        return {
            "test_id": result.test_id,
            "baseline": {
                "mean": baseline.mean,
                "stddev": baseline.stddev,
                "min": baseline.min_time,
                "max": baseline.max_time,
                "recorded_at": baseline.recorded_at,
                "git_commit": baseline.git_commit,
            },
            "current": {
                "mean": current_stats.mean,
                "stddev": current_stats.stddev,
                "min": current_stats.min_time,
                "max": current_stats.max_time,
                "runs": len(result.benchmark_runs),
            },
            "diff_percent": diff_pct,
            "diff_absolute": current_stats.mean - baseline.mean,
            "status": status,
            "is_regression": status == "regression",
            "is_improvement": status == "improvement",
        }
    
    def compare_all(
        self, 
        results: List[TestResult],
    ) -> List[Dict[str, Any]]:
        """
        Compare all benchmark results against baselines.
        
        Returns list of comparison dicts.
        """
        comparisons = []
        
        for result in results:
            if result.config and result.config.benchmark and result.benchmark_runs:
                comparison = self.compare(result)
                if comparison:
                    comparisons.append(comparison)
        
        return comparisons
    
    def get_regressions(
        self, 
        results: List[TestResult],
    ) -> List[Dict[str, Any]]:
        """Get only the benchmark regressions."""
        comparisons = self.compare_all(results)
        return [c for c in comparisons if c["is_regression"]]
    
    def get_improvements(
        self, 
        results: List[TestResult],
    ) -> List[Dict[str, Any]]:
        """Get only the benchmark improvements."""
        comparisons = self.compare_all(results)
        return [c for c in comparisons if c["is_improvement"]]
    
    @staticmethod
    def compute_stats(test_id: str, runs: List[float]) -> BenchmarkStats:
        """
        Compute statistics for benchmark runs.
        
        Args:
            test_id: Test identifier
            runs: List of run durations in seconds
        
        Returns:
            BenchmarkStats with computed values
        """
        if not runs:
            return BenchmarkStats(
                test_id=test_id,
                runs=[],
                mean=0.0,
                stddev=0.0,
                min_time=0.0,
                max_time=0.0,
                p50=0.0,
                p95=0.0,
            )
        
        sorted_runs = sorted(runs)
        n = len(sorted_runs)
        
        mean = statistics.mean(runs)
        stddev = statistics.stdev(runs) if n > 1 else 0.0
        min_time = sorted_runs[0]
        max_time = sorted_runs[-1]
        p50 = statistics.median(runs)
        
        # P95: 95th percentile
        p95_idx = int(n * 0.95)
        p95 = sorted_runs[min(p95_idx, n - 1)]
        
        return BenchmarkStats(
            test_id=test_id,
            runs=runs,
            mean=mean,
            stddev=stddev,
            min_time=min_time,
            max_time=max_time,
            p50=p50,
            p95=p95,
        )
    
    @staticmethod
    def _get_git_commit() -> Optional[str]:
        """Get current git commit hash."""
        try:
            result = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode == 0:
                return result.stdout.strip()[:12]
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pass
        return None
    
    @staticmethod
    def _get_git_branch() -> Optional[str]:
        """Get current git branch name."""
        try:
            result = subprocess.run(
                ["git", "rev-parse", "--abbrev-ref", "HEAD"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode == 0:
                return result.stdout.strip()
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pass
        return None


def format_duration(seconds: float) -> str:
    """Format duration for display."""
    if seconds < 0.001:
        return f"{seconds * 1_000_000:.2f}us"
    elif seconds < 1:
        return f"{seconds * 1_000:.2f}ms"
    else:
        return f"{seconds:.3f}s"


def format_diff(diff_pct: float) -> str:
    """Format percentage difference for display."""
    if diff_pct > 0:
        return f"+{diff_pct:.1f}%"
    else:
        return f"{diff_pct:.1f}%"
