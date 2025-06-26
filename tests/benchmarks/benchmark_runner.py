"""
Benchmark runner with reporting capabilities.

This module provides utilities to run benchmarks and generate
performance reports with threshold validation.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pytest

from .benchmark_config import BenchmarkResult


class BenchmarkRunner:
    """Runner for performance benchmarks with reporting."""

    def __init__(self, output_dir: Optional[Path] = None):
        """Initialize benchmark runner."""
        self.output_dir = output_dir or Path("benchmark_results")
        self.output_dir.mkdir(exist_ok=True)
        self.results: List[BenchmarkResult] = []

    def run_benchmarks(self, markers: str = "benchmark", verbose: bool = True) -> bool:
        """
        Run benchmarks and collect results.

        Args:
            markers: Pytest markers to select benchmarks
            verbose: Whether to print verbose output

        Returns:
            True if all benchmarks passed thresholds
        """
        # Run pytest with benchmark markers
        timestamp = datetime.now().isoformat()

        if verbose:
            print(f"Running benchmarks at {timestamp}")
            print("-" * 60)

        # Run benchmarks
        pytest_args = [
            "tests/benchmarks",
            f"-m={markers}",
            "-v" if verbose else "-q",
            "--tb=short",
        ]

        result = pytest.main(pytest_args)

        all_passed = result == 0

        if verbose:
            print("-" * 60)
            print(f"Benchmark run completed. All passed: {all_passed}")

        return all_passed

    def generate_report(self, results: List[BenchmarkResult]) -> Dict:
        """Generate benchmark report."""
        report = {
            "timestamp": datetime.now().isoformat(),
            "summary": {
                "total_benchmarks": len(results),
                "passed": sum(1 for r in results if r.passed),
                "failed": sum(1 for r in results if not r.passed),
            },
            "results": [],
        }

        for result in results:
            result_data = {
                "name": result.name,
                "passed": result.passed,
                "metrics": {
                    "duration": result.duration,
                    "throughput": result.throughput,
                    "latency_avg": result.latency_avg,
                    "latency_p95": result.latency_p95,
                    "latency_p99": result.latency_p99,
                    "latency_max": result.latency_max,
                    "error_rate": result.error_rate,
                    "memory_used_mb": result.memory_used_mb,
                    "cpu_percent": result.cpu_percent,
                },
            }

            if not result.passed:
                result_data["failure_reason"] = result.failure_reason

            if result.metadata:
                result_data["metadata"] = result.metadata

            report["results"].append(result_data)

        return report

    def save_report(self, report: Dict, filename: Optional[str] = None) -> Path:
        """Save benchmark report to file."""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"benchmark_report_{timestamp}.json"

        filepath = self.output_dir / filename

        with open(filepath, "w") as f:
            json.dump(report, f, indent=2)

        return filepath

    def compare_results(
        self, current: List[BenchmarkResult], baseline: List[BenchmarkResult]
    ) -> Dict:
        """Compare current results against baseline."""
        comparison = {
            "improved": [],
            "regressed": [],
            "unchanged": [],
        }

        # Create baseline lookup
        baseline_by_name = {r.name: r for r in baseline}

        for current_result in current:
            baseline_result = baseline_by_name.get(current_result.name)

            if not baseline_result:
                continue

            # Compare key metrics
            throughput_change = (
                (current_result.throughput - baseline_result.throughput)
                / baseline_result.throughput
                if baseline_result.throughput > 0
                else 0
            )

            latency_change = (
                (current_result.latency_avg - baseline_result.latency_avg)
                / baseline_result.latency_avg
                if baseline_result.latency_avg > 0
                else 0
            )

            comparison_entry = {
                "name": current_result.name,
                "throughput_change": throughput_change,
                "latency_change": latency_change,
                "current": {
                    "throughput": current_result.throughput,
                    "latency_avg": current_result.latency_avg,
                },
                "baseline": {
                    "throughput": baseline_result.throughput,
                    "latency_avg": baseline_result.latency_avg,
                },
            }

            # Categorize change
            if throughput_change > 0.1 or latency_change < -0.1:
                comparison["improved"].append(comparison_entry)
            elif throughput_change < -0.1 or latency_change > 0.1:
                comparison["regressed"].append(comparison_entry)
            else:
                comparison["unchanged"].append(comparison_entry)

        return comparison

    def print_summary(self, report: Dict) -> None:
        """Print benchmark summary to console."""
        print("\nBenchmark Summary")
        print("=" * 60)
        print(f"Total benchmarks: {report['summary']['total_benchmarks']}")
        print(f"Passed: {report['summary']['passed']}")
        print(f"Failed: {report['summary']['failed']}")
        print()

        if report["summary"]["failed"] > 0:
            print("Failed Benchmarks:")
            print("-" * 40)
            for result in report["results"]:
                if not result["passed"]:
                    print(f"  - {result['name']}")
                    print(f"    Reason: {result.get('failure_reason', 'Unknown')}")
            print()

        print("Performance Metrics:")
        print("-" * 40)
        for result in report["results"]:
            if result["passed"]:
                metrics = result["metrics"]
                print(f"  {result['name']}:")
                print(f"    Throughput: {metrics['throughput']:.1f} ops/sec")
                print(f"    Avg Latency: {metrics['latency_avg']*1000:.1f} ms")
                print(f"    P99 Latency: {metrics['latency_p99']*1000:.1f} ms")


def main():
    """Run benchmarks from command line."""
    import argparse

    parser = argparse.ArgumentParser(description="Run async-cassandra benchmarks")
    parser.add_argument(
        "--markers", default="benchmark", help="Pytest markers to select benchmarks"
    )
    parser.add_argument("--output", type=Path, help="Output directory for reports")
    parser.add_argument("--quiet", action="store_true", help="Suppress verbose output")

    args = parser.parse_args()

    runner = BenchmarkRunner(output_dir=args.output)

    # Run benchmarks
    all_passed = runner.run_benchmarks(markers=args.markers, verbose=not args.quiet)

    # Generate and save report
    if runner.results:
        report = runner.generate_report(runner.results)
        report_path = runner.save_report(report)

        if not args.quiet:
            runner.print_summary(report)
            print(f"\nReport saved to: {report_path}")

    return 0 if all_passed else 1


if __name__ == "__main__":
    exit(main())
