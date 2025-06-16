#!/usr/bin/env python3
"""Run all tests with proper configuration and reporting."""

import subprocess
import sys
import time
from pathlib import Path

# Test configurations
TEST_SUITES = [
    {
        "name": "Core Tests",
        "command": ["pytest", "tests/_core", "tests/_resilience", "-v", "--tb=short"],
        "timeout": 120,
    },
    {
        "name": "Unit Tests",
        "command": [
            "pytest",
            "tests/unit/",
            "-v",
            "--tb=short",
            "-k",
            "not test_timeout_implementation",
        ],
        "timeout": 180,
    },
    {
        "name": "Feature Tests",
        "command": ["pytest", "tests/_features", "-v", "--tb=short"],
        "timeout": 120,
    },
    {
        "name": "Integration Tests",
        "command": ["pytest", "tests/integration/", "-v", "--tb=short", "-m", "integration"],
        "timeout": 300,
    },
    {
        "name": "FastAPI Tests",
        "command": ["pytest", "tests/fastapi/", "-v", "--tb=short"],
        "timeout": 180,
    },
    {
        "name": "BDD Tests",
        "command": ["pytest", "tests/bdd/", "-v", "--tb=short"],
        "timeout": 180,
    },
]


def run_test_suite(suite):
    """Run a single test suite."""
    print(f"\n{'='*60}")
    print(f"Running {suite['name']}")
    print(f"{'='*60}")

    start_time = time.time()

    try:
        result = subprocess.run(
            suite["command"], timeout=suite["timeout"], capture_output=True, text=True
        )

        duration = time.time() - start_time

        # Print output
        if result.stdout:
            print(result.stdout)
        if result.stderr:
            print(result.stderr, file=sys.stderr)

        if result.returncode == 0:
            print(f"\n✅ {suite['name']} PASSED in {duration:.1f}s")
            return True
        else:
            print(f"\n❌ {suite['name']} FAILED in {duration:.1f}s")
            return False

    except subprocess.TimeoutExpired:
        duration = time.time() - start_time
        print(f"\n⏱️ {suite['name']} TIMED OUT after {duration:.1f}s")
        return False
    except Exception as e:
        print(f"\n💥 {suite['name']} ERROR: {e}")
        return False


def main():
    """Run all test suites."""
    print("Starting comprehensive test run...")

    # Ensure we're in the right directory
    project_root = Path(__file__).parent.parent
    subprocess.run(["bash", "scripts/quick_cassandra.sh", "check"], cwd=project_root)

    results = {}
    total_start = time.time()

    for suite in TEST_SUITES:
        results[suite["name"]] = run_test_suite(suite)

    total_duration = time.time() - total_start

    # Summary
    print(f"\n{'='*60}")
    print("TEST SUMMARY")
    print(f"{'='*60}")

    passed = sum(1 for v in results.values() if v)
    failed = len(results) - passed

    for suite_name, result in results.items():
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"{suite_name:.<40} {status}")

    print(f"\nTotal: {passed} passed, {failed} failed in {total_duration:.1f}s")

    if failed > 0:
        sys.exit(1)
    else:
        print("\n🎉 All tests passed!")
        sys.exit(0)


if __name__ == "__main__":
    main()
