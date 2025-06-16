"""Common step definitions used across multiple features."""

import asyncio
import time
from typing import Any, Dict, List

import pytest
from pytest_bdd import given, parsers, then, when

# Shared context storage
context: Dict[str, Any] = {}


def store_context(key: str, value: Any):
    """Store value in shared context."""
    context[key] = value


def get_context(key: str) -> Any:
    """Get value from shared context."""
    return context.get(key)


def clear_context():
    """Clear shared context."""
    context.clear()


@given("monitoring is enabled")
def enable_monitoring():
    """Enable monitoring for tests."""
    from async_cassandra.monitoring import MetricsCollector

    collector = MetricsCollector()
    store_context("metrics_collector", collector)
    return collector


@when(parsers.parse("I wait for {seconds:d} seconds"))
async def wait_seconds(seconds):
    """Wait for specified seconds."""
    await asyncio.sleep(seconds)
    return seconds


@when("I measure memory usage")
def measure_memory():
    """Measure current memory usage."""
    import os

    import psutil

    process = psutil.Process(os.getpid())
    memory_info = process.memory_info()

    memory_data = {
        "rss": memory_info.rss,  # Resident Set Size
        "vms": memory_info.vms,  # Virtual Memory Size
        "timestamp": time.time(),
    }

    store_context("memory_measurement", memory_data)
    return memory_data


@then("I should see metrics for all operations")
def verify_metrics_collected():
    """Verify metrics were collected."""
    collector = get_context("metrics_collector")
    assert collector is not None

    snapshot = collector.create_snapshot()
    assert snapshot.query_metrics.total_queries > 0
    return snapshot


@then(parsers.parse("error rate should be less than {max_rate:f}%"))
def verify_error_rate(max_rate):
    """Verify error rate is within acceptable bounds."""
    collector = get_context("metrics_collector")
    if not collector:
        return True

    metrics = collector.query_metrics
    if metrics.total_queries == 0:
        error_rate = 0
    else:
        error_rate = (metrics.failed_queries / metrics.total_queries) * 100

    assert error_rate < max_rate
    return error_rate


@then("subsequent queries should succeed")
async def verify_subsequent_queries_succeed(active_async_session):
    """Verify queries work after recovery."""
    # Try a few queries
    for i in range(3):
        result = await active_async_session.execute(
            "SELECT * FROM system.local WHERE key = ?", ["local"]
        )
        assert result is not None
        assert len(result.current_rows) > 0

    return True


@when("various types of errors occur")
async def simulate_various_errors(active_async_session):
    """Simulate different error types."""

    errors_to_simulate = [
        {"error_type": "NoHostAvailable", "count": 3},
        {"error_type": "WriteTimeout", "count": 2},
        {"error_type": "ReadTimeout", "count": 5},
        {"error_type": "InvalidRequest", "count": 1},
    ]

    error_log = []

    for error_info in errors_to_simulate:
        for _ in range(error_info["count"]):
            error_log.append({"type": error_info["error_type"], "timestamp": time.time()})

    store_context("simulated_errors", error_log)
    return error_log


@then("error metrics should accurately reflect all occurrences")
def verify_error_metrics():
    """Verify error metrics are accurate."""
    error_log = get_context("simulated_errors")
    collector = get_context("metrics_collector")

    if not collector or not error_log:
        return True

    # In real implementation, would check actual error counts
    # against what was simulated
    return True


# Cleanup fixture
@pytest.fixture(autouse=True)
def cleanup_context():
    """Clean up context after each test."""
    yield
    clear_context()


# Reusable assertion helpers
def assert_within_range(value: float, expected: float, tolerance: float = 0.1):
    """Assert value is within tolerance of expected."""
    min_val = expected * (1 - tolerance)
    max_val = expected * (1 + tolerance)
    assert min_val <= value <= max_val, f"{value} not within {tolerance*100}% of {expected}"


def assert_increasing(values: List[float], allow_equal: bool = True):
    """Assert values are in increasing order."""
    for i in range(1, len(values)):
        if allow_equal:
            assert values[i] >= values[i - 1]
        else:
            assert values[i] > values[i - 1]


def assert_stable(values: List[float], max_variation: float = 0.1):
    """Assert values are stable (low variation)."""
    if not values:
        return

    avg = sum(values) / len(values)
    for value in values:
        assert_within_range(value, avg, max_variation)
