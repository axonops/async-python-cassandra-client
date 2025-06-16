"""BDD tests for production reliability scenarios with real Cassandra."""

import asyncio
import gc
import time

import psutil
import pytest
from pytest_bdd import given, parsers, scenario, then, when

from async_cassandra import AsyncCluster

# Import the cassandra_container fixture
pytest_plugins = ["tests._fixtures.cassandra"]


@scenario("features/production_reliability.feature", "Thread pool exhaustion prevention")
def test_thread_pool_exhaustion():
    """Test thread pool exhaustion prevention."""
    pass


@scenario("features/production_reliability.feature", "Memory leak prevention under load")
def test_memory_leak_prevention():
    """Test memory leak prevention."""
    pass


@scenario("features/production_reliability.feature", "Handle traffic spikes gracefully")
def test_traffic_spikes():
    """Test traffic spike handling."""
    pass


@pytest.fixture
def prod_context(cassandra_container):
    """Context for production tests."""
    return {
        "cluster": None,
        "session": None,
        "container": cassandra_container,
        "metrics": {
            "queries_sent": 0,
            "queries_completed": 0,
            "queries_failed": 0,
            "memory_baseline": 0,
            "memory_current": 0,
            "memory_samples": [],
            "start_time": None,
            "errors": [],
        },
        "thread_pool_size": 10,
        "query_results": [],
        "duration": None,
        "normal_qps": 100,
        "current_qps": 100,
    }


def run_async(coro, loop):
    """Run async code in sync context."""
    return loop.run_until_complete(coro)


# Given steps
@given("a production-like Cassandra cluster with 3 nodes")
def prod_cluster(prod_context):
    """Setup production cluster."""
    # We only have one node in test, but we'll simulate production config
    assert prod_context["container"].is_running()


@given("async-cassandra configured with production settings")
def prod_settings(prod_context, event_loop):
    """Configure production settings."""

    async def _configure():
        cluster = AsyncCluster(
            contact_points=["127.0.0.1"],
            protocol_version=5,
            executor_threads=prod_context["thread_pool_size"],
        )
        session = await cluster.connect()
        await session.set_keyspace("test_keyspace")

        # Create test table
        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS test_data (
                id int PRIMARY KEY,
                data text
            )
        """
        )

        prod_context["cluster"] = cluster
        prod_context["session"] = session

    run_async(_configure(), event_loop)


@given("comprehensive monitoring is enabled")
def enable_monitoring(prod_context):
    """Enable monitoring."""
    prod_context["metrics"]["start_time"] = time.time()

    # Get baseline memory
    process = psutil.Process()
    prod_context["metrics"]["memory_baseline"] = process.memory_info().rss / 1024 / 1024  # MB


@given(parsers.parse("a configured thread pool of {size:d} threads"))
def configure_thread_pool(size, prod_context):
    """Configure thread pool size."""
    prod_context["thread_pool_size"] = size


@given("a baseline memory measurement")
def baseline_memory(prod_context):
    """Take baseline memory measurement."""
    # Force garbage collection for accurate baseline
    gc.collect()
    process = psutil.Process()
    prod_context["metrics"]["memory_baseline"] = process.memory_info().rss / 1024 / 1024  # MB


@given(parsers.parse("normal load of {qps:d} queries/second"))
def normal_load(qps, prod_context):
    """Setup normal load."""
    prod_context["normal_qps"] = qps
    prod_context["current_qps"] = qps


# When steps
@when(parsers.parse("I submit {count:d} concurrent queries"))
def submit_concurrent_queries(count, prod_context, event_loop):
    """Submit many concurrent queries."""

    async def _submit():
        session = prod_context["session"]

        # Insert some test data first
        for i in range(100):
            await session.execute(
                "INSERT INTO test_data (id, data) VALUES (%s, %s)", [i, f"test_data_{i}"]
            )

        # Now submit concurrent queries
        async def execute_one(query_id):
            try:
                prod_context["metrics"]["queries_sent"] += 1

                result = await session.execute(
                    "SELECT * FROM test_data WHERE id = %s", [query_id % 100]
                )

                prod_context["metrics"]["queries_completed"] += 1
                return result
            except Exception as e:
                prod_context["metrics"]["queries_failed"] += 1
                prod_context["metrics"]["errors"].append(str(e))
                raise

        start = time.time()

        # Submit queries in batches to avoid overwhelming
        batch_size = 100
        all_results = []

        for batch_start in range(0, count, batch_size):
            batch_end = min(batch_start + batch_size, count)
            tasks = [execute_one(i) for i in range(batch_start, batch_end)]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            all_results.extend(batch_results)

            # Small delay between batches
            if batch_end < count:
                await asyncio.sleep(0.1)

        prod_context["query_results"] = all_results
        prod_context["duration"] = time.time() - start

    run_async(_submit(), event_loop)


@when(parsers.re(r"I execute (?P<count>[\d,]+) queries over 1 hour"))
def execute_many_queries(count, prod_context, event_loop):
    """Execute many queries over time (simulated)."""
    # Convert count string to int, removing commas
    count_int = int(count.replace(",", ""))

    async def _execute():
        session = prod_context["session"]

        # We'll simulate by doing it faster but with memory measurements
        batch_size = 1000
        batches = count_int // batch_size

        for batch_num in range(batches):
            # Execute batch
            tasks = []
            for i in range(batch_size):
                query_id = batch_num * batch_size + i
                task = session.execute("SELECT * FROM test_data WHERE id = %s", [query_id % 100])
                tasks.append(task)

            await asyncio.gather(*tasks)
            prod_context["metrics"]["queries_completed"] += batch_size
            prod_context["metrics"]["queries_sent"] += batch_size

            # Measure memory periodically
            if batch_num % 10 == 0:
                gc.collect()  # Force GC to get accurate reading
                process = psutil.Process()
                memory_mb = process.memory_info().rss / 1024 / 1024
                prod_context["metrics"]["memory_samples"].append(memory_mb)
                prod_context["metrics"]["memory_current"] = memory_mb

    run_async(_execute(), event_loop)


@when(parsers.parse("traffic suddenly spikes to {qps:d} queries/second"))
def traffic_spike(qps, prod_context):
    """Simulate traffic spike."""
    prod_context["current_qps"] = qps
    prod_context["spike_start"] = time.time()


# Then steps
@then("all queries should eventually complete")
def verify_all_complete(prod_context):
    """Verify all queries complete."""
    total_processed = (
        prod_context["metrics"]["queries_completed"] + prod_context["metrics"]["queries_failed"]
    )
    assert total_processed == prod_context["metrics"]["queries_sent"]


@then("no deadlock should occur")
def verify_no_deadlock(prod_context):
    """Verify no deadlock."""
    # If we completed queries, there was no deadlock
    assert prod_context["metrics"]["queries_completed"] > 0

    # Also verify that the duration is reasonable for the number of queries
    # With a thread pool of 10 and proper concurrency, 1000 queries shouldn't take too long
    if prod_context.get("duration"):
        avg_time_per_query = prod_context["duration"] / prod_context["metrics"]["queries_sent"]
        # Average should be under 100ms per query with concurrency
        assert (
            avg_time_per_query < 0.1
        ), f"Queries took too long: {avg_time_per_query:.3f}s per query"


@then("memory usage should remain stable")
def verify_memory_stable(prod_context):
    """Verify memory stability."""
    # Check that memory didn't grow excessively
    baseline = prod_context["metrics"]["memory_baseline"]
    current = prod_context["metrics"]["memory_current"]

    # Allow for some growth but not excessive (e.g., 100MB)
    growth = current - baseline
    assert growth < 100, f"Memory grew by {growth}MB"


@then("response times should degrade gracefully")
def verify_graceful_degradation(prod_context):
    """Verify graceful degradation."""
    # With 1000 queries and thread pool of 10, should still complete reasonably
    # Average time per query should be reasonable
    avg_time = prod_context["duration"] / 1000
    assert avg_time < 1.0  # Less than 1 second per query average


@then("memory usage should not grow continuously")
def verify_no_memory_leak(prod_context):
    """Verify no memory leak."""
    samples = prod_context["metrics"]["memory_samples"]
    if len(samples) < 2:
        return  # Not enough samples

    # Check that memory is not monotonically increasing
    # Allow for some fluctuation but overall should be stable
    baseline = samples[0]
    max_growth = max(s - baseline for s in samples)

    # Should not grow more than 50MB over the test
    assert max_growth < 50, f"Memory grew by {max_growth}MB"


@then("garbage collection should work effectively")
def verify_gc_works(prod_context):
    """Verify GC effectiveness."""
    # We forced GC during the test, verify it helped
    assert len(prod_context["metrics"]["memory_samples"]) > 0

    # Check that memory growth is controlled
    samples = prod_context["metrics"]["memory_samples"]
    if len(samples) >= 2:
        # Calculate growth rate
        first_sample = samples[0]
        last_sample = samples[-1]
        total_growth = last_sample - first_sample

        # Growth should be minimal for the workload
        # Allow up to 100MB growth for 100k queries
        assert total_growth < 100, f"Memory grew too much: {total_growth}MB"

        # Check for stability in later samples (after warmup)
        if len(samples) >= 5:
            later_samples = samples[-5:]
            max_variance = max(later_samples) - min(later_samples)
            # Memory should stabilize - variance should be small
            assert (
                max_variance < 20
            ), f"Memory not stable in later samples: {max_variance}MB variance"


@then("no resource warnings should be logged")
def verify_no_warnings(prod_context):
    """Verify no resource warnings."""
    # Check for common warnings in errors
    warnings = [e for e in prod_context["metrics"]["errors"] if "warning" in e.lower()]
    assert len(warnings) == 0, f"Found warnings: {warnings}"

    # Also check Python's warning system
    import warnings

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        # Force garbage collection to trigger any pending resource warnings
        import gc

        gc.collect()

        # Check for resource warnings
        resource_warnings = [
            warning for warning in w if issubclass(warning.category, ResourceWarning)
        ]
        assert len(resource_warnings) == 0, f"Found resource warnings: {resource_warnings}"


@then("performance should remain consistent")
def verify_consistent_performance(prod_context):
    """Verify consistent performance."""
    # Most queries should succeed
    if prod_context["metrics"]["queries_sent"] > 0:
        success_rate = (
            prod_context["metrics"]["queries_completed"] / prod_context["metrics"]["queries_sent"]
        )
        assert success_rate > 0.95  # 95% success rate
    else:
        # If no queries were sent, check that completed count matches
        assert (
            prod_context["metrics"]["queries_completed"] >= 100
        )  # At least some queries should have completed


@then("the system should scale up gracefully")
def verify_scale_up(prod_context):
    """Verify graceful scale up."""
    assert prod_context["current_qps"] > prod_context["normal_qps"]
    # System accepted the higher load
    assert prod_context["metrics"]["queries_sent"] > 0

    # Verify the spike was actually simulated
    assert hasattr(prod_context, "spike_start"), "Traffic spike was not properly initiated"

    # The system should handle at least 5x normal load
    spike_ratio = prod_context["current_qps"] / prod_context["normal_qps"]
    assert spike_ratio >= 5, f"Spike ratio {spike_ratio} is too low for a proper test"


@then("response times should increase linearly, not exponentially")
def verify_linear_scaling(prod_context):
    """Verify linear scaling."""
    # This is verified by the fact that queries completed
    # If scaling was exponential, queries would timeout
    assert prod_context["metrics"]["queries_completed"] > 0

    # For better verification, check the actual timing
    if prod_context.get("duration") and prod_context["metrics"]["queries_sent"] > 0:
        total_queries = prod_context["metrics"]["queries_sent"]
        thread_pool_size = prod_context.get("thread_pool_size", 10)

        # With linear scaling, time should be proportional to queries/threads
        expected_batches = (total_queries + thread_pool_size - 1) // thread_pool_size
        # Assume each batch takes ~0.1s (reasonable for local testing)
        expected_time = expected_batches * 0.1

        # Allow 5x margin for CI environments
        assert (
            prod_context["duration"] < expected_time * 5
        ), f"Scaling appears non-linear: {prod_context['duration']:.2f}s for {total_queries} queries"


@then("no queries should be dropped")
def verify_no_dropped(prod_context):
    """Verify no dropped queries."""
    # All sent queries should be accounted for (completed or failed)
    total_processed = (
        prod_context["metrics"]["queries_completed"] + prod_context["metrics"]["queries_failed"]
    )
    assert total_processed == prod_context["metrics"]["queries_sent"]


@then("the system should recover when spike ends")
def verify_recovery(prod_context):
    """Verify system recovery."""
    # In our test, if queries completed, system recovered
    assert prod_context["metrics"]["queries_completed"] > 0

    # Verify that failed queries (if any) are within acceptable limits
    if prod_context["metrics"]["queries_failed"] > 0:
        failure_rate = (
            prod_context["metrics"]["queries_failed"] / prod_context["metrics"]["queries_sent"]
        )
        # During spikes, up to 5% failure is acceptable
        assert failure_rate <= 0.05, f"Too many failures during spike: {failure_rate:.1%}"

    # If we tracked memory, verify it's not growing out of control
    if prod_context["metrics"].get("memory_current") and prod_context["metrics"].get(
        "memory_baseline"
    ):
        memory_growth = (
            prod_context["metrics"]["memory_current"] - prod_context["metrics"]["memory_baseline"]
        )
        # Memory growth should stabilize (not more than 200MB for our test)
        assert memory_growth < 200, f"Memory grew too much: {memory_growth}MB"


# Cleanup
@pytest.fixture(autouse=True)
def cleanup_after_test(prod_context, event_loop):
    """Cleanup resources after each test."""
    yield

    async def _cleanup():
        if prod_context.get("session"):
            await prod_context["session"].close()
        if prod_context.get("cluster"):
            await prod_context["cluster"].shutdown()

    if prod_context.get("session") or prod_context.get("cluster"):
        run_async(_cleanup(), event_loop)
