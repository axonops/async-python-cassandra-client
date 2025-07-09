"""BDD tests for concurrent load handling with real Cassandra."""

import asyncio
import gc
import time

import psutil
import pytest
from pytest_bdd import given, parsers, scenario, then, when

from async_cassandra import AsyncCluster

# Import the cassandra_container fixture
pytest_plugins = ["tests._fixtures.cassandra"]


@scenario("features/concurrent_load.feature", "Thread pool exhaustion prevention")
def test_thread_pool_exhaustion():
    """
    Test thread pool exhaustion prevention.

    What this tests:
    ---------------
    1. Thread pool limits respected
    2. No deadlock under load
    3. Queries complete eventually
    4. Graceful degradation

    Why this matters:
    ----------------
    Thread exhaustion causes:
    - Application hangs
    - Query timeouts
    - Poor user experience

    Must handle high load
    without blocking.
    """
    pass


@scenario("features/concurrent_load.feature", "Memory leak prevention under load")
def test_memory_leak_prevention():
    """
    Test memory leak prevention.

    What this tests:
    ---------------
    1. Memory usage stable
    2. GC works effectively
    3. No continuous growth
    4. Resources cleaned up

    Why this matters:
    ----------------
    Memory leaks fatal:
    - OOM crashes
    - Performance degradation
    - Service instability

    Long-running apps need
    stable memory usage.
    """
    pass


@pytest.fixture
def load_context(cassandra_container):
    """Context for concurrent load tests."""
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
    }


def run_async(coro, loop):
    """Run async code in sync context."""
    return loop.run_until_complete(coro)


# Given steps
@given("a running Cassandra cluster")
def running_cluster(load_context):
    """Verify Cassandra cluster is running."""
    assert load_context["container"].is_running()


@given("async-cassandra configured with default settings")
def default_settings(load_context, event_loop):
    """Configure with default settings."""

    async def _configure():
        cluster = AsyncCluster(
            contact_points=["127.0.0.1"],
            protocol_version=5,
            executor_threads=load_context.get("thread_pool_size", 10),
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

        load_context["cluster"] = cluster
        load_context["session"] = session

    run_async(_configure(), event_loop)


@given(parsers.parse("a configured thread pool of {size:d} threads"))
def configure_thread_pool(size, load_context):
    """Configure thread pool size."""
    load_context["thread_pool_size"] = size


@given("a baseline memory measurement")
def baseline_memory(load_context):
    """Take baseline memory measurement."""
    # Force garbage collection for accurate baseline
    gc.collect()
    process = psutil.Process()
    load_context["metrics"]["memory_baseline"] = process.memory_info().rss / 1024 / 1024  # MB


# When steps
@when(parsers.parse("I submit {count:d} concurrent queries"))
def submit_concurrent_queries(count, load_context, event_loop):
    """Submit many concurrent queries."""

    async def _submit():
        session = load_context["session"]

        # Insert some test data first
        for i in range(100):
            await session.execute(
                "INSERT INTO test_data (id, data) VALUES (%s, %s)", [i, f"test_data_{i}"]
            )

        # Now submit concurrent queries
        async def execute_one(query_id):
            try:
                load_context["metrics"]["queries_sent"] += 1

                result = await session.execute(
                    "SELECT * FROM test_data WHERE id = %s", [query_id % 100]
                )

                load_context["metrics"]["queries_completed"] += 1
                return result
            except Exception as e:
                load_context["metrics"]["queries_failed"] += 1
                load_context["metrics"]["errors"].append(str(e))
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

        load_context["query_results"] = all_results
        load_context["duration"] = time.time() - start

    run_async(_submit(), event_loop)


@when(parsers.re(r"I execute (?P<count>[\d,]+) queries"))
def execute_many_queries(count, load_context, event_loop):
    """Execute many queries."""
    # Convert count string to int, removing commas
    count_int = int(count.replace(",", ""))

    async def _execute():
        session = load_context["session"]

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
            load_context["metrics"]["queries_completed"] += batch_size
            load_context["metrics"]["queries_sent"] += batch_size

            # Measure memory periodically
            if batch_num % 10 == 0:
                gc.collect()  # Force GC to get accurate reading
                process = psutil.Process()
                memory_mb = process.memory_info().rss / 1024 / 1024
                load_context["metrics"]["memory_samples"].append(memory_mb)
                load_context["metrics"]["memory_current"] = memory_mb

    run_async(_execute(), event_loop)


# Then steps
@then("all queries should eventually complete")
def verify_all_complete(load_context):
    """Verify all queries complete."""
    total_processed = (
        load_context["metrics"]["queries_completed"] + load_context["metrics"]["queries_failed"]
    )
    assert total_processed == load_context["metrics"]["queries_sent"]


@then("no deadlock should occur")
def verify_no_deadlock(load_context):
    """Verify no deadlock."""
    # If we completed queries, there was no deadlock
    assert load_context["metrics"]["queries_completed"] > 0

    # Also verify that the duration is reasonable for the number of queries
    # With a thread pool of 10 and proper concurrency, 1000 queries shouldn't take too long
    if load_context.get("duration"):
        avg_time_per_query = load_context["duration"] / load_context["metrics"]["queries_sent"]
        # Average should be under 100ms per query with concurrency
        assert (
            avg_time_per_query < 0.1
        ), f"Queries took too long: {avg_time_per_query:.3f}s per query"


@then("memory usage should remain stable")
def verify_memory_stable(load_context):
    """Verify memory stability."""
    # Check that memory didn't grow excessively
    baseline = load_context["metrics"]["memory_baseline"]
    current = load_context["metrics"]["memory_current"]

    # Allow for some growth but not excessive (e.g., 100MB)
    growth = current - baseline
    assert growth < 100, f"Memory grew by {growth}MB"


@then("response times should degrade gracefully")
def verify_graceful_degradation(load_context):
    """Verify graceful degradation."""
    # With 1000 queries and thread pool of 10, should still complete reasonably
    # Average time per query should be reasonable
    avg_time = load_context["duration"] / 1000
    assert avg_time < 1.0  # Less than 1 second per query average


@then("memory usage should not grow continuously")
def verify_no_memory_leak(load_context):
    """Verify no memory leak."""
    samples = load_context["metrics"]["memory_samples"]
    if len(samples) < 2:
        return  # Not enough samples

    # Check that memory is not monotonically increasing
    # Allow for some fluctuation but overall should be stable
    baseline = samples[0]
    max_growth = max(s - baseline for s in samples)

    # Should not grow more than 50MB over the test
    assert max_growth < 50, f"Memory grew by {max_growth}MB"


@then("garbage collection should work effectively")
def verify_gc_works(load_context):
    """Verify GC effectiveness."""
    # We forced GC during the test, verify it helped
    assert len(load_context["metrics"]["memory_samples"]) > 0

    # Check that memory growth is controlled
    samples = load_context["metrics"]["memory_samples"]
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
def verify_no_warnings(load_context):
    """Verify no resource warnings."""
    # Check for common warnings in errors
    warnings = [e for e in load_context["metrics"]["errors"] if "warning" in e.lower()]
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
def verify_consistent_performance(load_context):
    """Verify consistent performance."""
    # Most queries should succeed
    if load_context["metrics"]["queries_sent"] > 0:
        success_rate = (
            load_context["metrics"]["queries_completed"] / load_context["metrics"]["queries_sent"]
        )
        assert success_rate > 0.95  # 95% success rate
    else:
        # If no queries were sent, check that completed count matches
        assert (
            load_context["metrics"]["queries_completed"] >= 100
        )  # At least some queries should have completed


# Cleanup
@pytest.fixture(autouse=True)
def cleanup_after_test(load_context, event_loop):
    """Cleanup resources after each test."""
    yield

    async def _cleanup():
        if load_context.get("session"):
            await load_context["session"].close()
        if load_context.get("cluster"):
            await load_context["cluster"].shutdown()

    if load_context.get("session") or load_context.get("cluster"):
        run_async(_cleanup(), event_loop)
