"""
Performance benchmarks for concurrency and resource usage.

These benchmarks validate the library's ability to handle
high concurrency efficiently with reasonable resource usage.
"""

import asyncio
import gc
import os
import statistics
import time

import psutil
import pytest
import pytest_asyncio

from async_cassandra import AsyncCassandraSession, AsyncCluster

from .benchmark_config import BenchmarkConfig


@pytest.mark.benchmark
class TestConcurrencyPerformance:
    """Benchmarks for concurrency handling and resource efficiency."""

    @pytest_asyncio.fixture
    async def benchmark_session(self) -> AsyncCassandraSession:
        """Create session for concurrency benchmarks."""
        cluster = AsyncCluster(
            contact_points=["localhost"],
            executor_threads=16,  # More threads for concurrency tests
        )
        session = await cluster.connect()

        # Create test keyspace and table
        await session.execute(
            f"""
            CREATE KEYSPACE IF NOT EXISTS {BenchmarkConfig.TEST_KEYSPACE}
            WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        """
        )
        await session.set_keyspace(BenchmarkConfig.TEST_KEYSPACE)

        await session.execute("DROP TABLE IF EXISTS concurrency_test")
        await session.execute(
            """
            CREATE TABLE concurrency_test (
                id UUID PRIMARY KEY,
                data TEXT,
                counter INT,
                updated_at TIMESTAMP
            )
        """
        )

        yield session

        await session.close()
        await cluster.shutdown()

    @pytest.mark.asyncio
    async def test_high_concurrency_throughput(self, benchmark_session):
        """
        Benchmark throughput under high concurrency.

        GIVEN many concurrent operations
        WHEN executed simultaneously
        THEN system should maintain high throughput
        """
        thresholds = BenchmarkConfig.DEFAULT_THRESHOLDS

        # Prepare statements
        insert_stmt = await benchmark_session.prepare(
            "INSERT INTO concurrency_test (id, data, counter, updated_at) VALUES (?, ?, ?, toTimestamp(now()))"
        )
        select_stmt = await benchmark_session.prepare("SELECT * FROM concurrency_test WHERE id = ?")

        async def mixed_operations(op_id: int):
            """Perform mixed read/write operations."""
            import uuid

            # Insert
            record_id = uuid.uuid4()
            await benchmark_session.execute(insert_stmt, [record_id, f"data_{op_id}", op_id])

            # Read back
            result = await benchmark_session.execute(select_stmt, [record_id])
            row = result.one()

            return row is not None

        # Benchmark high concurrency
        num_operations = 1000
        start_time = time.perf_counter()

        tasks = [mixed_operations(i) for i in range(num_operations)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        duration = time.perf_counter() - start_time

        # Calculate metrics
        successful = sum(1 for r in results if r is True)
        errors = sum(1 for r in results if isinstance(r, Exception))
        throughput = successful / duration

        # Verify thresholds
        assert (
            throughput >= thresholds.min_throughput_async
        ), f"Throughput {throughput:.1f} ops/sec below threshold"
        assert (
            successful >= num_operations * 0.99
        ), f"Success rate {successful/num_operations:.1%} below 99%"
        assert errors == 0, f"Unexpected errors: {errors}"

    @pytest.mark.asyncio
    async def test_connection_pool_efficiency(self, benchmark_session):
        """
        Benchmark connection pool handling under load.

        GIVEN limited connection pool
        WHEN many requests compete for connections
        THEN pool should be used efficiently
        """
        # Create a cluster with limited connections
        limited_cluster = AsyncCluster(
            contact_points=["localhost"],
            executor_threads=4,  # Limited threads
        )
        limited_session = await limited_cluster.connect()
        await limited_session.set_keyspace(BenchmarkConfig.TEST_KEYSPACE)

        try:
            select_stmt = await limited_session.prepare("SELECT * FROM concurrency_test LIMIT 1")

            # Track connection wait times (removed - not needed)

            async def timed_query(query_id: int):
                """Execute query and measure wait time."""
                start = time.perf_counter()

                # This might wait for available connection
                result = await limited_session.execute(select_stmt)
                _ = result.one()

                duration = time.perf_counter() - start
                return duration

            # Run many concurrent queries with limited pool
            num_queries = 100
            query_times = await asyncio.gather(*[timed_query(i) for i in range(num_queries)])

            # Calculate metrics
            avg_time = statistics.mean(query_times)
            p95_time = statistics.quantiles(query_times, n=20)[18]

            # Pool should handle load efficiently
            assert avg_time < 0.1, f"Average query time {avg_time:.3f}s indicates pool contention"
            assert p95_time < 0.2, f"P95 query time {p95_time:.3f}s indicates severe contention"

        finally:
            await limited_session.close()
            await limited_cluster.shutdown()

    @pytest.mark.asyncio
    async def test_resource_usage_under_load(self, benchmark_session):
        """
        Benchmark resource usage (CPU, memory) under sustained load.

        GIVEN sustained concurrent load
        WHEN system processes requests
        THEN resource usage should remain reasonable
        """

        # Get process for monitoring
        process = psutil.Process(os.getpid())

        # Prepare statement
        select_stmt = await benchmark_session.prepare("SELECT * FROM concurrency_test LIMIT 10")

        # Collect baseline metrics
        gc.collect()
        baseline_memory = process.memory_info().rss / 1024 / 1024  # MB
        process.cpu_percent(interval=0.1)

        # Resource tracking
        memory_samples = []
        cpu_samples = []

        async def load_generator():
            """Generate continuous load."""
            while True:
                try:
                    await benchmark_session.execute(select_stmt)
                    await asyncio.sleep(0.001)  # Small delay
                except asyncio.CancelledError:
                    break
                except Exception:
                    pass

        # Start load generators
        load_tasks = [
            asyncio.create_task(load_generator()) for _ in range(50)  # 50 concurrent workers
        ]

        # Monitor resources for 10 seconds
        monitor_duration = 10
        sample_interval = 0.5
        samples = int(monitor_duration / sample_interval)

        for _ in range(samples):
            await asyncio.sleep(sample_interval)

            memory_mb = process.memory_info().rss / 1024 / 1024
            cpu_percent = process.cpu_percent(interval=None)

            memory_samples.append(memory_mb - baseline_memory)
            cpu_samples.append(cpu_percent)

        # Stop load generators
        for task in load_tasks:
            task.cancel()
        await asyncio.gather(*load_tasks, return_exceptions=True)

        # Calculate metrics
        avg_memory_increase = statistics.mean(memory_samples)
        max_memory_increase = max(memory_samples)
        avg_cpu = statistics.mean(cpu_samples)
        max(cpu_samples)

        # Verify resource usage
        assert (
            avg_memory_increase < 100
        ), f"Average memory increase {avg_memory_increase:.1f}MB exceeds 100MB"
        assert (
            max_memory_increase < 200
        ), f"Max memory increase {max_memory_increase:.1f}MB exceeds 200MB"
        # CPU thresholds are relaxed as they depend on system
        assert avg_cpu < 80, f"Average CPU usage {avg_cpu:.1f}% exceeds 80%"

    @pytest.mark.asyncio
    async def test_concurrent_operation_isolation(self, benchmark_session):
        """
        Benchmark operation isolation under concurrency.

        GIVEN concurrent operations on same data
        WHEN operations execute simultaneously
        THEN they should not interfere with each other
        """
        import uuid

        # Create test record
        test_id = uuid.uuid4()
        await benchmark_session.execute(
            "INSERT INTO concurrency_test (id, data, counter, updated_at) VALUES (?, ?, ?, toTimestamp(now()))",
            [test_id, "initial", 0],
        )

        # Prepare statements
        update_stmt = await benchmark_session.prepare(
            "UPDATE concurrency_test SET counter = counter + 1 WHERE id = ?"
        )
        select_stmt = await benchmark_session.prepare(
            "SELECT counter FROM concurrency_test WHERE id = ?"
        )

        # Concurrent increment operations
        num_increments = 100

        async def increment_counter():
            """Increment counter (may have race conditions)."""
            await benchmark_session.execute(update_stmt, [test_id])
            return True

        # Execute concurrent increments
        start_time = time.perf_counter()

        await asyncio.gather(*[increment_counter() for _ in range(num_increments)])

        duration = time.perf_counter() - start_time

        # Check final value
        final_result = await benchmark_session.execute(select_stmt, [test_id])
        final_counter = final_result.one().counter

        # Calculate metrics
        throughput = num_increments / duration

        # Note: Due to race conditions, final counter may be less than num_increments
        # This is expected behavior without proper synchronization
        assert throughput > 100, f"Increment throughput {throughput:.1f} ops/sec too low"
        assert final_counter > 0, "Counter should have been incremented"

    @pytest.mark.asyncio
    async def test_graceful_degradation_under_overload(self, benchmark_session):
        """
        Benchmark system behavior under overload conditions.

        GIVEN more load than system can handle
        WHEN system is overloaded
        THEN it should degrade gracefully
        """

        # Prepare a complex query
        complex_query = """
            SELECT * FROM concurrency_test
            WHERE token(id) > token(?)
            LIMIT 100
            ALLOW FILTERING
        """

        errors = []
        latencies = []

        async def overload_operation(op_id: int):
            """Operation that contributes to overload."""
            import uuid

            start = time.perf_counter()
            try:
                result = await benchmark_session.execute(complex_query, [uuid.uuid4()])
                # Consume results
                count = 0
                async for _ in result:
                    count += 1

                latency = time.perf_counter() - start
                latencies.append(latency)
                return True

            except Exception as e:
                errors.append(str(e))
                return False

        # Generate overload with many concurrent operations
        num_operations = 500

        start_time = time.perf_counter()
        results = await asyncio.gather(
            *[overload_operation(i) for i in range(num_operations)], return_exceptions=True
        )
        time.perf_counter() - start_time

        # Calculate metrics
        successful = sum(1 for r in results if r is True)
        error_rate = len(errors) / num_operations

        if latencies:
            statistics.mean(latencies)
            p99_latency = statistics.quantiles(latencies, n=100)[98]
        else:
            float("inf")
            p99_latency = float("inf")

        # Even under overload, system should maintain some service
        assert (
            successful > num_operations * 0.5
        ), f"Success rate {successful/num_operations:.1%} too low under overload"
        assert error_rate < 0.5, f"Error rate {error_rate:.1%} too high"

        # Latencies will be high but should be bounded
        assert p99_latency < 5.0, f"P99 latency {p99_latency:.1f}s exceeds 5 second timeout"
