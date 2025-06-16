"""
Performance benchmarks for query operations.

These benchmarks measure latency, throughput, and resource usage
for various query patterns.
"""

import asyncio
import statistics
import time

import pytest
import pytest_asyncio

from async_cassandra import AsyncCassandraSession, AsyncCluster

from .benchmark_config import BenchmarkConfig


@pytest.mark.benchmark
class TestQueryPerformance:
    """Benchmarks for query performance."""

    @pytest_asyncio.fixture
    async def benchmark_session(self) -> AsyncCassandraSession:
        """Create session for benchmarking."""
        cluster = AsyncCluster(
            contact_points=["localhost"],
            executor_threads=8,  # Optimized for benchmarks
        )
        session = await cluster.connect()

        # Create benchmark keyspace and table
        await session.execute(
            f"""
            CREATE KEYSPACE IF NOT EXISTS {BenchmarkConfig.TEST_KEYSPACE}
            WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        """
        )
        await session.set_keyspace(BenchmarkConfig.TEST_KEYSPACE)

        await session.execute(f"DROP TABLE IF EXISTS {BenchmarkConfig.TEST_TABLE}")
        await session.execute(
            f"""
            CREATE TABLE {BenchmarkConfig.TEST_TABLE} (
                id INT PRIMARY KEY,
                data TEXT,
                value DOUBLE,
                created_at TIMESTAMP
            )
        """
        )

        # Insert test data
        insert_stmt = await session.prepare(
            f"INSERT INTO {BenchmarkConfig.TEST_TABLE} (id, data, value, created_at) VALUES (?, ?, ?, toTimestamp(now()))"
        )

        for i in range(BenchmarkConfig.LARGE_DATASET_SIZE):
            await session.execute(insert_stmt, [i, f"test_data_{i}", i * 1.5])

        yield session

        await session.close()
        await cluster.shutdown()

    @pytest.mark.asyncio
    async def test_single_query_latency(self, benchmark_session):
        """
        Benchmark single query latency.

        GIVEN a simple query
        WHEN executed individually
        THEN latency should be within acceptable thresholds
        """
        thresholds = BenchmarkConfig.DEFAULT_THRESHOLDS

        # Prepare statement
        select_stmt = await benchmark_session.prepare(
            f"SELECT * FROM {BenchmarkConfig.TEST_TABLE} WHERE id = ?"
        )

        # Warm up
        for i in range(10):
            await benchmark_session.execute(select_stmt, [i])

        # Benchmark
        latencies = []
        errors = 0

        for i in range(100):
            start = time.perf_counter()
            try:
                result = await benchmark_session.execute(select_stmt, [i % 1000])
                _ = result.one()  # Force result materialization
                latency = time.perf_counter() - start
                latencies.append(latency)
            except Exception:
                errors += 1

        # Calculate metrics
        avg_latency = statistics.mean(latencies)
        p95_latency = statistics.quantiles(latencies, n=20)[18]  # 95th percentile
        p99_latency = statistics.quantiles(latencies, n=100)[98]  # 99th percentile
        max_latency = max(latencies)

        # Verify thresholds
        assert (
            avg_latency < thresholds.single_query_avg
        ), f"Average latency {avg_latency:.3f}s exceeds threshold {thresholds.single_query_avg}s"
        assert (
            p95_latency < thresholds.single_query_p95
        ), f"P95 latency {p95_latency:.3f}s exceeds threshold {thresholds.single_query_p95}s"
        assert (
            p99_latency < thresholds.single_query_p99
        ), f"P99 latency {p99_latency:.3f}s exceeds threshold {thresholds.single_query_p99}s"
        assert (
            max_latency < thresholds.single_query_max
        ), f"Max latency {max_latency:.3f}s exceeds threshold {thresholds.single_query_max}s"
        assert errors == 0, f"Query errors occurred: {errors}"

    @pytest.mark.asyncio
    async def test_concurrent_query_throughput(self, benchmark_session):
        """
        Benchmark concurrent query throughput.

        GIVEN multiple concurrent queries
        WHEN executed with asyncio
        THEN throughput should meet minimum requirements
        """
        thresholds = BenchmarkConfig.DEFAULT_THRESHOLDS

        # Prepare statement
        select_stmt = await benchmark_session.prepare(
            f"SELECT * FROM {BenchmarkConfig.TEST_TABLE} WHERE id = ?"
        )

        async def execute_query(query_id: int):
            """Execute a single query."""
            try:
                result = await benchmark_session.execute(select_stmt, [query_id % 1000])
                _ = result.one()
                return True, time.perf_counter()
            except Exception:
                return False, time.perf_counter()

        # Benchmark concurrent execution
        num_queries = 1000
        start_time = time.perf_counter()

        tasks = [execute_query(i) for i in range(num_queries)]
        results = await asyncio.gather(*tasks)

        end_time = time.perf_counter()
        duration = end_time - start_time

        # Calculate metrics
        successful = sum(1 for success, _ in results if success)
        throughput = successful / duration

        # Verify thresholds
        assert (
            throughput >= thresholds.min_throughput_async
        ), f"Throughput {throughput:.1f} qps below threshold {thresholds.min_throughput_async} qps"
        assert (
            successful >= num_queries * 0.99
        ), f"Success rate {successful/num_queries:.1%} below 99%"

    @pytest.mark.asyncio
    async def test_async_vs_sync_performance(self, benchmark_session):
        """
        Benchmark async performance advantage over sync-style execution.

        GIVEN the same workload
        WHEN executed async vs sequentially
        THEN async should show significant performance improvement
        """
        thresholds = BenchmarkConfig.DEFAULT_THRESHOLDS

        # Prepare statement
        select_stmt = await benchmark_session.prepare(
            f"SELECT * FROM {BenchmarkConfig.TEST_TABLE} WHERE id = ?"
        )

        num_queries = 100

        # Benchmark sequential execution
        sync_start = time.perf_counter()
        for i in range(num_queries):
            result = await benchmark_session.execute(select_stmt, [i])
            _ = result.one()
        sync_duration = time.perf_counter() - sync_start
        sync_throughput = num_queries / sync_duration

        # Benchmark concurrent execution
        async_start = time.perf_counter()
        tasks = []
        for i in range(num_queries):
            task = benchmark_session.execute(select_stmt, [i])
            tasks.append(task)
        await asyncio.gather(*tasks)
        async_duration = time.perf_counter() - async_start
        async_throughput = num_queries / async_duration

        # Calculate speedup
        speedup = async_throughput / sync_throughput

        # Verify thresholds
        assert (
            speedup >= thresholds.concurrency_speedup_factor
        ), f"Async speedup {speedup:.1f}x below threshold {thresholds.concurrency_speedup_factor}x"
        assert (
            async_throughput >= thresholds.min_throughput_async
        ), f"Async throughput {async_throughput:.1f} qps below threshold"

    @pytest.mark.asyncio
    async def test_query_latency_under_load(self, benchmark_session):
        """
        Benchmark query latency under sustained load.

        GIVEN continuous query load
        WHEN system is under stress
        THEN latency should remain acceptable
        """
        thresholds = BenchmarkConfig.DEFAULT_THRESHOLDS

        # Prepare statement
        select_stmt = await benchmark_session.prepare(
            f"SELECT * FROM {BenchmarkConfig.TEST_TABLE} WHERE id = ?"
        )

        latencies = []
        errors = 0

        async def query_worker(worker_id: int, duration: float):
            """Worker that continuously executes queries."""
            nonlocal errors
            worker_latencies = []
            end_time = time.perf_counter() + duration

            while time.perf_counter() < end_time:
                start = time.perf_counter()
                try:
                    query_id = int(time.time() * 1000) % 1000
                    result = await benchmark_session.execute(select_stmt, [query_id])
                    _ = result.one()
                    latency = time.perf_counter() - start
                    worker_latencies.append(latency)
                except Exception:
                    errors += 1

                # Small delay to prevent overwhelming
                await asyncio.sleep(0.001)

            return worker_latencies

        # Run workers concurrently for sustained load
        num_workers = 50
        test_duration = 10  # seconds

        worker_tasks = [query_worker(i, test_duration) for i in range(num_workers)]

        worker_results = await asyncio.gather(*worker_tasks)

        # Aggregate all latencies
        for worker_latencies in worker_results:
            latencies.extend(worker_latencies)

        # Calculate metrics
        avg_latency = statistics.mean(latencies)
        statistics.quantiles(latencies, n=20)[18]
        p99_latency = statistics.quantiles(latencies, n=100)[98]
        error_rate = errors / len(latencies) if latencies else 1.0

        # Verify thresholds under load (relaxed)
        assert (
            avg_latency < thresholds.single_query_avg * 2
        ), f"Average latency under load {avg_latency:.3f}s exceeds 2x threshold"
        assert (
            p99_latency < thresholds.single_query_p99 * 2
        ), f"P99 latency under load {p99_latency:.3f}s exceeds 2x threshold"
        assert (
            error_rate < thresholds.max_error_rate
        ), f"Error rate {error_rate:.1%} exceeds threshold {thresholds.max_error_rate:.1%}"

    @pytest.mark.asyncio
    async def test_prepared_statement_performance(self, benchmark_session):
        """
        Benchmark prepared statement performance advantage.

        GIVEN queries that can be prepared
        WHEN using prepared statements vs simple statements
        THEN prepared statements should show performance benefit
        """
        num_queries = 500

        # Benchmark simple statements
        simple_latencies = []
        simple_start = time.perf_counter()

        for i in range(num_queries):
            query_start = time.perf_counter()
            result = await benchmark_session.execute(
                f"SELECT * FROM {BenchmarkConfig.TEST_TABLE} WHERE id = {i}"
            )
            _ = result.one()
            simple_latencies.append(time.perf_counter() - query_start)

        simple_duration = time.perf_counter() - simple_start

        # Benchmark prepared statements
        prepared_stmt = await benchmark_session.prepare(
            f"SELECT * FROM {BenchmarkConfig.TEST_TABLE} WHERE id = ?"
        )

        prepared_latencies = []
        prepared_start = time.perf_counter()

        for i in range(num_queries):
            query_start = time.perf_counter()
            result = await benchmark_session.execute(prepared_stmt, [i])
            _ = result.one()
            prepared_latencies.append(time.perf_counter() - query_start)

        prepared_duration = time.perf_counter() - prepared_start

        # Calculate metrics
        simple_avg = statistics.mean(simple_latencies)
        prepared_avg = statistics.mean(prepared_latencies)
        performance_gain = (simple_avg - prepared_avg) / simple_avg

        # Verify prepared statements are faster
        assert prepared_duration < simple_duration, "Prepared statements should be faster overall"
        assert prepared_avg < simple_avg, "Prepared statements should have lower average latency"
        assert (
            performance_gain > 0.1
        ), f"Prepared statements should show >10% performance gain, got {performance_gain:.1%}"
