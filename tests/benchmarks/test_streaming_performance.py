"""
Performance benchmarks for streaming operations.

These benchmarks ensure streaming provides memory-efficient
data processing without significant performance overhead.
"""

import asyncio
import gc
import os
import statistics
import time

import psutil
import pytest
import pytest_asyncio

from async_cassandra import AsyncCassandraSession, AsyncCluster, StreamConfig

from .benchmark_config import BenchmarkConfig


@pytest.mark.benchmark
class TestStreamingPerformance:
    """Benchmarks for streaming performance and memory efficiency."""

    @pytest_asyncio.fixture
    async def benchmark_session(self) -> AsyncCassandraSession:
        """Create session with large dataset for streaming benchmarks."""
        cluster = AsyncCluster(
            contact_points=["localhost"],
            executor_threads=8,
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

        await session.execute("DROP TABLE IF EXISTS streaming_test")
        await session.execute(
            """
            CREATE TABLE streaming_test (
                partition_id INT,
                row_id INT,
                data TEXT,
                value DOUBLE,
                metadata MAP<TEXT, TEXT>,
                PRIMARY KEY (partition_id, row_id)
            )
        """
        )

        # Insert large dataset across multiple partitions
        insert_stmt = await session.prepare(
            "INSERT INTO streaming_test (partition_id, row_id, data, value, metadata) VALUES (?, ?, ?, ?, ?)"
        )

        # Create 100 partitions with 1000 rows each = 100k rows
        batch_size = 100
        for partition in range(100):
            batch = []
            for row in range(1000):
                metadata = {f"key_{i}": f"value_{i}" for i in range(5)}
                batch.append((partition, row, f"data_{partition}_{row}" * 10, row * 1.5, metadata))

            # Insert in batches
            for i in range(0, len(batch), batch_size):
                await asyncio.gather(
                    *[session.execute(insert_stmt, params) for params in batch[i : i + batch_size]]
                )

        yield session

        await session.close()
        await cluster.shutdown()

    @pytest.mark.asyncio
    async def test_streaming_memory_efficiency(self, benchmark_session):
        """
        Benchmark memory usage of streaming vs regular queries.

        GIVEN a large result set
        WHEN using streaming vs loading all data
        THEN streaming should use significantly less memory
        """
        thresholds = BenchmarkConfig.DEFAULT_THRESHOLDS

        # Get process for memory monitoring
        process = psutil.Process(os.getpid())

        # Force garbage collection
        gc.collect()

        # Measure baseline memory
        process.memory_info().rss / 1024 / 1024  # MB

        # Test 1: Regular query (loads all into memory)
        regular_start_memory = process.memory_info().rss / 1024 / 1024

        regular_result = await benchmark_session.execute("SELECT * FROM streaming_test LIMIT 10000")
        regular_rows = []
        async for row in regular_result:
            regular_rows.append(row)

        regular_peak_memory = process.memory_info().rss / 1024 / 1024
        regular_memory_used = regular_peak_memory - regular_start_memory

        # Clear memory
        del regular_rows
        del regular_result
        gc.collect()
        await asyncio.sleep(0.1)

        # Test 2: Streaming query
        stream_start_memory = process.memory_info().rss / 1024 / 1024

        stream_config = StreamConfig(fetch_size=100, max_pages=None)
        stream_result = await benchmark_session.execute_stream(
            "SELECT * FROM streaming_test LIMIT 10000", stream_config=stream_config
        )

        row_count = 0
        max_stream_memory = stream_start_memory

        async for row in stream_result:
            row_count += 1
            if row_count % 1000 == 0:
                current_memory = process.memory_info().rss / 1024 / 1024
                max_stream_memory = max(max_stream_memory, current_memory)

        stream_memory_used = max_stream_memory - stream_start_memory

        # Calculate memory efficiency
        memory_ratio = stream_memory_used / regular_memory_used if regular_memory_used > 0 else 0

        # Verify thresholds
        assert (
            memory_ratio < thresholds.streaming_memory_overhead
        ), f"Streaming memory ratio {memory_ratio:.2f} exceeds threshold {thresholds.streaming_memory_overhead}"
        assert (
            stream_memory_used < regular_memory_used
        ), f"Streaming used more memory ({stream_memory_used:.1f}MB) than regular ({regular_memory_used:.1f}MB)"

    @pytest.mark.asyncio
    async def test_streaming_throughput(self, benchmark_session):
        """
        Benchmark streaming throughput for large datasets.

        GIVEN a large dataset
        WHEN processing with streaming
        THEN throughput should be acceptable
        """

        stream_config = StreamConfig(fetch_size=1000)

        # Benchmark streaming throughput
        start_time = time.perf_counter()
        row_count = 0

        stream_result = await benchmark_session.execute_stream(
            "SELECT * FROM streaming_test LIMIT 50000", stream_config=stream_config
        )

        async for row in stream_result:
            row_count += 1
            # Simulate minimal processing
            _ = row.partition_id + row.row_id

        duration = time.perf_counter() - start_time
        throughput = row_count / duration

        # Verify throughput
        assert (
            throughput > 10000
        ), f"Streaming throughput {throughput:.0f} rows/sec below minimum 10k rows/sec"
        assert row_count == 50000, f"Expected 50000 rows, got {row_count}"

    @pytest.mark.asyncio
    async def test_streaming_latency_overhead(self, benchmark_session):
        """
        Benchmark latency overhead of streaming vs regular queries.

        GIVEN queries of various sizes
        WHEN comparing streaming vs regular execution
        THEN streaming overhead should be minimal
        """
        thresholds = BenchmarkConfig.DEFAULT_THRESHOLDS

        test_sizes = [100, 1000, 5000]

        for size in test_sizes:
            # Regular query timing
            regular_start = time.perf_counter()
            regular_result = await benchmark_session.execute(
                f"SELECT * FROM streaming_test LIMIT {size}"
            )
            regular_rows = []
            async for row in regular_result:
                regular_rows.append(row)
            regular_duration = time.perf_counter() - regular_start

            # Streaming query timing
            stream_config = StreamConfig(fetch_size=min(100, size))
            stream_start = time.perf_counter()
            stream_result = await benchmark_session.execute_stream(
                f"SELECT * FROM streaming_test LIMIT {size}", stream_config=stream_config
            )
            stream_rows = []
            async for row in stream_result:
                stream_rows.append(row)
            stream_duration = time.perf_counter() - stream_start

            # Calculate overhead
            overhead_ratio = (
                stream_duration / regular_duration if regular_duration > 0 else float("inf")
            )

            # Verify overhead is acceptable
            assert (
                overhead_ratio < thresholds.streaming_latency_overhead
            ), f"Streaming overhead {overhead_ratio:.2f}x for {size} rows exceeds threshold"
            assert len(stream_rows) == len(
                regular_rows
            ), f"Row count mismatch: streaming={len(stream_rows)}, regular={len(regular_rows)}"

    @pytest.mark.asyncio
    async def test_streaming_page_processing_performance(self, benchmark_session):
        """
        Benchmark page-by-page processing performance.

        GIVEN streaming with page iteration
        WHEN processing pages individually
        THEN performance should scale linearly with data size
        """
        stream_config = StreamConfig(fetch_size=500, max_pages=100)

        page_latencies = []
        total_rows = 0

        start_time = time.perf_counter()

        stream_result = await benchmark_session.execute_stream(
            "SELECT * FROM streaming_test LIMIT 10000", stream_config=stream_config
        )

        async for page in stream_result.pages():
            page_start = time.perf_counter()

            # Process page
            page_rows = 0
            for row in page:
                page_rows += 1
                # Simulate processing
                _ = row.value * 2

            page_duration = time.perf_counter() - page_start
            page_latencies.append(page_duration)
            total_rows += page_rows

        total_duration = time.perf_counter() - start_time

        # Calculate metrics
        avg_page_latency = statistics.mean(page_latencies)
        page_throughput = len(page_latencies) / total_duration
        row_throughput = total_rows / total_duration

        # Verify performance
        assert (
            avg_page_latency < 0.1
        ), f"Average page processing time {avg_page_latency:.3f}s exceeds 100ms"
        assert (
            page_throughput > 10
        ), f"Page throughput {page_throughput:.1f} pages/sec below minimum"
        assert row_throughput > 5000, f"Row throughput {row_throughput:.0f} rows/sec below minimum"

    @pytest.mark.asyncio
    async def test_concurrent_streaming_operations(self, benchmark_session):
        """
        Benchmark concurrent streaming operations.

        GIVEN multiple concurrent streaming queries
        WHEN executed simultaneously
        THEN system should handle them efficiently
        """

        async def stream_worker(worker_id: int):
            """Worker that processes a streaming query."""
            stream_config = StreamConfig(fetch_size=100)

            start = time.perf_counter()
            row_count = 0

            # Each worker queries different partition
            stream_result = await benchmark_session.execute_stream(
                f"SELECT * FROM streaming_test WHERE partition_id = {worker_id} LIMIT 1000",
                stream_config=stream_config,
            )

            async for row in stream_result:
                row_count += 1

            duration = time.perf_counter() - start
            return duration, row_count

        # Run concurrent streaming operations
        num_workers = 10
        start_time = time.perf_counter()

        results = await asyncio.gather(*[stream_worker(i) for i in range(num_workers)])

        total_duration = time.perf_counter() - start_time

        # Calculate metrics
        worker_durations = [d for d, _ in results]
        total_rows = sum(count for _, count in results)
        avg_worker_duration = statistics.mean(worker_durations)

        # Verify concurrent performance
        assert (
            total_duration < avg_worker_duration * 2
        ), "Concurrent streams should show parallelism benefit"
        assert all(
            count >= 900 for _, count in results
        ), "All workers should process most of their rows"
        assert total_rows >= num_workers * 900, f"Total rows {total_rows} below expected minimum"
