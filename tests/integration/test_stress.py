"""
Stress tests for high concurrency scenarios against real Cassandra.

These tests are marked as 'stress' and can be run separately:
    pytest -m stress tests/integration/test_stress.py
"""

import asyncio
import random
import statistics
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone

import pytest
import pytest_asyncio
from cassandra.query import BatchStatement, BatchType

from async_cassandra import AsyncCassandraSession, AsyncCluster, StreamConfig


@pytest.mark.integration
@pytest.mark.stress
class TestStressScenarios:
    """Stress test scenarios for async-cassandra."""

    @pytest_asyncio.fixture
    async def stress_session(self) -> AsyncCassandraSession:
        """Create session for stress testing."""
        cluster = AsyncCluster(
            contact_points=["localhost"],
            # Optimize for high concurrency - use maximum threads
            executor_threads=128,  # Maximum allowed by constants
            # Note: The underlying cassandra-driver manages connection pooling
            # Default is 2 connections per host with up to 32768 requests per connection
        )
        session = await cluster.connect()

        # Create stress test keyspace with higher replication for production-like scenario
        await session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS stress_test
            WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
        """
        )
        await session.set_keyspace("stress_test")

        # Create various tables for different scenarios
        await session.execute("DROP TABLE IF EXISTS high_volume")
        await session.execute(
            """
            CREATE TABLE high_volume (
                partition_key UUID,
                clustering_key TIMESTAMP,
                data TEXT,
                metrics MAP<TEXT, DOUBLE>,
                tags SET<TEXT>,
                PRIMARY KEY (partition_key, clustering_key)
            ) WITH CLUSTERING ORDER BY (clustering_key DESC)
        """
        )

        await session.execute("DROP TABLE IF EXISTS wide_rows")
        await session.execute(
            """
            CREATE TABLE wide_rows (
                partition_key UUID,
                column_id INT,
                data BLOB,
                PRIMARY KEY (partition_key, column_id)
            )
        """
        )

        yield session

        await session.close()
        await cluster.shutdown()

    @pytest.mark.asyncio
    @pytest.mark.timeout(60)  # 1 minute timeout
    async def test_extreme_concurrent_writes(self, stress_session: AsyncCassandraSession):
        """Test handling 10,000 concurrent write operations."""
        insert_stmt = await stress_session.prepare(
            """
            INSERT INTO high_volume (partition_key, clustering_key, data, metrics, tags)
            VALUES (?, ?, ?, ?, ?)
        """
        )

        async def write_record(i: int):
            """Write a single record with timing."""
            start = time.perf_counter()
            try:
                await stress_session.execute(
                    insert_stmt,
                    [
                        uuid.uuid4(),
                        datetime.now(timezone.utc),
                        f"stress_test_data_{i}_" + "x" * random.randint(100, 1000),
                        {
                            "latency": random.random() * 100,
                            "throughput": random.random() * 1000,
                            "cpu": random.random() * 100,
                        },
                        {f"tag{j}" for j in range(random.randint(1, 10))},
                    ],
                )
                return time.perf_counter() - start, None
            except Exception as exc:
                return time.perf_counter() - start, str(exc)

        # Launch 10,000 concurrent writes
        print("\nLaunching 10,000 concurrent writes...")
        start_time = time.time()

        tasks = [write_record(i) for i in range(10000)]
        results = await asyncio.gather(*tasks)

        total_time = time.time() - start_time

        # Analyze results
        durations = [r[0] for r in results]
        errors = [r[1] for r in results if r[1] is not None]

        successful_writes = len(results) - len(errors)
        avg_duration = statistics.mean(durations)
        p95_duration = statistics.quantiles(durations, n=20)[18]  # 95th percentile
        p99_duration = statistics.quantiles(durations, n=100)[98]  # 99th percentile

        print("\nResults for 10,000 concurrent writes:")
        print(f"  Total time: {total_time:.2f}s")
        print(f"  Successful writes: {successful_writes}")
        print(f"  Failed writes: {len(errors)}")
        print(f"  Throughput: {successful_writes/total_time:.0f} writes/sec")
        print(f"  Average latency: {avg_duration*1000:.2f}ms")
        print(f"  P95 latency: {p95_duration*1000:.2f}ms")
        print(f"  P99 latency: {p99_duration*1000:.2f}ms")

        # If there are errors, show a sample
        if errors:
            print("\nSample errors (first 5):")
            for i, err in enumerate(errors[:5]):
                print(f"  {i+1}. {err}")

        # Assertions
        assert successful_writes == 10000  # ALL writes MUST succeed - this is a DB driver!
        assert len(errors) == 0, f"Write failures detected: {errors[:10]}"  # Show first 10 errors
        assert total_time < 60  # Should complete within 60 seconds
        assert avg_duration < 3.0  # Average latency under 3 seconds (relaxed for CI)

    @pytest.mark.asyncio
    @pytest.mark.timeout(60)
    async def test_sustained_load(self, stress_session: AsyncCassandraSession):
        """Test sustained high load over time (30 seconds)."""
        insert_stmt = await stress_session.prepare(
            """
            INSERT INTO high_volume (partition_key, clustering_key, data, metrics, tags)
            VALUES (?, ?, ?, ?, ?)
        """
        )

        select_stmt = await stress_session.prepare(
            """
            SELECT * FROM high_volume WHERE partition_key = ?
            ORDER BY clustering_key DESC LIMIT 10
        """
        )

        # Track metrics over time
        metrics_by_second = defaultdict(
            lambda: {
                "writes": 0,
                "reads": 0,
                "errors": 0,
                "write_latencies": [],
                "read_latencies": [],
            }
        )

        # Shared state for operations
        written_partitions = []
        write_lock = asyncio.Lock()

        async def continuous_writes():
            """Continuously write data."""
            while time.time() - start_time < 30:
                try:
                    partition_key = uuid.uuid4()
                    start = time.perf_counter()

                    await stress_session.execute(
                        insert_stmt,
                        [
                            partition_key,
                            datetime.now(timezone.utc),
                            "sustained_load_test_" + "x" * 500,
                            {"metric": random.random()},
                            {f"tag{i}" for i in range(5)},
                        ],
                    )

                    duration = time.perf_counter() - start
                    second = int(time.time() - start_time)
                    metrics_by_second[second]["writes"] += 1
                    metrics_by_second[second]["write_latencies"].append(duration)

                    async with write_lock:
                        written_partitions.append(partition_key)

                except Exception:
                    second = int(time.time() - start_time)
                    metrics_by_second[second]["errors"] += 1

                await asyncio.sleep(0.001)  # Small delay to prevent overwhelming

        async def continuous_reads():
            """Continuously read data."""
            await asyncio.sleep(1)  # Let some writes happen first

            while time.time() - start_time < 30:
                if written_partitions:
                    try:
                        async with write_lock:
                            partition_key = random.choice(written_partitions[-100:])

                        start = time.perf_counter()
                        await stress_session.execute(select_stmt, [partition_key])

                        duration = time.perf_counter() - start
                        second = int(time.time() - start_time)
                        metrics_by_second[second]["reads"] += 1
                        metrics_by_second[second]["read_latencies"].append(duration)

                    except Exception:
                        second = int(time.time() - start_time)
                        metrics_by_second[second]["errors"] += 1

                await asyncio.sleep(0.002)  # Slightly slower than writes

        # Run sustained load test
        print("\nRunning 30-second sustained load test...")
        start_time = time.time()

        # Create multiple workers for each operation type
        write_tasks = [continuous_writes() for _ in range(50)]
        read_tasks = [continuous_reads() for _ in range(30)]

        await asyncio.gather(*write_tasks, *read_tasks)

        # Analyze results
        print("\nSustained load test results by second:")
        print("Second | Writes/s | Reads/s | Errors | Avg Write ms | Avg Read ms")
        print("-" * 70)

        total_writes = 0
        total_reads = 0
        total_errors = 0

        for second in sorted(metrics_by_second.keys()):
            metrics = metrics_by_second[second]
            avg_write_ms = (
                statistics.mean(metrics["write_latencies"]) * 1000
                if metrics["write_latencies"]
                else 0
            )
            avg_read_ms = (
                statistics.mean(metrics["read_latencies"]) * 1000
                if metrics["read_latencies"]
                else 0
            )

            print(
                f"{second:6d} | {metrics['writes']:8d} | {metrics['reads']:7d} | "
                f"{metrics['errors']:6d} | {avg_write_ms:12.2f} | {avg_read_ms:11.2f}"
            )

            total_writes += metrics["writes"]
            total_reads += metrics["reads"]
            total_errors += metrics["errors"]

        print(f"\nTotal operations: {total_writes + total_reads}")
        print(f"Total errors: {total_errors}")
        print(f"Error rate: {total_errors/(total_writes + total_reads)*100:.2f}%")

        # Assertions
        assert total_writes > 10000  # Should achieve high write throughput
        assert total_reads > 5000  # Should achieve good read throughput
        assert total_errors < (total_writes + total_reads) * 0.01  # Less than 1% error rate

    @pytest.mark.asyncio
    @pytest.mark.timeout(45)
    async def test_wide_row_performance(self, stress_session: AsyncCassandraSession):
        """Test performance with wide rows (many columns per partition)."""
        insert_stmt = await stress_session.prepare(
            """
            INSERT INTO wide_rows (partition_key, column_id, data)
            VALUES (?, ?, ?)
        """
        )

        # Create a few partitions with many columns each
        partition_keys = [uuid.uuid4() for _ in range(10)]
        columns_per_partition = 10000

        print(f"\nCreating wide rows with {columns_per_partition} columns per partition...")

        async def create_wide_row(partition_key: uuid.UUID):
            """Create a single wide row."""
            # Use batch inserts for efficiency
            batch_size = 100

            for batch_start in range(0, columns_per_partition, batch_size):
                batch = BatchStatement(batch_type=BatchType.UNLOGGED)

                for i in range(batch_start, min(batch_start + batch_size, columns_per_partition)):
                    batch.add(
                        insert_stmt,
                        [
                            partition_key,
                            i,
                            random.randbytes(random.randint(100, 1000)),  # Variable size data
                        ],
                    )

                await stress_session.execute(batch)

        # Create wide rows concurrently
        start_time = time.time()
        await asyncio.gather(*[create_wide_row(pk) for pk in partition_keys])
        create_time = time.time() - start_time

        print(f"Created {len(partition_keys)} wide rows in {create_time:.2f}s")

        # Test reading wide rows
        select_all_stmt = await stress_session.prepare(
            """
            SELECT * FROM wide_rows WHERE partition_key = ?
        """
        )

        select_range_stmt = await stress_session.prepare(
            """
            SELECT * FROM wide_rows WHERE partition_key = ?
            AND column_id >= ? AND column_id < ?
        """
        )

        # Read entire wide rows
        print("\nReading entire wide rows...")
        read_times = []

        for pk in partition_keys:
            start = time.perf_counter()
            result = await stress_session.execute(select_all_stmt, [pk])
            rows = []
            async for row in result:
                rows.append(row)
            read_times.append(time.perf_counter() - start)
            assert len(rows) == columns_per_partition

        print(
            f"Average time to read {columns_per_partition} columns: {statistics.mean(read_times)*1000:.2f}ms"
        )

        # Read ranges from wide rows
        print("\nReading column ranges...")
        range_times = []

        for _ in range(100):
            pk = random.choice(partition_keys)
            start_col = random.randint(0, columns_per_partition - 1000)
            end_col = start_col + 1000

            start = time.perf_counter()
            result = await stress_session.execute(select_range_stmt, [pk, start_col, end_col])
            rows = []
            async for row in result:
                rows.append(row)
            range_times.append(time.perf_counter() - start)
            assert 900 <= len(rows) <= 1000  # Approximately 1000 columns

        print(f"Average time to read 1000-column range: {statistics.mean(range_times)*1000:.2f}ms")

        # Stream through wide rows
        print("\nStreaming through wide rows...")
        stream_config = StreamConfig(fetch_size=1000)

        stream_start = time.time()
        total_streamed = 0

        for pk in partition_keys[:3]:  # Stream through 3 partitions
            result = await stress_session.execute_stream(
                "SELECT * FROM wide_rows WHERE partition_key = %s",
                [pk],
                stream_config=stream_config,
            )

            async for row in result:
                total_streamed += 1

        stream_time = time.time() - stream_start
        print(
            f"Streamed {total_streamed} rows in {stream_time:.2f}s "
            f"({total_streamed/stream_time:.0f} rows/sec)"
        )

        # Assertions
        assert statistics.mean(read_times) < 5.0  # Reading wide row under 5 seconds
        assert statistics.mean(range_times) < 0.5  # Range query under 500ms
        assert total_streamed == columns_per_partition * 3  # All rows streamed

    @pytest.mark.asyncio
    @pytest.mark.timeout(30)
    async def test_connection_pool_limits(self, stress_session: AsyncCassandraSession):
        """Test behavior at connection pool limits."""
        # Create a query that takes some time
        select_stmt = await stress_session.prepare(
            """
            SELECT * FROM high_volume LIMIT 1000
        """
        )

        # First, insert some data
        insert_stmt = await stress_session.prepare(
            """
            INSERT INTO high_volume (partition_key, clustering_key, data, metrics, tags)
            VALUES (?, ?, ?, ?, ?)
        """
        )

        for i in range(100):
            await stress_session.execute(
                insert_stmt,
                [
                    uuid.uuid4(),
                    datetime.now(timezone.utc),
                    f"test_data_{i}",
                    {"metric": float(i)},
                    {f"tag{i}"},
                ],
            )

        print("\nTesting connection pool under extreme load...")

        # Launch many more concurrent queries than available connections
        # Python driver has 1 connection per host for protocol v3+
        num_queries = 1000

        async def timed_query(query_id: int):
            """Execute query with timing."""
            start = time.perf_counter()
            try:
                await stress_session.execute(select_stmt)
                return query_id, time.perf_counter() - start, None
            except Exception as exc:
                return query_id, time.perf_counter() - start, str(exc)

        # Execute all queries concurrently
        start_time = time.time()
        results = await asyncio.gather(*[timed_query(i) for i in range(num_queries)])
        total_time = time.time() - start_time

        # Analyze queueing behavior
        successful = [r for r in results if r[2] is None]
        failed = [r for r in results if r[2] is not None]
        latencies = [r[1] for r in successful]

        print("\nConnection pool stress test results:")
        print(f"  Total queries: {num_queries}")
        print(f"  Successful: {len(successful)}")
        print(f"  Failed: {len(failed)}")
        print(f"  Total time: {total_time:.2f}s")
        print(f"  Throughput: {len(successful)/total_time:.0f} queries/sec")
        print(f"  Min latency: {min(latencies)*1000:.2f}ms")
        print(f"  Avg latency: {statistics.mean(latencies)*1000:.2f}ms")
        print(f"  Max latency: {max(latencies)*1000:.2f}ms")
        print(f"  P95 latency: {statistics.quantiles(latencies, n=20)[18]*1000:.2f}ms")

        # Despite connection limits, should handle high concurrency well
        assert len(successful) >= num_queries * 0.95  # 95% success rate
        assert statistics.mean(latencies) < 2.0  # Average under 2 seconds
