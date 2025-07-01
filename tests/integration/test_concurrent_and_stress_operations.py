"""
Consolidated integration tests for concurrent operations and stress testing.

This module combines all concurrent operation tests from multiple files,
providing comprehensive coverage of high-concurrency scenarios.

Tests consolidated from:
- test_concurrent_operations.py - Basic concurrent operations
- test_stress.py - High-volume stress testing
- Various concurrent tests from other files

Test Organization:
==================
1. Basic Concurrent Operations - Read/write/mixed operations
2. High-Volume Stress Tests - Extreme concurrency scenarios
3. Sustained Load Testing - Long-running concurrent operations
4. Connection Pool Testing - Behavior at connection limits
5. Wide Row Performance - Concurrent operations on large data
"""

import asyncio
import random
import statistics
import time
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import pytest
import pytest_asyncio
from cassandra.cluster import Cluster as SyncCluster
from cassandra.query import BatchStatement, BatchType

from async_cassandra import AsyncCassandraSession, AsyncCluster, StreamConfig


@pytest.mark.asyncio
@pytest.mark.integration
class TestConcurrentOperations:
    """Test basic concurrent operations with real Cassandra."""

    # ========================================
    # Basic Concurrent Operations
    # ========================================

    async def test_concurrent_reads(self, cassandra_session: AsyncCassandraSession):
        """
        Test high-concurrency read operations.

        What this tests:
        ---------------
        1. 1000 concurrent read operations
        2. Connection pool handling
        3. Read performance under load
        4. No interference between reads

        Why this matters:
        ----------------
        Read-heavy workloads are common in production.
        The driver must handle many concurrent reads efficiently.
        """
        # Get the unique table name
        users_table = cassandra_session._test_users_table

        # Insert test data first
        insert_stmt = await cassandra_session.prepare(
            f"INSERT INTO {users_table} (id, name, email, age) VALUES (?, ?, ?, ?)"
        )

        test_ids = []
        for i in range(100):
            test_id = uuid.uuid4()
            test_ids.append(test_id)
            await cassandra_session.execute(
                insert_stmt, [test_id, f"User {i}", f"user{i}@test.com", 20 + (i % 50)]
            )

        # Perform 1000 concurrent reads
        select_stmt = await cassandra_session.prepare(f"SELECT * FROM {users_table} WHERE id = ?")

        async def read_record(record_id):
            start = time.time()
            result = await cassandra_session.execute(select_stmt, [record_id])
            duration = time.time() - start
            rows = []
            async for row in result:
                rows.append(row)
            return rows[0] if rows else None, duration

        # Create 1000 read tasks (reading the same 100 records multiple times)
        tasks = []
        for i in range(1000):
            record_id = test_ids[i % len(test_ids)]
            tasks.append(read_record(record_id))

        start_time = time.time()
        results = await asyncio.gather(*tasks)
        total_time = time.time() - start_time

        # Verify results
        successful_reads = [r for r, _ in results if r is not None]
        assert len(successful_reads) == 1000

        # Check performance
        durations = [d for _, d in results]
        avg_duration = sum(durations) / len(durations)

        print("\nConcurrent read test results:")
        print(f"  Total time: {total_time:.2f}s")
        print(f"  Average read latency: {avg_duration*1000:.2f}ms")
        print(f"  Reads per second: {1000/total_time:.0f}")

        # Performance assertions (relaxed for CI environments)
        assert total_time < 15.0  # Should complete within 15 seconds
        assert avg_duration < 0.5  # Average latency under 500ms

    async def test_concurrent_writes(self, cassandra_session: AsyncCassandraSession):
        """
        Test high-concurrency write operations.

        What this tests:
        ---------------
        1. 500 concurrent write operations
        2. Write performance under load
        3. No data loss or corruption
        4. Error handling under load

        Why this matters:
        ----------------
        Write-heavy workloads test the driver's ability
        to handle many concurrent mutations efficiently.
        """
        # Get the unique table name
        users_table = cassandra_session._test_users_table

        insert_stmt = await cassandra_session.prepare(
            f"INSERT INTO {users_table} (id, name, email, age) VALUES (?, ?, ?, ?)"
        )

        async def write_record(i):
            start = time.time()
            try:
                await cassandra_session.execute(
                    insert_stmt,
                    [uuid.uuid4(), f"Concurrent User {i}", f"concurrent{i}@test.com", 25],
                )
                return True, time.time() - start
            except Exception:
                return False, time.time() - start

        # Create 500 concurrent write tasks
        tasks = [write_record(i) for i in range(500)]

        start_time = time.time()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        total_time = time.time() - start_time

        # Count successes
        successful_writes = sum(1 for r in results if isinstance(r, tuple) and r[0])
        failed_writes = 500 - successful_writes

        print("\nConcurrent write test results:")
        print(f"  Total time: {total_time:.2f}s")
        print(f"  Successful writes: {successful_writes}")
        print(f"  Failed writes: {failed_writes}")
        print(f"  Writes per second: {successful_writes/total_time:.0f}")

        # Should have very high success rate
        assert successful_writes >= 495  # Allow up to 1% failure
        assert total_time < 10.0  # Should complete within 10 seconds

    async def test_mixed_concurrent_operations(self, cassandra_session: AsyncCassandraSession):
        """
        Test mixed read/write/update operations under high concurrency.

        What this tests:
        ---------------
        1. 600 mixed operations (200 inserts, 300 reads, 100 updates)
        2. Different operation types running concurrently
        3. No interference between operation types
        4. Consistent performance across operation types

        Why this matters:
        ----------------
        Real workloads mix different operation types.
        The driver must handle them all efficiently.
        """
        # Get the unique table name
        users_table = cassandra_session._test_users_table

        # Prepare statements
        insert_stmt = await cassandra_session.prepare(
            f"INSERT INTO {users_table} (id, name, email, age) VALUES (?, ?, ?, ?)"
        )
        select_stmt = await cassandra_session.prepare(f"SELECT * FROM {users_table} WHERE id = ?")
        update_stmt = await cassandra_session.prepare(
            f"UPDATE {users_table} SET age = ? WHERE id = ?"
        )

        # Pre-populate some data
        existing_ids = []
        for i in range(50):
            user_id = uuid.uuid4()
            existing_ids.append(user_id)
            await cassandra_session.execute(
                insert_stmt, [user_id, f"Existing User {i}", f"existing{i}@test.com", 30]
            )

        # Define operation types
        async def insert_operation(i):
            return await cassandra_session.execute(
                insert_stmt,
                [uuid.uuid4(), f"New User {i}", f"new{i}@test.com", 25],
            )

        async def select_operation(user_id):
            result = await cassandra_session.execute(select_stmt, [user_id])
            rows = []
            async for row in result:
                rows.append(row)
            return rows

        async def update_operation(user_id):
            new_age = random.randint(20, 60)
            return await cassandra_session.execute(update_stmt, [new_age, user_id])

        # Create mixed operations
        operations = []

        # 200 inserts
        for i in range(200):
            operations.append(insert_operation(i))

        # 300 selects
        for _ in range(300):
            user_id = random.choice(existing_ids)
            operations.append(select_operation(user_id))

        # 100 updates
        for _ in range(100):
            user_id = random.choice(existing_ids)
            operations.append(update_operation(user_id))

        # Shuffle to mix operation types
        random.shuffle(operations)

        # Execute all operations concurrently
        start_time = time.time()
        results = await asyncio.gather(*operations, return_exceptions=True)
        total_time = time.time() - start_time

        # Count results
        successful = sum(1 for r in results if not isinstance(r, Exception))
        failed = sum(1 for r in results if isinstance(r, Exception))

        print("\nMixed operations test results:")
        print(f"  Total operations: {len(operations)}")
        print(f"  Successful: {successful}")
        print(f"  Failed: {failed}")
        print(f"  Total time: {total_time:.2f}s")
        print(f"  Operations per second: {successful/total_time:.0f}")

        # Should have very high success rate
        assert successful >= 590  # Allow up to ~2% failure
        assert total_time < 15.0  # Should complete within 15 seconds

    async def test_concurrent_counter_updates(self, cassandra_session, shared_keyspace_setup):
        """
        Test concurrent counter updates.

        What this tests:
        ---------------
        1. 100 concurrent counter increments
        2. Counter consistency under concurrent updates
        3. No lost updates
        4. Correct final counter value

        Why this matters:
        ----------------
        Counters have special semantics in Cassandra.
        Concurrent updates must not lose increments.
        """
        # Create counter table
        table_name = f"concurrent_counters_{uuid.uuid4().hex[:8]}"
        await cassandra_session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY,
                count COUNTER
            )
            """
        )

        # Prepare update statement
        update_stmt = await cassandra_session.prepare(
            f"UPDATE {table_name} SET count = count + ? WHERE id = ?"
        )

        counter_id = "test_counter"
        increment_value = 1

        # Perform concurrent increments
        async def increment_counter(i):
            try:
                await cassandra_session.execute(update_stmt, (increment_value, counter_id))
                return True
            except Exception:
                return False

        # Run 100 concurrent increments
        tasks = [increment_counter(i) for i in range(100)]
        results = await asyncio.gather(*tasks)

        successful_updates = sum(1 for r in results if r is True)

        # Verify final counter value
        result = await cassandra_session.execute(
            f"SELECT count FROM {table_name} WHERE id = %s", (counter_id,)
        )
        row = result.one()
        final_count = row.count if row else 0

        print("\nCounter concurrent update results:")
        print(f"  Successful updates: {successful_updates}/100")
        print(f"  Final counter value: {final_count}")

        # All updates should succeed and be reflected
        assert successful_updates == 100
        assert final_count == 100


@pytest.mark.integration
@pytest.mark.stress
class TestStressScenarios:
    """Stress test scenarios for async-cassandra."""

    @pytest_asyncio.fixture
    async def stress_session(self) -> AsyncCassandraSession:
        """Create session optimized for stress testing."""
        cluster = AsyncCluster(
            contact_points=["localhost"],
            # Optimize for high concurrency - use maximum threads
            executor_threads=128,  # Maximum allowed
        )
        session = await cluster.connect()

        # Create stress test keyspace
        await session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS stress_test
            WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
        """
        )
        await session.set_keyspace("stress_test")

        # Create tables for different scenarios
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
        """
        Test handling 10,000 concurrent write operations.

        What this tests:
        ---------------
        1. Extreme write concurrency (10,000 operations)
        2. Thread pool handling under extreme load
        3. Memory usage under high concurrency
        4. Error rates at scale
        5. Latency distribution (P95, P99)

        Why this matters:
        ----------------
        Production systems may experience traffic spikes.
        The driver must handle extreme load gracefully.
        """
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
        assert successful_writes == 10000  # ALL writes MUST succeed
        assert len(errors) == 0, f"Write failures detected: {errors[:10]}"
        assert total_time < 60  # Should complete within 60 seconds
        assert avg_duration < 3.0  # Average latency under 3 seconds

    @pytest.mark.asyncio
    @pytest.mark.timeout(60)
    async def test_sustained_load(self, stress_session: AsyncCassandraSession):
        """
        Test sustained high load over time (30 seconds).

        What this tests:
        ---------------
        1. Sustained concurrent operations over 30 seconds
        2. Performance consistency over time
        3. Resource stability (no leaks)
        4. Error rates under sustained load
        5. Read/write balance under load

        Why this matters:
        ----------------
        Production systems run continuously.
        The driver must maintain performance over time.
        """
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
        """
        Test performance with wide rows (many columns per partition).

        What this tests:
        ---------------
        1. Creating wide rows with 10,000 columns
        2. Reading entire wide rows
        3. Reading column ranges
        4. Streaming through wide rows
        5. Performance with large result sets

        Why this matters:
        ----------------
        Wide rows are common in time-series and IoT data.
        The driver must handle them efficiently.
        """
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
        """
        Test behavior at connection pool limits.

        What this tests:
        ---------------
        1. 1000 concurrent queries exceeding connection pool
        2. Query queueing behavior
        3. No deadlocks or stalls
        4. Graceful handling of pool exhaustion
        5. Performance under pool pressure

        Why this matters:
        ----------------
        Connection pools have limits. The driver must
        handle more concurrent requests than connections.
        """
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


@pytest.mark.asyncio
@pytest.mark.integration
class TestConcurrentPatterns:
    """Test specific concurrent patterns and edge cases."""

    async def test_concurrent_streaming_sessions(self, cassandra_session, shared_keyspace_setup):
        """
        Test multiple sessions streaming concurrently.

        What this tests:
        ---------------
        1. Multiple streaming operations in parallel
        2. Resource isolation between streams
        3. Memory management with concurrent streams
        4. No interference between streams

        Why this matters:
        ----------------
        Streaming is resource-intensive. Multiple concurrent
        streams must not interfere with each other.
        """
        # Create test table with data
        table_name = f"streaming_test_{uuid.uuid4().hex[:8]}"
        await cassandra_session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                partition_key INT,
                clustering_key INT,
                data TEXT,
                PRIMARY KEY (partition_key, clustering_key)
            )
            """
        )

        # Insert data for streaming
        insert_stmt = await cassandra_session.prepare(
            f"INSERT INTO {table_name} (partition_key, clustering_key, data) VALUES (?, ?, ?)"
        )

        for partition in range(5):
            for cluster in range(1000):
                await cassandra_session.execute(
                    insert_stmt, (partition, cluster, f"data_{partition}_{cluster}")
                )

        # Define streaming function
        async def stream_partition(partition_id):
            """Stream all data from a partition."""
            count = 0
            stream_config = StreamConfig(fetch_size=100)

            async with await cassandra_session.execute_stream(
                f"SELECT * FROM {table_name} WHERE partition_key = %s",
                [partition_id],
                stream_config=stream_config,
            ) as stream:
                async for row in stream:
                    count += 1
                    assert row.partition_key == partition_id

            return partition_id, count

        # Run multiple streams concurrently
        print("\nRunning 5 concurrent streaming operations...")
        start_time = time.time()

        results = await asyncio.gather(*[stream_partition(i) for i in range(5)])

        total_time = time.time() - start_time

        # Verify results
        for partition_id, count in results:
            assert count == 1000, f"Partition {partition_id} had {count} rows, expected 1000"

        print(f"Streamed 5000 total rows across 5 streams in {total_time:.2f}s")
        assert total_time < 10.0  # Should complete reasonably fast

    async def test_concurrent_empty_results(self, cassandra_session, shared_keyspace_setup):
        """
        Test concurrent queries returning empty results.

        What this tests:
        ---------------
        1. 20 concurrent queries with no results
        2. Proper handling of empty result sets
        3. No resource leaks with empty results
        4. Consistent behavior

        Why this matters:
        ----------------
        Empty results are common in production.
        They must be handled efficiently.
        """
        # Create test table
        table_name = f"empty_results_{uuid.uuid4().hex[:8]}"
        await cassandra_session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id UUID PRIMARY KEY,
                data TEXT
            )
            """
        )

        # Don't insert any data - all queries will return empty

        select_stmt = await cassandra_session.prepare(f"SELECT * FROM {table_name} WHERE id = ?")

        async def query_empty(i):
            """Query for non-existent data."""
            result = await cassandra_session.execute(select_stmt, (uuid.uuid4(),))
            rows = list(result)
            return len(rows)

        # Run concurrent empty queries
        tasks = [query_empty(i) for i in range(20)]
        results = await asyncio.gather(*tasks)

        # All should return 0 rows
        assert all(count == 0 for count in results)
        print("\nAll 20 concurrent empty queries completed successfully")

    async def test_concurrent_failures_recovery(self, cassandra_session, shared_keyspace_setup):
        """
        Test concurrent queries with simulated failures and recovery.

        What this tests:
        ---------------
        1. Concurrent operations with random failures
        2. Retry mechanism under concurrent load
        3. Recovery from transient errors
        4. No cascading failures

        Why this matters:
        ----------------
        Network issues and transient failures happen.
        The driver must handle them gracefully.
        """
        # Create test table
        table_name = f"failure_test_{uuid.uuid4().hex[:8]}"
        await cassandra_session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id UUID PRIMARY KEY,
                attempt INT,
                data TEXT
            )
            """
        )

        insert_stmt = await cassandra_session.prepare(
            f"INSERT INTO {table_name} (id, attempt, data) VALUES (?, ?, ?)"
        )

        # Track attempts per operation
        attempt_counts = {}

        async def operation_with_retry(op_id):
            """Perform operation with retry on failure."""
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    # Simulate random failures (20% chance)
                    if random.random() < 0.2 and attempt < max_retries - 1:
                        raise Exception("Simulated transient failure")

                    # Perform the operation
                    await cassandra_session.execute(
                        insert_stmt, (uuid.uuid4(), attempt + 1, f"operation_{op_id}")
                    )

                    attempt_counts[op_id] = attempt + 1
                    return True

                except Exception:
                    if attempt == max_retries - 1:
                        # Final attempt failed
                        attempt_counts[op_id] = max_retries
                        return False
                    # Retry after brief delay
                    await asyncio.sleep(0.1 * (attempt + 1))

        # Run operations concurrently
        print("\nRunning 50 concurrent operations with simulated failures...")
        tasks = [operation_with_retry(i) for i in range(50)]
        results = await asyncio.gather(*tasks)

        successful = sum(1 for r in results if r is True)
        failed = sum(1 for r in results if r is False)

        # Analyze retry patterns
        retry_histogram = {}
        for attempts in attempt_counts.values():
            retry_histogram[attempts] = retry_histogram.get(attempts, 0) + 1

        print("\nResults:")
        print(f"  Successful: {successful}/50")
        print(f"  Failed: {failed}/50")
        print(f"  Retry distribution: {retry_histogram}")

        # Most operations should succeed (possibly with retries)
        assert successful >= 45  # At least 90% success rate

    async def test_async_vs_sync_performance(self, cassandra_session, shared_keyspace_setup):
        """
        Test async wrapper performance vs sync driver for concurrent operations.

        What this tests:
        ---------------
        1. Performance comparison between async and sync drivers
        2. 50 concurrent operations for both approaches
        3. Thread pool vs event loop efficiency
        4. Overhead of async wrapper

        Why this matters:
        ----------------
        Users need to know the async wrapper provides
        performance benefits for concurrent operations.
        """
        # Create sync cluster and session for comparison
        sync_cluster = SyncCluster(["localhost"])
        sync_session = sync_cluster.connect()
        sync_session.execute(
            f"USE {cassandra_session.keyspace}"
        )  # Use same keyspace as async session

        # Create test table
        table_name = f"perf_comparison_{uuid.uuid4().hex[:8]}"
        await cassandra_session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INT PRIMARY KEY,
                value TEXT
            )
            """
        )

        # Number of concurrent operations
        num_ops = 50

        # Prepare statements
        sync_insert = sync_session.prepare(f"INSERT INTO {table_name} (id, value) VALUES (?, ?)")
        async_insert = await cassandra_session.prepare(
            f"INSERT INTO {table_name} (id, value) VALUES (?, ?)"
        )

        # Sync approach with thread pool
        print("\nTesting sync driver with thread pool...")
        start_sync = time.time()
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = []
            for i in range(num_ops):
                future = executor.submit(sync_session.execute, sync_insert, (i, f"sync_{i}"))
                futures.append(future)

            # Wait for all
            for future in futures:
                future.result()
        sync_time = time.time() - start_sync

        # Async approach
        print("Testing async wrapper...")
        start_async = time.time()
        tasks = []
        for i in range(num_ops):
            task = cassandra_session.execute(async_insert, (i + 1000, f"async_{i}"))
            tasks.append(task)

        await asyncio.gather(*tasks)
        async_time = time.time() - start_async

        # Results
        print(f"\nPerformance comparison for {num_ops} concurrent operations:")
        print(f"  Sync with thread pool: {sync_time:.3f}s")
        print(f"  Async wrapper: {async_time:.3f}s")
        print(f"  Speedup: {sync_time/async_time:.2f}x")

        # Verify all data was inserted
        result = await cassandra_session.execute(f"SELECT COUNT(*) FROM {table_name}")
        total_count = result.one()[0]
        assert total_count == num_ops * 2  # Both sync and async inserts

        # Cleanup
        sync_session.shutdown()
        sync_cluster.shutdown()
