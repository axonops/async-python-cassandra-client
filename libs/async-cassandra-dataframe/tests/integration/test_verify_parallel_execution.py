"""
Integration test to verify parallel query execution is working.

What this tests:
---------------
1. Queries actually run in parallel against real Cassandra
2. Execution time proves parallelism (not sequential)
3. Concurrency limits are respected
4. All data is returned correctly

Why this matters:
----------------
- User specifically requested verification of parallel execution
- This is a critical performance feature
- Must ensure queries run concurrently to Cassandra
"""

import asyncio
import time

import pytest
from async_cassandra import AsyncCluster
from async_cassandra_dataframe.reader import read_cassandra_table


@pytest.mark.integration
class TestVerifyParallelExecution:
    """Verify parallel query execution against real Cassandra."""

    @pytest.mark.asyncio
    async def test_parallel_execution_is_faster_than_sequential(self):
        """Parallel execution should be significantly faster than sequential."""
        cluster = AsyncCluster(["localhost"])
        try:
            session = await cluster.connect()

            # Create test keyspace and table
            await session.execute(
                """
                CREATE KEYSPACE IF NOT EXISTS test_parallel_verify
                WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
            """
            )
            await session.set_keyspace("test_parallel_verify")

            await session.execute("DROP TABLE IF EXISTS large_table")
            await session.execute(
                """
                CREATE TABLE large_table (
                    id INT PRIMARY KEY,
                    data TEXT
                )
            """
            )

            # Insert enough data to create multiple partitions
            insert_stmt = await session.prepare("INSERT INTO large_table (id, data) VALUES (?, ?)")

            # Insert 10k rows to ensure multiple token ranges
            batch_size = 100
            for batch_start in range(0, 10000, batch_size):
                batch_tasks = []
                for i in range(batch_start, batch_start + batch_size):
                    batch_tasks.append(session.execute(insert_stmt, (i, f"data_{i}")))
                await asyncio.gather(*batch_tasks)

            # Test sequential (max_concurrent_partitions=1)
            start_seq = time.time()
            df_seq = await read_cassandra_table(
                session=session,
                keyspace="test_parallel_verify",
                table="large_table",
                max_concurrent_partitions=1,  # Force sequential
                memory_per_partition_mb=1,  # Small partitions to create many
            )
            time_sequential = time.time() - start_seq

            # Test parallel (max_concurrent_partitions=5)
            start_par = time.time()
            df_par = await read_cassandra_table(
                session=session,
                keyspace="test_parallel_verify",
                table="large_table",
                max_concurrent_partitions=5,  # Allow parallel
                memory_per_partition_mb=1,  # Same partition size
            )
            time_parallel = time.time() - start_par

            # Verify results are the same
            assert len(df_seq) == 10000
            assert len(df_par) == 10000
            assert set(df_seq["id"]) == set(df_par["id"])

            # Parallel should be significantly faster
            speedup = time_sequential / time_parallel
            print(f"\nSequential: {time_sequential:.2f}s")
            print(f"Parallel: {time_parallel:.2f}s")
            print(f"Speedup: {speedup:.2f}x")

            # Should be at least 1.2x faster with parallel
            assert speedup > 1.2, f"Parallel not faster enough: {speedup:.2f}x"
        finally:
            await cluster.shutdown()

    @pytest.mark.asyncio
    async def test_concurrent_queries_with_monitoring(self):
        """Monitor actual concurrent connections to verify parallelism."""
        cluster = AsyncCluster(["localhost"])
        try:
            session = await cluster.connect()

            # Create test data
            await session.execute(
                """
                CREATE KEYSPACE IF NOT EXISTS test_parallel_monitor
                WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
            """
            )
            await session.set_keyspace("test_parallel_monitor")

            await session.execute("DROP TABLE IF EXISTS monitor_table")
            await session.execute(
                """
                CREATE TABLE monitor_table (
                    id INT PRIMARY KEY,
                    data TEXT
                )
            """
            )

            # Insert data
            insert_stmt = await session.prepare(
                "INSERT INTO monitor_table (id, data) VALUES (?, ?)"
            )
            for i in range(1000):
                await session.execute(insert_stmt, (i, f"data_{i}"))

            # Track query execution
            query_times = []

            # Hook into the actual query execution
            original_execute = session.execute_stream

            async def tracked_execute(*args, **kwargs):
                start = time.time()
                query_times.append(("start", start))
                try:
                    result = await original_execute(*args, **kwargs)
                    return result
                finally:
                    end = time.time()
                    query_times.append(("end", end))

            session.execute_stream = tracked_execute

            # Read with parallel execution
            await read_cassandra_table(
                session=session,
                keyspace="test_parallel_monitor",
                table="monitor_table",
                max_concurrent_partitions=3,
                memory_per_partition_mb=0.1,  # Small to create multiple partitions
            )

            # Analyze query overlap
            starts = [t for event, t in query_times if event == "start"]
            ends = [t for event, t in query_times if event == "end"]

            # Count max concurrent queries
            max_concurrent = 0
            for t in starts:
                # Count how many queries were running at this start time
                concurrent = sum(1 for s, e in zip(starts, ends, strict=False) if s <= t < e)
                max_concurrent = max(max_concurrent, concurrent)

            print(f"\nTotal queries: {len(starts)}")
            print(f"Max concurrent: {max_concurrent}")

            # Should have multiple queries running concurrently
            assert max_concurrent >= 2, "Should have concurrent queries"
            assert max_concurrent <= 3, "Should respect concurrency limit"
        finally:
            await cluster.shutdown()

    @pytest.mark.asyncio
    async def test_partition_based_parallelism(self):
        """Verify parallelism is based on token range partitions."""
        cluster = AsyncCluster(["localhost"])
        try:
            session = await cluster.connect()

            # Create test setup
            await session.execute(
                """
                CREATE KEYSPACE IF NOT EXISTS test_partition_parallel
                WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
            """
            )
            await session.set_keyspace("test_partition_parallel")

            await session.execute("DROP TABLE IF EXISTS partition_test")
            await session.execute(
                """
                CREATE TABLE partition_test (
                    partition_key INT,
                    cluster_key INT,
                    data TEXT,
                    PRIMARY KEY (partition_key, cluster_key)
                )
            """
            )

            # Insert data across multiple partitions
            insert_stmt = await session.prepare(
                "INSERT INTO partition_test (partition_key, cluster_key, data) VALUES (?, ?, ?)"
            )

            # Create 100 partitions with 10 rows each
            for pk in range(100):
                for ck in range(10):
                    await session.execute(insert_stmt, (pk, ck, f"data_{pk}_{ck}"))

            # Track which token ranges are being queried
            queried_ranges = []

            original_execute = session.execute_stream

            async def track_token_queries(*args, **kwargs):
                query = str(args[0]) if args else ""
                if "TOKEN(" in query:
                    # Extract token range from query
                    import re

                    match = re.search(r"TOKEN.*?>=\s*(-?\d+).*?<=\s*(-?\d+)", query)
                    if match:
                        start_token = int(match.group(1))
                        end_token = int(match.group(2))
                        queried_ranges.append((start_token, end_token))
                return await original_execute(*args, **kwargs)

            session.execute_stream = track_token_queries

            # Read with parallel execution
            df = await read_cassandra_table(
                session=session,
                keyspace="test_partition_parallel",
                table="partition_test",
                max_concurrent_partitions=4,
                memory_per_partition_mb=0.01,  # Very small to create many partitions
            )

            # Verify we got all data
            assert len(df) == 1000  # 100 partitions * 10 rows

            # Verify multiple token ranges were queried
            print(f"\nToken ranges queried: {len(queried_ranges)}")
            assert len(queried_ranges) > 1, "Should query multiple token ranges"

            # Verify ranges don't overlap significantly
            # (some overlap is OK due to wraparound handling)
            for i, (start1, end1) in enumerate(queried_ranges):
                for j, (start2, end2) in enumerate(queried_ranges):
                    if i != j:
                        # Check for complete overlap
                        if start1 == start2 and end1 == end2:
                            pytest.fail("Duplicate token ranges queried")
        finally:
            await cluster.shutdown()
