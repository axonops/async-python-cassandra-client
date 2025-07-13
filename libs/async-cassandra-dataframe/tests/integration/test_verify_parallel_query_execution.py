"""
Verify that parallel query execution is actually working.

What this tests:
---------------
1. Queries execute concurrently, not sequentially
2. Concurrency limits are respected
3. Performance improvement from parallelization
4. All data is returned correctly

Why this matters:
----------------
- User specifically asked to verify parallel execution is working
- Critical for performance - sequential would be unusable
- Must ensure max_concurrent_partitions config works
"""

import asyncio
import time

import async_cassandra_dataframe as cdf
import pytest


@pytest.mark.integration
class TestVerifyParallelQueryExecution:
    """Verify queries run in parallel as configured."""

    @pytest.mark.asyncio
    async def test_execution_time_proves_parallelism(self, session, test_table_name):
        """Parallel execution should be significantly faster than sequential."""
        # Create table with enough data for multiple partitions
        await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                id INT PRIMARY KEY,
                data TEXT
            )
        """
        )

        # Insert data - using prepared statement for speed
        insert_stmt = await session.prepare(
            f"INSERT INTO {test_table_name} (id, data) VALUES (?, ?)"
        )

        # Insert 5000 rows
        print("\nInserting test data...")
        batch_size = 100
        for batch_start in range(0, 5000, batch_size):
            tasks = []
            for i in range(batch_start, batch_start + batch_size):
                tasks.append(session.execute(insert_stmt, (i, f"data_{i}" * 10)))
            await asyncio.gather(*tasks)

        # Measure sequential execution (max_concurrent_partitions=1)
        print("\nTesting sequential execution...")
        start_seq = time.time()
        df_seq = await cdf.read_cassandra_table(
            session=session,
            keyspace=session.keyspace,
            table=test_table_name,
            max_concurrent_partitions=1,  # Force sequential
            memory_per_partition_mb=0.5,  # Small partitions to create many
        )
        time_sequential = time.time() - start_seq

        # Measure parallel execution (max_concurrent_partitions=5)
        print("\nTesting parallel execution...")
        start_par = time.time()
        df_par = await cdf.read_cassandra_table(
            session=session,
            keyspace=session.keyspace,
            table=test_table_name,
            max_concurrent_partitions=5,  # Allow parallel
            memory_per_partition_mb=0.5,  # Same partition size
        )
        time_parallel = time.time() - start_par

        # Verify we got all data
        assert len(df_seq) == 5000, f"Sequential: expected 5000 rows, got {len(df_seq)}"
        assert len(df_par) == 5000, f"Parallel: expected 5000 rows, got {len(df_par)}"

        # Verify same data
        seq_ids = set(df_seq["id"].values)
        par_ids = set(df_par["id"].values)
        assert seq_ids == par_ids, "Data mismatch between sequential and parallel"

        # Calculate speedup
        speedup = time_sequential / time_parallel

        print("\n=== PARALLEL EXECUTION VERIFICATION ===")
        print(f"Sequential time: {time_sequential:.2f}s")
        print(f"Parallel time: {time_parallel:.2f}s")
        print(f"Speedup: {speedup:.2f}x")
        print("=====================================")

        # Parallel should be noticeably faster
        # With 5 concurrent queries vs 1, even with overhead we should see speedup
        assert speedup > 1.3, f"Parallel not faster enough: only {speedup:.2f}x speedup"

        # But not impossibly fast (would indicate a bug)
        assert speedup < 10, f"Speedup too high ({speedup:.2f}x), might indicate a bug"

    @pytest.mark.asyncio
    async def test_concurrency_limit_is_respected(self, session, test_table_name):
        """max_concurrent_partitions should limit concurrent queries."""
        # Create table
        await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                id INT PRIMARY KEY,
                data TEXT
            )
        """
        )

        # Insert data
        insert_stmt = await session.prepare(
            f"INSERT INTO {test_table_name} (id, data) VALUES (?, ?)"
        )
        for i in range(1000):
            await session.execute(insert_stmt, (i, f"data_{i}"))

        # Track concurrent executions by hooking into session
        concurrent_count = 0
        max_concurrent_seen = 0
        query_timeline = []

        original_execute_stream = session.execute_stream

        async def tracked_execute_stream(*args, **kwargs):
            nonlocal concurrent_count, max_concurrent_seen

            # Record start
            concurrent_count += 1
            max_concurrent_seen = max(max_concurrent_seen, concurrent_count)
            start_time = time.time()
            query_timeline.append(("start", start_time, concurrent_count))

            try:
                # Add small delay to ensure overlap
                await asyncio.sleep(0.05)
                result = await original_execute_stream(*args, **kwargs)
                return result
            finally:
                # Record end
                concurrent_count -= 1
                end_time = time.time()
                query_timeline.append(("end", end_time, concurrent_count))

        session.execute_stream = tracked_execute_stream

        # Read with specific concurrency limit
        max_concurrent_config = 3
        await cdf.read_cassandra_table(
            session=session,
            keyspace=session.keyspace,
            table=test_table_name,
            max_concurrent_partitions=max_concurrent_config,
            memory_per_partition_mb=0.1,  # Small to create multiple partitions
        )

        # Restore original
        session.execute_stream = original_execute_stream

        # Analyze results
        print("\n=== CONCURRENCY VERIFICATION ===")
        print(f"Configured max concurrent: {max_concurrent_config}")
        print(f"Actual max concurrent seen: {max_concurrent_seen}")
        print(f"Total queries executed: {len([e for e in query_timeline if e[0] == 'start'])}")

        # Should respect the limit
        assert (
            max_concurrent_seen <= max_concurrent_config
        ), f"Exceeded concurrency limit: {max_concurrent_seen} > {max_concurrent_config}"

        # Should actually use parallelism (not just sequential)
        assert (
            max_concurrent_seen >= 2
        ), f"No parallelism detected, max concurrent was only {max_concurrent_seen}"

        # Verify timeline shows overlap
        starts = [e for e in query_timeline if e[0] == "start"]
        if len(starts) >= 2:
            # Check that second query started before first ended
            # first_start = starts[0][1]  # Variable not used
            second_start = starts[1][1]
            first_end = next(e[1] for e in query_timeline if e[0] == "end")

            assert second_start < first_end, "Queries not overlapping - running sequentially!"

        print("✓ Concurrency limit respected")
        print("✓ Queries executing in parallel")
        print("================================")

    @pytest.mark.asyncio
    async def test_token_range_based_parallelism(self, session, test_table_name):
        """Verify parallelism works via token range partitioning."""
        # Create table
        await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                partition_key INT,
                cluster_key INT,
                data TEXT,
                PRIMARY KEY (partition_key, cluster_key)
            )
        """
        )

        # Insert data across partitions
        insert_stmt = await session.prepare(
            f"""
            INSERT INTO {test_table_name}
            (partition_key, cluster_key, data) VALUES (?, ?, ?)
        """
        )

        # Create 50 partitions with 20 rows each
        for pk in range(50):
            for ck in range(20):
                await session.execute(insert_stmt, (pk, ck, f"data_{pk}_{ck}"))

        # Track token range queries
        token_queries = []

        original_prepare = session.prepare

        async def track_prepare(query, *args, **kwargs):
            if "TOKEN(" in query:
                token_queries.append(query)
            return await original_prepare(query, *args, **kwargs)

        session.prepare = track_prepare

        # Read with parallelism
        df = await cdf.read_cassandra_table(
            session=session,
            keyspace=session.keyspace,
            table=test_table_name,
            max_concurrent_partitions=4,
            memory_per_partition_mb=0.01,  # Very small to force multiple ranges
        )

        # Restore
        session.prepare = original_prepare

        # Verify results
        assert len(df) == 1000  # 50 * 20

        # Should have multiple token range queries
        print(f"\nToken range queries executed: {len(token_queries)}")
        assert len(token_queries) > 1, "Should query multiple token ranges for parallelism"

        # Token queries should have different ranges
        import re

        ranges_seen = set()
        for query in token_queries:
            match = re.search(r"TOKEN.*?>=\s*(-?\d+).*?<=\s*(-?\d+)", query)
            if match:
                range_tuple = (int(match.group(1)), int(match.group(2)))
                ranges_seen.add(range_tuple)

        print(f"Unique token ranges: {len(ranges_seen)}")
        assert len(ranges_seen) > 1, "Should have different token ranges"
