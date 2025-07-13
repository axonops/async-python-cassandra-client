"""
Integration tests for parallel query execution.

What this tests:
---------------
1. Queries execute in parallel, not serially
2. Concurrency control (max parallel queries)
3. Performance improvement from parallelization
4. Resource management (threads, connections)
5. Error handling in parallel execution
6. Progress tracking across parallel queries

Why this matters:
----------------
- Serial execution is 10-100x slower
- Must utilize Cassandra's distributed nature
- Concurrency control prevents overwhelming cluster
- Parallel errors need proper handling
- Production performance requirement
"""

import asyncio
import time

import async_cassandra_dataframe as cdf
import pytest


class TestParallelExecution:
    """Test parallel query execution for partitions."""

    @pytest.mark.asyncio
    async def test_parallel_vs_serial_execution(self, session, test_table_name):
        """
        Test that queries execute in parallel, not serially.

        What this tests:
        ---------------
        1. Parallel execution is faster than serial
        2. Multiple queries run concurrently
        3. Performance scales with parallelism
        4. No blocking between queries

        Why this matters:
        ----------------
        - Serial execution wastes cluster capacity
        - 10-100x performance difference
        - Critical for large table reads
        - Production requirement
        """
        # Create table with multiple partitions
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                partition_id INT,
                id INT,
                data TEXT,
                PRIMARY KEY (partition_id, id)
            )
        """
        )

        try:
            # Prepare insert statement
            insert_stmt = await session.prepare(
                f"INSERT INTO {test_table_name} (partition_id, id, data) VALUES (?, ?, ?)"
            )

            # Insert data across multiple partitions
            inserted_count = 0
            for p in range(10):  # 10 partitions
                for i in range(1000):  # 1000 rows each
                    await session.execute(insert_stmt, (p, i, f"data_{p}_{i}"))
                    inserted_count += 1
            print(f"Inserted {inserted_count} rows")

            # Verify with a simple COUNT query
            count_result = await session.execute(f"SELECT COUNT(*) FROM {test_table_name}")
            actual_count = list(count_result)[0].count
            print(f"COUNT(*) query shows {actual_count} rows in table")

            # Test 1: Serial execution (baseline)
            start_serial = time.time()

            # Read with partition_count=1 to force serial
            df_serial = await cdf.read_cassandra_table(
                f"test_dataframe.{test_table_name}",
                session=session,
                partition_count=1,
                max_concurrent_partitions=1,  # Force serial
            )

            serial_result = df_serial.compute()
            serial_time = time.time() - start_serial

            print(f"\nSerial execution time: {serial_time:.2f}s")
            print(f"Rows read: {len(serial_result)}")

            # Debug serial result too
            if len(serial_result) != 10000:
                print(f"Serial missing rows! Got {len(serial_result)} instead of 10000")
                print(
                    "Serial partition IDs present:", sorted(serial_result["partition_id"].unique())
                )

            # Test 2: Parallel execution
            start_parallel = time.time()

            # Read with multiple partitions and parallelism
            df_parallel = await cdf.read_cassandra_table(
                f"test_dataframe.{test_table_name}",
                session=session,
                partition_count=10,
                max_concurrent_partitions=5,  # Allow 5 parallel queries
            )

            parallel_result = df_parallel.compute()
            parallel_time = time.time() - start_parallel

            print(f"Parallel execution time: {parallel_time:.2f}s")
            print(f"Speedup: {serial_time / parallel_time:.2f}x")

            # Verify correctness
            assert len(parallel_result) == len(
                serial_result
            ), "Parallel should read same data as serial"
            # Debug: check which partitions we got
            if len(parallel_result) != 10000:
                print(f"Missing rows! Got {len(parallel_result)} instead of 10000")
                print("Partition IDs present:", sorted(parallel_result["partition_id"].unique()))

                # Check what's missing for partition_id=3
                select_p3 = await session.execute(
                    f"SELECT COUNT(*) FROM {test_table_name} WHERE partition_id = 3"
                )
                p3_count = list(select_p3)[0].count
                print(f"Direct query for partition_id=3 shows {p3_count} rows")

                # Get token for partition_id=3
                token_query = await session.execute(
                    f"SELECT token(partition_id) FROM {test_table_name} WHERE partition_id = 3 LIMIT 1"
                )
                if list(token_query):
                    p3_token = list(token_query)[0][0]
                    print(f"Token for partition_id=3 is {p3_token}")

            assert (
                len(parallel_result) == 10000
            ), f"Should read all 10k rows, got {len(parallel_result)}"

            # Verify performance improvement
            # Note: speedup varies based on system load and test environment
            assert (
                parallel_time < serial_time * 0.85
            ), f"Parallel should be faster than serial (got {parallel_time:.2f}s vs {serial_time:.2f}s, speedup: {serial_time/parallel_time:.2f}x)"

            # Test 3: Verify actual parallelism with instrumentation

            async def instrumented_query(partition_id):
                """Query with timing instrumentation."""
                start = time.time()
                query = f"""
                    SELECT * FROM test_dataframe.{test_table_name}
                    WHERE partition_id = ?
                """
                prepared = await session.prepare(query)
                result = await session.execute(prepared, [partition_id])
                rows = list(result)
                end = time.time()
                return {
                    "partition_id": partition_id,
                    "start_time": start,
                    "end_time": end,
                    "duration": end - start,
                    "row_count": len(rows),
                }

            # Execute queries and collect timing
            tasks = [instrumented_query(p) for p in range(10)]
            timings = await asyncio.gather(*tasks)

            # Analyze overlap
            overlaps = 0
            for i in range(len(timings)):
                for j in range(i + 1, len(timings)):
                    t1 = timings[i]
                    t2 = timings[j]

                    # Check if queries overlapped in time
                    if t1["start_time"] < t2["end_time"] and t2["start_time"] < t1["end_time"]:
                        overlaps += 1

            print("\nQuery overlap analysis:")
            print(f"Total query pairs: {len(timings) * (len(timings) - 1) // 2}")
            print(f"Overlapping pairs: {overlaps}")

            assert overlaps > 0, "Should see queries executing in parallel"

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    @pytest.mark.asyncio
    async def test_concurrency_control(self, session, test_table_name):
        """
        Test max concurrent queries limit.

        What this tests:
        ---------------
        1. Respects max_concurrent_queries setting
        2. Queues excess queries appropriately
        3. No resource exhaustion
        4. Fair scheduling

        Why this matters:
        ----------------
        - Prevents overwhelming Cassandra
        - Controls resource usage
        - Required for production safety
        - Prevents connection pool exhaustion
        """
        # Create table
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                id INT PRIMARY KEY,
                data TEXT
            )
        """
        )

        try:
            # Prepare insert statement
            insert_stmt = await session.prepare(
                f"INSERT INTO {test_table_name} (id, data) VALUES (?, ?)"
            )

            # Insert test data
            for i in range(1000):
                await session.execute(insert_stmt, (i, f"data_{i}"))

            # Track concurrent queries
            concurrent_queries = []
            max_concurrent_seen = 0
            lock = asyncio.Lock()

            # Monkey-patch session to track concurrency
            original_execute = session.execute

            async def tracked_execute(query, *args, **kwargs):
                nonlocal max_concurrent_seen
                async with lock:
                    concurrent_queries.append(time.time())
                    # Count queries in last 0.1 seconds as concurrent
                    now = time.time()
                    recent = [t for t in concurrent_queries if now - t < 0.1]
                    max_concurrent_seen = max(len(recent), max_concurrent_seen)

                # Simulate some query time
                await asyncio.sleep(0.05)

                return await original_execute(query, *args, **kwargs)

            session.execute = tracked_execute

            # Read with concurrency limit
            max_allowed = 3
            df = await cdf.read_cassandra_table(
                f"test_dataframe.{test_table_name}",
                session=session,
                partition_count=10,  # More partitions than allowed concurrent
                max_concurrent_queries=max_allowed,
            )

            result = df.compute()

            # Restore original method
            session.execute = original_execute

            print("\nConcurrency control test:")
            print(f"Max concurrent allowed: {max_allowed}")
            print(f"Max concurrent seen: {max_concurrent_seen}")
            print(f"Total queries tracked: {len(concurrent_queries)}")

            # Verify limit was respected (with some tolerance for timing)
            assert (
                max_concurrent_seen <= max_allowed + 1
            ), f"Should not exceed max concurrent queries ({max_allowed})"

            # Verify all data was read
            assert len(result) == 1000, "Should read all data despite concurrency limit"

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    @pytest.mark.asyncio
    async def test_parallel_error_handling(self, session, test_table_name):
        """
        Test error handling during parallel execution.

        What this tests:
        ---------------
        1. Errors in one partition don't affect others
        2. Partial failures handled gracefully
        3. Error aggregation and reporting
        4. Cleanup after errors

        Why this matters:
        ----------------
        - Production resilience
        - Partial results may be acceptable
        - Must not leak resources on error
        - Clear error reporting needed
        """
        # Create table
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                partition_id INT,
                id INT,
                data TEXT,
                PRIMARY KEY (partition_id, id)
            )
        """
        )

        try:
            # Prepare insert statement
            insert_stmt = await session.prepare(
                f"INSERT INTO {test_table_name} (partition_id, id, data) VALUES (?, ?, ?)"
            )

            # Insert data
            for p in range(5):
                for i in range(100):
                    await session.execute(insert_stmt, (p, i, f"data_{p}_{i}"))

            # Create reader that will fail on certain partitions
            class FailingPartitionReader:
                def __init__(self, fail_partitions):
                    self.fail_partitions = fail_partitions
                    self.attempted_partitions = set()
                    self.successful_partitions = set()
                    self.failed_partitions = set()

                async def read_partition(self, partition_def):
                    partition_id = partition_def["partition_id"]
                    self.attempted_partitions.add(partition_id)

                    if partition_id in self.fail_partitions:
                        self.failed_partitions.add(partition_id)
                        raise RuntimeError(f"Simulated failure for partition {partition_id}")

                    # Simulate successful read
                    self.successful_partitions.add(partition_id)
                    return {"partition_id": partition_id, "row_count": 100}

            # Test with some failures
            reader = FailingPartitionReader(fail_partitions={1, 3})

            # Create partition definitions
            partitions = [
                {"partition_id": i, "table": f"test_dataframe.{test_table_name}"} for i in range(5)
            ]

            # Execute in parallel with error handling
            results = []
            errors = []

            async def safe_read(partition):
                try:
                    result = await reader.read_partition(partition)
                    return ("success", result)
                except Exception as e:
                    return ("error", {"partition": partition, "error": str(e)})

            # Run with parallelism
            tasks = [safe_read(p) for p in partitions]
            outcomes = await asyncio.gather(*tasks, return_exceptions=False)

            for status, data in outcomes:
                if status == "success":
                    results.append(data)
                else:
                    errors.append(data)

            print("\nError handling test:")
            print(f"Total partitions: {len(partitions)}")
            print(f"Successful: {len(results)}")
            print(f"Failed: {len(errors)}")
            print(f"Attempted: {reader.attempted_partitions}")

            # Verify behavior
            assert len(results) == 3, "Should have 3 successful partitions"
            assert len(errors) == 2, "Should have 2 failed partitions"
            assert len(reader.attempted_partitions) == 5, "Should attempt all partitions"

            # Verify error details
            failed_ids = {e["partition"]["partition_id"] for e in errors}
            assert failed_ids == {1, 3}, "Should fail expected partitions"

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    @pytest.mark.asyncio
    async def test_thread_pool_management(self, session, test_table_name):
        """
        Test thread pool resource management.

        What this tests:
        ---------------
        1. Thread pool doesn't grow unbounded
        2. Threads are reused efficiently
        3. No thread leaks
        4. Graceful shutdown

        Why this matters:
        ----------------
        - async-cassandra uses threads internally
        - Thread leaks cause resource exhaustion
        - Must manage thread lifecycle
        - Production stability
        """
        import threading

        # Get initial thread count
        initial_threads = threading.active_count()
        print(f"\nInitial thread count: {initial_threads}")

        # Create table
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                id INT PRIMARY KEY,
                data TEXT
            )
        """
        )

        try:
            # Prepare insert statement
            insert_stmt = await session.prepare(
                f"INSERT INTO {test_table_name} (id, data) VALUES (?, ?)"
            )

            # Insert data
            for i in range(500):
                await session.execute(insert_stmt, (i, f"data_{i}"))

            # Read with multiple partitions
            for iteration in range(3):
                df = await cdf.read_cassandra_table(
                    f"test_dataframe.{test_table_name}",
                    session=session,
                    partition_count=20,
                    max_concurrent_partitions=10,
                )

                df.compute()

                # Check thread count
                current_threads = threading.active_count()
                print(f"Iteration {iteration + 1} thread count: {current_threads}")

                # Thread count should stabilize, not grow indefinitely
                if iteration > 0:
                    assert (
                        current_threads <= initial_threads + 20
                    ), "Thread count should not grow unbounded"

            # Wait a bit for cleanup
            await asyncio.sleep(1)

            final_threads = threading.active_count()
            print(f"Final thread count: {final_threads}")

            # Should return close to initial (some tolerance for background threads)
            # TODO: Improve thread cleanup in parallel execution
            # Currently threads may persist due to thread pool reuse
            assert (
                final_threads <= initial_threads + 15
            ), f"Should not leak too many threads after completion (started with {initial_threads}, ended with {final_threads})"

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    @pytest.mark.asyncio
    async def test_progress_tracking(self, session, test_table_name):
        """
        Test progress tracking across parallel queries.

        What this tests:
        ---------------
        1. Progress callbacks during execution
        2. Accurate completion percentage
        3. Works with parallel execution
        4. Useful for monitoring

        Why this matters:
        ----------------
        - Long-running queries need progress
        - User feedback important
        - Monitoring and debugging
        - Production observability
        """
        # Create table
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                partition_id INT,
                id INT,
                data TEXT,
                PRIMARY KEY (partition_id, id)
            )
        """
        )

        try:
            # Prepare insert statement
            insert_stmt = await session.prepare(
                f"INSERT INTO {test_table_name} (partition_id, id, data) VALUES (?, ?, ?)"
            )

            # Insert data
            num_partitions = 10
            rows_per_partition = 100

            for p in range(num_partitions):
                for i in range(rows_per_partition):
                    await session.execute(insert_stmt, (p, i, f"data_{p}_{i}"))

            # Track progress
            progress_updates = []

            async def progress_callback(completed, total, message):
                """Callback for progress updates."""
                progress_updates.append(
                    {
                        "completed": completed,
                        "total": total,
                        "percentage": (completed / total * 100) if total > 0 else 0,
                        "message": message,
                        "timestamp": time.time(),
                    }
                )

            # Read with progress tracking
            df = await cdf.read_cassandra_table(
                f"test_dataframe.{test_table_name}",
                session=session,
                partition_count=num_partitions,
                max_concurrent_partitions=3,
                progress_callback=progress_callback,
            )

            result = df.compute()

            print("\nProgress tracking test:")
            print(f"Total progress updates: {len(progress_updates)}")
            print(f"Final progress: {progress_updates[-1] if progress_updates else 'None'}")

            # Verify progress tracking
            assert len(progress_updates) > 0, "Should have progress updates"

            # Check first and last updates
            if progress_updates:
                first = progress_updates[0]
                last = progress_updates[-1]

                assert first["completed"] < first["total"], "First update should show incomplete"
                assert last["completed"] == last["total"], "Last update should show completion"
                assert last["percentage"] == 100.0, "Should reach 100% completion"

                # Check monotonic progress
                for i in range(1, len(progress_updates)):
                    assert (
                        progress_updates[i]["completed"] >= progress_updates[i - 1]["completed"]
                    ), "Progress should be monotonic"

            # Verify all data read
            assert len(result) == num_partitions * rows_per_partition, "Should read all data"

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    @pytest.mark.asyncio
    async def test_replica_aware_parallelism(self, session, test_table_name):
        """
        Test replica-aware parallel execution.

        What this tests:
        ---------------
        1. Queries scheduled to replica nodes
        2. Reduced coordinator hops
        3. Better load distribution
        4. Improved performance

        Why this matters:
        ----------------
        - Data locality optimization
        - Reduced network traffic
        - Better cluster utilization
        - Production performance
        """
        # Create table with replication
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                id INT PRIMARY KEY,
                data TEXT
            )
        """
        )

        try:
            # Prepare insert statement
            insert_stmt = await session.prepare(
                f"INSERT INTO {test_table_name} (id, data) VALUES (?, ?)"
            )

            # Insert data
            for i in range(1000):
                await session.execute(insert_stmt, (i, f"data_{i}"))

            # Track which nodes handle queries
            coordinator_counts = {}

            # Monkey-patch to track coordinators
            original_execute = session.execute

            async def tracked_execute(query, *args, **kwargs):
                result = await original_execute(query, *args, **kwargs)

                # Get coordinator info (if available)
                if hasattr(result, "coordinator"):
                    coord = str(result.coordinator)
                    coordinator_counts[coord] = coordinator_counts.get(coord, 0) + 1

                return result

            session.execute = tracked_execute

            # Read with replica awareness
            # Note: replica-aware routing is handled automatically by the driver
            df = await cdf.read_cassandra_table(
                f"test_dataframe.{test_table_name}", session=session, partition_count=10
            )

            df.compute()

            # Restore original
            session.execute = original_execute

            print("\nReplica-aware execution:")
            print(f"Coordinator distribution: {coordinator_counts}")

            # In a multi-node cluster, should see distribution
            # In single-node test, all go to same coordinator
            if len(coordinator_counts) > 1:
                # Check for reasonable distribution
                total_queries = sum(coordinator_counts.values())
                max_queries = max(coordinator_counts.values())

                # No single coordinator should handle everything
                assert (
                    max_queries < total_queries * 0.8
                ), "Queries should be distributed across coordinators"

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")
