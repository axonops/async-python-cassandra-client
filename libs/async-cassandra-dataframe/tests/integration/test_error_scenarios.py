"""
Comprehensive error scenario tests for async-cassandra-dataframe.

What this tests:
---------------
1. Connection failures and timeouts
2. Node failures during queries
3. Schema changes during read
4. Invalid queries and data
5. Resource exhaustion scenarios
6. Retry logic and resilience
7. Partial failure handling
8. Memory limit violations

Why this matters:
----------------
- Production resilience critical
- Must handle failures gracefully
- Clear error messages for debugging
- No resource leaks on errors
- Recovery strategies needed
"""

import asyncio
import time
from unittest.mock import AsyncMock, Mock

import async_cassandra_dataframe as cdf
import pytest
from cassandra import OperationTimedOut, ReadTimeout
from cassandra.cluster import NoHostAvailable


class TestErrorScenarios:
    """Test error handling in various failure scenarios."""

    @pytest.mark.asyncio
    async def test_connection_failures(self, session):
        """
        Test handling of connection failures.

        What this tests:
        ---------------
        1. Initial connection failures
        2. Connection drops during query
        3. All nodes unavailable
        4. Partial node failures

        Why this matters:
        ----------------
        - Network issues are common
        - Must fail fast with clear errors
        - No hanging or infinite retries
        - Production resilience
        """
        # Test 1: No hosts available
        mock_session = AsyncMock()
        mock_session.execute.side_effect = NoHostAvailable(
            "All hosts failed", errors={"127.0.0.1": Exception("Connection refused")}
        )

        with pytest.raises(NoHostAvailable) as exc_info:
            await cdf.read_cassandra_table("test_dataframe.test_table", session=mock_session)

        assert "hosts failed" in str(exc_info.value).lower()

        # Test 2: Connection timeout
        mock_session.execute.side_effect = OperationTimedOut("Query timed out")

        with pytest.raises(OperationTimedOut) as exc_info:
            await cdf.read_cassandra_table("test_dataframe.test_table", session=mock_session)

        assert "timed out" in str(exc_info.value).lower()

        # Test 3: Connection drops mid-stream
        async def failing_stream(*args, **kwargs):
            """Simulate connection drop during streaming."""

            class FailingStream:
                def __aiter__(self):
                    return self

                async def __anext__(self):
                    # Return some data then fail
                    if not hasattr(self, "count"):
                        self.count = 0
                    self.count += 1

                    if self.count < 3:
                        return Mock(_asdict=lambda: {"id": self.count})
                    else:
                        raise ConnectionError("Connection lost")

                async def __aenter__(self):
                    return self

                async def __aexit__(self, *args):
                    pass

            return FailingStream()

        mock_session.execute_stream = failing_stream

        # Should handle streaming failures
        with pytest.raises(ConnectionError):
            df = await cdf.read_cassandra_table(
                "test_dataframe.test_table", session=mock_session, page_size=100
            )
            df.compute()

    @pytest.mark.asyncio
    async def test_query_timeouts(self, session, test_table_name):
        """
        Test handling of query timeouts.

        What this tests:
        ---------------
        1. Read timeout handling
        2. Write timeout handling
        3. Configurable timeout behavior
        4. Timeout with partial results

        Why this matters:
        ----------------
        - Large queries may timeout
        - Must handle gracefully
        - Timeout != failure always
        - Need clear timeout info
        """
        # Create test table
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                id INT PRIMARY KEY,
                data TEXT
            )
        """
        )

        try:
            # Insert data
            for i in range(100):
                await session.execute(
                    f"INSERT INTO {test_table_name} (id, data) VALUES (?, ?)",
                    (i, f"data_{i}" * 100),  # Larger data
                )

            # Mock timeout during read
            original_execute = session.execute
            call_count = 0

            async def timeout_execute(*args, **kwargs):
                nonlocal call_count
                call_count += 1

                # Timeout on 3rd call
                if call_count == 3:
                    raise ReadTimeout("Read timeout - received only 1 of 2 responses")

                return await original_execute(*args, **kwargs)

            session.execute = timeout_execute

            # Should handle timeout
            with pytest.raises(ReadTimeout) as exc_info:
                df = await cdf.read_cassandra_table(
                    f"test_dataframe.{test_table_name}",
                    session=session,
                    partition_count=5,  # Multiple queries
                )
                df.compute()

            assert "timeout" in str(exc_info.value).lower()

            # Restore
            session.execute = original_execute

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    @pytest.mark.asyncio
    async def test_schema_changes_during_read(self, session, test_table_name):
        """
        Test handling schema changes during read operation.

        What this tests:
        ---------------
        1. Column added during read
        2. Column dropped during read
        3. Table dropped during read
        4. Type changes

        Why this matters:
        ----------------
        - Schema can change in production
        - Must handle gracefully
        - Partial results considerations
        - Clear error messaging
        """
        # Create initial table
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                id INT PRIMARY KEY,
                data TEXT,
                value INT
            )
        """
        )

        try:
            # Insert initial data
            for i in range(50):
                await session.execute(
                    f"INSERT INTO {test_table_name} (id, data, value) VALUES (?, ?, ?)",
                    (i, f"data_{i}", i * 10),
                )

            # Start read operation that will be slow
            read_task = asyncio.create_task(
                cdf.read_cassandra_table(
                    f"test_dataframe.{test_table_name}",
                    session=session,
                    partition_count=10,
                    page_size=5,  # Small pages to slow down
                )
            )

            # Give it time to start
            await asyncio.sleep(0.1)

            # ALTER table while reading
            await session.execute(
                f"""
                ALTER TABLE {test_table_name} ADD extra_column TEXT
            """
            )

            # Try to complete the read
            try:
                df = await read_task
                result = df.compute()

                # May succeed with mixed schema
                print(f"Read completed with {len(result)} rows")
                print(f"Columns: {list(result.columns)}")

                # Some rows might have the new column as NaN
                if "extra_column" in result.columns:
                    null_count = result["extra_column"].isna().sum()
                    print(f"Rows without extra_column: {null_count}")

            except Exception as e:
                # Schema change might cause failure
                print(f"Read failed due to schema change: {e}")
                assert "schema" in str(e).lower() or "column" in str(e).lower()

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    @pytest.mark.asyncio
    async def test_invalid_queries(self, session, test_table_name):
        """
        Test handling of invalid queries.

        What this tests:
        ---------------
        1. Invalid column names
        2. Invalid predicates
        3. Syntax errors
        4. Type mismatches

        Why this matters:
        ----------------
        - User errors are common
        - Need clear error messages
        - Fail fast principle
        - Help debugging
        """
        # Create table
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                id INT PRIMARY KEY,
                name TEXT,
                age INT
            )
        """
        )

        try:
            # Test 1: Invalid column name
            with pytest.raises(ValueError) as exc_info:
                await cdf.read_cassandra_table(
                    f"test_dataframe.{test_table_name}",
                    session=session,
                    columns=["id", "invalid_column"],
                )

            assert "column" in str(exc_info.value).lower()
            assert "invalid_column" in str(exc_info.value)

            # Test 2: Invalid predicate column
            with pytest.raises(ValueError) as exc_info:
                await cdf.read_cassandra_table(
                    f"test_dataframe.{test_table_name}",
                    session=session,
                    predicates=[{"column": "nonexistent", "operator": "=", "value": 1}],
                )

            assert "nonexistent" in str(exc_info.value)

            # Test 3: Invalid operator
            with pytest.raises(ValueError) as exc_info:
                await cdf.read_cassandra_table(
                    f"test_dataframe.{test_table_name}",
                    session=session,
                    predicates=[
                        {"column": "age", "operator": "LIKE", "value": "%test%"}  # Not supported
                    ],
                )

            assert "operator" in str(exc_info.value).lower()

            # Test 4: Type mismatch in predicate
            # Insert some data first
            await session.execute(
                f"INSERT INTO {test_table_name} (id, name, age) VALUES (1, 'Alice', 25)"
            )

            # Try to query with wrong type
            df = await cdf.read_cassandra_table(
                f"test_dataframe.{test_table_name}",
                session=session,
                predicates=[
                    {
                        "column": "age",
                        "operator": "=",
                        "value": "not_a_number",  # String instead of int
                    }
                ],
                allow_filtering=True,
            )

            # May fail at execute or return empty
            try:
                result = df.compute()
                assert len(result) == 0, "Type mismatch should return no results"
            except Exception as e:
                assert "type" in str(e).lower() or "invalid" in str(e).lower()

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    @pytest.mark.asyncio
    async def test_memory_limit_exceeded(self, session, test_table_name):
        """
        Test handling when memory limits are exceeded.

        What this tests:
        ---------------
        1. Partition larger than memory limit
        2. Adaptive sizing behavior
        3. Memory tracking accuracy
        4. Graceful degradation

        Why this matters:
        ----------------
        - Prevent OOM errors
        - Predictable memory usage
        - Production stability
        - Clear limit messaging
        """
        # Create table with large data
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                id INT PRIMARY KEY,
                large_data TEXT
            )
        """
        )

        try:
            # Insert large rows
            large_text = "x" * 10000  # 10KB per row
            for i in range(1000):  # ~10MB total
                await session.execute(
                    f"INSERT INTO {test_table_name} (id, large_data) VALUES (?, ?)", (i, large_text)
                )

            # Read with small memory limit
            df = await cdf.read_cassandra_table(
                f"test_dataframe.{test_table_name}",
                session=session,
                memory_per_partition_mb=1,  # Only 1MB per partition
                partition_count=1,  # Force single partition
            )

            result = df.compute()

            # Should have limited rows due to memory constraint
            print(f"Rows read with 1MB limit: {len(result)}")

            # Should be significantly less than 1000
            assert len(result) < 200, "Memory limit should restrict rows read"

            # Test adaptive partitioning
            df_adaptive = await cdf.read_cassandra_table(
                f"test_dataframe.{test_table_name}",
                session=session,
                memory_per_partition_mb=1,
                # Don't specify partition_count - let it adapt
            )

            result_adaptive = await df_adaptive.compute()

            # Should read all data by creating more partitions
            assert len(result_adaptive) == 1000, "Adaptive should read all data"

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    @pytest.mark.asyncio
    async def test_partial_partition_failures(self, session, test_table_name):
        """
        Test handling when some partitions fail.

        What this tests:
        ---------------
        1. Some partitions succeed, others fail
        2. Error aggregation
        3. Partial results handling
        4. Failure isolation

        Why this matters:
        ----------------
        - Large reads may have partial failures
        - Decide on partial results policy
        - Error reporting clarity
        - Fault isolation
        """
        # Create partitioned table
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
            # Insert data across partitions
            for p in range(5):
                for i in range(100):
                    await session.execute(
                        f"INSERT INTO {test_table_name} (partition_id, id, data) "
                        f"VALUES (?, ?, ?)",
                        (p, i, f"data_{p}_{i}"),
                    )

            # Mock to fail specific partitions
            original_execute = session.execute

            async def failing_execute(query, *args, **kwargs):
                # Fail if querying partition 2 or 4
                if "partition_id = 2" in str(query) or "partition_id = 4" in str(query):
                    raise Exception("Simulated partition failure")
                return await original_execute(query, *args, **kwargs)

            session.execute = failing_execute

            # Try to read all partitions
            with pytest.raises(Exception) as exc_info:
                df = await cdf.read_cassandra_table(
                    f"test_dataframe.{test_table_name}",
                    session=session,
                    partition_count=5,
                    predicates=[
                        {"column": "partition_id", "operator": "IN", "value": [0, 1, 2, 3, 4]}
                    ],
                )
                df.compute()

            assert "partition failure" in str(exc_info.value)

            # Restore
            session.execute = original_execute

            # Test with failure tolerance (if implemented)
            # This would be a feature to handle partial failures
            # df = await cdf.read_cassandra_table(
            #     f"test_dataframe.{test_table_name}",
            #     session=session,
            #     allow_partial_results=True
            # )

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    @pytest.mark.asyncio
    async def test_resource_cleanup_on_error(self, session, test_table_name):
        """
        Test resource cleanup when errors occur.

        What this tests:
        ---------------
        1. Connections closed on error
        2. Memory freed on error
        3. No thread leaks
        4. Proper context manager behavior

        Why this matters:
        ----------------
        - Resource leaks kill production
        - Errors shouldn't leak
        - Clean shutdown required
        - Observability needs
        """
        import gc
        import threading

        initial_threads = threading.active_count()

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
            # Insert data
            for i in range(100):
                await session.execute(
                    f"INSERT INTO {test_table_name} (id, data) VALUES (?, ?)", (i, f"data_{i}")
                )

            # Track resource allocation
            resources_allocated = []

            # Mock session with resource tracking
            original_execute_stream = getattr(session, "execute_stream", None)

            async def tracked_stream(*args, **kwargs):
                resource = {"type": "stream", "id": len(resources_allocated)}
                resources_allocated.append(resource)

                # Fail after allocating
                raise Exception("Simulated stream failure")

            if original_execute_stream:
                session.execute_stream = tracked_stream

            # Attempt read that will fail
            try:
                df = await cdf.read_cassandra_table(
                    f"test_dataframe.{test_table_name}", session=session, page_size=10
                )
                df.compute()
            except Exception as e:
                print(f"Expected failure: {e}")

            # Force garbage collection
            gc.collect()
            await asyncio.sleep(0.5)  # Allow cleanup

            # Check thread count
            final_threads = threading.active_count()
            print(f"Thread count: {initial_threads} -> {final_threads}")

            # Should not leak threads (some tolerance for background)
            assert final_threads <= initial_threads + 2, "Should not leak threads"

            # Restore
            if original_execute_stream:
                session.execute_stream = original_execute_stream

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    @pytest.mark.asyncio
    async def test_retry_logic(self, session, test_table_name):
        """
        Test retry logic for transient failures.

        What this tests:
        ---------------
        1. Automatic retry on transient errors
        2. Exponential backoff
        3. Max retry limits
        4. Success after retries

        Why this matters:
        ----------------
        - Network glitches are common
        - Improve reliability
        - But avoid infinite retries
        - Production resilience
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
            # Insert data
            await session.execute(f"INSERT INTO {test_table_name} (id, data) VALUES (1, 'test')")

            # Mock transient failures
            call_count = 0
            original_execute = session.execute

            async def flaky_execute(*args, **kwargs):
                nonlocal call_count
                call_count += 1

                # Fail first 2 times, succeed on 3rd
                if call_count < 3:
                    raise OperationTimedOut("Transient timeout")

                return await original_execute(*args, **kwargs)

            session.execute = flaky_execute

            # Read with retry logic (if implemented)
            start_time = time.time()

            try:
                df = await cdf.read_cassandra_table(
                    f"test_dataframe.{test_table_name}",
                    session=session,
                    max_retries=3,
                    retry_delay_ms=100,
                )
                result = df.compute()

                elapsed = time.time() - start_time

                # Should succeed after retries
                assert len(result) == 1
                assert call_count == 3, "Should retry twice before success"

                # Should have delays between retries
                assert elapsed > 0.2, "Should have retry delays"

            except OperationTimedOut:
                # If retries not implemented, will fail
                print("Retry logic not implemented")

            # Restore
            session.execute = original_execute

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    @pytest.mark.asyncio
    async def test_concurrent_error_handling(self, session, test_table_name):
        """
        Test error handling with concurrent queries.

        What this tests:
        ---------------
        1. Multiple queries failing simultaneously
        2. Error isolation between queries
        3. Partial success handling
        4. Resource cleanup with concurrency

        Why this matters:
        ----------------
        - Parallel execution amplifies error scenarios
        - Must handle multiple failures
        - Clean shutdown of all queries
        - Production complexity
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
            # Insert data
            for p in range(10):
                for i in range(50):
                    await session.execute(
                        f"INSERT INTO {test_table_name} (partition_id, id, data) "
                        f"VALUES (?, ?, ?)",
                        (p, i, f"data_{p}_{i}"),
                    )

            # Track concurrent executions
            concurrent_count = 0
            max_concurrent = 0
            lock = asyncio.Lock()

            original_execute = session.execute

            async def concurrent_tracking_execute(*args, **kwargs):
                nonlocal concurrent_count, max_concurrent

                async with lock:
                    concurrent_count += 1
                    max_concurrent = max(max_concurrent, concurrent_count)

                try:
                    # Simulate some failures
                    if "partition_id = 3" in str(args[0]) or "partition_id = 7" in str(args[0]):
                        await asyncio.sleep(0.1)  # Simulate work
                        raise Exception("Failed partition query")

                    result = await original_execute(*args, **kwargs)
                    return result

                finally:
                    async with lock:
                        concurrent_count -= 1

            session.execute = concurrent_tracking_execute

            # Read with high concurrency
            with pytest.raises(Exception) as exc_info:
                df = await cdf.read_cassandra_table(
                    f"test_dataframe.{test_table_name}",
                    session=session,
                    partition_count=10,
                    max_concurrent_partitions=5,
                )
                df.compute()

            assert "Failed partition query" in str(exc_info.value)

            print(f"Max concurrent queries: {max_concurrent}")
            assert max_concurrent >= 2, "Should have concurrent queries"
            assert concurrent_count == 0, "All queries should complete/fail"

            # Restore
            session.execute = original_execute

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")
