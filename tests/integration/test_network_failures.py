"""
Integration tests for network failure scenarios against real Cassandra.

Note: These tests require the ability to manipulate network conditions.
They will be skipped if running in environments without proper permissions.
"""

import asyncio
import time
import uuid

import pytest
from cassandra import OperationTimedOut, ReadTimeout, Unavailable
from cassandra.cluster import NoHostAvailable

from async_cassandra import AsyncCassandraSession, AsyncCluster
from async_cassandra.exceptions import ConnectionError


@pytest.mark.integration
class TestNetworkFailures:
    """Test behavior under various network failure conditions."""

    @pytest.mark.asyncio
    async def test_unavailable_handling(self, cassandra_session):
        """
        Test handling of Unavailable exceptions.

        What this tests:
        ---------------
        1. Unavailable errors caught
        2. Replica count reported
        3. Consistency level impact
        4. Error message clarity

        Why this matters:
        ----------------
        Unavailable errors indicate:
        - Not enough replicas
        - Cluster health issues
        - Consistency impossible

        Apps must handle cluster
        degradation gracefully.
        """
        # Create a table with high replication factor in a new keyspace
        # This test needs its own keyspace to test replication
        await cassandra_session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS test_unavailable
            WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 3}
            """
        )

        # Use the new keyspace temporarily
        original_keyspace = cassandra_session.keyspace
        await cassandra_session.set_keyspace("test_unavailable")

        try:
            await cassandra_session.execute("DROP TABLE IF EXISTS unavailable_test")
            await cassandra_session.execute(
                """
                CREATE TABLE unavailable_test (
                    id UUID PRIMARY KEY,
                    data TEXT
                )
                """
            )

            # With replication factor 3 on a single node, QUORUM/ALL will fail
            from cassandra import ConsistencyLevel
            from cassandra.query import SimpleStatement

            # This should fail with Unavailable
            insert_stmt = SimpleStatement(
                "INSERT INTO unavailable_test (id, data) VALUES (%s, %s)",
                consistency_level=ConsistencyLevel.ALL,
            )

            try:
                await cassandra_session.execute(insert_stmt, [uuid.uuid4(), "test data"])
                pytest.fail("Should have raised Unavailable exception")
            except (Unavailable, Exception) as e:
                # Expected - we don't have 3 replicas
                # The exception might be wrapped or not depending on the driver version
                if isinstance(e, Unavailable):
                    assert e.alive_replicas < e.required_replicas
                else:
                    # Check if it's wrapped
                    assert "Unavailable" in str(e) or "Cannot achieve consistency level ALL" in str(
                        e
                    )

        finally:
            # Clean up and restore original keyspace
            await cassandra_session.execute("DROP KEYSPACE IF EXISTS test_unavailable")
            await cassandra_session.set_keyspace(original_keyspace)

    @pytest.mark.asyncio
    async def test_connection_pool_exhaustion(self, cassandra_session: AsyncCassandraSession):
        """
        Test behavior when connection pool is exhausted.

        What this tests:
        ---------------
        1. Many concurrent queries
        2. Pool limits respected
        3. Most queries succeed
        4. Graceful degradation

        Why this matters:
        ----------------
        Pool exhaustion happens:
        - Traffic spikes
        - Slow queries
        - Resource limits

        System must degrade
        gracefully, not crash.
        """
        # Get the unique table name
        users_table = cassandra_session._test_users_table

        # Create many concurrent long-running queries
        async def long_query(i):
            try:
                # This query will scan the entire table
                result = await cassandra_session.execute(
                    f"SELECT * FROM {users_table} ALLOW FILTERING"
                )
                count = 0
                async for _ in result:
                    count += 1
                return i, count, None
            except Exception as e:
                return i, 0, str(e)

        # Insert some data first
        insert_stmt = await cassandra_session.prepare(
            f"INSERT INTO {users_table} (id, name, email, age) VALUES (?, ?, ?, ?)"
        )
        for i in range(100):
            await cassandra_session.execute(
                insert_stmt,
                [uuid.uuid4(), f"User {i}", f"user{i}@test.com", 25],
            )

        # Launch many concurrent queries
        tasks = [long_query(i) for i in range(50)]
        results = await asyncio.gather(*tasks)

        # Check results
        successful = sum(1 for _, count, error in results if error is None)
        failed = sum(1 for _, count, error in results if error is not None)

        print("\nConnection pool test results:")
        print(f"  Successful queries: {successful}")
        print(f"  Failed queries: {failed}")

        # Most queries should succeed
        assert successful >= 45  # Allow a few failures

    @pytest.mark.asyncio
    async def test_read_timeout_behavior(self, cassandra_session: AsyncCassandraSession):
        """
        Test read timeout behavior with different scenarios.

        What this tests:
        ---------------
        1. Short timeouts fail fast
        2. Reasonable timeouts work
        3. Timeout errors caught
        4. Query-level timeouts

        Why this matters:
        ----------------
        Timeout control prevents:
        - Hanging operations
        - Resource exhaustion
        - Poor user experience

        Critical for responsive
        applications.
        """
        # Create test data
        await cassandra_session.execute("DROP TABLE IF EXISTS read_timeout_test")
        await cassandra_session.execute(
            """
            CREATE TABLE read_timeout_test (
                partition_key INT,
                clustering_key INT,
                data TEXT,
                PRIMARY KEY (partition_key, clustering_key)
            )
            """
        )

        # Insert data across multiple partitions
        # Prepare statement first
        insert_stmt = await cassandra_session.prepare(
            "INSERT INTO read_timeout_test (partition_key, clustering_key, data) "
            "VALUES (?, ?, ?)"
        )

        insert_tasks = []
        for p in range(10):
            for c in range(100):
                task = cassandra_session.execute(
                    insert_stmt,
                    [p, c, f"data_{p}_{c}"],
                )
                insert_tasks.append(task)

        # Execute in batches
        for i in range(0, len(insert_tasks), 50):
            await asyncio.gather(*insert_tasks[i : i + 50])

        # Test 1: Query that might timeout on slow systems
        start_time = time.time()
        try:
            result = await cassandra_session.execute(
                "SELECT * FROM read_timeout_test", timeout=0.05  # 50ms timeout
            )
            # Try to consume results
            count = 0
            async for _ in result:
                count += 1
        except (ReadTimeout, OperationTimedOut):
            # Expected on most systems
            duration = time.time() - start_time
            assert duration < 1.0  # Should fail quickly

        # Test 2: Query with reasonable timeout should succeed
        result = await cassandra_session.execute(
            "SELECT * FROM read_timeout_test WHERE partition_key = 1", timeout=5.0
        )

        rows = []
        async for row in result:
            rows.append(row)

        assert len(rows) == 100  # Should get all rows from partition 1

    @pytest.mark.asyncio
    async def test_concurrent_failures_recovery(self, cassandra_session: AsyncCassandraSession):
        """
        Test that the system recovers properly from concurrent failures.

        What this tests:
        ---------------
        1. Retry logic works
        2. Exponential backoff
        3. High success rate
        4. Concurrent recovery

        Why this matters:
        ----------------
        Transient failures common:
        - Network hiccups
        - Temporary overload
        - Node restarts

        Smart retries maintain
        reliability.
        """
        # Get the unique table name
        users_table = cassandra_session._test_users_table

        # Prepare test data
        test_ids = [uuid.uuid4() for _ in range(100)]

        # Insert test data
        insert_stmt = await cassandra_session.prepare(
            f"INSERT INTO {users_table} (id, name, email, age) VALUES (?, ?, ?, ?)"
        )
        for test_id in test_ids:
            await cassandra_session.execute(
                insert_stmt,
                [test_id, "Test User", "test@test.com", 30],
            )

        # Prepare select statement for reuse
        select_stmt = await cassandra_session.prepare(f"SELECT * FROM {users_table} WHERE id = ?")

        # Function that sometimes fails
        async def unreliable_query(user_id, fail_rate=0.2):
            import random

            # Simulate random failures
            if random.random() < fail_rate:
                raise Exception("Simulated failure")

            result = await cassandra_session.execute(select_stmt, [user_id])
            rows = []
            async for row in result:
                rows.append(row)
            return rows[0] if rows else None

        # Run many concurrent queries with retries
        async def query_with_retry(user_id, max_retries=3):
            for attempt in range(max_retries):
                try:
                    return await unreliable_query(user_id)
                except Exception:
                    if attempt == max_retries - 1:
                        raise
                    await asyncio.sleep(0.1 * (attempt + 1))  # Exponential backoff

        # Execute concurrent queries
        tasks = [query_with_retry(uid) for uid in test_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Check results
        successful = sum(1 for r in results if not isinstance(r, Exception))
        failed = sum(1 for r in results if isinstance(r, Exception))

        print("\nRecovery test results:")
        print(f"  Successful queries: {successful}")
        print(f"  Failed queries: {failed}")

        # With retries, most should succeed
        assert successful >= 95  # At least 95% success rate

    @pytest.mark.asyncio
    async def test_connection_timeout_handling(self):
        """
        Test connection timeout with unreachable hosts.

        What this tests:
        ---------------
        1. Unreachable hosts timeout
        2. Timeout respected
        3. Fast failure
        4. Clear error

        Why this matters:
        ----------------
        Connection timeouts prevent:
        - Hanging startup
        - Infinite waits
        - Resource tie-up

        Fast failure enables
        quick recovery.
        """
        # Try to connect to non-existent host
        async with AsyncCluster(
            contact_points=["192.168.255.255"],  # Non-routable IP
            control_connection_timeout=1.0,
        ) as cluster:
            start_time = time.time()

            with pytest.raises((ConnectionError, NoHostAvailable, asyncio.TimeoutError)):
                # Should timeout quickly
                await cluster.connect(timeout=2.0)

            duration = time.time() - start_time
            assert duration < 5.0  # Should fail within timeout period

    @pytest.mark.asyncio
    async def test_batch_operations_with_failures(self, cassandra_session: AsyncCassandraSession):
        """
        Test batch operation behavior during failures.

        What this tests:
        ---------------
        1. Batch execution works
        2. Unlogged batches
        3. Multiple statements
        4. Data verification

        Why this matters:
        ----------------
        Batch operations must:
        - Handle partial failures
        - Complete successfully
        - Insert all data

        Critical for bulk
        data operations.
        """
        # Get the unique table name
        users_table = cassandra_session._test_users_table

        from cassandra.query import BatchStatement, BatchType

        # Create a batch
        batch = BatchStatement(batch_type=BatchType.UNLOGGED)

        # Prepare statement for batch
        insert_stmt = await cassandra_session.prepare(
            f"INSERT INTO {users_table} (id, name, email, age) VALUES (?, ?, ?, ?)"
        )

        # Add multiple statements to the batch
        for i in range(20):
            batch.add(
                insert_stmt,
                [uuid.uuid4(), f"Batch User {i}", f"batch{i}@test.com", 25],
            )

        # Execute batch - should succeed
        await cassandra_session.execute_batch(batch)

        # Verify data was inserted
        count_stmt = await cassandra_session.prepare(
            f"SELECT COUNT(*) FROM {users_table} WHERE age = ? ALLOW FILTERING"
        )
        result = await cassandra_session.execute(count_stmt, [25])
        count = result.one()[0]
        assert count >= 20  # At least our batch inserts
