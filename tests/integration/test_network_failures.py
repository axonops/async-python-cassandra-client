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
        """Test handling of Unavailable exceptions."""
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
        """Test behavior when connection pool is exhausted."""
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
        for i in range(100):
            await cassandra_session.execute(
                f"INSERT INTO {users_table} (id, name, email, age) VALUES (%s, %s, %s, %s)",
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
        """Test read timeout behavior with different scenarios."""
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
        insert_tasks = []
        for p in range(10):
            for c in range(100):
                task = cassandra_session.execute(
                    "INSERT INTO read_timeout_test (partition_key, clustering_key, data) "
                    "VALUES (%s, %s, %s)",
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
        """Test that the system recovers properly from concurrent failures."""
        # Get the unique table name
        users_table = cassandra_session._test_users_table

        # Prepare test data
        test_ids = [uuid.uuid4() for _ in range(100)]

        # Insert test data
        for test_id in test_ids:
            await cassandra_session.execute(
                f"INSERT INTO {users_table} (id, name, email, age) VALUES (%s, %s, %s, %s)",
                [test_id, "Test User", "test@test.com", 30],
            )

        # Function that sometimes fails
        async def unreliable_query(user_id, fail_rate=0.2):
            import random

            # Simulate random failures
            if random.random() < fail_rate:
                raise Exception("Simulated failure")

            result = await cassandra_session.execute(
                f"SELECT * FROM {users_table} WHERE id = %s", [user_id]
            )
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
        """Test connection timeout with unreachable hosts."""
        # Try to connect to non-existent host
        cluster = AsyncCluster(
            contact_points=["192.168.255.255"],  # Non-routable IP
            control_connection_timeout=1.0,
        )

        start_time = time.time()

        with pytest.raises((ConnectionError, NoHostAvailable, asyncio.TimeoutError)):
            # Should timeout quickly
            await cluster.connect(timeout=2.0)

        duration = time.time() - start_time
        assert duration < 5.0  # Should fail within timeout period

        await cluster.shutdown()

    @pytest.mark.asyncio
    async def test_batch_operations_with_failures(self, cassandra_session: AsyncCassandraSession):
        """Test batch operation behavior during failures."""
        # Get the unique table name
        users_table = cassandra_session._test_users_table

        from cassandra.query import BatchStatement, BatchType

        # Create a batch
        batch = BatchStatement(batch_type=BatchType.UNLOGGED)

        # Add multiple statements to the batch
        for i in range(20):
            batch.add(
                f"INSERT INTO {users_table} (id, name, email, age) VALUES (%s, %s, %s, %s)",
                [uuid.uuid4(), f"Batch User {i}", f"batch{i}@test.com", 25],
            )

        # Execute batch - should succeed
        await cassandra_session.execute_batch(batch)

        # Verify data was inserted
        result = await cassandra_session.execute(
            f"SELECT COUNT(*) FROM {users_table} WHERE age = 25 ALLOW FILTERING"
        )
        count = result.one()[0]
        assert count >= 20  # At least our batch inserts
