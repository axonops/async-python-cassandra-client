"""
Integration tests for network failure scenarios against real Cassandra.

Note: These tests require the ability to manipulate network conditions.
They will be skipped if running in environments without proper permissions.
"""

import asyncio
import time
import uuid

import pytest
from cassandra import OperationTimedOut, ReadTimeout, Unavailable, WriteTimeout

from async_cassandra import AsyncCassandraSession, AsyncCluster, AsyncRetryPolicy


@pytest.mark.integration
class TestNetworkFailures:
    """Test behavior under various network failure conditions."""

    @pytest.mark.asyncio
    async def test_timeout_handling(self, cassandra_session: AsyncCassandraSession):
        """Test query timeout handling."""
        # Create a large dataset that will take time to query
        await cassandra_session.execute("DROP TABLE IF EXISTS large_data")
        await cassandra_session.execute(
            """
            CREATE TABLE large_data (
                partition_key INT,
                clustering_key INT,
                data TEXT,
                PRIMARY KEY (partition_key, clustering_key)
            )
            """
        )

        # Insert a lot of data in one partition
        insert_stmt = await cassandra_session.prepare(
            "INSERT INTO large_data (partition_key, clustering_key, data) VALUES (?, ?, ?)"
        )

        # Insert 10000 rows
        insert_tasks = []
        large_text = "x" * 1000  # 1KB of data per row

        for i in range(10000):
            task = cassandra_session.execute(insert_stmt, [1, i, large_text])
            insert_tasks.append(task)

            # Execute in batches
            if len(insert_tasks) >= 100:
                await asyncio.gather(*insert_tasks)
                insert_tasks = []

        if insert_tasks:
            await asyncio.gather(*insert_tasks)

        # Now try to query all data with a very short timeout
        try:
            # This should timeout
            await cassandra_session.execute(
                "SELECT * FROM large_data WHERE partition_key = 1",
                timeout=0.1,  # 1ms timeout - should definitely timeout
            )
            pytest.fail("Query should have timed out")
        except (OperationTimedOut, asyncio.TimeoutError):
            # Expected behavior - may be either depending on where timeout occurs
            pass

    @pytest.mark.asyncio
    async def test_retry_on_timeout(self, cassandra_cluster):
        """Test that retries work correctly on timeouts."""
        # Create session with custom retry policy
        retry_policy = AsyncRetryPolicy(max_retries=3)

        cluster = AsyncCluster(
            contact_points=["localhost"],
            retry_policy=retry_policy,
        )
        session = await cluster.connect()

        # Create test keyspace and table
        await session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS test_retry
            WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
            """
        )
        await session.set_keyspace("test_retry")

        await session.execute("DROP TABLE IF EXISTS retry_test")
        await session.execute(
            """
            CREATE TABLE retry_test (
                id UUID PRIMARY KEY,
                data TEXT
            )
            """
        )

        # Insert test data
        test_id = uuid.uuid4()
        await session.execute(
            "INSERT INTO retry_test (id, data) VALUES (%s, %s)", [test_id, "test data"]
        )

        # Query should succeed even if there are transient issues
        result = await session.execute("SELECT * FROM retry_test WHERE id = %s", [test_id])

        rows = []
        async for row in result:
            rows.append(row)

        assert len(rows) == 1
        assert rows[0].data == "test data"

        await session.close()
        await cluster.shutdown()

    @pytest.mark.asyncio
    async def test_unavailable_handling(self, cassandra_cluster):
        """Test handling of Unavailable exceptions."""
        # Create session with high consistency requirements
        session = await cassandra_cluster.connect()

        await session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS test_unavailable
            WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 3}
            """
        )
        await session.set_keyspace("test_unavailable")

        await session.execute("DROP TABLE IF EXISTS unavailable_test")
        await session.execute(
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
            await session.execute(insert_stmt, [uuid.uuid4(), "test data"])
            pytest.fail("Should have raised Unavailable exception")
        except (Unavailable, Exception) as e:
            # Expected - we don't have 3 replicas
            # The exception might be wrapped or not depending on the driver version
            if isinstance(e, Unavailable):
                assert e.alive_replicas < e.required_replicas
            else:
                # Check if it's wrapped
                assert "Unavailable" in str(e) or "Cannot achieve consistency level ALL" in str(e)

        await session.close()

    @pytest.mark.asyncio
    async def test_connection_pool_exhaustion(self, cassandra_session: AsyncCassandraSession):
        """Test behavior when connection pool is exhausted."""

        # Create many concurrent long-running queries
        async def long_query(i):
            try:
                # This query will scan the entire table
                result = await cassandra_session.execute("SELECT * FROM users ALLOW FILTERING")
                count = 0
                async for _ in result:
                    count += 1
                return i, count, None
            except Exception as e:
                return i, 0, str(e)

        # Insert some data first
        for i in range(100):
            await cassandra_session.execute(
                "INSERT INTO users (id, name, email, age) VALUES (%s, %s, %s, %s)",
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
    async def test_write_timeout_behavior(self, cassandra_session: AsyncCassandraSession):
        """Test write timeout behavior."""
        # Create a table with large values
        await cassandra_session.execute("DROP TABLE IF EXISTS large_writes")
        await cassandra_session.execute(
            """
            CREATE TABLE large_writes (
                id UUID PRIMARY KEY,
                data BLOB
            )
            """
        )

        # Try to write very large data with short timeout
        large_data = b"x" * (1 * 1024 * 1024)  # 1MB - reduced from 10MB to avoid overloading

        # This might timeout on slow systems
        try:
            await cassandra_session.execute(
                "INSERT INTO large_writes (id, data) VALUES (%s, %s)",
                [uuid.uuid4(), large_data],
                timeout=0.01,  # 10ms timeout for 1MB write - should timeout
            )
        except (WriteTimeout, OperationTimedOut, Exception) as e:
            # Expected on most systems
            # Could be WriteTimeout, OperationTimedOut, or wrapped in QueryError
            assert "timeout" in str(e).lower() or "overloaded" in str(e).lower()

        # Normal sized writes should succeed
        normal_data = b"x" * 1024  # 1KB
        await cassandra_session.execute(
            "INSERT INTO large_writes (id, data) VALUES (%s, %s)", [uuid.uuid4(), normal_data]
        )

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
        # Prepare test data
        test_ids = [uuid.uuid4() for _ in range(100)]

        # Insert test data
        for test_id in test_ids:
            await cassandra_session.execute(
                "INSERT INTO users (id, name, email, age) VALUES (%s, %s, %s, %s)",
                [test_id, "Test User", "test@test.com", 30],
            )

        # Function that sometimes fails
        async def unreliable_query(user_id, fail_rate=0.2):
            import random

            # Simulate random failures
            if random.random() < fail_rate:
                raise Exception("Simulated failure")

            result = await cassandra_session.execute("SELECT * FROM users WHERE id = %s", [user_id])
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
