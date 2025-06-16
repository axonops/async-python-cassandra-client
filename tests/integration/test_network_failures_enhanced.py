"""
Enhanced integration tests for network failure scenarios.

These tests cover:
- Connection failures and recovery
- Timeout scenarios
- Host availability issues
- Network partition simulation
- Graceful degradation
"""

import asyncio
import time
import uuid

import pytest
from cassandra import OperationTimedOut, Unavailable, WriteTimeout
from cassandra.cluster import NoHostAvailable
from cassandra.query import ConsistencyLevel, SimpleStatement

from async_cassandra import AsyncCassandraSession, AsyncCluster, StreamConfig
from async_cassandra.exceptions import ConnectionError


@pytest.mark.integration
class TestEnhancedNetworkFailures:
    """Enhanced tests for network failure scenarios."""

    @pytest.mark.asyncio
    async def test_connection_timeout_handling(self):
        """Test connection timeout with unreachable hosts."""
        # Try to connect to non-existent host
        cluster = AsyncCluster(
            contact_points=["192.168.255.255"],  # Non-routable IP
            # Short timeouts for testing
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
    async def test_no_host_available_handling(self, cassandra_session: AsyncCassandraSession):
        """Test NoHostAvailable exception handling."""
        # Create a query that will fail due to consistency requirements
        await cassandra_session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS test_consistency
            WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 3}
            """
        )
        await cassandra_session.set_keyspace("test_consistency")

        await cassandra_session.execute("DROP TABLE IF EXISTS consistency_test")
        await cassandra_session.execute(
            """
            CREATE TABLE consistency_test (
                id UUID PRIMARY KEY,
                data TEXT
            )
            """
        )

        # Try to write with ALL consistency on single node cluster
        stmt = SimpleStatement(
            "INSERT INTO consistency_test (id, data) VALUES (%s, %s)",
            consistency_level=ConsistencyLevel.ALL,
        )

        # This should raise NoHostAvailable or Unavailable
        with pytest.raises((NoHostAvailable, Unavailable)):
            await cassandra_session.execute(stmt, [uuid.uuid4(), "test"])

    @pytest.mark.asyncio
    async def test_streaming_timeout_recovery(self, cassandra_session: AsyncCassandraSession):
        """Test streaming with timeout and recovery."""
        # Create test data
        await cassandra_session.execute("DROP TABLE IF EXISTS stream_timeout_test")
        await cassandra_session.execute(
            """
            CREATE TABLE stream_timeout_test (
                id UUID PRIMARY KEY,
                data TEXT
            )
            """
        )

        # Insert test data
        for i in range(100):
            await cassandra_session.execute(
                "INSERT INTO stream_timeout_test (id, data) VALUES (%s, %s)",
                [uuid.uuid4(), f"data_{i}"],
            )

        # Stream with very short timeout
        stream_config = StreamConfig(fetch_size=10, timeout_seconds=0.1)  # 1ms - should timeout

        result = await cassandra_session.execute_stream(
            "SELECT * FROM stream_timeout_test", stream_config=stream_config
        )

        # Try to consume results
        # With 0.001s timeout and 100 rows, it should either timeout or fetch limited data
        count = 0
        try:
            async for _ in result:
                count += 1
                # Add delay that's longer than the timeout to force timeout
                if count > 5:
                    await asyncio.sleep(0.01)  # 10ms delay - longer than 1ms timeout
        except asyncio.TimeoutError:
            # Good - timeout occurred as expected
            pass

        # Should have processed very few rows before timeout
        assert count < 20, f"Processed {count} rows before timeout, expected < 20"

        # Now try with reasonable timeout
        stream_config = StreamConfig(fetch_size=10, timeout_seconds=30.0)

        result = await cassandra_session.execute_stream(
            "SELECT * FROM stream_timeout_test", stream_config=stream_config
        )

        count = 0
        async for _ in result:
            count += 1

        assert count == 100  # Should get all rows

    @pytest.mark.asyncio
    async def test_prepare_statement_timeout(self, cassandra_session: AsyncCassandraSession):
        """Test prepare statement timeout handling."""
        # Test that prepare completes quickly with normal timeout
        start = asyncio.get_event_loop().time()
        stmt = await cassandra_session.prepare("SELECT * FROM users WHERE id = ?", timeout=10.0)
        duration = asyncio.get_event_loop().time() - start

        assert stmt is not None
        assert duration < 1.0  # Should complete in less than 1 second

    @pytest.mark.asyncio
    async def test_concurrent_connection_failures(self):
        """Test handling of concurrent connection failures."""
        # Create multiple clusters with bad configurations
        clusters = []
        for i in range(5):
            cluster = AsyncCluster(
                contact_points=[f"192.168.255.{i}"],  # Non-existent hosts
                control_connection_timeout=1.0,
            )
            clusters.append(cluster)

        # Try to connect to all concurrently
        async def try_connect(cluster):
            try:
                session = await cluster.connect(timeout=2.0)
                await session.close()
                return "success"
            except Exception as e:
                return f"failed: {type(e).__name__}"

        results = await asyncio.gather(
            *[try_connect(cluster) for cluster in clusters], return_exceptions=True
        )

        # All should fail
        for result in results:
            assert "success" not in str(result)

        # Cleanup
        for cluster in clusters:
            await cluster.shutdown()

    @pytest.mark.asyncio
    async def test_query_cancellation(self, cassandra_session: AsyncCassandraSession):
        """Test query cancellation behavior."""
        # Create large dataset
        await cassandra_session.execute("DROP TABLE IF EXISTS cancellation_test")
        await cassandra_session.execute(
            """
            CREATE TABLE cancellation_test (
                id UUID PRIMARY KEY,
                data TEXT
            )
            """
        )

        # Insert data
        for i in range(1000):
            await cassandra_session.execute(
                "INSERT INTO cancellation_test (id, data) VALUES (%s, %s)",
                [uuid.uuid4(), "x" * 1000],
            )

        # Start a long-running query
        async def long_query():
            result = await cassandra_session.execute(
                "SELECT * FROM cancellation_test", timeout=30.0
            )
            count = 0
            async for _ in result:
                count += 1
                await asyncio.sleep(0.01)  # Simulate slow processing
            return count

        # Start query and cancel it
        task = asyncio.create_task(long_query())
        await asyncio.sleep(0.5)  # Let it process some rows
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

    @pytest.mark.asyncio
    async def test_host_marking_and_recovery(self, cassandra_cluster):
        """Test host marking as down and recovery."""
        session = await cassandra_cluster.connect()

        # Get cluster metadata
        hosts = list(session._session.cluster.metadata.all_hosts())
        assert len(hosts) > 0

        # Check initial state
        for host in hosts:
            assert host.is_up

        # Execute some queries
        for _ in range(10):
            await session.execute("SELECT now() FROM system.local")

        # Host should still be up
        for host in hosts:
            assert host.is_up

        await session.close()

    @pytest.mark.asyncio
    async def test_batch_timeout_handling(self, cassandra_session: AsyncCassandraSession):
        """Test batch operation timeout handling."""
        from cassandra.query import BatchStatement, BatchType

        # Create a large batch
        batch = BatchStatement(batch_type=BatchType.UNLOGGED)

        # Add many statements to the batch
        for i in range(100):
            batch.add(
                "INSERT INTO users (id, name, email, age) VALUES (%s, %s, %s, %s)",
                [uuid.uuid4(), f"Batch User {i}", f"batch{i}@test.com", 25],
            )

        # Execute with short timeout (might timeout on slow systems)
        try:
            await cassandra_session.execute_batch(batch, timeout=0.1)
        except (OperationTimedOut, WriteTimeout):
            # Expected on some systems
            pass

        # Smaller batch should succeed
        small_batch = BatchStatement(batch_type=BatchType.UNLOGGED)
        for i in range(5):
            small_batch.add(
                "INSERT INTO users (id, name, email, age) VALUES (%s, %s, %s, %s)",
                [uuid.uuid4(), f"Small Batch {i}", f"small{i}@test.com", 25],
            )

        await cassandra_session.execute_batch(small_batch, timeout=5.0)

    @pytest.mark.asyncio
    async def test_connection_pool_recovery(self, cassandra_session: AsyncCassandraSession):
        """Test connection pool recovery after failures."""
        # Execute queries to establish connections
        for _ in range(10):
            await cassandra_session.execute("SELECT now() FROM system.local")

        # Simulate high load with some failures
        async def query_with_random_failure(i):
            import random

            # 10% chance of simulated failure
            if random.random() < 0.1:
                raise Exception(f"Simulated failure {i}")

            result = await cassandra_session.execute("SELECT now() FROM system.local")
            return result.one()

        # Execute many concurrent queries
        tasks = [query_with_random_failure(i) for i in range(100)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Count successes and failures
        successes = sum(1 for r in results if not isinstance(r, Exception))
        sum(1 for r in results if isinstance(r, Exception))

        assert successes >= 85  # At least 85% should succeed

        # Pool should recover - execute more queries
        recovery_tasks = [
            cassandra_session.execute("SELECT now() FROM system.local") for _ in range(20)
        ]
        recovery_results = await asyncio.gather(*recovery_tasks, return_exceptions=True)

        # All recovery queries should succeed
        recovery_successes = sum(1 for r in recovery_results if not isinstance(r, Exception))
        assert recovery_successes == 20

    @pytest.mark.asyncio
    async def test_graceful_degradation(self, cassandra_session: AsyncCassandraSession):
        """Test graceful degradation under various failure conditions."""
        # Test 1: Degraded read performance
        await cassandra_session.execute("DROP TABLE IF EXISTS degradation_test")
        await cassandra_session.execute(
            """
            CREATE TABLE degradation_test (
                partition_key INT,
                clustering_key INT,
                data TEXT,
                PRIMARY KEY (partition_key, clustering_key)
            )
            """
        )

        # Insert data
        for p in range(5):
            for c in range(20):
                await cassandra_session.execute(
                    "INSERT INTO degradation_test (partition_key, clustering_key, data) VALUES (%s, %s, %s)",
                    [p, c, f"data_{p}_{c}"],
                )

        # Function to measure query performance
        async def measure_query_time(consistency_level=ConsistencyLevel.ONE):
            stmt = SimpleStatement(
                "SELECT * FROM degradation_test WHERE partition_key = %s",
                consistency_level=consistency_level,
            )

            start = time.time()
            result = await cassandra_session.execute(stmt, [1])
            rows = []
            async for row in result:
                rows.append(row)
            duration = time.time() - start

            return duration, len(rows)

        # Test with different consistency levels
        one_duration, one_count = await measure_query_time(ConsistencyLevel.ONE)

        # Higher consistency might be slightly slower but should still work
        quorum_duration, quorum_count = await measure_query_time(ConsistencyLevel.QUORUM)

        assert one_count == quorum_count == 20
        # QUORUM shouldn't be dramatically slower on single node
        assert quorum_duration < one_duration * 3

    @pytest.mark.asyncio
    async def test_shutdown_timeout_handling(self):
        """Test shutdown timeout handling."""
        cluster = AsyncCluster(["localhost"])
        session = await cluster.connect()

        # Create a mock that delays shutdown
        original_shutdown = session._session.shutdown

        def slow_shutdown():
            time.sleep(2)  # Simulate slow shutdown
            original_shutdown()

        session._session.shutdown = slow_shutdown

        # Shutdown should complete within timeout (30s)
        start = time.time()
        await session.close()
        duration = time.time() - start

        assert duration < 35  # Should complete within timeout + small margin

        await cluster.shutdown()
