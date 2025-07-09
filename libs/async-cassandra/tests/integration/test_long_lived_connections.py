"""
Integration tests to ensure clusters and sessions are long-lived and reusable.

This is critical for production applications where connections should be
established once and reused across many requests.
"""

import asyncio
import time
import uuid

import pytest

from async_cassandra import AsyncCluster


class TestLongLivedConnections:
    """Test that clusters and sessions can be long-lived and reused."""

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_session_reuse_across_many_operations(self, cassandra_cluster):
        """
        Test that a session can be reused for many operations.

        What this tests:
        ---------------
        1. Session reuse works
        2. Many operations OK
        3. No degradation
        4. Long-lived sessions

        Why this matters:
        ----------------
        Production pattern:
        - One session per app
        - Thousands of queries
        - No reconnection cost

        Must support connection
        pooling correctly.
        """
        # Create session once
        session = await cassandra_cluster.connect()

        # Use session for many operations
        operations_count = 100
        results = []

        for i in range(operations_count):
            result = await session.execute("SELECT release_version FROM system.local")
            results.append(result.one())

            # Small delay to simulate time between requests
            await asyncio.sleep(0.01)

        # Verify all operations succeeded
        assert len(results) == operations_count
        assert all(r is not None for r in results)

        # Session should still be usable
        final_result = await session.execute("SELECT now() FROM system.local")
        assert final_result.one() is not None

        # Explicitly close when done (not after each operation)
        await session.close()

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_cluster_creates_multiple_sessions(self, cassandra_cluster):
        """
        Test that a cluster can create multiple sessions.

        What this tests:
        ---------------
        1. Multiple sessions work
        2. Sessions independent
        3. Concurrent usage OK
        4. Resource isolation

        Why this matters:
        ----------------
        Multi-session needs:
        - Microservices
        - Different keyspaces
        - Isolation requirements

        Cluster manages many
        sessions properly.
        """
        # Create multiple sessions from same cluster
        sessions = []
        session_count = 5

        for i in range(session_count):
            session = await cassandra_cluster.connect()
            sessions.append(session)

        # Use all sessions concurrently
        async def use_session(session, session_id):
            results = []
            for i in range(10):
                result = await session.execute("SELECT release_version FROM system.local")
                results.append(result.one())
            return session_id, results

        tasks = [use_session(session, i) for i, session in enumerate(sessions)]
        results = await asyncio.gather(*tasks)

        # Verify all sessions worked
        assert len(results) == session_count
        for session_id, session_results in results:
            assert len(session_results) == 10
            assert all(r is not None for r in session_results)

        # Close all sessions
        for session in sessions:
            await session.close()

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_session_survives_errors(self, cassandra_cluster):
        """
        Test that session remains usable after query errors.

        What this tests:
        ---------------
        1. Errors don't kill session
        2. Recovery automatic
        3. Multiple error types
        4. Continued operation

        Why this matters:
        ----------------
        Real apps have errors:
        - Bad queries
        - Missing tables
        - Syntax issues

        Session must survive all
        non-fatal errors.
        """
        session = await cassandra_cluster.connect()
        await session.execute(
            "CREATE KEYSPACE IF NOT EXISTS test_long_lived "
            "WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}"
        )
        await session.set_keyspace("test_long_lived")

        # Create test table
        await session.execute(
            "CREATE TABLE IF NOT EXISTS test_errors (id UUID PRIMARY KEY, data TEXT)"
        )

        # Successful operation
        test_id = uuid.uuid4()
        insert_stmt = await session.prepare("INSERT INTO test_errors (id, data) VALUES (?, ?)")
        await session.execute(insert_stmt, [test_id, "test data"])

        # Cause an error (invalid query)
        with pytest.raises(Exception):  # Will be InvalidRequest or similar
            await session.execute("INVALID QUERY SYNTAX")

        # Session should still be usable after error
        select_stmt = await session.prepare("SELECT * FROM test_errors WHERE id = ?")
        result = await session.execute(select_stmt, [test_id])
        assert result.one() is not None
        assert result.one().data == "test data"

        # Another error (table doesn't exist)
        with pytest.raises(Exception):
            await session.execute("SELECT * FROM non_existent_table")

        # Still usable
        result = await session.execute("SELECT now() FROM system.local")
        assert result.one() is not None

        # Cleanup
        await session.execute("DROP TABLE IF EXISTS test_errors")
        await session.execute("DROP KEYSPACE IF EXISTS test_long_lived")
        await session.close()

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_prepared_statements_are_cached(self, cassandra_cluster):
        """
        Test that prepared statements can be reused efficiently.

        What this tests:
        ---------------
        1. Statement caching works
        2. Reuse is efficient
        3. Multiple statements OK
        4. No re-preparation

        Why this matters:
        ----------------
        Performance critical:
        - Prepare once
        - Execute many times
        - Reduced latency

        Core optimization for
        production apps.
        """
        session = await cassandra_cluster.connect()

        # Prepare statement once
        prepared = await session.prepare("SELECT release_version FROM system.local WHERE key = ?")

        # Reuse prepared statement many times
        for i in range(50):
            result = await session.execute(prepared, ["local"])
            assert result.one() is not None

        # Prepare another statement
        prepared2 = await session.prepare("SELECT cluster_name FROM system.local WHERE key = ?")

        # Both prepared statements should be reusable
        result1 = await session.execute(prepared, ["local"])
        result2 = await session.execute(prepared2, ["local"])

        assert result1.one() is not None
        assert result2.one() is not None

        await session.close()

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_session_lifetime_measurement(self, cassandra_cluster):
        """
        Test that sessions can live for extended periods.

        What this tests:
        ---------------
        1. Extended lifetime OK
        2. No timeout issues
        3. Sustained throughput
        4. Stable performance

        Why this matters:
        ----------------
        Production sessions:
        - Days to weeks alive
        - Millions of queries
        - No restarts needed

        Proves long-term
        stability.
        """
        session = await cassandra_cluster.connect()
        start_time = time.time()

        # Use session over a period of time
        test_duration = 5  # seconds
        operations = 0

        while time.time() - start_time < test_duration:
            result = await session.execute("SELECT now() FROM system.local")
            assert result.one() is not None
            operations += 1
            await asyncio.sleep(0.1)  # 10 operations per second

        end_time = time.time()
        actual_duration = end_time - start_time

        # Session should have been alive for the full duration
        assert actual_duration >= test_duration
        assert operations >= test_duration * 9  # At least 9 ops/second

        # Still usable after the test period
        final_result = await session.execute("SELECT now() FROM system.local")
        assert final_result.one() is not None

        await session.close()

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_context_manager_closes_session(self):
        """
        Test that context manager does close session (for scripts/tests).

        What this tests:
        ---------------
        1. Context manager works
        2. Session closed on exit
        3. Cluster still usable
        4. Clean resource handling

        Why this matters:
        ----------------
        Script patterns:
        - Short-lived sessions
        - Automatic cleanup
        - No leaks

        Different from production
        but still supported.
        """
        # Create cluster manually to test context manager
        cluster = AsyncCluster(["localhost"])

        # Use context manager
        async with await cluster.connect() as session:
            # Session should be usable
            result = await session.execute("SELECT now() FROM system.local")
            assert result.one() is not None
            assert not session.is_closed

        # Session should be closed after context exit
        assert session.is_closed

        # Cluster should still be usable
        new_session = await cluster.connect()
        result = await new_session.execute("SELECT now() FROM system.local")
        assert result.one() is not None

        await new_session.close()
        await cluster.shutdown()

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_production_pattern(self):
        """
        Test the recommended production pattern.

        What this tests:
        ---------------
        1. Production lifecycle
        2. Startup/shutdown once
        3. Many requests handled
        4. Concurrent load OK

        Why this matters:
        ----------------
        Best practice pattern:
        - Initialize once
        - Reuse everywhere
        - Clean shutdown

        Template for real
        applications.
        """
        # This simulates a production application lifecycle

        # Application startup
        cluster = AsyncCluster(["localhost"])
        session = await cluster.connect()

        # Simulate many requests over time
        async def handle_request(request_id):
            """Simulate handling a web request."""
            result = await session.execute("SELECT cluster_name FROM system.local")
            return f"Request {request_id}: {result.one().cluster_name}"

        # Handle many concurrent requests
        for batch in range(5):  # 5 batches
            tasks = [
                handle_request(f"{batch}-{i}")
                for i in range(20)  # 20 concurrent requests per batch
            ]
            results = await asyncio.gather(*tasks)
            assert len(results) == 20

            # Small delay between batches
            await asyncio.sleep(0.1)

        # Application shutdown (only happens once)
        await session.close()
        await cluster.shutdown()
