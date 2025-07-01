"""
Integration tests for context manager safety with real Cassandra.

These tests ensure that context managers behave correctly with actual
Cassandra connections and don't close shared resources inappropriately.
"""

import asyncio
import uuid

import pytest
from cassandra import InvalidRequest

from async_cassandra import AsyncCluster
from async_cassandra.streaming import StreamConfig


@pytest.mark.integration
class TestContextManagerSafetyIntegration:
    """Test context manager safety with real Cassandra connections."""

    @pytest.mark.asyncio
    async def test_session_remains_open_after_query_error(self, cassandra_session):
        """
        Test that session remains usable after a query error occurs.

        What this tests:
        ---------------
        1. Query errors don't close session
        2. Session still usable
        3. New queries work
        4. Insert/select functional

        Why this matters:
        ----------------
        Error recovery critical:
        - Apps have query errors
        - Must continue operating
        - No resource leaks

        Sessions must survive
        individual query failures.
        """
        # Get the unique table name
        users_table = cassandra_session._test_users_table

        # Try a bad query
        with pytest.raises(InvalidRequest):
            await cassandra_session.execute(
                "SELECT * FROM table_that_definitely_does_not_exist_xyz123"
            )

        # Session should still be usable
        user_id = uuid.uuid4()
        insert_prepared = await cassandra_session.prepare(
            f"INSERT INTO {users_table} (id, name) VALUES (?, ?)"
        )
        await cassandra_session.execute(insert_prepared, [user_id, "Test User"])

        # Verify insert worked
        select_prepared = await cassandra_session.prepare(
            f"SELECT * FROM {users_table} WHERE id = ?"
        )
        result = await cassandra_session.execute(select_prepared, [user_id])
        row = result.one()
        assert row.name == "Test User"

    @pytest.mark.asyncio
    async def test_streaming_error_doesnt_close_session(self, cassandra_session):
        """
        Test that an error during streaming doesn't close the session.

        What this tests:
        ---------------
        1. Stream errors handled
        2. Session stays open
        3. New streams work
        4. Regular queries work

        Why this matters:
        ----------------
        Streaming failures common:
        - Large result sets
        - Network interruptions
        - Query timeouts

        Session must survive
        streaming failures.
        """
        # Create test table
        await cassandra_session.execute(
            """
            CREATE TABLE IF NOT EXISTS test_stream_data (
                id UUID PRIMARY KEY,
                value INT
            )
            """
        )

        # Insert some data
        insert_prepared = await cassandra_session.prepare(
            "INSERT INTO test_stream_data (id, value) VALUES (?, ?)"
        )
        for i in range(10):
            await cassandra_session.execute(insert_prepared, [uuid.uuid4(), i])

        # Stream with an error (simulate by using bad query)
        try:
            async with await cassandra_session.execute_stream(
                "SELECT * FROM non_existent_table"
            ) as stream:
                async for row in stream:
                    pass
        except Exception:
            pass  # Expected

        # Session should still work
        result = await cassandra_session.execute("SELECT COUNT(*) FROM test_stream_data")
        assert result.one()[0] == 10

        # Try another streaming query - should work
        count = 0
        async with await cassandra_session.execute_stream(
            "SELECT * FROM test_stream_data"
        ) as stream:
            async for row in stream:
                count += 1
        assert count == 10

    @pytest.mark.asyncio
    async def test_concurrent_streaming_sessions(self, cassandra_session, cassandra_cluster):
        """
        Test that multiple sessions can stream concurrently without interference.

        What this tests:
        ---------------
        1. Multiple sessions work
        2. Concurrent streaming OK
        3. No interference
        4. Independent results

        Why this matters:
        ----------------
        Multi-session patterns:
        - Worker processes
        - Parallel processing
        - Load distribution

        Sessions must be truly
        independent.
        """
        # Create test table
        await cassandra_session.execute(
            """
            CREATE TABLE IF NOT EXISTS test_concurrent_data (
                partition INT,
                id UUID,
                value TEXT,
                PRIMARY KEY (partition, id)
            )
            """
        )

        # Insert data in different partitions
        insert_prepared = await cassandra_session.prepare(
            "INSERT INTO test_concurrent_data (partition, id, value) VALUES (?, ?, ?)"
        )
        for partition in range(3):
            for i in range(100):
                await cassandra_session.execute(
                    insert_prepared,
                    [partition, uuid.uuid4(), f"value_{partition}_{i}"],
                )

        # Stream from multiple sessions concurrently
        async def stream_partition(partition_id):
            # Create new session and connect to the shared keyspace
            session = await cassandra_cluster.connect()
            await session.set_keyspace("integration_test")
            try:
                count = 0
                config = StreamConfig(fetch_size=10)

                query_prepared = await session.prepare(
                    "SELECT * FROM test_concurrent_data WHERE partition = ?"
                )
                async with await session.execute_stream(
                    query_prepared, [partition_id], stream_config=config
                ) as stream:
                    async for row in stream:
                        assert row.value.startswith(f"value_{partition_id}_")
                        count += 1

                return count
            finally:
                await session.close()

        # Run streams concurrently
        results = await asyncio.gather(
            stream_partition(0), stream_partition(1), stream_partition(2)
        )

        # Each partition should have 100 rows
        assert all(count == 100 for count in results)

    @pytest.mark.asyncio
    async def test_session_context_manager_with_streaming(self, cassandra_cluster):
        """
        Test using session context manager with streaming operations.

        What this tests:
        ---------------
        1. Session context managers
        2. Streaming within context
        3. Error cleanup works
        4. Resources freed

        Why this matters:
        ----------------
        Context managers ensure:
        - Proper cleanup
        - Exception safety
        - Resource management

        Critical for production
        reliability.
        """
        try:
            # Use session in context manager
            async with await cassandra_cluster.connect() as session:
                await session.set_keyspace("integration_test")
                await session.execute(
                    """
                    CREATE TABLE IF NOT EXISTS test_session_ctx_data (
                        id UUID PRIMARY KEY,
                        value TEXT
                    )
                    """
                )

                # Insert data
                insert_prepared = await session.prepare(
                    "INSERT INTO test_session_ctx_data (id, value) VALUES (?, ?)"
                )
                for i in range(50):
                    await session.execute(
                        insert_prepared,
                        [uuid.uuid4(), f"value_{i}"],
                    )

                # Stream data
                count = 0
                async with await session.execute_stream(
                    "SELECT * FROM test_session_ctx_data"
                ) as stream:
                    async for row in stream:
                        count += 1

                assert count == 50

                # Raise an error to test cleanup
                if True:  # Always true, but makes intent clear
                    raise ValueError("Test error")

        except ValueError:
            # Expected error
            pass

        # Cluster should still be usable
        verify_session = await cassandra_cluster.connect()
        await verify_session.set_keyspace("integration_test")
        result = await verify_session.execute("SELECT COUNT(*) FROM test_session_ctx_data")
        assert result.one()[0] == 50

        # Cleanup
        await verify_session.close()

    @pytest.mark.asyncio
    async def test_cluster_context_manager_multiple_sessions(self, cassandra_cluster):
        """
        Test cluster context manager with multiple sessions.

        What this tests:
        ---------------
        1. Multiple sessions per cluster
        2. Independent session lifecycle
        3. Cluster cleanup on exit
        4. Session isolation

        Why this matters:
        ----------------
        Multi-session patterns:
        - Connection pooling
        - Worker threads
        - Service isolation

        Cluster must manage all
        sessions properly.
        """
        # Use cluster in context manager
        async with AsyncCluster(["localhost"]) as cluster:
            # Create multiple sessions
            sessions = []
            for i in range(3):
                session = await cluster.connect()
                sessions.append(session)

            # Use all sessions
            for i, session in enumerate(sessions):
                result = await session.execute("SELECT release_version FROM system.local")
                assert result.one() is not None

            # Close only one session
            await sessions[0].close()

            # Other sessions should still work
            for session in sessions[1:]:
                result = await session.execute("SELECT release_version FROM system.local")
                assert result.one() is not None

            # Close remaining sessions
            for session in sessions[1:]:
                await session.close()

        # After cluster context exits, cluster is shut down
        # Trying to use it should fail
        with pytest.raises(Exception):
            await cluster.connect()

    @pytest.mark.asyncio
    async def test_nested_streaming_contexts(self, cassandra_session):
        """
        Test nested streaming context managers.

        What this tests:
        ---------------
        1. Nested streams work
        2. Inner/outer independence
        3. Proper cleanup order
        4. No resource conflicts

        Why this matters:
        ----------------
        Nested patterns common:
        - Parent-child queries
        - Hierarchical data
        - Complex workflows

        Must handle nested contexts
        without deadlocks.
        """
        # Create test tables
        await cassandra_session.execute(
            """
            CREATE TABLE IF NOT EXISTS test_nested_categories (
                id UUID PRIMARY KEY,
                name TEXT
            )
            """
        )

        await cassandra_session.execute(
            """
            CREATE TABLE IF NOT EXISTS test_nested_items (
                category_id UUID,
                id UUID,
                name TEXT,
                PRIMARY KEY (category_id, id)
            )
            """
        )

        # Insert test data
        categories = []
        category_prepared = await cassandra_session.prepare(
            "INSERT INTO test_nested_categories (id, name) VALUES (?, ?)"
        )
        item_prepared = await cassandra_session.prepare(
            "INSERT INTO test_nested_items (category_id, id, name) VALUES (?, ?, ?)"
        )

        for i in range(3):
            cat_id = uuid.uuid4()
            categories.append(cat_id)
            await cassandra_session.execute(
                category_prepared,
                [cat_id, f"Category {i}"],
            )

            # Insert items for this category
            for j in range(5):
                await cassandra_session.execute(
                    item_prepared,
                    [cat_id, uuid.uuid4(), f"Item {i}-{j}"],
                )

        # Nested streaming
        category_count = 0
        item_count = 0

        # Stream categories
        async with await cassandra_session.execute_stream(
            "SELECT * FROM test_nested_categories"
        ) as cat_stream:
            async for category in cat_stream:
                category_count += 1

                # For each category, stream its items
                query_prepared = await cassandra_session.prepare(
                    "SELECT * FROM test_nested_items WHERE category_id = ?"
                )
                async with await cassandra_session.execute_stream(
                    query_prepared, [category.id]
                ) as item_stream:
                    async for item in item_stream:
                        item_count += 1

        assert category_count == 3
        assert item_count == 15  # 3 categories * 5 items each

        # Session should still be usable
        result = await cassandra_session.execute("SELECT COUNT(*) FROM test_nested_categories")
        assert result.one()[0] == 3
