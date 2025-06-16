"""
Integration tests for context manager safety with real Cassandra.

These tests ensure that context managers behave correctly with actual
Cassandra connections and don't close shared resources inappropriately.
"""

import asyncio
import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest
from cassandra import InvalidRequest

from async_cassandra import AsyncCluster
from async_cassandra.streaming import StreamConfig


@pytest.mark.integration
class TestContextManagerSafetyIntegration:
    """Test context manager safety with real Cassandra connections."""

    @pytest.mark.asyncio
    async def test_session_remains_open_after_query_error(self, cassandra_cluster):
        """
        Test that session remains usable after a query error occurs.
        """
        cluster = AsyncCluster(["localhost"])
        session = await cluster.connect()

        try:
            # Create test keyspace and table
            await session.execute(
                """
                CREATE KEYSPACE IF NOT EXISTS test_context_safety
                WITH REPLICATION = {
                    'class': 'SimpleStrategy',
                    'replication_factor': 1
                }
                """
            )
            await session.set_keyspace("test_context_safety")

            await session.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id UUID PRIMARY KEY,
                    name TEXT
                )
                """
            )

            # Try a bad query
            with pytest.raises(InvalidRequest):
                await session.execute("SELECT * FROM non_existent_table")

            # Session should still be usable
            user_id = uuid.uuid4()
            await session.execute(
                "INSERT INTO users (id, name) VALUES (%s, %s)", [user_id, "Test User"]
            )

            # Verify insert worked
            result = await session.execute("SELECT * FROM users WHERE id = %s", [user_id])
            row = result.one()
            assert row.name == "Test User"

        finally:
            await session.execute("DROP KEYSPACE IF EXISTS test_context_safety")
            await session.close()
            await cluster.shutdown()

    @pytest.mark.asyncio
    async def test_streaming_error_doesnt_close_session(self, cassandra_cluster):
        """
        Test that an error during streaming doesn't close the session.
        """
        cluster = AsyncCluster(["localhost"])
        session = await cluster.connect()

        try:
            # Create test data
            await session.execute(
                """
                CREATE KEYSPACE IF NOT EXISTS test_stream_safety
                WITH REPLICATION = {
                    'class': 'SimpleStrategy',
                    'replication_factor': 1
                }
                """
            )
            await session.set_keyspace("test_stream_safety")

            await session.execute(
                """
                CREATE TABLE IF NOT EXISTS data (
                    id UUID PRIMARY KEY,
                    value INT
                )
                """
            )

            # Insert some data
            for i in range(10):
                await session.execute(
                    "INSERT INTO data (id, value) VALUES (%s, %s)", [uuid.uuid4(), i]
                )

            # Stream with an error (simulate by using bad query)
            try:
                async with await session.execute_stream(
                    "SELECT * FROM non_existent_table"
                ) as stream:
                    async for row in stream:
                        pass
            except Exception:
                pass  # Expected

            # Session should still work
            result = await session.execute("SELECT COUNT(*) FROM data")
            assert result.one()[0] == 10

            # Try another streaming query - should work
            count = 0
            async with await session.execute_stream("SELECT * FROM data") as stream:
                async for row in stream:
                    count += 1
            assert count == 10

        finally:
            await session.execute("DROP KEYSPACE IF EXISTS test_stream_safety")
            await session.close()
            await cluster.shutdown()

    @pytest.mark.asyncio
    async def test_concurrent_streaming_sessions(self, cassandra_cluster):
        """
        Test that multiple sessions can stream concurrently without interference.
        """
        cluster = AsyncCluster(["localhost"])

        try:
            # Create test data
            setup_session = await cluster.connect()
            await setup_session.execute(
                """
                CREATE KEYSPACE IF NOT EXISTS test_concurrent_streams
                WITH REPLICATION = {
                    'class': 'SimpleStrategy',
                    'replication_factor': 1
                }
                """
            )
            await setup_session.set_keyspace("test_concurrent_streams")

            await setup_session.execute(
                """
                CREATE TABLE IF NOT EXISTS data (
                    partition INT,
                    id UUID,
                    value TEXT,
                    PRIMARY KEY (partition, id)
                )
                """
            )

            # Insert data in different partitions
            for partition in range(3):
                for i in range(100):
                    await setup_session.execute(
                        "INSERT INTO data (partition, id, value) VALUES (%s, %s, %s)",
                        [partition, uuid.uuid4(), f"value_{partition}_{i}"],
                    )

            await setup_session.close()

            # Stream from multiple sessions concurrently
            async def stream_partition(partition_id):
                session = await cluster.connect("test_concurrent_streams")
                try:
                    count = 0
                    config = StreamConfig(fetch_size=10)

                    query = "SELECT * FROM data WHERE partition = %s"
                    async with await session.execute_stream(
                        query, [partition_id], stream_config=config
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

        finally:
            # Cleanup
            cleanup_session = await cluster.connect()
            await cleanup_session.execute("DROP KEYSPACE IF EXISTS test_concurrent_streams")
            await cleanup_session.close()
            await cluster.shutdown()

    @pytest.mark.asyncio
    async def test_session_context_manager_with_streaming(self, cassandra_cluster):
        """
        Test using session context manager with streaming operations.
        """
        cluster = AsyncCluster(["localhost"])

        try:
            # Setup
            setup_session = await cluster.connect()
            await setup_session.execute(
                """
                CREATE KEYSPACE IF NOT EXISTS test_session_context
                WITH REPLICATION = {
                    'class': 'SimpleStrategy',
                    'replication_factor': 1
                }
                """
            )
            await setup_session.close()

            # Use session in context manager
            async with await cluster.connect("test_session_context") as session:
                await session.execute(
                    """
                    CREATE TABLE IF NOT EXISTS data (
                        id UUID PRIMARY KEY,
                        value TEXT
                    )
                    """
                )

                # Insert data
                for i in range(50):
                    await session.execute(
                        "INSERT INTO data (id, value) VALUES (%s, %s)", [uuid.uuid4(), f"value_{i}"]
                    )

                # Stream data
                count = 0
                async with await session.execute_stream("SELECT * FROM data") as stream:
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
        verify_session = await cluster.connect("test_session_context")
        result = await verify_session.execute("SELECT COUNT(*) FROM data")
        assert result.one()[0] == 50

        # Cleanup
        await verify_session.execute("DROP KEYSPACE IF EXISTS test_session_context")
        await verify_session.close()
        await cluster.shutdown()

    @pytest.mark.asyncio
    async def test_thread_pool_access_during_context_exit(self, cassandra_cluster):
        """
        Test that other threads can access session while context manager exits.
        """
        cluster = AsyncCluster(["localhost"])
        session = await cluster.connect()

        try:
            # Setup
            await session.execute(
                """
                CREATE KEYSPACE IF NOT EXISTS test_thread_safety
                WITH REPLICATION = {
                    'class': 'SimpleStrategy',
                    'replication_factor': 1
                }
                """
            )
            await session.set_keyspace("test_thread_safety")

            await session.execute(
                """
                CREATE TABLE IF NOT EXISTS counter (
                    id TEXT PRIMARY KEY,
                    count INT
                )
                """
            )

            # Initialize counter
            await session.execute("INSERT INTO counter (id, count) VALUES ('main', 0)")

            # Function to run in thread
            def increment_counter(session_ref, counter_name):
                """Increment counter from thread."""
                # Create new event loop for thread
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

                async def do_increment():
                    # Get current value
                    result = await session_ref.execute(
                        "SELECT count FROM counter WHERE id = %s", [counter_name]
                    )
                    current = result.one().count

                    # Increment
                    await session_ref.execute(
                        "UPDATE counter SET count = %s WHERE id = %s", [current + 1, counter_name]
                    )

                loop.run_until_complete(do_increment())
                loop.close()

            # Use thread pool
            with ThreadPoolExecutor(max_workers=3) as executor:
                # Start background threads
                futures = []
                for i in range(3):
                    future = executor.submit(increment_counter, session, "main")
                    futures.append(future)

                # Use streaming in main thread with context manager
                async with await session.execute_stream("SELECT * FROM counter") as stream:
                    async for row in stream:
                        # While streaming, other threads are working
                        await asyncio.sleep(0.1)

                # Wait for threads
                for future in futures:
                    future.result(timeout=5.0)

            # Verify all increments worked
            result = await session.execute("SELECT count FROM counter WHERE id = 'main'")
            final_count = result.one().count
            assert final_count == 3  # Each thread incremented once

        finally:
            await session.execute("DROP KEYSPACE IF EXISTS test_thread_safety")
            await session.close()
            await cluster.shutdown()

    @pytest.mark.asyncio
    async def test_cluster_context_manager_multiple_sessions(self, cassandra_cluster):
        """
        Test cluster context manager with multiple sessions.
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
    async def test_nested_streaming_contexts(self, cassandra_cluster):
        """
        Test nested streaming context managers.
        """
        cluster = AsyncCluster(["localhost"])
        session = await cluster.connect()

        try:
            # Setup
            await session.execute(
                """
                CREATE KEYSPACE IF NOT EXISTS test_nested_streams
                WITH REPLICATION = {
                    'class': 'SimpleStrategy',
                    'replication_factor': 1
                }
                """
            )
            await session.set_keyspace("test_nested_streams")

            await session.execute(
                """
                CREATE TABLE IF NOT EXISTS categories (
                    id UUID PRIMARY KEY,
                    name TEXT
                )
                """
            )

            await session.execute(
                """
                CREATE TABLE IF NOT EXISTS items (
                    category_id UUID,
                    id UUID,
                    name TEXT,
                    PRIMARY KEY (category_id, id)
                )
                """
            )

            # Insert test data
            categories = []
            for i in range(3):
                cat_id = uuid.uuid4()
                categories.append(cat_id)
                await session.execute(
                    "INSERT INTO categories (id, name) VALUES (%s, %s)", [cat_id, f"Category {i}"]
                )

                # Insert items for this category
                for j in range(5):
                    await session.execute(
                        "INSERT INTO items (category_id, id, name) VALUES (%s, %s, %s)",
                        [cat_id, uuid.uuid4(), f"Item {i}-{j}"],
                    )

            # Nested streaming
            category_count = 0
            item_count = 0

            # Stream categories
            async with await session.execute_stream("SELECT * FROM categories") as cat_stream:
                async for category in cat_stream:
                    category_count += 1

                    # For each category, stream its items
                    query = "SELECT * FROM items WHERE category_id = %s"
                    async with await session.execute_stream(query, [category.id]) as item_stream:
                        async for item in item_stream:
                            item_count += 1

            assert category_count == 3
            assert item_count == 15  # 3 categories * 5 items each

            # Session should still be usable
            result = await session.execute("SELECT COUNT(*) FROM categories")
            assert result.one()[0] == 3

        finally:
            await session.execute("DROP KEYSPACE IF EXISTS test_nested_streams")
            await session.close()
            await cluster.shutdown()
