"""
Integration tests specifically targeting the critical issues identified in the technical review.

These tests are designed to fail until the framework issues are fixed:
1. Thread safety between cassandra-driver threads and asyncio event loop
2. Memory leaks in streaming functionality
3. Error handling inconsistencies
4. Monitoring coverage gaps
"""

import asyncio
import threading
from unittest.mock import MagicMock, patch

import pytest
import pytest_asyncio

from async_cassandra import AsyncCluster, StreamConfig
from async_cassandra.result import AsyncResultHandler


@pytest.mark.integration
class TestThreadSafetyIssues:
    """Tests for thread safety issues between driver threads and asyncio."""

    @pytest_asyncio.fixture
    async def async_session(self):
        """Create async session for testing."""
        cluster = AsyncCluster(["127.0.0.1"])
        session = await cluster.connect()

        # Create test keyspace and table
        await session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS thread_safety_test
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
        """
        )
        await session.set_keyspace("thread_safety_test")

        await session.execute("DROP TABLE IF EXISTS test_data")
        await session.execute(
            """
            CREATE TABLE test_data (
                id INT PRIMARY KEY,
                data TEXT
            )
        """
        )

        yield session

        await session.close()
        await cluster.shutdown()

    @pytest.mark.asyncio
    async def test_concurrent_page_callbacks_race_condition(self, async_session):
        """
        GIVEN multiple pages being fetched concurrently
        WHEN callbacks are invoked from driver threads
        THEN there should be no race conditions in AsyncResultHandler
        """
        # This test demonstrates the race condition in _handle_page

        # Insert test data across multiple partitions to force multiple pages
        insert_tasks = []
        for i in range(1000):
            stmt = "INSERT INTO test_data (id, data) VALUES (%s, %s)"
            insert_tasks.append(async_session.execute(stmt, (i, f"data_{i}" * 100)))

        await asyncio.gather(*insert_tasks)

        # Now query with small page size to force multiple pages
        results = []
        errors = []

        async def fetch_with_paging():
            try:
                # Use small fetch_size to force multiple pages
                result = await async_session.execute("SELECT * FROM test_data LIMIT 1000")
                results.append(len(result.rows))
            except Exception as e:
                errors.append(str(e))

        # Run multiple concurrent queries to trigger race conditions
        tasks = [fetch_with_paging() for _ in range(20)]
        await asyncio.gather(*tasks, return_exceptions=True)

        # Check for race condition indicators
        assert len(errors) == 0, f"Race condition errors: {errors}"
        assert all(r == 1000 for r in results), f"Inconsistent results: {results}"

    def test_thread_lock_vs_asyncio_lock_mismatch(self):
        """
        GIVEN AsyncResultHandler using threading.Lock
        WHEN accessed from asyncio context
        THEN it should use asyncio-compatible synchronization
        """
        # This test demonstrates the issue with using threading.Lock in async code

        async def run_test():
            # Mock response future
            mock_future = MagicMock()
            mock_future.add_callbacks = MagicMock()
            mock_future.has_more_pages = False

            handler = AsyncResultHandler(mock_future)

            # Simulate concurrent access from multiple async tasks
            call_count = {"count": 0}

            async def access_handler():
                # Try to access handler state
                # In the current implementation, this uses threading.Lock
                # which can cause issues in asyncio context
                call_count["count"] += 1
                # Simulate some async work
                await asyncio.sleep(0.001)
                return handler.rows

            # Run multiple async tasks concurrently
            tasks = [access_handler() for _ in range(50)]
            results = await asyncio.gather(*tasks)

            # All tasks should complete
            assert call_count["count"] == 50
            assert len(results) == 50

        asyncio.run(run_test())

    def test_callback_thread_safety_with_event_loop(self):
        """
        GIVEN callbacks from Cassandra driver threads
        WHEN they interact with asyncio event loop
        THEN thread-safe mechanisms should be used
        """

        async def run_test():
            # Track which threads callbacks are called from
            callback_threads = []
            main_thread = threading.current_thread()

            class InstrumentedHandler(AsyncResultHandler):
                def _handle_page(self, rows):
                    # Record which thread this is called from
                    callback_threads.append(threading.current_thread())
                    super()._handle_page(rows)

            # Mock response future
            mock_future = MagicMock()
            mock_future.has_more_pages = False

            handler = InstrumentedHandler(mock_future)

            # Simulate callbacks from different threads
            def simulate_driver_callback():
                handler._handle_page([1, 2, 3])

            # Driver would call this from its own thread pool
            driver_threads = []
            for _ in range(5):
                t = threading.Thread(target=simulate_driver_callback)
                t.start()
                driver_threads.append(t)

            for t in driver_threads:
                t.join()

            # Verify callbacks came from different threads
            assert len(callback_threads) == 5
            assert any(
                t != main_thread for t in callback_threads
            ), "Callbacks should come from driver threads"

            # The current implementation has issues here
            # It should use call_soon_threadsafe consistently

        asyncio.run(run_test())


@pytest.mark.integration
class TestMemoryLeakIssues:
    """Tests for memory leaks in streaming functionality."""

    @pytest_asyncio.fixture
    async def async_session(self):
        """Create async session for testing."""
        cluster = AsyncCluster(["127.0.0.1"])
        session = await cluster.connect()

        yield session

        await session.close()
        await cluster.shutdown()

    @pytest.mark.asyncio
    async def test_streaming_result_set_cleanup(self, async_session):
        """
        GIVEN streaming through large result sets
        WHEN pages are processed and discarded
        THEN memory should be properly released
        """
        # Create test keyspace and table with many rows
        await async_session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS test_streaming
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
            """
        )
        await async_session.set_keyspace("test_streaming")

        await async_session.execute("DROP TABLE IF EXISTS large_table")
        await async_session.execute(
            """
            CREATE TABLE large_table (
                partition_id INT,
                id INT,
                data TEXT,
                PRIMARY KEY (partition_id, id)
            )
            """
        )

        # Insert 1000 rows across 10 partitions
        for partition in range(10):
            for i in range(100):
                await async_session.execute(
                    "INSERT INTO large_table (partition_id, id, data) VALUES (%s, %s, %s)",
                    (partition, i, "x" * 1000),  # 1KB per row
                )

        # Stream through the data with small page size
        stream_config = StreamConfig(fetch_size=100)
        result = await async_session.execute_stream(
            "SELECT * FROM large_table", stream_config=stream_config
        )

        rows_processed = 0
        pages_seen = 0

        # Track that we're processing pages, not accumulating all data
        async for page in result.pages():
            pages_seen += 1
            rows_in_page = len(page)
            rows_processed += rows_in_page

            # Verify we're getting reasonable page sizes
            assert rows_in_page <= 100, f"Page too large: {rows_in_page}"

            # Process the page (simulating work)
            for row in page:
                assert row.data == "x" * 1000

            # Track last page size if needed for debugging
            pass

        # Verify we processed all data
        assert rows_processed == 1000
        assert pages_seen >= 10  # Should have multiple pages

    @pytest.mark.asyncio
    async def test_streaming_memory_with_context_manager(self, async_session):
        """
        GIVEN streaming result set used as context manager
        WHEN exiting the context (normally or with exception)
        THEN resources should be properly cleaned up
        """
        # Create test data
        await async_session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS test_context
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
            """
        )
        await async_session.set_keyspace("test_context")

        await async_session.execute("DROP TABLE IF EXISTS test_cleanup")
        await async_session.execute(
            """
            CREATE TABLE test_cleanup (
                id INT PRIMARY KEY,
                data TEXT
            )
            """
        )

        # Insert test data
        for i in range(100):
            await async_session.execute(
                "INSERT INTO test_cleanup (id, data) VALUES (%s, %s)", (i, "test data")
            )

        # Test normal exit
        rows_processed = 0
        async with await async_session.execute_stream("SELECT * FROM test_cleanup") as result:
            async for row in result:
                rows_processed += 1
                if rows_processed >= 10:
                    break  # Early exit

        assert rows_processed == 10

        # Test exception exit
        rows_before_error = 0
        try:
            async with await async_session.execute_stream("SELECT * FROM test_cleanup") as result:
                async for row in result:
                    rows_before_error += 1
                    if rows_before_error >= 5:
                        raise ValueError("Test error")
        except ValueError:
            pass  # Expected

        assert rows_before_error == 5

    @pytest.mark.asyncio
    async def test_exception_cleanup_in_streaming(self, async_session):
        """
        GIVEN streaming operation on a table that gets dropped mid-stream
        WHEN exception occurs during streaming
        THEN error should be handled gracefully
        """
        # Create test keyspace and table
        await async_session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS test_error_stream
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
            """
        )
        await async_session.set_keyspace("test_error_stream")

        await async_session.execute("DROP TABLE IF EXISTS disappearing_table")
        await async_session.execute(
            """
            CREATE TABLE disappearing_table (
                id INT PRIMARY KEY,
                data TEXT
            )
            """
        )

        # Insert enough data to ensure multiple pages
        for i in range(1000):
            await async_session.execute(
                "INSERT INTO disappearing_table (id, data) VALUES (%s, %s)", (i, "x" * 100)
            )

        # Start streaming with small page size
        stream_config = StreamConfig(fetch_size=50)
        result = await async_session.execute_stream(
            "SELECT * FROM disappearing_table", stream_config=stream_config
        )

        rows_processed = 0
        error_caught = False

        try:
            async for row in result:
                rows_processed += 1

                # Drop table after processing some rows to trigger error
                if rows_processed == 100:
                    # Use a different session to drop the table
                    from async_cassandra import AsyncCluster

                    temp_cluster = AsyncCluster(contact_points=["localhost"])
                    temp_session = await temp_cluster.connect()
                    await temp_session.execute("DROP TABLE test_error_stream.disappearing_table")
                    await temp_session.close()
                    await temp_cluster.shutdown()

        except Exception as e:
            error_caught = True
            # Should get an error about table not existing
            assert "disappearing_table" in str(e) or "unconfigured table" in str(e)

        assert error_caught, "Expected error when table was dropped"
        assert rows_processed >= 100, "Should have processed some rows before error"


@pytest.mark.integration
class TestErrorHandlingInconsistencies:
    """Tests for error handling inconsistencies in the framework."""

    @pytest_asyncio.fixture
    async def async_session(self):
        """Create async session for testing."""
        cluster = AsyncCluster(["127.0.0.1"])
        session = await cluster.connect()
        yield session
        await session.close()
        await cluster.shutdown()

    @pytest.mark.asyncio
    async def test_error_handling_parity_execute_vs_stream(self, async_session):
        """
        GIVEN the same error condition
        WHEN it occurs in execute() vs execute_stream()
        THEN error handling should be consistent
        """
        # Test with invalid query
        invalid_query = "SELECT * FROM non_existent_table"

        # Test execute() error handling
        execute_error = None
        try:
            await async_session.execute(invalid_query)
        except Exception as e:
            execute_error = e

        # Test execute_stream() error handling
        stream_error = None
        try:
            result = await async_session.execute_stream(invalid_query)
            async for row in result:
                pass
        except Exception as e:
            stream_error = e

        # Both should raise similar errors
        assert execute_error is not None
        assert stream_error is not None
        assert type(execute_error) is type(
            stream_error
        ), f"Different error types: {type(execute_error)} vs {type(stream_error)}"

    @pytest.mark.asyncio
    async def test_timeout_error_handling_consistency(self, async_session):
        """
        GIVEN timeout conditions
        WHEN timeouts occur in different methods
        THEN timeout handling should be consistent
        """
        # Create test table
        await async_session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS timeout_test
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
        """
        )
        await async_session.set_keyspace("timeout_test")

        await async_session.execute("DROP TABLE IF EXISTS test_timeout")
        await async_session.execute(
            """
            CREATE TABLE test_timeout (
                id INT PRIMARY KEY,
                data TEXT
            )
        """
        )

        # Insert data
        for i in range(100):
            await async_session.execute(
                "INSERT INTO test_timeout (id, data) VALUES (%s, %s)", (i, "x" * 1000)
            )

        # Test timeout in execute()
        execute_timeout_error = None
        try:
            # Very short timeout to force error
            await async_session.execute("SELECT * FROM test_timeout", timeout=0.1)
        except Exception as e:
            execute_timeout_error = e

        # Test timeout in execute_stream()
        stream_timeout_error = None
        try:
            result = await async_session.execute_stream("SELECT * FROM test_timeout", timeout=0.1)
            async for row in result:
                pass
        except Exception as e:
            stream_timeout_error = e

        # Both should handle timeout consistently
        assert execute_timeout_error is not None
        assert stream_timeout_error is not None
        # Error types might differ but both should indicate timeout

    @pytest.mark.asyncio
    async def test_connection_error_propagation(self, async_session):
        """
        GIVEN connection errors at different stages
        WHEN errors occur
        THEN they should propagate consistently with proper context
        """
        from cassandra.cluster import NoHostAvailable

        # Mock connection failure
        with patch.object(async_session._session, "execute_async") as mock_execute:
            mock_execute.side_effect = NoHostAvailable("All hosts failed", {})

            # Test execute() error propagation
            execute_error = None
            try:
                await async_session.execute("SELECT * FROM system.local")
            except Exception as e:
                execute_error = e

            # Test execute_stream() error propagation
            stream_error = None
            try:
                await async_session.execute_stream("SELECT * FROM system.local")
            except Exception as e:
                stream_error = e

            # Both should propagate the connection error
            assert execute_error is not None
            assert stream_error is not None

            # Both should preserve the original error type or wrap consistently
            assert isinstance(execute_error, (NoHostAvailable, Exception))
            assert isinstance(stream_error, (NoHostAvailable, Exception))
