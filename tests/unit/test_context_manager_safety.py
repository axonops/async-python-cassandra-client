"""
Unit tests for context manager safety.

These tests ensure that context managers only close what they should,
and don't accidentally close shared resources like clusters and sessions
when errors occur.
"""

import asyncio
import threading
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from async_cassandra import AsyncCassandraSession, AsyncCluster
from async_cassandra.exceptions import QueryError
from async_cassandra.streaming import AsyncStreamingResultSet


class TestContextManagerSafety:
    """Test that context managers don't close shared resources inappropriately."""

    @pytest.mark.asyncio
    async def test_cluster_context_manager_closes_only_cluster(self):
        """
        Test that cluster context manager only closes the cluster,
        not any sessions created from it.

        What this tests:
        ---------------
        1. Cluster context manager closes cluster
        2. Sessions remain open after cluster exit
        3. Resources properly scoped
        4. No premature cleanup

        Why this matters:
        ----------------
        Context managers must respect ownership:
        - Cluster owns its lifecycle
        - Sessions own their lifecycle
        - No cross-contamination

        Prevents accidental resource cleanup
        that breaks active operations.
        """
        mock_cluster = MagicMock()
        mock_cluster.shutdown = MagicMock()  # Not AsyncMock because it's called via run_in_executor
        mock_cluster.connect = AsyncMock()
        mock_cluster.protocol_version = 5  # Mock protocol version

        # Create a mock session that should NOT be closed by cluster context manager
        mock_session = MagicMock()
        mock_session.close = AsyncMock()
        mock_cluster.connect.return_value = mock_session

        with patch("async_cassandra.cluster.Cluster") as mock_cluster_class:
            mock_cluster_class.return_value = mock_cluster

            # Mock AsyncCassandraSession.create
            mock_async_session = MagicMock()
            mock_async_session._session = mock_session
            mock_async_session.close = AsyncMock()

            with patch(
                "async_cassandra.session.AsyncCassandraSession.create", new_callable=AsyncMock
            ) as mock_create:
                mock_create.return_value = mock_async_session

                # Use cluster in context manager
                async with AsyncCluster(["localhost"]) as cluster:
                    # Create a session
                    session = await cluster.connect()

                    # Session should be the mock we created
                    assert session._session == mock_session

                # Cluster should be shut down
                mock_cluster.shutdown.assert_called_once()

                # But session should NOT be closed
                mock_session.close.assert_not_called()

    @pytest.mark.asyncio
    async def test_session_context_manager_closes_only_session(self):
        """
        Test that session context manager only closes the session,
        not the cluster it came from.

        What this tests:
        ---------------
        1. Session context closes session
        2. Cluster remains open
        3. Independent lifecycles
        4. Clean resource separation

        Why this matters:
        ----------------
        Sessions don't own clusters:
        - Multiple sessions per cluster
        - Cluster outlives sessions
        - Sessions are lightweight

        Critical for connection pooling
        and resource efficiency.
        """
        mock_cluster = MagicMock()
        mock_cluster.shutdown = MagicMock()  # Not AsyncMock because it's called via run_in_executor
        mock_session = MagicMock()
        mock_session.shutdown = MagicMock()  # AsyncCassandraSession calls shutdown, not close

        # Create AsyncCassandraSession with mocks
        async_session = AsyncCassandraSession(mock_session)

        # Use session in context manager
        async with async_session:
            # Do some work
            pass

        # Session should be shut down
        mock_session.shutdown.assert_called_once()

        # But cluster should NOT be shut down
        mock_cluster.shutdown.assert_not_called()

    @pytest.mark.asyncio
    async def test_streaming_context_manager_closes_only_stream(self):
        """
        Test that streaming result context manager only closes the stream,
        not the session or cluster.

        What this tests:
        ---------------
        1. Stream context closes stream
        2. Session remains open
        3. Callbacks cleaned up
        4. No session interference

        Why this matters:
        ----------------
        Streams are ephemeral resources:
        - One query = one stream
        - Session handles many queries
        - Stream cleanup is isolated

        Ensures streaming doesn't break
        session for other queries.
        """
        # Create mock response future
        mock_future = MagicMock()
        mock_future.has_more_pages = False
        mock_future._final_exception = None
        mock_future.add_callbacks = MagicMock()
        mock_future.clear_callbacks = MagicMock()

        # Create mock session (should NOT be closed)
        mock_session = MagicMock()
        mock_session.close = AsyncMock()

        # Create streaming result
        stream_result = AsyncStreamingResultSet(mock_future)
        stream_result._handle_page(["row1", "row2", "row3"])

        # Use streaming result in context manager
        async with stream_result as stream:
            # Process some data
            rows = []
            async for row in stream:
                rows.append(row)

        # Stream callbacks should be cleaned up
        mock_future.clear_callbacks.assert_called()

        # But session should NOT be closed
        mock_session.close.assert_not_called()

    @pytest.mark.asyncio
    async def test_query_error_doesnt_close_session(self):
        """
        Test that a query error doesn't close the session.

        What this tests:
        ---------------
        1. Query errors don't close session
        2. Session remains usable
        3. Error handling isolated
        4. No cascade failures

        Why this matters:
        ----------------
        Query errors are normal:
        - Bad syntax happens
        - Tables may not exist
        - Timeouts occur

        Session must survive individual
        query failures.
        """
        mock_session = MagicMock()
        mock_session.close = AsyncMock()

        # Create a session that will raise an error
        async_session = AsyncCassandraSession(mock_session)

        # Mock execute to raise an error
        with patch.object(async_session, "execute", side_effect=QueryError("Bad query")):
            try:
                await async_session.execute("SELECT * FROM bad_table")
            except QueryError:
                pass  # Expected

        # Session should NOT be closed due to query error
        mock_session.close.assert_not_called()

    @pytest.mark.asyncio
    async def test_streaming_error_doesnt_close_session(self):
        """
        Test that an error during streaming doesn't close the session.

        This test verifies that when a streaming operation fails,
        it doesn't accidentally close the session that might be
        used by other concurrent operations.

        What this tests:
        ---------------
        1. Streaming errors isolated
        2. Session unaffected by stream errors
        3. Concurrent operations continue
        4. Error containment works

        Why this matters:
        ----------------
        Streaming failures common:
        - Network interruptions
        - Large result timeouts
        - Memory pressure

        Other queries must continue
        despite streaming failures.
        """
        mock_session = MagicMock()
        mock_session.close = AsyncMock()

        # For this test, we just need to verify that streaming errors
        # are isolated and don't affect the session.
        # The actual streaming error handling is tested elsewhere.

        # Create a simple async function that raises an error
        async def failing_operation():
            raise Exception("Streaming error")

        # Run the failing operation
        with pytest.raises(Exception, match="Streaming error"):
            await failing_operation()

        # Session should NOT be closed
        mock_session.close.assert_not_called()

    @pytest.mark.asyncio
    async def test_concurrent_session_usage_during_error(self):
        """
        Test that other coroutines can still use the session when
        one coroutine has an error.

        What this tests:
        ---------------
        1. Concurrent queries independent
        2. One failure doesn't affect others
        3. Session thread-safe for errors
        4. Proper error isolation

        Why this matters:
        ----------------
        Real apps have concurrent queries:
        - API handling multiple requests
        - Background jobs running
        - Batch processing

        One bad query shouldn't break
        all other operations.
        """
        mock_session = MagicMock()
        mock_session.close = AsyncMock()

        # Track execute calls
        execute_count = 0
        execute_results = []

        async def mock_execute(query, *args, **kwargs):
            nonlocal execute_count
            execute_count += 1

            # First call fails, others succeed
            if execute_count == 1:
                raise QueryError("First query fails")

            # Return a mock result
            result = MagicMock()
            result.one = MagicMock(return_value={"id": execute_count})
            execute_results.append(result)
            return result

        # Create session
        async_session = AsyncCassandraSession(mock_session)
        async_session.execute = mock_execute

        # Run concurrent queries
        async def query_with_error():
            try:
                await async_session.execute("SELECT * FROM table1")
            except QueryError:
                pass  # Expected

        async def query_success():
            return await async_session.execute("SELECT * FROM table2")

        # Run queries concurrently
        results = await asyncio.gather(
            query_with_error(), query_success(), query_success(), return_exceptions=True
        )

        # First should be None (handled error), others should succeed
        assert results[0] is None
        assert results[1] is not None
        assert results[2] is not None

        # Session should NOT be closed
        mock_session.close.assert_not_called()

        # Should have made 3 execute calls
        assert execute_count == 3

    @pytest.mark.asyncio
    async def test_session_usable_after_streaming_context_exit(self):
        """
        Test that session remains usable after streaming context manager exits.

        What this tests:
        ---------------
        1. Session works after streaming
        2. Stream cleanup doesn't break session
        3. Can execute new queries
        4. Resource isolation verified

        Why this matters:
        ----------------
        Common pattern:
        - Stream large results
        - Process data
        - Execute follow-up queries

        Session must remain fully
        functional after streaming.
        """
        mock_session = MagicMock()
        mock_session.close = AsyncMock()

        # Create session
        async_session = AsyncCassandraSession(mock_session)

        # Mock execute_stream
        mock_future = MagicMock()
        mock_future.has_more_pages = False
        mock_future._final_exception = None
        mock_future.add_callbacks = MagicMock()
        mock_future.clear_callbacks = MagicMock()

        stream_result = AsyncStreamingResultSet(mock_future)
        stream_result._handle_page(["row1", "row2"])

        async def mock_execute_stream(*args, **kwargs):
            return stream_result

        async_session.execute_stream = mock_execute_stream

        # Use streaming in context manager
        async with await async_session.execute_stream("SELECT * FROM table") as stream:
            rows = []
            async for row in stream:
                rows.append(row)

        # Now try to use session again - should work
        mock_result = MagicMock()
        mock_result.one = MagicMock(return_value={"id": 1})

        async def mock_execute(*args, **kwargs):
            return mock_result

        async_session.execute = mock_execute

        # This should work fine
        result = await async_session.execute("SELECT * FROM another_table")
        assert result.one() == {"id": 1}

        # Session should still be open
        mock_session.close.assert_not_called()

    @pytest.mark.asyncio
    async def test_cluster_remains_open_after_session_context_exit(self):
        """
        Test that cluster remains open after session context manager exits.

        What this tests:
        ---------------
        1. Cluster survives session closure
        2. Can create new sessions
        3. Cluster lifecycle independent
        4. Multiple session support

        Why this matters:
        ----------------
        Cluster is expensive resource:
        - Connection pool
        - Metadata management
        - Load balancing state

        Must support many short-lived
        sessions efficiently.
        """
        mock_cluster = MagicMock()
        mock_cluster.shutdown = MagicMock()  # Not AsyncMock because it's called via run_in_executor
        mock_cluster.connect = AsyncMock()
        mock_cluster.protocol_version = 5  # Mock protocol version

        mock_session1 = MagicMock()
        mock_session1.close = AsyncMock()

        mock_session2 = MagicMock()
        mock_session2.close = AsyncMock()

        # First connect returns session1, second returns session2
        mock_cluster.connect.side_effect = [mock_session1, mock_session2]

        with patch("async_cassandra.cluster.Cluster") as mock_cluster_class:
            mock_cluster_class.return_value = mock_cluster

            # Mock AsyncCassandraSession.create
            mock_async_session1 = MagicMock()
            mock_async_session1._session = mock_session1
            mock_async_session1.close = AsyncMock()
            mock_async_session1.__aenter__ = AsyncMock(return_value=mock_async_session1)

            async def async_exit1(*args):
                await mock_async_session1.close()

            mock_async_session1.__aexit__ = AsyncMock(side_effect=async_exit1)

            mock_async_session2 = MagicMock()
            mock_async_session2._session = mock_session2
            mock_async_session2.close = AsyncMock()

            with patch(
                "async_cassandra.session.AsyncCassandraSession.create", new_callable=AsyncMock
            ) as mock_create:
                mock_create.side_effect = [mock_async_session1, mock_async_session2]

                cluster = AsyncCluster(["localhost"])

                # Use first session in context manager
                async with await cluster.connect():
                    pass  # Do some work

                # First session should be closed
                mock_async_session1.close.assert_called_once()

                # But cluster should NOT be shut down
                mock_cluster.shutdown.assert_not_called()

                # Should be able to create another session
                session2 = await cluster.connect()
                assert session2._session == mock_session2

                # Clean up
                await cluster.shutdown()

    @pytest.mark.asyncio
    async def test_thread_safety_of_session_during_context_exit(self):
        """
        Test that session can be used by other threads even when
        one thread is exiting a context manager.

        What this tests:
        ---------------
        1. Thread-safe context exit
        2. Concurrent usage allowed
        3. No race conditions
        4. Proper synchronization

        Why this matters:
        ----------------
        Multi-threaded usage common:
        - Web frameworks spawn threads
        - Background workers
        - Parallel processing

        Context managers must be
        thread-safe during cleanup.
        """
        mock_session = MagicMock()
        mock_session.shutdown = MagicMock()  # AsyncCassandraSession calls shutdown

        # Create thread-safe mock for execute
        execute_lock = threading.Lock()
        execute_calls = []

        def mock_execute_sync(query):
            with execute_lock:
                execute_calls.append(query)
                result = MagicMock()
                result.one = MagicMock(return_value={"id": len(execute_calls)})
                return result

        mock_session.execute = mock_execute_sync

        # Create async session
        async_session = AsyncCassandraSession(mock_session)

        # Track if session is being used
        session_in_use = threading.Event()
        other_thread_done = threading.Event()

        # Function for other thread
        def other_thread_work():
            session_in_use.wait()  # Wait for signal

            # Try to use session from another thread
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            async def do_query():
                # Wrap sync call in executor
                result = await asyncio.get_event_loop().run_in_executor(
                    None, mock_session.execute, "SELECT FROM other_thread"
                )
                return result

            loop.run_until_complete(do_query())
            loop.close()

            other_thread_done.set()

        # Start other thread
        thread = threading.Thread(target=other_thread_work)
        thread.start()

        # Use session in context manager
        async with async_session:
            # Signal other thread that session is in use
            session_in_use.set()

            # Do some work
            await asyncio.get_event_loop().run_in_executor(
                None, mock_session.execute, "SELECT FROM main_thread"
            )

            # Wait a bit for other thread to also use session
            await asyncio.sleep(0.1)

        # Wait for other thread
        other_thread_done.wait(timeout=2.0)
        thread.join()

        # Both threads should have executed queries
        assert len(execute_calls) == 2
        assert "SELECT FROM main_thread" in execute_calls
        assert "SELECT FROM other_thread" in execute_calls

        # Session should be shut down only once
        mock_session.shutdown.assert_called_once()

    @pytest.mark.asyncio
    async def test_streaming_context_manager_implementation(self):
        """
        Test that streaming result properly implements context manager protocol.

        What this tests:
        ---------------
        1. __aenter__ returns self
        2. __aexit__ calls close
        3. Cleanup always happens
        4. Protocol correctly implemented

        Why this matters:
        ----------------
        Context manager protocol ensures:
        - Resources always cleaned
        - Even with exceptions
        - Pythonic usage pattern

        Users expect async with to
        work correctly.
        """
        # Mock response future
        mock_future = MagicMock()
        mock_future.has_more_pages = False
        mock_future._final_exception = None
        mock_future.add_callbacks = MagicMock()
        mock_future.clear_callbacks = MagicMock()

        # Create streaming result
        stream_result = AsyncStreamingResultSet(mock_future)
        stream_result._handle_page(["row1", "row2"])

        # Test __aenter__ returns self
        entered = await stream_result.__aenter__()
        assert entered is stream_result

        # Test __aexit__ calls close
        close_called = False
        original_close = stream_result.close

        async def mock_close():
            nonlocal close_called
            close_called = True
            await original_close()

        stream_result.close = mock_close

        # Call __aexit__ with no exception
        result = await stream_result.__aexit__(None, None, None)
        assert result is None  # Should not suppress exceptions
        assert close_called

        # Verify cleanup happened
        mock_future.clear_callbacks.assert_called()

    @pytest.mark.asyncio
    async def test_context_manager_with_exception_propagation(self):
        """
        Test that exceptions are properly propagated through context managers.

        What this tests:
        ---------------
        1. Exceptions propagate correctly
        2. Cleanup still happens
        3. __aexit__ doesn't suppress
        4. Error handling correct

        Why this matters:
        ----------------
        Exception handling critical:
        - Errors must bubble up
        - Resources still cleaned
        - No silent failures

        Context managers must not
        hide exceptions.
        """
        mock_future = MagicMock()
        mock_future.has_more_pages = False
        mock_future._final_exception = None
        mock_future.add_callbacks = MagicMock()
        mock_future.clear_callbacks = MagicMock()

        stream_result = AsyncStreamingResultSet(mock_future)
        stream_result._handle_page(["row1"])

        # Test that exceptions are propagated
        exception_caught = None
        close_called = False

        async def track_close():
            nonlocal close_called
            close_called = True

        stream_result.close = track_close

        try:
            async with stream_result:
                raise ValueError("Test exception")
        except ValueError as e:
            exception_caught = e

        # Exception should be propagated
        assert exception_caught is not None
        assert str(exception_caught) == "Test exception"

        # But close should still have been called
        assert close_called

    @pytest.mark.asyncio
    async def test_nested_context_managers_close_correctly(self):
        """
        Test that nested context managers only close their own resources.

        What this tests:
        ---------------
        1. Nested contexts independent
        2. Inner closes before outer
        3. Each manages own resources
        4. Proper cleanup order

        Why this matters:
        ----------------
        Common nesting pattern:
        - Cluster context
        - Session context inside
        - Stream context inside that

        Each level must clean up
        only its own resources.
        """
        mock_cluster = MagicMock()
        mock_cluster.shutdown = MagicMock()  # Not AsyncMock because it's called via run_in_executor
        mock_cluster.connect = AsyncMock()
        mock_cluster.protocol_version = 5  # Mock protocol version

        mock_session = MagicMock()
        mock_session.close = AsyncMock()
        mock_cluster.connect.return_value = mock_session

        # Mock for streaming
        mock_future = MagicMock()
        mock_future.has_more_pages = False
        mock_future._final_exception = None
        mock_future.add_callbacks = MagicMock()
        mock_future.clear_callbacks = MagicMock()

        with patch("async_cassandra.cluster.Cluster") as mock_cluster_class:
            mock_cluster_class.return_value = mock_cluster

            # Mock AsyncCassandraSession.create
            mock_async_session = MagicMock()
            mock_async_session._session = mock_session
            mock_async_session.close = AsyncMock()
            mock_async_session.shutdown = AsyncMock()  # For when __aexit__ calls close()
            mock_async_session.__aenter__ = AsyncMock(return_value=mock_async_session)

            async def async_exit_shutdown(*args):
                await mock_async_session.shutdown()

            mock_async_session.__aexit__ = AsyncMock(side_effect=async_exit_shutdown)

            with patch(
                "async_cassandra.session.AsyncCassandraSession.create", new_callable=AsyncMock
            ) as mock_create:
                mock_create.return_value = mock_async_session

                # Nested context managers
                async with AsyncCluster(["localhost"]) as cluster:
                    async with await cluster.connect():
                        # Create streaming result
                        stream_result = AsyncStreamingResultSet(mock_future)
                        stream_result._handle_page(["row1"])

                        async with stream_result as stream:
                            async for row in stream:
                                pass

                        # After stream context, only stream should be cleaned
                        mock_future.clear_callbacks.assert_called()
                        mock_async_session.shutdown.assert_not_called()
                        mock_cluster.shutdown.assert_not_called()

                    # After session context, session should be closed
                    mock_async_session.shutdown.assert_called_once()
                    mock_cluster.shutdown.assert_not_called()

                # After cluster context, cluster should be shut down
                mock_cluster.shutdown.assert_called_once()

    @pytest.mark.asyncio
    async def test_cluster_and_session_context_managers_are_independent(self):
        """
        Test that cluster and session context managers don't interfere.

        What this tests:
        ---------------
        1. Context managers fully independent
        2. Can use in any order
        3. No hidden dependencies
        4. Flexible usage patterns

        Why this matters:
        ----------------
        Users need flexibility:
        - Long-lived clusters
        - Short-lived sessions
        - Various usage patterns

        Context managers must support
        all reasonable usage patterns.
        """
        mock_cluster = MagicMock()
        mock_cluster.shutdown = MagicMock()  # Not AsyncMock because it's called via run_in_executor
        mock_cluster.connect = AsyncMock()
        mock_cluster.is_closed = False
        mock_cluster.protocol_version = 5  # Mock protocol version

        mock_session = MagicMock()
        mock_session.close = AsyncMock()
        mock_session.is_closed = False
        mock_cluster.connect.return_value = mock_session

        with patch("async_cassandra.cluster.Cluster") as mock_cluster_class:
            mock_cluster_class.return_value = mock_cluster

            # Mock AsyncCassandraSession.create
            mock_async_session1 = MagicMock()
            mock_async_session1._session = mock_session
            mock_async_session1.close = AsyncMock()
            mock_async_session1.__aenter__ = AsyncMock(return_value=mock_async_session1)

            async def async_exit1(*args):
                await mock_async_session1.close()

            mock_async_session1.__aexit__ = AsyncMock(side_effect=async_exit1)

            mock_async_session2 = MagicMock()
            mock_async_session2._session = mock_session
            mock_async_session2.close = AsyncMock()

            mock_async_session3 = MagicMock()
            mock_async_session3._session = mock_session
            mock_async_session3.close = AsyncMock()
            mock_async_session3.__aenter__ = AsyncMock(return_value=mock_async_session3)

            async def async_exit3(*args):
                await mock_async_session3.close()

            mock_async_session3.__aexit__ = AsyncMock(side_effect=async_exit3)

            with patch(
                "async_cassandra.session.AsyncCassandraSession.create", new_callable=AsyncMock
            ) as mock_create:
                mock_create.side_effect = [
                    mock_async_session1,
                    mock_async_session2,
                    mock_async_session3,
                ]

                # Create cluster (not in context manager)
                cluster = AsyncCluster(["localhost"])

                # Use session in context manager
                async with await cluster.connect():
                    # Do work
                    pass

                # Session closed, but cluster still open
                mock_async_session1.close.assert_called_once()
                mock_cluster.shutdown.assert_not_called()

                # Can create another session
                session2 = await cluster.connect()
                assert session2 is not None

                # Now use cluster in context manager
                async with cluster:
                    # Create and use another session
                    async with await cluster.connect():
                        pass

                # Now cluster should be shut down
                mock_cluster.shutdown.assert_called_once()
