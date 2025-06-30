"""Error recovery and handling tests.

This module tests various error scenarios including NoHostAvailable,
connection errors, and proper error propagation through the async layer.
"""

import asyncio
from unittest.mock import Mock

import pytest
from cassandra import ConsistencyLevel, InvalidRequest, Unavailable
from cassandra.cluster import NoHostAvailable

from async_cassandra import AsyncCassandraSession as AsyncSession
from async_cassandra import AsyncCluster


def create_mock_response_future(rows=None, has_more_pages=False):
    """Helper to create a properly configured mock ResponseFuture."""
    mock_future = Mock()
    mock_future.has_more_pages = has_more_pages
    mock_future.timeout = None  # Avoid comparison issues
    mock_future.add_callbacks = Mock()

    def handle_callbacks(callback=None, errback=None):
        if callback:
            callback(rows if rows is not None else [])

    mock_future.add_callbacks.side_effect = handle_callbacks
    return mock_future


class TestErrorRecovery:
    """Test error recovery and handling scenarios."""

    @pytest.mark.resilience
    @pytest.mark.quick
    @pytest.mark.critical
    async def test_no_host_available_error(self):
        """Test handling of NoHostAvailable errors."""
        errors = {
            "127.0.0.1": ConnectionRefusedError("Connection refused"),
            "127.0.0.2": TimeoutError("Connection timeout"),
        }

        # Create a real async session with mocked underlying session
        mock_session = Mock()
        mock_session.execute_async.side_effect = NoHostAvailable(
            "Unable to connect to any servers", errors
        )

        async_session = AsyncSession(mock_session)

        with pytest.raises(NoHostAvailable) as exc_info:
            await async_session.execute("SELECT * FROM users")

        assert "Unable to connect to any servers" in str(exc_info.value)
        assert "127.0.0.1" in exc_info.value.errors
        assert "127.0.0.2" in exc_info.value.errors

    @pytest.mark.resilience
    async def test_invalid_request_error(self):
        """Test handling of invalid request errors."""
        mock_session = Mock()
        mock_session.execute_async.side_effect = InvalidRequest("Invalid CQL syntax")

        async_session = AsyncSession(mock_session)

        with pytest.raises(InvalidRequest, match="Invalid CQL syntax"):
            await async_session.execute("INVALID QUERY SYNTAX")

    @pytest.mark.resilience
    async def test_unavailable_error(self):
        """Test handling of unavailable errors."""
        mock_session = Mock()
        mock_session.execute_async.side_effect = Unavailable(
            "Cannot achieve consistency",
            consistency=ConsistencyLevel.QUORUM,
            required_replicas=2,
            alive_replicas=1,
        )

        async_session = AsyncSession(mock_session)

        with pytest.raises(Unavailable) as exc_info:
            await async_session.execute("SELECT * FROM users")

        assert exc_info.value.consistency == ConsistencyLevel.QUORUM
        assert exc_info.value.required_replicas == 2
        assert exc_info.value.alive_replicas == 1

    @pytest.mark.resilience
    @pytest.mark.critical
    async def test_error_in_async_callback(self):
        """Test error handling in async callbacks."""
        from async_cassandra.result import AsyncResultHandler

        # Create a mock ResponseFuture
        mock_future = Mock()
        mock_future.has_more_pages = False
        mock_future.add_callbacks = Mock()
        mock_future.timeout = None  # Set timeout to None to avoid comparison issues

        handler = AsyncResultHandler(mock_future)
        test_error = RuntimeError("Callback error")

        # Manually call the error handler to simulate callback error
        handler._handle_error(test_error)

        with pytest.raises(RuntimeError, match="Callback error"):
            await handler.get_result()

    @pytest.mark.resilience
    async def test_connection_pool_exhaustion_recovery(self):
        """Test recovery from connection pool exhaustion."""
        mock_session = Mock()

        # Create a mock ResponseFuture for successful response
        mock_future = create_mock_response_future([{"id": 1}])

        # Simulate pool exhaustion then recovery
        responses = [
            NoHostAvailable("Pool exhausted", {}),
            NoHostAvailable("Pool exhausted", {}),
            mock_future,  # Recovery returns ResponseFuture
        ]
        mock_session.execute_async.side_effect = responses

        async_session = AsyncSession(mock_session)

        # First two attempts fail
        for i in range(2):
            with pytest.raises(NoHostAvailable):
                await async_session.execute("SELECT * FROM users")

        # Third attempt succeeds
        result = await async_session.execute("SELECT * FROM users")
        assert result._rows == [{"id": 1}]

    @pytest.mark.resilience
    async def test_partial_write_error_handling(self):
        """Test handling of partial write errors."""
        mock_session = Mock()

        # Simulate partial write success
        mock_session.execute_async.side_effect = Exception(
            "Coordinator node timed out during write"
        )

        async_session = AsyncSession(mock_session)

        with pytest.raises(Exception, match="Coordinator node timed out"):
            await async_session.execute("INSERT INTO users (id, name) VALUES (?, ?)", [1, "test"])

    @pytest.mark.resilience
    async def test_error_during_prepared_statement(self):
        """Test error handling during prepared statement execution."""
        mock_session = Mock()
        mock_prepared = Mock()

        # Prepare succeeds
        mock_session.prepare.return_value = mock_prepared

        # But execution fails
        mock_session.execute_async.side_effect = InvalidRequest("Invalid parameter")

        async_session = AsyncSession(mock_session)

        # Prepare statement
        prepared = await async_session.prepare("SELECT * FROM users WHERE id = ?")
        assert prepared == mock_prepared

        # Execute should fail
        with pytest.raises(InvalidRequest, match="Invalid parameter"):
            await async_session.execute(prepared, [None])

    @pytest.mark.resilience
    @pytest.mark.critical
    @pytest.mark.timeout(40)  # Increase timeout to account for 5s shutdown delay
    async def test_graceful_shutdown_with_pending_queries(self):
        """Test graceful shutdown when queries are pending."""
        mock_session = Mock()
        mock_cluster = Mock()

        # Track shutdown completion
        shutdown_complete = asyncio.Event()

        # Mock the cluster shutdown to complete quickly
        def mock_shutdown():
            shutdown_complete.set()

        mock_cluster.shutdown = mock_shutdown

        # Create queries that will complete after a delay
        query_complete = asyncio.Event()

        # Create mock ResponseFutures
        def create_mock_future(*args):
            mock_future = Mock()
            mock_future.has_more_pages = False
            mock_future.timeout = None
            mock_future.add_callbacks = Mock()

            def handle_callbacks(callback=None, errback=None):
                # Schedule the callback to be called after a short delay
                # This simulates a query that completes during shutdown
                def delayed_callback():
                    if callback:
                        callback([])  # Call with empty rows
                    query_complete.set()

                # Use asyncio to schedule the callback
                asyncio.get_event_loop().call_later(0.1, delayed_callback)

            mock_future.add_callbacks.side_effect = handle_callbacks
            return mock_future

        mock_session.execute_async.side_effect = create_mock_future

        cluster = AsyncCluster()
        cluster._cluster = mock_cluster
        cluster._cluster.protocol_version = 5  # Mock protocol version
        cluster._cluster.connect.return_value = mock_session

        session = await cluster.connect()

        # Start a query
        query_task = asyncio.create_task(session.execute("SELECT * FROM table"))

        # Give query time to start
        await asyncio.sleep(0.05)

        # Start shutdown in background (it will wait 5 seconds after driver shutdown)
        shutdown_task = asyncio.create_task(cluster.shutdown())

        # Wait for driver shutdown to complete
        await shutdown_complete.wait()

        # Query should complete during the 5 second wait
        await query_complete.wait()

        # Wait for the query task to actually complete
        # Use wait_for with a timeout to avoid hanging if something goes wrong
        try:
            await asyncio.wait_for(query_task, timeout=1.0)
        except asyncio.TimeoutError:
            pytest.fail("Query task did not complete within timeout")

        # Wait for full shutdown including the 5 second delay
        await shutdown_task

        # Verify everything completed properly
        assert query_task.done()
        assert not query_task.cancelled()  # Query completed normally
        assert cluster.is_closed

    @pytest.mark.resilience
    async def test_error_stack_trace_preservation(self):
        """Test that error stack traces are preserved through async layer."""
        mock_session = Mock()

        # Create an error with traceback info
        try:
            raise InvalidRequest("Original error")
        except InvalidRequest as e:
            original_error = e

        mock_session.execute_async.side_effect = original_error

        async_session = AsyncSession(mock_session)

        try:
            await async_session.execute("SELECT * FROM users")
        except InvalidRequest as e:
            # Stack trace should be preserved
            assert str(e) == "Original error"
            assert e.__traceback__ is not None

    @pytest.mark.resilience
    async def test_concurrent_error_isolation(self):
        """Test that errors in concurrent queries don't affect each other."""
        mock_session = Mock()

        # Different errors for different queries
        def execute_side_effect(query, *args, **kwargs):
            if "table1" in query:
                raise InvalidRequest("Error in table1")
            elif "table2" in query:
                # Create a mock ResponseFuture for success
                return create_mock_response_future([{"id": 2}])
            elif "table3" in query:
                raise NoHostAvailable("No hosts for table3", {})
            else:
                # Create a mock ResponseFuture for empty result
                return create_mock_response_future([])

        mock_session.execute_async.side_effect = execute_side_effect

        async_session = AsyncSession(mock_session)

        # Execute queries concurrently
        tasks = [
            async_session.execute("SELECT * FROM table1"),
            async_session.execute("SELECT * FROM table2"),
            async_session.execute("SELECT * FROM table3"),
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Verify each query got its expected result/error
        assert isinstance(results[0], InvalidRequest)
        assert "Error in table1" in str(results[0])

        assert not isinstance(results[1], Exception)
        assert results[1]._rows == [{"id": 2}]

        assert isinstance(results[2], NoHostAvailable)
        assert "No hosts for table3" in str(results[2])
