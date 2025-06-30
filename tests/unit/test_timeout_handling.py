"""
Unit tests for timeout handling in async-cassandra.
"""

import asyncio
from unittest.mock import Mock

import pytest

from async_cassandra.result import AsyncResultHandler, AsyncResultSet
from async_cassandra.session import AsyncCassandraSession


@pytest.mark.asyncio
class TestTimeoutHandling:
    """Test timeout functionality in async operations."""

    async def test_get_result_with_explicit_timeout(self):
        """Test that get_result respects explicit timeout parameter."""
        # Mock ResponseFuture that never completes
        response_future = Mock()
        response_future.has_more_pages = True  # Indicate more pages to prevent immediate completion
        response_future.add_callbacks = Mock()

        handler = AsyncResultHandler(response_future)

        # Test explicit timeout
        with pytest.raises(asyncio.TimeoutError):
            await handler.get_result(timeout=0.1)

    async def test_get_result_with_query_timeout(self):
        """Test that get_result uses query timeout from ResponseFuture."""
        # Mock ResponseFuture with timeout attribute
        response_future = Mock()
        response_future.has_more_pages = True  # Indicate more pages to prevent immediate completion
        response_future.add_callbacks = Mock()
        response_future.timeout = 0.1  # Query timeout

        handler = AsyncResultHandler(response_future)

        # Should timeout using query timeout
        with pytest.raises(asyncio.TimeoutError):
            await handler.get_result()  # No explicit timeout

    async def test_get_result_no_timeout(self):
        """Test that get_result works without timeout when query completes."""
        # Mock ResponseFuture that will have pages
        response_future = Mock()
        response_future.has_more_pages = True  # Start with more pages
        response_future.add_callbacks = Mock()
        response_future.start_fetching_next_page = Mock()
        # Ensure timeout is not set
        response_future.timeout = None

        handler = AsyncResultHandler(response_future)

        # Get the callback that was registered
        call_args = response_future.add_callbacks.call_args
        callback = call_args.kwargs.get("callback")

        # Simulate first page
        callback(["row1"])

        # Now indicate no more pages for second callback
        response_future.has_more_pages = False
        callback(["row2"])

        # Should complete successfully
        result = await handler.get_result()
        assert isinstance(result, AsyncResultSet)
        assert len(result.rows) == 2

    async def test_session_execute_with_timeout(self):
        """Test that session.execute passes timeout to handler."""
        # Mock cassandra session
        mock_session = Mock()
        response_future = Mock()
        response_future.has_more_pages = True  # Indicate more pages to prevent immediate completion
        response_future.add_callbacks = Mock()
        response_future.timeout = None  # No query-level timeout
        mock_session.execute_async = Mock(return_value=response_future)

        # Create async session
        async_session = AsyncCassandraSession(mock_session)

        # Test with timeout - should timeout since callbacks are never called
        with pytest.raises(asyncio.TimeoutError):
            await async_session.execute("SELECT * FROM test", timeout=0.1)

    async def test_timeout_does_not_affect_successful_queries(self):
        """Test that timeout doesn't interfere with queries that complete in time."""
        # Mock ResponseFuture that completes quickly
        response_future = Mock()
        response_future.has_more_pages = False
        response_future.add_callbacks = Mock()
        response_future.timeout = 10.0  # Long timeout

        handler = AsyncResultHandler(response_future)

        # Complete immediately after handler is created
        call_args = response_future.add_callbacks.call_args
        if call_args:
            kwargs = call_args.kwargs
            callback = kwargs.get("callback")
            if callback:
                callback(["row1"])

        # Should complete successfully without timeout
        result = await handler.get_result()
        assert len(result.rows) == 1

    async def test_timeout_cancellation_cleanup(self):
        """Test that timeout cancellation is handled properly."""
        # Mock ResponseFuture
        response_future = Mock()
        response_future.has_more_pages = True  # Indicate more pages to prevent immediate completion
        response_future.add_callbacks = Mock()

        handler = AsyncResultHandler(response_future)

        # Create a task that will be cancelled
        task = asyncio.create_task(handler.get_result(timeout=10.0))

        # Cancel the task
        await asyncio.sleep(0.1)
        task.cancel()

        # Should raise CancelledError
        with pytest.raises(asyncio.CancelledError):
            await task
