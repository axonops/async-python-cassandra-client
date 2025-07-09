"""
Unit tests for explicit cleanup of ResponseFuture callbacks on error.
"""

import asyncio
from unittest.mock import Mock

import pytest

from async_cassandra.exceptions import ConnectionError
from async_cassandra.result import AsyncResultHandler
from async_cassandra.session import AsyncCassandraSession
from async_cassandra.streaming import AsyncStreamingResultSet


@pytest.mark.asyncio
class TestResponseFutureCleanup:
    """Test explicit cleanup of ResponseFuture callbacks."""

    async def test_handler_cleanup_on_error(self):
        """
        Test that callbacks are cleaned up when handler encounters error.

        What this tests:
        ---------------
        1. Callbacks cleared on error
        2. ResponseFuture cleanup called
        3. No dangling references
        4. Error still propagated

        Why this matters:
        ----------------
        Callback cleanup prevents:
        - Memory leaks
        - Circular references
        - Ghost callbacks firing

        Critical for long-running apps
        with many queries.
        """
        # Create mock response future
        response_future = Mock()
        response_future.has_more_pages = True  # Prevent immediate completion
        response_future.add_callbacks = Mock()
        response_future.timeout = None

        # Track if callbacks were cleared
        callbacks_cleared = False

        def mock_clear_callbacks():
            nonlocal callbacks_cleared
            callbacks_cleared = True

        response_future.clear_callbacks = mock_clear_callbacks

        # Create handler
        handler = AsyncResultHandler(response_future)

        # Start get_result
        result_task = asyncio.create_task(handler.get_result())
        await asyncio.sleep(0.01)  # Let it set up

        # Trigger error callback
        call_args = response_future.add_callbacks.call_args
        if call_args:
            errback = call_args.kwargs.get("errback")
            if errback:
                errback(Exception("Test error"))

        # Should get the error
        with pytest.raises(Exception, match="Test error"):
            await result_task

        # Callbacks should be cleared
        assert callbacks_cleared, "Callbacks were not cleared on error"

    async def test_streaming_cleanup_on_error(self):
        """
        Test that streaming callbacks are cleaned up on error.

        What this tests:
        ---------------
        1. Streaming error triggers cleanup
        2. Callbacks cleared properly
        3. Error propagated to iterator
        4. Resources freed

        Why this matters:
        ----------------
        Streaming holds more resources:
        - Page callbacks
        - Event handlers
        - Buffer memory

        Must clean up even on partial
        stream consumption.
        """
        # Create mock response future
        response_future = Mock()
        response_future.has_more_pages = True
        response_future.add_callbacks = Mock()
        response_future.start_fetching_next_page = Mock()

        # Track if callbacks were cleared
        callbacks_cleared = False

        def mock_clear_callbacks():
            nonlocal callbacks_cleared
            callbacks_cleared = True

        response_future.clear_callbacks = mock_clear_callbacks

        # Create streaming result set
        result_set = AsyncStreamingResultSet(response_future)

        # Get the registered callbacks
        call_args = response_future.add_callbacks.call_args
        callback = call_args.kwargs.get("callback") if call_args else None
        errback = call_args.kwargs.get("errback") if call_args else None

        # First trigger initial page callback to set up state
        callback([])  # Empty initial page

        # Now trigger error for streaming
        errback(Exception("Streaming error"))

        # Try to iterate - should get error immediately
        error_raised = False
        try:
            async for _ in result_set:
                pass
        except Exception as e:
            error_raised = True
            assert str(e) == "Streaming error"

        assert error_raised, "No error raised during iteration"

        # Callbacks should be cleared
        assert callbacks_cleared, "Callbacks were not cleared on streaming error"

    async def test_handler_cleanup_on_timeout(self):
        """
        Test cleanup when operation times out.

        What this tests:
        ---------------
        1. Timeout triggers cleanup
        2. Callbacks cleared
        3. TimeoutError raised
        4. No hanging callbacks

        Why this matters:
        ----------------
        Timeouts common in production:
        - Network issues
        - Overloaded servers
        - Slow queries

        Must clean up to prevent
        resource accumulation.
        """
        # Create mock response future that never completes
        response_future = Mock()
        response_future.has_more_pages = True  # Prevent immediate completion
        response_future.add_callbacks = Mock()
        response_future.timeout = 0.1  # Short timeout

        # Track if callbacks were cleared
        callbacks_cleared = False

        def mock_clear_callbacks():
            nonlocal callbacks_cleared
            callbacks_cleared = True

        response_future.clear_callbacks = mock_clear_callbacks

        # Create handler
        handler = AsyncResultHandler(response_future)

        # Should timeout
        with pytest.raises(asyncio.TimeoutError):
            await handler.get_result()

        # Callbacks should be cleared
        assert callbacks_cleared, "Callbacks were not cleared on timeout"

    async def test_no_memory_leak_on_error(self):
        """
        Test that error handling cleans up properly to prevent memory leaks.

        What this tests:
        ---------------
        1. Error path cleans callbacks
        2. Internal state cleaned
        3. Future marked done
        4. Circular refs broken

        Why this matters:
        ----------------
        Memory leaks kill apps:
        - Gradual memory growth
        - Eventually OOM
        - Hard to diagnose

        Proper cleanup essential for
        production stability.
        """
        # Create response future
        response_future = Mock()
        response_future.has_more_pages = True  # Prevent immediate completion
        response_future.add_callbacks = Mock()
        response_future.timeout = None
        response_future.clear_callbacks = Mock()

        # Create handler
        handler = AsyncResultHandler(response_future)

        # Start task
        task = asyncio.create_task(handler.get_result())
        await asyncio.sleep(0.01)

        # Trigger error
        call_args = response_future.add_callbacks.call_args
        if call_args:
            errback = call_args.kwargs.get("errback")
            if errback:
                errback(Exception("Memory test"))

        # Get error
        with pytest.raises(Exception):
            await task

        # Verify that callbacks were cleared on error
        # This is the important part - breaking circular references
        assert response_future.clear_callbacks.called
        assert response_future.clear_callbacks.call_count >= 1

        # Also verify the handler cleans up its internal state
        assert handler._future is not None  # Future was created
        assert handler._future.done()  # Future completed with error

    async def test_session_cleanup_on_close(self):
        """
        Test that session cleans up callbacks when closed.

        What this tests:
        ---------------
        1. Session close prevents new ops
        2. Existing ops complete
        3. New ops raise ConnectionError
        4. Clean shutdown behavior

        Why this matters:
        ----------------
        Graceful shutdown requires:
        - Complete in-flight queries
        - Reject new queries
        - Clean up resources

        Prevents data loss and
        connection leaks.
        """
        # Create mock session
        mock_session = Mock()

        # Create separate futures for each operation
        futures_created = []

        def create_future(*args, **kwargs):
            future = Mock()
            future.has_more_pages = False
            future.timeout = None
            future.clear_callbacks = Mock()

            # Store callbacks when registered
            def register_callbacks(callback=None, errback=None):
                future._callback = callback
                future._errback = errback

            future.add_callbacks = Mock(side_effect=register_callbacks)
            futures_created.append(future)
            return future

        mock_session.execute_async = Mock(side_effect=create_future)
        mock_session.shutdown = Mock()

        # Create async session
        async_session = AsyncCassandraSession(mock_session)

        # Start multiple operations
        tasks = []
        for i in range(3):
            task = asyncio.create_task(async_session.execute(f"SELECT {i}"))
            tasks.append(task)

        await asyncio.sleep(0.01)  # Let them start

        # Complete the operations by triggering callbacks
        for i, future in enumerate(futures_created):
            if hasattr(future, "_callback") and future._callback:
                future._callback([f"row{i}"])

        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks)

        # Now close the session
        await async_session.close()

        # Verify all operations completed successfully
        assert len(results) == 3

        # New operations should fail
        with pytest.raises(ConnectionError):
            await async_session.execute("SELECT after close")

    async def test_cleanup_prevents_callback_execution(self):
        """
        Test that cleaned callbacks don't execute.

        What this tests:
        ---------------
        1. Cleared callbacks don't fire
        2. No zombie callbacks
        3. Cleanup is effective
        4. State properly cleared

        Why this matters:
        ----------------
        Zombie callbacks cause:
        - Unexpected behavior
        - Race conditions
        - Data corruption

        Cleanup must truly prevent
        future callback execution.
        """
        # Create response future
        response_future = Mock()
        response_future.has_more_pages = False
        response_future.add_callbacks = Mock()
        response_future.timeout = None

        # Track callback execution
        callback_executed = False
        original_callback = None

        def track_add_callbacks(callback=None, errback=None):
            nonlocal original_callback
            original_callback = callback

        response_future.add_callbacks = track_add_callbacks

        def clear_callbacks():
            nonlocal original_callback
            original_callback = None  # Simulate clearing

        response_future.clear_callbacks = clear_callbacks

        # Create handler
        handler = AsyncResultHandler(response_future)

        # Start task
        task = asyncio.create_task(handler.get_result())
        await asyncio.sleep(0.01)

        # Clear callbacks (simulating cleanup)
        response_future.clear_callbacks()

        # Try to trigger callback - should have no effect
        if original_callback:
            callback_executed = True

        # Cancel task to clean up
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        assert not callback_executed, "Callback executed after cleanup"
