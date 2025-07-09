"""
Unit tests for event loop reference handling.
"""

import asyncio
from unittest.mock import Mock

import pytest

from async_cassandra.result import AsyncResultHandler
from async_cassandra.streaming import AsyncStreamingResultSet


@pytest.mark.asyncio
class TestEventLoopHandling:
    """Test that event loop references are not stored."""

    async def test_result_handler_no_stored_loop_reference(self):
        """
        Test that AsyncResultHandler doesn't store event loop reference initially.

        What this tests:
        ---------------
        1. No loop reference at creation
        2. Future not created eagerly
        3. Early result tracking exists
        4. Lazy initialization pattern

        Why this matters:
        ----------------
        Event loop references problematic:
        - Can't share across threads
        - Prevents object reuse
        - Causes "attached to different loop" errors

        Lazy creation allows flexible
        usage across different contexts.
        """
        # Create handler
        response_future = Mock()
        response_future.has_more_pages = False
        response_future.add_callbacks = Mock()
        response_future.timeout = None

        handler = AsyncResultHandler(response_future)

        # Verify no _loop attribute initially
        assert not hasattr(handler, "_loop")
        # Future should be None initially
        assert handler._future is None
        # Should have early result/error tracking
        assert hasattr(handler, "_early_result")
        assert hasattr(handler, "_early_error")

    async def test_streaming_no_stored_loop_reference(self):
        """
        Test that AsyncStreamingResultSet doesn't store event loop reference initially.

        What this tests:
        ---------------
        1. Loop starts as None
        2. No eager event creation
        3. Clean initial state
        4. Ready for any loop

        Why this matters:
        ----------------
        Streaming objects created in threads:
        - Driver callbacks from thread pool
        - No event loop in creation context
        - Must defer loop capture

        Enables thread-safe object creation
        before async iteration.
        """
        # Create streaming result set
        response_future = Mock()
        response_future.has_more_pages = False
        response_future.add_callbacks = Mock()

        result_set = AsyncStreamingResultSet(response_future)

        # _loop is initialized to None
        assert result_set._loop is None

    async def test_future_created_on_first_get_result(self):
        """
        Test that future is created on first call to get_result.

        What this tests:
        ---------------
        1. Future created on demand
        2. Loop captured at usage time
        3. Callbacks work correctly
        4. Results properly aggregated

        Why this matters:
        ----------------
        Just-in-time future creation:
        - Captures correct event loop
        - Avoids cross-loop issues
        - Works with any async context

        Critical for framework integration
        where object creation context differs
        from usage context.
        """
        # Create handler with has_more_pages=True to prevent immediate completion
        response_future = Mock()
        response_future.has_more_pages = True  # Start with more pages
        response_future.add_callbacks = Mock()
        response_future.start_fetching_next_page = Mock()
        response_future.timeout = None

        handler = AsyncResultHandler(response_future)

        # Future should not be created yet
        assert handler._future is None

        # Get the callback that was registered
        call_args = response_future.add_callbacks.call_args
        callback = call_args.kwargs.get("callback") if call_args else None

        # Start get_result task
        result_task = asyncio.create_task(handler.get_result())
        await asyncio.sleep(0.01)

        # Future should now be created
        assert handler._future is not None
        assert hasattr(handler, "_loop")

        # Trigger callbacks to complete the future
        if callback:
            # First page
            callback(["row1"])
            # Now indicate no more pages
            response_future.has_more_pages = False
            # Second page (final)
            callback(["row2"])

        # Get result
        result = await result_task
        assert len(result.rows) == 2

    async def test_streaming_page_ready_lazy_creation(self):
        """
        Test that page_ready event is created lazily.

        What this tests:
        ---------------
        1. Event created on iteration start
        2. Thread callbacks work correctly
        3. Loop captured at right time
        4. Cross-thread coordination works

        Why this matters:
        ----------------
        Streaming uses thread callbacks:
        - Driver calls from thread pool
        - Event needed for coordination
        - Must work across thread boundaries

        Lazy event creation ensures
        correct loop association for
        thread-to-async communication.
        """
        # Create streaming result set
        response_future = Mock()
        response_future.has_more_pages = False
        response_future._final_exception = None  # Important: must be None
        response_future.add_callbacks = Mock()

        result_set = AsyncStreamingResultSet(response_future)

        # Page ready event should not exist yet
        assert result_set._page_ready is None

        # Trigger callback from a thread (like the real driver)
        args = response_future.add_callbacks.call_args
        callback = args[1]["callback"]

        import threading

        def thread_callback():
            callback(["row1", "row2"])

        thread = threading.Thread(target=thread_callback)
        thread.start()

        # Start iteration - this should create the event
        rows = []
        async for row in result_set:
            rows.append(row)

        # Now page_ready should be created
        assert result_set._page_ready is not None
        assert isinstance(result_set._page_ready, asyncio.Event)
        assert len(rows) == 2

        # Loop should also be stored now
        assert result_set._loop is not None
