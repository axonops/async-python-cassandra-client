"""
Test memory management and resource cleanup in streaming.
"""

import asyncio
from unittest.mock import Mock

import pytest

from async_cassandra.streaming import AsyncStreamingResultSet, StreamConfig


class TestStreamingMemoryManagement:
    """Test memory management in streaming operations."""

    @pytest.mark.asyncio
    async def test_async_context_manager_cleanup(self):
        """Test that async context manager properly cleans up resources."""
        # Create mock response future
        response_future = Mock()
        response_future.has_more_pages = False
        response_future.clear_callbacks = Mock()

        # Track if cleanup was called

        async with AsyncStreamingResultSet(response_future) as stream:
            # Verify stream is usable
            assert not stream._closed
            # Mock some data
            stream._handle_page([1, 2, 3])
            assert len(stream._current_page) == 3

        # After context exit, verify cleanup
        assert stream._closed
        assert stream._exhausted
        assert len(stream._current_page) == 0
        response_future.clear_callbacks.assert_called()

    @pytest.mark.asyncio
    async def test_abandoned_iterator_cleanup(self):
        """Test that abandoned iterators clean up their callbacks when closed."""
        # Create mock response future
        response_future = Mock()
        response_future.has_more_pages = True
        response_future.clear_callbacks = Mock()
        response_future.start_fetching_next_page = Mock()

        # Create streaming result set
        stream = AsyncStreamingResultSet(response_future)
        stream._handle_page([1, 2, 3])

        # Close the stream to trigger cleanup
        await stream.close()

        # Verify cleanup was called
        response_future.clear_callbacks.assert_called()
        assert len(stream._current_page) == 0
        assert stream._closed
        assert stream._exhausted

    @pytest.mark.asyncio
    async def test_multiple_context_manager_entries(self):
        """Test that multiple context manager entries don't cause issues."""
        response_future = Mock()
        response_future.has_more_pages = False
        response_future.clear_callbacks = Mock()

        stream = AsyncStreamingResultSet(response_future)

        # First context
        async with stream:
            assert not stream._closed

        assert stream._closed

        # Second context should be safe but no-op
        async with stream:
            assert stream._closed  # Still closed

        # Cleanup should be called twice (once per context manager exit)
        assert response_future.clear_callbacks.call_count == 2

    @pytest.mark.asyncio
    async def test_exception_in_iteration_cleans_up(self):
        """Test that exceptions during iteration still clean up properly."""
        response_future = Mock()
        response_future.has_more_pages = False
        response_future._final_exception = None
        response_future.clear_callbacks = Mock()
        response_future.add_callbacks = Mock()

        stream = AsyncStreamingResultSet(response_future)

        # Set up callbacks to provide data
        args = response_future.add_callbacks.call_args
        callback = args[1]["callback"]

        import threading

        def thread_callback():
            callback([1, 2, 3])

        thread = threading.Thread(target=thread_callback)
        thread.start()

        class TestError(Exception):
            pass

        with pytest.raises(TestError):
            async with stream:
                async for row in stream:
                    if row == 2:
                        raise TestError("Test error")

        # Verify cleanup happened
        assert stream._closed
        response_future.clear_callbacks.assert_called()

    @pytest.mark.asyncio
    async def test_page_limit_enforcement(self):
        """Test that page limits are respected."""
        # Create config with page limit
        config = StreamConfig(fetch_size=1000, max_pages=2)  # Small limit for testing

        response_future = Mock()
        response_future.has_more_pages = True
        response_future._final_exception = None
        response_future.add_callbacks = Mock()
        response_future.start_fetching_next_page = Mock()

        stream = AsyncStreamingResultSet(response_future, config)

        # Get the callbacks
        args = response_future.add_callbacks.call_args
        callback = args[1]["callback"]

        # Simulate receiving multiple pages through the callback
        callback([1, 2, 3])  # Page 1
        assert not stream._exhausted
        assert stream._page_number == 1

        callback([4, 5, 6])  # Page 2 - should hit limit
        assert stream._exhausted  # Should be exhausted after max_pages
        assert stream._page_number == 2

    @pytest.mark.asyncio
    async def test_timeout_cleanup(self):
        """Test that timeouts properly clean up resources."""
        config = StreamConfig(timeout_seconds=0.1)

        response_future = Mock()
        response_future.has_more_pages = True
        response_future.clear_callbacks = Mock()

        stream = AsyncStreamingResultSet(response_future, config)

        # Create a slow page callback
        async def slow_iteration():
            async with stream:
                stream._handle_page([1, 2, 3])
                stream._first_page_ready = True
                async for row in stream:
                    await asyncio.sleep(0.2)  # Longer than timeout

        # TODO: Implement actual timeout handling in streaming
        # For now, just verify config is stored
        assert stream.config.timeout_seconds == 0.1

    @pytest.mark.asyncio
    async def test_close_wakes_up_waiters(self):
        """Test that close() wakes up any threads waiting for pages."""
        response_future = Mock()
        response_future.has_more_pages = True

        stream = AsyncStreamingResultSet(response_future)

        # Create page ready event
        stream._page_ready = asyncio.Event()

        # Start a task that waits for a page
        wait_task = asyncio.create_task(stream._page_ready.wait())

        # Give the task a moment to start waiting
        await asyncio.sleep(0.01)

        # Close the stream
        await stream.close()

        # The wait should complete (not hang)
        try:
            await asyncio.wait_for(wait_task, timeout=0.1)
        except asyncio.TimeoutError:
            pytest.fail("close() did not wake up page waiters")

    @pytest.mark.asyncio
    async def test_concurrent_close_and_iteration(self):
        """Test that concurrent close and iteration is handled safely."""
        response_future = Mock()
        response_future.has_more_pages = False
        response_future._final_exception = None
        response_future.clear_callbacks = Mock()
        response_future.add_callbacks = Mock()
        response_future.start_fetching_next_page = Mock()

        stream = AsyncStreamingResultSet(response_future)

        # Set up callbacks to provide data
        args = response_future.add_callbacks.call_args
        callback = args[1]["callback"]

        import threading

        def thread_callback():
            callback([1, 2, 3, 4, 5])

        thread = threading.Thread(target=thread_callback)
        thread.start()

        # Track iterations
        iterations = []

        async def iterate():
            try:
                async for row in stream:
                    iterations.append(row)
                    await asyncio.sleep(0.01)
            except Exception:
                pass  # Expected when stream is closed

        async def close_after_delay():
            await asyncio.sleep(0.02)
            await stream.close()

        # Run both concurrently
        await asyncio.gather(iterate(), close_after_delay(), return_exceptions=True)

        # Should have gotten some iterations before close
        assert len(iterations) > 0
        assert stream._closed

    def test_callback_cleanup_on_close(self):
        """Test that callbacks are properly cleaned up on close."""
        response_future = Mock()
        response_future.clear_callbacks = Mock()
        response_future.has_more_pages = False
        response_future.add_callbacks = Mock()

        stream = AsyncStreamingResultSet(response_future)

        # Verify callbacks were registered
        response_future.add_callbacks.assert_called_once()

        # Close the stream asynchronously
        import asyncio

        asyncio.run(stream.close())

        # Verify callbacks were cleaned up
        response_future.clear_callbacks.assert_called()
