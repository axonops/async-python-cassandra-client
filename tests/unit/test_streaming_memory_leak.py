"""
Unit tests for streaming memory leak fixes.
"""

from unittest.mock import Mock

import pytest

from async_cassandra.streaming import AsyncStreamingResultSet, StreamConfig


@pytest.mark.asyncio
class TestStreamingMemoryLeak:
    """Test memory leak fixes in streaming error scenarios."""

    async def test_current_page_cleared_on_error(self):
        """Test that _current_page is cleared when an error occurs."""
        # Mock ResponseFuture with some data
        response_future = Mock()
        response_future.has_more_pages = True
        response_future.add_callbacks = Mock()

        # Create streaming result set
        config = StreamConfig(fetch_size=100)
        result_set = AsyncStreamingResultSet(response_future, config)

        # Simulate receiving a large page of data
        large_page = ["row" + str(i) for i in range(1000)]

        # Trigger page callback to populate _current_page
        args = response_future.add_callbacks.call_args
        callback = args[1]["callback"]
        callback(large_page)

        # Verify page is populated
        assert len(result_set._current_page) == 1000
        assert result_set._current_index == 0

        # Simulate an error
        errback = args[1]["errback"]
        errback(Exception("Connection lost"))

        # Verify that _current_page was cleared to prevent memory leak
        assert len(result_set._current_page) == 0
        assert result_set._current_index == 0
        assert result_set._exhausted is True
        assert result_set._error is not None

    async def test_memory_released_after_error_during_iteration(self):
        """Test that memory is released when error occurs during iteration."""
        # Mock ResponseFuture
        response_future = Mock()
        response_future.has_more_pages = True
        response_future._final_exception = None
        response_future.add_callbacks = Mock()

        # Create streaming result set
        config = StreamConfig(fetch_size=50)
        result_set = AsyncStreamingResultSet(response_future, config)

        # Add first page of data from a thread
        first_page = ["row" + str(i) for i in range(50)]
        args = response_future.add_callbacks.call_args
        callback = args[1]["callback"]
        errback = args[1]["errback"]

        import threading

        def thread_callback():
            callback(first_page)

        thread = threading.Thread(target=thread_callback)
        thread.start()
        thread.join()

        # Start iterating
        rows_read = 0
        try:
            async for row in result_set:
                rows_read += 1
                # After reading 10 rows, simulate an error from a thread
                if rows_read == 10:

                    def thread_errback():
                        errback(Exception("Network error"))

                    thread = threading.Thread(target=thread_errback)
                    thread.start()
        except Exception:
            pass

        # Verify cleanup happened
        assert result_set._current_page == []  # Error handler should clear the page
        assert result_set._error is not None
        assert rows_read == 10  # Should have read exactly 10 rows before error

    async def test_multiple_errors_dont_accumulate_memory(self):
        """Test that multiple errors don't cause memory accumulation."""
        # Mock ResponseFuture
        response_future = Mock()
        response_future.has_more_pages = True
        response_future.add_callbacks = Mock()

        # Create streaming result set
        config = StreamConfig(fetch_size=100)
        result_set = AsyncStreamingResultSet(response_future, config)

        args = response_future.add_callbacks.call_args
        callback = args[1]["callback"]
        errback = args[1]["errback"]

        # Simulate multiple cycles of data and errors
        for i in range(5):
            # Add a page of data
            page = ["row" + str(j) for j in range(100)]
            callback(page)

            # Verify page is there
            assert len(result_set._current_page) == 100

            # Trigger an error
            errback(Exception(f"Error {i}"))

            # Verify cleanup
            assert len(result_set._current_page) == 0
            assert result_set._current_index == 0

    async def test_close_clears_memory(self):
        """Test that close() method properly clears memory."""
        # Mock ResponseFuture
        response_future = Mock()
        response_future.has_more_pages = False
        response_future.add_callbacks = Mock()

        # Create streaming result set
        config = StreamConfig(fetch_size=100)
        result_set = AsyncStreamingResultSet(response_future, config)

        # Add some data
        args = response_future.add_callbacks.call_args
        callback = args[1]["callback"]
        callback(["row1", "row2", "row3"])

        # Verify data is there
        assert len(result_set._current_page) == 3

        # Close the result set
        await result_set.close()

        # Verify cleanup - close() clears _current_page
        assert result_set._current_page == []
        assert result_set._closed
        assert result_set._exhausted

    async def test_error_during_page_callback_clears_memory(self):
        """Test that errors in page callback still clear memory properly."""
        # Mock ResponseFuture
        response_future = Mock()
        response_future.has_more_pages = True
        response_future.add_callbacks = Mock()

        # Create streaming result set with a faulty page callback
        def bad_callback(page_num, total_rows):
            if page_num > 1:
                raise Exception("Callback error")

        config = StreamConfig(fetch_size=50, page_callback=bad_callback)
        result_set = AsyncStreamingResultSet(response_future, config)

        args = response_future.add_callbacks.call_args
        callback = args[1]["callback"]
        errback = args[1]["errback"]

        # First page should work
        callback(["row1", "row2"])
        assert len(result_set._current_page) == 2

        # Simulate error on second page
        # The callback error is caught and logged, but then we simulate a real error
        errback(Exception("Query error"))

        # Memory should still be cleared
        assert len(result_set._current_page) == 0
        assert result_set._error is not None
