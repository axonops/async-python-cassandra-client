"""Consolidated streaming functionality tests.

This module combines all streaming tests including basic functionality,
memory management, and error handling.
"""

import asyncio
import gc
import threading
import weakref
from unittest.mock import Mock

import pytest
from cassandra import ConsistencyLevel, InvalidRequest

from async_cassandra.streaming import (
    AsyncStreamingResultSet,
    StreamConfig,
    StreamingResultHandler,
    create_streaming_statement,
)


class TestStreamingCore:
    """Core streaming functionality tests."""

    @pytest.mark.features
    @pytest.mark.quick
    @pytest.mark.critical
    async def test_single_page_streaming(self):
        """Test streaming with single page of results."""
        mock_result_set = Mock()
        mock_result_set.has_more_pages = False
        # Ensure _final_exception is None to avoid triggering error path
        mock_result_set._final_exception = None

        # We need to simulate the callback being called from a thread
        # The AsyncStreamingResultSet expects callbacks to be called after it sets up
        def add_callbacks(callback=None, errback=None):
            # Simulate the driver calling the callback from a thread
            if callback:
                # Call from a thread like the actual driver does
                def thread_callback():
                    callback([1, 2, 3])

                thread = threading.Thread(target=thread_callback)
                thread.start()

        mock_result_set.add_callbacks = add_callbacks

        streaming_result = AsyncStreamingResultSet(mock_result_set)

        results = []
        async for row in streaming_result:
            results.append(row)

        assert results == [1, 2, 3]

    @pytest.mark.features
    @pytest.mark.critical
    async def test_multi_page_streaming(self):
        """Test streaming with multiple pages."""
        # First page
        mock_result_set = Mock()
        mock_result_set.has_more_pages = True
        mock_result_set._final_exception = None

        callbacks = {"callback": None, "errback": None}
        page_count = 0

        def add_callbacks(callback=None, errback=None):
            nonlocal page_count
            callbacks["callback"] = callback
            callbacks["errback"] = errback

            # Simulate first page callback
            if callback and page_count == 0:
                page_count += 1

                def thread_callback():
                    callback([1, 2, 3])

                thread = threading.Thread(target=thread_callback)
                thread.start()

        def start_fetching():
            # Simulate fetching next page
            mock_result_set.has_more_pages = False
            if callbacks["callback"]:

                def thread_callback():
                    callbacks["callback"]([4, 5, 6])

                thread = threading.Thread(target=thread_callback)
                thread.start()

        mock_result_set.add_callbacks = add_callbacks
        mock_result_set.start_fetching_next_page = start_fetching

        streaming_result = AsyncStreamingResultSet(mock_result_set)

        results = []
        async for row in streaming_result:
            results.append(row)

        assert results == [1, 2, 3, 4, 5, 6]

    @pytest.mark.features
    async def test_page_based_iteration(self):
        """Test iterating by pages instead of rows."""
        # First page
        mock_result_set = Mock()
        mock_result_set.has_more_pages = True
        mock_result_set._final_exception = None

        callbacks = {"callback": None, "errback": None}
        page_count = 0

        def add_callbacks(callback=None, errback=None):
            nonlocal page_count
            callbacks["callback"] = callback
            callbacks["errback"] = errback

            # Simulate first page callback
            if callback and page_count == 0:
                page_count += 1

                def thread_callback():
                    callback([1, 2, 3])

                thread = threading.Thread(target=thread_callback)
                thread.start()

        def start_fetching():
            # Simulate fetching next page
            mock_result_set.has_more_pages = False
            if callbacks["callback"]:

                def thread_callback():
                    callbacks["callback"]([4, 5, 6])

                thread = threading.Thread(target=thread_callback)
                thread.start()

        mock_result_set.add_callbacks = add_callbacks
        mock_result_set.start_fetching_next_page = start_fetching

        streaming_result = AsyncStreamingResultSet(mock_result_set)

        pages = []
        async for page in streaming_result.pages():
            pages.append(list(page))

        assert pages == [[1, 2, 3], [4, 5, 6]]

    @pytest.mark.features
    async def test_error_during_streaming(self):
        """Test error handling during streaming."""
        mock_result_set = Mock()
        mock_result_set.has_more_pages = True
        mock_result_set._final_exception = None

        callbacks = {"callback": None, "errback": None}
        page_count = 0

        def add_callbacks(callback=None, errback=None):
            nonlocal page_count
            callbacks["callback"] = callback
            callbacks["errback"] = errback

            # Simulate first page callback
            if callback and page_count == 0:
                page_count += 1

                def thread_callback():
                    callback([1, 2, 3])

                thread = threading.Thread(target=thread_callback)
                thread.start()

        def start_fetching():
            # Simulate error on next page
            if callbacks["errback"]:

                def thread_errback():
                    callbacks["errback"](InvalidRequest("Query error"))

                thread = threading.Thread(target=thread_errback)
                thread.start()

        mock_result_set.add_callbacks = add_callbacks
        mock_result_set.start_fetching_next_page = start_fetching

        streaming_result = AsyncStreamingResultSet(mock_result_set)

        results = []
        with pytest.raises(InvalidRequest, match="Query error"):
            async for row in streaming_result:
                results.append(row)

        # Should have gotten first page results before error
        assert results == [1, 2, 3]

    @pytest.mark.features
    async def test_streaming_cancellation(self):
        """Test cancelling streaming iteration."""
        mock_result_set = Mock()
        mock_result_set.has_more_pages = True
        mock_result_set._final_exception = None

        def add_callbacks(callback=None, errback=None):
            if callback:

                def thread_callback():
                    callback(list(range(100)))

                thread = threading.Thread(target=thread_callback)
                thread.start()

        mock_result_set.add_callbacks = add_callbacks

        streaming_result = AsyncStreamingResultSet(mock_result_set)

        results = []
        async for row in streaming_result:
            results.append(row)
            if len(results) >= 10:
                break  # Cancel iteration

        assert len(results) == 10
        assert results == list(range(10))

    @pytest.mark.features
    def test_create_streaming_statement(self):
        """Test creating streaming statements."""
        # Basic statement
        stmt = create_streaming_statement("SELECT * FROM users")
        assert stmt.query_string == "SELECT * FROM users"
        assert stmt.fetch_size == 1000

        # Custom fetch size
        stmt = create_streaming_statement("SELECT * FROM users", fetch_size=100)
        assert stmt.fetch_size == 100

        # With consistency level
        stmt = create_streaming_statement(
            "SELECT * FROM users", consistency_level=ConsistencyLevel.QUORUM
        )
        assert stmt.consistency_level == ConsistencyLevel.QUORUM


class TestStreamingMemoryManagement:
    """Test memory management in streaming operations."""

    @pytest.mark.features
    @pytest.mark.critical
    async def test_memory_cleanup_on_error(self):
        """Test that memory is properly cleaned up on errors."""
        mock_result_set = Mock()
        mock_result_set.has_more_pages = True
        mock_result_set._final_exception = None

        # Set up to have data in current page
        def add_callbacks(callback=None, errback=None):
            # Don't call callback immediately to test manual error handling
            pass

        mock_result_set.add_callbacks = add_callbacks

        streaming_result = AsyncStreamingResultSet(mock_result_set)

        # Manually set some data
        streaming_result._current_page = [1, 2, 3]

        # Simulate error
        error = InvalidRequest("Test error")
        streaming_result._handle_error(error)

        # _current_page should be cleared
        assert streaming_result._current_page == []
        assert streaming_result._error == error

    @pytest.mark.features
    async def test_no_memory_leak_on_multiple_errors(self):
        """Test that multiple errors don't cause memory accumulation."""
        mock_result_set = Mock()
        mock_result_set.has_more_pages = True
        mock_result_set._final_exception = None

        def add_callbacks(callback=None, errback=None):
            # Don't call callbacks
            pass

        mock_result_set.add_callbacks = add_callbacks

        streaming_result = AsyncStreamingResultSet(mock_result_set)

        # Simulate multiple errors
        for i in range(10):
            streaming_result._current_page = list(range(1000))  # Simulate page load
            streaming_result._handle_error(Exception(f"Error {i}"))

            # Should be cleared each time
            assert streaming_result._current_page == []

    @pytest.mark.features
    @pytest.mark.critical
    async def test_async_context_manager_cleanup(self):
        """Test cleanup when using async context manager."""
        mock_result_set = Mock()
        mock_result_set.has_more_pages = True
        mock_result_set._final_exception = None

        def add_callbacks(callback=None, errback=None):
            if callback:

                def thread_callback():
                    callback([1, 2, 3])

                thread = threading.Thread(target=thread_callback)
                thread.start()

        mock_result_set.add_callbacks = add_callbacks

        async with AsyncStreamingResultSet(mock_result_set) as stream:
            results = []
            async for row in stream:
                results.append(row)
                if len(results) >= 2:
                    break

        # Should have cleaned up
        assert stream._closed
        assert stream._current_page == []

    @pytest.mark.features
    async def test_close_clears_callbacks(self):
        """Test that close() clears all callbacks and resources."""
        mock_result_set = Mock()
        mock_result_set.has_more_pages = True
        mock_result_set._final_exception = None
        mock_result_set.clear_callbacks = Mock()

        def add_callbacks(callback=None, errback=None):
            # Don't call callbacks
            pass

        mock_result_set.add_callbacks = add_callbacks

        streaming_result = AsyncStreamingResultSet(mock_result_set)

        # Set some data
        streaming_result._current_page = [1, 2, 3]

        # Close should clear everything
        await streaming_result.close()

        assert streaming_result._closed
        assert streaming_result._current_page == []
        # clear_callbacks is called internally if available

    @pytest.mark.features
    async def test_exception_in_iteration_cleans_up(self):
        """Test cleanup when exception occurs during iteration."""
        mock_result_set = Mock()
        mock_result_set.has_more_pages = False
        mock_result_set._final_exception = None

        def add_callbacks(callback=None, errback=None):
            if callback:

                def thread_callback():
                    callback([1, 2, 3])

                thread = threading.Thread(target=thread_callback)
                thread.start()

        mock_result_set.add_callbacks = add_callbacks

        streaming_result = AsyncStreamingResultSet(mock_result_set)

        # Force exception during iteration
        async def bad_iteration():
            async for row in streaming_result:
                if row == 2:
                    raise ValueError("Test error")

        with pytest.raises(ValueError, match="Test error"):
            async with streaming_result:
                await bad_iteration()

        # Should still be cleaned up
        assert streaming_result._closed

    @pytest.mark.features
    async def test_page_limit_enforcement(self):
        """Test that page limits are enforced."""
        mock_result_set = Mock()
        mock_result_set.has_more_pages = True
        mock_result_set._final_exception = None

        callbacks = {"callback": None}
        fetch_count = 0

        def add_callbacks(callback=None, errback=None):
            callbacks["callback"] = callback
            # First page
            if callback:

                def thread_callback():
                    callback([1, 2, 3])

                thread = threading.Thread(target=thread_callback)
                thread.start()

        def start_fetching():
            nonlocal fetch_count
            fetch_count += 1
            if fetch_count < 5:
                mock_result_set.has_more_pages = True
                if callbacks["callback"]:

                    def thread_callback():
                        callbacks["callback"](
                            [fetch_count * 3 + 1, fetch_count * 3 + 2, fetch_count * 3 + 3]
                        )

                    thread = threading.Thread(target=thread_callback)
                    thread.start()
            else:
                mock_result_set.has_more_pages = False

        mock_result_set.add_callbacks = add_callbacks
        mock_result_set.start_fetching_next_page = start_fetching

        # Limit to 3 pages
        streaming_result = AsyncStreamingResultSet(mock_result_set, StreamConfig(max_pages=3))

        all_results = []
        async for row in streaming_result:
            all_results.append(row)

        # Should only get 3 pages worth of data
        assert len(all_results) == 9  # 3 pages * 3 rows each

    @pytest.mark.features
    async def test_weakref_cleanup(self):
        """Test that streaming results can be garbage collected."""
        mock_result_set = Mock()
        mock_result_set.has_more_pages = False
        mock_result_set._final_exception = None

        def add_callbacks(callback=None, errback=None):
            if callback:

                def thread_callback():
                    callback([1, 2, 3])

                thread = threading.Thread(target=thread_callback)
                thread.start()

        mock_result_set.add_callbacks = add_callbacks

        streaming_result = AsyncStreamingResultSet(mock_result_set)
        weak_ref = weakref.ref(streaming_result)

        # Use it briefly
        async for _ in streaming_result:
            break

        # Delete reference
        del streaming_result

        # Force garbage collection
        gc.collect()

        # Should be collected
        assert weak_ref() is None


class TestStreamingResultHandler:
    """Test StreamingResultHandler functionality."""

    @pytest.mark.features
    @pytest.mark.quick
    async def test_get_streaming_result(self):
        """Test getting streaming result through handler."""
        mock_future = Mock()
        handler = StreamingResultHandler(mock_future)

        # StreamingResultHandler works differently - it returns a streaming result set directly
        result = await handler.get_streaming_result()
        assert isinstance(result, AsyncStreamingResultSet)
        assert result.response_future == mock_future

    @pytest.mark.features
    async def test_streaming_handler_error(self):
        """Test error handling in streaming handler."""
        mock_future = Mock()
        mock_future.has_more_pages = False
        mock_future._final_exception = None

        error = InvalidRequest("Streaming error")

        def add_callbacks(callback=None, errback=None):
            if errback:
                # Call the errback after a short delay to let the async iteration start
                def thread_errback():
                    import time

                    time.sleep(0.01)  # Small delay to ensure iteration starts
                    errback(error)

                thread = threading.Thread(target=thread_errback)
                thread.start()

        mock_future.add_callbacks = add_callbacks

        handler = StreamingResultHandler(mock_future)
        result = await handler.get_streaming_result()

        # Set up the event loop and page_ready event
        result._loop = asyncio.get_running_loop()
        result._page_ready = asyncio.Event()

        with pytest.raises(InvalidRequest, match="Streaming error"):
            async for row in result:
                pass
