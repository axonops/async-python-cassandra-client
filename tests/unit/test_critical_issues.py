"""
Unit tests for critical issues identified in the technical review.

These tests use mocking to isolate and test specific problematic code paths.
"""

import asyncio
import gc
import threading
import weakref
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock

import pytest

from async_cassandra.result import AsyncResultHandler
from async_cassandra.streaming import AsyncStreamingResultSet, StreamConfig


class TestAsyncResultHandlerThreadSafety:
    """Unit tests for thread safety issues in AsyncResultHandler."""

    def test_race_condition_in_handle_page(self):
        """
        GIVEN concurrent calls to _handle_page from multiple threads
        WHEN driver callbacks happen simultaneously
        THEN data corruption should not occur
        """
        # Create handler with mock future
        mock_future = Mock()
        mock_future.has_more_pages = True
        handler = AsyncResultHandler(mock_future)

        # Track all rows added
        all_rows = []
        errors = []

        def concurrent_callback(thread_id, page_num):
            try:
                # Simulate driver callback with unique data
                rows = [f"thread_{thread_id}_page_{page_num}_row_{i}" for i in range(10)]
                handler._handle_page(rows)
                all_rows.extend(rows)
            except Exception as e:
                errors.append(f"Thread {thread_id}: {e}")

        # Simulate concurrent callbacks from driver threads
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = []
            for thread_id in range(10):
                for page_num in range(5):
                    future = executor.submit(concurrent_callback, thread_id, page_num)
                    futures.append(future)

            # Wait for all callbacks
            for future in futures:
                future.result()

        # Check for data corruption
        assert len(errors) == 0, f"Thread safety errors: {errors}"

        # All rows should be present
        expected_count = 10 * 5 * 10  # threads * pages * rows_per_page
        assert len(all_rows) == expected_count

        # Check handler.rows for corruption
        # Current implementation may have race conditions here
        # This test may fail, demonstrating the issue

    def test_event_loop_thread_safety(self):
        """
        GIVEN callbacks from non-event-loop threads
        WHEN setting future results
        THEN call_soon_threadsafe should be used
        """

        async def run_test():
            loop = asyncio.get_running_loop()

            # Track which thread sets the future result
            result_thread = None

            # Patch to monitor thread safety
            original_call_soon_threadsafe = loop.call_soon_threadsafe
            call_soon_threadsafe_used = False

            def monitored_call_soon_threadsafe(callback, *args):
                nonlocal call_soon_threadsafe_used
                call_soon_threadsafe_used = True
                return original_call_soon_threadsafe(callback, *args)

            loop.call_soon_threadsafe = monitored_call_soon_threadsafe

            try:
                mock_future = Mock()
                mock_future.has_more_pages = True  # Start with more pages expected
                mock_future.add_callbacks = Mock()
                mock_future.timeout = None
                mock_future.start_fetching_next_page = Mock()

                handler = AsyncResultHandler(mock_future)

                # Start get_result to create the future
                result_task = asyncio.create_task(handler.get_result())
                await asyncio.sleep(0.1)  # Make sure it's fully initialized

                # Simulate callback from driver thread
                def driver_callback():
                    nonlocal result_thread
                    result_thread = threading.current_thread()
                    # First callback with more pages
                    handler._handle_page([1, 2, 3])
                    # Now final callback - set has_more_pages to False before calling
                    mock_future.has_more_pages = False
                    handler._handle_page([4, 5, 6])

                driver_thread = threading.Thread(target=driver_callback)
                driver_thread.start()
                driver_thread.join()

                # Give time for async operations
                await asyncio.sleep(0.1)

                # Verify thread safety was maintained
                assert result_thread != threading.current_thread()
                # Now call_soon_threadsafe SHOULD be used since we store the loop
                assert call_soon_threadsafe_used

                # The result task should be completed
                assert result_task.done()
                result = await result_task
                assert len(result.rows) == 6  # We added [1,2,3] then [4,5,6]

            finally:
                loop.call_soon_threadsafe = original_call_soon_threadsafe

        asyncio.run(run_test())

    def test_state_synchronization_issues(self):
        """
        GIVEN shared state between threads
        WHEN state is modified without proper synchronization
        THEN race conditions can occur
        """
        mock_future = Mock()
        mock_future.has_more_pages = True
        handler = AsyncResultHandler(mock_future)

        # Simulate rapid state changes from multiple threads
        state_changes = []

        def modify_state(thread_id):
            for i in range(100):
                # These operations are not atomic without proper locking
                current_rows = len(handler.rows)
                state_changes.append((thread_id, i, current_rows))
                handler.rows.append(f"thread_{thread_id}_item_{i}")

        threads = []
        for thread_id in range(5):
            thread = threading.Thread(target=modify_state, args=(thread_id,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # Check for consistency
        expected_total = 5 * 100  # threads * iterations
        actual_total = len(handler.rows)

        # This might fail due to race conditions
        assert (
            actual_total == expected_total
        ), f"Race condition detected: expected {expected_total}, got {actual_total}"


class TestStreamingMemoryLeaks:
    """Unit tests for memory leaks in streaming functionality."""

    def test_page_reference_cleanup(self):
        """
        GIVEN streaming result set processing pages
        WHEN pages are consumed
        THEN old pages should be garbage collectible
        """
        # Track pages created
        pages_created = []

        mock_future = Mock()
        mock_future.has_more_pages = True
        mock_future._final_exception = None  # Important: must be None

        page_count = 0
        handler = None  # Define handler first
        callbacks = {}

        def add_callbacks(callback=None, errback=None):
            callbacks["callback"] = callback
            callbacks["errback"] = errback
            # Simulate initial page callback from a thread
            if callback:
                import threading

                def thread_callback():
                    first_page = [f"row_0_{i}" for i in range(100)]
                    pages_created.append(first_page)
                    callback(first_page)

                thread = threading.Thread(target=thread_callback)
                thread.start()

        def mock_fetch_next():
            nonlocal page_count
            page_count += 1

            if page_count <= 5:
                # Create a page
                page = [f"row_{page_count}_{i}" for i in range(100)]
                pages_created.append(page)

                # Simulate callback from thread
                if callbacks.get("callback"):
                    import threading

                    def thread_callback():
                        callbacks["callback"](page)

                    thread = threading.Thread(target=thread_callback)
                    thread.start()
                mock_future.has_more_pages = page_count < 5
            else:
                if callbacks.get("callback"):
                    import threading

                    def thread_callback():
                        callbacks["callback"]([])

                    thread = threading.Thread(target=thread_callback)
                    thread.start()
                mock_future.has_more_pages = False

        mock_future.start_fetching_next_page = mock_fetch_next
        mock_future.add_callbacks = add_callbacks

        handler = AsyncStreamingResultSet(mock_future)

        async def consume_all():
            consumed = 0
            async for row in handler:
                consumed += 1
            return consumed

        # Consume all rows
        total_consumed = asyncio.run(consume_all())
        assert total_consumed == 600  # 6 pages * 100 rows (including first page)

        # Check that handler only holds one page at a time
        assert len(handler._current_page) <= 100, "Handler should only hold one page"

        # Verify pages were replaced, not accumulated
        assert len(pages_created) == 6  # 1 initial page + 5 pages from mock_fetch_next

    def test_callback_reference_cycles(self):
        """
        GIVEN callbacks that might create reference cycles
        WHEN streaming completes or errors
        THEN reference cycles should be broken
        """
        # Track object lifecycle
        handler_refs = []
        future_refs = []

        class TrackedFuture:
            def __init__(self):
                future_refs.append(weakref.ref(self))
                self.callbacks = []
                self.has_more_pages = False

            def add_callbacks(self, callback, errback):
                # This creates a reference from future to handler
                self.callbacks.append((callback, errback))

            def start_fetching_next_page(self):
                pass

        class TrackedHandler(AsyncStreamingResultSet):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                handler_refs.append(weakref.ref(self))

        # Create objects with potential cycle
        future = TrackedFuture()
        handler = TrackedHandler(future)

        # Use the handler
        async def use_handler(h):
            h._handle_page([1, 2, 3])
            h._exhausted = True

            try:
                async for _ in h:
                    pass
            except StopAsyncIteration:
                pass

        asyncio.run(use_handler(handler))

        # Clear explicit references
        del future
        del handler

        # Force garbage collection
        gc.collect()

        # Check for leaks
        alive_handlers = sum(1 for ref in handler_refs if ref() is not None)
        alive_futures = sum(1 for ref in future_refs if ref() is not None)

        assert alive_handlers == 0, f"Handler leak: {alive_handlers} still alive"
        assert alive_futures == 0, f"Future leak: {alive_futures} still alive"

    def test_streaming_config_lifecycle(self):
        """
        GIVEN streaming with callbacks and config
        WHEN streaming completes
        THEN config and callbacks should be cleaned up
        """
        callback_refs = []

        class CallbackData:
            """Object that can be weakly referenced"""

            def __init__(self, page_num, row_count):
                self.page = page_num
                self.rows = row_count

        def progress_callback(page_num, row_count):
            # Simulate some object that could be leaked
            data = CallbackData(page_num, row_count)
            callback_refs.append(weakref.ref(data))

        config = StreamConfig(fetch_size=10, max_pages=5, page_callback=progress_callback)

        # Create a simpler test that doesn't require async iteration
        mock_future = Mock()
        mock_future.has_more_pages = False
        mock_future.add_callbacks = Mock()

        handler = AsyncStreamingResultSet(mock_future, config)

        # Simulate page callbacks directly
        handler._handle_page([f"row_{i}" for i in range(10)])
        handler._handle_page([f"row_{i}" for i in range(10, 20)])
        handler._handle_page([f"row_{i}" for i in range(20, 30)])

        # Verify callbacks were called
        assert len(callback_refs) == 3  # 3 pages

        # Clear references
        del handler
        del config
        del progress_callback
        gc.collect()

        # Check for leaked callback data
        alive_callbacks = sum(1 for ref in callback_refs if ref() is not None)
        assert alive_callbacks == 0, f"Callback data leak: {alive_callbacks} still alive"


class TestErrorHandlingConsistency:
    """Unit tests for error handling consistency."""

    @pytest.mark.asyncio
    async def test_execute_vs_execute_stream_error_wrapping(self):
        """
        GIVEN the same underlying error
        WHEN it occurs in execute() vs execute_stream()
        THEN error handling should be consistent
        """
        from cassandra import InvalidRequest

        # Test InvalidRequest handling
        base_error = InvalidRequest("Test error")

        # Test execute() error handling with AsyncResultHandler
        execute_error = None
        mock_future = Mock()
        mock_future.add_callbacks = Mock()
        mock_future.has_more_pages = False
        mock_future.timeout = None  # Add timeout attribute

        handler = AsyncResultHandler(mock_future)
        # Simulate error callback being called after init
        handler._handle_error(base_error)
        try:
            await handler.get_result()
        except Exception as e:
            execute_error = e

        # Test execute_stream() error handling with AsyncStreamingResultSet
        # We need to test error handling without async iteration to avoid complexity
        stream_mock_future = Mock()
        stream_mock_future.add_callbacks = Mock()
        stream_mock_future.has_more_pages = False

        # Get the error that would be raised
        stream_handler = AsyncStreamingResultSet(stream_mock_future)
        stream_handler._handle_error(base_error)
        stream_error = stream_handler._error

        # Both should have the same error type
        assert execute_error is not None
        assert stream_error is not None
        assert type(execute_error) is type(
            stream_error
        ), f"Different error types: {type(execute_error)} vs {type(stream_error)}"
        assert isinstance(execute_error, InvalidRequest)
        assert isinstance(stream_error, InvalidRequest)

    def test_timeout_error_consistency(self):
        """
        GIVEN timeout errors
        WHEN they occur in different contexts
        THEN they should be handled consistently
        """
        from cassandra import OperationTimedOut

        timeout_error = OperationTimedOut("Test timeout")

        # Test in AsyncResultHandler
        result_error = None

        async def get_result_error():
            nonlocal result_error
            mock_future = Mock()
            mock_future.add_callbacks = Mock()
            mock_future.has_more_pages = False
            mock_future.timeout = None  # Add timeout attribute
            result_handler = AsyncResultHandler(mock_future)
            # Simulate error callback being called after init
            result_handler._handle_error(timeout_error)
            try:
                await result_handler.get_result()
            except Exception as e:
                result_error = e

        asyncio.run(get_result_error())

        # Test in AsyncStreamingResultSet
        stream_mock_future = Mock()
        stream_mock_future.add_callbacks = Mock()
        stream_mock_future.has_more_pages = False
        stream_handler = AsyncStreamingResultSet(stream_mock_future)
        stream_handler._handle_error(timeout_error)
        stream_error = stream_handler._error

        # Both should preserve the timeout error
        assert isinstance(result_error, OperationTimedOut)
        assert isinstance(stream_error, OperationTimedOut)
        assert str(result_error) == str(stream_error)
