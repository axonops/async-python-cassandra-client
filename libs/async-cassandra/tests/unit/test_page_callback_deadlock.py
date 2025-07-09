"""
Unit tests for page callback execution outside lock.

This module tests a critical deadlock prevention mechanism in streaming
results. Page callbacks must be executed outside the internal lock to
prevent deadlocks when callbacks try to interact with the result set.

Test Organization:
==================
- Lock behavior during callbacks
- Error isolation in callbacks
- Performance with slow callbacks
- Callback data accuracy

Key Testing Principles:
======================
- Callbacks must not hold internal locks
- Callback errors must not affect streaming
- Slow callbacks must not block iteration
- Callbacks are optional (no overhead when unused)
"""

import threading
import time
from unittest.mock import Mock

import pytest

from async_cassandra.streaming import AsyncStreamingResultSet, StreamConfig


@pytest.mark.asyncio
class TestPageCallbackDeadlock:
    """Test that page callbacks are executed outside the lock to prevent deadlocks."""

    async def test_page_callback_executed_outside_lock(self):
        """
        Test that page callback is called outside the lock.

        What this tests:
        ---------------
        1. Page callback runs without holding _lock
        2. Lock is released before callback execution
        3. Callback can acquire lock if needed
        4. No deadlock risk from callbacks

        Why this matters:
        ----------------
        Previous implementations held the lock during callbacks,
        which caused deadlocks when:
        - Callbacks tried to iterate the result set
        - Callbacks called methods that needed the lock
        - Multiple threads were involved

        This test ensures callbacks run in a "clean" context
        without holding internal locks, preventing deadlocks.
        """
        # Track if callback was called while lock was held
        lock_held_during_callback = None
        callback_called = threading.Event()

        # Create a custom callback that checks lock status
        def page_callback(page_num, row_count):
            nonlocal lock_held_during_callback
            # Try to acquire the lock - if we can't, it's held by _handle_page
            lock_held_during_callback = not result_set._lock.acquire(blocking=False)
            if not lock_held_during_callback:
                result_set._lock.release()
            callback_called.set()

        # Create streaming result set with callback
        response_future = Mock()
        response_future.has_more_pages = False
        response_future._final_exception = None
        response_future.add_callbacks = Mock()

        config = StreamConfig(page_callback=page_callback)
        result_set = AsyncStreamingResultSet(response_future, config)

        # Trigger page callback
        args = response_future.add_callbacks.call_args
        page_handler = args[1]["callback"]
        page_handler(["row1", "row2", "row3"])

        # Wait for callback
        assert callback_called.wait(timeout=2.0)

        # Callback should have been called outside the lock
        assert lock_held_during_callback is False

    async def test_callback_error_does_not_affect_streaming(self):
        """
        Test that callback errors don't affect streaming functionality.

        What this tests:
        ---------------
        1. Callback exceptions are caught and isolated
        2. Streaming continues normally after callback error
        3. All rows are still accessible
        4. No corruption of internal state

        Why this matters:
        ----------------
        User callbacks might have bugs or throw exceptions.
        These errors should not:
        - Crash the streaming process
        - Lose data or skip rows
        - Corrupt the result set state

        This ensures robustness against user code errors.
        """

        # Create a callback that raises an error
        def bad_callback(page_num, row_count):
            raise ValueError("Callback error")

        # Create streaming result set
        response_future = Mock()
        response_future.has_more_pages = False
        response_future._final_exception = None
        response_future.add_callbacks = Mock()

        config = StreamConfig(page_callback=bad_callback)
        result_set = AsyncStreamingResultSet(response_future, config)

        # Trigger page with bad callback from a thread
        args = response_future.add_callbacks.call_args
        page_handler = args[1]["callback"]

        def thread_callback():
            page_handler(["row1", "row2"])

        thread = threading.Thread(target=thread_callback)
        thread.start()

        # Should still be able to iterate results despite callback error
        rows = []
        async for row in result_set:
            rows.append(row)

        assert len(rows) == 2
        assert rows == ["row1", "row2"]

    async def test_slow_callback_does_not_block_iteration(self):
        """
        Test that slow callbacks don't block result iteration.

        What this tests:
        ---------------
        1. Slow callbacks run asynchronously
        2. Row iteration proceeds without waiting
        3. Callback duration doesn't affect iteration speed
        4. No performance impact from slow callbacks

        Why this matters:
        ----------------
        Page callbacks might do expensive operations:
        - Write to databases
        - Send network requests
        - Perform complex calculations

        These slow operations should not block the main
        iteration thread. Users can process rows immediately
        while callbacks run in the background.
        """
        callback_times = []
        iteration_start_time = None

        # Create a slow callback
        def slow_callback(page_num, row_count):
            callback_times.append(time.time())
            time.sleep(0.5)  # Simulate slow callback

        # Create streaming result set
        response_future = Mock()
        response_future.has_more_pages = False
        response_future._final_exception = None
        response_future.add_callbacks = Mock()

        config = StreamConfig(page_callback=slow_callback)
        result_set = AsyncStreamingResultSet(response_future, config)

        # Trigger page from a thread
        args = response_future.add_callbacks.call_args
        page_handler = args[1]["callback"]

        def thread_callback():
            page_handler(["row1", "row2"])

        thread = threading.Thread(target=thread_callback)
        thread.start()

        # Start iteration immediately
        iteration_start_time = time.time()
        rows = []
        async for row in result_set:
            rows.append(row)
        iteration_end_time = time.time()

        # Iteration should complete quickly, not waiting for callback
        iteration_duration = iteration_end_time - iteration_start_time
        assert iteration_duration < 0.2  # Much less than callback duration

        # Results should be available
        assert len(rows) == 2

        # Wait for thread to complete to avoid event loop closed warning
        thread.join(timeout=1.0)

    async def test_callback_receives_correct_page_info(self):
        """
        Test that callbacks receive correct page information.

        What this tests:
        ---------------
        1. Page numbers increment correctly (1, 2, 3...)
        2. Row counts match actual page sizes
        3. Multiple pages tracked accurately
        4. Last page handled correctly

        Why this matters:
        ----------------
        Callbacks often need to:
        - Track progress through large result sets
        - Update progress bars or metrics
        - Log page processing statistics
        - Detect when processing is complete

        Accurate page information enables these use cases.
        """
        page_infos = []

        def track_pages(page_num, row_count):
            page_infos.append((page_num, row_count))

        # Create streaming result set
        response_future = Mock()
        response_future.has_more_pages = True
        response_future._final_exception = None
        response_future.add_callbacks = Mock()
        response_future.start_fetching_next_page = Mock()

        config = StreamConfig(page_callback=track_pages)
        AsyncStreamingResultSet(response_future, config)

        # Get page handler
        args = response_future.add_callbacks.call_args
        page_handler = args[1]["callback"]

        # Simulate multiple pages
        page_handler(["row1", "row2"])
        page_handler(["row3", "row4", "row5"])
        response_future.has_more_pages = False
        page_handler(["row6"])

        # Check callback data
        assert len(page_infos) == 3
        assert page_infos[0] == (1, 2)  # First page: 2 rows
        assert page_infos[1] == (2, 3)  # Second page: 3 rows
        assert page_infos[2] == (3, 1)  # Third page: 1 row

    async def test_no_callback_no_overhead(self):
        """
        Test that having no callback doesn't add overhead.

        What this tests:
        ---------------
        1. No performance penalty without callbacks
        2. Page handling is fast when no callback
        3. 1000 rows processed in <10ms
        4. Optional feature has zero cost when unused

        Why this matters:
        ----------------
        Most streaming operations don't use callbacks.
        The callback feature should have zero overhead
        when not used, following the principle:
        "You don't pay for what you don't use"

        This ensures the callback feature doesn't slow
        down the common case of simple iteration.
        """
        # Create streaming result set without callback
        response_future = Mock()
        response_future.has_more_pages = False
        response_future._final_exception = None
        response_future.add_callbacks = Mock()

        result_set = AsyncStreamingResultSet(response_future)

        # Trigger page from a thread
        args = response_future.add_callbacks.call_args
        page_handler = args[1]["callback"]

        rows = ["row" + str(i) for i in range(1000)]
        start_time = time.time()

        def thread_callback():
            page_handler(rows)

        thread = threading.Thread(target=thread_callback)
        thread.start()
        thread.join()  # Wait for thread to complete
        handle_time = time.time() - start_time

        # Should be very fast without callback
        assert handle_time < 0.01

        # Should still work normally
        count = 0
        async for row in result_set:
            count += 1

        assert count == 1000
