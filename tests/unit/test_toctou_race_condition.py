"""
Unit tests for TOCTOU (Time-of-Check-Time-of-Use) race condition in AsyncCloseable.

TOCTOU Race Conditions Explained:
=================================
A TOCTOU race condition occurs when there's a gap between checking a condition
(Time-of-Check) and using that information (Time-of-Use). In our context:

1. Thread A checks if session is closed (is_closed == False)
2. Thread B closes the session
3. Thread A tries to execute query on now-closed session
4. Result: Unexpected errors or undefined behavior

These tests verify that our AsyncCassandraSession properly handles these race
conditions by ensuring atomicity between the check and the operation.

Key Concepts:
- Atomicity: The check and operation must be indivisible
- Thread Safety: Operations must be safe when called concurrently
- Deterministic Behavior: Same conditions should produce same results
- Proper Error Handling: Errors should be predictable (ConnectionError)
"""

import asyncio
from unittest.mock import Mock

import pytest

from async_cassandra.exceptions import ConnectionError
from async_cassandra.session import AsyncCassandraSession


@pytest.mark.asyncio
class TestTOCTOURaceCondition:
    """
    Test TOCTOU race condition in is_closed checks.

    These tests simulate concurrent operations to verify that our session
    implementation properly handles race conditions between checking if
    the session is closed and performing operations.

    The tests use asyncio.create_task() and asyncio.gather() to simulate
    true concurrent execution where operations can interleave at any point.
    """

    async def test_concurrent_close_and_execute(self):
        """
        Test race condition between close() and execute().

        Scenario:
        ---------
        1. Two coroutines run concurrently:
           - One tries to execute a query
           - One tries to close the session
        2. The race occurs when:
           - Execute checks is_closed (returns False)
           - Close() sets is_closed to True and shuts down
           - Execute tries to proceed with a closed session

        Expected Behavior:
        -----------------
        With proper atomicity:
        - If execute starts first: Query completes successfully
        - If close completes first: Execute fails with ConnectionError
        - No other errors should occur (no race condition errors)

        Implementation Details:
        ----------------------
        - Uses asyncio.sleep(0.001) to increase chance of race
        - Manually triggers callbacks to simulate driver responses
        - Tracks whether a race condition was detected
        """
        # Create session
        mock_session = Mock()
        mock_response_future = Mock()
        mock_response_future.has_more_pages = False
        mock_response_future.add_callbacks = Mock()
        mock_response_future.timeout = None
        mock_session.execute_async = Mock(return_value=mock_response_future)
        mock_session.shutdown = Mock()  # Add shutdown mock
        async_session = AsyncCassandraSession(mock_session)

        # Track if race condition occurred
        race_detected = False
        execute_error = None

        async def close_session():
            """Close session after a small delay."""
            # Small delay to increase chance of race condition
            await asyncio.sleep(0.001)
            await async_session.close()

        async def execute_query():
            """Execute query that might race with close."""
            nonlocal race_detected, execute_error
            try:
                # Start execute task
                task = asyncio.create_task(async_session.execute("SELECT * FROM test"))

                # Trigger the callback to simulate driver response
                await asyncio.sleep(0)  # Yield to let execute start
                if mock_response_future.add_callbacks.called:
                    # Extract the callback function from the mock call
                    args = mock_response_future.add_callbacks.call_args
                    callback = args[1]["callback"]
                    # Simulate successful query response
                    callback(["row1"])

                # Wait for result
                await task
            except ConnectionError as e:
                execute_error = e
            except Exception as e:
                # If we get here, the race condition allowed execution
                # after is_closed check passed but before actual execution
                race_detected = True
                execute_error = e

        # Run both concurrently
        close_task = asyncio.create_task(close_session())
        execute_task = asyncio.create_task(execute_query())

        await asyncio.gather(close_task, execute_task, return_exceptions=True)

        # With atomic operations, the behavior is deterministic:
        # - If execute starts before close, it will complete successfully
        # - If close completes before execute starts, we get ConnectionError
        # No other errors should occur (no race conditions)
        if execute_error is not None:
            # If there was an error, it should only be ConnectionError
            assert isinstance(execute_error, ConnectionError)
            # No race condition detected
            assert not race_detected
        else:
            # Execute succeeded - this is valid if it started before close
            assert not race_detected

    async def test_multiple_concurrent_operations_during_close(self):
        """
        Test multiple operations racing with close.

        Scenario:
        ---------
        This test simulates a real-world scenario where multiple different
        operations (execute, prepare, execute_stream) are running concurrently
        when a close() is initiated. This tests the atomicity of ALL operations,
        not just execute.

        Race Conditions Being Tested:
        ----------------------------
        1. Execute query vs close
        2. Prepare statement vs close
        3. Execute stream vs close
        All happening simultaneously!

        Expected Behavior:
        -----------------
        Each operation should either:
        - Complete successfully (if it started before close)
        - Fail with ConnectionError (if close completed first)

        There should be NO mixed states or unexpected errors due to races.

        Implementation Details:
        ----------------------
        - Creates separate mock futures for each operation type
        - Tracks which operations succeed vs fail
        - Verifies all failures are ConnectionError (not race errors)
        - Uses operation_count to return different futures for different calls
        """
        # Create session
        mock_session = Mock()

        # Create separate mock futures for each operation
        execute_future = Mock()
        execute_future.has_more_pages = False
        execute_future.timeout = None
        execute_callbacks = []
        execute_future.add_callbacks = Mock(
            side_effect=lambda callback=None, errback=None: execute_callbacks.append(
                (callback, errback)
            )
        )

        prepare_future = Mock()
        prepare_future.timeout = None

        stream_future = Mock()
        stream_future.has_more_pages = False
        stream_future.timeout = None
        stream_callbacks = []
        stream_future.add_callbacks = Mock(
            side_effect=lambda callback=None, errback=None: stream_callbacks.append(
                (callback, errback)
            )
        )

        # Track which operation is being called
        operation_count = 0

        def mock_execute_async(*args, **kwargs):
            nonlocal operation_count
            operation_count += 1
            if operation_count == 1:
                return execute_future
            elif operation_count == 2:
                return stream_future
            else:
                return execute_future

        mock_session.execute_async = Mock(side_effect=mock_execute_async)
        mock_session.prepare = Mock(return_value=prepare_future)
        mock_session.shutdown = Mock()
        async_session = AsyncCassandraSession(mock_session)

        results = {"execute": None, "prepare": None, "execute_stream": None}
        errors = {"execute": None, "prepare": None, "execute_stream": None}

        async def close_session():
            """Close session after small delay."""
            await asyncio.sleep(0.001)
            await async_session.close()

        async def run_operations():
            """Run multiple operations that might race."""
            # Create tasks for each operation
            tasks = []

            # Execute
            async def run_execute():
                try:
                    result_task = asyncio.create_task(async_session.execute("SELECT 1"))
                    # Let the operation start
                    await asyncio.sleep(0)
                    # Trigger callback if registered
                    if execute_callbacks:
                        callback, _ = execute_callbacks[0]
                        if callback:
                            callback(["row1"])
                    await result_task
                    results["execute"] = "success"
                except Exception as e:
                    errors["execute"] = e

            tasks.append(run_execute())

            # Prepare
            async def run_prepare():
                try:
                    await async_session.prepare("SELECT ?")
                    results["prepare"] = "success"
                except Exception as e:
                    errors["prepare"] = e

            tasks.append(run_prepare())

            # Execute stream
            async def run_stream():
                try:
                    result_task = asyncio.create_task(async_session.execute_stream("SELECT 2"))
                    # Let the operation start
                    await asyncio.sleep(0)
                    # Trigger callback if registered
                    if stream_callbacks:
                        callback, _ = stream_callbacks[0]
                        if callback:
                            callback(["row2"])
                    await result_task
                    results["execute_stream"] = "success"
                except Exception as e:
                    errors["execute_stream"] = e

            tasks.append(run_stream())

            # Run all operations concurrently
            await asyncio.gather(*tasks, return_exceptions=True)

        # Run concurrently
        await asyncio.gather(close_session(), run_operations(), return_exceptions=True)

        # All operations should either succeed or fail with ConnectionError
        # Not a mix of behaviors due to race conditions
        for op_name in ["execute", "prepare", "execute_stream"]:
            if errors[op_name] is not None:
                # This assertion will fail until race condition is fixed
                assert isinstance(
                    errors[op_name], ConnectionError
                ), f"{op_name} failed with {type(errors[op_name])} instead of ConnectionError"

    async def test_execute_after_close(self):
        """
        Test that execute after close always fails with ConnectionError.

        This is the baseline test - no race condition here.

        Scenario:
        ---------
        1. Close the session completely
        2. Try to execute a query

        Expected:
        ---------
        Should ALWAYS fail with ConnectionError and proper error message.
        This tests the non-race condition case to ensure basic behavior works.
        """
        # Create session
        mock_session = Mock()
        mock_session.shutdown = Mock()
        async_session = AsyncCassandraSession(mock_session)

        # Close the session
        await async_session.close()

        # Try to execute - should always fail with ConnectionError
        with pytest.raises(ConnectionError, match="Session is closed"):
            await async_session.execute("SELECT 1")

    async def test_is_closed_check_atomicity(self):
        """
        Test that is_closed check and operation are atomic.

        This is the most complex test - it specifically targets the moment
        between checking is_closed and starting the operation.

        Scenario:
        ---------
        1. Thread A: Checks is_closed (returns False)
        2. Thread B: Waits for check to complete, then closes session
        3. Thread A: Tries to execute based on the is_closed check

        The Race Window:
        ---------------
        In broken code:
        - is_closed check passes (False)
        - close() happens before execute starts
        - execute proceeds anyway → undefined behavior

        With Proper Atomicity:
        --------------------
        The is_closed check and operation start must be atomic:
        - Either both happen before close (success)
        - Or both happen after close (ConnectionError)
        - Never a mix!

        Implementation Details:
        ----------------------
        - check_passed flag: Signals when is_closed returned False
        - close_after_check: Waits for flag, then closes
        - Tracks all state transitions to verify atomicity
        """
        # Create session
        mock_session = Mock()

        check_passed = False
        operation_started = False
        close_called = False
        execute_callbacks = []

        # Create a mock future that tracks callbacks
        mock_response_future = Mock()
        mock_response_future.has_more_pages = False
        mock_response_future.timeout = None
        mock_response_future.add_callbacks = Mock(
            side_effect=lambda callback=None, errback=None: execute_callbacks.append(
                (callback, errback)
            )
        )

        # Track when execute_async is called to detect the exact race timing
        def tracked_execute(*args, **kwargs):
            nonlocal operation_started
            operation_started = True
            # Return the mock future - this simulates the driver's async operation
            return mock_response_future

        mock_session.execute_async = Mock(side_effect=tracked_execute)
        mock_session.shutdown = Mock()
        async_session = AsyncCassandraSession(mock_session)

        execute_task = None
        execute_error = None

        async def execute_with_check():
            nonlocal check_passed, execute_task, execute_error
            try:
                # The is_closed check happens inside execute()
                if not async_session.is_closed:
                    check_passed = True
                    # Start the execute operation
                    execute_task = asyncio.create_task(async_session.execute("SELECT 1"))
                    # Let it start
                    await asyncio.sleep(0)
                    # Trigger callback if registered
                    if execute_callbacks:
                        callback, _ = execute_callbacks[0]
                        if callback:
                            callback(["row1"])
                    # Wait for completion
                    await execute_task
            except Exception as e:
                execute_error = e

        async def close_after_check():
            nonlocal close_called
            # Wait for is_closed check to pass (returns False)
            for _ in range(100):  # Max 100 iterations
                if check_passed:
                    break
                await asyncio.sleep(0.001)
            # Now close while execute might be in progress
            # This is the critical moment - we're closing right after
            # the is_closed check but possibly before execute starts
            close_called = True
            await async_session.close()

        # Run both concurrently
        await asyncio.gather(execute_with_check(), close_after_check(), return_exceptions=True)

        # Check results
        assert check_passed
        assert close_called

        # With proper atomicity in the fixed implementation:
        # Either the operation completes successfully (if it started before close)
        # Or it fails with ConnectionError (if close happened first)
        if execute_error is not None:
            assert isinstance(execute_error, ConnectionError)

    async def test_close_close_race(self):
        """
        Test concurrent close() calls.

        Scenario:
        ---------
        Multiple threads/coroutines all try to close the session at once.
        This can happen in cleanup scenarios where multiple error handlers
        or finalizers might try to ensure the session is closed.

        Expected Behavior:
        -----------------
        - Only ONE actual close/shutdown should occur
        - All close() calls should complete successfully
        - No errors or exceptions
        - is_closed should be True after all complete

        Why This Matters:
        ----------------
        Without proper locking:
        - Multiple threads might call shutdown()
        - Could lead to errors or resource leaks
        - State might become inconsistent

        Implementation:
        --------------
        - Wraps shutdown() to count actual calls
        - Runs 5 concurrent close() operations
        - Verifies shutdown() called exactly once
        """
        # Create session
        mock_session = Mock()
        mock_session.shutdown = Mock()
        async_session = AsyncCassandraSession(mock_session)

        close_count = 0
        original_shutdown = async_session._session.shutdown

        def count_closes():
            nonlocal close_count
            close_count += 1
            return original_shutdown()

        async_session._session.shutdown = count_closes

        # Multiple concurrent closes
        tasks = [async_session.close() for _ in range(5)]
        await asyncio.gather(*tasks)

        # Should only close once despite concurrent calls
        # This test should pass as the lock prevents multiple closes
        assert close_count == 1
        assert async_session.is_closed
