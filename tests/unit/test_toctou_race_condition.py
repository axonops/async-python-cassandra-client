"""
Unit tests for TOCTOU (Time-of-Check-Time-of-Use) race condition in AsyncCloseable.
"""

import asyncio
from unittest.mock import Mock

import pytest

from async_cassandra.exceptions import ConnectionError
from async_cassandra.session import AsyncCassandraSession


@pytest.mark.asyncio
class TestTOCTOURaceCondition:
    """Test TOCTOU race condition in is_closed checks."""

    async def test_concurrent_close_and_execute(self):
        """Test race condition between close() and execute()."""
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
            await asyncio.sleep(0.001)
            await async_session.close()

        async def execute_query():
            """Execute query that might race with close."""
            nonlocal race_detected, execute_error
            try:
                # Start execute task
                task = asyncio.create_task(async_session.execute("SELECT * FROM test"))

                # Trigger the callback
                await asyncio.sleep(0)  # Yield to let execute start
                if mock_response_future.add_callbacks.called:
                    args = mock_response_future.add_callbacks.call_args
                    callback = args[1]["callback"]
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
        """Test multiple operations racing with close."""
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
        """Test that execute after close always fails with ConnectionError."""
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
        """Test that is_closed check and operation are atomic."""
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

        # Track when execute_async is called
        def tracked_execute(*args, **kwargs):
            nonlocal operation_started
            operation_started = True
            # Return the mock future
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
            # Wait for check to pass
            for _ in range(100):  # Max 100 iterations
                if check_passed:
                    break
                await asyncio.sleep(0.001)
            # Now close while execute might be running
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
        """Test concurrent close() calls."""
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
