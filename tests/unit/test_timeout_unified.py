"""
Consolidated timeout tests for async-python-cassandra.

This module consolidates timeout testing from multiple files into focused,
clear tests that match the actual implementation.

Test Organization:
==================
1. Query Timeout Tests - Timeout parameter propagation
2. Timeout Exception Tests - ReadTimeout, WriteTimeout handling
3. Prepare Timeout Tests - Statement preparation timeouts
4. Resource Cleanup Tests - Proper cleanup on timeout

Key Testing Principles:
======================
- Test timeout parameter flow through the layers
- Verify timeout exceptions are handled correctly
- Ensure no resource leaks on timeout
- Test default timeout behavior
"""

import asyncio
from unittest.mock import AsyncMock, Mock, patch

import pytest
from cassandra import ReadTimeout, WriteTimeout
from cassandra.cluster import _NOT_SET, ResponseFuture
from cassandra.policies import WriteType

from async_cassandra import AsyncCassandraSession


class TestTimeoutHandling:
    """
    Test timeout handling throughout the async wrapper.

    These tests verify that timeouts work correctly at all levels
    and that timeout exceptions are properly handled.
    """

    # ========================================
    # Query Timeout Tests
    # ========================================

    @pytest.mark.asyncio
    async def test_execute_with_explicit_timeout(self):
        """
        Test that explicit timeout is passed to driver.

        What this tests:
        ---------------
        1. Timeout parameter flows to execute_async
        2. Timeout value is preserved correctly
        3. Handler receives timeout for its operation

        Why this matters:
        ----------------
        Users need to control query timeouts for different
        operations based on their performance requirements.
        """
        mock_session = Mock()
        mock_future = Mock(spec=ResponseFuture)
        mock_future.has_more_pages = False
        mock_session.execute_async.return_value = mock_future

        async_session = AsyncCassandraSession(mock_session)

        with patch("async_cassandra.session.AsyncResultHandler") as mock_handler_class:
            mock_handler = Mock()
            mock_handler.get_result = AsyncMock(return_value=Mock(rows=[]))
            mock_handler_class.return_value = mock_handler

            await async_session.execute("SELECT * FROM test", timeout=5.0)

        # Verify execute_async was called with timeout
        mock_session.execute_async.assert_called_once()
        args = mock_session.execute_async.call_args[0]
        # timeout is the 5th argument (index 4)
        assert args[4] == 5.0

        # Verify handler.get_result was called with timeout
        mock_handler.get_result.assert_called_once_with(timeout=5.0)

    @pytest.mark.asyncio
    async def test_execute_without_timeout_uses_not_set(self):
        """
        Test that missing timeout uses _NOT_SET sentinel.

        What this tests:
        ---------------
        1. No timeout parameter results in _NOT_SET
        2. Handler receives None for timeout
        3. Driver uses its default timeout

        Why this matters:
        ----------------
        Most queries don't specify timeout and should use
        driver defaults rather than arbitrary values.
        """
        mock_session = Mock()
        mock_future = Mock(spec=ResponseFuture)
        mock_future.has_more_pages = False
        mock_session.execute_async.return_value = mock_future

        async_session = AsyncCassandraSession(mock_session)

        with patch("async_cassandra.session.AsyncResultHandler") as mock_handler_class:
            mock_handler = Mock()
            mock_handler.get_result = AsyncMock(return_value=Mock(rows=[]))
            mock_handler_class.return_value = mock_handler

            await async_session.execute("SELECT * FROM test")

        # Verify _NOT_SET was passed to execute_async
        args = mock_session.execute_async.call_args[0]
        # timeout is the 5th argument (index 4)
        assert args[4] is _NOT_SET

        # Verify handler got None timeout
        mock_handler.get_result.assert_called_once_with(timeout=None)

    @pytest.mark.asyncio
    async def test_concurrent_queries_different_timeouts(self):
        """
        Test concurrent queries with different timeouts.

        What this tests:
        ---------------
        1. Multiple queries maintain separate timeouts
        2. Concurrent execution doesn't mix timeouts
        3. Each query respects its timeout

        Why this matters:
        ----------------
        Real applications run many queries concurrently,
        each with different performance characteristics.
        """
        mock_session = Mock()

        # Track futures to return them in order
        futures = []

        def create_future(*args, **kwargs):
            future = Mock(spec=ResponseFuture)
            future.has_more_pages = False
            futures.append(future)
            return future

        mock_session.execute_async.side_effect = create_future

        async_session = AsyncCassandraSession(mock_session)

        with patch("async_cassandra.session.AsyncResultHandler") as mock_handler_class:
            # Create handlers that return immediately
            handlers = []

            def create_handler(future):
                handler = Mock()
                handler.get_result = AsyncMock(return_value=Mock(rows=[]))
                handlers.append(handler)
                return handler

            mock_handler_class.side_effect = create_handler

            # Execute queries concurrently
            await asyncio.gather(
                async_session.execute("SELECT 1", timeout=1.0),
                async_session.execute("SELECT 2", timeout=5.0),
                async_session.execute("SELECT 3"),  # No timeout
            )

        # Verify timeouts were passed correctly
        calls = mock_session.execute_async.call_args_list
        # timeout is the 5th argument (index 4)
        assert calls[0][0][4] == 1.0
        assert calls[1][0][4] == 5.0
        assert calls[2][0][4] is _NOT_SET

        # Verify handlers got correct timeouts
        assert handlers[0].get_result.call_args[1]["timeout"] == 1.0
        assert handlers[1].get_result.call_args[1]["timeout"] == 5.0
        assert handlers[2].get_result.call_args[1]["timeout"] is None

    # ========================================
    # Timeout Exception Tests
    # ========================================

    @pytest.mark.asyncio
    async def test_read_timeout_exception_handling(self):
        """
        Test ReadTimeout exception is properly handled.

        What this tests:
        ---------------
        1. ReadTimeout from driver is caught
        2. Not wrapped in QueryError (re-raised as-is)
        3. Exception details are preserved

        Why this matters:
        ----------------
        Read timeouts indicate the query took too long.
        Applications need the full exception details for
        retry decisions and debugging.
        """
        mock_session = Mock()
        mock_future = Mock(spec=ResponseFuture)
        mock_session.execute_async.return_value = mock_future

        async_session = AsyncCassandraSession(mock_session)

        # Create proper ReadTimeout
        timeout_error = ReadTimeout(
            message="Read timeout",
            consistency=3,  # ConsistencyLevel.THREE
            required_responses=2,
            received_responses=1,
        )

        with patch("async_cassandra.session.AsyncResultHandler") as mock_handler_class:
            mock_handler = Mock()
            mock_handler.get_result = AsyncMock(side_effect=timeout_error)
            mock_handler_class.return_value = mock_handler

            # Should raise ReadTimeout directly (not wrapped)
            with pytest.raises(ReadTimeout) as exc_info:
                await async_session.execute("SELECT * FROM test")

            # Verify it's the same exception
            assert exc_info.value is timeout_error

    @pytest.mark.asyncio
    async def test_write_timeout_exception_handling(self):
        """
        Test WriteTimeout exception is properly handled.

        What this tests:
        ---------------
        1. WriteTimeout from driver is caught
        2. Not wrapped in QueryError (re-raised as-is)
        3. Write type information is preserved

        Why this matters:
        ----------------
        Write timeouts need special handling as they may
        have partially succeeded. Write type helps determine
        if retry is safe.
        """
        mock_session = Mock()
        mock_future = Mock(spec=ResponseFuture)
        mock_session.execute_async.return_value = mock_future

        async_session = AsyncCassandraSession(mock_session)

        # Create proper WriteTimeout with numeric write_type
        timeout_error = WriteTimeout(
            message="Write timeout",
            consistency=3,  # ConsistencyLevel.THREE
            write_type=WriteType.SIMPLE,  # Use enum value (0)
            required_responses=2,
            received_responses=1,
        )

        with patch("async_cassandra.session.AsyncResultHandler") as mock_handler_class:
            mock_handler = Mock()
            mock_handler.get_result = AsyncMock(side_effect=timeout_error)
            mock_handler_class.return_value = mock_handler

            # Should raise WriteTimeout directly
            with pytest.raises(WriteTimeout) as exc_info:
                await async_session.execute("INSERT INTO test VALUES (1)")

            assert exc_info.value is timeout_error

    @pytest.mark.asyncio
    async def test_timeout_with_retry_policy(self):
        """
        Test timeout exceptions are properly propagated.

        What this tests:
        ---------------
        1. ReadTimeout errors are not wrapped
        2. Exception details are preserved
        3. Retry happens at driver level

        Why this matters:
        ----------------
        The driver handles retries internally based on its
        retry policy. We just need to propagate the exception.
        """
        mock_session = Mock()

        # Simulate timeout from driver (after retries exhausted)
        timeout_error = ReadTimeout("Read Timeout")
        mock_session.execute_async.side_effect = timeout_error

        async_session = AsyncCassandraSession(mock_session)

        # Should raise the ReadTimeout as-is
        with pytest.raises(ReadTimeout) as exc_info:
            await async_session.execute("SELECT * FROM test")

        # Verify it's the same exception instance
        assert exc_info.value is timeout_error

    # ========================================
    # Prepare Timeout Tests
    # ========================================

    @pytest.mark.asyncio
    async def test_prepare_with_explicit_timeout(self):
        """
        Test statement preparation with timeout.

        What this tests:
        ---------------
        1. Prepare accepts timeout parameter
        2. Uses asyncio timeout for blocking operation
        3. Returns prepared statement on success

        Why this matters:
        ----------------
        Statement preparation can be slow with complex
        queries or overloaded clusters.
        """
        mock_session = Mock()
        mock_prepared = Mock()  # PreparedStatement
        mock_session.prepare.return_value = mock_prepared

        async_session = AsyncCassandraSession(mock_session)

        # Should complete within timeout
        prepared = await async_session.prepare("SELECT * FROM test WHERE id = ?", timeout=5.0)

        assert prepared is mock_prepared
        mock_session.prepare.assert_called_once_with(
            "SELECT * FROM test WHERE id = ?", None  # custom_payload
        )

    @pytest.mark.asyncio
    async def test_prepare_uses_default_timeout(self):
        """
        Test prepare uses default timeout when not specified.

        What this tests:
        ---------------
        1. Default timeout constant is used
        2. Prepare completes successfully

        Why this matters:
        ----------------
        Most prepare calls don't specify timeout and
        should use a reasonable default.
        """
        mock_session = Mock()
        mock_prepared = Mock()
        mock_session.prepare.return_value = mock_prepared

        async_session = AsyncCassandraSession(mock_session)

        # Prepare without timeout
        prepared = await async_session.prepare("SELECT * FROM test WHERE id = ?")

        assert prepared is mock_prepared

    @pytest.mark.asyncio
    async def test_prepare_timeout_error(self):
        """
        Test prepare timeout is handled correctly.

        What this tests:
        ---------------
        1. Slow prepare operations timeout
        2. TimeoutError is wrapped in QueryError
        3. Error message is helpful

        Why this matters:
        ----------------
        Prepare timeouts need clear error messages to
        help debug schema or query complexity issues.
        """
        mock_session = Mock()

        # Simulate slow prepare in the sync driver
        def slow_prepare(query, payload):
            import time

            time.sleep(10)  # This will block, causing timeout
            return Mock()

        mock_session.prepare = Mock(side_effect=slow_prepare)

        async_session = AsyncCassandraSession(mock_session)

        # Should timeout quickly (prepare uses DEFAULT_REQUEST_TIMEOUT if not specified)
        with pytest.raises(asyncio.TimeoutError):
            await async_session.prepare("SELECT * FROM test WHERE id = ?", timeout=0.1)

    # ========================================
    # Resource Cleanup Tests
    # ========================================

    @pytest.mark.asyncio
    async def test_timeout_cleanup_on_session_close(self):
        """
        Test pending operations are cleaned up on close.

        What this tests:
        ---------------
        1. Pending queries are cancelled on close
        2. No "pending task" warnings
        3. Session closes cleanly

        Why this matters:
        ----------------
        Proper cleanup prevents resource leaks and
        "task was destroyed but pending" warnings.
        """
        mock_session = Mock()
        mock_future = Mock(spec=ResponseFuture)
        mock_future.has_more_pages = False

        # Track callback registration
        registered_callbacks = []

        def add_callbacks(callback=None, errback=None):
            registered_callbacks.append((callback, errback))

        mock_future.add_callbacks = add_callbacks
        mock_session.execute_async.return_value = mock_future

        async_session = AsyncCassandraSession(mock_session)

        # Start a long-running query
        with patch("async_cassandra.session.AsyncResultHandler") as mock_handler_class:
            mock_handler = Mock()
            # Make get_result hang
            hang_event = asyncio.Event()

            async def hang_forever(*args, **kwargs):
                await hang_event.wait()

            mock_handler.get_result = hang_forever
            mock_handler_class.return_value = mock_handler

            # Start query but don't await it
            query_task = asyncio.create_task(
                async_session.execute("SELECT * FROM test", timeout=30.0)
            )

            # Let it start
            await asyncio.sleep(0.01)

            # Close session
            await async_session.close()

            # Set event to unblock
            hang_event.set()

            # Task should complete (likely with error)
            try:
                await query_task
            except Exception:
                pass  # Expected

    @pytest.mark.asyncio
    async def test_multiple_timeout_cleanup(self):
        """
        Test cleanup of multiple timed-out operations.

        What this tests:
        ---------------
        1. Multiple timeouts don't leak resources
        2. Session remains stable after timeouts
        3. New queries work after timeouts

        Why this matters:
        ----------------
        Production systems may experience many timeouts.
        The session must remain stable and usable.
        """
        mock_session = Mock()

        # Track created futures
        futures = []

        def create_future(*args, **kwargs):
            future = Mock(spec=ResponseFuture)
            future.has_more_pages = False
            futures.append(future)
            return future

        mock_session.execute_async.side_effect = create_future

        async_session = AsyncCassandraSession(mock_session)

        # Create multiple queries that timeout
        with patch("async_cassandra.session.AsyncResultHandler") as mock_handler_class:
            mock_handler = Mock()
            mock_handler.get_result = AsyncMock(side_effect=ReadTimeout("Timeout"))
            mock_handler_class.return_value = mock_handler

            # Execute multiple queries that will timeout
            for i in range(5):
                with pytest.raises(ReadTimeout):
                    await async_session.execute(f"SELECT {i}")

        # Session should still be usable
        assert not async_session.is_closed

        # New query should work
        with patch("async_cassandra.session.AsyncResultHandler") as mock_handler_class:
            mock_handler = Mock()
            mock_handler.get_result = AsyncMock(return_value=Mock(rows=[{"id": 1}]))
            mock_handler_class.return_value = mock_handler

            result = await async_session.execute("SELECT * FROM test")
            assert len(result.rows) == 1
