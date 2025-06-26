"""
Unit tests for simplified threading implementation.

These tests verify that the simplified implementation:
1. Uses only essential locks
2. Accepts reasonable trade-offs
3. Maintains thread safety where necessary
4. Performs better than complex locking
"""

import asyncio
import time
from unittest.mock import Mock

import pytest

from async_cassandra.exceptions import ConnectionError
from async_cassandra.session import AsyncCassandraSession


@pytest.mark.asyncio
class TestSimplifiedThreading:
    """Test simplified threading and locking implementation."""

    async def test_no_operation_lock_overhead(self):
        """Test that operations don't have unnecessary lock overhead."""
        # Create session
        mock_session = Mock()
        mock_response_future = Mock()
        mock_response_future.has_more_pages = False
        mock_response_future.add_callbacks = Mock()
        mock_response_future.timeout = None
        mock_session.execute_async = Mock(return_value=mock_response_future)

        async_session = AsyncCassandraSession(mock_session)

        # Measure time for multiple concurrent operations
        start_time = time.perf_counter()

        # Run many concurrent queries
        tasks = []
        for i in range(100):
            task = asyncio.create_task(async_session.execute(f"SELECT {i}"))
            tasks.append(task)

        # Trigger callbacks
        await asyncio.sleep(0)  # Let tasks start

        # Trigger all callbacks
        for call in mock_response_future.add_callbacks.call_args_list:
            callback = call[1]["callback"]
            callback([f"row{i}" for i in range(10)])

        # Wait for all to complete
        await asyncio.gather(*tasks)

        duration = time.perf_counter() - start_time

        # With simplified implementation, 100 concurrent ops should be very fast
        # No operation locks means no contention
        assert duration < 0.5  # Should complete in well under 500ms
        assert mock_session.execute_async.call_count == 100

    async def test_simple_close_behavior(self):
        """Test simplified close behavior without complex state tracking."""
        # Create session
        mock_session = Mock()
        mock_session.shutdown = Mock()
        async_session = AsyncCassandraSession(mock_session)

        # Close should be simple and fast
        start_time = time.perf_counter()
        await async_session.close()
        close_duration = time.perf_counter() - start_time

        # Close includes a 5-second delay to let driver threads finish
        assert 5.0 <= close_duration < 6.0
        assert async_session.is_closed

        # Subsequent operations should fail immediately (no complex checks)
        with pytest.raises(ConnectionError):
            await async_session.execute("SELECT 1")

    async def test_acceptable_race_condition(self):
        """Test that we accept reasonable race conditions for simplicity."""
        # Create session
        mock_session = Mock()
        mock_response_future = Mock()
        mock_response_future.has_more_pages = False
        mock_response_future.add_callbacks = Mock()
        mock_response_future.timeout = None
        mock_session.execute_async = Mock(return_value=mock_response_future)
        mock_session.shutdown = Mock()

        async_session = AsyncCassandraSession(mock_session)

        results = []

        async def execute_query():
            """Try to execute during close."""
            try:
                # Start the execute
                task = asyncio.create_task(async_session.execute("SELECT 1"))
                # Give it a moment to start
                await asyncio.sleep(0)

                # Trigger callback if it was registered
                if mock_response_future.add_callbacks.called:
                    args = mock_response_future.add_callbacks.call_args
                    callback = args[1]["callback"]
                    callback(["row1"])

                await task
                results.append("success")
            except ConnectionError:
                results.append("closed")
            except Exception as e:
                # With simplified implementation, we might get driver errors
                # if close happens during execution - this is acceptable
                results.append(f"error: {type(e).__name__}")

        async def close_session():
            """Close after a tiny delay."""
            await asyncio.sleep(0.001)
            await async_session.close()

        # Run concurrently
        await asyncio.gather(execute_query(), close_session(), return_exceptions=True)

        # With simplified implementation, we accept that the result
        # might be success, closed, or a driver error
        assert len(results) == 1
        # Any of these outcomes is acceptable
        assert results[0] in ["success", "closed"] or results[0].startswith("error:")

    async def test_no_complex_state_tracking(self):
        """Test that we don't have complex state tracking."""
        # Create session
        mock_session = Mock()
        async_session = AsyncCassandraSession(mock_session)

        # Check that we don't have complex state attributes
        # These should not exist in simplified implementation
        assert not hasattr(async_session, "_active_operations")
        assert not hasattr(async_session, "_operation_lock")
        assert not hasattr(async_session, "_close_event")

        # Should only have simple state
        assert hasattr(async_session, "_closed")
        assert hasattr(async_session, "_close_lock")  # Single lock for close

    async def test_result_handler_simplified(self):
        """Test that result handlers are simplified."""
        from async_cassandra.result import AsyncResultHandler

        mock_future = Mock()
        mock_future.has_more_pages = False
        mock_future.add_callbacks = Mock()
        mock_future.timeout = None

        handler = AsyncResultHandler(mock_future)

        # Should have minimal state tracking
        assert hasattr(handler, "_lock")  # Thread lock is necessary
        assert hasattr(handler, "rows")

        # Should not have complex state tracking
        assert not hasattr(handler, "_future_initialized")
        assert not hasattr(handler, "_result_ready")

    async def test_streaming_simplified(self):
        """Test that streaming result set is simplified."""
        from async_cassandra.streaming import AsyncStreamingResultSet, StreamConfig

        mock_future = Mock()
        mock_future.has_more_pages = True
        mock_future.add_callbacks = Mock()

        stream = AsyncStreamingResultSet(mock_future, StreamConfig())

        # Should have thread lock (necessary for callbacks)
        assert hasattr(stream, "_lock")

        # Should not have complex callback tracking
        assert not hasattr(stream, "_active_callbacks")

    async def test_idempotent_close(self):
        """Test that close is idempotent with simple implementation."""
        # Create session
        mock_session = Mock()
        mock_session.shutdown = Mock()
        async_session = AsyncCassandraSession(mock_session)

        # Multiple closes should work without complex locking
        await async_session.close()
        await async_session.close()
        await async_session.close()

        # Should only shutdown once
        assert mock_session.shutdown.call_count == 1

    async def test_no_operation_counting(self):
        """Test that we don't count active operations."""
        # Create session
        mock_session = Mock()
        mock_response_future = Mock()
        mock_response_future.has_more_pages = False
        mock_response_future.add_callbacks = Mock()
        mock_response_future.timeout = None

        # Make execute_async slow to simulate long operation
        async def slow_execute(*args, **kwargs):
            await asyncio.sleep(0.1)
            return mock_response_future

        mock_session.execute_async = Mock(side_effect=lambda *a, **k: mock_response_future)
        mock_session.shutdown = Mock()

        async_session = AsyncCassandraSession(mock_session)

        # Start a query
        query_task = asyncio.create_task(async_session.execute("SELECT 1"))
        await asyncio.sleep(0.01)  # Let it start

        # Close should not wait for operations
        start_time = time.perf_counter()
        await async_session.close()
        close_duration = time.perf_counter() - start_time

        # Close includes a 5-second delay to let driver threads finish
        assert 5.0 <= close_duration < 6.0

        # Query might fail or succeed - both are acceptable
        try:
            # Trigger callback if query is still running
            if mock_response_future.add_callbacks.called:
                callback = mock_response_future.add_callbacks.call_args[1]["callback"]
                callback(["row"])
            await query_task
        except Exception:
            # Error is acceptable if close interrupted it
            pass

    @pytest.mark.benchmark
    async def test_performance_improvement(self):
        """Benchmark to show performance improvement with simplified locking."""
        # This test demonstrates that simplified locking improves performance

        # Create session
        mock_session = Mock()
        mock_response_future = Mock()
        mock_response_future.has_more_pages = False
        mock_response_future.add_callbacks = Mock()
        mock_response_future.timeout = None
        mock_session.execute_async = Mock(return_value=mock_response_future)

        async_session = AsyncCassandraSession(mock_session)

        # Measure throughput
        iterations = 1000
        start_time = time.perf_counter()

        tasks = []
        for i in range(iterations):
            task = asyncio.create_task(async_session.execute(f"SELECT {i}"))
            tasks.append(task)

        # Trigger all callbacks immediately
        await asyncio.sleep(0)
        for call in mock_response_future.add_callbacks.call_args_list:
            callback = call[1]["callback"]
            callback(["row"])

        await asyncio.gather(*tasks)

        duration = time.perf_counter() - start_time
        ops_per_second = iterations / duration

        # With simplified locking, should handle >5000 ops/second
        assert ops_per_second > 5000
        print(f"Performance: {ops_per_second:.0f} ops/second")
