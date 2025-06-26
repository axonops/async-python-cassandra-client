"""
Unit tests for fire-and-forget metrics recording.
"""

import asyncio
from unittest.mock import AsyncMock, Mock, patch

import pytest

from async_cassandra.exceptions import QueryError
from async_cassandra.metrics import MetricsMiddleware
from async_cassandra.session import AsyncCassandraSession


@pytest.mark.asyncio
class TestFireAndForgetMetrics:
    """Test fire-and-forget metrics functionality."""

    async def test_metrics_do_not_block_query_success(self):
        """Test that slow metrics recording doesn't block query completion."""
        # Mock cassandra session
        mock_session = Mock()
        response_future = Mock()
        response_future.has_more_pages = False
        response_future.add_callbacks = Mock()
        response_future.timeout = None
        mock_session.execute_async = Mock(return_value=response_future)

        # Mock slow metrics
        mock_metrics = Mock(spec=MetricsMiddleware)

        async def slow_record(*args, **kwargs):
            await asyncio.sleep(2.0)  # Simulate slow metrics

        mock_metrics.record_query_metrics = AsyncMock(side_effect=slow_record)

        # Create async session with metrics
        async_session = AsyncCassandraSession(mock_session, metrics=mock_metrics)

        # Start the query execution task
        start_time = asyncio.get_event_loop().time()
        query_task = asyncio.create_task(async_session.execute("SELECT * FROM test"))

        # Give a moment for the execute_async to be called
        await asyncio.sleep(0.01)

        # Trigger success callback
        args = response_future.add_callbacks.call_args
        callback = args[1]["callback"]
        callback(["row1"])

        result = await query_task

        end_time = asyncio.get_event_loop().time()
        duration = end_time - start_time

        # Query should complete quickly without waiting for slow metrics
        assert duration < 0.5  # Much less than the 2s metrics recording
        assert len(result.rows) == 1

    async def test_metrics_error_does_not_affect_query(self):
        """Test that metrics errors don't affect query execution."""
        # Mock cassandra session
        mock_session = Mock()
        response_future = Mock()
        response_future.has_more_pages = False
        response_future.add_callbacks = Mock()
        response_future.timeout = None
        mock_session.execute_async = Mock(return_value=response_future)

        # Mock metrics that raises error
        mock_metrics = Mock(spec=MetricsMiddleware)
        mock_metrics.record_query_metrics = AsyncMock(side_effect=Exception("Metrics error"))

        # Create async session with faulty metrics
        async_session = AsyncCassandraSession(mock_session, metrics=mock_metrics)

        # Start query execution
        query_task = asyncio.create_task(async_session.execute("SELECT * FROM test"))
        await asyncio.sleep(0.01)

        # Trigger success callback
        args = response_future.add_callbacks.call_args
        callback = args[1]["callback"]
        callback(["row1", "row2"])

        # Query should succeed despite metrics error
        result = await query_task
        assert len(result.rows) == 2

        # Give time for the background task to run
        await asyncio.sleep(0.1)

    async def test_metrics_recorded_on_query_error(self):
        """Test that metrics are recorded even when query fails."""
        # Mock cassandra session
        mock_session = Mock()
        response_future = Mock()
        response_future.has_more_pages = False
        response_future.add_callbacks = Mock()
        mock_session.execute_async = Mock(return_value=response_future)

        # Mock metrics
        mock_metrics = Mock(spec=MetricsMiddleware)
        mock_metrics.record_query_metrics = AsyncMock()

        # Create async session
        async_session = AsyncCassandraSession(mock_session, metrics=mock_metrics)

        # Start query execution
        query_task = asyncio.create_task(async_session.execute("SELECT * FROM test"))
        await asyncio.sleep(0.01)

        # Trigger error callback
        args = response_future.add_callbacks.call_args
        errback = args[1]["errback"]
        errback(Exception("Query failed"))

        # Query should fail
        with pytest.raises(QueryError):
            await query_task

        # Give time for background task
        await asyncio.sleep(0.1)

        # Metrics should have been called
        mock_metrics.record_query_metrics.assert_called_once()
        call_args = mock_metrics.record_query_metrics.call_args[1]
        assert call_args["success"] is False
        # The error type should be captured from the raised exception
        assert call_args["error_type"] is not None

    async def test_no_event_loop_does_not_crash(self):
        """Test that missing event loop doesn't crash the application."""
        # This test simulates a scenario where asyncio.create_task might fail
        mock_session = Mock()
        mock_metrics = Mock(spec=MetricsMiddleware)

        async_session = AsyncCassandraSession(mock_session, metrics=mock_metrics)

        # Call _record_metrics_async directly to test error handling
        # This should not raise any exceptions
        async_session._record_metrics_async(
            query_str="SELECT * FROM test",
            duration=0.1,
            success=True,
            error_type=None,
            parameters_count=0,
            result_size=1,
        )

    async def test_metrics_called_with_correct_parameters(self):
        """Test that metrics are called with correct parameters."""
        # Mock cassandra session
        mock_session = Mock()
        response_future = Mock()
        response_future.has_more_pages = False
        response_future.add_callbacks = Mock()
        response_future.timeout = None
        mock_session.execute_async = Mock(return_value=response_future)

        # Mock metrics
        mock_metrics = Mock(spec=MetricsMiddleware)
        mock_metrics.record_query_metrics = AsyncMock()

        # Create async session
        async_session = AsyncCassandraSession(mock_session, metrics=mock_metrics)

        # Start query execution
        query_task = asyncio.create_task(
            async_session.execute("SELECT * FROM test WHERE id = ?", ["123"])
        )
        await asyncio.sleep(0.01)

        # Trigger success callback
        args = response_future.add_callbacks.call_args
        callback = args[1]["callback"]
        callback(["row1", "row2", "row3"])

        # Get result
        await query_task

        # Give time for background task
        await asyncio.sleep(0.1)

        # Check metrics were called correctly
        mock_metrics.record_query_metrics.assert_called_once()
        call_args = mock_metrics.record_query_metrics.call_args[1]
        assert call_args["query"] == "SELECT * FROM test WHERE id = ?"
        assert call_args["success"] is True
        assert call_args["error_type"] is None
        assert call_args["parameters_count"] == 1
        assert call_args["result_size"] == 3
        assert call_args["duration"] > 0

    @patch("async_cassandra.session.logger")
    async def test_metrics_error_is_logged(self, mock_logger):
        """Test that metrics errors are logged."""
        # Mock cassandra session
        mock_session = Mock()
        response_future = Mock()
        response_future.has_more_pages = False
        response_future.add_callbacks = Mock()
        response_future.timeout = None
        mock_session.execute_async = Mock(return_value=response_future)

        # Mock metrics that raises error
        mock_metrics = Mock(spec=MetricsMiddleware)
        mock_metrics.record_query_metrics = AsyncMock(
            side_effect=Exception("Metrics recording failed")
        )

        # Create async session
        async_session = AsyncCassandraSession(mock_session, metrics=mock_metrics)

        # Start query execution
        query_task = asyncio.create_task(async_session.execute("SELECT * FROM test"))
        await asyncio.sleep(0.01)

        # Trigger success callback
        args = response_future.add_callbacks.call_args
        callback = args[1]["callback"]
        callback(["row1"])

        # Execute query
        await query_task

        # Give time for background task and logging
        await asyncio.sleep(0.2)

        # Check that warning was logged
        mock_logger.warning.assert_called_with("Failed to record metrics: Metrics recording failed")
