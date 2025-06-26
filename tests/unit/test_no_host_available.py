"""
Unit tests for NoHostAvailable exception handling.
"""

import asyncio
from unittest.mock import Mock

import pytest
from cassandra.cluster import NoHostAvailable

from async_cassandra.exceptions import QueryError
from async_cassandra.session import AsyncCassandraSession


@pytest.mark.asyncio
class TestNoHostAvailableHandling:
    """Test NoHostAvailable exception handling."""

    async def test_execute_raises_no_host_available_directly(self):
        """Test that NoHostAvailable is raised directly without wrapping."""
        # Mock cassandra session that raises NoHostAvailable
        mock_session = Mock()
        mock_session.execute_async = Mock(side_effect=NoHostAvailable("All hosts are down", {}))

        # Create async session
        async_session = AsyncCassandraSession(mock_session)

        # Should raise NoHostAvailable directly, not wrapped in QueryError
        with pytest.raises(NoHostAvailable) as exc_info:
            await async_session.execute("SELECT * FROM test")

        # Verify it's the original exception
        assert "All hosts are down" in str(exc_info.value)

    async def test_execute_stream_raises_no_host_available_directly(self):
        """Test that execute_stream raises NoHostAvailable directly."""
        # Mock cassandra session that raises NoHostAvailable
        mock_session = Mock()
        mock_session.execute_async = Mock(side_effect=NoHostAvailable("Connection failed", {}))

        # Create async session
        async_session = AsyncCassandraSession(mock_session)

        # Should raise NoHostAvailable directly
        with pytest.raises(NoHostAvailable) as exc_info:
            await async_session.execute_stream("SELECT * FROM test")

        # Verify it's the original exception
        assert "Connection failed" in str(exc_info.value)

    async def test_no_host_available_preserves_host_errors(self):
        """Test that NoHostAvailable preserves detailed host error information."""
        # Create NoHostAvailable with host errors
        host_errors = {
            "host1": Exception("Connection refused"),
            "host2": Exception("Host unreachable"),
        }
        no_host_error = NoHostAvailable("No hosts available", host_errors)

        # Mock cassandra session
        mock_session = Mock()
        mock_session.execute_async = Mock(side_effect=no_host_error)

        # Create async session
        async_session = AsyncCassandraSession(mock_session)

        # Execute and catch exception
        with pytest.raises(NoHostAvailable) as exc_info:
            await async_session.execute("SELECT * FROM test")

        # Verify host errors are preserved
        caught_exception = exc_info.value
        assert hasattr(caught_exception, "errors")
        assert "host1" in caught_exception.errors
        assert "host2" in caught_exception.errors

    async def test_metrics_recorded_for_no_host_available(self):
        """Test that metrics are recorded when NoHostAvailable occurs."""
        # Mock cassandra session
        mock_session = Mock()
        mock_session.execute_async = Mock(side_effect=NoHostAvailable("All hosts down", {}))

        # Mock metrics
        from async_cassandra.metrics import MetricsMiddleware

        mock_metrics = Mock(spec=MetricsMiddleware)
        mock_metrics.record_query_metrics = Mock()

        # Create async session with metrics
        async_session = AsyncCassandraSession(mock_session, metrics=mock_metrics)

        # Execute and expect NoHostAvailable
        with pytest.raises(NoHostAvailable):
            await async_session.execute("SELECT * FROM test")

        # Give time for fire-and-forget metrics
        await asyncio.sleep(0.1)

        # Verify metrics were called with correct error type
        mock_metrics.record_query_metrics.assert_called_once()
        call_args = mock_metrics.record_query_metrics.call_args[1]
        assert call_args["success"] is False
        assert call_args["error_type"] == "NoHostAvailable"

    async def test_other_exceptions_still_wrapped(self):
        """Test that non-Cassandra exceptions are still wrapped in QueryError."""
        # Mock cassandra session that raises generic exception
        mock_session = Mock()
        mock_session.execute_async = Mock(side_effect=RuntimeError("Unexpected error"))

        # Create async session
        async_session = AsyncCassandraSession(mock_session)

        # Should wrap in QueryError
        with pytest.raises(QueryError) as exc_info:
            await async_session.execute("SELECT * FROM test")

        # Verify it's wrapped
        assert "Query execution failed" in str(exc_info.value)
        assert isinstance(exc_info.value.__cause__, RuntimeError)

    async def test_all_cassandra_exceptions_not_wrapped(self):
        """Test that all Cassandra exceptions are raised directly."""
        # Test each Cassandra exception type
        from cassandra import (
            InvalidRequest,
            OperationTimedOut,
            ReadTimeout,
            Unavailable,
            WriteTimeout,
            WriteType,
        )

        cassandra_exceptions = [
            InvalidRequest("Invalid query"),
            ReadTimeout("Read timeout", consistency=1, required_responses=3, received_responses=1),
            WriteTimeout(
                "Write timeout",
                consistency=1,
                required_responses=3,
                received_responses=1,
                write_type=WriteType.SIMPLE,
            ),
            Unavailable(
                "Not enough replicas", consistency=1, required_replicas=3, alive_replicas=1
            ),
            OperationTimedOut("Operation timed out"),
            NoHostAvailable("No hosts", {}),
        ]

        for exception in cassandra_exceptions:
            # Mock session
            mock_session = Mock()
            mock_session.execute_async = Mock(side_effect=exception)

            # Create async session
            async_session = AsyncCassandraSession(mock_session)

            # Should raise original exception type
            with pytest.raises(type(exception)) as exc_info:
                await async_session.execute("SELECT * FROM test")

            # Verify it's the exact same exception
            assert exc_info.value is exception
