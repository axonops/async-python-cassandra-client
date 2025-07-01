"""
Unit tests for NoHostAvailable exception handling.

This module tests the specific handling of NoHostAvailable errors,
which indicate that no Cassandra nodes are available to handle requests.

Test Organization:
==================
1. Direct Exception Propagation - NoHostAvailable raised without wrapping
2. Error Details Preservation - Host-specific errors maintained
3. Metrics Recording - Failure metrics tracked correctly
4. Exception Type Consistency - All Cassandra exceptions handled uniformly

Key Testing Principles:
======================
- NoHostAvailable must not be wrapped in QueryError
- Host error details must be preserved
- Metrics must capture connection failures
- Cassandra exceptions get special treatment
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
        """
        Test that NoHostAvailable is raised directly without wrapping.

        What this tests:
        ---------------
        1. NoHostAvailable propagates unchanged
        2. Not wrapped in QueryError
        3. Original message preserved
        4. Exception type maintained

        Why this matters:
        ----------------
        NoHostAvailable requires special handling:
        - Indicates infrastructure problems
        - May need different retry strategy
        - Often requires manual intervention

        Wrapping it would hide its specific nature and
        break error handling code that catches NoHostAvailable.
        """
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
        """
        Test that execute_stream raises NoHostAvailable directly.

        What this tests:
        ---------------
        1. Streaming also preserves NoHostAvailable
        2. Consistent with execute() behavior
        3. No wrapping in streaming path
        4. Same exception handling for both methods

        Why this matters:
        ----------------
        Applications need consistent error handling:
        - Same exceptions from execute() and execute_stream()
        - Can reuse error handling logic
        - No surprises when switching methods

        This ensures streaming doesn't introduce
        different error handling requirements.
        """
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
        """
        Test that NoHostAvailable preserves detailed host error information.

        What this tests:
        ---------------
        1. Host-specific errors in 'errors' dict
        2. Each host's failure reason preserved
        3. Error details not lost in propagation
        4. Can diagnose per-host problems

        Why this matters:
        ----------------
        NoHostAvailable.errors contains valuable debugging info:
        - Which hosts failed and why
        - Connection refused vs timeout vs other
        - Helps identify patterns (all timeout = network issue)

        Operations teams need these details to:
        - Identify which nodes are problematic
        - Diagnose network vs node issues
        - Take targeted corrective action
        """
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
        """
        Test that metrics are recorded when NoHostAvailable occurs.

        What this tests:
        ---------------
        1. Metrics capture NoHostAvailable errors
        2. Error type recorded as 'NoHostAvailable'
        3. Success=False in metrics
        4. Fire-and-forget metrics don't block

        Why this matters:
        ----------------
        Monitoring connection failures is critical:
        - Track cluster health over time
        - Alert on connection problems
        - Identify patterns and trends

        NoHostAvailable metrics help detect:
        - Cluster-wide outages
        - Network partitions
        - Configuration problems
        """
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
        """
        Test that non-Cassandra exceptions are still wrapped in QueryError.

        What this tests:
        ---------------
        1. Non-Cassandra exceptions wrapped in QueryError
        2. Only Cassandra exceptions get special treatment
        3. Generic errors still provide context
        4. Original exception in __cause__

        Why this matters:
        ----------------
        Different exception types need different handling:
        - Cassandra exceptions: domain-specific, preserve as-is
        - Other exceptions: wrap for context and consistency

        This ensures unexpected errors still get
        meaningful context while preserving Cassandra's
        carefully designed exception hierarchy.
        """
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
        """
        Test that all Cassandra exceptions are raised directly.

        What this tests:
        ---------------
        1. All Cassandra exception types preserved
        2. InvalidRequest, timeouts, Unavailable, etc.
        3. Exact exception instances propagated
        4. Consistent handling across all types

        Why this matters:
        ----------------
        Cassandra's exception hierarchy is well-designed:
        - Each type indicates specific problems
        - Contains relevant diagnostic information
        - Enables proper retry strategies

        Wrapping would:
        - Break existing error handlers
        - Hide important error details
        - Prevent proper retry logic

        This comprehensive test ensures all Cassandra
        exceptions are treated consistently.
        """
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
