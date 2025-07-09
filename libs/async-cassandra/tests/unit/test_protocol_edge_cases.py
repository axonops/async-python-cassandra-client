"""
Unit tests for protocol-level edge cases.

Tests how the async wrapper handles:
- Protocol version negotiation issues
- Protocol errors during queries
- Custom payloads
- Large queries
- Various Cassandra exceptions

Test Organization:
==================
1. Protocol Negotiation - Version negotiation failures
2. Protocol Errors - Errors during query execution
3. Custom Payloads - Application-specific protocol data
4. Query Size Limits - Large query handling
5. Error Recovery - Recovery from protocol issues

Key Testing Principles:
======================
- Test protocol boundary conditions
- Verify error propagation
- Ensure graceful degradation
- Test recovery mechanisms
"""

from unittest.mock import Mock, patch

import pytest
from cassandra import InvalidRequest, OperationTimedOut, UnsupportedOperation
from cassandra.cluster import NoHostAvailable, Session
from cassandra.connection import ProtocolError

from async_cassandra import AsyncCassandraSession
from async_cassandra.exceptions import ConnectionError


class TestProtocolEdgeCases:
    """Test protocol-level edge cases and error handling."""

    def create_error_future(self, exception):
        """Create a mock future that raises the given exception."""
        future = Mock()
        callbacks = []
        errbacks = []

        def add_callbacks(callback=None, errback=None):
            if callback:
                callbacks.append(callback)
            if errback:
                errbacks.append(errback)
                # Call errback immediately with the error
                errback(exception)

        future.add_callbacks = add_callbacks
        future.has_more_pages = False
        future.timeout = None
        future.clear_callbacks = Mock()
        return future

    def create_success_future(self, result):
        """Create a mock future that returns a result."""
        future = Mock()
        callbacks = []
        errbacks = []

        def add_callbacks(callback=None, errback=None):
            if callback:
                callbacks.append(callback)
                # For success, the callback expects an iterable of rows
                mock_rows = [result] if result else []
                callback(mock_rows)
            if errback:
                errbacks.append(errback)

        future.add_callbacks = add_callbacks
        future.has_more_pages = False
        future.timeout = None
        future.clear_callbacks = Mock()
        return future

    @pytest.fixture
    def mock_session(self):
        """Create a mock session."""
        session = Mock(spec=Session)
        session.execute_async = Mock()
        session.prepare = Mock()
        session.cluster = Mock()
        session.cluster.protocol_version = 5
        return session

    @pytest.mark.asyncio
    async def test_protocol_version_negotiation_failure(self):
        """
        Test handling of protocol version negotiation failures.

        What this tests:
        ---------------
        1. Protocol negotiation can fail
        2. NoHostAvailable with ProtocolError
        3. Wrapped in ConnectionError
        4. Clear error message

        Why this matters:
        ----------------
        Protocol negotiation failures occur when:
        - Client/server version mismatch
        - Unsupported protocol features
        - Configuration conflicts

        Users need clear guidance on
        version compatibility issues.
        """
        from async_cassandra import AsyncCluster

        with patch("async_cassandra.cluster.Cluster") as mock_cluster_class:
            # Create mock cluster instance
            mock_cluster = Mock()
            mock_cluster_class.return_value = mock_cluster

            # Simulate protocol negotiation failure during connect
            mock_cluster.connect.side_effect = NoHostAvailable(
                "Unable to connect to any servers",
                {"127.0.0.1": ProtocolError("Cannot negotiate protocol version")},
            )

            async_cluster = AsyncCluster(contact_points=["127.0.0.1"])

            # Should fail with connection error
            with pytest.raises(ConnectionError) as exc_info:
                await async_cluster.connect()

            assert "Failed to connect" in str(exc_info.value)

            await async_cluster.shutdown()

    @pytest.mark.asyncio
    async def test_protocol_error_during_query(self, mock_session):
        """
        Test handling of protocol errors during query execution.

        What this tests:
        ---------------
        1. Protocol errors during execution
        2. ProtocolError passed through without wrapping
        3. Direct exception access
        4. Error details preserved as-is

        Why this matters:
        ----------------
        Protocol errors indicate:
        - Corrupted messages
        - Protocol violations
        - Driver/server bugs

        Users need direct access for
        proper error handling and debugging.
        """
        async_session = AsyncCassandraSession(mock_session)

        # Simulate protocol error
        mock_session.execute_async.return_value = self.create_error_future(
            ProtocolError("Invalid or unsupported protocol version")
        )

        # ProtocolError is now passed through without wrapping
        with pytest.raises(ProtocolError) as exc_info:
            await async_session.execute("SELECT * FROM test")

        assert "Invalid or unsupported protocol version" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_custom_payload_handling(self, mock_session):
        """
        Test handling of custom payloads in protocol.

        What this tests:
        ---------------
        1. Custom payloads passed through
        2. Payload data preserved
        3. No interference with query
        4. Application metadata works

        Why this matters:
        ----------------
        Custom payloads enable:
        - Request tracing
        - Application context
        - Cross-system correlation

        Used for debugging and monitoring
        in production systems.
        """
        async_session = AsyncCassandraSession(mock_session)

        # Track custom payloads
        sent_payloads = []

        def execute_async_side_effect(*args, **kwargs):
            # Extract custom payload if provided
            custom_payload = args[3] if len(args) > 3 else kwargs.get("custom_payload")
            if custom_payload:
                sent_payloads.append(custom_payload)

            return self.create_success_future({"payload_received": True})

        mock_session.execute_async.side_effect = execute_async_side_effect

        # Execute with custom payload
        custom_data = {"app_name": "test_app", "request_id": "12345"}
        result = await async_session.execute("SELECT * FROM test", custom_payload=custom_data)

        # Verify payload was sent
        assert len(sent_payloads) == 1
        assert sent_payloads[0] == custom_data
        assert result.rows[0]["payload_received"] is True

    @pytest.mark.asyncio
    async def test_large_query_handling(self, mock_session):
        """
        Test handling of very large queries.

        What this tests:
        ---------------
        1. Query size limits enforced
        2. InvalidRequest for oversized queries
        3. Clear size limit in error
        4. Not wrapped (Cassandra error)

        Why this matters:
        ----------------
        Query size limits prevent:
        - Memory exhaustion
        - Network overload
        - Protocol buffer overflow

        Applications must chunk large
        operations or use prepared statements.
        """
        async_session = AsyncCassandraSession(mock_session)

        # Create very large query
        large_values = ["x" * 1000 for _ in range(100)]  # ~100KB of data
        large_query = f"INSERT INTO test (id, data) VALUES (1, '{','.join(large_values)}')"

        # Execution fails due to size
        mock_session.execute_async.return_value = self.create_error_future(
            InvalidRequest("Query string length (102400) is greater than maximum allowed (65535)")
        )

        # InvalidRequest is not wrapped
        with pytest.raises(InvalidRequest) as exc_info:
            await async_session.execute(large_query)

        assert "greater than maximum allowed" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_unsupported_operation(self, mock_session):
        """
        Test handling of unsupported operations.

        What this tests:
        ---------------
        1. UnsupportedOperation errors passed through
        2. No wrapping - direct exception access
        3. Feature limitations clearly visible
        4. Version-specific features preserved

        Why this matters:
        ----------------
        Features vary by protocol version:
        - Continuous paging (v5+)
        - Duration type (v5+)
        - Per-query keyspace (v5+)

        Users need direct access to handle
        version-specific feature errors.
        """
        async_session = AsyncCassandraSession(mock_session)

        # Simulate unsupported operation
        mock_session.execute_async.return_value = self.create_error_future(
            UnsupportedOperation("Continuous paging is not supported by this protocol version")
        )

        # UnsupportedOperation is now passed through without wrapping
        with pytest.raises(UnsupportedOperation) as exc_info:
            await async_session.execute("SELECT * FROM test")

        assert "Continuous paging is not supported" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_protocol_error_recovery(self, mock_session):
        """
        Test recovery from protocol-level errors.

        What this tests:
        ---------------
        1. Protocol errors can be transient
        2. Recovery possible after errors
        3. Direct exception handling
        4. Eventually succeeds

        Why this matters:
        ----------------
        Some protocol errors are recoverable:
        - Stream ID conflicts
        - Temporary corruption
        - Race conditions

        Users can implement retry logic
        with new connections as needed.
        """
        async_session = AsyncCassandraSession(mock_session)

        # Track protocol errors
        error_count = 0

        def execute_async_side_effect(*args, **kwargs):
            nonlocal error_count
            error_count += 1

            if error_count <= 2:
                # First attempts fail with protocol error
                return self.create_error_future(ProtocolError("Protocol error: Invalid stream id"))
            else:
                # Recovery succeeds
                return self.create_success_future({"recovered": True})

        mock_session.execute_async.side_effect = execute_async_side_effect

        # First two attempts should fail
        for i in range(2):
            with pytest.raises(ProtocolError):
                await async_session.execute("SELECT * FROM test")

        # Third attempt should succeed
        result = await async_session.execute("SELECT * FROM test")
        assert result.rows[0]["recovered"] is True
        assert error_count == 3

    @pytest.mark.asyncio
    async def test_protocol_version_in_session(self, mock_session):
        """
        Test accessing protocol version from session.

        What this tests:
        ---------------
        1. Protocol version accessible
        2. Available via cluster object
        3. Version doesn't affect queries
        4. Useful for debugging

        Why this matters:
        ----------------
        Applications may need version info:
        - Feature detection
        - Compatibility checks
        - Debugging protocol issues

        Version should be easily accessible
        for runtime decisions.
        """
        async_session = AsyncCassandraSession(mock_session)

        # Protocol version should be accessible via cluster
        assert mock_session.cluster.protocol_version == 5

        # Execute query to verify protocol version doesn't affect normal operation
        mock_session.execute_async.return_value = self.create_success_future(
            {"protocol_version": mock_session.cluster.protocol_version}
        )

        result = await async_session.execute("SELECT * FROM system.local")
        assert result.rows[0]["protocol_version"] == 5

    @pytest.mark.asyncio
    async def test_timeout_vs_protocol_error(self, mock_session):
        """
        Test differentiating between timeouts and protocol errors.

        What this tests:
        ---------------
        1. Timeouts not wrapped
        2. Protocol errors wrapped
        3. Different error handling
        4. Clear distinction

        Why this matters:
        ----------------
        Different errors need different handling:
        - Timeouts: often transient, retry
        - Protocol errors: serious, investigate

        Applications must distinguish to
        implement proper error handling.
        """
        async_session = AsyncCassandraSession(mock_session)

        # Test timeout
        mock_session.execute_async.return_value = self.create_error_future(
            OperationTimedOut("Request timed out")
        )

        # OperationTimedOut is not wrapped
        with pytest.raises(OperationTimedOut):
            await async_session.execute("SELECT * FROM test")

        # Test protocol error
        mock_session.execute_async.return_value = self.create_error_future(
            ProtocolError("Protocol violation")
        )

        # ProtocolError is now passed through without wrapping
        with pytest.raises(ProtocolError):
            await async_session.execute("SELECT * FROM test")

    @pytest.mark.asyncio
    async def test_prepare_with_protocol_error(self, mock_session):
        """
        Test prepared statement with protocol errors.

        What this tests:
        ---------------
        1. Prepare can fail with protocol error
        2. Passed through without wrapping
        3. Statement preparation issues visible
        4. Direct exception access

        Why this matters:
        ----------------
        Prepare failures indicate:
        - Schema issues
        - Protocol limitations
        - Query complexity problems

        Users need direct access to
        handle preparation failures.
        """
        async_session = AsyncCassandraSession(mock_session)

        # Prepare fails with protocol error
        mock_session.prepare.side_effect = ProtocolError("Cannot prepare statement")

        # ProtocolError is now passed through without wrapping
        with pytest.raises(ProtocolError) as exc_info:
            await async_session.prepare("SELECT * FROM test WHERE id = ?")

        assert "Cannot prepare statement" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_execution_profile_with_protocol_settings(self, mock_session):
        """
        Test execution profiles don't interfere with protocol handling.

        What this tests:
        ---------------
        1. Execution profiles work correctly
        2. Profile parameter passed through
        3. No protocol interference
        4. Custom settings preserved

        Why this matters:
        ----------------
        Execution profiles customize:
        - Consistency levels
        - Retry policies
        - Load balancing

        Must work seamlessly with
        protocol-level features.
        """
        async_session = AsyncCassandraSession(mock_session)

        # Execute with custom execution profile
        mock_session.execute_async.return_value = self.create_success_future({"profile": "custom"})

        result = await async_session.execute(
            "SELECT * FROM test", execution_profile="custom_profile"
        )

        # Verify execution profile was passed
        mock_session.execute_async.assert_called_once()
        call_args = mock_session.execute_async.call_args
        # Check positional arguments: query, parameters, trace, custom_payload, timeout, execution_profile
        assert call_args[0][5] == "custom_profile"  # execution_profile is 6th parameter (index 5)
        assert result.rows[0]["profile"] == "custom"

    @pytest.mark.asyncio
    async def test_batch_with_protocol_error(self, mock_session):
        """
        Test batch execution with protocol errors.

        What this tests:
        ---------------
        1. Batch operations can hit protocol limits
        2. Protocol errors passed through directly
        3. Batch size limits visible to users
        4. Native exception handling

        Why this matters:
        ----------------
        Batches have protocol limits:
        - Maximum batch size
        - Statement count limits
        - Protocol buffer constraints

        Users need direct access to
        handle batch size errors.
        """
        from cassandra.query import BatchStatement, BatchType

        async_session = AsyncCassandraSession(mock_session)

        # Create batch
        batch = BatchStatement(batch_type=BatchType.LOGGED)
        batch.add("INSERT INTO test (id) VALUES (1)")
        batch.add("INSERT INTO test (id) VALUES (2)")

        # Batch execution fails with protocol error
        mock_session.execute_async.return_value = self.create_error_future(
            ProtocolError("Batch too large for protocol")
        )

        # ProtocolError is now passed through without wrapping
        with pytest.raises(ProtocolError) as exc_info:
            await async_session.execute_batch(batch)

        assert "Batch too large" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_no_host_available_with_protocol_errors(self, mock_session):
        """
        Test NoHostAvailable containing protocol errors.

        What this tests:
        ---------------
        1. NoHostAvailable can contain various errors
        2. Protocol errors preserved per host
        3. Mixed error types handled
        4. Detailed error information

        Why this matters:
        ----------------
        Connection failures vary by host:
        - Some have protocol issues
        - Others timeout
        - Mixed failure modes

        Detailed per-host errors help
        diagnose cluster-wide issues.
        """
        async_session = AsyncCassandraSession(mock_session)

        # Create NoHostAvailable with protocol errors
        errors = {
            "10.0.0.1": ProtocolError("Protocol version mismatch"),
            "10.0.0.2": ProtocolError("Protocol negotiation failed"),
            "10.0.0.3": OperationTimedOut("Connection timeout"),
        }

        mock_session.execute_async.return_value = self.create_error_future(
            NoHostAvailable("Unable to connect to any servers", errors)
        )

        # NoHostAvailable is not wrapped
        with pytest.raises(NoHostAvailable) as exc_info:
            await async_session.execute("SELECT * FROM test")

        assert "Unable to connect to any servers" in str(exc_info.value)
        assert len(exc_info.value.errors) == 3
        assert isinstance(exc_info.value.errors["10.0.0.1"], ProtocolError)
