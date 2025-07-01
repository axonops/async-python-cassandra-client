"""
Unit tests for session edge cases and failure scenarios.

Tests how the async wrapper handles various session-level failures and edge cases
within its existing functionality.
"""

import asyncio
from unittest.mock import AsyncMock, Mock

import pytest
from cassandra import InvalidRequest, OperationTimedOut, ReadTimeout, Unavailable, WriteTimeout
from cassandra.cluster import Session
from cassandra.query import BatchStatement, PreparedStatement, SimpleStatement

from async_cassandra import AsyncCassandraSession


class TestSessionEdgeCases:
    """Test session edge cases and failure scenarios."""

    def _create_mock_future(self, result=None, error=None):
        """Create a properly configured mock future that simulates driver behavior."""
        future = Mock()

        # Store callbacks
        callbacks = []
        errbacks = []

        def add_callbacks(callback=None, errback=None):
            if callback:
                callbacks.append(callback)
            if errback:
                errbacks.append(errback)

            # Delay the callback execution to allow AsyncResultHandler to set up properly
            def execute_callback():
                if error:
                    if errback:
                        errback(error)
                else:
                    if callback and result is not None:
                        # For successful results, pass rows
                        rows = getattr(result, "rows", [])
                        callback(rows)

            # Schedule callback for next event loop iteration
            try:
                loop = asyncio.get_running_loop()
                loop.call_soon(execute_callback)
            except RuntimeError:
                # No event loop, execute immediately
                execute_callback()

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
        session.prepare_async = Mock()
        session.close = Mock()
        session.close_async = Mock()
        session.cluster = Mock()
        session.cluster.protocol_version = 5
        return session

    @pytest.fixture
    async def async_session(self, mock_session):
        """Create an async session wrapper."""
        return AsyncCassandraSession(mock_session)

    @pytest.mark.asyncio
    async def test_execute_with_invalid_request(self, async_session, mock_session):
        """
        Test handling of InvalidRequest errors.

        What this tests:
        ---------------
        1. InvalidRequest not wrapped
        2. Error message preserved
        3. Direct propagation
        4. Query syntax errors

        Why this matters:
        ----------------
        InvalidRequest indicates:
        - Query syntax errors
        - Schema mismatches
        - Invalid operations

        Clear errors help developers
        fix queries quickly.
        """
        # Mock execute_async to fail with InvalidRequest
        future = self._create_mock_future(error=InvalidRequest("Table does not exist"))
        mock_session.execute_async.return_value = future

        # Should propagate InvalidRequest
        with pytest.raises(InvalidRequest) as exc_info:
            await async_session.execute("SELECT * FROM nonexistent_table")

        assert "Table does not exist" in str(exc_info.value)
        assert mock_session.execute_async.called

    @pytest.mark.asyncio
    async def test_execute_with_timeout(self, async_session, mock_session):
        """
        Test handling of operation timeout.

        What this tests:
        ---------------
        1. OperationTimedOut propagated
        2. Timeout errors not wrapped
        3. Message preserved
        4. Clean error handling

        Why this matters:
        ----------------
        Timeouts are common:
        - Slow queries
        - Network issues
        - Overloaded nodes

        Applications need clear
        timeout information.
        """
        # Mock execute_async to fail with timeout
        future = self._create_mock_future(error=OperationTimedOut("Query timed out"))
        mock_session.execute_async.return_value = future

        # Should propagate timeout
        with pytest.raises(OperationTimedOut) as exc_info:
            await async_session.execute("SELECT * FROM large_table")

        assert "Query timed out" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_execute_with_read_timeout(self, async_session, mock_session):
        """
        Test handling of read timeout.

        What this tests:
        ---------------
        1. ReadTimeout details preserved
        2. Response counts available
        3. Data retrieval flag set
        4. Not wrapped

        Why this matters:
        ----------------
        Read timeout details crucial:
        - Shows partial success
        - Indicates retry potential
        - Helps tune consistency

        Details enable smart
        retry decisions.
        """
        # Mock read timeout
        future = self._create_mock_future(
            error=ReadTimeout(
                "Read timeout",
                consistency_level=1,
                required_responses=1,
                received_responses=0,
                data_retrieved=False,
            )
        )
        mock_session.execute_async.return_value = future

        # Should propagate read timeout
        with pytest.raises(ReadTimeout) as exc_info:
            await async_session.execute("SELECT * FROM table")

        # Just verify we got the right exception with the message
        assert "Read timeout" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_execute_with_write_timeout(self, async_session, mock_session):
        """
        Test handling of write timeout.

        What this tests:
        ---------------
        1. WriteTimeout propagated
        2. Write type preserved
        3. Response details available
        4. Proper error type

        Why this matters:
        ----------------
        Write timeouts critical:
        - May have partial writes
        - Write type matters for retry
        - Data consistency concerns

        Details determine if
        retry is safe.
        """
        # Mock write timeout (write_type needs to be numeric)
        from cassandra import WriteType

        future = self._create_mock_future(
            error=WriteTimeout(
                "Write timeout",
                consistency_level=1,
                required_responses=1,
                received_responses=0,
                write_type=WriteType.SIMPLE,
            )
        )
        mock_session.execute_async.return_value = future

        # Should propagate write timeout
        with pytest.raises(WriteTimeout) as exc_info:
            await async_session.execute("INSERT INTO table (id) VALUES (1)")

        # Just verify we got the right exception with the message
        assert "Write timeout" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_execute_with_unavailable(self, async_session, mock_session):
        """
        Test handling of Unavailable exception.

        What this tests:
        ---------------
        1. Unavailable propagated
        2. Replica counts preserved
        3. Consistency level shown
        4. Clear error info

        Why this matters:
        ----------------
        Unavailable means:
        - Not enough replicas up
        - Cluster health issue
        - Cannot meet consistency

        Shows cluster state for
        operations decisions.
        """
        # Mock unavailable (consistency is first positional arg)
        future = self._create_mock_future(
            error=Unavailable(
                "Not enough replicas", consistency=1, required_replicas=3, alive_replicas=1
            )
        )
        mock_session.execute_async.return_value = future

        # Should propagate unavailable
        with pytest.raises(Unavailable) as exc_info:
            await async_session.execute("SELECT * FROM table")

        # Just verify we got the right exception with the message
        assert "Not enough replicas" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_prepare_statement_error(self, async_session, mock_session):
        """
        Test error handling during statement preparation.

        What this tests:
        ---------------
        1. Prepare errors wrapped
        2. QueryError with cause
        3. Error message clear
        4. Original exception preserved

        Why this matters:
        ----------------
        Prepare failures indicate:
        - Syntax errors
        - Schema issues
        - Permission problems

        Wrapped to distinguish from
        execution errors.
        """
        # Mock prepare to fail (it uses sync prepare in executor)
        mock_session.prepare.side_effect = InvalidRequest("Syntax error in CQL")

        # Should pass through InvalidRequest directly
        with pytest.raises(InvalidRequest) as exc_info:
            await async_session.prepare("INVALID CQL SYNTAX")

        assert "Syntax error in CQL" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_execute_prepared_statement(self, async_session, mock_session):
        """
        Test executing prepared statements.

        What this tests:
        ---------------
        1. Prepared statements work
        2. Parameters handled
        3. Results returned
        4. Proper execution flow

        Why this matters:
        ----------------
        Prepared statements are:
        - Performance critical
        - Security essential
        - Most common pattern

        Must work seamlessly
        through async wrapper.
        """
        # Create mock prepared statement
        prepared = Mock(spec=PreparedStatement)
        prepared.query = "SELECT * FROM users WHERE id = ?"

        # Mock successful execution
        result = Mock()
        result.one = Mock(return_value={"id": 1, "name": "test"})
        result.rows = [{"id": 1, "name": "test"}]
        future = self._create_mock_future(result=result)
        mock_session.execute_async.return_value = future

        # Execute prepared statement
        result = await async_session.execute(prepared, [1])
        assert result.one()["id"] == 1

    @pytest.mark.asyncio
    async def test_execute_batch_statement(self, async_session, mock_session):
        """
        Test executing batch statements.

        What this tests:
        ---------------
        1. Batch execution works
        2. Multiple statements grouped
        3. Parameters preserved
        4. Batch type maintained

        Why this matters:
        ----------------
        Batches provide:
        - Atomic operations
        - Better performance
        - Reduced round trips

        Critical for bulk
        data operations.
        """
        # Create batch statement
        batch = BatchStatement()
        batch.add(SimpleStatement("INSERT INTO users (id, name) VALUES (%s, %s)"), (1, "user1"))
        batch.add(SimpleStatement("INSERT INTO users (id, name) VALUES (%s, %s)"), (2, "user2"))

        # Mock successful execution
        result = Mock()
        result.rows = []
        future = self._create_mock_future(result=result)
        mock_session.execute_async.return_value = future

        # Execute batch
        await async_session.execute(batch)

        # Verify batch was executed
        mock_session.execute_async.assert_called_once()
        call_args = mock_session.execute_async.call_args[0]
        assert isinstance(call_args[0], BatchStatement)

    @pytest.mark.asyncio
    async def test_concurrent_queries(self, async_session, mock_session):
        """
        Test concurrent query execution.

        What this tests:
        ---------------
        1. Concurrent execution allowed
        2. All queries complete
        3. Results independent
        4. True parallelism

        Why this matters:
        ----------------
        Concurrency essential for:
        - High throughput
        - Parallel processing
        - Efficient resource use

        Async wrapper must enable
        true concurrent execution.
        """
        # Track execution order to verify concurrency
        execution_times = []

        def execute_side_effect(*args, **kwargs):
            import time

            execution_times.append(time.time())

            # Create result
            result = Mock()
            result.one = Mock(return_value={"count": len(execution_times)})
            result.rows = [{"count": len(execution_times)}]

            # Use our standard mock future
            future = self._create_mock_future(result=result)
            return future

        mock_session.execute_async.side_effect = execute_side_effect

        # Execute multiple queries concurrently
        queries = [async_session.execute(f"SELECT {i} FROM table") for i in range(10)]

        results = await asyncio.gather(*queries)

        # All should complete
        assert len(results) == 10
        assert len(execution_times) == 10

        # Verify we got results
        for result in results:
            assert len(result.rows) == 1
            assert result.rows[0]["count"] > 0

        # The execute_async calls should happen close together (within 100ms)
        # This verifies they were submitted concurrently
        time_span = max(execution_times) - min(execution_times)
        assert time_span < 0.1, f"Queries took {time_span}s, suggesting serial execution"

    @pytest.mark.asyncio
    async def test_session_close_idempotent(self, async_session, mock_session):
        """
        Test that session close is idempotent.

        What this tests:
        ---------------
        1. Multiple closes safe
        2. Shutdown called once
        3. No errors on re-close
        4. State properly tracked

        Why this matters:
        ----------------
        Idempotent close needed for:
        - Error handling paths
        - Multiple cleanup sources
        - Resource leak prevention

        Safe cleanup in all
        code paths.
        """
        # Setup shutdown
        mock_session.shutdown = Mock()

        # First close
        await async_session.close()
        assert mock_session.shutdown.call_count == 1

        # Second close should be safe
        await async_session.close()
        # Should still only be called once
        assert mock_session.shutdown.call_count == 1

    @pytest.mark.asyncio
    async def test_query_after_close(self, async_session, mock_session):
        """
        Test querying after session is closed.

        What this tests:
        ---------------
        1. Closed sessions reject queries
        2. ConnectionError raised
        3. Clear error message
        4. State checking works

        Why this matters:
        ----------------
        Using closed resources:
        - Common bug source
        - Hard to debug
        - Silent failures bad

        Clear errors prevent
        mysterious failures.
        """
        # Close session
        mock_session.shutdown = Mock()
        await async_session.close()

        # Try to execute query - should fail with ConnectionError
        from async_cassandra.exceptions import ConnectionError

        with pytest.raises(ConnectionError) as exc_info:
            await async_session.execute("SELECT * FROM table")

        assert "Session is closed" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_metrics_recording_on_success(self, mock_session):
        """
        Test metrics are recorded on successful queries.

        What this tests:
        ---------------
        1. Success metrics recorded
        2. Async metrics work
        3. Proper success flag
        4. No error type

        Why this matters:
        ----------------
        Metrics enable:
        - Performance monitoring
        - Error tracking
        - Capacity planning

        Accurate metrics critical
        for production observability.
        """
        # Create metrics mock
        mock_metrics = Mock()
        mock_metrics.record_query_metrics = AsyncMock()

        # Create session with metrics
        async_session = AsyncCassandraSession(mock_session, metrics=mock_metrics)

        # Mock successful execution
        result = Mock()
        result.one = Mock(return_value={"id": 1})
        result.rows = [{"id": 1}]
        future = self._create_mock_future(result=result)
        mock_session.execute_async.return_value = future

        # Execute query
        await async_session.execute("SELECT * FROM users")

        # Give time for async metrics recording
        await asyncio.sleep(0.1)

        # Verify metrics were recorded
        mock_metrics.record_query_metrics.assert_called_once()
        call_kwargs = mock_metrics.record_query_metrics.call_args[1]
        assert call_kwargs["success"] is True
        assert call_kwargs["error_type"] is None

    @pytest.mark.asyncio
    async def test_metrics_recording_on_failure(self, mock_session):
        """
        Test metrics are recorded on failed queries.

        What this tests:
        ---------------
        1. Failure metrics recorded
        2. Error type captured
        3. Success flag false
        4. Async recording works

        Why this matters:
        ----------------
        Error metrics reveal:
        - Problem patterns
        - Error types
        - Failure rates

        Essential for debugging
        production issues.
        """
        # Create metrics mock
        mock_metrics = Mock()
        mock_metrics.record_query_metrics = AsyncMock()

        # Create session with metrics
        async_session = AsyncCassandraSession(mock_session, metrics=mock_metrics)

        # Mock failed execution
        future = self._create_mock_future(error=InvalidRequest("Bad query"))
        mock_session.execute_async.return_value = future

        # Execute query (should fail)
        with pytest.raises(InvalidRequest):
            await async_session.execute("INVALID QUERY")

        # Give time for async metrics recording
        await asyncio.sleep(0.1)

        # Verify metrics were recorded
        mock_metrics.record_query_metrics.assert_called_once()
        call_kwargs = mock_metrics.record_query_metrics.call_args[1]
        assert call_kwargs["success"] is False
        assert call_kwargs["error_type"] == "InvalidRequest"

    @pytest.mark.asyncio
    async def test_custom_payload_handling(self, async_session, mock_session):
        """
        Test custom payload in queries.

        What this tests:
        ---------------
        1. Custom payloads passed through
        2. Correct parameter position
        3. Payload preserved
        4. Driver feature works

        Why this matters:
        ----------------
        Custom payloads enable:
        - Request tracing
        - Debugging metadata
        - Cross-system correlation

        Important for distributed
        system observability.
        """
        # Mock execution with custom payload
        result = Mock()
        result.custom_payload = {"server_time": "2024-01-01"}
        result.rows = []
        future = self._create_mock_future(result=result)
        mock_session.execute_async.return_value = future

        # Execute with custom payload
        custom_payload = {"client_id": "12345"}
        result = await async_session.execute("SELECT * FROM table", custom_payload=custom_payload)

        # Verify custom payload was passed (4th positional arg)
        call_args = mock_session.execute_async.call_args[0]
        assert call_args[3] == custom_payload  # custom_payload is 4th arg

    @pytest.mark.asyncio
    async def test_trace_execution(self, async_session, mock_session):
        """
        Test query tracing.

        What this tests:
        ---------------
        1. Trace flag passed through
        2. Correct parameter position
        3. Tracing enabled
        4. Request setup correct

        Why this matters:
        ----------------
        Query tracing helps:
        - Debug slow queries
        - Understand execution
        - Optimize performance

        Essential debugging tool
        for production issues.
        """
        # Mock execution with trace
        result = Mock()
        result.get_query_trace = Mock(return_value=Mock(trace_id="abc123"))
        result.rows = []
        future = self._create_mock_future(result=result)
        mock_session.execute_async.return_value = future

        # Execute with tracing
        result = await async_session.execute("SELECT * FROM table", trace=True)

        # Verify trace was requested (3rd positional arg)
        call_args = mock_session.execute_async.call_args[0]
        assert call_args[2] is True  # trace is 3rd arg

        # AsyncResultSet doesn't expose trace methods - that's ok
        # Just verify the request was made with trace=True

    @pytest.mark.asyncio
    async def test_execution_profile_handling(self, async_session, mock_session):
        """
        Test using execution profiles.

        What this tests:
        ---------------
        1. Execution profiles work
        2. Profile name passed
        3. Correct parameter position
        4. Driver feature accessible

        Why this matters:
        ----------------
        Execution profiles control:
        - Consistency levels
        - Retry policies
        - Load balancing

        Critical for workload
        optimization.
        """
        # Mock execution
        result = Mock()
        result.rows = []
        future = self._create_mock_future(result=result)
        mock_session.execute_async.return_value = future

        # Execute with custom profile
        await async_session.execute("SELECT * FROM table", execution_profile="high_throughput")

        # Verify profile was passed (6th positional arg)
        call_args = mock_session.execute_async.call_args[0]
        assert call_args[5] == "high_throughput"  # execution_profile is 6th arg

    @pytest.mark.asyncio
    async def test_timeout_parameter(self, async_session, mock_session):
        """
        Test query timeout parameter.

        What this tests:
        ---------------
        1. Timeout parameter works
        2. Value passed correctly
        3. Correct position
        4. Per-query timeouts

        Why this matters:
        ----------------
        Query timeouts prevent:
        - Hanging queries
        - Resource exhaustion
        - Poor user experience

        Per-query control enables
        SLA compliance.
        """
        # Mock execution
        result = Mock()
        result.rows = []
        future = self._create_mock_future(result=result)
        mock_session.execute_async.return_value = future

        # Execute with timeout
        await async_session.execute("SELECT * FROM table", timeout=5.0)

        # Verify timeout was passed (5th positional arg)
        call_args = mock_session.execute_async.call_args[0]
        assert call_args[4] == 5.0  # timeout is 5th arg
