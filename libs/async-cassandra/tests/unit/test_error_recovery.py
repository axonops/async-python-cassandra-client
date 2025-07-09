"""Error recovery and handling tests.

This module tests various error scenarios including NoHostAvailable,
connection errors, and proper error propagation through the async layer.

Test Organization:
==================
1. Connection Errors - NoHostAvailable, pool exhaustion
2. Query Errors - InvalidRequest, Unavailable
3. Callback Errors - Errors in async callbacks
4. Shutdown Scenarios - Graceful shutdown with pending queries
5. Error Isolation - Concurrent query error isolation

Key Testing Principles:
======================
- Errors must propagate with full context
- Stack traces must be preserved
- Concurrent errors must be isolated
- Graceful degradation under failure
- Recovery after transient failures
"""

import asyncio
from unittest.mock import Mock

import pytest
from cassandra import ConsistencyLevel, InvalidRequest, Unavailable
from cassandra.cluster import NoHostAvailable

from async_cassandra import AsyncCassandraSession as AsyncSession
from async_cassandra import AsyncCluster


def create_mock_response_future(rows=None, has_more_pages=False):
    """
    Helper to create a properly configured mock ResponseFuture.

    This helper ensures mock ResponseFutures behave like real ones,
    with proper callback handling and attribute setup.
    """
    mock_future = Mock()
    mock_future.has_more_pages = has_more_pages
    mock_future.timeout = None  # Avoid comparison issues
    mock_future.add_callbacks = Mock()

    def handle_callbacks(callback=None, errback=None):
        if callback:
            callback(rows if rows is not None else [])

    mock_future.add_callbacks.side_effect = handle_callbacks
    return mock_future


class TestErrorRecovery:
    """Test error recovery and handling scenarios."""

    @pytest.mark.resilience
    @pytest.mark.quick
    @pytest.mark.critical
    async def test_no_host_available_error(self):
        """
        Test handling of NoHostAvailable errors.

        What this tests:
        ---------------
        1. NoHostAvailable errors propagate correctly
        2. Error details include all failed hosts
        3. Connection errors for each host preserved
        4. Error message is informative

        Why this matters:
        ----------------
        NoHostAvailable is a critical error indicating:
        - All nodes are down or unreachable
        - Network partition or configuration issues
        - Need for manual intervention

        Applications need full error details to diagnose
        and alert on infrastructure problems.
        """
        errors = {
            "127.0.0.1": ConnectionRefusedError("Connection refused"),
            "127.0.0.2": TimeoutError("Connection timeout"),
        }

        # Create a real async session with mocked underlying session
        mock_session = Mock()
        mock_session.execute_async.side_effect = NoHostAvailable(
            "Unable to connect to any servers", errors
        )

        async_session = AsyncSession(mock_session)

        with pytest.raises(NoHostAvailable) as exc_info:
            await async_session.execute("SELECT * FROM users")

        assert "Unable to connect to any servers" in str(exc_info.value)
        assert "127.0.0.1" in exc_info.value.errors
        assert "127.0.0.2" in exc_info.value.errors

    @pytest.mark.resilience
    async def test_invalid_request_error(self):
        """
        Test handling of invalid request errors.

        What this tests:
        ---------------
        1. InvalidRequest errors propagate cleanly
        2. Error message preserved exactly
        3. No wrapping or modification
        4. Useful for debugging CQL issues

        Why this matters:
        ----------------
        InvalidRequest indicates:
        - Syntax errors in CQL
        - Schema mismatches
        - Invalid parameters

        Developers need the exact error message from
        Cassandra to fix their queries.
        """
        mock_session = Mock()
        mock_session.execute_async.side_effect = InvalidRequest("Invalid CQL syntax")

        async_session = AsyncSession(mock_session)

        with pytest.raises(InvalidRequest, match="Invalid CQL syntax"):
            await async_session.execute("INVALID QUERY SYNTAX")

    @pytest.mark.resilience
    async def test_unavailable_error(self):
        """
        Test handling of unavailable errors.

        What this tests:
        ---------------
        1. Unavailable errors include consistency details
        2. Required vs available replicas reported
        3. Consistency level preserved
        4. All error attributes accessible

        Why this matters:
        ----------------
        Unavailable errors help diagnose:
        - Insufficient replicas for consistency
        - Node failures affecting availability
        - Need to adjust consistency levels

        Applications can use this info to:
        - Retry with lower consistency
        - Alert on degraded availability
        - Make informed consistency trade-offs
        """
        mock_session = Mock()
        mock_session.execute_async.side_effect = Unavailable(
            "Cannot achieve consistency",
            consistency=ConsistencyLevel.QUORUM,
            required_replicas=2,
            alive_replicas=1,
        )

        async_session = AsyncSession(mock_session)

        with pytest.raises(Unavailable) as exc_info:
            await async_session.execute("SELECT * FROM users")

        assert exc_info.value.consistency == ConsistencyLevel.QUORUM
        assert exc_info.value.required_replicas == 2
        assert exc_info.value.alive_replicas == 1

    @pytest.mark.resilience
    @pytest.mark.critical
    async def test_error_in_async_callback(self):
        """
        Test error handling in async callbacks.

        What this tests:
        ---------------
        1. Errors in callbacks are captured
        2. AsyncResultHandler propagates callback errors
        3. Original error type and message preserved
        4. Async layer doesn't swallow errors

        Why this matters:
        ----------------
        The async wrapper uses callbacks to bridge
        sync driver to async/await. Errors in this
        bridge must not be lost or corrupted.

        This ensures reliability of error reporting
        through the entire async pipeline.
        """
        from async_cassandra.result import AsyncResultHandler

        # Create a mock ResponseFuture
        mock_future = Mock()
        mock_future.has_more_pages = False
        mock_future.add_callbacks = Mock()
        mock_future.timeout = None  # Set timeout to None to avoid comparison issues

        handler = AsyncResultHandler(mock_future)
        test_error = RuntimeError("Callback error")

        # Manually call the error handler to simulate callback error
        handler._handle_error(test_error)

        with pytest.raises(RuntimeError, match="Callback error"):
            await handler.get_result()

    @pytest.mark.resilience
    async def test_connection_pool_exhaustion_recovery(self):
        """
        Test recovery from connection pool exhaustion.

        What this tests:
        ---------------
        1. Pool exhaustion errors are transient
        2. Retry after exhaustion can succeed
        3. No permanent failure from temporary exhaustion
        4. Application can recover automatically

        Why this matters:
        ----------------
        Connection pools can be temporarily exhausted during:
        - Traffic spikes
        - Slow queries holding connections
        - Network delays

        Applications should be able to recover when
        connections become available again, without
        manual intervention or restart.
        """
        mock_session = Mock()

        # Create a mock ResponseFuture for successful response
        mock_future = create_mock_response_future([{"id": 1}])

        # Simulate pool exhaustion then recovery
        responses = [
            NoHostAvailable("Pool exhausted", {}),
            NoHostAvailable("Pool exhausted", {}),
            mock_future,  # Recovery returns ResponseFuture
        ]
        mock_session.execute_async.side_effect = responses

        async_session = AsyncSession(mock_session)

        # First two attempts fail
        for i in range(2):
            with pytest.raises(NoHostAvailable):
                await async_session.execute("SELECT * FROM users")

        # Third attempt succeeds
        result = await async_session.execute("SELECT * FROM users")
        assert result._rows == [{"id": 1}]

    @pytest.mark.resilience
    async def test_partial_write_error_handling(self):
        """
        Test handling of partial write errors.

        What this tests:
        ---------------
        1. Coordinator timeout errors propagate
        2. Write might have partially succeeded
        3. Error message indicates uncertainty
        4. Application can handle ambiguity

        Why this matters:
        ----------------
        Partial writes are dangerous because:
        - Some replicas might have the data
        - Some might not (inconsistent state)
        - Retry might cause duplicates

        Applications need to know when writes
        are ambiguous to handle appropriately.
        """
        mock_session = Mock()

        # Simulate partial write success
        mock_session.execute_async.side_effect = Exception(
            "Coordinator node timed out during write"
        )

        async_session = AsyncSession(mock_session)

        with pytest.raises(Exception, match="Coordinator node timed out"):
            await async_session.execute("INSERT INTO users (id, name) VALUES (?, ?)", [1, "test"])

    @pytest.mark.resilience
    async def test_error_during_prepared_statement(self):
        """
        Test error handling during prepared statement execution.

        What this tests:
        ---------------
        1. Prepare succeeds but execute can fail
        2. Parameter validation errors propagate
        3. Prepared statements don't mask errors
        4. Error occurs at execution, not preparation

        Why this matters:
        ----------------
        Prepared statements can fail at execution due to:
        - Invalid parameter types
        - Null values where not allowed
        - Value size exceeding limits

        The async layer must propagate these execution
        errors clearly for debugging.
        """
        mock_session = Mock()
        mock_prepared = Mock()

        # Prepare succeeds
        mock_session.prepare.return_value = mock_prepared

        # But execution fails
        mock_session.execute_async.side_effect = InvalidRequest("Invalid parameter")

        async_session = AsyncSession(mock_session)

        # Prepare statement
        prepared = await async_session.prepare("SELECT * FROM users WHERE id = ?")
        assert prepared == mock_prepared

        # Execute should fail
        with pytest.raises(InvalidRequest, match="Invalid parameter"):
            await async_session.execute(prepared, [None])

    @pytest.mark.resilience
    @pytest.mark.critical
    @pytest.mark.timeout(40)  # Increase timeout to account for 5s shutdown delay
    async def test_graceful_shutdown_with_pending_queries(self):
        """
        Test graceful shutdown when queries are pending.

        What this tests:
        ---------------
        1. Shutdown waits for driver to finish
        2. Pending queries can complete during shutdown
        3. 5-second grace period for completion
        4. Clean shutdown without hanging

        Why this matters:
        ----------------
        Applications need graceful shutdown to:
        - Complete in-flight requests
        - Avoid data loss or corruption
        - Clean up resources properly

        The 5-second delay gives driver threads
        time to complete ongoing operations before
        forcing termination.
        """
        mock_session = Mock()
        mock_cluster = Mock()

        # Track shutdown completion
        shutdown_complete = asyncio.Event()

        # Mock the cluster shutdown to complete quickly
        def mock_shutdown():
            shutdown_complete.set()

        mock_cluster.shutdown = mock_shutdown

        # Create queries that will complete after a delay
        query_complete = asyncio.Event()

        # Create mock ResponseFutures
        def create_mock_future(*args):
            mock_future = Mock()
            mock_future.has_more_pages = False
            mock_future.timeout = None
            mock_future.add_callbacks = Mock()

            def handle_callbacks(callback=None, errback=None):
                # Schedule the callback to be called after a short delay
                # This simulates a query that completes during shutdown
                def delayed_callback():
                    if callback:
                        callback([])  # Call with empty rows
                    query_complete.set()

                # Use asyncio to schedule the callback
                asyncio.get_event_loop().call_later(0.1, delayed_callback)

            mock_future.add_callbacks.side_effect = handle_callbacks
            return mock_future

        mock_session.execute_async.side_effect = create_mock_future

        cluster = AsyncCluster()
        cluster._cluster = mock_cluster
        cluster._cluster.protocol_version = 5  # Mock protocol version
        cluster._cluster.connect.return_value = mock_session

        session = await cluster.connect()

        # Start a query
        query_task = asyncio.create_task(session.execute("SELECT * FROM table"))

        # Give query time to start
        await asyncio.sleep(0.05)

        # Start shutdown in background (it will wait 5 seconds after driver shutdown)
        shutdown_task = asyncio.create_task(cluster.shutdown())

        # Wait for driver shutdown to complete
        await shutdown_complete.wait()

        # Query should complete during the 5 second wait
        await query_complete.wait()

        # Wait for the query task to actually complete
        # Use wait_for with a timeout to avoid hanging if something goes wrong
        try:
            await asyncio.wait_for(query_task, timeout=1.0)
        except asyncio.TimeoutError:
            pytest.fail("Query task did not complete within timeout")

        # Wait for full shutdown including the 5 second delay
        await shutdown_task

        # Verify everything completed properly
        assert query_task.done()
        assert not query_task.cancelled()  # Query completed normally
        assert cluster.is_closed

    @pytest.mark.resilience
    async def test_error_stack_trace_preservation(self):
        """
        Test that error stack traces are preserved through async layer.

        What this tests:
        ---------------
        1. Original exception traceback preserved
        2. Error message unchanged
        3. Exception type maintained
        4. Debugging information intact

        Why this matters:
        ----------------
        Stack traces are critical for debugging:
        - Show where error originated
        - Include call chain context
        - Help identify root cause

        The async wrapper must not lose or corrupt
        this debugging information while propagating
        errors across thread boundaries.
        """
        mock_session = Mock()

        # Create an error with traceback info
        try:
            raise InvalidRequest("Original error")
        except InvalidRequest as e:
            original_error = e

        mock_session.execute_async.side_effect = original_error

        async_session = AsyncSession(mock_session)

        try:
            await async_session.execute("SELECT * FROM users")
        except InvalidRequest as e:
            # Stack trace should be preserved
            assert str(e) == "Original error"
            assert e.__traceback__ is not None

    @pytest.mark.resilience
    async def test_concurrent_error_isolation(self):
        """
        Test that errors in concurrent queries don't affect each other.

        What this tests:
        ---------------
        1. Each query gets its own error/result
        2. Failures don't cascade to other queries
        3. Mixed success/failure scenarios work
        4. Error types are preserved per query

        Why this matters:
        ----------------
        Applications often run many queries concurrently:
        - Dashboard fetching multiple metrics
        - Batch processing different tables
        - Parallel data aggregation

        One query's failure should not affect others.
        Each query should succeed or fail independently
        based on its own merits.
        """
        mock_session = Mock()

        # Different errors for different queries
        def execute_side_effect(query, *args, **kwargs):
            if "table1" in query:
                raise InvalidRequest("Error in table1")
            elif "table2" in query:
                # Create a mock ResponseFuture for success
                return create_mock_response_future([{"id": 2}])
            elif "table3" in query:
                raise NoHostAvailable("No hosts for table3", {})
            else:
                # Create a mock ResponseFuture for empty result
                return create_mock_response_future([])

        mock_session.execute_async.side_effect = execute_side_effect

        async_session = AsyncSession(mock_session)

        # Execute queries concurrently
        tasks = [
            async_session.execute("SELECT * FROM table1"),
            async_session.execute("SELECT * FROM table2"),
            async_session.execute("SELECT * FROM table3"),
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Verify each query got its expected result/error
        assert isinstance(results[0], InvalidRequest)
        assert "Error in table1" in str(results[0])

        assert not isinstance(results[1], Exception)
        assert results[1]._rows == [{"id": 2}]

        assert isinstance(results[2], NoHostAvailable)
        assert "No hosts for table3" in str(results[2])
