"""
Unit tests for prepared statement invalidation and re-preparation.

Tests how the async wrapper handles:
- Prepared statements being invalidated by schema changes
- Automatic re-preparation
- Concurrent invalidation scenarios
"""

import asyncio
from unittest.mock import Mock

import pytest
from cassandra import InvalidRequest, OperationTimedOut
from cassandra.cluster import Session
from cassandra.query import BatchStatement, BatchType, PreparedStatement

from async_cassandra import AsyncCassandraSession


class TestPreparedStatementInvalidation:
    """Test prepared statement invalidation and recovery."""

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

    def create_prepared_future(self, prepared_stmt):
        """Create a mock future for prepare_async that returns a prepared statement."""
        future = Mock()
        callbacks = []
        errbacks = []

        def add_callbacks(callback=None, errback=None):
            if callback:
                callbacks.append(callback)
                # Prepare callback gets the prepared statement directly
                callback(prepared_stmt)
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
        session.prepare_async = Mock()
        session.cluster = Mock()
        session.get_execution_profile = Mock(return_value=Mock())
        return session

    @pytest.fixture
    def mock_prepared_statement(self):
        """Create a mock prepared statement."""
        stmt = Mock(spec=PreparedStatement)
        stmt.query_id = b"test_query_id"
        stmt.query = "SELECT * FROM test WHERE id = ?"

        # Create a mock bound statement with proper attributes
        bound_stmt = Mock()
        bound_stmt.custom_payload = None
        bound_stmt.routing_key = None
        bound_stmt.keyspace = None
        bound_stmt.consistency_level = None
        bound_stmt.fetch_size = None
        bound_stmt.serial_consistency_level = None
        bound_stmt.retry_policy = None

        stmt.bind = Mock(return_value=bound_stmt)
        return stmt

    @pytest.mark.asyncio
    async def test_prepared_statement_invalidation_error(
        self, mock_session, mock_prepared_statement
    ):
        """
        Test that invalidated prepared statements raise InvalidRequest.

        What this tests:
        ---------------
        1. Invalidated statements detected
        2. InvalidRequest exception raised
        3. Clear error message provided
        4. No automatic re-preparation

        Why this matters:
        ----------------
        Schema changes invalidate statements:
        - Column added/removed
        - Table recreated
        - Type changes

        Applications must handle invalidation
        and re-prepare statements.
        """
        async_session = AsyncCassandraSession(mock_session)

        # First prepare succeeds (using sync prepare method)
        mock_session.prepare.return_value = mock_prepared_statement

        # Prepare statement
        prepared = await async_session.prepare("SELECT * FROM test WHERE id = ?")
        assert prepared == mock_prepared_statement

        # Setup execution to fail with InvalidRequest (statement invalidated)
        mock_session.execute_async.return_value = self.create_error_future(
            InvalidRequest("Prepared statement is invalid")
        )

        # Execute with invalidated statement - should raise InvalidRequest
        with pytest.raises(InvalidRequest) as exc_info:
            await async_session.execute(prepared, [1])

        assert "Prepared statement is invalid" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_manual_reprepare_after_invalidation(self, mock_session, mock_prepared_statement):
        """
        Test manual re-preparation after invalidation.

        What this tests:
        ---------------
        1. Re-preparation creates new statement
        2. New statement has different ID
        3. Execution works after re-prepare
        4. Old statement remains invalid

        Why this matters:
        ----------------
        Recovery pattern after invalidation:
        - Catch InvalidRequest
        - Re-prepare statement
        - Retry with new statement

        Critical for handling schema
        evolution in production.
        """
        async_session = AsyncCassandraSession(mock_session)

        # First prepare succeeds (using sync prepare method)
        mock_session.prepare.return_value = mock_prepared_statement

        # Prepare statement
        prepared = await async_session.prepare("SELECT * FROM test WHERE id = ?")

        # Setup execution to fail with InvalidRequest
        mock_session.execute_async.return_value = self.create_error_future(
            InvalidRequest("Prepared statement is invalid")
        )

        # First execution fails
        with pytest.raises(InvalidRequest):
            await async_session.execute(prepared, [1])

        # Create new prepared statement
        new_prepared = Mock(spec=PreparedStatement)
        new_prepared.query_id = b"new_query_id"
        new_prepared.query = "SELECT * FROM test WHERE id = ?"

        # Create bound statement with proper attributes
        new_bound = Mock()
        new_bound.custom_payload = None
        new_bound.routing_key = None
        new_bound.keyspace = None
        new_prepared.bind = Mock(return_value=new_bound)

        # Re-prepare manually
        mock_session.prepare.return_value = new_prepared
        prepared2 = await async_session.prepare("SELECT * FROM test WHERE id = ?")
        assert prepared2 == new_prepared
        assert prepared2.query_id != prepared.query_id

        # Now execution succeeds with new prepared statement
        mock_session.execute_async.return_value = self.create_success_future({"id": 1})
        result = await async_session.execute(prepared2, [1])
        assert result.rows[0]["id"] == 1

    @pytest.mark.asyncio
    async def test_concurrent_invalidation_handling(self, mock_session, mock_prepared_statement):
        """
        Test that concurrent executions all fail with invalidation.

        What this tests:
        ---------------
        1. All concurrent queries fail
        2. Each gets InvalidRequest
        3. No race conditions
        4. Consistent error handling

        Why this matters:
        ----------------
        Under high concurrency:
        - Many queries may use same statement
        - All must handle invalidation
        - No query should hang or corrupt

        Ensures thread-safe error propagation
        for invalidated statements.
        """
        async_session = AsyncCassandraSession(mock_session)

        # Prepare statement
        mock_session.prepare.return_value = mock_prepared_statement
        prepared = await async_session.prepare("SELECT * FROM test WHERE id = ?")

        # All executions fail with invalidation
        mock_session.execute_async.return_value = self.create_error_future(
            InvalidRequest("Prepared statement is invalid")
        )

        # Execute multiple concurrent queries
        tasks = [async_session.execute(prepared, [i]) for i in range(5)]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # All should fail with InvalidRequest
        assert len(results) == 5
        assert all(isinstance(r, InvalidRequest) for r in results)
        assert all("Prepared statement is invalid" in str(r) for r in results)

    @pytest.mark.asyncio
    async def test_invalidation_during_batch_execution(self, mock_session, mock_prepared_statement):
        """
        Test prepared statement invalidation during batch execution.

        What this tests:
        ---------------
        1. Batch with prepared statements
        2. Invalidation affects batch
        3. Whole batch fails
        4. Error clearly indicates issue

        Why this matters:
        ----------------
        Batches often contain prepared statements:
        - Bulk inserts/updates
        - Multi-row operations
        - Transaction-like semantics

        Batch invalidation requires re-preparing
        all statements in the batch.
        """
        async_session = AsyncCassandraSession(mock_session)

        # Prepare statement
        mock_session.prepare.return_value = mock_prepared_statement
        prepared = await async_session.prepare("INSERT INTO test (id, value) VALUES (?, ?)")

        # Create batch with prepared statement
        batch = BatchStatement(batch_type=BatchType.LOGGED)
        batch.add(prepared, (1, "value1"))
        batch.add(prepared, (2, "value2"))

        # Batch execution fails with invalidation
        mock_session.execute_async.return_value = self.create_error_future(
            InvalidRequest("Prepared statement is invalid")
        )

        # Batch execution should fail
        with pytest.raises(InvalidRequest) as exc_info:
            await async_session.execute(batch)

        assert "Prepared statement is invalid" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_invalidation_error_propagation(self, mock_session, mock_prepared_statement):
        """
        Test that non-invalidation errors are properly propagated.

        What this tests:
        ---------------
        1. Non-invalidation errors preserved
        2. Timeouts not confused with invalidation
        3. Error types maintained
        4. No incorrect error wrapping

        Why this matters:
        ----------------
        Different errors need different handling:
        - Timeouts: retry same statement
        - Invalidation: re-prepare needed
        - Other errors: various responses

        Accurate error types enable
        correct recovery strategies.
        """
        async_session = AsyncCassandraSession(mock_session)

        # Prepare statement
        mock_session.prepare.return_value = mock_prepared_statement
        prepared = await async_session.prepare("SELECT * FROM test WHERE id = ?")

        # Execution fails with different error (not invalidation)
        mock_session.execute_async.return_value = self.create_error_future(
            OperationTimedOut("Query timed out")
        )

        # Should propagate the error
        with pytest.raises(OperationTimedOut) as exc_info:
            await async_session.execute(prepared, [1])

        assert "Query timed out" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_reprepare_failure_handling(self, mock_session, mock_prepared_statement):
        """
        Test handling when re-preparation itself fails.

        What this tests:
        ---------------
        1. Re-preparation can fail
        2. Table might be dropped
        3. QueryError wraps prepare errors
        4. Original cause preserved

        Why this matters:
        ----------------
        Re-preparation fails when:
        - Table/keyspace dropped
        - Permissions changed
        - Query now invalid

        Applications must handle both
        invalidation AND re-prepare failure.
        """
        async_session = AsyncCassandraSession(mock_session)

        # Initial prepare succeeds
        mock_session.prepare.return_value = mock_prepared_statement
        prepared = await async_session.prepare("SELECT * FROM test WHERE id = ?")

        # Execution fails with invalidation
        mock_session.execute_async.return_value = self.create_error_future(
            InvalidRequest("Prepared statement is invalid")
        )

        # First execution fails
        with pytest.raises(InvalidRequest):
            await async_session.execute(prepared, [1])

        # Re-preparation fails (e.g., table dropped)
        mock_session.prepare.side_effect = InvalidRequest("Table test does not exist")

        # Re-prepare attempt should fail - InvalidRequest passed through
        with pytest.raises(InvalidRequest) as exc_info:
            await async_session.prepare("SELECT * FROM test WHERE id = ?")

        assert "Table test does not exist" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_prepared_statement_cache_behavior(self, mock_session):
        """
        Test that prepared statements are not cached by the async wrapper.

        What this tests:
        ---------------
        1. No built-in caching in wrapper
        2. Each prepare goes to driver
        3. Driver handles caching
        4. Different IDs for re-prepares

        Why this matters:
        ----------------
        Caching strategy important:
        - Driver caches per connection
        - Application may cache globally
        - Wrapper stays simple

        Applications should implement
        their own caching strategy.
        """
        async_session = AsyncCassandraSession(mock_session)

        # Create different prepared statements for same query
        stmt1 = Mock(spec=PreparedStatement)
        stmt1.query_id = b"id1"
        stmt1.query = "SELECT * FROM test WHERE id = ?"
        bound1 = Mock(custom_payload=None)
        stmt1.bind = Mock(return_value=bound1)

        stmt2 = Mock(spec=PreparedStatement)
        stmt2.query_id = b"id2"
        stmt2.query = "SELECT * FROM test WHERE id = ?"
        bound2 = Mock(custom_payload=None)
        stmt2.bind = Mock(return_value=bound2)

        # First prepare
        mock_session.prepare.return_value = stmt1
        prepared1 = await async_session.prepare("SELECT * FROM test WHERE id = ?")
        assert prepared1.query_id == b"id1"

        # Second prepare of same query (no caching in wrapper)
        mock_session.prepare.return_value = stmt2
        prepared2 = await async_session.prepare("SELECT * FROM test WHERE id = ?")
        assert prepared2.query_id == b"id2"

        # Verify prepare was called twice
        assert mock_session.prepare.call_count == 2

    @pytest.mark.asyncio
    async def test_invalidation_with_custom_payload(self, mock_session, mock_prepared_statement):
        """
        Test prepared statement invalidation with custom payload.

        What this tests:
        ---------------
        1. Custom payloads work with prepare
        2. Payload passed to driver
        3. Invalidation still detected
        4. Tracing/debugging preserved

        Why this matters:
        ----------------
        Custom payloads used for:
        - Request tracing
        - Performance monitoring
        - Debugging metadata

        Must work correctly even during
        error scenarios like invalidation.
        """
        async_session = AsyncCassandraSession(mock_session)

        # Prepare with custom payload
        custom_payload = {"app_name": "test_app"}
        mock_session.prepare.return_value = mock_prepared_statement

        prepared = await async_session.prepare(
            "SELECT * FROM test WHERE id = ?", custom_payload=custom_payload
        )

        # Verify custom payload was passed
        mock_session.prepare.assert_called_with("SELECT * FROM test WHERE id = ?", custom_payload)

        # Execute fails with invalidation
        mock_session.execute_async.return_value = self.create_error_future(
            InvalidRequest("Prepared statement is invalid")
        )

        with pytest.raises(InvalidRequest):
            await async_session.execute(prepared, [1])

    @pytest.mark.asyncio
    async def test_statement_id_tracking(self, mock_session):
        """
        Test that statement IDs are properly tracked.

        What this tests:
        ---------------
        1. Each statement has unique ID
        2. IDs preserved in errors
        3. Can identify which statement failed
        4. Helpful error messages

        Why this matters:
        ----------------
        Statement IDs help debugging:
        - Which statement invalidated
        - Correlate with server logs
        - Track statement lifecycle

        Essential for troubleshooting
        production invalidation issues.
        """
        async_session = AsyncCassandraSession(mock_session)

        # Create statements with specific IDs
        stmt1 = Mock(spec=PreparedStatement, query_id=b"id1", query="SELECT 1")
        stmt2 = Mock(spec=PreparedStatement, query_id=b"id2", query="SELECT 2")

        # Prepare multiple statements
        mock_session.prepare.side_effect = [stmt1, stmt2]

        prepared1 = await async_session.prepare("SELECT 1")
        prepared2 = await async_session.prepare("SELECT 2")

        # Verify different IDs
        assert prepared1.query_id == b"id1"
        assert prepared2.query_id == b"id2"
        assert prepared1.query_id != prepared2.query_id

        # Execute with specific statement
        mock_session.execute_async.return_value = self.create_error_future(
            InvalidRequest(f"Prepared statement with ID {stmt1.query_id.hex()} is invalid")
        )

        # Should fail with specific error message
        with pytest.raises(InvalidRequest) as exc_info:
            await async_session.execute(prepared1)

        assert stmt1.query_id.hex() in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_invalidation_after_schema_change(self, mock_session):
        """
        Test prepared statement invalidation after schema change.

        What this tests:
        ---------------
        1. Statement works before change
        2. Schema change invalidates
        3. Result metadata mismatch detected
        4. Clear error about metadata

        Why this matters:
        ----------------
        Common schema changes that invalidate:
        - ALTER TABLE ADD COLUMN
        - DROP/RECREATE TABLE
        - Type modifications

        This is the most common cause of
        invalidation in production systems.
        """
        async_session = AsyncCassandraSession(mock_session)

        # Prepare statement
        stmt = Mock(spec=PreparedStatement)
        stmt.query_id = b"test_id"
        stmt.query = "SELECT id, name FROM users WHERE id = ?"
        bound = Mock(custom_payload=None)
        stmt.bind = Mock(return_value=bound)

        mock_session.prepare.return_value = stmt
        prepared = await async_session.prepare("SELECT id, name FROM users WHERE id = ?")

        # First execution succeeds
        mock_session.execute_async.return_value = self.create_success_future(
            {"id": 1, "name": "Alice"}
        )
        result = await async_session.execute(prepared, [1])
        assert result.rows[0]["name"] == "Alice"

        # Simulate schema change (column added)
        # Next execution fails with invalidation
        mock_session.execute_async.return_value = self.create_error_future(
            InvalidRequest("Prepared query has an invalid result metadata")
        )

        with pytest.raises(InvalidRequest) as exc_info:
            await async_session.execute(prepared, [2])

        assert "invalid result metadata" in str(exc_info.value)
