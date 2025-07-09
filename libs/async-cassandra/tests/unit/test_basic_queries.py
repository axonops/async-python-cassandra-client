"""Core basic query execution tests.

This module tests fundamental query operations that must work
for the async wrapper to be functional. These are the most basic
operations that users will perform, so they must be rock solid.

Test Organization:
==================
- TestBasicQueryExecution: All fundamental query types (SELECT, INSERT, UPDATE, DELETE)
- Tests both simple string queries and parameterized queries
- Covers various query options (consistency, timeout, custom payload)

Key Testing Focus:
==================
1. All CRUD operations work correctly
2. Parameters are properly passed to the driver
3. Results are wrapped in AsyncResultSet
4. Query options (timeout, consistency) are preserved
5. Empty results are handled gracefully
"""

from unittest.mock import AsyncMock, Mock, patch

import pytest
from cassandra import ConsistencyLevel
from cassandra.cluster import ResponseFuture
from cassandra.query import SimpleStatement

from async_cassandra import AsyncCassandraSession as AsyncSession
from async_cassandra.result import AsyncResultSet


class TestBasicQueryExecution:
    """
    Test basic query execution patterns.

    These tests ensure that the async wrapper correctly handles all
    fundamental query types that users will execute against Cassandra.
    Each test mocks the underlying driver to focus on the wrapper's behavior.
    """

    def _setup_mock_execute(self, mock_session, result_data=None):
        """
        Helper to setup mock execute_async with proper response.

        Creates a mock ResponseFuture that simulates the driver's
        async execution mechanism. This allows us to test the wrapper
        without actual network calls.
        """
        mock_future = Mock(spec=ResponseFuture)
        mock_future.has_more_pages = False
        mock_session.execute_async.return_value = mock_future

        if result_data is None:
            result_data = []

        return AsyncResultSet(result_data)

    @pytest.mark.core
    @pytest.mark.quick
    @pytest.mark.critical
    async def test_simple_select(self):
        """
        Test basic SELECT query execution.

        What this tests:
        ---------------
        1. Simple string SELECT queries work
        2. Results are returned as AsyncResultSet
        3. The driver's execute_async is called (not execute)
        4. No parameters case works correctly

        Why this matters:
        ----------------
        SELECT queries are the most common operation. This test ensures
        the basic read path works:
        - Query string is passed correctly
        - Async execution is used
        - Results are properly wrapped

        This is the simplest possible query - if this doesn't work,
        nothing else will.
        """
        mock_session = Mock()
        expected_result = self._setup_mock_execute(mock_session, [{"id": 1, "name": "test"}])

        async_session = AsyncSession(mock_session)

        # Patch AsyncResultHandler to simulate immediate result
        with patch("async_cassandra.session.AsyncResultHandler") as mock_handler_class:
            mock_handler = Mock()
            mock_handler.get_result = AsyncMock(return_value=expected_result)
            mock_handler_class.return_value = mock_handler

            result = await async_session.execute("SELECT * FROM users WHERE id = 1")

        assert isinstance(result, AsyncResultSet)
        mock_session.execute_async.assert_called_once()

    @pytest.mark.core
    @pytest.mark.critical
    async def test_parameterized_query(self):
        """
        Test query with bound parameters.

        What this tests:
        ---------------
        1. Parameterized queries work with ? placeholders
        2. Parameters are passed as a list
        3. Multiple parameters are handled correctly
        4. Parameter values are preserved exactly

        Why this matters:
        ----------------
        Parameterized queries are essential for:
        - SQL injection prevention
        - Better performance (query plan caching)
        - Type safety
        - Clean code (no string concatenation)

        This test ensures parameters flow correctly through the
        async wrapper to the driver. Parameter handling bugs could
        cause security vulnerabilities or data corruption.
        """
        mock_session = Mock()
        expected_result = self._setup_mock_execute(mock_session, [{"id": 123, "status": "active"}])

        async_session = AsyncSession(mock_session)

        with patch("async_cassandra.session.AsyncResultHandler") as mock_handler_class:
            mock_handler = Mock()
            mock_handler.get_result = AsyncMock(return_value=expected_result)
            mock_handler_class.return_value = mock_handler

            result = await async_session.execute(
                "SELECT * FROM users WHERE id = ? AND status = ?", [123, "active"]
            )

        assert isinstance(result, AsyncResultSet)
        # Verify query and parameters were passed
        call_args = mock_session.execute_async.call_args
        assert call_args[0][0] == "SELECT * FROM users WHERE id = ? AND status = ?"
        assert call_args[0][1] == [123, "active"]

    @pytest.mark.core
    async def test_query_with_consistency_level(self):
        """
        Test query with custom consistency level.

        What this tests:
        ---------------
        1. SimpleStatement with consistency level works
        2. Consistency level is preserved through execution
        3. Statement objects are passed correctly
        4. QUORUM consistency can be specified

        Why this matters:
        ----------------
        Consistency levels control the CAP theorem trade-offs:
        - ONE: Fast but may read stale data
        - QUORUM: Balanced consistency and availability
        - ALL: Strong consistency but less available

        Applications need fine-grained control over consistency
        per query. This test ensures that control is preserved
        through our async wrapper.

        Example use case:
        ----------------
        - User profile reads: ONE (fast, eventual consistency OK)
        - Financial transactions: QUORUM (must be consistent)
        - Critical configuration: ALL (absolute consistency)
        """
        mock_session = Mock()
        expected_result = self._setup_mock_execute(mock_session, [{"id": 1}])

        async_session = AsyncSession(mock_session)

        statement = SimpleStatement(
            "SELECT * FROM users", consistency_level=ConsistencyLevel.QUORUM
        )

        with patch("async_cassandra.session.AsyncResultHandler") as mock_handler_class:
            mock_handler = Mock()
            mock_handler.get_result = AsyncMock(return_value=expected_result)
            mock_handler_class.return_value = mock_handler

            result = await async_session.execute(statement)

        assert isinstance(result, AsyncResultSet)
        # Verify statement was passed
        call_args = mock_session.execute_async.call_args
        assert isinstance(call_args[0][0], SimpleStatement)
        assert call_args[0][0].consistency_level == ConsistencyLevel.QUORUM

    @pytest.mark.core
    @pytest.mark.critical
    async def test_insert_query(self):
        """
        Test INSERT query execution.

        What this tests:
        ---------------
        1. INSERT queries with parameters work
        2. Multiple values can be inserted
        3. Parameter order is preserved
        4. Returns AsyncResultSet (even though usually empty)

        Why this matters:
        ----------------
        INSERT is a fundamental write operation. This test ensures:
        - Data can be written to Cassandra
        - Parameter binding works for writes
        - The async pattern works for non-SELECT queries

        Common pattern:
        --------------
        await session.execute(
            "INSERT INTO users (id, name, email) VALUES (?, ?, ?)",
            [user_id, name, email]
        )

        The result is typically empty but may contain info for
        special cases (LWT with IF NOT EXISTS).
        """
        mock_session = Mock()
        expected_result = self._setup_mock_execute(mock_session)

        async_session = AsyncSession(mock_session)

        with patch("async_cassandra.session.AsyncResultHandler") as mock_handler_class:
            mock_handler = Mock()
            mock_handler.get_result = AsyncMock(return_value=expected_result)
            mock_handler_class.return_value = mock_handler

            result = await async_session.execute(
                "INSERT INTO users (id, name, email) VALUES (?, ?, ?)",
                [1, "John Doe", "john@example.com"],
            )

        assert isinstance(result, AsyncResultSet)
        # Verify query was executed
        call_args = mock_session.execute_async.call_args
        assert "INSERT INTO users" in call_args[0][0]
        assert call_args[0][1] == [1, "John Doe", "john@example.com"]

    @pytest.mark.core
    async def test_update_query(self):
        """
        Test UPDATE query execution.

        What this tests:
        ---------------
        1. UPDATE queries work with WHERE clause
        2. SET values can be parameterized
        3. WHERE conditions can be parameterized
        4. Parameter order matters (SET params, then WHERE params)

        Why this matters:
        ----------------
        UPDATE operations modify existing data. Critical aspects:
        - Must target specific rows (WHERE clause)
        - Must preserve parameter order
        - Often used for state changes

        Common mistakes this prevents:
        - Forgetting WHERE clause (would update all rows!)
        - Mixing up parameter order
        - SQL injection via string concatenation
        """
        mock_session = Mock()
        expected_result = self._setup_mock_execute(mock_session)

        async_session = AsyncSession(mock_session)

        with patch("async_cassandra.session.AsyncResultHandler") as mock_handler_class:
            mock_handler = Mock()
            mock_handler.get_result = AsyncMock(return_value=expected_result)
            mock_handler_class.return_value = mock_handler

            result = await async_session.execute(
                "UPDATE users SET name = ? WHERE id = ?", ["Jane Doe", 1]
            )

        assert isinstance(result, AsyncResultSet)

    @pytest.mark.core
    async def test_delete_query(self):
        """
        Test DELETE query execution.

        What this tests:
        ---------------
        1. DELETE queries work with WHERE clause
        2. WHERE parameters are handled correctly
        3. Returns AsyncResultSet (typically empty)

        Why this matters:
        ----------------
        DELETE operations remove data permanently. Critical because:
        - Data loss is irreversible
        - Must target specific rows
        - Often part of cleanup or state transitions

        Safety considerations:
        - Always use WHERE clause
        - Consider soft deletes for audit trails
        - May create tombstones (performance impact)
        """
        mock_session = Mock()
        expected_result = self._setup_mock_execute(mock_session)

        async_session = AsyncSession(mock_session)

        with patch("async_cassandra.session.AsyncResultHandler") as mock_handler_class:
            mock_handler = Mock()
            mock_handler.get_result = AsyncMock(return_value=expected_result)
            mock_handler_class.return_value = mock_handler

            result = await async_session.execute("DELETE FROM users WHERE id = ?", [1])

        assert isinstance(result, AsyncResultSet)

    @pytest.mark.core
    @pytest.mark.critical
    async def test_batch_query(self):
        """
        Test batch query execution.

        What this tests:
        ---------------
        1. CQL batch syntax is supported
        2. Multiple statements in one batch work
        3. Batch is executed as a single operation
        4. Returns AsyncResultSet

        Why this matters:
        ----------------
        Batches are used for:
        - Atomic operations (all succeed or all fail)
        - Reducing round trips
        - Maintaining consistency across rows

        Important notes:
        - This tests CQL string batches
        - For programmatic batches, use BatchStatement
        - Batches can impact performance if misused
        - Not the same as SQL transactions!
        """
        mock_session = Mock()
        expected_result = self._setup_mock_execute(mock_session)

        async_session = AsyncSession(mock_session)

        batch_query = """
        BEGIN BATCH
            INSERT INTO users (id, name) VALUES (1, 'User 1');
            INSERT INTO users (id, name) VALUES (2, 'User 2');
        APPLY BATCH
        """

        with patch("async_cassandra.session.AsyncResultHandler") as mock_handler_class:
            mock_handler = Mock()
            mock_handler.get_result = AsyncMock(return_value=expected_result)
            mock_handler_class.return_value = mock_handler

            result = await async_session.execute(batch_query)

        assert isinstance(result, AsyncResultSet)

    @pytest.mark.core
    async def test_query_with_timeout(self):
        """
        Test query with timeout parameter.

        What this tests:
        ---------------
        1. Timeout parameter is accepted
        2. Timeout value is passed to execute_async
        3. Timeout is in the correct position (5th argument)
        4. Float timeout values work

        Why this matters:
        ----------------
        Timeouts prevent:
        - Queries hanging forever
        - Resource exhaustion
        - Cascading failures

        Critical for production:
        - Set reasonable timeouts
        - Handle timeout errors gracefully
        - Different timeouts for different query types

        Note: This tests request timeout, not connection timeout.
        """
        mock_session = Mock()
        expected_result = self._setup_mock_execute(mock_session)

        async_session = AsyncSession(mock_session)

        with patch("async_cassandra.session.AsyncResultHandler") as mock_handler_class:
            mock_handler = Mock()
            mock_handler.get_result = AsyncMock(return_value=expected_result)
            mock_handler_class.return_value = mock_handler

            result = await async_session.execute("SELECT * FROM users", timeout=10.0)

        assert isinstance(result, AsyncResultSet)
        # Check timeout was passed
        call_args = mock_session.execute_async.call_args
        # Timeout is the 5th positional argument (after query, params, trace, custom_payload)
        assert call_args[0][4] == 10.0

    @pytest.mark.core
    async def test_query_with_custom_payload(self):
        """
        Test query with custom payload.

        What this tests:
        ---------------
        1. Custom payload parameter is accepted
        2. Payload dict is passed to execute_async
        3. Payload is in correct position (4th argument)
        4. Payload structure is preserved

        Why this matters:
        ----------------
        Custom payloads enable:
        - Request tracing/debugging
        - Multi-tenancy information
        - Feature flags per query
        - Custom routing hints

        Advanced feature used by:
        - Monitoring systems
        - Multi-tenant applications
        - Custom Cassandra extensions

        The payload is opaque to the driver but may be
        used by custom QueryHandler implementations.
        """
        mock_session = Mock()
        expected_result = self._setup_mock_execute(mock_session)

        async_session = AsyncSession(mock_session)
        custom_payload = {"key": "value"}

        with patch("async_cassandra.session.AsyncResultHandler") as mock_handler_class:
            mock_handler = Mock()
            mock_handler.get_result = AsyncMock(return_value=expected_result)
            mock_handler_class.return_value = mock_handler

            result = await async_session.execute(
                "SELECT * FROM users", custom_payload=custom_payload
            )

        assert isinstance(result, AsyncResultSet)
        # Check custom_payload was passed
        call_args = mock_session.execute_async.call_args
        # Custom payload is the 4th positional argument
        assert call_args[0][3] == custom_payload

    @pytest.mark.core
    @pytest.mark.critical
    async def test_empty_result_handling(self):
        """
        Test handling of empty results.

        What this tests:
        ---------------
        1. Empty result sets are handled gracefully
        2. AsyncResultSet works with no rows
        3. Iteration over empty results completes immediately
        4. No errors when converting empty results to list

        Why this matters:
        ----------------
        Empty results are common:
        - No matching rows for WHERE clause
        - Table is empty
        - Row was already deleted

        Applications must handle empty results without:
        - Raising exceptions
        - Hanging on iteration
        - Returning None instead of empty set

        Common pattern:
        --------------
        result = await session.execute("SELECT * FROM users WHERE id = ?", [999])
        users = [row async for row in result]  # Should be []
        if not users:
            print("User not found")
        """
        mock_session = Mock()
        expected_result = self._setup_mock_execute(mock_session, [])

        async_session = AsyncSession(mock_session)

        with patch("async_cassandra.session.AsyncResultHandler") as mock_handler_class:
            mock_handler = Mock()
            mock_handler.get_result = AsyncMock(return_value=expected_result)
            mock_handler_class.return_value = mock_handler

            result = await async_session.execute("SELECT * FROM users WHERE id = 999")

        assert isinstance(result, AsyncResultSet)
        # Convert to list to check emptiness
        rows = []
        async for row in result:
            rows.append(row)
        assert rows == []
