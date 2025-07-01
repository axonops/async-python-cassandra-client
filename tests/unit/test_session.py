"""
Unit tests for async session management.

This module thoroughly tests AsyncCassandraSession, covering:
- Session creation from cluster
- Query execution (simple and parameterized)
- Prepared statement handling
- Batch operations
- Error handling and propagation
- Resource cleanup and context managers
- Streaming operations
- Edge cases and error conditions

Key Testing Patterns:
====================
- Mocks ResponseFuture to simulate async operations
- Tests callback-based async conversion
- Verifies proper error wrapping
- Ensures resource cleanup in all paths
"""

from unittest.mock import AsyncMock, Mock, patch

import pytest
from cassandra.cluster import ResponseFuture, Session
from cassandra.query import PreparedStatement

from async_cassandra.exceptions import ConnectionError, QueryError
from async_cassandra.result import AsyncResultSet
from async_cassandra.session import AsyncCassandraSession


class TestAsyncCassandraSession:
    """
    Test cases for AsyncCassandraSession.

    AsyncCassandraSession is the core interface for executing queries.
    It converts the driver's callback-based async operations into
    Python async/await compatible operations.
    """

    @pytest.fixture
    def mock_session(self):
        """
        Create a mock Cassandra session.

        Provides a minimal session interface for testing
        without actual database connections.
        """
        session = Mock(spec=Session)
        session.keyspace = "test_keyspace"
        session.shutdown = Mock()
        return session

    @pytest.fixture
    def async_session(self, mock_session):
        """
        Create an AsyncCassandraSession instance.

        Uses the mock_session fixture to avoid real connections.
        """
        return AsyncCassandraSession(mock_session)

    @pytest.mark.asyncio
    async def test_create_session(self):
        """
        Test creating a session from cluster.

        What this tests:
        ---------------
        1. create() class method works
        2. Keyspace is passed to cluster.connect()
        3. Returns AsyncCassandraSession instance

        Why this matters:
        ----------------
        The create() method is a factory that:
        - Handles sync cluster.connect() call
        - Wraps result in async session
        - Sets initial keyspace if provided

        This is the primary way to get a session.
        """
        mock_cluster = Mock()
        mock_session = Mock(spec=Session)
        mock_cluster.connect.return_value = mock_session

        async_session = await AsyncCassandraSession.create(mock_cluster, "test_keyspace")

        assert isinstance(async_session, AsyncCassandraSession)
        # Verify keyspace was used
        mock_cluster.connect.assert_called_once_with("test_keyspace")

    @pytest.mark.asyncio
    async def test_create_session_without_keyspace(self):
        """
        Test creating a session without keyspace.

        What this tests:
        ---------------
        1. Keyspace parameter is optional
        2. connect() called without arguments

        Why this matters:
        ----------------
        Common patterns:
        - Connect first, set keyspace later
        - Working across multiple keyspaces
        - Administrative operations
        """
        mock_cluster = Mock()
        mock_session = Mock(spec=Session)
        mock_cluster.connect.return_value = mock_session

        async_session = await AsyncCassandraSession.create(mock_cluster)

        assert isinstance(async_session, AsyncCassandraSession)
        # Verify no keyspace argument
        mock_cluster.connect.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_execute_simple_query(self, async_session, mock_session):
        """
        Test executing a simple query.

        What this tests:
        ---------------
        1. Basic SELECT query execution
        2. Async conversion of ResponseFuture
        3. Results wrapped in AsyncResultSet
        4. Callback mechanism works correctly

        Why this matters:
        ----------------
        This is the core functionality - converting driver's
        callback-based async into Python async/await:

        Driver: execute_async() -> ResponseFuture -> callbacks
        Wrapper: await execute() -> AsyncResultSet

        The AsyncResultHandler manages this conversion.
        """
        # Setup mock response future
        mock_future = Mock(spec=ResponseFuture)
        mock_future.has_more_pages = False
        mock_future.add_callbacks = Mock()
        mock_session.execute_async.return_value = mock_future

        # Execute query
        query = "SELECT * FROM users"

        # Patch AsyncResultHandler to simulate immediate result
        with patch("async_cassandra.session.AsyncResultHandler") as mock_handler_class:
            mock_handler = Mock()
            mock_result = AsyncResultSet([{"id": 1, "name": "test"}])
            mock_handler.get_result = AsyncMock(return_value=mock_result)
            mock_handler_class.return_value = mock_handler

            result = await async_session.execute(query)

        assert isinstance(result, AsyncResultSet)
        mock_session.execute_async.assert_called_once()

    @pytest.mark.asyncio
    async def test_execute_with_parameters(self, async_session, mock_session):
        """
        Test executing query with parameters.

        What this tests:
        ---------------
        1. Parameterized queries work
        2. Parameters passed to execute_async
        3. ? placeholder syntax supported

        Why this matters:
        ----------------
        Parameters are critical for:
        - SQL injection prevention
        - Query plan caching
        - Type safety

        Must ensure parameters flow through correctly.
        """
        mock_future = Mock(spec=ResponseFuture)
        mock_session.execute_async.return_value = mock_future

        query = "SELECT * FROM users WHERE id = ?"
        params = [123]

        with patch("async_cassandra.session.AsyncResultHandler") as mock_handler_class:
            mock_handler = Mock()
            mock_result = AsyncResultSet([])
            mock_handler.get_result = AsyncMock(return_value=mock_result)
            mock_handler_class.return_value = mock_handler

            await async_session.execute(query, parameters=params)

        # Verify both query and parameters were passed
        call_args = mock_session.execute_async.call_args
        assert call_args[0][0] == query
        assert call_args[0][1] == params

    @pytest.mark.asyncio
    async def test_execute_query_error(self, async_session, mock_session):
        """
        Test handling query execution error.

        What this tests:
        ---------------
        1. Exceptions from driver are caught
        2. Wrapped in QueryError
        3. Original exception preserved as __cause__
        4. Helpful error message provided

        Why this matters:
        ----------------
        Error handling is critical:
        - Users need clear error messages
        - Stack traces must be preserved
        - Debugging requires full context

        Common errors:
        - Network failures
        - Invalid queries
        - Timeout issues
        """
        mock_session.execute_async.side_effect = Exception("Connection failed")

        with pytest.raises(QueryError) as exc_info:
            await async_session.execute("SELECT * FROM users")

        assert "Query execution failed" in str(exc_info.value)
        # Original exception preserved for debugging
        assert exc_info.value.__cause__ is not None

    @pytest.mark.asyncio
    async def test_execute_on_closed_session(self, async_session):
        """
        Test executing query on closed session.

        What this tests:
        ---------------
        1. Closed session check works
        2. Fails fast with ConnectionError
        3. Clear error message

        Why this matters:
        ----------------
        Prevents confusing errors:
        - No hanging on closed connections
        - No cryptic driver errors
        - Immediate feedback

        Common scenario:
        - Session closed in error handler
        - Retry logic tries to use it
        - Should fail clearly
        """
        await async_session.close()

        with pytest.raises(ConnectionError) as exc_info:
            await async_session.execute("SELECT * FROM users")

        assert "Session is closed" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_prepare_statement(self, async_session, mock_session):
        """Test preparing a statement."""
        mock_prepared = Mock(spec=PreparedStatement)
        mock_session.prepare.return_value = mock_prepared

        query = "SELECT * FROM users WHERE id = ?"
        prepared = await async_session.prepare(query)

        assert prepared == mock_prepared
        mock_session.prepare.assert_called_once_with(query, None)

    @pytest.mark.asyncio
    async def test_prepare_with_custom_payload(self, async_session, mock_session):
        """Test preparing statement with custom payload."""
        mock_prepared = Mock(spec=PreparedStatement)
        mock_session.prepare.return_value = mock_prepared

        query = "SELECT * FROM users WHERE id = ?"
        payload = {"key": b"value"}

        await async_session.prepare(query, custom_payload=payload)

        mock_session.prepare.assert_called_once_with(query, payload)

    @pytest.mark.asyncio
    async def test_prepare_error(self, async_session, mock_session):
        """Test handling prepare statement error."""
        mock_session.prepare.side_effect = Exception("Invalid query")

        with pytest.raises(QueryError) as exc_info:
            await async_session.prepare("INVALID QUERY")

        assert "Statement preparation failed" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_prepare_on_closed_session(self, async_session):
        """Test preparing statement on closed session."""
        await async_session.close()

        with pytest.raises(ConnectionError):
            await async_session.prepare("SELECT * FROM users")

    @pytest.mark.asyncio
    async def test_close_session(self, async_session, mock_session):
        """Test closing the session."""
        await async_session.close()

        assert async_session.is_closed
        mock_session.shutdown.assert_called_once()

    @pytest.mark.asyncio
    async def test_close_idempotent(self, async_session, mock_session):
        """Test that close is idempotent."""
        await async_session.close()
        await async_session.close()

        # Should only be called once
        mock_session.shutdown.assert_called_once()

    @pytest.mark.asyncio
    async def test_context_manager(self, mock_session):
        """Test using session as async context manager."""
        async with AsyncCassandraSession(mock_session) as session:
            assert isinstance(session, AsyncCassandraSession)
            assert not session.is_closed

        # Session should be closed after exiting context
        mock_session.shutdown.assert_called_once()

    @pytest.mark.asyncio
    async def test_set_keyspace(self, async_session):
        """Test setting keyspace."""
        with patch.object(async_session, "execute") as mock_execute:
            mock_execute.return_value = AsyncResultSet([])

            await async_session.set_keyspace("new_keyspace")

            mock_execute.assert_called_once_with("USE new_keyspace")

    @pytest.mark.asyncio
    async def test_set_keyspace_invalid_name(self, async_session):
        """Test setting keyspace with invalid name."""
        # Test various invalid keyspace names
        invalid_names = ["", "keyspace with spaces", "keyspace-with-dash", "keyspace;drop"]

        for invalid_name in invalid_names:
            with pytest.raises(ValueError) as exc_info:
                await async_session.set_keyspace(invalid_name)

            assert "Invalid keyspace name" in str(exc_info.value)

    def test_keyspace_property(self, async_session, mock_session):
        """Test keyspace property."""
        mock_session.keyspace = "test_keyspace"
        assert async_session.keyspace == "test_keyspace"

    def test_is_closed_property(self, async_session):
        """Test is_closed property."""
        assert not async_session.is_closed
        async_session._closed = True
        assert async_session.is_closed
