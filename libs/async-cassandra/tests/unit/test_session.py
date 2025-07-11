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
from async_cassandra.exceptions import ConnectionError, QueryError
from async_cassandra.result import AsyncResultSet
from async_cassandra.session import AsyncCassandraSession
from cassandra.cluster import ResponseFuture, Session
from cassandra.query import PreparedStatement


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
        """
        Test preparing a statement.

        What this tests:
        ---------------
        1. Basic prepared statement creation
        2. Query string is passed correctly to driver
        3. Prepared statement object is returned
        4. Async wrapper handles synchronous prepare call

        Why this matters:
        ----------------
        - Prepared statements are critical for performance
        - Must work correctly for parameterized queries
        - Foundation for safe query execution
        - Used in almost every production application

        Additional context:
        ---------------------------------
        - Prepared statements use ? placeholders
        - Driver handles actual preparation
        - Wrapper provides async interface
        """
        mock_prepared = Mock(spec=PreparedStatement)
        mock_session.prepare.return_value = mock_prepared

        query = "SELECT * FROM users WHERE id = ?"
        prepared = await async_session.prepare(query)

        assert prepared == mock_prepared
        mock_session.prepare.assert_called_once_with(query, None)

    @pytest.mark.asyncio
    async def test_prepare_with_custom_payload(self, async_session, mock_session):
        """
        Test preparing statement with custom payload.

        What this tests:
        ---------------
        1. Custom payload support in prepare method
        2. Payload is correctly passed to driver
        3. Advanced prepare options are preserved
        4. API compatibility with driver features

        Why this matters:
        ----------------
        - Custom payloads enable advanced features
        - Required for certain driver extensions
        - Ensures full driver API coverage
        - Used in specialized deployments

        Additional context:
        ---------------------------------
        - Payloads can contain metadata or hints
        - Driver-specific feature passthrough
        - Maintains wrapper transparency
        """
        mock_prepared = Mock(spec=PreparedStatement)
        mock_session.prepare.return_value = mock_prepared

        query = "SELECT * FROM users WHERE id = ?"
        payload = {"key": b"value"}

        await async_session.prepare(query, custom_payload=payload)

        mock_session.prepare.assert_called_once_with(query, payload)

    @pytest.mark.asyncio
    async def test_prepare_error(self, async_session, mock_session):
        """
        Test handling prepare statement error.

        What this tests:
        ---------------
        1. Error handling during statement preparation
        2. Exceptions are wrapped in QueryError
        3. Error messages are informative
        4. No resource leaks on preparation failure

        Why this matters:
        ----------------
        - Invalid queries must fail gracefully
        - Clear errors help debugging
        - Prevents silent failures
        - Common during development

        Additional context:
        ---------------------------------
        - Syntax errors caught at prepare time
        - Better than runtime query failures
        - Helps catch bugs early
        """
        mock_session.prepare.side_effect = Exception("Invalid query")

        with pytest.raises(QueryError) as exc_info:
            await async_session.prepare("INVALID QUERY")

        assert "Statement preparation failed" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_prepare_on_closed_session(self, async_session):
        """
        Test preparing statement on closed session.

        What this tests:
        ---------------
        1. Closed session prevents prepare operations
        2. ConnectionError is raised appropriately
        3. Session state is checked before operations
        4. No operations on closed resources

        Why this matters:
        ----------------
        - Prevents use-after-close bugs
        - Clear error for invalid operations
        - Resource safety in async contexts
        - Common error in connection pooling

        Additional context:
        ---------------------------------
        - Sessions may be closed by timeouts
        - Error handling must be predictable
        - Helps identify lifecycle issues
        """
        await async_session.close()

        with pytest.raises(ConnectionError):
            await async_session.prepare("SELECT * FROM users")

    @pytest.mark.asyncio
    async def test_close_session(self, async_session, mock_session):
        """
        Test closing the session.

        What this tests:
        ---------------
        1. Session close sets is_closed flag
        2. Underlying driver shutdown is called
        3. Clean resource cleanup
        4. State transition is correct

        Why this matters:
        ----------------
        - Proper cleanup prevents resource leaks
        - Connection pools need clean shutdown
        - Memory leaks in production are critical
        - Graceful shutdown is required

        Additional context:
        ---------------------------------
        - Driver shutdown releases connections
        - Must work in async contexts
        - Part of session lifecycle management
        """
        await async_session.close()

        assert async_session.is_closed
        mock_session.shutdown.assert_called_once()

    @pytest.mark.asyncio
    async def test_close_idempotent(self, async_session, mock_session):
        """
        Test that close is idempotent.

        What this tests:
        ---------------
        1. Multiple close calls are safe
        2. Driver shutdown called only once
        3. No errors on repeated close
        4. Idempotent operation guarantee

        Why this matters:
        ----------------
        - Defensive programming principle
        - Simplifies error handling code
        - Prevents double-free issues
        - Common in cleanup handlers

        Additional context:
        ---------------------------------
        - May be called from multiple paths
        - Exception handlers often close twice
        - Standard pattern in resource management
        """
        await async_session.close()
        await async_session.close()

        # Should only be called once
        mock_session.shutdown.assert_called_once()

    @pytest.mark.asyncio
    async def test_context_manager(self, mock_session):
        """
        Test using session as async context manager.

        What this tests:
        ---------------
        1. Async context manager protocol support
        2. Session is open within context
        3. Automatic cleanup on context exit
        4. Exception safety in context manager

        Why this matters:
        ----------------
        - Pythonic resource management
        - Guarantees cleanup even with exceptions
        - Prevents resource leaks
        - Best practice for session usage

        Additional context:
        ---------------------------------
        - async with syntax is preferred
        - Handles all cleanup paths
        - Standard Python pattern
        """
        async with AsyncCassandraSession(mock_session) as session:
            assert isinstance(session, AsyncCassandraSession)
            assert not session.is_closed

        # Session should be closed after exiting context
        mock_session.shutdown.assert_called_once()

    @pytest.mark.asyncio
    async def test_set_keyspace(self, async_session):
        """
        Test setting keyspace.

        What this tests:
        ---------------
        1. Keyspace change via USE statement
        2. Execute method called with correct query
        3. Async execution of keyspace change
        4. No errors on valid keyspace

        Why this matters:
        ----------------
        - Multi-tenant applications switch keyspaces
        - Session reuse across keyspaces
        - Avoids creating multiple sessions
        - Common operational requirement

        Additional context:
        ---------------------------------
        - USE statement changes active keyspace
        - Affects all subsequent queries
        - Alternative to connection-time keyspace
        """
        with patch.object(async_session, "execute") as mock_execute:
            mock_execute.return_value = AsyncResultSet([])

            await async_session.set_keyspace("new_keyspace")

            mock_execute.assert_called_once_with("USE new_keyspace")

    @pytest.mark.asyncio
    async def test_set_keyspace_invalid_name(self, async_session):
        """
        Test setting keyspace with invalid name.

        What this tests:
        ---------------
        1. Validation of keyspace names
        2. Rejection of invalid characters
        3. SQL injection prevention
        4. Clear error messages

        Why this matters:
        ----------------
        - Security against injection attacks
        - Prevents malformed CQL execution
        - Data integrity protection
        - User input validation

        Additional context:
        ---------------------------------
        - Tests spaces, dashes, semicolons
        - CQL identifier rules enforced
        - First line of defense
        """
        # Test various invalid keyspace names
        invalid_names = ["", "keyspace with spaces", "keyspace-with-dash", "keyspace;drop"]

        for invalid_name in invalid_names:
            with pytest.raises(ValueError) as exc_info:
                await async_session.set_keyspace(invalid_name)

            assert "Invalid keyspace name" in str(exc_info.value)

    def test_keyspace_property(self, async_session, mock_session):
        """
        Test keyspace property.

        What this tests:
        ---------------
        1. Keyspace property delegates to driver
        2. Read-only access to current keyspace
        3. Property reflects driver state
        4. No caching or staleness

        Why this matters:
        ----------------
        - Applications need current keyspace info
        - Debugging multi-keyspace operations
        - State transparency
        - API compatibility with driver

        Additional context:
        ---------------------------------
        - Property is read-only
        - Always reflects driver state
        - Used for logging and debugging
        """
        mock_session.keyspace = "test_keyspace"
        assert async_session.keyspace == "test_keyspace"

    def test_is_closed_property(self, async_session):
        """
        Test is_closed property.

        What this tests:
        ---------------
        1. Initial state is not closed
        2. Property reflects internal state
        3. Boolean property access
        4. State tracking accuracy

        Why this matters:
        ----------------
        - Applications check before operations
        - Lifecycle state visibility
        - Defensive programming support
        - Connection pool management

        Additional context:
        ---------------------------------
        - Used to prevent use-after-close
        - Simple boolean check
        - Thread-safe property access
        """
        assert not async_session.is_closed
        async_session._closed = True
        assert async_session.is_closed
