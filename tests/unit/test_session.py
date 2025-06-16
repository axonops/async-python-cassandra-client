"""
Unit tests for async session management.
"""

from unittest.mock import AsyncMock, Mock, patch

import pytest
from cassandra.cluster import ResponseFuture, Session
from cassandra.query import PreparedStatement

from async_cassandra.exceptions import ConnectionError, QueryError
from async_cassandra.result import AsyncResultSet
from async_cassandra.session import AsyncCassandraSession


class TestAsyncCassandraSession:
    """Test cases for AsyncCassandraSession."""

    @pytest.fixture
    def mock_session(self):
        """Create a mock Cassandra session."""
        session = Mock(spec=Session)
        session.keyspace = "test_keyspace"
        session.shutdown = Mock()
        return session

    @pytest.fixture
    def async_session(self, mock_session):
        """Create an AsyncCassandraSession instance."""
        return AsyncCassandraSession(mock_session)

    @pytest.mark.asyncio
    async def test_create_session(self):
        """Test creating a session from cluster."""
        mock_cluster = Mock()
        mock_session = Mock(spec=Session)
        mock_cluster.connect.return_value = mock_session

        async_session = await AsyncCassandraSession.create(mock_cluster, "test_keyspace")

        assert isinstance(async_session, AsyncCassandraSession)
        mock_cluster.connect.assert_called_once_with("test_keyspace")

    @pytest.mark.asyncio
    async def test_create_session_without_keyspace(self):
        """Test creating a session without keyspace."""
        mock_cluster = Mock()
        mock_session = Mock(spec=Session)
        mock_cluster.connect.return_value = mock_session

        async_session = await AsyncCassandraSession.create(mock_cluster)

        assert isinstance(async_session, AsyncCassandraSession)
        mock_cluster.connect.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_execute_simple_query(self, async_session, mock_session):
        """Test executing a simple query."""
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
        """Test executing query with parameters."""
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

        # Verify parameters were passed
        call_args = mock_session.execute_async.call_args
        assert call_args[0][0] == query
        assert call_args[0][1] == params

    @pytest.mark.asyncio
    async def test_execute_query_error(self, async_session, mock_session):
        """Test handling query execution error."""
        mock_session.execute_async.side_effect = Exception("Connection failed")

        with pytest.raises(QueryError) as exc_info:
            await async_session.execute("SELECT * FROM users")

        assert "Query execution failed" in str(exc_info.value)
        assert exc_info.value.__cause__ is not None

    @pytest.mark.asyncio
    async def test_execute_on_closed_session(self, async_session):
        """Test executing query on closed session."""
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
