"""Core basic query execution tests.

This module tests fundamental query operations that must work
for the async wrapper to be functional.
"""

from unittest.mock import AsyncMock, Mock, patch

import pytest
from cassandra import ConsistencyLevel
from cassandra.cluster import ResponseFuture
from cassandra.query import SimpleStatement

from async_cassandra import AsyncCassandraSession as AsyncSession
from async_cassandra.result import AsyncResultSet


class TestBasicQueryExecution:
    """Test basic query execution patterns."""

    def _setup_mock_execute(self, mock_session, result_data=None):
        """Helper to setup mock execute_async with proper response."""
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
        """Test basic SELECT query execution."""
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
        """Test query with bound parameters."""
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
        """Test query with custom consistency level."""
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
        """Test INSERT query execution."""
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
        """Test UPDATE query execution."""
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
        """Test DELETE query execution."""
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
        """Test batch query execution."""
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
        """Test query with timeout parameter."""
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
        """Test query with custom payload."""
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
        """Test handling of empty results."""
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
