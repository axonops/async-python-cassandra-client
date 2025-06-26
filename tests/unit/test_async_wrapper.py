"""Core async wrapper functionality tests.

This module consolidates tests for the fundamental async wrapper components
including AsyncCluster, AsyncSession, and base functionality.
"""

from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import ResponseFuture

from async_cassandra import AsyncCassandraSession as AsyncSession
from async_cassandra import AsyncCluster
from async_cassandra.base import AsyncContextManageable
from async_cassandra.result import AsyncResultSet


class TestAsyncContextManageable:
    """Test the async context manager mixin functionality."""

    @pytest.mark.core
    @pytest.mark.quick
    async def test_async_context_manager(self):
        """Test basic async context manager functionality."""

        class TestClass(AsyncContextManageable):
            entered = False
            exited = False

            async def __aenter__(self):
                self.entered = True
                return self

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                self.exited = True

        async with TestClass() as obj:
            assert obj.entered
            assert not obj.exited

        assert obj.exited

    @pytest.mark.core
    async def test_context_manager_with_exception(self):
        """Test context manager handles exceptions properly."""

        class TestClass(AsyncContextManageable):
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                assert exc_type is ValueError
                assert str(exc_val) == "test error"
                return False  # Don't suppress exception

        with pytest.raises(ValueError, match="test error"):
            async with TestClass():
                raise ValueError("test error")


class TestAsyncCluster:
    """Test AsyncCluster core functionality."""

    @pytest.mark.core
    @pytest.mark.quick
    def test_init_defaults(self):
        """Test AsyncCluster initialization with default values."""
        cluster = AsyncCluster()
        # Check cluster was created with defaults
        assert cluster._cluster is not None
        assert cluster._close_lock is not None

    @pytest.mark.core
    def test_init_custom_values(self):
        """Test AsyncCluster initialization with custom values."""
        auth_provider = PlainTextAuthProvider(username="user", password="pass")
        cluster = AsyncCluster(
            contact_points=["192.168.1.1", "192.168.1.2"],
            port=9043,
            auth_provider=auth_provider,
            executor_threads=16,
        )
        # Check cluster was created
        assert cluster._cluster is not None
        assert cluster._cluster.executor._max_workers == 16

    @pytest.mark.core
    @patch("async_cassandra.cluster.Cluster", new_callable=MagicMock)
    async def test_connect(self, mock_cluster_class):
        """Test cluster connection."""
        mock_cluster = mock_cluster_class.return_value
        mock_cluster.protocol_version = 5  # Mock protocol version
        mock_session = Mock()
        mock_cluster.connect.return_value = mock_session

        cluster = AsyncCluster()
        session = await cluster.connect()

        assert isinstance(session, AsyncSession)
        assert session._session == mock_session
        mock_cluster.connect.assert_called_once()

    @pytest.mark.core
    @patch("async_cassandra.cluster.Cluster", new_callable=MagicMock)
    async def test_shutdown(self, mock_cluster_class):
        """Test cluster shutdown."""
        mock_cluster = mock_cluster_class.return_value

        cluster = AsyncCluster()
        await cluster.shutdown()

        mock_cluster.shutdown.assert_called_once()

    @pytest.mark.core
    @pytest.mark.critical
    async def test_context_manager(self):
        """Test AsyncCluster as context manager."""
        with patch("async_cassandra.cluster.Cluster") as mock_cluster_class:
            mock_cluster = mock_cluster_class.return_value

            async with AsyncCluster() as cluster:
                assert cluster._cluster == mock_cluster

            mock_cluster.shutdown.assert_called_once()


class TestAsyncSession:
    """Test AsyncSession core functionality."""

    @pytest.mark.core
    @pytest.mark.quick
    def test_init(self):
        """Test AsyncSession initialization."""
        mock_session = Mock()
        async_session = AsyncSession(mock_session)
        assert async_session._session == mock_session

    @pytest.mark.core
    @pytest.mark.critical
    async def test_execute_simple_query(self):
        """Test executing a simple query."""
        mock_session = Mock()
        mock_future = Mock(spec=ResponseFuture)
        mock_future.has_more_pages = False
        mock_session.execute_async.return_value = mock_future

        async_session = AsyncSession(mock_session)

        # Patch AsyncResultHandler to simulate immediate result
        with patch("async_cassandra.session.AsyncResultHandler") as mock_handler_class:
            mock_handler = Mock()
            mock_result = AsyncResultSet([{"id": 1, "name": "test"}])
            mock_handler.get_result = AsyncMock(return_value=mock_result)
            mock_handler_class.return_value = mock_handler

            result = await async_session.execute("SELECT * FROM users")

        assert isinstance(result, AsyncResultSet)
        mock_session.execute_async.assert_called_once()

    @pytest.mark.core
    async def test_execute_with_parameters(self):
        """Test executing query with parameters."""
        mock_session = Mock()
        mock_future = Mock(spec=ResponseFuture)
        mock_session.execute_async.return_value = mock_future

        async_session = AsyncSession(mock_session)

        with patch("async_cassandra.session.AsyncResultHandler") as mock_handler_class:
            mock_handler = Mock()
            mock_result = AsyncResultSet([])
            mock_handler.get_result = AsyncMock(return_value=mock_result)
            mock_handler_class.return_value = mock_handler

            await async_session.execute("SELECT * FROM users WHERE id = ?", [123])

        # Verify parameters were passed
        call_args = mock_session.execute_async.call_args
        assert call_args[0][0] == "SELECT * FROM users WHERE id = ?"
        assert call_args[0][1] == [123]

    @pytest.mark.core
    async def test_prepare(self):
        """Test preparing statements."""
        mock_session = Mock()
        mock_prepared = Mock()
        mock_session.prepare.return_value = mock_prepared

        async_session = AsyncSession(mock_session)
        prepared = await async_session.prepare("SELECT * FROM users WHERE id = ?")

        assert prepared == mock_prepared
        mock_session.prepare.assert_called_once_with("SELECT * FROM users WHERE id = ?", None)

    @pytest.mark.core
    async def test_close(self):
        """Test closing session."""
        mock_session = Mock()
        async_session = AsyncSession(mock_session)

        await async_session.close()
        mock_session.shutdown.assert_called_once()

    @pytest.mark.core
    @pytest.mark.critical
    async def test_context_manager(self):
        """Test AsyncSession as context manager."""
        mock_session = Mock()

        async with AsyncSession(mock_session) as session:
            assert session._session == mock_session

        mock_session.shutdown.assert_called_once()

    @pytest.mark.core
    async def test_set_keyspace(self):
        """Test setting keyspace."""
        mock_session = Mock()
        mock_future = Mock(spec=ResponseFuture)
        mock_session.execute_async.return_value = mock_future

        async_session = AsyncSession(mock_session)

        with patch("async_cassandra.session.AsyncResultHandler") as mock_handler_class:
            mock_handler = Mock()
            mock_result = AsyncResultSet([])
            mock_handler.get_result = AsyncMock(return_value=mock_result)
            mock_handler_class.return_value = mock_handler

            await async_session.set_keyspace("test_keyspace")

        # Check that execute_async was called with the USE query
        call_args = mock_session.execute_async.call_args
        assert call_args[0][0] == "USE test_keyspace"
