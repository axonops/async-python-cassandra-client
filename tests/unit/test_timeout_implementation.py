"""
Test timeout implementation for async operations.
"""

import asyncio
from unittest.mock import AsyncMock, Mock, patch

import pytest

from async_cassandra.cluster import AsyncCluster
from async_cassandra.session import AsyncCassandraSession
from async_cassandra.streaming import AsyncStreamingResultSet, StreamConfig


class TestClusterTimeouts:
    """Test timeout handling in AsyncCluster."""

    @pytest.mark.asyncio
    async def test_connect_with_timeout(self):
        """Test that connect respects timeout parameter."""
        cluster = AsyncCluster(["127.0.0.1"])

        # Mock AsyncCassandraSession.create to take too long
        async def slow_create(*args):
            await asyncio.sleep(5)  # Longer than timeout
            return Mock()

        with patch.object(AsyncCassandraSession, "create", side_effect=slow_create):
            with pytest.raises(asyncio.TimeoutError):
                await cluster.connect(timeout=0.1)

    @pytest.mark.asyncio
    async def test_connect_default_timeout(self):
        """Test that connect uses default timeout when not specified."""
        cluster = AsyncCluster(["127.0.0.1"])

        # Mock successful connection
        mock_session = Mock()
        with patch.object(AsyncCassandraSession, "create", return_value=mock_session):
            # This should complete quickly (mocked)
            session = await cluster.connect()
            assert session == mock_session

    @pytest.mark.asyncio
    async def test_shutdown_timeout(self):
        """Test that shutdown has a reasonable timeout."""
        cluster = AsyncCluster(["127.0.0.1"])

        # Mock the underlying cluster shutdown to hang
        def slow_shutdown():
            import time

            time.sleep(2)  # Longer than 1s timeout

        cluster._cluster.shutdown = slow_shutdown

        # Should timeout after 1 second
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(cluster.close(), timeout=1)


class TestSessionTimeouts:
    """Test timeout handling in AsyncCassandraSession."""

    @pytest.fixture
    def mock_session(self):
        """Create a mock session."""
        session = Mock()
        session._session = Mock()
        session._session.cluster = Mock()
        session.execute = AsyncMock()
        return session

    @pytest.mark.asyncio
    async def test_prepare_with_timeout(self, mock_session):
        """Test that prepare respects timeout parameter."""
        async_session = AsyncCassandraSession(mock_session._session)

        # Mock prepare to take too long
        def slow_prepare(*args):
            import time

            time.sleep(5)  # Longer than timeout
            return Mock()

        mock_session._session.prepare = slow_prepare
        async_session._session = mock_session._session

        with pytest.raises(asyncio.TimeoutError):
            await async_session.prepare("SELECT * FROM test", timeout=0.1)

    @pytest.mark.asyncio
    async def test_prepare_default_timeout(self, mock_session):
        """Test that prepare uses default timeout when not specified."""
        async_session = AsyncCassandraSession(mock_session._session)

        # Mock successful prepare
        mock_prepared = Mock()
        mock_session._session.prepare = Mock(return_value=mock_prepared)
        async_session._session = mock_session._session

        # This should complete quickly (mocked)
        prepared = await async_session.prepare("SELECT * FROM test")
        assert prepared == mock_prepared

    @pytest.mark.asyncio
    async def test_session_shutdown_timeout(self, mock_session):
        """Test that session shutdown has a reasonable timeout."""
        async_session = AsyncCassandraSession(mock_session._session)

        # Mock the session shutdown to hang
        def slow_shutdown():
            import time

            time.sleep(2)  # Longer than 1s timeout

        mock_session._session.shutdown = slow_shutdown
        async_session._session = mock_session._session

        # Should timeout after 1 second
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(async_session.close(), timeout=1)


class TestStreamingTimeouts:
    """Test timeout handling in streaming operations."""

    @pytest.fixture
    def mock_response_future(self):
        """Create a mock response future."""
        future = Mock()
        future.has_more_pages = True
        future.clear_callbacks = Mock()
        future.start_fetching_next_page = Mock()
        return future

    @pytest.mark.asyncio
    async def test_streaming_with_timeout_config(self, mock_response_future):
        """Test that streaming respects timeout in config."""
        # Mock has_more_pages to be True
        mock_response_future.has_more_pages = True
        mock_response_future._final_exception = None

        # Mock add_callbacks to do nothing
        mock_response_future.add_callbacks = Mock()

        # Mock start_fetching_next_page to do nothing (no callback will be called)
        mock_response_future.start_fetching_next_page = Mock()

        config = StreamConfig(timeout_seconds=0.1)
        stream = AsyncStreamingResultSet(mock_response_future, config)

        # Should timeout waiting for page
        with pytest.raises(asyncio.TimeoutError):
            await stream._fetch_next_page()

    @pytest.mark.asyncio
    async def test_streaming_without_timeout_config(self, mock_response_future):
        """Test that streaming works without timeout when not configured."""
        config = StreamConfig()  # No timeout
        stream = AsyncStreamingResultSet(mock_response_future, config)

        # Create page ready event
        stream._page_ready = asyncio.Event()
        stream._loop = asyncio.get_running_loop()

        # Set the event after a short delay
        async def set_event():
            await asyncio.sleep(0.1)
            stream._page_ready.set()
            stream._exhausted = True

        task = asyncio.create_task(set_event())

        # Should complete without timeout
        result = await stream._fetch_next_page()
        assert result is False  # No more pages

        # Clean up the task
        await task

    @pytest.mark.asyncio
    async def test_streaming_iteration_timeout(self, mock_response_future):
        """Test that streaming iteration respects timeout."""
        config = StreamConfig(timeout_seconds=0.1)
        stream = AsyncStreamingResultSet(mock_response_future, config)

        # Mock page data
        stream._first_page_ready = False
        stream._page_ready = asyncio.Event()
        stream._loop = asyncio.get_running_loop()

        # Never set the page ready event

        # Should timeout during iteration
        with pytest.raises(asyncio.TimeoutError):
            async for row in stream:
                pass  # Should never get here

    @pytest.mark.asyncio
    async def test_streaming_pages_timeout(self, mock_response_future):
        """Test that streaming pages iteration respects timeout."""
        config = StreamConfig(timeout_seconds=0.1)
        stream = AsyncStreamingResultSet(mock_response_future, config)

        # Mock initial state
        stream._first_page_ready = False
        stream._page_ready = asyncio.Event()
        stream._loop = asyncio.get_running_loop()

        # Never set the page ready event

        # Should timeout during pages iteration
        with pytest.raises(asyncio.TimeoutError):
            async for page in stream.pages():
                pass  # Should never get here


class TestTimeoutConstants:
    """Test that timeout constants are properly used."""

    def test_default_constants_exist(self):
        """Test that default timeout constants are defined."""
        from async_cassandra.constants import DEFAULT_CONNECTION_TIMEOUT, DEFAULT_REQUEST_TIMEOUT

        assert DEFAULT_CONNECTION_TIMEOUT == 30.0  # Increased for larger heap sizes
        assert DEFAULT_REQUEST_TIMEOUT == 120.0

    @pytest.mark.asyncio
    async def test_connect_uses_default_constant(self):
        """Test that connect uses DEFAULT_CONNECTION_TIMEOUT."""
        cluster = AsyncCluster(["127.0.0.1"])

        # Mock to track timeout value
        create_called_with_timeout = None

        async def mock_create(*args):
            return Mock()

        async def mock_wait_for(coro, timeout):
            nonlocal create_called_with_timeout
            create_called_with_timeout = timeout
            return await coro

        with patch.object(AsyncCassandraSession, "create", side_effect=mock_create):
            with patch("asyncio.wait_for", side_effect=mock_wait_for):
                await cluster.connect()

        # Should use default timeout
        assert create_called_with_timeout == 30.0  # Increased for larger heap sizes

    @pytest.mark.asyncio
    async def test_prepare_uses_default_constant(self):
        """Test that prepare uses DEFAULT_REQUEST_TIMEOUT."""
        mock_session = Mock()
        mock_session.prepare = Mock(return_value=Mock())
        async_session = AsyncCassandraSession(mock_session)
        async_session._session = mock_session

        # Mock to track timeout value
        prepare_called_with_timeout = None

        async def mock_wait_for(coro, timeout):
            nonlocal prepare_called_with_timeout
            prepare_called_with_timeout = timeout
            return await coro

        with patch("asyncio.wait_for", side_effect=mock_wait_for):
            await async_session.prepare("SELECT * FROM test")

        # Should use default timeout
        assert prepare_called_with_timeout == 120.0
