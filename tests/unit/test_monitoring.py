"""
Simple unit tests for monitoring module - tests core functionality only.
"""

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, Mock

import pytest

from async_cassandra.monitoring import (
    HOST_STATUS_UP,
    ClusterMetrics,
    ConnectionMonitor,
    HostMetrics,
    RateLimitedSession,
)


class TestHostMetrics:
    """Test HostMetrics dataclass."""

    def test_host_metrics_creation(self):
        """Test creating HostMetrics instance."""
        metrics = HostMetrics(
            address="192.168.1.1",
            datacenter="dc1",
            rack="rack1",
            status=HOST_STATUS_UP,
            release_version="4.0.0",
            connection_count=1,
            latency_ms=15.5,
            last_error=None,
            last_check=datetime.now(timezone.utc),
        )

        assert metrics.address == "192.168.1.1"
        assert metrics.datacenter == "dc1"
        assert metrics.rack == "rack1"
        assert metrics.status == HOST_STATUS_UP
        assert metrics.release_version == "4.0.0"
        assert metrics.connection_count == 1
        assert metrics.latency_ms == 15.5


class TestConnectionMonitor:
    """Test ConnectionMonitor basic functionality."""

    @pytest.fixture
    def mock_session(self):
        """Create a mock session."""
        session = Mock()
        session._session = Mock()
        session._session.cluster = Mock()
        session._session.cluster.metadata = Mock()
        session._session.cluster.metadata.cluster_name = "TestCluster"
        session._session.cluster.protocol_version = 5
        session._session.cluster.metadata.all_hosts = Mock(return_value=[])
        return session

    @pytest.mark.asyncio
    async def test_monitor_creation(self, mock_session):
        """Test creating ConnectionMonitor."""
        monitor = ConnectionMonitor(mock_session)

        assert monitor.session == mock_session
        assert isinstance(monitor.metrics, dict)
        assert monitor.metrics["monitoring_started"] is not None

    @pytest.mark.asyncio
    async def test_get_cluster_metrics_empty(self, mock_session):
        """Test getting cluster metrics with no hosts."""
        monitor = ConnectionMonitor(mock_session)

        metrics = await monitor.get_cluster_metrics()

        assert isinstance(metrics, ClusterMetrics)
        assert metrics.cluster_name == "TestCluster"
        assert metrics.protocol_version == 5
        assert len(metrics.hosts) == 0
        assert metrics.total_connections == 0


class TestRateLimitedSession:
    """Test RateLimitedSession basic functionality."""

    @pytest.fixture
    def mock_session(self):
        """Create a mock async session."""
        session = Mock()
        session.execute = AsyncMock(return_value="result")
        session.prepare = AsyncMock(return_value="prepared")
        return session

    @pytest.mark.asyncio
    async def test_rate_limited_creation(self, mock_session):
        """Test creating RateLimitedSession."""
        rate_limited = RateLimitedSession(mock_session, max_concurrent=10)

        assert rate_limited.session == mock_session
        assert rate_limited.semaphore._value == 10
        assert rate_limited.metrics["total_requests"] == 0

    @pytest.mark.asyncio
    async def test_execute_with_rate_limiting(self, mock_session):
        """Test execute method with rate limiting."""
        rate_limited = RateLimitedSession(mock_session, max_concurrent=2)

        # Execute a query
        result = await rate_limited.execute("SELECT * FROM test")

        assert result == "result"
        assert rate_limited.metrics["total_requests"] == 1
        mock_session.execute.assert_called_once_with("SELECT * FROM test", None)

    @pytest.mark.asyncio
    async def test_concurrent_requests_limited(self, mock_session):
        """Test that concurrent requests are limited."""
        rate_limited = RateLimitedSession(mock_session, max_concurrent=2)

        # Make the mock execute method slow
        async def slow_execute(*args, **kwargs):
            await asyncio.sleep(0.1)
            return "result"

        mock_session.execute = slow_execute

        # Track active requests
        active_requests = []

        async def track_and_execute():
            active_requests.append(rate_limited.metrics["active_requests"])
            result = await rate_limited.execute("SELECT * FROM test")
            return result

        # Start 4 concurrent requests with a limit of 2
        tasks = [track_and_execute() for _ in range(4)]
        results = await asyncio.gather(*tasks)

        # All should complete successfully
        assert all(r == "result" for r in results)
        assert rate_limited.metrics["total_requests"] == 4

        # At most 2 should have been active at once
        assert max(active_requests) <= 2
