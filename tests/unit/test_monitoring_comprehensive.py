"""
Comprehensive unit tests for the monitoring module.
"""

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, Mock, patch

import pytest

from async_cassandra.monitoring import (
    HOST_STATUS_DOWN,
    HOST_STATUS_UNKNOWN,
    HOST_STATUS_UP,
    ClusterMetrics,
    ConnectionMonitor,
    HostMetrics,
    RateLimitedSession,
    create_monitored_session,
)


class TestHostMetrics:
    """Test HostMetrics dataclass comprehensively."""

    def test_host_metrics_all_fields(self):
        """Test creating HostMetrics with all fields."""
        now = datetime.now(timezone.utc)
        metrics = HostMetrics(
            address="192.168.1.1",
            datacenter="dc1",
            rack="rack1",
            status=HOST_STATUS_UP,
            release_version="4.0.0",
            connection_count=1,
            latency_ms=15.5,
            last_error="Connection timeout",
            last_check=now,
        )

        assert metrics.address == "192.168.1.1"
        assert metrics.datacenter == "dc1"
        assert metrics.rack == "rack1"
        assert metrics.status == HOST_STATUS_UP
        assert metrics.release_version == "4.0.0"
        assert metrics.connection_count == 1
        assert metrics.latency_ms == 15.5
        assert metrics.last_error == "Connection timeout"
        assert metrics.last_check == now

    def test_host_metrics_minimal(self):
        """Test creating HostMetrics with minimal fields."""
        metrics = HostMetrics(
            address="10.0.0.1",
            datacenter=None,
            rack=None,
            status=HOST_STATUS_DOWN,
            release_version=None,
            connection_count=0,
        )

        assert metrics.address == "10.0.0.1"
        assert metrics.datacenter is None
        assert metrics.rack is None
        assert metrics.status == HOST_STATUS_DOWN
        assert metrics.release_version is None
        assert metrics.connection_count == 0
        assert metrics.latency_ms is None
        assert metrics.last_error is None
        assert metrics.last_check is None


class TestClusterMetrics:
    """Test ClusterMetrics dataclass."""

    def test_cluster_metrics_creation(self):
        """Test creating ClusterMetrics."""
        now = datetime.now(timezone.utc)
        host1 = HostMetrics(
            address="192.168.1.1",
            datacenter="dc1",
            rack="rack1",
            status=HOST_STATUS_UP,
            release_version="4.0.0",
            connection_count=1,
        )
        host2 = HostMetrics(
            address="192.168.1.2",
            datacenter="dc1",
            rack="rack2",
            status=HOST_STATUS_DOWN,
            release_version="4.0.0",
            connection_count=0,
        )

        metrics = ClusterMetrics(
            timestamp=now,
            cluster_name="TestCluster",
            protocol_version=5,
            hosts=[host1, host2],
            total_connections=1,
            healthy_hosts=1,
            unhealthy_hosts=1,
            app_metrics={"requests": 100},
        )

        assert metrics.timestamp == now
        assert metrics.cluster_name == "TestCluster"
        assert metrics.protocol_version == 5
        assert len(metrics.hosts) == 2
        assert metrics.total_connections == 1
        assert metrics.healthy_hosts == 1
        assert metrics.unhealthy_hosts == 1
        assert metrics.app_metrics["requests"] == 100


class TestConnectionMonitor:
    """Test ConnectionMonitor comprehensively."""

    @pytest.fixture
    def mock_host(self):
        """Create a mock host."""
        host = Mock()
        host.address = "192.168.1.1"
        host.datacenter = "dc1"
        host.rack = "rack1"
        host.is_up = True
        host.release_version = "4.0.0"
        return host

    @pytest.fixture
    def mock_session(self):
        """Create a mock session with cluster metadata."""
        session = Mock()
        session._session = Mock()
        session._session.cluster = Mock()
        session._session.cluster.metadata = Mock()
        session._session.cluster.metadata.cluster_name = "TestCluster"
        session._session.cluster.protocol_version = 4
        session._session.cluster.metadata.all_hosts = Mock(return_value=[])
        session.execute = AsyncMock()
        return session

    def test_monitor_initialization(self, mock_session):
        """Test ConnectionMonitor initialization."""
        monitor = ConnectionMonitor(mock_session)

        assert monitor.session == mock_session
        assert monitor.metrics["requests_sent"] == 0
        assert monitor.metrics["requests_completed"] == 0
        assert monitor.metrics["requests_failed"] == 0
        assert monitor.metrics["last_health_check"] is None
        assert "monitoring_started" in monitor.metrics
        assert monitor._monitoring_task is None
        assert monitor._callbacks == []

    def test_add_callback(self, mock_session):
        """Test adding callbacks."""
        monitor = ConnectionMonitor(mock_session)

        callback1 = Mock()
        callback2 = Mock()

        monitor.add_callback(callback1)
        monitor.add_callback(callback2)

        assert len(monitor._callbacks) == 2
        assert callback1 in monitor._callbacks
        assert callback2 in monitor._callbacks

    @pytest.mark.asyncio
    async def test_check_host_health_up(self, mock_session, mock_host):
        """Test checking health of an up host."""
        monitor = ConnectionMonitor(mock_session)

        # Mock successful query execution
        mock_session.execute.return_value = None

        with patch("asyncio.get_event_loop") as mock_loop:
            mock_loop.return_value.time.side_effect = [1.0, 1.015]  # 15ms latency

            metrics = await monitor.check_host_health(mock_host)

        assert metrics.address == "192.168.1.1"
        assert metrics.datacenter == "dc1"
        assert metrics.rack == "rack1"
        assert metrics.status == HOST_STATUS_UP
        assert metrics.release_version == "4.0.0"
        assert metrics.connection_count == 1
        assert abs(metrics.latency_ms - 15.0) < 0.1  # Allow small floating point differences
        assert metrics.last_error is None
        assert metrics.last_check is not None

        # Verify query was executed
        mock_session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_check_host_health_down(self, mock_session):
        """Test checking health of a down host."""
        mock_host = Mock()
        mock_host.address = "192.168.1.2"
        mock_host.datacenter = "dc1"
        mock_host.rack = "rack1"
        mock_host.is_up = False
        mock_host.release_version = "4.0.0"

        monitor = ConnectionMonitor(mock_session)
        metrics = await monitor.check_host_health(mock_host)

        assert metrics.address == "192.168.1.2"
        assert metrics.status == HOST_STATUS_DOWN
        assert metrics.connection_count == 0
        assert metrics.latency_ms is None

        # No query should be executed for down host
        mock_session.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_check_host_health_error(self, mock_session, mock_host):
        """Test checking health when query fails."""
        monitor = ConnectionMonitor(mock_session)

        # Mock query execution failure
        mock_session.execute.side_effect = Exception("Connection error")

        metrics = await monitor.check_host_health(mock_host)

        assert metrics.address == "192.168.1.1"
        assert metrics.status == HOST_STATUS_UNKNOWN
        assert metrics.connection_count == 0
        assert metrics.last_error == "Connection error"
        assert metrics.latency_ms is None

    @pytest.mark.asyncio
    async def test_get_cluster_metrics(self, mock_session, mock_host):
        """Test getting full cluster metrics."""
        # Setup two hosts
        mock_host2 = Mock()
        mock_host2.address = "192.168.1.2"
        mock_host2.datacenter = "dc1"
        mock_host2.rack = "rack2"
        mock_host2.is_up = False
        mock_host2.release_version = "4.0.0"

        mock_session._session.cluster.metadata.all_hosts.return_value = [mock_host, mock_host2]

        monitor = ConnectionMonitor(mock_session)
        monitor.metrics["requests_sent"] = 100

        with patch("asyncio.get_event_loop") as mock_loop:
            mock_loop.return_value.time.side_effect = [1.0, 1.010]  # 10ms latency

            metrics = await monitor.get_cluster_metrics()

        assert isinstance(metrics, ClusterMetrics)
        assert metrics.cluster_name == "TestCluster"
        assert metrics.protocol_version == 4
        assert len(metrics.hosts) == 2
        assert metrics.total_connections == 1  # Only one host is up
        assert metrics.healthy_hosts == 1
        assert metrics.unhealthy_hosts == 1
        assert metrics.app_metrics["requests_sent"] == 100

    @pytest.mark.asyncio
    async def test_warmup_connections(self, mock_session, mock_host):
        """Test warming up connections."""
        mock_session._session.cluster.metadata.all_hosts.return_value = [mock_host]

        monitor = ConnectionMonitor(mock_session)

        await monitor.warmup_connections()

        # Should execute one query per up host
        mock_session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_warmup_connections_with_failures(self, mock_session, mock_host):
        """Test warming up connections with some failures."""
        mock_host2 = Mock()
        mock_host2.address = "192.168.1.2"
        mock_host2.is_up = True

        mock_session._session.cluster.metadata.all_hosts.return_value = [mock_host, mock_host2]

        # First succeeds, second fails
        mock_session.execute.side_effect = [None, Exception("Connection failed")]

        monitor = ConnectionMonitor(mock_session)

        # Should not raise exception
        await monitor.warmup_connections()

        # Should attempt both hosts
        assert mock_session.execute.call_count == 2

    @pytest.mark.asyncio
    async def test_start_stop_monitoring(self, mock_session):
        """Test starting and stopping monitoring."""
        monitor = ConnectionMonitor(mock_session)

        # Start monitoring
        await monitor.start_monitoring(interval=0.1)
        assert monitor._monitoring_task is not None
        assert not monitor._monitoring_task.done()

        # Stop monitoring
        await monitor.stop_monitoring()
        assert monitor._monitoring_task.done()

    @pytest.mark.asyncio
    async def test_monitoring_loop_callbacks(self, mock_session):
        """Test monitoring loop calls callbacks."""
        monitor = ConnectionMonitor(mock_session)

        # Add callbacks
        sync_callback = Mock()
        async_callback = AsyncMock()
        monitor.add_callback(sync_callback)
        monitor.add_callback(async_callback)

        # Run one iteration of monitoring loop
        with patch.object(monitor, "get_cluster_metrics") as mock_get_metrics:
            mock_metrics = Mock()
            mock_metrics.timestamp = datetime.now(timezone.utc)
            mock_metrics.healthy_hosts = 2
            mock_metrics.unhealthy_hosts = 0
            mock_get_metrics.return_value = mock_metrics

            # Run monitoring loop once
            task = asyncio.create_task(monitor._monitoring_loop(10))
            await asyncio.sleep(0.1)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        # Both callbacks should be called
        sync_callback.assert_called_once_with(mock_metrics)
        async_callback.assert_called_once_with(mock_metrics)

    @pytest.mark.asyncio
    async def test_monitoring_loop_error_handling(self, mock_session):
        """Test monitoring loop handles errors gracefully."""
        monitor = ConnectionMonitor(mock_session)

        # Add callback that raises exception
        def bad_callback(metrics):
            raise Exception("Callback error")

        monitor.add_callback(bad_callback)

        # Mock get_cluster_metrics to control the loop
        with patch.object(monitor, "get_cluster_metrics") as mock_get_metrics:
            mock_metrics = Mock()
            mock_metrics.timestamp = datetime.now(timezone.utc)
            mock_metrics.healthy_hosts = 1
            mock_metrics.unhealthy_hosts = 0
            mock_get_metrics.return_value = mock_metrics

            # Run monitoring loop
            task = asyncio.create_task(monitor._monitoring_loop(10))
            await asyncio.sleep(0.1)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        # Loop should continue despite callback error
        mock_get_metrics.assert_called()

    def test_get_connection_summary(self, mock_session):
        """Test getting connection summary."""
        mock_host1 = Mock(is_up=True)
        mock_host2 = Mock(is_up=False)
        mock_host3 = Mock(is_up=True)

        mock_session._session.cluster.metadata.all_hosts.return_value = [
            mock_host1,
            mock_host2,
            mock_host3,
        ]

        monitor = ConnectionMonitor(mock_session)
        summary = monitor.get_connection_summary()

        assert summary["total_hosts"] == 3
        assert summary["up_hosts"] == 2
        assert summary["down_hosts"] == 1
        assert summary["protocol_version"] == 4
        assert summary["max_requests_per_connection"] == 32768
        assert "note" in summary


class TestRateLimitedSession:
    """Test RateLimitedSession comprehensively."""

    @pytest.fixture
    def mock_session(self):
        """Create a mock async session."""
        session = Mock()
        session.execute = AsyncMock(return_value="result")
        session.prepare = AsyncMock(return_value="prepared")
        return session

    def test_initialization(self, mock_session):
        """Test RateLimitedSession initialization."""
        rate_limited = RateLimitedSession(mock_session, max_concurrent=100)

        assert rate_limited.session == mock_session
        assert rate_limited.semaphore._value == 100
        assert rate_limited.metrics["total_requests"] == 0
        assert rate_limited.metrics["active_requests"] == 0
        assert rate_limited.metrics["rejected_requests"] == 0

    @pytest.mark.asyncio
    async def test_execute_basic(self, mock_session):
        """Test basic execute functionality."""
        rate_limited = RateLimitedSession(mock_session)

        result = await rate_limited.execute("SELECT * FROM test", {"id": 1})

        assert result == "result"
        assert rate_limited.metrics["total_requests"] == 1
        assert rate_limited.metrics["active_requests"] == 0
        mock_session.execute.assert_called_once_with("SELECT * FROM test", {"id": 1})

    @pytest.mark.asyncio
    async def test_execute_with_kwargs(self, mock_session):
        """Test execute with additional kwargs."""
        rate_limited = RateLimitedSession(mock_session)

        result = await rate_limited.execute("SELECT * FROM test", None, timeout=10, trace=True)

        assert result == "result"
        mock_session.execute.assert_called_once_with(
            "SELECT * FROM test", None, timeout=10, trace=True
        )

    @pytest.mark.asyncio
    async def test_prepare_not_rate_limited(self, mock_session):
        """Test that prepare is not rate limited."""
        rate_limited = RateLimitedSession(mock_session, max_concurrent=1)

        # Prepare multiple statements concurrently
        tasks = [
            rate_limited.prepare("SELECT * FROM test1"),
            rate_limited.prepare("SELECT * FROM test2"),
            rate_limited.prepare("SELECT * FROM test3"),
        ]

        results = await asyncio.gather(*tasks)

        # All should complete without rate limiting
        assert all(r == "prepared" for r in results)
        assert mock_session.prepare.call_count == 3

    @pytest.mark.asyncio
    async def test_concurrent_rate_limiting(self, mock_session):
        """Test that rate limiting actually limits concurrency."""
        rate_limited = RateLimitedSession(mock_session, max_concurrent=2)

        # Track concurrent executions
        concurrent_count = 0
        max_concurrent = 0

        async def slow_execute(*args, **kwargs):
            nonlocal concurrent_count, max_concurrent
            concurrent_count += 1
            max_concurrent = max(max_concurrent, concurrent_count)
            await asyncio.sleep(0.05)
            concurrent_count -= 1
            return "result"

        mock_session.execute = slow_execute

        # Start 5 concurrent requests
        tasks = [rate_limited.execute(f"SELECT {i}") for i in range(5)]
        await asyncio.gather(*tasks)

        # Should never exceed max_concurrent
        assert max_concurrent <= 2
        assert rate_limited.metrics["total_requests"] == 5

    @pytest.mark.asyncio
    async def test_metrics_accuracy(self, mock_session):
        """Test that metrics are accurately tracked."""
        rate_limited = RateLimitedSession(mock_session, max_concurrent=3)

        # Execute some successful queries
        await rate_limited.execute("SELECT 1")
        await rate_limited.execute("SELECT 2")

        assert rate_limited.metrics["total_requests"] == 2
        assert rate_limited.metrics["active_requests"] == 0

        # Test active requests tracking
        async def check_active(*args, **kwargs):
            # During execution, active_requests should be > 0
            assert rate_limited.metrics["active_requests"] > 0
            return "result"

        mock_session.execute = check_active
        await rate_limited.execute("SELECT 3")

        assert rate_limited.metrics["total_requests"] == 3
        assert rate_limited.metrics["active_requests"] == 0

    def test_get_metrics(self, mock_session):
        """Test getting metrics returns a copy."""
        rate_limited = RateLimitedSession(mock_session)

        metrics1 = rate_limited.get_metrics()
        metrics1["total_requests"] = 999

        metrics2 = rate_limited.get_metrics()
        assert metrics2["total_requests"] == 0  # Original unchanged


class TestCreateMonitoredSession:
    """Test create_monitored_session factory function."""

    @pytest.mark.asyncio
    async def test_create_basic_session(self):
        """Test creating basic monitored session."""
        with patch("async_cassandra.cluster.AsyncCluster") as mock_cluster_class:
            mock_cluster = Mock()
            mock_session = Mock()
            mock_session._session = Mock()
            mock_session._session.cluster = Mock()
            mock_session._session.cluster.metadata = Mock()
            mock_session._session.cluster.metadata.all_hosts = Mock(return_value=[])
            mock_session._session.cluster.metadata.cluster_name = "Test"
            mock_session._session.cluster.protocol_version = 4

            mock_cluster_class.return_value = mock_cluster
            mock_cluster.connect = AsyncMock(return_value=mock_session)

            session, monitor = await create_monitored_session(
                ["127.0.0.1"], keyspace="test_ks", warmup=False
            )

            assert session == mock_session
            assert isinstance(monitor, ConnectionMonitor)
            mock_cluster_class.assert_called_once_with(contact_points=["127.0.0.1"])
            mock_cluster.connect.assert_called_once_with("test_ks")

    @pytest.mark.asyncio
    async def test_create_rate_limited_session(self):
        """Test creating rate limited session."""
        with patch("async_cassandra.cluster.AsyncCluster") as mock_cluster_class:
            mock_cluster = Mock()
            mock_session = Mock()
            mock_session._session = Mock()
            mock_session._session.cluster = Mock()
            mock_session._session.cluster.metadata = Mock()
            mock_session._session.cluster.metadata.all_hosts = Mock(return_value=[])
            mock_session._session.cluster.metadata.cluster_name = "Test"
            mock_session._session.cluster.protocol_version = 4

            mock_cluster_class.return_value = mock_cluster
            mock_cluster.connect = AsyncMock(return_value=mock_session)

            session, monitor = await create_monitored_session(
                ["127.0.0.1"], max_concurrent=500, warmup=False
            )

            assert isinstance(session, RateLimitedSession)
            assert session.semaphore._value == 500
            assert isinstance(monitor, ConnectionMonitor)

    @pytest.mark.asyncio
    async def test_create_with_warmup(self):
        """Test creating session with connection warmup."""
        with patch("async_cassandra.cluster.AsyncCluster") as mock_cluster_class:
            mock_cluster = Mock()
            mock_session = Mock()
            mock_session._session = Mock()
            mock_session._session.cluster = Mock()
            mock_session._session.cluster.metadata = Mock()
            mock_session._session.cluster.metadata.all_hosts = Mock(return_value=[])
            mock_session._session.cluster.metadata.cluster_name = "Test"
            mock_session._session.cluster.protocol_version = 4
            mock_session.execute = AsyncMock()

            mock_cluster_class.return_value = mock_cluster
            mock_cluster.connect = AsyncMock(return_value=mock_session)

            with patch.object(ConnectionMonitor, "warmup_connections") as mock_warmup:
                mock_warmup.return_value = asyncio.Future()
                mock_warmup.return_value.set_result(None)

                session, monitor = await create_monitored_session(["127.0.0.1"], warmup=True)

                mock_warmup.assert_called_once()
