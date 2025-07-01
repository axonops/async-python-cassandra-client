"""
Unified monitoring and metrics tests for async-python-cassandra.

This module provides comprehensive tests for the monitoring and metrics
functionality based on the actual implementation.

Test Organization:
==================
1. Metrics Data Classes - Testing QueryMetrics and ConnectionMetrics
2. InMemoryMetricsCollector - Testing the in-memory metrics backend
3. PrometheusMetricsCollector - Testing Prometheus integration
4. MetricsMiddleware - Testing the middleware layer
5. ConnectionMonitor - Testing connection health monitoring
6. RateLimitedSession - Testing rate limiting functionality
7. Integration Tests - Testing the full monitoring stack

Key Testing Principles:
======================
- All metrics methods are async and must be awaited
- Test thread safety with asyncio.Lock
- Verify metrics accuracy and aggregation
- Test graceful degradation without prometheus_client
- Ensure monitoring doesn't impact performance
"""

import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, Mock, patch

import pytest

from async_cassandra.metrics import (
    ConnectionMetrics,
    InMemoryMetricsCollector,
    MetricsMiddleware,
    PrometheusMetricsCollector,
    QueryMetrics,
    create_metrics_system,
)
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


class TestMetricsDataClasses:
    """Test the metrics data classes."""

    def test_query_metrics_creation(self):
        """Test QueryMetrics dataclass creation and fields."""
        now = datetime.now(timezone.utc)
        metrics = QueryMetrics(
            query_hash="abc123",
            duration=0.123,
            success=True,
            error_type=None,
            timestamp=now,
            parameters_count=2,
            result_size=10,
        )

        assert metrics.query_hash == "abc123"
        assert metrics.duration == 0.123
        assert metrics.success is True
        assert metrics.error_type is None
        assert metrics.timestamp == now
        assert metrics.parameters_count == 2
        assert metrics.result_size == 10

    def test_query_metrics_defaults(self):
        """Test QueryMetrics default values."""
        metrics = QueryMetrics(
            query_hash="xyz789", duration=0.05, success=False, error_type="Timeout"
        )

        assert metrics.parameters_count == 0
        assert metrics.result_size == 0
        assert isinstance(metrics.timestamp, datetime)
        assert metrics.timestamp.tzinfo == timezone.utc

    def test_connection_metrics_creation(self):
        """Test ConnectionMetrics dataclass creation."""
        now = datetime.now(timezone.utc)
        metrics = ConnectionMetrics(
            host="127.0.0.1",
            is_healthy=True,
            last_check=now,
            response_time=0.02,
            error_count=0,
            total_queries=100,
        )

        assert metrics.host == "127.0.0.1"
        assert metrics.is_healthy is True
        assert metrics.last_check == now
        assert metrics.response_time == 0.02
        assert metrics.error_count == 0
        assert metrics.total_queries == 100

    def test_host_metrics_creation(self):
        """Test HostMetrics dataclass for monitoring."""
        now = datetime.now(timezone.utc)
        metrics = HostMetrics(
            address="127.0.0.1",
            datacenter="dc1",
            rack="rack1",
            status=HOST_STATUS_UP,
            release_version="4.0.1",
            connection_count=1,
            latency_ms=5.2,
            last_error=None,
            last_check=now,
        )

        assert metrics.address == "127.0.0.1"
        assert metrics.datacenter == "dc1"
        assert metrics.rack == "rack1"
        assert metrics.status == HOST_STATUS_UP
        assert metrics.release_version == "4.0.1"
        assert metrics.connection_count == 1
        assert metrics.latency_ms == 5.2
        assert metrics.last_error is None
        assert metrics.last_check == now

    def test_cluster_metrics_creation(self):
        """Test ClusterMetrics aggregation dataclass."""
        now = datetime.now(timezone.utc)
        host1 = HostMetrics("127.0.0.1", "dc1", "rack1", HOST_STATUS_UP, "4.0.1", 1)
        host2 = HostMetrics("127.0.0.2", "dc1", "rack2", HOST_STATUS_DOWN, "4.0.1", 0)

        cluster = ClusterMetrics(
            timestamp=now,
            cluster_name="test_cluster",
            protocol_version=4,
            hosts=[host1, host2],
            total_connections=1,
            healthy_hosts=1,
            unhealthy_hosts=1,
            app_metrics={"requests_sent": 100},
        )

        assert cluster.timestamp == now
        assert cluster.cluster_name == "test_cluster"
        assert cluster.protocol_version == 4
        assert len(cluster.hosts) == 2
        assert cluster.total_connections == 1
        assert cluster.healthy_hosts == 1
        assert cluster.unhealthy_hosts == 1
        assert cluster.app_metrics["requests_sent"] == 100


class TestInMemoryMetricsCollector:
    """Test the in-memory metrics collection system."""

    @pytest.mark.asyncio
    async def test_record_query_metrics(self):
        """Test recording query metrics."""
        collector = InMemoryMetricsCollector(max_entries=100)

        # Create and record metrics
        metrics = QueryMetrics(
            query_hash="abc123", duration=0.1, success=True, parameters_count=1, result_size=5
        )

        await collector.record_query(metrics)

        # Check it was recorded
        assert len(collector.query_metrics) == 1
        assert collector.query_metrics[0] == metrics
        assert collector.query_counts["abc123"] == 1

    @pytest.mark.asyncio
    async def test_record_query_with_error(self):
        """Test recording failed queries."""
        collector = InMemoryMetricsCollector()

        # Record failed query
        metrics = QueryMetrics(
            query_hash="xyz789", duration=0.05, success=False, error_type="InvalidRequest"
        )

        await collector.record_query(metrics)

        # Check error counting
        assert collector.error_counts["InvalidRequest"] == 1
        assert len(collector.query_metrics) == 1

    @pytest.mark.asyncio
    async def test_max_entries_limit(self):
        """Test that collector respects max_entries limit."""
        collector = InMemoryMetricsCollector(max_entries=5)

        # Record more than max entries
        for i in range(10):
            metrics = QueryMetrics(query_hash=f"query_{i}", duration=0.1, success=True)
            await collector.record_query(metrics)

        # Should only keep the last 5
        assert len(collector.query_metrics) == 5
        # Verify it's the last 5 queries (deque behavior)
        hashes = [m.query_hash for m in collector.query_metrics]
        assert hashes == ["query_5", "query_6", "query_7", "query_8", "query_9"]

    @pytest.mark.asyncio
    async def test_record_connection_health(self):
        """Test recording connection health metrics."""
        collector = InMemoryMetricsCollector()

        # Record healthy connection
        healthy = ConnectionMetrics(
            host="127.0.0.1",
            is_healthy=True,
            last_check=datetime.now(timezone.utc),
            response_time=0.02,
            error_count=0,
            total_queries=50,
        )
        await collector.record_connection_health(healthy)

        # Record unhealthy connection
        unhealthy = ConnectionMetrics(
            host="127.0.0.2",
            is_healthy=False,
            last_check=datetime.now(timezone.utc),
            response_time=0,
            error_count=5,
            total_queries=10,
        )
        await collector.record_connection_health(unhealthy)

        # Check storage
        assert "127.0.0.1" in collector.connection_metrics
        assert "127.0.0.2" in collector.connection_metrics
        assert collector.connection_metrics["127.0.0.1"].is_healthy is True
        assert collector.connection_metrics["127.0.0.2"].is_healthy is False

    @pytest.mark.asyncio
    async def test_get_stats_no_data(self):
        """Test get_stats with no data."""
        collector = InMemoryMetricsCollector()
        stats = await collector.get_stats()

        assert stats == {"message": "No metrics available"}

    @pytest.mark.asyncio
    async def test_get_stats_with_recent_queries(self):
        """Test get_stats with recent query data."""
        collector = InMemoryMetricsCollector()

        # Record some recent queries
        now = datetime.now(timezone.utc)
        for i in range(5):
            metrics = QueryMetrics(
                query_hash=f"query_{i}",
                duration=0.1 * (i + 1),
                success=i % 2 == 0,
                error_type="Timeout" if i % 2 else None,
                timestamp=now - timedelta(minutes=1),
                result_size=10 * i,
            )
            await collector.record_query(metrics)

        stats = await collector.get_stats()

        # Check structure
        assert "query_performance" in stats
        assert "error_summary" in stats
        assert "top_queries" in stats
        assert "connection_health" in stats

        # Check calculations
        perf = stats["query_performance"]
        assert perf["total_queries"] == 5
        assert perf["recent_queries_5min"] == 5
        assert perf["success_rate"] == 0.6  # 3 out of 5
        assert "avg_duration_ms" in perf
        assert "min_duration_ms" in perf
        assert "max_duration_ms" in perf

        # Check error summary
        assert stats["error_summary"]["Timeout"] == 2

    @pytest.mark.asyncio
    async def test_get_stats_with_old_queries(self):
        """Test get_stats filters out old queries."""
        collector = InMemoryMetricsCollector()

        # Record old query
        old_metrics = QueryMetrics(
            query_hash="old_query",
            duration=0.1,
            success=True,
            timestamp=datetime.now(timezone.utc) - timedelta(minutes=10),
        )
        await collector.record_query(old_metrics)

        stats = await collector.get_stats()

        # Should have no recent queries
        assert stats["query_performance"]["message"] == "No recent queries"
        assert stats["error_summary"] == {}

    @pytest.mark.asyncio
    async def test_thread_safety(self):
        """Test that collector is thread-safe with async operations."""
        collector = InMemoryMetricsCollector(max_entries=1000)

        async def record_many(start_id: int):
            for i in range(100):
                metrics = QueryMetrics(
                    query_hash=f"query_{start_id}_{i}", duration=0.01, success=True
                )
                await collector.record_query(metrics)

        # Run multiple concurrent tasks
        tasks = [record_many(i * 100) for i in range(5)]
        await asyncio.gather(*tasks)

        # Should have recorded all 500
        assert len(collector.query_metrics) == 500


class TestPrometheusMetricsCollector:
    """Test the Prometheus metrics collector."""

    def test_initialization_without_prometheus_client(self):
        """Test initialization when prometheus_client is not available."""
        with patch.dict("sys.modules", {"prometheus_client": None}):
            collector = PrometheusMetricsCollector()

            assert collector._available is False
            assert collector.query_duration is None
            assert collector.query_total is None
            assert collector.connection_health is None
            assert collector.error_total is None

    @pytest.mark.asyncio
    async def test_record_query_without_prometheus(self):
        """Test recording works gracefully without prometheus_client."""
        with patch.dict("sys.modules", {"prometheus_client": None}):
            collector = PrometheusMetricsCollector()

            # Should not raise
            metrics = QueryMetrics(query_hash="test", duration=0.1, success=True)
            await collector.record_query(metrics)

    @pytest.mark.asyncio
    async def test_record_connection_without_prometheus(self):
        """Test connection recording without prometheus_client."""
        with patch.dict("sys.modules", {"prometheus_client": None}):
            collector = PrometheusMetricsCollector()

            # Should not raise
            metrics = ConnectionMetrics(
                host="127.0.0.1",
                is_healthy=True,
                last_check=datetime.now(timezone.utc),
                response_time=0.02,
            )
            await collector.record_connection_health(metrics)

    @pytest.mark.asyncio
    async def test_get_stats_without_prometheus(self):
        """Test get_stats without prometheus_client."""
        with patch.dict("sys.modules", {"prometheus_client": None}):
            collector = PrometheusMetricsCollector()
            stats = await collector.get_stats()

            assert stats == {"error": "Prometheus client not available"}

    @pytest.mark.asyncio
    async def test_with_prometheus_client(self):
        """Test with mocked prometheus_client."""
        # Mock prometheus_client
        mock_histogram = Mock()
        mock_counter = Mock()
        mock_gauge = Mock()

        mock_prometheus = Mock()
        mock_prometheus.Histogram.return_value = mock_histogram
        mock_prometheus.Counter.return_value = mock_counter
        mock_prometheus.Gauge.return_value = mock_gauge

        with patch.dict("sys.modules", {"prometheus_client": mock_prometheus}):
            collector = PrometheusMetricsCollector()

            assert collector._available is True
            assert collector.query_duration is mock_histogram
            assert collector.query_total is mock_counter
            assert collector.connection_health is mock_gauge
            assert collector.error_total is mock_counter

            # Test recording query
            metrics = QueryMetrics(query_hash="prepared_stmt_123", duration=0.05, success=True)
            await collector.record_query(metrics)

            # Verify Prometheus metrics were updated
            mock_histogram.labels.assert_called_with(query_type="prepared", success="success")
            mock_histogram.labels().observe.assert_called_with(0.05)
            mock_counter.labels.assert_called_with(query_type="prepared", success="success")
            mock_counter.labels().inc.assert_called()


class TestMetricsMiddleware:
    """Test the metrics middleware functionality."""

    @pytest.mark.asyncio
    async def test_middleware_creation(self):
        """Test creating metrics middleware."""
        collector = InMemoryMetricsCollector()
        middleware = MetricsMiddleware([collector])

        assert len(middleware.collectors) == 1
        assert middleware._enabled is True

    def test_enable_disable(self):
        """Test enabling and disabling middleware."""
        middleware = MetricsMiddleware([])

        # Initially enabled
        assert middleware._enabled is True

        # Disable
        middleware.disable()
        assert middleware._enabled is False

        # Re-enable
        middleware.enable()
        assert middleware._enabled is True

    @pytest.mark.asyncio
    async def test_record_query_metrics(self):
        """Test recording metrics through middleware."""
        collector = InMemoryMetricsCollector()
        middleware = MetricsMiddleware([collector])

        # Record a query
        await middleware.record_query_metrics(
            query="SELECT * FROM users WHERE id = ?",
            duration=0.05,
            success=True,
            error_type=None,
            parameters_count=1,
            result_size=1,
        )

        # Check it was recorded
        assert len(collector.query_metrics) == 1
        recorded = collector.query_metrics[0]
        assert recorded.duration == 0.05
        assert recorded.success is True
        assert recorded.parameters_count == 1
        assert recorded.result_size == 1

    @pytest.mark.asyncio
    async def test_record_query_metrics_disabled(self):
        """Test that disabled middleware doesn't record."""
        collector = InMemoryMetricsCollector()
        middleware = MetricsMiddleware([collector])
        middleware.disable()

        # Try to record
        await middleware.record_query_metrics(
            query="SELECT * FROM users", duration=0.05, success=True
        )

        # Nothing should be recorded
        assert len(collector.query_metrics) == 0

    def test_normalize_query(self):
        """Test query normalization for grouping."""
        middleware = MetricsMiddleware([])

        # Test normalization creates consistent hashes
        query1 = "SELECT * FROM users WHERE id = 123"
        query2 = "SELECT * FROM users WHERE id = 456"
        query3 = "select   *   from   users   where   id   =   789"

        # Different values but same structure should get same hash
        hash1 = middleware._normalize_query(query1)
        hash2 = middleware._normalize_query(query2)
        hash3 = middleware._normalize_query(query3)

        assert hash1 == hash2  # Same query structure
        assert hash1 == hash3  # Whitespace normalized

    def test_normalize_query_different_structures(self):
        """Test normalization of different query structures."""
        middleware = MetricsMiddleware([])

        queries = [
            "SELECT * FROM users WHERE id = ?",
            "SELECT * FROM users WHERE name = ?",
            "INSERT INTO users VALUES (?, ?)",
            "DELETE FROM users WHERE id = ?",
        ]

        hashes = [middleware._normalize_query(q) for q in queries]

        # All should be different
        assert len(set(hashes)) == len(queries)

    @pytest.mark.asyncio
    async def test_record_connection_metrics(self):
        """Test recording connection health through middleware."""
        collector = InMemoryMetricsCollector()
        middleware = MetricsMiddleware([collector])

        await middleware.record_connection_metrics(
            host="127.0.0.1", is_healthy=True, response_time=0.02, error_count=0, total_queries=100
        )

        assert "127.0.0.1" in collector.connection_metrics
        metrics = collector.connection_metrics["127.0.0.1"]
        assert metrics.is_healthy is True
        assert metrics.response_time == 0.02

    @pytest.mark.asyncio
    async def test_multiple_collectors(self):
        """Test middleware with multiple collectors."""
        collector1 = InMemoryMetricsCollector()
        collector2 = InMemoryMetricsCollector()
        middleware = MetricsMiddleware([collector1, collector2])

        await middleware.record_query_metrics(
            query="SELECT * FROM test", duration=0.1, success=True
        )

        # Both collectors should have the metrics
        assert len(collector1.query_metrics) == 1
        assert len(collector2.query_metrics) == 1

    @pytest.mark.asyncio
    async def test_collector_error_handling(self):
        """Test middleware handles collector errors gracefully."""
        # Create a failing collector
        failing_collector = Mock()
        failing_collector.record_query = AsyncMock(side_effect=Exception("Collector failed"))

        # And a working collector
        working_collector = InMemoryMetricsCollector()

        middleware = MetricsMiddleware([failing_collector, working_collector])

        # Should not raise
        await middleware.record_query_metrics(
            query="SELECT * FROM test", duration=0.1, success=True
        )

        # Working collector should still get metrics
        assert len(working_collector.query_metrics) == 1


class TestConnectionMonitor:
    """Test the connection monitoring functionality."""

    def test_monitor_initialization(self):
        """Test ConnectionMonitor initialization."""
        mock_session = Mock()
        monitor = ConnectionMonitor(mock_session)

        assert monitor.session == mock_session
        assert monitor.metrics["requests_sent"] == 0
        assert monitor.metrics["requests_completed"] == 0
        assert monitor.metrics["requests_failed"] == 0
        assert monitor._monitoring_task is None
        assert len(monitor._callbacks) == 0

    def test_add_callback(self):
        """Test adding monitoring callbacks."""
        mock_session = Mock()
        monitor = ConnectionMonitor(mock_session)

        callback1 = Mock()
        callback2 = Mock()

        monitor.add_callback(callback1)
        monitor.add_callback(callback2)

        assert len(monitor._callbacks) == 2
        assert callback1 in monitor._callbacks
        assert callback2 in monitor._callbacks

    @pytest.mark.asyncio
    async def test_check_host_health_up(self):
        """Test checking health of an up host."""
        mock_session = Mock()
        mock_session.execute = AsyncMock(return_value=Mock())

        monitor = ConnectionMonitor(mock_session)

        # Mock host
        host = Mock()
        host.address = "127.0.0.1"
        host.datacenter = "dc1"
        host.rack = "rack1"
        host.is_up = True
        host.release_version = "4.0.1"

        metrics = await monitor.check_host_health(host)

        assert metrics.address == "127.0.0.1"
        assert metrics.datacenter == "dc1"
        assert metrics.rack == "rack1"
        assert metrics.status == HOST_STATUS_UP
        assert metrics.release_version == "4.0.1"
        assert metrics.connection_count == 1
        assert metrics.latency_ms is not None
        assert metrics.latency_ms > 0
        assert isinstance(metrics.last_check, datetime)

    @pytest.mark.asyncio
    async def test_check_host_health_down(self):
        """Test checking health of a down host."""
        mock_session = Mock()
        monitor = ConnectionMonitor(mock_session)

        # Mock host
        host = Mock()
        host.address = "127.0.0.1"
        host.datacenter = "dc1"
        host.rack = "rack1"
        host.is_up = False
        host.release_version = "4.0.1"

        metrics = await monitor.check_host_health(host)

        assert metrics.address == "127.0.0.1"
        assert metrics.status == HOST_STATUS_DOWN
        assert metrics.connection_count == 0
        assert metrics.latency_ms is None
        assert metrics.last_check is None

    @pytest.mark.asyncio
    async def test_check_host_health_with_error(self):
        """Test host health check with connection error."""
        mock_session = Mock()
        mock_session.execute = AsyncMock(side_effect=Exception("Connection failed"))

        monitor = ConnectionMonitor(mock_session)

        # Mock host
        host = Mock()
        host.address = "127.0.0.1"
        host.datacenter = "dc1"
        host.rack = "rack1"
        host.is_up = True
        host.release_version = "4.0.1"

        metrics = await monitor.check_host_health(host)

        assert metrics.address == "127.0.0.1"
        assert metrics.status == HOST_STATUS_UNKNOWN
        assert metrics.connection_count == 0
        assert metrics.last_error == "Connection failed"

    @pytest.mark.asyncio
    async def test_get_cluster_metrics(self):
        """Test getting comprehensive cluster metrics."""
        mock_session = Mock()
        mock_session.execute = AsyncMock(return_value=Mock())

        # Mock cluster
        mock_cluster = Mock()
        mock_cluster.metadata.cluster_name = "test_cluster"
        mock_cluster.protocol_version = 4

        # Mock hosts
        host1 = Mock()
        host1.address = "127.0.0.1"
        host1.datacenter = "dc1"
        host1.rack = "rack1"
        host1.is_up = True
        host1.release_version = "4.0.1"

        host2 = Mock()
        host2.address = "127.0.0.2"
        host2.datacenter = "dc1"
        host2.rack = "rack2"
        host2.is_up = False
        host2.release_version = "4.0.1"

        mock_cluster.metadata.all_hosts.return_value = [host1, host2]
        mock_session._session.cluster = mock_cluster

        monitor = ConnectionMonitor(mock_session)
        metrics = await monitor.get_cluster_metrics()

        assert isinstance(metrics, ClusterMetrics)
        assert metrics.cluster_name == "test_cluster"
        assert metrics.protocol_version == 4
        assert len(metrics.hosts) == 2
        assert metrics.healthy_hosts == 1
        assert metrics.unhealthy_hosts == 1
        assert metrics.total_connections == 1

    @pytest.mark.asyncio
    async def test_warmup_connections(self):
        """Test warming up connections to hosts."""
        mock_session = Mock()
        mock_session.execute = AsyncMock(return_value=Mock())

        # Mock cluster
        mock_cluster = Mock()
        host1 = Mock(is_up=True, address="127.0.0.1")
        host2 = Mock(is_up=True, address="127.0.0.2")
        host3 = Mock(is_up=False, address="127.0.0.3")

        mock_cluster.metadata.all_hosts.return_value = [host1, host2, host3]
        mock_session._session.cluster = mock_cluster

        monitor = ConnectionMonitor(mock_session)
        await monitor.warmup_connections()

        # Should only warm up the two up hosts
        assert mock_session.execute.call_count == 2

    @pytest.mark.asyncio
    async def test_warmup_connections_with_failures(self):
        """Test connection warmup with some failures."""
        mock_session = Mock()
        # First call succeeds, second fails
        mock_session.execute = AsyncMock(side_effect=[Mock(), Exception("Failed")])

        # Mock cluster
        mock_cluster = Mock()
        host1 = Mock(is_up=True, address="127.0.0.1")
        host2 = Mock(is_up=True, address="127.0.0.2")

        mock_cluster.metadata.all_hosts.return_value = [host1, host2]
        mock_session._session.cluster = mock_cluster

        monitor = ConnectionMonitor(mock_session)
        # Should not raise
        await monitor.warmup_connections()

    @pytest.mark.asyncio
    async def test_start_stop_monitoring(self):
        """Test starting and stopping monitoring."""
        mock_session = Mock()
        mock_session.execute = AsyncMock(return_value=Mock())

        # Mock cluster
        mock_cluster = Mock()
        mock_cluster.metadata.cluster_name = "test"
        mock_cluster.protocol_version = 4
        mock_cluster.metadata.all_hosts.return_value = []
        mock_session._session.cluster = mock_cluster

        monitor = ConnectionMonitor(mock_session)

        # Start monitoring
        await monitor.start_monitoring(interval=0.1)
        assert monitor._monitoring_task is not None
        assert not monitor._monitoring_task.done()

        # Let it run briefly
        await asyncio.sleep(0.2)

        # Stop monitoring
        await monitor.stop_monitoring()
        assert monitor._monitoring_task.done()

    @pytest.mark.asyncio
    async def test_monitoring_loop_with_callbacks(self):
        """Test monitoring loop executes callbacks."""
        mock_session = Mock()
        mock_session.execute = AsyncMock(return_value=Mock())

        # Mock cluster
        mock_cluster = Mock()
        mock_cluster.metadata.cluster_name = "test"
        mock_cluster.protocol_version = 4
        mock_cluster.metadata.all_hosts.return_value = []
        mock_session._session.cluster = mock_cluster

        monitor = ConnectionMonitor(mock_session)

        # Track callback executions
        callback_metrics = []

        def sync_callback(metrics):
            callback_metrics.append(metrics)

        async def async_callback(metrics):
            await asyncio.sleep(0.01)
            callback_metrics.append(metrics)

        monitor.add_callback(sync_callback)
        monitor.add_callback(async_callback)

        # Start monitoring
        await monitor.start_monitoring(interval=0.1)

        # Wait for at least one check
        await asyncio.sleep(0.2)

        # Stop monitoring
        await monitor.stop_monitoring()

        # Both callbacks should have been called at least once
        assert len(callback_metrics) >= 1

    def test_get_connection_summary(self):
        """Test getting connection summary."""
        mock_session = Mock()

        # Mock cluster
        mock_cluster = Mock()
        mock_cluster.protocol_version = 4

        host1 = Mock(is_up=True)
        host2 = Mock(is_up=True)
        host3 = Mock(is_up=False)

        mock_cluster.metadata.all_hosts.return_value = [host1, host2, host3]
        mock_session._session.cluster = mock_cluster

        monitor = ConnectionMonitor(mock_session)
        summary = monitor.get_connection_summary()

        assert summary["total_hosts"] == 3
        assert summary["up_hosts"] == 2
        assert summary["down_hosts"] == 1
        assert summary["protocol_version"] == 4
        assert summary["max_requests_per_connection"] == 32768


class TestRateLimitedSession:
    """Test the rate-limited session wrapper."""

    @pytest.mark.asyncio
    async def test_basic_execute(self):
        """Test basic execute with rate limiting."""
        mock_session = Mock()
        mock_session.execute = AsyncMock(return_value=Mock(rows=[{"id": 1}]))

        # Create rate limited session (default 1000 concurrent)
        limited = RateLimitedSession(mock_session, max_concurrent=10)

        result = await limited.execute("SELECT * FROM users")

        assert result.rows == [{"id": 1}]
        mock_session.execute.assert_called_once_with("SELECT * FROM users", None)

    @pytest.mark.asyncio
    async def test_execute_with_parameters(self):
        """Test execute with parameters."""
        mock_session = Mock()
        mock_session.execute = AsyncMock(return_value=Mock(rows=[]))

        limited = RateLimitedSession(mock_session)

        await limited.execute("SELECT * FROM users WHERE id = ?", parameters=[123], timeout=5.0)

        mock_session.execute.assert_called_once_with(
            "SELECT * FROM users WHERE id = ?", [123], timeout=5.0
        )

    @pytest.mark.asyncio
    async def test_prepare_not_rate_limited(self):
        """Test that prepare statements are not rate limited."""
        mock_session = Mock()
        mock_session.prepare = AsyncMock(return_value=Mock())

        limited = RateLimitedSession(mock_session, max_concurrent=1)

        # Should not be delayed
        stmt = await limited.prepare("SELECT * FROM users WHERE id = ?")

        assert stmt is not None
        mock_session.prepare.assert_called_once()

    @pytest.mark.asyncio
    async def test_concurrent_rate_limiting(self):
        """Test rate limiting with concurrent requests."""
        mock_session = Mock()

        # Track concurrent executions
        concurrent_count = 0
        max_concurrent_seen = 0

        async def track_execute(*args, **kwargs):
            nonlocal concurrent_count, max_concurrent_seen
            concurrent_count += 1
            max_concurrent_seen = max(max_concurrent_seen, concurrent_count)
            await asyncio.sleep(0.05)  # Simulate query time
            concurrent_count -= 1
            return Mock(rows=[])

        mock_session.execute = track_execute

        # Very limited concurrency: 2
        limited = RateLimitedSession(mock_session, max_concurrent=2)

        # Try to execute 4 queries concurrently
        tasks = [limited.execute(f"SELECT {i}") for i in range(4)]

        await asyncio.gather(*tasks)

        # Should never exceed max_concurrent
        assert max_concurrent_seen <= 2

    def test_get_metrics(self):
        """Test getting rate limiter metrics."""
        mock_session = Mock()
        limited = RateLimitedSession(mock_session)

        metrics = limited.get_metrics()

        assert metrics["total_requests"] == 0
        assert metrics["active_requests"] == 0
        assert metrics["rejected_requests"] == 0

    @pytest.mark.asyncio
    async def test_metrics_tracking(self):
        """Test that metrics are tracked correctly."""
        mock_session = Mock()
        mock_session.execute = AsyncMock(return_value=Mock())

        limited = RateLimitedSession(mock_session)

        # Execute some queries
        await limited.execute("SELECT 1")
        await limited.execute("SELECT 2")

        metrics = limited.get_metrics()
        assert metrics["total_requests"] == 2
        assert metrics["active_requests"] == 0  # Both completed


class TestIntegration:
    """Test integration of monitoring components."""

    def test_create_metrics_system_memory(self):
        """Test creating metrics system with memory backend."""
        middleware = create_metrics_system(backend="memory")

        assert isinstance(middleware, MetricsMiddleware)
        assert len(middleware.collectors) == 1
        assert isinstance(middleware.collectors[0], InMemoryMetricsCollector)

    def test_create_metrics_system_prometheus(self):
        """Test creating metrics system with prometheus."""
        middleware = create_metrics_system(backend="memory", prometheus_enabled=True)

        assert isinstance(middleware, MetricsMiddleware)
        assert len(middleware.collectors) == 2
        assert isinstance(middleware.collectors[0], InMemoryMetricsCollector)
        assert isinstance(middleware.collectors[1], PrometheusMetricsCollector)

    @pytest.mark.asyncio
    async def test_create_monitored_session(self):
        """Test creating a fully monitored session."""
        # Mock cluster and session creation
        mock_cluster = Mock()
        mock_session = Mock()
        mock_session._session = Mock()
        mock_session._session.cluster = Mock()
        mock_session._session.cluster.metadata = Mock()
        mock_session._session.cluster.metadata.all_hosts.return_value = []
        mock_session.execute = AsyncMock(return_value=Mock())

        mock_cluster.connect = AsyncMock(return_value=mock_session)

        with patch("async_cassandra.cluster.AsyncCluster", return_value=mock_cluster):
            session, monitor = await create_monitored_session(
                contact_points=["127.0.0.1"], keyspace="test", max_concurrent=100, warmup=False
            )

            # Should return rate limited session and monitor
            assert isinstance(session, RateLimitedSession)
            assert isinstance(monitor, ConnectionMonitor)
            assert session.session == mock_session

    @pytest.mark.asyncio
    async def test_create_monitored_session_no_rate_limit(self):
        """Test creating monitored session without rate limiting."""
        # Mock cluster and session creation
        mock_cluster = Mock()
        mock_session = Mock()
        mock_session._session = Mock()
        mock_session._session.cluster = Mock()
        mock_session._session.cluster.metadata = Mock()
        mock_session._session.cluster.metadata.all_hosts.return_value = []

        mock_cluster.connect = AsyncMock(return_value=mock_session)

        with patch("async_cassandra.cluster.AsyncCluster", return_value=mock_cluster):
            session, monitor = await create_monitored_session(
                contact_points=["127.0.0.1"], max_concurrent=None, warmup=False
            )

            # Should return original session (not rate limited)
            assert session == mock_session
            assert isinstance(monitor, ConnectionMonitor)
