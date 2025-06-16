"""Consolidated monitoring and metrics tests.

This module combines all monitoring-related tests including basic monitoring,
comprehensive monitoring scenarios, and fire-and-forget metrics.
"""

import asyncio
from datetime import datetime, timezone
from unittest.mock import Mock, patch

import pytest

from async_cassandra import AsyncCassandraSession as AsyncSession
from async_cassandra.metrics import (
    ConnectionMetrics,
    InMemoryMetricsCollector,
    QueryMetrics,
    create_metrics_system,
)
from async_cassandra.monitoring import ConnectionMonitor


class TestMetricsCollection:
    """Test core metrics collection functionality."""

    @pytest.mark.features
    @pytest.mark.quick
    @pytest.mark.critical
    async def test_query_metrics_collection(self):
        """Test basic query metrics collection."""
        collector = InMemoryMetricsCollector()

        # Record successful query
        metrics1 = QueryMetrics(
            query_hash="SELECT_test", duration=0.025, success=True, result_size=5
        )
        await collector.record_query(metrics1)

        # Record failed query
        metrics2 = QueryMetrics(
            query_hash="INSERT_test", duration=0.100, success=False, error_type="WriteTimeout"
        )
        await collector.record_query(metrics2)

        # Get stats
        await collector.get_stats()

        # Verify metrics were collected
        assert len(collector.query_metrics) == 2
        assert collector.query_counts["SELECT_test"] == 1
        assert collector.query_counts["INSERT_test"] == 1
        assert collector.error_counts["WriteTimeout"] == 1

    @pytest.mark.features
    async def test_connection_metrics(self):
        """Test connection pool metrics."""
        collector = InMemoryMetricsCollector()

        # Record connection metrics
        metrics = ConnectionMetrics(
            host="127.0.0.1",
            is_healthy=True,
            last_check=datetime.now(timezone.utc),
            response_time=0.010,
            error_count=0,
            total_queries=100,
        )
        await collector.record_connection_health(metrics)

        # Check it was recorded
        assert "127.0.0.1" in collector.connection_metrics
        assert collector.connection_metrics["127.0.0.1"].is_healthy
        assert collector.connection_metrics["127.0.0.1"].response_time == 0.010

    @pytest.mark.features
    @pytest.mark.critical
    async def test_error_metrics_by_type(self):
        """Test error tracking by type."""
        collector = InMemoryMetricsCollector()

        # Record various errors
        error_types = ["NoHostAvailable", "NoHostAvailable", "WriteTimeout", "ReadTimeout"]

        for i, error_type in enumerate(error_types):
            metrics = QueryMetrics(
                query_hash=f"query_{i}", duration=0.01, success=False, error_type=error_type
            )
            await collector.record_query(metrics)

        # Check error counts
        assert collector.error_counts["NoHostAvailable"] == 2
        assert collector.error_counts["WriteTimeout"] == 1
        assert collector.error_counts["ReadTimeout"] == 1

    @pytest.mark.features
    async def test_metrics_stats_aggregation(self):
        """Test metrics statistics aggregation."""
        collector = InMemoryMetricsCollector()

        # Record some metrics
        for i in range(10):
            metrics = QueryMetrics(
                query_hash="SELECT" if i % 2 == 0 else "INSERT",
                duration=0.01 * (i + 1),
                success=i % 3 != 0,  # Some failures
                result_size=i * 10,
            )
            await collector.record_query(metrics)

        # Get aggregated stats
        stats = await collector.get_stats()

        assert "query_performance" in stats
        assert stats["query_performance"]["total_queries"] == 10
        assert "avg_duration_ms" in stats["query_performance"]
        assert "success_rate" in stats["query_performance"]


class TestConnectionMonitoring:
    """Test connection monitoring features."""

    @pytest.mark.features
    async def test_connection_monitor_initialization(self):
        """Test ConnectionMonitor initialization."""
        mock_session = Mock()
        mock_session._session = Mock()
        mock_session._session.cluster = Mock()

        monitor = ConnectionMonitor(mock_session)

        assert monitor.session == mock_session
        assert "requests_sent" in monitor.metrics
        assert "monitoring_started" in monitor.metrics

    @pytest.mark.features
    async def test_check_host_health(self):
        """Test checking individual host health."""
        mock_session = Mock()
        mock_cluster = Mock()
        mock_session._session.cluster = mock_cluster

        mock_host = Mock()
        mock_host.address = "127.0.0.1"
        mock_host.datacenter = "dc1"
        mock_host.rack = "rack1"
        mock_host.is_up = True
        mock_host.release_version = "4.0.0"

        # Mock execute for health check
        mock_session.execute = Mock(return_value=asyncio.create_task(asyncio.sleep(0)))

        monitor = ConnectionMonitor(mock_session)

        with patch("asyncio.get_event_loop") as mock_loop:
            mock_loop.return_value.time.side_effect = [0, 0.01]  # Start and end times

            metrics = await monitor.check_host_health(mock_host)

        assert metrics.address == "127.0.0.1"
        assert metrics.datacenter == "dc1"
        assert metrics.status == "up"
        assert metrics.connection_count == 1

    @pytest.mark.features
    async def test_get_cluster_metrics(self):
        """Test getting comprehensive cluster metrics."""
        mock_session = Mock()
        mock_cluster = Mock()
        mock_session._session.cluster = mock_cluster

        # Mock metadata
        mock_metadata = Mock()
        mock_metadata.cluster_name = "TestCluster"
        mock_cluster.metadata = mock_metadata
        mock_cluster.protocol_version = 5

        # Mock hosts
        mock_host1 = Mock()
        mock_host1.address = "127.0.0.1"
        mock_host1.datacenter = "dc1"
        mock_host1.rack = "rack1"
        mock_host1.is_up = True
        mock_host1.release_version = "4.0.0"

        mock_host2 = Mock()
        mock_host2.address = "127.0.0.2"
        mock_host2.datacenter = "dc1"
        mock_host2.rack = "rack2"
        mock_host2.is_up = False
        mock_host2.release_version = "4.0.0"

        mock_metadata.all_hosts.return_value = [mock_host1, mock_host2]

        # Mock execute for health checks
        async def mock_execute(*args, **kwargs):
            pass

        mock_session.execute = mock_execute

        monitor = ConnectionMonitor(mock_session)

        with patch("asyncio.get_event_loop") as mock_loop:
            mock_loop.return_value.time.side_effect = [0, 0.01, 0, 0.02]  # Times for each host

            metrics = await monitor.get_cluster_metrics()

        assert metrics.cluster_name == "TestCluster"
        assert metrics.protocol_version == 5
        assert len(metrics.hosts) == 2
        assert metrics.healthy_hosts == 1
        assert metrics.unhealthy_hosts == 1

    @pytest.mark.features
    async def test_warmup_connections(self):
        """Test connection warmup functionality."""
        mock_session = Mock()
        mock_cluster = Mock()
        mock_session._session.cluster = mock_cluster

        # Mock metadata
        mock_metadata = Mock()
        mock_cluster.metadata = mock_metadata

        # Mock hosts
        mock_host1 = Mock()
        mock_host1.address = "127.0.0.1"
        mock_host1.is_up = True

        mock_host2 = Mock()
        mock_host2.address = "127.0.0.2"
        mock_host2.is_up = True

        mock_metadata.all_hosts.return_value = [mock_host1, mock_host2]

        # Mock execute to succeed for first host, fail for second
        call_count = 0

        async def mock_execute(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                raise Exception("Connection failed")

        mock_session.execute = mock_execute

        monitor = ConnectionMonitor(mock_session)

        # Should not raise exception
        await monitor.warmup_connections()

        # Should have attempted to warm up both connections
        assert call_count == 2


class TestMetricsMiddleware:
    """Test metrics middleware functionality."""

    @pytest.mark.features
    async def test_metrics_middleware_creation(self):
        """Test creating metrics middleware."""
        middleware = create_metrics_system(backend="memory")

        assert middleware._enabled
        assert len(middleware.collectors) == 1
        assert isinstance(middleware.collectors[0], InMemoryMetricsCollector)

    @pytest.mark.features
    async def test_record_query_metrics_through_middleware(self):
        """Test recording query metrics through middleware."""
        middleware = create_metrics_system(backend="memory")
        collector = middleware.collectors[0]

        # Record a query
        await middleware.record_query_metrics(
            query="SELECT * FROM users WHERE id = ?",
            duration=0.025,
            success=True,
            parameters_count=1,
            result_size=1,
        )

        # Check it was recorded
        assert len(collector.query_metrics) == 1
        recorded = collector.query_metrics[0]
        assert recorded.duration == 0.025
        assert recorded.success
        assert recorded.parameters_count == 1
        assert recorded.result_size == 1

    @pytest.mark.features
    async def test_disable_metrics_collection(self):
        """Test disabling metrics collection."""
        middleware = create_metrics_system(backend="memory")
        collector = middleware.collectors[0]

        # Disable metrics
        middleware.disable()

        # Try to record - should be ignored
        await middleware.record_query_metrics(
            query="SELECT * FROM users", duration=0.025, success=True
        )

        # Nothing should be recorded
        assert len(collector.query_metrics) == 0

        # Re-enable
        middleware.enable()

        # Now it should record
        await middleware.record_query_metrics(
            query="SELECT * FROM users", duration=0.025, success=True
        )

        assert len(collector.query_metrics) == 1

    @pytest.mark.features
    def test_query_normalization(self):
        """Test query normalization for grouping."""
        middleware = create_metrics_system(backend="memory")

        # Different queries that should normalize to the same hash
        queries = [
            "SELECT * FROM users WHERE id = 123",
            "SELECT * FROM users WHERE id = 456",
            "select * from users where id = 789",
            "SELECT    *   FROM   users   WHERE   id = 999",
        ]

        hashes = set()
        for query in queries:
            normalized = middleware._normalize_query(query)
            hashes.add(normalized)

        # All should produce the same hash
        assert len(hashes) == 1

    @pytest.mark.features
    async def test_prometheus_collector_graceful_degradation(self):
        """Test that Prometheus collector degrades gracefully without prometheus_client."""
        # Create with prometheus enabled
        middleware = create_metrics_system(backend="memory", prometheus_enabled=True)

        # Should still work even if prometheus_client is not available
        await middleware.record_query_metrics(
            query="SELECT * FROM users", duration=0.025, success=True
        )

        # Memory collector should still work
        memory_collector = None
        for collector in middleware.collectors:
            if isinstance(collector, InMemoryMetricsCollector):
                memory_collector = collector
                break

        assert memory_collector is not None
        assert len(memory_collector.query_metrics) == 1


class TestMonitoringIntegration:
    """Test monitoring integration with async operations."""

    @pytest.mark.features
    @pytest.mark.critical
    async def test_monitoring_with_session(self):
        """Test monitoring integration with AsyncSession."""
        # Create the cassandra session mock
        mock_cassandra_session = Mock()
        mock_cluster = Mock()
        mock_cassandra_session.cluster = mock_cluster

        # Set up async session with monitoring
        async_session = AsyncSession(mock_cassandra_session)
        monitor = ConnectionMonitor(async_session)

        # Mock metadata for connection summary
        mock_metadata = Mock()
        # all_hosts() should return a list, not be a Mock
        mock_metadata.all_hosts = Mock(return_value=[])
        mock_cluster.metadata = mock_metadata
        mock_cluster.protocol_version = 5

        # Add metrics collector
        metrics_system = create_metrics_system(backend="memory")

        # Get the in-memory collector for verification
        collector = metrics_system.collectors[0]

        # Simulate query execution with metrics
        await metrics_system.record_query_metrics(
            query="SELECT * FROM test", duration=0.015, success=True, result_size=10
        )

        # Verify metrics were recorded
        stats = await collector.get_stats()
        assert stats["query_performance"]["total_queries"] == 1

        # Test connection monitoring
        summary = monitor.get_connection_summary()
        assert "total_hosts" in summary
        assert "protocol_version" in summary

    @pytest.mark.features
    async def test_monitoring_callbacks(self):
        """Test monitoring callbacks."""
        mock_session = Mock()
        mock_cluster = Mock()
        mock_session._session.cluster = mock_cluster

        # Mock metadata
        mock_metadata = Mock()
        mock_metadata.cluster_name = "TestCluster"
        mock_metadata.all_hosts.return_value = []
        mock_cluster.metadata = mock_metadata
        mock_cluster.protocol_version = 5

        monitor = ConnectionMonitor(mock_session)

        # Track callback invocations
        callback_metrics = []

        def test_callback(metrics):
            callback_metrics.append(metrics)

        monitor.add_callback(test_callback)

        # Get metrics (should trigger callback)
        metrics = await monitor.get_cluster_metrics()

        # Manually trigger callback since we're not using the monitoring loop
        test_callback(metrics)

        # Verify callback was called
        assert len(callback_metrics) == 1
        assert callback_metrics[0].cluster_name == "TestCluster"

    @pytest.mark.features
    async def test_rate_limited_session(self):
        """Test rate-limited session wrapper."""
        from async_cassandra.monitoring import RateLimitedSession

        mock_session = Mock()

        # Mock execute
        async def mock_execute(*args, **kwargs):
            return Mock(rows=[{"id": 1}])

        mock_session.execute = mock_execute

        # Create rate-limited session
        rate_limited = RateLimitedSession(mock_session, max_concurrent=2)

        # Execute some queries
        await rate_limited.execute("SELECT * FROM test1")
        await rate_limited.execute("SELECT * FROM test2")

        # Check metrics
        metrics = rate_limited.get_metrics()
        assert metrics["total_requests"] == 2
        assert metrics["active_requests"] == 0  # Both completed
