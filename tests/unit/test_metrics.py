"""
Unit tests for metrics module.
"""

from datetime import datetime, timezone
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


class TestQueryMetrics:
    """Test QueryMetrics dataclass."""

    def test_query_metrics_creation(self):
        """Test creating QueryMetrics instance."""
        metrics = QueryMetrics(
            query_hash="SELECT * FROM users",
            duration=0.123,
            success=True,
            error_type=None,
            parameters_count=0,
            result_size=10,
            timestamp=datetime.now(timezone.utc),
        )

        assert metrics.query_hash == "SELECT * FROM users"
        assert metrics.duration == 0.123
        assert metrics.success is True
        assert metrics.error_type is None
        assert metrics.parameters_count == 0
        assert metrics.result_size == 10
        assert isinstance(metrics.timestamp, datetime)

    def test_query_metrics_with_error(self):
        """Test QueryMetrics with error information."""
        metrics = QueryMetrics(
            query_hash="INSERT INTO users",
            duration=0.456,
            success=False,
            error_type="InvalidRequest",
            parameters_count=3,
            result_size=0,
            timestamp=datetime.now(timezone.utc),
        )

        assert metrics.query_hash == "INSERT INTO users"
        assert metrics.duration == 0.456
        assert metrics.success is False
        assert metrics.error_type == "InvalidRequest"
        assert metrics.parameters_count == 3
        assert metrics.result_size == 0

    def test_query_metrics_defaults(self):
        """Test QueryMetrics default values."""
        metrics = QueryMetrics(
            query_hash="DELETE FROM users",
            duration=0.789,
            success=True,
        )

        assert metrics.error_type is None
        assert metrics.parameters_count == 0
        assert metrics.result_size == 0
        assert isinstance(metrics.timestamp, datetime)


class TestConnectionMetrics:
    """Test ConnectionMetrics dataclass."""

    def test_connection_metrics_creation(self):
        """Test creating ConnectionMetrics instance."""
        timestamp = datetime.now(timezone.utc)
        metrics = ConnectionMetrics(
            host="127.0.0.1",
            is_healthy=True,
            last_check=timestamp,
            response_time=0.015,
            error_count=0,
            total_queries=100,
        )

        assert metrics.host == "127.0.0.1"
        assert metrics.is_healthy is True
        assert metrics.last_check == timestamp
        assert metrics.response_time == 0.015
        assert metrics.error_count == 0
        assert metrics.total_queries == 100

    def test_connection_metrics_defaults(self):
        """Test ConnectionMetrics default values."""
        timestamp = datetime.now(timezone.utc)
        metrics = ConnectionMetrics(
            host="192.168.1.1",
            is_healthy=False,
            last_check=timestamp,
            response_time=0.123,
        )

        assert metrics.host == "192.168.1.1"
        assert metrics.is_healthy is False
        assert metrics.error_count == 0
        assert metrics.total_queries == 0


class TestInMemoryMetricsCollector:
    """Test InMemoryMetricsCollector."""

    @pytest.mark.asyncio
    async def test_init(self):
        """Test collector initialization."""
        collector = InMemoryMetricsCollector()
        assert collector.max_entries == 10000
        assert len(collector.query_metrics) == 0
        assert len(collector.connection_metrics) == 0

        # Test with custom max_entries
        collector = InMemoryMetricsCollector(max_entries=100)
        assert collector.max_entries == 100

    @pytest.mark.asyncio
    async def test_record_query(self):
        """Test recording query metrics."""
        collector = InMemoryMetricsCollector()

        metrics = QueryMetrics(
            query_hash="hash123",
            duration=0.1,
            success=True,
            result_size=5,
        )

        await collector.record_query(metrics)

        assert len(collector.query_metrics) == 1
        assert collector.query_counts["hash123"] == 1
        assert len(collector.error_counts) == 0

    @pytest.mark.asyncio
    async def test_record_query_with_error(self):
        """Test recording failed query metrics."""
        collector = InMemoryMetricsCollector()

        metrics = QueryMetrics(
            query_hash="hash456",
            duration=0.2,
            success=False,
            error_type="TimeoutError",
        )

        await collector.record_query(metrics)

        assert len(collector.query_metrics) == 1
        assert collector.query_counts["hash456"] == 1
        assert collector.error_counts["TimeoutError"] == 1

    @pytest.mark.asyncio
    async def test_max_entries_limit(self):
        """Test that collector respects max_entries limit."""
        collector = InMemoryMetricsCollector(max_entries=3)

        # Record 5 queries
        for i in range(5):
            metrics = QueryMetrics(
                query_hash=f"hash{i}",
                duration=0.1,
                success=True,
            )
            await collector.record_query(metrics)

        # Should only keep last 3
        assert len(collector.query_metrics) == 3

        # Verify it's the last 3 queries
        hashes = [m.query_hash for m in collector.query_metrics]
        assert hashes == ["hash2", "hash3", "hash4"]

    @pytest.mark.asyncio
    async def test_record_connection_health(self):
        """Test recording connection health metrics."""
        collector = InMemoryMetricsCollector()

        metrics = ConnectionMetrics(
            host="127.0.0.1",
            is_healthy=True,
            last_check=datetime.now(timezone.utc),
            response_time=0.01,
            error_count=0,
            total_queries=50,
        )

        await collector.record_connection_health(metrics)

        assert "127.0.0.1" in collector.connection_metrics
        assert collector.connection_metrics["127.0.0.1"] == metrics

    @pytest.mark.asyncio
    async def test_get_stats_no_data(self):
        """Test get_stats with no data."""
        collector = InMemoryMetricsCollector()
        stats = await collector.get_stats()

        assert stats == {"message": "No metrics available"}

    @pytest.mark.asyncio
    async def test_get_stats_with_data(self):
        """Test get_stats with query data."""
        collector = InMemoryMetricsCollector()

        # Add some query metrics
        for i in range(3):
            metrics = QueryMetrics(
                query_hash="SELECT",
                duration=0.1 * (i + 1),
                success=True,
                timestamp=datetime.now(timezone.utc),
            )
            await collector.record_query(metrics)

        # Add a failed query
        await collector.record_query(
            QueryMetrics(
                query_hash="INSERT",
                duration=0.5,
                success=False,
                error_type="InvalidRequest",
                timestamp=datetime.now(timezone.utc),
            )
        )

        stats = await collector.get_stats()

        assert "query_performance" in stats
        assert stats["query_performance"]["total_queries"] == 4
        assert stats["query_performance"]["success_rate"] == 0.75
        assert "error_summary" in stats
        assert stats["error_summary"]["InvalidRequest"] == 1
        assert "top_queries" in stats
        assert stats["top_queries"]["SELECT"] == 3


class TestPrometheusMetricsCollector:
    """Test PrometheusMetricsCollector."""

    @pytest.mark.asyncio
    async def test_init_without_prometheus(self):
        """Test initialization when prometheus_client is not available."""
        with patch.dict("sys.modules", {"prometheus_client": None}):
            collector = PrometheusMetricsCollector()
            assert not collector._available
            assert collector.query_duration is None

    @pytest.mark.asyncio
    async def test_record_query_without_prometheus(self):
        """Test recording metrics when prometheus is not available."""
        with patch.dict("sys.modules", {"prometheus_client": None}):
            collector = PrometheusMetricsCollector()

            # Should not raise any errors
            metrics = QueryMetrics(
                query_hash="test",
                duration=0.1,
                success=True,
            )
            await collector.record_query(metrics)

    @pytest.mark.asyncio
    async def test_get_stats_without_prometheus(self):
        """Test get_stats when prometheus is not available."""
        with patch.dict("sys.modules", {"prometheus_client": None}):
            collector = PrometheusMetricsCollector()
            stats = await collector.get_stats()

            assert stats == {"error": "Prometheus client not available"}


class TestMetricsMiddleware:
    """Test MetricsMiddleware."""

    @pytest.mark.asyncio
    async def test_init(self):
        """Test middleware initialization."""
        collector1 = InMemoryMetricsCollector()
        collector2 = InMemoryMetricsCollector()

        middleware = MetricsMiddleware([collector1, collector2])

        assert len(middleware.collectors) == 2
        assert middleware._enabled is True

    @pytest.mark.asyncio
    async def test_enable_disable(self):
        """Test enabling and disabling metrics."""
        middleware = MetricsMiddleware([])

        assert middleware._enabled is True

        middleware.disable()
        assert middleware._enabled is False

        middleware.enable()
        assert middleware._enabled is True

    @pytest.mark.asyncio
    async def test_record_query_metrics(self):
        """Test recording query metrics through middleware."""
        collector = InMemoryMetricsCollector()
        middleware = MetricsMiddleware([collector])

        await middleware.record_query_metrics(
            query="SELECT * FROM users WHERE id = ?",
            duration=0.123,
            success=True,
            parameters_count=1,
            result_size=1,
        )

        assert len(collector.query_metrics) == 1
        recorded = collector.query_metrics[0]
        assert recorded.duration == 0.123
        assert recorded.success is True
        assert recorded.parameters_count == 1
        assert recorded.result_size == 1

    @pytest.mark.asyncio
    async def test_record_query_metrics_disabled(self):
        """Test that metrics are not recorded when disabled."""
        collector = InMemoryMetricsCollector()
        middleware = MetricsMiddleware([collector])
        middleware.disable()

        await middleware.record_query_metrics(
            query="SELECT * FROM users",
            duration=0.1,
            success=True,
        )

        assert len(collector.query_metrics) == 0

    @pytest.mark.asyncio
    async def test_record_connection_metrics(self):
        """Test recording connection metrics through middleware."""
        collector = InMemoryMetricsCollector()
        middleware = MetricsMiddleware([collector])

        await middleware.record_connection_metrics(
            host="127.0.0.1",
            is_healthy=True,
            response_time=0.015,
            error_count=0,
            total_queries=100,
        )

        assert "127.0.0.1" in collector.connection_metrics
        metrics = collector.connection_metrics["127.0.0.1"]
        assert metrics.is_healthy is True
        assert metrics.response_time == 0.015

    @pytest.mark.asyncio
    async def test_normalize_query(self):
        """Test query normalization."""
        middleware = MetricsMiddleware([])

        # Test basic normalization
        query1 = "SELECT * FROM users WHERE id = 123"
        query2 = "SELECT * FROM users WHERE id = 456"

        hash1 = middleware._normalize_query(query1)
        hash2 = middleware._normalize_query(query2)

        # Should produce same hash for similar queries
        assert hash1 == hash2
        assert len(hash1) == 12  # MD5 hash truncated to 12 chars

        # Test string literal normalization
        query3 = "SELECT * FROM users WHERE name = 'John'"
        query4 = "SELECT * FROM users WHERE name = 'Jane'"

        hash3 = middleware._normalize_query(query3)
        hash4 = middleware._normalize_query(query4)

        assert hash3 == hash4

    @pytest.mark.asyncio
    async def test_collector_error_handling(self):
        """Test that collector errors don't break middleware."""
        # Create a collector that raises errors
        bad_collector = Mock()
        bad_collector.record_query = AsyncMock(side_effect=Exception("Collector error"))

        good_collector = InMemoryMetricsCollector()

        middleware = MetricsMiddleware([bad_collector, good_collector])

        # Should not raise, but should log warning
        await middleware.record_query_metrics(
            query="SELECT * FROM users",
            duration=0.1,
            success=True,
        )

        # Good collector should still receive metrics
        assert len(good_collector.query_metrics) == 1


class TestCreateMetricsSystem:
    """Test create_metrics_system factory function."""

    def test_create_memory_backend(self):
        """Test creating metrics with memory backend."""
        metrics = create_metrics_system(backend="memory")

        assert isinstance(metrics, MetricsMiddleware)
        assert len(metrics.collectors) == 1
        assert isinstance(metrics.collectors[0], InMemoryMetricsCollector)

    def test_create_with_prometheus(self):
        """Test creating metrics with prometheus enabled."""
        with patch("async_cassandra.metrics.PrometheusMetricsCollector") as mock_prometheus:
            metrics = create_metrics_system(backend="memory", prometheus_enabled=True)

            assert isinstance(metrics, MetricsMiddleware)
            assert len(metrics.collectors) == 2
            assert isinstance(metrics.collectors[0], InMemoryMetricsCollector)
            mock_prometheus.assert_called_once()

    def test_create_prometheus_only(self):
        """Test creating metrics with only prometheus."""
        with patch("async_cassandra.metrics.PrometheusMetricsCollector") as mock_prometheus:
            metrics = create_metrics_system(backend="none", prometheus_enabled=True)

            assert isinstance(metrics, MetricsMiddleware)
            assert len(metrics.collectors) == 1
            mock_prometheus.assert_called_once()
