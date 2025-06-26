"""
async-cassandra: Async Python wrapper for the Cassandra Python driver.

This package provides true async/await support for Cassandra operations,
addressing performance limitations when using the official driver with
async frameworks like FastAPI.
"""

try:
    from importlib.metadata import PackageNotFoundError, version

    try:
        __version__ = version("async-cassandra")
    except PackageNotFoundError:
        # Package is not installed
        __version__ = "0.0.0+unknown"
except ImportError:
    # Python < 3.8
    __version__ = "0.0.0+unknown"

__author__ = "AxonOps"
__email__ = "community@axonops.com"

from .cluster import AsyncCluster
from .exceptions import AsyncCassandraError, ConnectionError, QueryError
from .metrics import (
    ConnectionMetrics,
    InMemoryMetricsCollector,
    MetricsCollector,
    MetricsMiddleware,
    PrometheusMetricsCollector,
    QueryMetrics,
    create_metrics_system,
)
from .monitoring import (
    HOST_STATUS_DOWN,
    HOST_STATUS_UNKNOWN,
    HOST_STATUS_UP,
    ClusterMetrics,
    ConnectionMonitor,
    HostMetrics,
    RateLimitedSession,
    create_monitored_session,
)
from .result import AsyncResultSet
from .retry_policy import AsyncRetryPolicy
from .session import AsyncCassandraSession
from .streaming import AsyncStreamingResultSet, StreamConfig, create_streaming_statement

__all__ = [
    "AsyncCassandraSession",
    "AsyncCluster",
    "AsyncCassandraError",
    "ConnectionError",
    "QueryError",
    "AsyncResultSet",
    "AsyncRetryPolicy",
    "ConnectionMonitor",
    "RateLimitedSession",
    "create_monitored_session",
    "HOST_STATUS_UP",
    "HOST_STATUS_DOWN",
    "HOST_STATUS_UNKNOWN",
    "HostMetrics",
    "ClusterMetrics",
    "AsyncStreamingResultSet",
    "StreamConfig",
    "create_streaming_statement",
    "MetricsMiddleware",
    "MetricsCollector",
    "InMemoryMetricsCollector",
    "PrometheusMetricsCollector",
    "QueryMetrics",
    "ConnectionMetrics",
    "create_metrics_system",
]
