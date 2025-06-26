"""
Connection monitoring utilities for async-cassandra.

This module provides tools to monitor connection health and performance metrics
for the async-cassandra wrapper. Since the Python driver maintains only one
connection per host, monitoring these connections is crucial.
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from cassandra.cluster import Host
from cassandra.query import SimpleStatement

from .session import AsyncCassandraSession

logger = logging.getLogger(__name__)


# Host status constants
HOST_STATUS_UP = "up"
HOST_STATUS_DOWN = "down"
HOST_STATUS_UNKNOWN = "unknown"


@dataclass
class HostMetrics:
    """Metrics for a single Cassandra host."""

    address: str
    datacenter: Optional[str]
    rack: Optional[str]
    status: str
    release_version: Optional[str]
    connection_count: int  # Always 1 for protocol v3+
    latency_ms: Optional[float] = None
    last_error: Optional[str] = None
    last_check: Optional[datetime] = None


@dataclass
class ClusterMetrics:
    """Metrics for the entire Cassandra cluster."""

    timestamp: datetime
    cluster_name: Optional[str]
    protocol_version: int
    hosts: List[HostMetrics]
    total_connections: int
    healthy_hosts: int
    unhealthy_hosts: int
    app_metrics: Dict[str, Any] = field(default_factory=dict)


class ConnectionMonitor:
    """
    Monitor async-cassandra connection health and metrics.

    Since the Python driver maintains only one connection per host,
    this monitor helps track the health and performance of these
    critical connections.
    """

    def __init__(self, session: AsyncCassandraSession):
        """
        Initialize the connection monitor.

        Args:
            session: The async Cassandra session to monitor
        """
        self.session = session
        self.metrics: Dict[str, Any] = {
            "requests_sent": 0,
            "requests_completed": 0,
            "requests_failed": 0,
            "last_health_check": None,
            "monitoring_started": datetime.now(timezone.utc),
        }
        self._monitoring_task: Optional[asyncio.Task[None]] = None
        self._callbacks: List[Callable[[ClusterMetrics], Any]] = []

    def add_callback(self, callback: Callable[[ClusterMetrics], Any]) -> None:
        """
        Add a callback to be called when metrics are collected.

        Args:
            callback: Function to call with cluster metrics
        """
        self._callbacks.append(callback)

    async def check_host_health(self, host: Host) -> HostMetrics:
        """
        Check the health of a specific host.

        Args:
            host: The host to check

        Returns:
            HostMetrics for the host
        """
        metrics = HostMetrics(
            address=str(host.address),
            datacenter=host.datacenter,
            rack=host.rack,
            status=HOST_STATUS_UP if host.is_up else HOST_STATUS_DOWN,
            release_version=host.release_version,
            connection_count=1 if host.is_up else 0,
        )

        if host.is_up:
            try:
                # Test connection latency with a simple query
                start = asyncio.get_event_loop().time()

                # Create a statement that routes to the specific host
                statement = SimpleStatement(
                    "SELECT now() FROM system.local",
                    # Note: host parameter might not be directly supported,
                    # but we try to measure general latency
                )

                await self.session.execute(statement)

                metrics.latency_ms = (asyncio.get_event_loop().time() - start) * 1000
                metrics.last_check = datetime.now(timezone.utc)

            except Exception as e:
                metrics.status = HOST_STATUS_UNKNOWN
                metrics.last_error = str(e)
                metrics.connection_count = 0
                logger.warning(f"Health check failed for host {host.address}: {e}")

        return metrics

    async def get_cluster_metrics(self) -> ClusterMetrics:
        """
        Get comprehensive metrics for the entire cluster.

        Returns:
            ClusterMetrics with current state
        """
        cluster = self.session._session.cluster

        # Collect metrics for all hosts
        host_metrics = []
        for host in cluster.metadata.all_hosts():
            host_metric = await self.check_host_health(host)
            host_metrics.append(host_metric)

        # Calculate summary statistics
        healthy_hosts = sum(1 for h in host_metrics if h.status == HOST_STATUS_UP)
        unhealthy_hosts = sum(1 for h in host_metrics if h.status != HOST_STATUS_UP)

        return ClusterMetrics(
            timestamp=datetime.now(timezone.utc),
            cluster_name=cluster.metadata.cluster_name,
            protocol_version=cluster.protocol_version,
            hosts=host_metrics,
            total_connections=sum(h.connection_count for h in host_metrics),
            healthy_hosts=healthy_hosts,
            unhealthy_hosts=unhealthy_hosts,
            app_metrics=self.metrics.copy(),
        )

    async def warmup_connections(self) -> None:
        """
        Pre-establish connections to all nodes.

        This is useful to avoid cold start latency on first queries.
        """
        logger.info("Warming up connections to all nodes...")

        cluster = self.session._session.cluster
        successful = 0
        failed = 0

        for host in cluster.metadata.all_hosts():
            if host.is_up:
                try:
                    # Execute a lightweight query to establish connection
                    statement = SimpleStatement("SELECT now() FROM system.local")
                    await self.session.execute(statement)
                    successful += 1
                    logger.debug(f"Warmed up connection to {host.address}")
                except Exception as e:
                    failed += 1
                    logger.warning(f"Failed to warm up connection to {host.address}: {e}")

        logger.info(f"Connection warmup complete: {successful} successful, {failed} failed")

    async def start_monitoring(self, interval: int = 60) -> None:
        """
        Start continuous monitoring.

        Args:
            interval: Seconds between health checks
        """
        if self._monitoring_task and not self._monitoring_task.done():
            logger.warning("Monitoring already running")
            return

        self._monitoring_task = asyncio.create_task(self._monitoring_loop(interval))
        logger.info(f"Started connection monitoring with {interval}s interval")

    async def stop_monitoring(self) -> None:
        """Stop continuous monitoring."""
        if self._monitoring_task:
            self._monitoring_task.cancel()
            try:
                await self._monitoring_task
            except asyncio.CancelledError:
                pass
            logger.info("Stopped connection monitoring")

    async def _monitoring_loop(self, interval: int) -> None:
        """Internal monitoring loop."""
        while True:
            try:
                metrics = await self.get_cluster_metrics()
                self.metrics["last_health_check"] = metrics.timestamp.isoformat()

                # Log summary
                logger.info(
                    f"Cluster health: {metrics.healthy_hosts} healthy, "
                    f"{metrics.unhealthy_hosts} unhealthy hosts"
                )

                # Alert on issues
                if metrics.unhealthy_hosts > 0:
                    logger.warning(f"ALERT: {metrics.unhealthy_hosts} hosts are unhealthy")

                # Call registered callbacks
                for callback in self._callbacks:
                    try:
                        result = callback(metrics)
                        if asyncio.iscoroutine(result):
                            await result
                    except Exception as e:
                        logger.error(f"Callback error: {e}")

                await asyncio.sleep(interval)

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Monitoring error: {e}")
                await asyncio.sleep(interval)

    def get_connection_summary(self) -> Dict[str, Any]:
        """
        Get a summary of connection status.

        Returns:
            Dictionary with connection summary
        """
        cluster = self.session._session.cluster
        hosts = list(cluster.metadata.all_hosts())

        return {
            "total_hosts": len(hosts),
            "up_hosts": sum(1 for h in hosts if h.is_up),
            "down_hosts": sum(1 for h in hosts if not h.is_up),
            "protocol_version": cluster.protocol_version,
            "max_requests_per_connection": 32768 if cluster.protocol_version >= 3 else 128,
            "note": "Python driver maintains 1 connection per host (protocol v3+)",
        }


class RateLimitedSession:
    """
    Rate-limited wrapper for AsyncCassandraSession.

    Since the Python driver is limited to one connection per host,
    this wrapper helps prevent overwhelming those connections.
    """

    def __init__(self, session: AsyncCassandraSession, max_concurrent: int = 1000):
        """
        Initialize rate-limited session.

        Args:
            session: The async session to wrap
            max_concurrent: Maximum concurrent requests
        """
        self.session = session
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.metrics = {"total_requests": 0, "active_requests": 0, "rejected_requests": 0}

    async def execute(self, query: Any, parameters: Any = None, **kwargs: Any) -> Any:
        """Execute a query with rate limiting."""
        async with self.semaphore:
            self.metrics["total_requests"] += 1
            self.metrics["active_requests"] += 1
            try:
                result = await self.session.execute(query, parameters, **kwargs)
                return result
            finally:
                self.metrics["active_requests"] -= 1

    async def prepare(self, query: str) -> Any:
        """Prepare a statement (not rate limited)."""
        return await self.session.prepare(query)

    def get_metrics(self) -> Dict[str, int]:
        """Get rate limiting metrics."""
        return self.metrics.copy()


async def create_monitored_session(
    contact_points: List[str],
    keyspace: Optional[str] = None,
    max_concurrent: Optional[int] = None,
    warmup: bool = True,
) -> Tuple[Union[RateLimitedSession, AsyncCassandraSession], ConnectionMonitor]:
    """
    Create a monitored and optionally rate-limited session.

    Args:
        contact_points: Cassandra contact points
        keyspace: Optional keyspace to use
        max_concurrent: Optional max concurrent requests
        warmup: Whether to warm up connections

    Returns:
        Tuple of (rate_limited_session, monitor)
    """
    from .cluster import AsyncCluster

    # Create cluster and session
    cluster = AsyncCluster(contact_points=contact_points)
    session = await cluster.connect(keyspace)

    # Create monitor
    monitor = ConnectionMonitor(session)

    # Warm up connections if requested
    if warmup:
        await monitor.warmup_connections()

    # Create rate-limited wrapper if requested
    if max_concurrent:
        rate_limited = RateLimitedSession(session, max_concurrent)
        return rate_limited, monitor
    else:
        return session, monitor
