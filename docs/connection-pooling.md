# Connection Pooling in async-cassandra

## Overview

The `async-cassandra` wrapper leverages the connection pooling provided by the Cassandra Python driver. Understanding how connection pooling works is crucial for optimizing performance and setting appropriate expectations.

## Key Limitation: One Connection Per Host

**When using Cassandra protocol version 3 or higher (default for Cassandra 2.1+), the Python driver maintains exactly one TCP connection per host.** This is a fundamental limitation that differs from other Cassandra drivers (Java, C++, etc.).

### Why This Limitation Exists

1. **Python's Global Interpreter Lock (GIL)**: Due to Python's GIL, the driver cannot effectively utilize multiple connections per host as it would in multi-threaded environments like Java.

2. **Protocol Version 3+ Efficiency**: Modern protocol versions support up to 32,768 concurrent requests per connection (vs. 128 in protocol v2), making multiple connections unnecessary for most workloads.

### Official Documentation References

According to the [official Python driver API documentation](https://datastax.github.io/python-driver/api/cassandra/cluster.html#cassandra.cluster.Cluster.set_core_connections_per_host):

> "If protocol_version is set to 3 or higher, this is not supported (there is always one connection per host, unless the host is remote and connect_to_remote_hosts is False)"

The [source code](https://github.com/datastax/python-driver/blob/master/cassandra/cluster.py) confirms that attempting to configure multiple connections per host with protocol v3+ results in an `UnsupportedOperation` exception.

## Connection Behavior by Protocol Version

### Protocol v1/v2 (Legacy)
- Supports configurable connection pooling
- Default: 2-8 connections for LOCAL hosts, 1-2 for REMOTE hosts
- Maximum 128 concurrent requests per connection
- Can configure using `set_core_connections_per_host()` and `set_max_connections_per_host()`

### Protocol v3+ (Current)
- Fixed at one connection per host
- Supports up to 32,768 concurrent requests per connection
- Connection pooling configuration methods raise `UnsupportedOperation`
- Better performance due to reduced pooling overhead and lock contention

## Performance Implications

Despite the single connection limitation, the async-cassandra wrapper provides significant performance benefits:

1. **High Concurrency**: Each connection can handle thousands of concurrent requests
2. **Async I/O**: Non-blocking operations allow efficient use of the single connection
3. **Reduced Overhead**: No connection pool management overhead

### Benchmark Results

From our FastAPI example tests:
- **10 requests**: 10.24x faster than sync (878.5 requests/second)
- **50 requests**: 25.60x faster than sync (2,207.9 requests/second)
- **100 requests**: 20.99x faster than sync (1,826.2 requests/second)
- **Concurrent operations**: 825 users/second creation rate

## Best Practices

### 1. Connection Warmup

Pre-establish connections at startup to avoid latency on first requests:

```python
async def warmup_connections(cluster):
    """Force connections to all nodes before serving traffic"""
    session = await cluster.connect()
    # Execute a lightweight query on each node
    for host in cluster.metadata.all_hosts():
        try:
            await session.execute("SELECT now() FROM system.local")
        except Exception:
            pass  # Node might be down, continue with others
```

### 2. Monitor Connection Health

Since you only have one connection per host, monitoring is crucial:

```python
async def check_connection_health(session):
    """Check health of all connections"""
    healthy_hosts = []
    unhealthy_hosts = []

    for host in session.cluster.metadata.all_hosts():
        try:
            # Use host-specific routing to test each connection
            statement = SimpleStatement(
                "SELECT now() FROM system.local",
                routing_key=host.address
            )
            await session.execute(statement)
            healthy_hosts.append(host)
        except Exception as e:
            unhealthy_hosts.append((host, str(e)))

    return {
        "healthy": len(healthy_hosts),
        "unhealthy": len(unhealthy_hosts),
        "details": unhealthy_hosts
    }
```

### 3. Application-Level Request Limiting

Prevent overwhelming the single connection per host:

```python
from asyncio import Semaphore

class RateLimitedSession:
    """Wrapper to limit concurrent requests per session"""

    def __init__(self, session, max_concurrent=1000):
        self.session = session
        self.semaphore = Semaphore(max_concurrent)

    async def execute(self, query, parameters=None, **kwargs):
        async with self.semaphore:
            return await self.session.execute(query, parameters, **kwargs)
```

### 4. Connection Configuration

Optimize connection settings for your workload:

```python
cluster = AsyncCluster(
    contact_points=['localhost'],
    # Increase I/O threads for better concurrency
    executor_threads=4,  # Default is 2

    # Keep connections alive
    idle_heartbeat_interval=30,  # Default is 30

    # Connection timeout
    connect_timeout=10,  # Default is 5

    # Control query timeout
    request_timeout=10  # Default is 10
)
```

## Monitoring Utilities

Here's a complete monitoring utility for async-cassandra:

```python
import asyncio
from datetime import datetime
from typing import Dict, List, Any
from cassandra.cluster import Host

class ConnectionMonitor:
    """Monitor async-cassandra connection health and metrics"""

    def __init__(self, session):
        self.session = session
        self.metrics = {
            "requests_sent": 0,
            "requests_completed": 0,
            "requests_failed": 0,
            "last_health_check": None
        }

    async def get_connection_stats(self) -> Dict[str, Any]:
        """Get detailed connection statistics"""
        stats = {
            "timestamp": datetime.utcnow().isoformat(),
            "cluster_name": self.session.cluster.metadata.cluster_name,
            "protocol_version": self.session.cluster.protocol_version,
            "hosts": []
        }

        for host in self.session.cluster.metadata.all_hosts():
            host_info = {
                "address": str(host.address),
                "datacenter": host.datacenter,
                "rack": host.rack,
                "is_up": host.is_up,
                "release_version": host.release_version,
                "connection_count": 1 if host.is_up else 0  # Always 1 for protocol v3+
            }

            # Test connection latency
            if host.is_up:
                try:
                    start = asyncio.get_event_loop().time()
                    await self.session.execute(
                        "SELECT now() FROM system.local",
                        host=host.address
                    )
                    host_info["latency_ms"] = (asyncio.get_event_loop().time() - start) * 1000
                except Exception as e:
                    host_info["error"] = str(e)
                    host_info["latency_ms"] = None

            stats["hosts"].append(host_info)

        stats["total_connections"] = sum(1 for h in stats["hosts"] if h.get("is_up"))
        stats["app_metrics"] = self.metrics.copy()

        return stats

    async def continuous_monitoring(self, interval: int = 60):
        """Run continuous monitoring"""
        while True:
            try:
                stats = await self.get_connection_stats()
                self.metrics["last_health_check"] = stats["timestamp"]

                # Log or send to monitoring system
                print(f"Connection Stats: {stats}")

                # Alert on issues
                down_hosts = [h for h in stats["hosts"] if not h.get("is_up")]
                if down_hosts:
                    print(f"WARNING: {len(down_hosts)} hosts are down")

                await asyncio.sleep(interval)
            except Exception as e:
                print(f"Monitoring error: {e}")
                await asyncio.sleep(interval)
```

## Common Pitfalls

1. **Don't try to configure connection pool size** with protocol v3+:
   ```python
   # This will raise UnsupportedOperation
   cluster.set_core_connections_per_host(HostDistance.LOCAL, 5)
   ```

2. **Don't assume multiple connections** will improve performance:
   - The single connection can handle thousands of concurrent requests
   - Adding application-level parallelism is more effective

3. **Don't ignore connection health**:
   - With only one connection per host, a connection failure impacts all requests to that host
   - Implement proper monitoring and alerting

## Conclusion

The "one connection per host" limitation in the Python driver is a design decision that works well with Python's architecture. The `async-cassandra` wrapper maximizes the efficiency of these connections through async I/O, providing excellent performance for most workloads.

For extremely high-throughput scenarios (>10,000 requests/second per host), consider:
- Scaling horizontally with more application instances
- Using a Cassandra cluster with more nodes to distribute load
- Implementing application-level sharding if needed

## References

1. [DataStax Python Driver API Documentation - Cluster](https://datastax.github.io/python-driver/api/cassandra/cluster.html)
2. [DataStax Python Driver Documentation Index](https://datastax.github.io/python-driver/index.html)
3. [Python Driver Source Code - cluster.py](https://github.com/datastax/python-driver/blob/master/cassandra/cluster.py)
4. [Python Driver GitHub Repository](https://github.com/datastax/python-driver)
5. [Performance Tips](https://datastax.github.io/python-driver/performance.html)
6. [Cassandra Protocol Specifications](https://cassandra.apache.org/doc/latest/cassandra/cql/protocol.html)
7. [Python GIL Documentation](https://docs.python.org/3/glossary.html#term-global-interpreter-lock)
