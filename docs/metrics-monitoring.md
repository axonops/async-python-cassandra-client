# Metrics and Monitoring Guide

async-cassandra provides comprehensive metrics collection and monitoring capabilities to help you understand your application's performance and behavior.

## Table of Contents

- [Overview](#overview)
- [Built-in Metrics](#built-in-metrics)
- [In-Memory Metrics Collector](#in-memory-metrics-collector)
- [Prometheus Integration](#prometheus-integration)
- [Connection Monitoring](#connection-monitoring)
- [Custom Metrics Collectors](#custom-metrics-collectors)
- [Metrics Middleware](#metrics-middleware)
- [FastAPI Integration](#fastapi-integration)
- [Grafana Dashboards](#grafana-dashboards)
- [Best Practices](#best-practices)

## Overview

The metrics system in async-cassandra provides:
- Query execution metrics (latency, throughput, errors)
- Connection health monitoring
- Resource usage tracking
- Integration with popular monitoring systems (Prometheus, Grafana)
- Extensible architecture for custom metrics

## Built-in Metrics

### Query Metrics

async-cassandra automatically tracks the following metrics for every query:

```python
from async_cassandra.metrics import QueryMetrics

# Metrics collected for each query:
# - query: The CQL query string
# - duration: Execution time in seconds
# - success: Whether the query succeeded
# - error_type: Type of error if failed (None if successful)
# - timestamp: When the query was executed
# - host: Which Cassandra node handled the query
```

### Connection Metrics

Connection health is monitored with:

```python
from async_cassandra import ConnectionMetrics

# Metrics collected for connections:
# - host: Cassandra node address
# - port: Connection port
# - is_up: Whether the connection is healthy
# - response_time: Health check latency
# - last_check_time: When the connection was last checked
# - error_count: Number of consecutive errors
```

## In-Memory Metrics Collector

The simplest way to start collecting metrics is with the built-in in-memory collector:

```python
import asyncio
from async_cassandra import AsyncCluster
from async_cassandra.metrics import InMemoryMetricsCollector, create_metrics_system

async def main():
    # Create cluster with metrics
    cluster = AsyncCluster(['localhost'])
    collector = InMemoryMetricsCollector(max_queries=1000)

    # Wrap session with metrics middleware
    base_session = await cluster.connect()
    session = create_metrics_system(base_session, [collector])

    # Use session normally - metrics are collected automatically
    await session.execute("SELECT * FROM system.local")

    # Get metrics
    stats = collector.get_stats()
    print(f"Total queries: {stats['total_queries']}")
    print(f"Failed queries: {stats['failed_queries']}")
    print(f"Average latency: {stats['avg_latency']:.3f}s")

    # Get detailed query metrics
    for query, metrics in stats['queries'].items():
        print(f"{query}: {metrics['count']} calls, "
              f"avg {metrics['avg_duration']:.3f}s")

asyncio.run(main())
```

### Clearing Metrics

```python
# Clear all collected metrics
collector.clear()

# The collector automatically limits stored queries
collector = InMemoryMetricsCollector(
    max_queries=100  # Only keep metrics for 100 most recent unique queries
)
```

## Prometheus Integration

For production monitoring, integrate with Prometheus:

```python
from prometheus_client import start_http_server
from async_cassandra.metrics import PrometheusMetricsCollector

# Start Prometheus metrics endpoint
start_http_server(8000)

# Create Prometheus collector
prometheus_collector = PrometheusMetricsCollector()

# Use with session
session = create_metrics_system(base_session, [prometheus_collector])

# Metrics are now available at http://localhost:8000/metrics
```

### Available Prometheus Metrics

```
# Query metrics
cassandra_queries_total{query="SELECT...", status="success"}
cassandra_query_duration_seconds{query="SELECT...", quantile="0.5"}
cassandra_queries_in_flight

# Connection metrics
cassandra_connection_up{host="127.0.0.1", port="9042"}
cassandra_connection_errors_total{host="127.0.0.1"}
cassandra_connection_check_duration_seconds{host="127.0.0.1"}
```

### Custom Labels

```python
# Add custom labels to all metrics
prometheus_collector = PrometheusMetricsCollector(
    labels={
        'app': 'my_service',
        'environment': 'production',
        'region': 'us-east-1'
    }
)
```

## Connection Monitoring

Monitor connection health with the ConnectionMonitor:

```python
from async_cassandra.monitoring import ConnectionMonitor

async def monitor_connections():
    monitor = ConnectionMonitor(session)

    # Get cluster metrics once
    metrics = await monitor.get_cluster_metrics()
    print(f"Healthy hosts: {metrics.healthy_hosts}")
    print(f"Unhealthy hosts: {metrics.unhealthy_hosts}")

    # Start continuous monitoring
    await monitor.start_monitoring(interval=30)  # Check every 30 seconds

    # Later, stop monitoring
    await monitor.stop_monitoring()
```

### Connection Health Callbacks

```python
# Get notified when metrics are collected
async def on_metrics_collected(metrics: ClusterMetrics):
    for host in metrics.hosts:
        if host.status != "up":
            print(f"ALERT: Host {host.address} is {host.status}")
            # Send alert, page on-call, etc.

monitor = ConnectionMonitor(session)
monitor.add_callback(on_metrics_collected)
```

## Custom Metrics Collectors

Create your own metrics collector by implementing the MetricsCollector interface:

```python
from async_cassandra.metrics import MetricsCollector, QueryMetrics
import json

class FileMetricsCollector(MetricsCollector):
    """Collector that writes metrics to a file."""

    def __init__(self, filename: str):
        self.filename = filename
        self.file = open(filename, 'a')

    def record_query(self, metrics: QueryMetrics):
        # Write query metrics to file
        self.file.write(json.dumps({
            'type': 'query',
            'query': metrics.query,
            'duration': metrics.duration,
            'success': metrics.success,
            'timestamp': metrics.timestamp.isoformat()
        }) + '\n')
        self.file.flush()

    def record_connection_health(self, metrics: ConnectionMetrics):
        # Write connection metrics to file
        self.file.write(json.dumps({
            'type': 'connection',
            'host': metrics.host,
            'is_up': metrics.is_up,
            'timestamp': metrics.last_check_time.isoformat()
        }) + '\n')
        self.file.flush()

    def get_stats(self) -> dict:
        return {'filename': self.filename}

    def clear(self):
        self.file.close()
        self.file = open(self.filename, 'w')

# Use custom collector
collector = FileMetricsCollector('/var/log/cassandra_metrics.jsonl')
session = create_metrics_system(base_session, [collector])
```

## Metrics Middleware

The MetricsMiddleware class wraps your session to automatically collect metrics:

```python
from async_cassandra.metrics import MetricsMiddleware

# Create middleware with multiple collectors
middleware = MetricsMiddleware(
    session=base_session,
    collectors=[
        InMemoryMetricsCollector(),
        PrometheusMetricsCollector(),
        FileMetricsCollector('/var/log/metrics.jsonl')
    ]
)

# Use as normal session
result = await middleware.execute("SELECT * FROM users")

# Access collectors
memory_collector = middleware.collectors[0]
stats = memory_collector.get_stats()
```

### Selective Metrics Collection

```python
# Only collect metrics for specific queries
class FilteredCollector(MetricsCollector):
    def __init__(self, pattern: str):
        self.pattern = pattern
        self.metrics = []

    def record_query(self, metrics: QueryMetrics):
        if self.pattern in metrics.query:
            self.metrics.append(metrics)

    # ... rest of implementation

# Only collect SELECT query metrics
collector = FilteredCollector("SELECT")
```

## FastAPI Integration

Complete example integrating metrics with FastAPI:

```python
from fastapi import FastAPI, Response
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from async_cassandra.metrics import (
    InMemoryMetricsCollector,
    PrometheusMetricsCollector,
    create_metrics_system
)

app = FastAPI()

# Global metrics collectors
memory_collector = InMemoryMetricsCollector()
prometheus_collector = PrometheusMetricsCollector()

@app.on_event("startup")
async def startup_event():
    global session
    cluster = AsyncCluster(['localhost'])
    base_session = await cluster.connect()

    # Wrap with metrics
    session = create_metrics_system(
        base_session,
        [memory_collector, prometheus_collector]
    )

@app.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint."""
    return Response(
        generate_latest(),
        media_type=CONTENT_TYPE_LATEST
    )

@app.get("/stats")
async def stats():
    """Application metrics summary."""
    return memory_collector.get_stats()

@app.get("/users/{user_id}")
async def get_user(user_id: str):
    # Metrics collected automatically
    stmt = await session.prepare("SELECT * FROM users WHERE id = ?")
    result = await session.execute(stmt, [user_id])
    return {"user": result.one()}
```

## Grafana Dashboards

Example Grafana dashboard configuration for async-cassandra metrics:

```json
{
  "dashboard": {
    "title": "async-cassandra Metrics",
    "panels": [
      {
        "title": "Query Rate",
        "targets": [{
          "expr": "rate(cassandra_queries_total[5m])"
        }]
      },
      {
        "title": "Query Latency (p95)",
        "targets": [{
          "expr": "histogram_quantile(0.95, cassandra_query_duration_seconds)"
        }]
      },
      {
        "title": "Error Rate",
        "targets": [{
          "expr": "rate(cassandra_queries_total{status='error'}[5m])"
        }]
      },
      {
        "title": "Connection Health",
        "targets": [{
          "expr": "cassandra_connection_up"
        }]
      }
    ]
  }
}
```

### Import Dashboard

1. Save the dashboard JSON to a file
2. In Grafana, go to Dashboards → Import
3. Upload the JSON file
4. Select your Prometheus data source
5. Click Import

## Best Practices

### 1. Use Multiple Collectors

```python
# Development: In-memory for debugging
# Production: Prometheus for monitoring
collectors = []

if settings.DEBUG:
    collectors.append(InMemoryMetricsCollector())

if settings.PROMETHEUS_ENABLED:
    collectors.append(PrometheusMetricsCollector())

session = create_metrics_system(base_session, collectors)
```

### 2. Set Appropriate Retention

```python
# Limit in-memory metrics to prevent memory issues
collector = InMemoryMetricsCollector(
    max_queries=1000  # Keep only last 1000 unique queries
)

# For Prometheus, configure retention in prometheus.yml
# global:
#   scrape_interval: 15s
#   evaluation_interval: 15s
# storage:
#   tsdb:
#     retention.time: 15d
```

### 3. Monitor Key Queries

```python
# Tag important queries for easier monitoring
async def get_user_by_email(email: str):
    # Add comment to identify in metrics
    query = "SELECT * FROM users WHERE email = ? -- get_user_by_email"
    stmt = await session.prepare(query)
    return await session.execute(stmt, [email])
```

### 4. Alert on Anomalies

```yaml
# Prometheus alerting rules
groups:
  - name: cassandra
    rules:
      - alert: HighQueryLatency
        expr: cassandra_query_duration_seconds{quantile="0.95"} > 1
        for: 5m
        annotations:
          summary: "High query latency detected"

      - alert: ConnectionDown
        expr: cassandra_connection_up == 0
        for: 1m
        annotations:
          summary: "Cassandra connection down"
```

### 5. Correlation with Application Metrics

```python
# Include request ID in queries for correlation
async def handle_request(request_id: str):
    # Add request context to query
    query = f"SELECT * FROM users WHERE id = ? -- req:{request_id}"
    stmt = await session.prepare(query)
    result = await session.execute(stmt, [user_id])

    # Can now correlate slow queries with specific requests
```

## Performance Impact

The metrics system is designed for minimal overhead:

- Query metrics: ~0.1ms per query
- Connection monitoring: Runs in background task
- Memory usage: Configurable limits
- Prometheus: Efficient binary protocol

To disable metrics in performance-critical sections:

```python
# Use base session directly (no metrics)
result = await base_session.execute(critical_query)

# Or temporarily disable
with metrics_disabled(session):
    result = await session.execute(critical_query)
```

## Troubleshooting

### Metrics Not Appearing

1. Verify collectors are registered:
```python
print(f"Active collectors: {len(session._middleware.collectors)}")
```

2. Check Prometheus endpoint:
```bash
curl http://localhost:8000/metrics | grep cassandra
```

3. Ensure queries are being executed through wrapped session

### High Memory Usage

1. Reduce max_queries limit:
```python
collector = InMemoryMetricsCollector(max_queries=100)
```

2. Use Prometheus instead of in-memory for production

3. Clear metrics periodically:
```python
async def clear_metrics_task():
    while True:
        await asyncio.sleep(3600)  # Every hour
        memory_collector.clear()
```

### Missing Connection Metrics

Ensure ConnectionMonitor is started:
```python
monitor = ConnectionMonitor(session, collector)
await monitor.start_monitoring()
```
