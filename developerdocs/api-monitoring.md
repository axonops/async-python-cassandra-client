# Monitoring and Metrics API Reference

This document covers the monitoring and metrics components of async-cassandra.

## Connection Monitoring

### ConnectionMonitor

Monitors connection health and cluster metrics.

```python
ConnectionMonitor(session: AsyncCassandraSession)
```

#### Methods

##### `add_callback`
```python
def add_callback(callback: Callable[[ClusterMetrics], Any]) -> None
```

Add a callback to be called when metrics are collected.

##### `check_host_health`
```python
async def check_host_health(host: Host) -> HostMetrics
```

Check the health of a specific host.

##### `get_cluster_metrics`
```python
async def get_cluster_metrics() -> ClusterMetrics
```

Get comprehensive metrics for the entire cluster.

##### `warmup_connections`
```python
async def warmup_connections() -> None
```

Pre-establish connections to all nodes to avoid cold start latency.

##### `start_monitoring`
```python
async def start_monitoring(interval: int = 60) -> None
```

Start continuous monitoring with specified interval (seconds).

##### `stop_monitoring`
```python
async def stop_monitoring() -> None
```

Stop continuous monitoring.

##### `get_connection_summary`
```python
def get_connection_summary() -> Dict[str, Any]
```

Get a summary of connection status.

### Data Classes

#### HostMetrics
```python
@dataclass
class HostMetrics:
    address: str
    datacenter: Optional[str]
    rack: Optional[str]
    status: str  # "up", "down", or "unknown"
    release_version: Optional[str]
    connection_count: int
    latency_ms: Optional[float] = None
    last_error: Optional[str] = None
    last_check: Optional[datetime] = None
```

#### ClusterMetrics
```python
@dataclass
class ClusterMetrics:
    timestamp: datetime
    cluster_name: Optional[str]
    protocol_version: int
    hosts: List[HostMetrics]
    total_connections: int
    healthy_hosts: int
    unhealthy_hosts: int
    app_metrics: Dict[str, Any] = field(default_factory=dict)
```

### RateLimitedSession

Rate-limited wrapper for AsyncCassandraSession.

```python
RateLimitedSession(
    session: AsyncCassandraSession,
    max_concurrent: int = 1000
)
```

#### Methods

- `execute`: Same as AsyncCassandraSession but rate-limited
- `prepare`: Same as AsyncCassandraSession (not rate-limited)
- `get_metrics()`: Get rate limiting metrics

### Helper Functions

#### `create_monitored_session`
```python
async def create_monitored_session(
    contact_points: List[str],
    keyspace: Optional[str] = None,
    max_concurrent: Optional[int] = None,
    warmup: bool = True
) -> Tuple[Union[RateLimitedSession, AsyncCassandraSession], ConnectionMonitor]
```

Create a monitored and optionally rate-limited session.

### Constants

- `HOST_STATUS_UP = "up"`
- `HOST_STATUS_DOWN = "down"`
- `HOST_STATUS_UNKNOWN = "unknown"`

## Metrics Collection

### MetricsMiddleware

Middleware to automatically collect metrics for async-cassandra operations.

```python
MetricsMiddleware(collectors: List[MetricsCollector])
```

#### Methods

##### `enable`/`disable`
```python
def enable() -> None
def disable() -> None
```

Enable or disable metrics collection.

##### `record_query_metrics`
```python
async def record_query_metrics(
    query: str,
    duration: float,
    success: bool,
    error_type: Optional[str] = None,
    parameters_count: int = 0,
    result_size: int = 0
) -> None
```

Record metrics for a query execution.

##### `record_connection_metrics`
```python
async def record_connection_metrics(
    host: str,
    is_healthy: bool,
    response_time: float,
    error_count: int = 0,
    total_queries: int = 0
) -> None
```

Record connection health metrics.

### Collectors

#### MetricsCollector (Base Class)

Abstract base class for metrics collection backends.

```python
class MetricsCollector:
    async def record_query(self, metrics: QueryMetrics) -> None
    async def record_connection_health(self, metrics: ConnectionMetrics) -> None
    async def get_stats(self) -> Dict[str, Any]
```

#### InMemoryMetricsCollector

In-memory metrics collector for development and testing.

```python
InMemoryMetricsCollector(max_entries: int = 10000)
```

#### PrometheusMetricsCollector

Prometheus metrics collector for production monitoring.

```python
PrometheusMetricsCollector()
```

Creates Prometheus metrics:
- `cassandra_query_duration_seconds` - Histogram
- `cassandra_queries_total` - Counter
- `cassandra_connection_healthy` - Gauge
- `cassandra_errors_total` - Counter

### Data Classes

#### QueryMetrics
```python
@dataclass
class QueryMetrics:
    query_hash: str
    duration: float
    success: bool
    error_type: Optional[str] = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    parameters_count: int = 0
    result_size: int = 0
```

#### ConnectionMetrics
```python
@dataclass
class ConnectionMetrics:
    host: str
    is_healthy: bool
    last_check: datetime
    response_time: float
    error_count: int = 0
    total_queries: int = 0
```

### Helper Functions

#### `create_metrics_system`
```python
def create_metrics_system(
    backend: str = "memory",
    prometheus_enabled: bool = False
) -> MetricsMiddleware
```

Create a metrics system with specified backend.

**Parameters:**
- `backend`: "memory" or "none"
- `prometheus_enabled`: Enable Prometheus metrics

## Streaming Components

### AsyncStreamingResultSet

Async iterator for streaming large result sets.

```python
AsyncStreamingResultSet(
    response_future: ResponseFuture,
    config: Optional[StreamConfig] = None
)
```

#### Methods

- `__aiter__()`: Async iteration over rows
- `pages()`: Async iteration over pages
- `cancel()`: Cancel streaming operation
- `close()`: Close and cleanup resources

#### Properties

- `page_number`: Current page number
- `total_rows_fetched`: Total rows fetched so far

### StreamConfig

Configuration for streaming results.

```python
@dataclass
class StreamConfig:
    fetch_size: int = 1000
    max_pages: Optional[int] = None
    page_callback: Optional[Callable[[int, int], None]] = None
    timeout_seconds: Optional[float] = None
```

### Helper Functions

#### `create_streaming_statement`
```python
def create_streaming_statement(
    query: str,
    fetch_size: int = 1000,
    consistency_level: Optional[ConsistencyLevel] = None
) -> SimpleStatement
```

Create a statement configured for streaming.
