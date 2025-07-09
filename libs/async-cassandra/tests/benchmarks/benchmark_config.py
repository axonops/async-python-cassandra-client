"""
Configuration and thresholds for performance benchmarks.
"""

from dataclasses import dataclass
from typing import Dict, Optional


@dataclass
class BenchmarkThresholds:
    """Performance thresholds for different operations."""

    # Query latency thresholds (in seconds)
    single_query_max: float = 0.1  # 100ms max for single query
    single_query_p99: float = 0.05  # 50ms for 99th percentile
    single_query_p95: float = 0.03  # 30ms for 95th percentile
    single_query_avg: float = 0.02  # 20ms average

    # Throughput thresholds (queries per second)
    min_throughput_sync: float = 50  # Minimum 50 qps for sync operations
    min_throughput_async: float = 500  # Minimum 500 qps for async operations

    # Concurrency thresholds
    max_concurrent_queries: int = 1000  # Support at least 1000 concurrent queries
    concurrency_speedup_factor: float = 5.0  # Async should be 5x faster than sync

    # Streaming thresholds
    streaming_memory_overhead: float = 1.5  # Max 50% more memory than data size
    streaming_latency_overhead: float = 1.2  # Max 20% slower than regular queries

    # Resource usage thresholds
    max_memory_per_connection: float = 10.0  # Max 10MB per connection
    max_cpu_usage_idle: float = 0.05  # Max 5% CPU when idle

    # Error rate thresholds
    max_error_rate: float = 0.01  # Max 1% error rate under load
    max_timeout_rate: float = 0.001  # Max 0.1% timeout rate


@dataclass
class BenchmarkResult:
    """Result of a benchmark run."""

    name: str
    duration: float
    operations: int
    throughput: float
    latency_avg: float
    latency_p95: float
    latency_p99: float
    latency_max: float
    errors: int
    error_rate: float
    memory_used_mb: float
    cpu_percent: float
    passed: bool
    failure_reason: Optional[str] = None
    metadata: Optional[Dict] = None


class BenchmarkConfig:
    """Configuration for benchmark runs."""

    # Test data configuration
    TEST_KEYSPACE = "benchmark_test"
    TEST_TABLE = "benchmark_data"

    # Data sizes for different benchmark scenarios
    SMALL_DATASET_SIZE = 100
    MEDIUM_DATASET_SIZE = 1000
    LARGE_DATASET_SIZE = 10000

    # Concurrency levels
    LOW_CONCURRENCY = 10
    MEDIUM_CONCURRENCY = 100
    HIGH_CONCURRENCY = 1000

    # Test durations
    QUICK_TEST_DURATION = 5  # seconds
    STANDARD_TEST_DURATION = 30  # seconds
    STRESS_TEST_DURATION = 300  # seconds (5 minutes)

    # Default thresholds
    DEFAULT_THRESHOLDS = BenchmarkThresholds()
