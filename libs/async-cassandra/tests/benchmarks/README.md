# Performance Benchmarks

This directory contains performance benchmarks that ensure async-cassandra maintains its performance characteristics and catches any regressions.

## Overview

The benchmarks measure key performance indicators with defined thresholds:
- Query latency (average, P95, P99, max)
- Throughput (queries per second)
- Concurrency handling
- Memory efficiency
- CPU usage
- Streaming performance

## Benchmark Categories

### 1. Query Performance (`test_query_performance.py`)
- Single query latency benchmarks
- Concurrent query throughput
- Async vs sync performance comparison
- Query latency under sustained load
- Prepared statement performance benefits

### 2. Streaming Performance (`test_streaming_performance.py`)
- Memory efficiency vs regular queries
- Streaming throughput for large datasets
- Latency overhead of streaming
- Page-by-page processing performance
- Concurrent streaming operations

### 3. Concurrency Performance (`test_concurrency_performance.py`)
- High concurrency throughput
- Connection pool efficiency
- Resource usage under load
- Operation isolation
- Graceful degradation under overload

## Performance Thresholds

Default performance thresholds are defined in `benchmark_config.py`:

```python
# Query latency thresholds
single_query_max: 100ms
single_query_p99: 50ms
single_query_p95: 30ms
single_query_avg: 20ms

# Throughput thresholds
min_throughput_sync: 50 qps
min_throughput_async: 500 qps

# Concurrency thresholds
max_concurrent_queries: 1000
concurrency_speedup_factor: 5x

# Resource thresholds
max_memory_per_connection: 10MB
max_error_rate: 1%
```

## Running Benchmarks

### Basic Usage

```bash
# Run all benchmarks
pytest tests/benchmarks/ -m benchmark

# Run specific benchmark category
pytest tests/benchmarks/test_query_performance.py -v

# Run with custom markers
pytest tests/benchmarks/ -m "benchmark and not slow"
```

### Using the Benchmark Runner

```bash
# Run benchmarks with report generation
python -m tests.benchmarks.benchmark_runner

# Run with custom output directory
python -m tests.benchmarks.benchmark_runner --output ./results

# Run specific benchmarks
python -m tests.benchmarks.benchmark_runner --markers "benchmark and query"
```

## Interpreting Results

### Success Criteria
- All benchmarks must pass their defined thresholds
- No performance regressions compared to baseline
- Resource usage remains within acceptable limits

### Common Failure Reasons
1. **Latency threshold exceeded**: Query taking longer than expected
2. **Throughput below minimum**: Not achieving required operations/second
3. **Memory overhead too high**: Streaming using too much memory
4. **Error rate exceeded**: Too many failures under load

## Writing New Benchmarks

When adding benchmarks:

1. **Define clear thresholds** based on expected performance
2. **Warm up** before measuring to avoid cold start effects
3. **Measure multiple iterations** for statistical significance
4. **Consider resource usage** not just speed
5. **Test edge cases** like overload conditions

Example structure:
```python
@pytest.mark.benchmark
async def test_new_performance_metric(benchmark_session):
    """
    Benchmark description.

    GIVEN initial conditions
    WHEN operation is performed
    THEN performance should meet thresholds
    """
    thresholds = BenchmarkConfig.DEFAULT_THRESHOLDS

    # Warm up
    # ... warm up code ...

    # Measure performance
    # ... measurement code ...

    # Verify thresholds
    assert metric < threshold, f"Metric {metric} exceeds threshold {threshold}"
```

## CI/CD Integration

Benchmarks should be run:
- On every PR to detect regressions
- Nightly for comprehensive testing
- Before releases to ensure performance

## Performance Monitoring

Results can be tracked over time to identify:
- Performance trends
- Gradual degradation
- Impact of changes
- Optimization opportunities
