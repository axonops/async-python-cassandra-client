# Thread Management in async-cassandra-dataframe

## Overview

The async-cassandra-dataframe library uses multiple threading mechanisms to handle async operations and parallel execution. This document explains the thread usage patterns and best practices for managing threads.

## Thread Sources

### 1. **Cassandra Driver Threads**
The cassandra-driver creates several threads:
- **Task Scheduler**: Manages async operations
- **Connection heartbeat**: Keeps connections alive
- **ThreadPoolExecutor-0_x**: Worker threads for I/O operations

These threads are managed by the driver and are necessary for operation.

### 2. **Dask Worker Threads**
When using Dask delayed execution (default):
- **ThreadPoolExecutor-1_x**: Dask's worker threads
- Created dynamically based on partition count
- Managed by Dask's scheduler

### 3. **CDF Async Threads**
For running async code in sync context:
- **cdf_async__x**: Limited pool of 4 threads
- Reused across multiple operations
- Can be manually cleaned up

### 4. **Asyncio Event Loop Threads**
- **asyncio_x**: Created by various async operations
- **event_loop**: Main event loop threads

## Thread Lifecycle

### Normal Operation
```python
# Initial state: ~1-6 threads (Python + Cassandra driver basics)

# After first read: ~10-15 threads
df = await cdf.read_cassandra_table("keyspace.table", session=session)

# Subsequent reads reuse threads: ~15-25 threads
# Some accumulation is normal due to Dask worker pools
```

### Thread Cleanup

The library implements several mechanisms to limit thread growth:

1. **Shared Thread Pool**: The `cdf_async__` threads are limited to 4 and reused
2. **Context Managers**: Streaming operations use context managers for cleanup
3. **Proper Event Loop Management**: Event loops are closed after use

### Manual Cleanup

For applications that need strict thread management:

```python
from async_cassandra_dataframe.reader import CassandraDataFrameReader

# After finishing all DataFrame operations
CassandraDataFrameReader.cleanup_executor()
```

## Best Practices

### 1. **Long-Running Applications**
- Monitor thread count over time
- Call `cleanup_executor()` during idle periods
- Consider restarting workers periodically

### 2. **High-Concurrency Scenarios**
- Limit `max_concurrent_partitions` to control parallel execution
- Use smaller partition counts to reduce Dask worker threads
- Consider using `use_parallel_execution=True` for better control

### 3. **Memory-Constrained Environments**
- Reduce `memory_per_partition_mb` to create more, smaller partitions
- Use streaming with smaller `page_size` values
- Monitor both thread count and memory usage

## Thread Count Guidelines

Expected thread counts for different scenarios:

| Scenario | Thread Count | Notes |
|----------|--------------|-------|
| Initial startup | 1-6 | Python + basic Cassandra |
| After first read | 10-15 | Driver + Dask + CDF threads |
| Heavy parallel load | 20-30 | Normal for concurrent operations |
| After cleanup | 15-25 | Some Cassandra threads persist |

## Troubleshooting

### High Thread Count (>50)
1. Check for unclosed sessions/clusters
2. Verify Dask isn't creating excessive workers
3. Call `cleanup_executor()` to release CDF threads
4. Consider reducing partition count

### Thread Leaks
1. Ensure all sessions are properly closed
2. Use context managers for all operations
3. Monitor thread names to identify sources
4. Restart application if necessary

## Implementation Details

### Thread Pool Configuration
```python
# CDF uses a limited thread pool
ThreadPoolExecutor(max_workers=4, thread_name_prefix="cdf_async_")
```

### Dask Configuration
```python
# Control Dask parallelism
df = await cdf.read_cassandra_table(
    "table",
    session=session,
    partition_count=10,  # Fewer partitions = fewer threads
    use_parallel_execution=True  # Use async instead of Dask threads
)
```

## Future Improvements

1. **Configurable Thread Pool Size**: Allow users to set max CDF threads
2. **Automatic Cleanup**: Implement periodic cleanup of idle threads
3. **Thread Pool Metrics**: Expose thread pool statistics
4. **Dask Scheduler Options**: Support custom Dask schedulers with better thread management
