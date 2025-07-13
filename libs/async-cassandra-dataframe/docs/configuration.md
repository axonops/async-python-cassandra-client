# Configuration Guide

async-cassandra-dataframe provides several configuration options to tune performance and behavior for your specific workload.

## Thread Pool Configuration

The library uses a thread pool to bridge between async and sync code when working with Dask. You can configure the thread pool size and idle cleanup behavior based on your workload.

### Setting Thread Pool Size

**Via Environment Variable (Recommended for Production)**
```bash
export CDF_THREAD_POOL_SIZE=8
export CDF_THREAD_NAME_PREFIX=my_app_
```

**Programmatically**
```python
from async_cassandra_dataframe.config import config

# Set thread pool size
config.set_thread_pool_size(8)

# Set thread name prefix (useful for debugging)
config.set_thread_name_prefix("my_app_")
```

### Guidelines for Thread Pool Size

- **Default**: 2 threads
- **CPU-bound workloads**: Number of CPU cores
- **I/O-bound workloads**: 2-4x number of CPU cores
- **Memory constrained**: Keep low (2-4 threads)

⚠️ **Note**: Thread pool configuration changes only affect new thread pools created after the change. Existing thread pools continue with their original configuration.

### Automatic Idle Thread Cleanup

The library can automatically clean up idle threads to prevent resource leaks in long-running applications.

**Via Environment Variables**
```bash
# Seconds before idle threads are cleaned up (0 to disable)
export CDF_THREAD_IDLE_TIMEOUT_SECONDS=60

# How often to check for idle threads
export CDF_THREAD_CLEANUP_INTERVAL_SECONDS=30
```

**Benefits of Idle Thread Cleanup**:
- Reduces memory usage in long-running applications
- Prevents thread accumulation during idle periods
- Threads are recreated automatically when needed
- No impact on performance during active periods

## Memory Configuration

Control memory usage per partition to prevent OOM errors:

```bash
# Memory limit per partition (MB)
export CDF_MEMORY_PER_PARTITION_MB=256

# Number of rows to fetch per query
export CDF_FETCH_SIZE=10000
```

## Concurrency Configuration

Control concurrent operations to protect your Cassandra cluster:

```bash
# Max concurrent partitions to read
export CDF_MAX_CONCURRENT_PARTITIONS=20
```

```python
# Limit concurrent queries to Cassandra
df = await cdf.read_cassandra_table(
    "keyspace.table",
    session=session,
    max_concurrent_queries=10  # Limit to 10 concurrent queries
)
```

## All Configuration Options

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `CDF_THREAD_POOL_SIZE` | 2 | Number of threads in the thread pool |
| `CDF_THREAD_NAME_PREFIX` | "cdf_io_" | Prefix for thread names |
| `CDF_THREAD_IDLE_TIMEOUT_SECONDS` | 60 | Seconds before idle threads are cleaned up (0 to disable) |
| `CDF_THREAD_CLEANUP_INTERVAL_SECONDS` | 30 | How often to check for idle threads |
| `CDF_MEMORY_PER_PARTITION_MB` | 128 | Memory limit per partition in MB |
| `CDF_FETCH_SIZE` | 5000 | Rows to fetch per query |
| `CDF_MAX_CONCURRENT_PARTITIONS` | 10 | Max partitions to read concurrently |

## Example: Production Configuration

```bash
# High-throughput configuration
export CDF_THREAD_POOL_SIZE=16
export CDF_MEMORY_PER_PARTITION_MB=512
export CDF_FETCH_SIZE=10000
export CDF_MAX_CONCURRENT_PARTITIONS=20

# Memory-constrained configuration
export CDF_THREAD_POOL_SIZE=4
export CDF_MEMORY_PER_PARTITION_MB=64
export CDF_FETCH_SIZE=1000
export CDF_MAX_CONCURRENT_PARTITIONS=5
```

## Monitoring Thread Pool Usage

You can monitor thread pool usage to optimize configuration:

```python
import threading

# List all threads
for thread in threading.enumerate():
    if thread.name.startswith("cdf_io_"):
        print(f"Thread: {thread.name}, Alive: {thread.is_alive()}")
```
