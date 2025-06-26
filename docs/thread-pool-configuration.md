# Configuring the Cassandra Driver Thread Pool

## Overview

The DataStax Cassandra Python driver uses a thread pool executor for I/O operations. This document explains how to configure the thread pool size.

## Default Configuration

By default, the cassandra-driver creates an executor with 2-4 threads (depending on the number of CPU cores):

```python
# Default behavior in cassandra-driver
from concurrent.futures import ThreadPoolExecutor
import multiprocessing

# The driver calculates threads as:
num_threads = min(4, multiprocessing.cpu_count() * 2)
```

## How to Configure Thread Pool Size

The `executor_threads` parameter controls the size of the thread pool:

```python
from async_cassandra import AsyncCluster

# Create cluster with custom thread pool size
cluster = AsyncCluster(
    contact_points=['localhost'],
    executor_threads=16  # Set the number of executor threads
)

# Verify the configuration
print(f"Thread pool size: {cluster._cluster.executor._max_workers}")
```

## Accessing the Thread Pool

You can access the underlying executor to monitor or interact with it:

```python
# Get the executor
executor = cluster._cluster.executor

# Check thread pool size
thread_count = executor._max_workers

# The executor is a standard concurrent.futures.ThreadPoolExecutor
# You can use any ThreadPoolExecutor methods and properties
```

## Important Notes

- The `executor_threads` parameter must be set for each cluster instance
- There is no global default configuration mechanism
- The executor is created when the cluster is initialized and cannot be changed afterwards
- Each thread in the pool can handle one blocking I/O operation at a time
- The async-cassandra wrapper uses this same thread pool for its operations

## Example

```python
import asyncio
from async_cassandra import AsyncCluster

async def main():
    # Create cluster with 8 executor threads
    cluster = AsyncCluster(
        contact_points=['localhost'],
        executor_threads=8
    )

    # Connect and use the cluster
    session = await cluster.connect()

    # Your queries will be executed using the configured thread pool
    result = await session.execute("SELECT * FROM system.local")

    # Cleanup
    await session.close()
    await cluster.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
```

## Tests and Examples

For more examples of thread pool configuration:

- **Unit Tests**: [`tests/unit/test_thread_pool_configuration.py`](../tests/unit/test_thread_pool_configuration.py) - Demonstrates configuration options and verifies behavior
- **Integration Tests**: [`tests/integration/test_thread_pool_configuration.py`](../tests/integration/test_thread_pool_configuration.py) - Shows real-world usage patterns
- **Example Script**: [`examples/thread_pool_configuration.py`](../examples/thread_pool_configuration.py) - Interactive examples comparing different thread pool sizes

## Official Documentation

For more information about the executor threads configuration in the DataStax Python driver:

- **Cluster Parameters**: [DataStax Python Driver - Cluster Class](https://docs.datastax.com/en/developer/python-driver/3.29/api/cassandra/cluster/#cassandra.cluster.Cluster)
- **Execution Profiles**: [DataStax Python Driver - Execution Profiles](https://docs.datastax.com/en/developer/python-driver/3.29/execution_profiles/)
- **Performance Tuning**: [DataStax Python Driver - Performance](https://docs.datastax.com/en/developer/python-driver/3.29/performance/)
