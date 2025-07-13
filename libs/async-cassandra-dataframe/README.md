# async-cassandra-dataframe

Dask DataFrame integration for Apache Cassandra, built on top of async-cassandra. Read and process Cassandra data at scale using distributed DataFrames.

## Features

- **Streaming/Adaptive Partitioning**: No need to estimate data sizes upfront - partitions are created dynamically based on memory constraints
- **Distributed Processing**: Leverages Dask for parallel processing across multiple workers
- **Memory Safety**: Configurable memory limits per partition prevent OOM errors
- **Comprehensive Type Support**: All Cassandra types including collections, UDTs, and special types
- **Metadata Queries**: Built-in support for WRITETIME and TTL queries
- **Production Ready**: Extensive testing, proper error handling, and memory management

## Installation

```bash
pip install async-cassandra-dataframe
```

## Quick Start

```python
import asyncio
from async_cassandra import AsyncCluster
import async_cassandra_dataframe as cdf

async def main():
    # Connect to Cassandra
    async with AsyncCluster(['localhost']) as cluster:
        async with cluster.connect() as session:
            # Read table as Dask DataFrame
            df = await cdf.read_cassandra_table(
                'myks.users',
                session=session,
                memory_per_partition_mb=128  # Memory limit per partition
            )

            # Perform distributed operations
            result = await df.groupby('country').size().compute()
            print(result)

asyncio.run(main())
```

## Key Concepts

### Streaming/Adaptive Approach

Unlike traditional approaches that require knowing data sizes upfront, this library uses a streaming approach:

```python
# No need to specify partition sizes or counts
df = await cdf.read_cassandra_table(
    'large_table',
    session=session,
    memory_per_partition_mb=256  # Just set memory limit
)
```

The library will:
1. Sample data to estimate row sizes
2. Create partitions that fit within memory limits
3. Stream data in memory-bounded chunks
4. Handle tables of any size without configuration

### Memory Management

Control memory usage per partition:

```python
# For large rows, use smaller partitions
df = await cdf.read_cassandra_table(
    'table_with_large_rows',
    session=session,
    memory_per_partition_mb=64  # Smaller partitions
)

# For small rows, use larger partitions
df = await cdf.read_cassandra_table(
    'table_with_small_rows',
    session=session,
    memory_per_partition_mb=512  # Larger partitions
)
```

### Distributed Execution

Works seamlessly with Dask distributed clusters:

```python
from dask.distributed import Client

# Connect to Dask cluster
async with Client('scheduler-address:8786', asynchronous=True) as client:
    df = await cdf.read_cassandra_table(
        'myks.events',
        session=session,
        client=client  # Use distributed cluster
    )

    # Operations run on cluster
    result = await df.map_partitions(process_partition).compute()
```

## Advanced Usage

### Column Selection

Read only specific columns to reduce memory and network usage:

```python
df = await cdf.read_cassandra_table(
    'users',
    session=session,
    columns=['id', 'name', 'email']
)
```

### Writetime and TTL Queries

Access Cassandra metadata columns:

```python
# Get writetime for specific columns
df = await cdf.read_cassandra_table(
    'audit_log',
    session=session,
    writetime_columns=['data', 'status']
)

# Get TTL for cache management
df = await cdf.read_cassandra_table(
    'cache_table',
    session=session,
    ttl_columns=['cache_data']
)

# Use wildcard for all eligible columns
df = await cdf.read_cassandra_table(
    'events',
    session=session,
    writetime_columns=['*']  # All non-PK columns
)
```

### Partition Control

Override adaptive partitioning when needed:

```python
# Fixed partition count
df = await cdf.read_cassandra_table(
    'predictable_table',
    session=session,
    partition_count=10  # Exactly 10 partitions
)
```

### Filtering

Apply simple filters (executed in Dask, not Cassandra):

```python
df = await cdf.read_cassandra_table(
    'events',
    session=session,
    filter_expr='timestamp > "2024-01-01"'
)
```

## Type Mapping

Cassandra types are mapped to appropriate pandas dtypes:

| Cassandra Type | Pandas Type | Notes |
|----------------|-------------|--------|
| `int`, `smallint`, `tinyint`, `bigint` | `int8/16/32/64` | Size-appropriate |
| `float`, `double` | `float32/64` | Precision preserved |
| `decimal` | `object` (Decimal) | Full precision |
| `text`, `varchar`, `ascii` | `object` (str) | |
| `timestamp` | `datetime64[ns, UTC]` | Always UTC |
| `date` | `datetime64[ns]` | |
| `time` | `timedelta64[ns]` | |
| `boolean` | `bool` | |
| `blob` | `object` (bytes) | |
| `uuid`, `timeuuid` | `object` (UUID) | |
| `list`, `set` | `object` (list) | Sets become lists |
| `map` | `object` (dict) | |
| Empty collections | `None` | Cassandra behavior |

## Performance Considerations

1. **Memory Limits**: Set based on your worker memory and row sizes
2. **Partition Count**: More partitions = more parallelism but also more overhead
3. **Column Selection**: Always select only needed columns
4. **Network**: Large results require good network between Cassandra and Dask workers

## Testing

The library includes comprehensive tests:

```bash
# Run all tests
make test

# Run specific test suites
make test-unit        # Unit tests only
make test-integration # Integration tests (requires Cassandra)
make test-distributed # Distributed tests (requires Dask cluster)
```

## Docker Compose Testing

Test with a full distributed environment:

```bash
# Start Cassandra and Dask cluster
docker-compose -f docker-compose.test.yml up -d

# Run distributed tests
make test-distributed

# Cleanup
docker-compose -f docker-compose.test.yml down
```

## Contributing

1. Follow TDD - write tests first
2. Ensure all tests pass including distributed tests
3. Follow the code style (black, isort, ruff)
4. Update documentation for new features

## License

Same as async-cassandra project.
