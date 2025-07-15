# async-cassandra-bulk

High-performance bulk operations for Apache Cassandra with async/await support.

## Features

- **Parallel exports** with token-aware range splitting for maximum performance
- **Multiple export formats**: CSV, JSON, and JSON Lines (JSONL)
- **Checkpointing and resumption** for fault-tolerant exports
- **Real-time progress tracking** with detailed statistics
- **Type-safe operations** with full type hints
- **Memory efficient** streaming for large datasets
- **Custom exporters** for specialized formats

## Installation

```bash
pip install async-cassandra-bulk
```

## Quick Start

```python
from async_cassandra import AsyncCluster
from async_cassandra_bulk import BulkOperator

async def export_data():
    async with AsyncCluster(['localhost']) as cluster:
        async with cluster.connect() as session:
            await session.set_keyspace('my_keyspace')

            operator = BulkOperator(session=session)

            # Count rows
            count = await operator.count('users')
            print(f"Total users: {count}")

            # Export to CSV
            stats = await operator.export(
                table='users',
                output_path='users.csv',
                format='csv'
            )
            print(f"Exported {stats.rows_processed} rows in {stats.duration_seconds:.2f}s")
```

## Key Features

### Parallel Processing

Utilizes token range splitting for parallel processing across multiple workers:

```python
stats = await operator.export(
    table='large_table',
    output_path='export.csv',
    concurrency=16  # Use 16 parallel workers
)
```

### Progress Tracking

Monitor export progress in real-time:

```python
def progress_callback(stats):
    print(f"Progress: {stats.progress_percentage:.1f}% ({stats.rows_processed} rows)")

stats = await operator.export(
    table='large_table',
    output_path='export.csv',
    progress_callback=progress_callback
)
```

### Checkpointing

Enable checkpointing for resumable exports:

```python
async def save_checkpoint(state):
    with open('checkpoint.json', 'w') as f:
        json.dump(state, f)

stats = await operator.export(
    table='huge_table',
    output_path='export.csv',
    checkpoint_interval=60,  # Checkpoint every minute
    checkpoint_callback=save_checkpoint
)
```

### Export Formats

Support for multiple output formats:

- **CSV**: Standard comma-separated values
- **JSON**: Complete JSON array
- **JSONL**: Streaming JSON Lines format

```python
# Export as JSON Lines (memory efficient for large datasets)
stats = await operator.export(
    table='events',
    output_path='events.jsonl',
    format='json',
    json_options={'mode': 'objects'}
)
```

## Documentation

- [API Reference](https://github.com/axonops/async-python-cassandra-client/blob/main/libs/async-cassandra-bulk/docs/API.md)
- [Examples](https://github.com/axonops/async-python-cassandra-client/tree/main/libs/async-cassandra-bulk/examples)

## Requirements

- Python 3.12+
- async-cassandra
- Apache Cassandra 3.0+

## License

Apache License 2.0
