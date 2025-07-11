# async-cassandra-bulk

High-performance bulk operations for Apache Cassandra with async/await support.

## Overview

`async-cassandra-bulk` provides efficient bulk data operations for Cassandra databases, including:

- **Parallel exports** with token-aware range splitting
- **Multiple export formats** (CSV, JSON, JSONL)
- **Checkpointing and resumption** for fault tolerance
- **Progress tracking** with real-time statistics
- **Type-safe operations** with full type hints

## Installation

```bash
pip install async-cassandra-bulk
```

## Quick Start

### Count Rows

```python
from async_cassandra import AsyncCluster
from async_cassandra_bulk import BulkOperator

async def count_users():
    async with AsyncCluster(['localhost']) as cluster:
        async with cluster.connect() as session:
            await session.set_keyspace('my_keyspace')

            operator = BulkOperator(session=session)
            count = await operator.count('users')
            print(f"Total users: {count}")
```

### Export to CSV

```python
async def export_users_to_csv():
    async with AsyncCluster(['localhost']) as cluster:
        async with cluster.connect() as session:
            await session.set_keyspace('my_keyspace')

            operator = BulkOperator(session=session)
            stats = await operator.export(
                table='users',
                output_path='users.csv',
                format='csv'
            )

            print(f"Exported {stats.rows_processed} rows")
            print(f"Duration: {stats.duration_seconds:.2f} seconds")
            print(f"Rate: {stats.rows_per_second:.0f} rows/second")
```

### Export with Progress Tracking

```python
def progress_callback(stats):
    print(f"Progress: {stats.progress_percentage:.1f}% "
          f"({stats.rows_processed} rows)")

async def export_with_progress():
    async with AsyncCluster(['localhost']) as cluster:
        async with cluster.connect() as session:
            await session.set_keyspace('my_keyspace')

            operator = BulkOperator(session=session)
            stats = await operator.export(
                table='large_table',
                output_path='export.json',
                format='json',
                progress_callback=progress_callback,
                concurrency=16  # Use 16 parallel workers
            )
```

## Advanced Usage

### Custom Export Formats

```python
from async_cassandra_bulk import BaseExporter, ParallelExporter

class CustomExporter(BaseExporter):
    async def write_header(self, columns):
        # Write custom header
        pass

    async def write_row(self, row):
        # Write row in custom format
        pass

    async def finalize(self):
        # Cleanup and close files
        pass

# Use custom exporter
exporter = CustomExporter(output_path='custom.dat')
parallel = ParallelExporter(
    session=session,
    table='my_table',
    exporter=exporter
)
stats = await parallel.export()
```

### Checkpointing for Large Exports

```python
checkpoint_file = 'export_checkpoint.json'

async def save_checkpoint(state):
    with open(checkpoint_file, 'w') as f:
        json.dump(state, f)

# Start export with checkpointing
operator = BulkOperator(session=session)
stats = await operator.export(
    table='huge_table',
    output_path='huge_export.csv',
    checkpoint_interval=60,  # Save every 60 seconds
    checkpoint_callback=save_checkpoint
)

# Resume from checkpoint if interrupted
if os.path.exists(checkpoint_file):
    with open(checkpoint_file, 'r') as f:
        checkpoint = json.load(f)

    stats = await operator.export(
        table='huge_table',
        output_path='huge_export_resumed.csv',
        resume_from=checkpoint
    )
```

### Export Specific Columns

```python
# Export only specific columns
stats = await operator.export(
    table='users',
    output_path='users_basic.csv',
    columns=['id', 'username', 'email', 'created_at']
)
```

### Export with Filtering

```python
# Export with WHERE clause
count = await operator.count(
    'events',
    where="created_at >= '2024-01-01' AND status = 'active' ALLOW FILTERING"
)

# Note: Export operations use token ranges for efficiency
# and don't support WHERE clauses. Use views or filter post-export.
```

## Export Formats

### CSV Export

```python
stats = await operator.export(
    table='products',
    output_path='products.csv',
    format='csv',
    csv_options={
        'delimiter': ',',
        'null_value': 'NULL',
        'escape_char': '\\',
        'quote_char': '"'
    }
)
```

### JSON Export (Array Mode)

```python
# Export as JSON array: [{"id": 1, ...}, {"id": 2, ...}]
stats = await operator.export(
    table='orders',
    output_path='orders.json',
    format='json',
    json_options={
        'mode': 'array',
        'pretty': True  # Pretty-print with indentation
    }
)
```

### JSON Lines Export (Streaming Mode)

```python
# Export as JSONL: one JSON object per line
stats = await operator.export(
    table='events',
    output_path='events.jsonl',
    format='json',
    json_options={
        'mode': 'objects'  # JSONL format
    }
)
```

## Performance Tuning

### Concurrency Settings

```python
# Adjust based on cluster size and network
stats = await operator.export(
    table='large_table',
    output_path='export.csv',
    concurrency=32,      # Number of parallel workers
    batch_size=5000,     # Rows per batch
    page_size=5000       # Cassandra page size
)
```

### Memory Management

For very large exports, use streaming mode and appropriate batch sizes:

```python
# Memory-efficient export
stats = await operator.export(
    table='billions_of_rows',
    output_path='huge.jsonl',
    format='json',
    json_options={'mode': 'objects'},  # Streaming JSONL
    batch_size=1000,  # Smaller batches
    concurrency=8     # Moderate concurrency
)
```

## Error Handling

```python
from async_cassandra_bulk import BulkOperationError

try:
    stats = await operator.export(
        table='my_table',
        output_path='export.csv'
    )
except BulkOperationError as e:
    print(f"Export failed: {e}")
    # Check partial results
    if hasattr(e, 'stats'):
        print(f"Processed {e.stats.rows_processed} rows before failure")
```

## Type Conversions

The exporters handle Cassandra type conversions automatically:

| Cassandra Type | CSV Format | JSON Format |
|----------------|------------|-------------|
| uuid | String (standard format) | String |
| timestamp | ISO 8601 string | ISO 8601 string |
| date | YYYY-MM-DD | String |
| time | HH:MM:SS.ffffff | String |
| decimal | String representation | Number or string |
| boolean | "true"/"false" | true/false |
| list/set | JSON array string | Array |
| map | JSON object string | Object |
| tuple | JSON array string | Array |

## Requirements

- Python 3.12+
- async-cassandra
- Cassandra 3.0+

## Testing

### Running Tests

The project includes comprehensive unit and integration tests.

#### Unit Tests

Unit tests can be run without any external dependencies:

```bash
make test-unit
```

#### Integration Tests

Integration tests require a real Cassandra instance. The easiest way is to use the Makefile commands which automatically detect Docker or Podman:

```bash
# Run all tests (starts Cassandra automatically)
make test

# Run only integration tests
make test-integration

# Check Cassandra status
make cassandra-status

# Manually start/stop Cassandra
make cassandra-start
make cassandra-stop
```

#### Using an Existing Cassandra Instance

If you have Cassandra running elsewhere:

```bash
export CASSANDRA_CONTACT_POINTS=192.168.1.100
export CASSANDRA_PORT=9042  # optional, defaults to 9042
make test-integration
```

### Code Quality

Before submitting changes, ensure all quality checks pass:

```bash
make lint    # Run all linters
make format  # Auto-format code
```

## License

Apache License 2.0
