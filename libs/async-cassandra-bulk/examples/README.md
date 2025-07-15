# Async Cassandra Bulk Examples

This directory contains examples demonstrating the usage of async-cassandra-bulk library.

## Examples

### 1. Basic Export (`basic_export.py`)

Demonstrates fundamental export operations:
- Connecting to Cassandra cluster
- Counting rows in tables
- Exporting to CSV format
- Exporting to JSON and JSONL formats
- Progress tracking during export
- Exporting specific columns

Run with:
```bash
python basic_export.py
```

### 2. Advanced Export (`advanced_export.py`)

Shows advanced features:
- Large dataset handling with progress tracking
- Checkpointing and resumable exports
- Custom exporter implementation (TSV format)
- Performance tuning comparisons
- Error handling and recovery

Run with:
```bash
python advanced_export.py
```

To test checkpoint/resume functionality:
1. Run the script and interrupt with Ctrl+C during export
2. Run again - it will resume from the checkpoint

## Prerequisites

1. **Cassandra Running**: Examples expect Cassandra on localhost:9042
   ```bash
   # Using Docker
   docker run -d -p 9042:9042 cassandra:4.1

   # Or using existing installation
   cassandra -f
   ```

2. **Dependencies Installed**:
   ```bash
   pip install async-cassandra async-cassandra-bulk
   ```

## Output

Examples create an `export_output/` directory with exported files:
- `users.csv` - Basic CSV export
- `users.json` - Pretty-printed JSON array
- `users.jsonl` - JSON Lines (streaming) format
- `events_large.csv` - Large dataset export
- `events.tsv` - Custom TSV format export

## Common Patterns

### Progress Tracking

```python
def progress_callback(stats):
    print(f"Progress: {stats.progress_percentage:.1f}% "
          f"({stats.rows_processed} rows)")

stats = await operator.export(
    table='my_table',
    output_path='export.csv',
    progress_callback=progress_callback
)
```

### Error Handling

```python
try:
    stats = await operator.export(
        table='my_table',
        output_path='export.csv'
    )
except Exception as e:
    print(f"Export failed: {e}")
    # Handle error appropriately
```

### Performance Tuning

```python
# For large tables, increase concurrency and batch size
stats = await operator.export(
    table='large_table',
    output_path='export.csv',
    concurrency=16,      # More parallel workers
    batch_size=5000,     # Larger batches
    page_size=5000       # Cassandra page size
)
```

## Troubleshooting

1. **Connection Error**: Ensure Cassandra is running and accessible
2. **Keyspace Not Found**: Examples create their own keyspace/tables
3. **Memory Issues**: Reduce batch_size and concurrency for very large exports
4. **Slow Performance**: Increase concurrency (up to number of CPU cores × 2)
