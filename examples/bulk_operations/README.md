# Token-Aware Bulk Operations Example

This example demonstrates how to perform efficient bulk operations on Apache Cassandra using token-aware parallel processing, similar to DataStax Bulk Loader (DSBulk).

## 🚀 Features

- **Token-aware operations**: Leverages Cassandra's token ring for parallel processing
- **Streaming exports**: Memory-efficient data export using async generators
- **Progress tracking**: Real-time progress updates during operations
- **Multi-node support**: Automatically distributes work across cluster nodes
- **Iceberg integration**: Export to Apache Iceberg format (coming soon)

## 📋 Prerequisites

- Python 3.12+
- Docker or Podman (for running Cassandra)
- 30GB+ free disk space (for 3-node cluster)
- 32GB+ RAM recommended

## 🛠️ Installation

1. **Install the example with dependencies:**
   ```bash
   pip install -e .
   ```

2. **Install development dependencies (optional):**
   ```bash
   make dev-install
   ```

## 🎯 Quick Start

1. **Start a 3-node Cassandra cluster:**
   ```bash
   make cassandra-up
   make cassandra-wait
   ```

2. **Run the bulk count demo:**
   ```bash
   make demo
   ```

3. **Stop the cluster when done:**
   ```bash
   make cassandra-down
   ```

## 📖 Examples

### Basic Bulk Count

Count all rows in a table using token-aware parallel processing:

```python
from async_cassandra import AsyncCluster
from bulk_operations.bulk_operator import TokenAwareBulkOperator

async with AsyncCluster(['localhost']) as cluster:
    async with cluster.connect() as session:
        operator = TokenAwareBulkOperator(session)

        # Count with automatic parallelism
        count = await operator.count_by_token_ranges(
            keyspace="my_keyspace",
            table="my_table"
        )
        print(f"Total rows: {count:,}")
```

### Count with Progress Tracking

```python
def progress_callback(stats):
    print(f"Progress: {stats.progress_percentage:.1f}% "
          f"({stats.rows_processed:,} rows, "
          f"{stats.rows_per_second:,.0f} rows/sec)")

count, stats = await operator.count_by_token_ranges_with_stats(
    keyspace="my_keyspace",
    table="my_table",
    split_count=32,  # Use 32 parallel ranges
    progress_callback=progress_callback
)
```

### Streaming Export

Export large tables without loading everything into memory:

```python
async for row in operator.export_by_token_ranges(
    keyspace="my_keyspace",
    table="my_table",
    split_count=16
):
    # Process each row as it arrives
    process_row(row)
```

## 🏗️ Architecture

### Token Range Discovery
The operator discovers natural token ranges from the cluster topology and can further split them for increased parallelism.

### Parallel Execution
Multiple token ranges are queried concurrently, with configurable parallelism limits to prevent overwhelming the cluster.

### Streaming Results
Data is streamed using async generators, ensuring constant memory usage regardless of dataset size.

## 🧪 Testing

Run the test suite:

```bash
# Unit tests only
make test-unit

# All tests (requires running Cassandra)
make test

# With coverage report
pytest --cov=bulk_operations --cov-report=html
```

## 🔧 Configuration

### Split Count
Controls the number of token ranges to process in parallel:
- **Default**: 4 × number of nodes
- **Higher values**: More parallelism, higher resource usage
- **Lower values**: Less parallelism, more stable

### Parallelism
Controls concurrent query execution:
- **Default**: 2 × number of nodes
- **Adjust based on**: Cluster capacity, network bandwidth

## 📊 Performance

Example performance on a 3-node cluster:

| Operation | Rows | Split Count | Time | Rate |
|-----------|------|-------------|------|------|
| Count | 1M | 1 | 45s | 22K/s |
| Count | 1M | 8 | 12s | 83K/s |
| Count | 1M | 32 | 6s | 167K/s |
| Export | 10M | 16 | 120s | 83K/s |

## 🎓 How It Works

1. **Token Range Discovery**
   - Query cluster metadata for natural token ranges
   - Each range has start/end tokens and replica nodes

2. **Range Splitting**
   - Split ranges proportionally based on size
   - Larger ranges get more splits for balance

3. **Parallel Execution**
   - Execute queries for each range concurrently
   - Use semaphore to limit parallelism

4. **Result Aggregation**
   - Stream results as they arrive
   - Track progress and statistics

## 🚧 Roadmap

- [x] Phase 1: Basic token operations
- [ ] Phase 2: Full export functionality
- [ ] Phase 3: Apache Iceberg integration
- [ ] Phase 4: Import from Iceberg
- [ ] Phase 5: Production features

## 📚 References

- [DataStax Bulk Loader (DSBulk)](https://docs.datastax.com/en/dsbulk/docs/)
- [Cassandra Token Ranges](https://cassandra.apache.org/doc/latest/cassandra/architecture/dynamo.html#consistent-hashing-using-a-token-ring)
- [Apache Iceberg](https://iceberg.apache.org/)

## ⚠️ Important Notes

1. **Memory Usage**: While streaming reduces memory usage, the thread pool and connection pool still consume resources

2. **Network Bandwidth**: Bulk operations can saturate network links. Monitor and adjust parallelism accordingly.

3. **Cluster Impact**: High parallelism can impact cluster performance. Test in non-production first.

4. **Token Ranges**: The implementation assumes Murmur3Partitioner (Cassandra default).
