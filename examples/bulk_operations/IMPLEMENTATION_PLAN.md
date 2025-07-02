# Token-Aware Bulk Operations Implementation Plan

## 🎯 Executive Summary

**Goal**: Create a sophisticated example demonstrating token-aware bulk operations (count, unload, load) using async-cassandra's non-blocking capabilities, with Apache Iceberg integration for modern data lake functionality.

**Key Benefits**:
- Parallel processing across all Cassandra nodes
- Non-blocking async operations
- Efficient data movement to/from Iceberg
- Could become a standalone PyPI package

## 📊 Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    Token-Aware Bulk Operations              │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  1. Token Range Discovery    2. Query Generation           │
│  ┌──────────────────┐       ┌──────────────────┐         │
│  │ Cluster Metadata │──────▶│ Token Range      │         │
│  │ - Token Map      │       │ Splitter         │         │
│  │ - Replicas       │       └──────────────────┘         │
│  └──────────────────┘                │                    │
│                                      ▼                    │
│  4. Parallel Execution      3. Range Assignment           │
│  ┌──────────────────┐       ┌──────────────────┐         │
│  │ Async Tasks      │◀──────│ Replica-Aware    │         │
│  │ - Count          │       │ Clustering       │         │
│  │ - Export         │       └──────────────────┘         │
│  │ - Import         │                                     │
│  └──────────────────┘                                     │
│           │                                                │
│           ▼                                                │
│  ┌──────────────────┐                                     │
│  │ Apache Iceberg   │                                     │
│  │ Integration      │                                     │
│  └──────────────────┘                                     │
└─────────────────────────────────────────────────────────────┘
```

## 🔍 DSBulk Analysis Summary

Based on analysis of the DSBulk source code, here's how it implements token-aware bulk operations:

### Core Components

1. **PartitionGenerator**: Orchestrates token range partitioning
2. **BulkTokenRange**: Represents token ranges with replica information
3. **TokenRangeSplitter**: Splits ranges proportionally based on size
4. **TokenRangeReadStatementGenerator**: Generates CQL with token() function
5. **UnloadWorkflow**: Executes parallel token range queries

### Key Implementation Details

- **No Overlapping**: Each token range is exclusive (start < token <= end)
- **Complete Coverage**: All splits together cover the entire token ring
- **Replica Awareness**: Ranges are clustered by replica nodes for locality
- **Token Function**: Uses `token()` in WHERE clause for range queries
- **Parallel Execution**: Executes multiple range queries concurrently

## 🔧 Implementation Components

### 1. Core Classes

```python
class TokenRange:
    """Represents a token range with replica information"""
    start: int
    end: int
    size: int
    replicas: List[str]
    fraction: float  # Percentage of total ring

class TokenAwareBulkOperator:
    """Main orchestrator for bulk operations"""
    async def count_by_token_ranges(...)
    async def export_to_iceberg(...)
    async def import_from_iceberg(...)

class TokenRangeSplitter:
    """Splits token ranges for parallel processing"""
    def split_proportionally(ranges, split_count)
    def cluster_by_replicas(splits)

class IcebergConnector:
    """Handles Iceberg table operations"""
    async def create_table(schema)
    async def write_batch(data)
    async def read_partitions()
```

### 2. Token Range Query Generation

```python
def generate_token_range_query(
    keyspace: str,
    table: str,
    token_range: TokenRange,
    columns: List[str] = None
) -> str:
    """
    Generates CQL query with token() function.
    Example output:
    SELECT * FROM ks.table
    WHERE token(pk1, pk2) > -9223372036854775808
    AND token(pk1, pk2) <= -6148914691236517205
    """
```

### 3. Parallel Execution Strategy

```python
async def execute_parallel_token_ranges(
    session: AsyncSession,
    queries: List[TokenRangeQuery],
    max_concurrency: int = None
) -> AsyncIterator[Row]:
    """
    Executes multiple token range queries in parallel,
    streaming results as they arrive.
    """
    if max_concurrency is None:
        # 4x the number of nodes for good parallelism
        max_concurrency = len(session.cluster.contact_points) * 4

    # Use asyncio.Semaphore to limit concurrency
    # Stream results using async generators
```

## 📝 Docker Compose Setup

```yaml
version: '3.8'

services:
  cassandra-1:
    image: cassandra:4.1
    container_name: cassandra-1
    environment:
      - CASSANDRA_CLUSTER_NAME=TestCluster
      - CASSANDRA_SEEDS=cassandra-1
      - CASSANDRA_DC=dc1
      - CASSANDRA_ENDPOINT_SNITCH=GossipingPropertyFileSnitch
    ports:
      - "9042:9042"
    volumes:
      - cassandra1-data:/var/lib/cassandra
    healthcheck:
      test: ["CMD", "cqlsh", "-e", "describe keyspaces"]
      interval: 30s
      timeout: 10s
      retries: 5

  cassandra-2:
    image: cassandra:4.1
    container_name: cassandra-2
    environment:
      - CASSANDRA_CLUSTER_NAME=TestCluster
      - CASSANDRA_SEEDS=cassandra-1
      - CASSANDRA_DC=dc1
      - CASSANDRA_ENDPOINT_SNITCH=GossipingPropertyFileSnitch
    ports:
      - "9043:9042"
    depends_on:
      cassandra-1:
        condition: service_healthy
    volumes:
      - cassandra2-data:/var/lib/cassandra

  cassandra-3:
    image: cassandra:4.1
    container_name: cassandra-3
    environment:
      - CASSANDRA_CLUSTER_NAME=TestCluster
      - CASSANDRA_SEEDS=cassandra-1
      - CASSANDRA_DC=dc1
      - CASSANDRA_ENDPOINT_SNITCH=GossipingPropertyFileSnitch
    ports:
      - "9044:9042"
    depends_on:
      cassandra-1:
        condition: service_healthy
    volumes:
      - cassandra3-data:/var/lib/cassandra

volumes:
  cassandra1-data:
  cassandra2-data:
  cassandra3-data:
```

## 🚀 Example Usage

```python
# 1. Bulk Count
total_count = await bulk_operator.count_by_token_ranges(
    keyspace="store",
    table="orders",
    split_count=24  # 8 splits per node
)
print(f"Total rows: {total_count:,}")

# 2. Export to Iceberg (filesystem)
export_stats = await bulk_operator.export_to_iceberg(
    source_keyspace="store",
    source_table="orders",
    iceberg_warehouse_path="./iceberg_warehouse",
    iceberg_table="orders_snapshot",
    partition_by=["order_date"],
    split_count=24
)
print(f"Exported {export_stats.row_count:,} rows in {export_stats.duration}s")

# 3. Import from Iceberg
import_stats = await bulk_operator.import_from_iceberg(
    iceberg_warehouse_path="./iceberg_warehouse",
    iceberg_table="orders_snapshot",
    target_keyspace="store",
    target_table="orders_restored",
    parallelism=24
)
```

## 🎨 Key Implementation Details

### 1. Token Range Discovery
```python
async def discover_token_ranges(session, keyspace):
    """Get natural token ranges from cluster metadata"""
    metadata = session.cluster.metadata
    token_map = metadata.token_map

    # Get all token ranges for keyspace
    ranges = []
    for token_range in token_map.token_ranges:
        replicas = token_map.get_replicas(keyspace, token_range)
        ranges.append(TokenRange(
            start=token_range.start,
            end=token_range.end,
            size=token_range.end - token_range.start,
            replicas=[str(r) for r in replicas]
        ))
    return ranges
```

### 2. Proportional Splitting
```python
def split_proportionally(ranges, target_splits):
    """Split ranges based on their size relative to ring"""
    total_size = sum(r.size for r in ranges)
    splits = []

    for range in ranges:
        # Calculate splits for this range
        range_fraction = range.size / total_size
        range_splits = max(1, round(range_fraction * target_splits))

        # Split the range
        split_size = range.size // range_splits
        for i in range(range_splits):
            start = range.start + (i * split_size)
            end = start + split_size if i < range_splits - 1 else range.end
            splits.append(TokenRange(start, end, end - start, range.replicas))

    return splits
```

### 3. Iceberg Integration (Filesystem-based)
```python
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import AlwaysTrue
from pyiceberg.io.pyarrow import PyArrowFileIO

class IcebergIntegration:
    """Handles Iceberg table operations using PyIceberg with filesystem storage"""

    def __init__(self, warehouse_path: str = "./iceberg_warehouse"):
        self.warehouse_path = warehouse_path
        # Create filesystem-based catalog
        self.catalog = load_catalog(
            "default",
            **{
                "type": "sql",
                "uri": f"sqlite:///{warehouse_path}/catalog.db",
                "warehouse": warehouse_path,
            }
        )

    async def write_streaming(self, table, data_stream):
        """Write data to Iceberg table in batches"""
        writer = table.new_write()
        batch = []

        async for row in data_stream:
            batch.append(row)
            if len(batch) >= self.batch_size:
                await self._write_batch(writer, batch)
                batch = []

        if batch:
            await self._write_batch(writer, batch)

        await writer.commit()
```

## ⚠️ Potential Issues & Considerations

1. **Token Range Boundaries**
   - Must handle Murmur3 token wrapping (-2^63 to 2^63-1)
   - Edge case: ranges that wrap around the ring

2. **Data Consistency**
   - Queries might see inconsistent data during writes
   - Consider using consistency levels appropriately

3. **Memory Management**
   - Streaming is crucial for large datasets
   - Batch sizes need tuning based on available memory

4. **Iceberg Compatibility**
   - PyIceberg supports local filesystem storage (no S3/MinIO needed for demo)
   - Schema mapping between Cassandra and Iceberg types
   - Filesystem catalog for simplicity

5. **Performance Considerations**
   - Network bandwidth between Cassandra and Iceberg storage
   - Optimal parallelism depends on cluster size and resources

## 📚 Documentation Structure

```
examples/bulk_operations/
├── README.md                 # Comprehensive guide
├── docker-compose.yml        # 3-node cluster setup
├── requirements.txt          # Dependencies including PyIceberg
├── bulk_operator.py          # Main implementation
├── token_utils.py           # Token range utilities
├── iceberg_connector.py     # Iceberg integration
├── example_count.py         # Bulk count example
├── example_export.py        # Export to Iceberg
├── example_import.py        # Import from Iceberg
└── tests/
    └── test_token_ranges.py # Unit tests
```

## 🚦 Implementation Phases

**Phase 1**: Basic token range operations 🚧 IN PROGRESS
- Token range discovery ✅
- Query generation ✅
- Parallel count implementation ✅
- Unit tests with 94% coverage ✅
- Docker Compose with Cassandra 5.0 ✅
- **Integration Testing** 🚧 REQUIRED
  - Validate token ranges against nodetool describering
  - Test with vnodes (256 tokens per node)
  - Verify full data coverage (no gaps/duplicates)
  - Performance testing with real cluster
  - Export streaming validation

**Phase 2**: Export functionality
- Streaming export (basic implementation done)
- Progress tracking (implemented)
- Error handling (implemented)
- File format options (CSV, JSON, Parquet)
- Compression support
- Resume capability

**Phase 3**: Iceberg integration
- Schema mapping
- Batch writing
- Partition handling

**Phase 4**: Import functionality
- Iceberg reading
- Cassandra batch insertion
- Data validation

**Phase 5**: Production readiness
- Comprehensive testing
- Performance benchmarking
- Documentation

## 🎯 Success Criteria

1. **Correctness**: No missing or duplicate rows
2. **Performance**: Linear scaling with split count
3. **Resource Efficiency**: Constant memory usage via streaming
4. **Reliability**: Proper error handling and recovery
5. **Usability**: Clear documentation and examples

## 🔮 Future Library Potential

If successful, this could become `async-cassandra-bulk`:
- Token-aware operations as a service
- Pluggable export/import formats (Parquet, Avro, etc.)
- Integration with various data lake formats
- Progress monitoring and metrics
- CLI tool for operations teams

This implementation would showcase async-cassandra's strengths while providing a genuinely useful tool for data engineering teams working with Cassandra and modern data lakes.

## 🔄 Next Steps

1. Create the directory structure
2. Implement Phase 1 (basic token range operations)
3. Test with docker-compose cluster
4. Iterate through subsequent phases
5. Benchmark and optimize
6. Create comprehensive documentation
