# Bulk Operations Feature Analysis for async-python-cassandra

## Executive Summary

This document analyzes the integration of bulk operations functionality into the async-python-cassandra library, inspired by DataStax Bulk Loader (DSBulk). After thorough analysis, I recommend a **monorepo structure** that maintains separation between the core library and bulk operations while enabling coordinated releases and shared infrastructure.

## Current State Analysis

### async-python-cassandra Library
- **Purpose**: Production-grade async wrapper for DataStax Cassandra Python driver
- **Philosophy**: Thin wrapper, minimal overhead, maximum stability
- **Architecture**: Clean separation of concerns with focused modules
- **Testing**: Rigorous TDD with comprehensive test coverage requirements

### Bulk Operations Example Application
The example in `examples/bulk_operations/` demonstrates:
- Token-aware parallel processing for count/export operations
- CSV, JSON, and Parquet export formats
- Progress tracking and resumability
- Memory-efficient streaming
- Iceberg integration (planned)

**Current Limitations**:
1. Limited Cassandra data type support
2. No data loading/import functionality
3. Missing cloud storage integration (S3, GCS, Azure)
4. Incomplete error handling and retry logic
5. No checkpointing/resume capability

### Current Implementation Gaps

The example application demonstrates core concepts but needs significant enhancement:

| Area | Current State | Required for Production |
|------|---------------|------------------------|
| **Operations** | Count, Export only | Need Load/Import |
| **Formats** | CSV, JSON, Parquet | Need Iceberg, cloud formats |
| **Sources** | Local files only | Need S3, GCS, Azure, URLs |
| **Data Types** | Limited subset | All Cassandra 5 types |
| **Checkpointing** | Basic progress tracking | Full resume capability |
| **Parallelization** | Fixed concurrency | Configurable, adaptive |
| **Error Handling** | Basic | Comprehensive retry logic |
| **Auth** | Basic | Kerberos, SSL, SCB for Astra |

## Architectural Considerations

### Option 1: Integration into Core Library ❌

**Pros**:
- Single package to install
- Shared connection management
- Integrated documentation

**Cons**:
- **Violates core principle**: No longer a "thin wrapper"
- **Increased complexity**: 10x more code, harder to maintain
- **Dependency bloat**: Parquet, Iceberg, cloud SDKs
- **Different use cases**: Bulk ops are batch, core is transactional
- **Testing burden**: Bulk ops need different test strategies
- **Stability risk**: Bulk features could destabilize core

### Option 2: Separate Package (`async-cassandra-bulk`) ✅

**Pros**:
- **Clean separation**: Core remains thin and stable
- **Independent evolution**: Can iterate quickly without affecting core
- **Optional dependencies**: Users only install what they need
- **Focused testing**: Different test strategies for different use cases
- **Clear ownership**: Can have different maintainers/release cycles
- **Industry standard**: Similar to pandas/dask, requests/httpx pattern

**Cons**:
- Two packages to install for full functionality
- Potential for version mismatches
- Separate documentation sites

## Recommendation: Create `async-cassandra-bulk`

### Package Structure
```
async-cassandra-bulk/
├── src/
│   └── async_cassandra_bulk/
│       ├── __init__.py
│       ├── operators/
│       │   ├── count.py
│       │   ├── export.py
│       │   └── load.py
│       ├── formats/
│       │   ├── csv.py
│       │   ├── json.py
│       │   ├── parquet.py
│       │   └── iceberg.py
│       ├── storage/
│       │   ├── local.py
│       │   ├── s3.py
│       │   ├── gcs.py
│       │   └── azure.py
│       ├── types/
│       │   └── converters.py
│       └── utils/
│           ├── token_ranges.py
│           ├── checkpointing.py
│           └── progress.py
├── tests/
├── docs/
└── pyproject.toml
```

### Implementation Roadmap

#### Phase 1: Core Foundation (4-6 weeks)
1. **Package Setup**
   - Create new repository/package structure
   - Set up CI/CD, testing framework
   - Establish documentation site

2. **Port Existing Functionality**
   - Token-aware operations framework
   - Count and export operations
   - CSV/JSON format support
   - Progress tracking

3. **Complete Data Type Support**
   - All Cassandra primitive types
   - Collection types (list, set, map)
   - UDTs and tuples
   - Comprehensive type conversion

#### Phase 2: Feature Parity with DSBulk (6-8 weeks)
1. **Load Operations**
   - CSV/JSON import
   - Batch processing
   - Error handling and retry
   - Data validation

2. **Cloud Storage Integration**
   - S3 support (boto3)
   - Google Cloud Storage
   - Azure Blob Storage
   - Generic URL support

3. **Checkpointing & Resume**
   - Checkpoint file format
   - Resume strategies
   - Failure recovery

#### Phase 3: Advanced Features (4-6 weeks)
1. **Modern Data Formats**
   - Apache Iceberg integration
   - Delta Lake support
   - Apache Hudi exploration

2. **Performance Optimizations**
   - Adaptive parallelism
   - Memory management
   - Compression optimization

3. **Enterprise Features**
   - Kerberos authentication
   - Advanced SSL/TLS
   - Astra DB optimization

### Design Principles

1. **Async-First**: Built on async-cassandra's async foundation
2. **Streaming**: Memory-efficient processing of large datasets
3. **Extensible**: Plugin architecture for formats and storage
4. **Resumable**: All operations support checkpointing
5. **Observable**: Comprehensive metrics and progress tracking
6. **Type-Safe**: Full type hints and mypy compliance

### Testing Strategy

Following the core library's standards:
- TDD with comprehensive test coverage
- Unit tests with mocks for storage/format modules
- Integration tests with real Cassandra
- Performance benchmarks against DSBulk
- FastAPI example app for real-world testing

### Dependencies

**Core**:
- async-cassandra (peer dependency)
- aiofiles (async file operations)

**Optional** (extras):
- pandas/pyarrow (Parquet support)
- boto3 (S3 support)
- google-cloud-storage (GCS support)
- azure-storage-blob (Azure support)
- pyiceberg (Iceberg support)

### Example Usage

```python
from async_cassandra import AsyncCluster
from async_cassandra_bulk import BulkOperator

async with AsyncCluster(['localhost']) as cluster:
    async with cluster.connect() as session:
        operator = BulkOperator(session)

        # Count with progress
        count = await operator.count(
            'my_keyspace.my_table',
            progress_callback=lambda p: print(f"{p.percentage:.1f}%")
        )

        # Export to S3
        await operator.export(
            'my_keyspace.my_table',
            's3://my-bucket/cassandra-export.parquet',
            format='parquet',
            compression='snappy'
        )

        # Load from CSV with checkpointing
        await operator.load(
            'my_keyspace.my_table',
            'https://example.com/data.csv.gz',
            format='csv',
            checkpoint='load_progress.json'
        )
```

## Conclusion

Creating a separate `async-cassandra-bulk` package is the right architectural decision. It:
- Preserves the core library's stability and simplicity
- Allows bulk operations to evolve independently
- Provides users with choice and flexibility
- Follows established patterns in the Python ecosystem

The example application provides a solid foundation, but significant work remains to achieve feature parity with DSBulk and meet production requirements.

## Monorepo Structure Recommendation

After analyzing modern Python monorepo practices and the requirements for coordinated releases, I recommend restructuring the project as a monorepo containing both packages. This provides the benefits of separation while enabling synchronized development.

### Proposed Monorepo Structure

```
async-python-cassandra/  # Repository root
├── libs/
│   ├── async-cassandra/          # Core library
│   │   ├── src/
│   │   │   └── async_cassandra/
│   │   ├── tests/
│   │   │   ├── unit/
│   │   │   ├── integration/
│   │   │   └── bdd/
│   │   ├── examples/
│   │   │   ├── basic_usage/
│   │   │   ├── fastapi_app/
│   │   │   └── advanced/
│   │   ├── docs/                  # Detailed library documentation
│   │   ├── pyproject.toml
│   │   └── README_PYPI.md        # Simple README for PyPI only
│   │
│   └── async-cassandra-bulk/     # Bulk operations
│       ├── src/
│       │   └── async_cassandra_bulk/
│       ├── tests/
│       │   ├── unit/
│       │   ├── integration/
│       │   └── performance/
│       ├── examples/
│       │   ├── csv_operations/
│       │   ├── iceberg_export/
│       │   ├── cloud_storage/
│       │   └── migration_from_dsbulk/
│       ├── docs/                  # Detailed library documentation
│       ├── pyproject.toml
│       └── README_PYPI.md        # Simple README for PyPI only
│
├── tools/                        # Shared tooling
│   ├── scripts/
│   └── docker/
│
├── docs/                         # Unified documentation
│   ├── core/
│   └── bulk/
│
├── .github/                      # CI/CD workflows
├── Makefile                      # Root-level commands
├── pyproject.toml               # Workspace configuration
└── README.md
```

### Benefits of Monorepo Approach

1. **Coordinated Releases**: Both packages can be versioned and released together
2. **Shared Infrastructure**: Common CI/CD, testing, and documentation
3. **Atomic Changes**: Breaking changes can be handled in a single PR
4. **Unified Development**: Easier onboarding and consistent tooling
5. **Cross-Package Testing**: Integration tests can span both packages

### Implementation Details

#### Root pyproject.toml (Workspace)
```toml
[tool.poetry]
name = "async-python-cassandra-workspace"
version = "0.1.0"
description = "Workspace for async-python-cassandra monorepo"

[tool.poetry.dependencies]
python = "^3.12"

[tool.poetry.group.dev.dependencies]
pytest = "^7.0.0"
black = "^23.0.0"
ruff = "^0.1.0"
mypy = "^1.0.0"

[build-system]
requires = ["poetry-core>=1.0.0"]
build-backend = "poetry.core.masonry.api"
```

#### Package Management
- Each package maintains its own `pyproject.toml`
- Core library has no dependency on bulk operations
- Bulk operations depends on core library via relative path
- Both packages published to PyPI independently

#### CI/CD Strategy
```yaml
# .github/workflows/release.yml
name: Release
on:
  push:
    tags:
      - 'v*'

jobs:
  release:
    runs-on: ubuntu-latest
    steps:
      - name: Build and publish async-cassandra
        working-directory: libs/async-cassandra
        run: |
          poetry build
          poetry publish

      - name: Build and publish async-cassandra-bulk
        working-directory: libs/async-cassandra-bulk
        run: |
          poetry build
          poetry publish
```

## Apache Iceberg as a Primary Format

### Why Iceberg Matters for Cassandra Bulk Operations

1. **Modern Data Lake Format**: Iceberg is becoming the standard for data lakes
2. **ACID Transactions**: Ensures data consistency during bulk operations
3. **Schema Evolution**: Handles Cassandra schema changes gracefully
4. **Time Travel**: Enables rollback and historical queries
5. **Partition Evolution**: Can reorganize data without rewriting

### Iceberg Integration Design

```python
# Example API for Iceberg export
await operator.export(
    'my_keyspace.my_table',
    format='iceberg',
    catalog={
        'type': 'glue',  # or 'hive', 'filesystem'
        'warehouse': 's3://my-bucket/warehouse'
    },
    table='my_namespace.my_table',
    partition_by=['year', 'month'],  # Optional partitioning
    properties={
        'write.format.default': 'parquet',
        'write.parquet.compression': 'snappy'
    }
)

# Example API for Iceberg import
await operator.load(
    'my_keyspace.my_table',
    format='iceberg',
    catalog={...},
    table='my_namespace.my_table',
    snapshot_id='...',  # Optional: specific snapshot
    filter='year = 2024'  # Optional: partition filter
)
```

### Iceberg Implementation Priorities

1. **Phase 1**: Basic Iceberg export
   - Filesystem catalog support
   - Parquet file format
   - Schema mapping from Cassandra to Iceberg

2. **Phase 2**: Advanced Iceberg features
   - Glue/Hive catalog support
   - Partitioning strategies
   - Incremental exports (CDC-like)
   - **AWS S3 Tables integration** (new priority)

3. **Phase 3**: Full bidirectional support
   - Iceberg to Cassandra import
   - Schema evolution handling
   - Multi-table transactions

## AWS S3 Tables Integration

### Overview
AWS S3 Tables is a new managed storage solution optimized for analytics workloads that provides:
- Built-in Apache Iceberg support (the only supported format)
- 3x faster query throughput and 10x higher TPS vs self-managed tables
- Automatic maintenance (compaction, snapshot management)
- Direct integration with AWS analytics services

### Implementation Approach

#### 1. Direct S3 Tables API Integration
```python
# Using boto3 S3Tables client
import boto3

s3tables = boto3.client('s3tables')

# Create table bucket
s3tables.create_table_bucket(
    name='my-analytics-bucket',
    region='us-east-1'
)

# Create table
s3tables.create_table(
    tableBucketARN='arn:aws:s3tables:...',
    namespace='cassandra_exports',
    name='user_data',
    format='ICEBERG'
)
```

#### 2. PyIceberg REST Catalog Integration
```python
from pyiceberg.catalog import load_catalog

# Configure PyIceberg for S3 Tables
catalog = load_catalog(
    "s3tables_catalog",
    **{
        "type": "rest",
        "warehouse": "arn:aws:s3tables:us-east-1:123456789:bucket/my-bucket",
        "uri": "https://s3tables.us-east-1.amazonaws.com/iceberg",
        "rest.sigv4-enabled": "true",
        "rest.signing-name": "s3tables",
        "rest.signing-region": "us-east-1"
    }
)

# Export Cassandra data to S3 Tables
await operator.export(
    'my_keyspace.my_table',
    format='s3tables',
    catalog=catalog,
    namespace='cassandra_exports',
    table='my_table',
    partition_by=['date', 'region']
)
```

### Benefits for Cassandra Bulk Operations

1. **Managed Infrastructure**: No need to manage Iceberg metadata, compaction, or snapshots
2. **Performance**: Optimized for analytics with automatic query acceleration
3. **Cost Efficiency**: Pay only for storage used, automatic optimization reduces costs
4. **Integration**: Direct access from Athena, EMR, Redshift, QuickSight
5. **Serverless**: No infrastructure to manage, scales automatically

### Required Dependencies

```toml
# In pyproject.toml
[tool.poetry.dependencies.s3tables]
boto3 = ">=1.38.0"  # S3Tables client support
pyiceberg = {version = ">=0.7.0", extras = ["pyarrow", "pandas", "s3fs"]}
aioboto3 = ">=12.0.0"  # Async S3 operations
```

### API Design for S3 Tables Export

```python
# High-level API
await operator.export_to_s3tables(
    source_keyspace='my_keyspace',
    source_table='my_table',
    s3_table_bucket='my-analytics-bucket',
    namespace='cassandra_exports',
    table_name='my_table',
    partition_spec={
        'year': 'timestamp.year()',
        'month': 'timestamp.month()'
    },
    maintenance_config={
        'compaction': {'enabled': True, 'target_file_size_mb': 512},
        'snapshot': {'min_snapshots_to_keep': 3, 'max_snapshot_age_days': 7}
    }
)

# Streaming large tables to S3 Tables
async with operator.stream_to_s3tables(
    source='my_keyspace.my_table',
    destination='s3tables://my-bucket/namespace/table',
    batch_size=100000
) as stream:
    async for progress in stream:
        print(f"Exported {progress.rows_written} rows...")
```

## Detailed Implementation Roadmap

### Phase 1: Repository Restructure & Foundation (Week 1-2)

**Goal**: Restructure to monorepo without breaking existing functionality

#### Tasks:
1. **Repository Structure**
   - Create monorepo directory structure
   - Move existing code to `libs/async-cassandra/src/`
   - Move existing tests to `libs/async-cassandra/tests/`
   - Move fastapi_app example to `libs/async-cassandra/examples/`
   - Create `libs/async-cassandra-bulk/` with proper structure
   - Move bulk_operations example code to `libs/async-cassandra-bulk/examples/`
   - Keep README_PYPI.md files for PyPI publishing (simple, standalone)
   - Create docs/ directories for detailed library documentation
   - Update all imports and paths
   - Ensure all existing tests pass

2. **Build System**
   - Configure Poetry workspaces or similar
   - Set up shared dev dependencies
   - Create root Makefile with commands for both packages
   - Ensure independent package builds

3. **CI/CD Updates**
   - Update GitHub Actions for monorepo
   - Separate test runs for each package
   - Add TestPyPI publication workflow
   - Verify both packages can be built and published

4. **Hello World for async-cassandra-bulk**
   ```python
   # Minimal implementation to verify packaging
   from async_cassandra import AsyncCluster

   class BulkOperator:
       def __init__(self, session):
           self.session = session

       async def hello(self):
           return "Hello from async-cassandra-bulk!"
   ```

5. **Documentation Updates**
   - Update async-cassandra README_PYPI.md to mention async-cassandra-bulk
   - Create async-cassandra-bulk README_PYPI.md with reference to core library
   - Ensure both PyPI pages cross-reference each other

6. **Validation**
   - Test installation from TestPyPI
   - Verify cross-package imports work
   - Ensure no regression in core library

### Phase 2: CSV Implementation with Core Features (Weeks 3-6)

**Goal**: Implement robust CSV export/import with all core functionality

#### 2.1 Core Infrastructure (Week 3)
1. **Token-aware framework**
   - Port token range discovery from example
   - Implement range splitting logic
   - Create parallel execution framework
   - Add progress tracking and stats

2. **Type System Foundation**
   - Create Cassandra type mapping framework
   - Support all Cassandra 5 primitive types
   - Handle NULL values consistently
   - Create extensible type converter registry
   - Writetime and TTL support framework

3. **Testing Infrastructure**
   - Set up integration test framework
   - Create test fixtures for all Cassandra types
   - Add performance benchmarking
   - Follow TDD approach per CLAUDE.md

4. **Metrics, Logging & Callbacks Framework**
   - Structured logging with context (operation_id, table, range)
   - Metrics collection (rows/sec, bytes/sec, errors, latency)
   - Progress callback interface
   - Built-in callback library

#### 2.2 CSV Export Implementation (Week 4)
1. **Basic CSV Export**
   - Streaming export with configurable batch size
   - Memory-efficient processing
   - Proper CSV escaping and quoting
   - Custom delimiter support

2. **Advanced Features**
   - Column selection and ordering
   - Custom NULL representation
   - Header row options
   - Compression support (gzip, bz2)

3. **Concurrency & Performance**
   - Configurable parallelism
   - Backpressure handling
   - Resource pooling
   - Thread safety

4. **Type Mappings for CSV**
   ```python
   # Example type mapping design
   CSV_TYPE_CONVERTERS = {
       'ascii': lambda v: v,
       'bigint': lambda v: str(v),
       'blob': lambda v: base64.b64encode(v).decode('ascii'),
       'boolean': lambda v: 'true' if v else 'false',
       'date': lambda v: v.isoformat(),
       'decimal': lambda v: str(v),
       'double': lambda v: str(v),
       'float': lambda v: str(v),
       'inet': lambda v: str(v),
       'int': lambda v: str(v),
       'text': lambda v: v,
       'time': lambda v: v.isoformat(),
       'timestamp': lambda v: v.isoformat(),
       'timeuuid': lambda v: str(v),
       'uuid': lambda v: str(v),
       'varchar': lambda v: v,
       'varint': lambda v: str(v),
       # Collections
       'list': lambda v: json.dumps(v),
       'set': lambda v: json.dumps(list(v)),
       'map': lambda v: json.dumps(v),
       # UDTs and Tuples
       'udt': lambda v: json.dumps(v._asdict()),
       'tuple': lambda v: json.dumps(v)
   }
   ```

#### 2.3 CSV Import Implementation (Week 5)
1. **Basic CSV Import**
   - Streaming import with batching
   - Type inference and validation
   - Error handling and reporting
   - Prepared statement usage

2. **Advanced Features**
   - Custom type parsers
   - Batch size optimization
   - Retry logic for failures
   - Progress checkpointing

3. **Data Validation**
   - Schema validation
   - Type conversion errors
   - Constraint checking
   - Bad data handling options

#### 2.4 Testing & Documentation (Week 6)
1. **Comprehensive Testing**
   - Unit tests for all components
   - Integration tests with real Cassandra
   - Performance benchmarks
   - Stress tests for large datasets

2. **Documentation**
   - API documentation
   - Usage examples
   - Performance tuning guide
   - Migration from DSBulk guide

### Phase 3: Additional Formats (Weeks 7-10)

**Goal**: Add JSON, Parquet, and Iceberg support with filesystem storage only

#### 3.1 JSON Format (Week 7)
1. **JSON Export**
   - JSON Lines (JSONL) format
   - Pretty-printed JSON array option
   - Streaming for large datasets
   - Complex type preservation

2. **JSON Import**
   - Schema inference
   - Flexible parsing options
   - Nested object handling
   - Error recovery

3. **JSON-Specific Type Mappings**
   - Native JSON type preservation
   - Binary data encoding options
   - Date/time format flexibility
   - Collection handling

#### 3.2 Parquet Format (Week 8)
1. **Parquet Export**
   - PyArrow integration
   - Schema mapping from Cassandra
   - Compression options (snappy, gzip, brotli)
   - Row group size optimization

2. **Parquet Import**
   - Schema validation
   - Type coercion
   - Batch reading
   - Memory management

3. **Parquet-Specific Features**
   - Column pruning
   - Predicate pushdown preparation
   - Statistics generation
   - Metadata preservation

#### 3.3 Apache Iceberg Format (Week 9-10)
1. **Iceberg Export**
   - PyIceberg integration
   - Filesystem catalog only
   - Schema evolution support
   - Partition specification

2. **Iceberg Table Management**
   - Table creation
   - Schema mapping
   - Snapshot management
   - Metadata handling

3. **Iceberg-Specific Features**
   - Time travel preparation
   - Hidden partitioning
   - Sort order configuration
   - Table properties

### Phase 4: Cloud Storage Support (Weeks 11-14)

**Goal**: Add support for cloud storage locations

#### 4.1 Storage Abstraction Layer (Week 11)
1. **Storage Interface**
   - Abstract storage provider
   - Async file operations
   - Streaming uploads/downloads
   - Progress tracking

2. **Local Filesystem**
   - Reference implementation
   - Path handling
   - Permission management
   - Temporary file handling

#### 4.2 AWS S3 Support (Week 12)
1. **S3 Storage Provider**
   - Boto3/aioboto3 integration
   - Multipart upload support
   - IAM role support
   - S3 Transfer acceleration

2. **S3 Tables Integration**
   - Direct S3 Tables API usage
   - PyIceberg REST catalog
   - Automatic table management
   - Maintenance configuration

3. **AWS-Specific Features**
   - Presigned URLs
   - Server-side encryption
   - Object tagging
   - Lifecycle policies

#### 4.3 Azure & GCS Support (Week 13)
1. **Azure Blob Storage**
   - Azure SDK integration
   - SAS token support
   - Managed identity auth
   - Blob tiers

2. **Google Cloud Storage**
   - GCS client integration
   - Service account auth
   - Bucket policies
   - Object metadata

#### 4.4 Integration & Polish (Week 14)
1. **Unified API**
   - URL scheme handling (s3://, gs://, az://)
   - Common configuration
   - Error handling
   - Retry strategies

2. **Performance Optimization**
   - Connection pooling
   - Parallel uploads
   - Bandwidth throttling
   - Cost optimization

### Phase 5: DataStax Astra Support (Weeks 15-16)

**Goal**: Add support for DataStax Astra cloud database

#### 5.1 Astra Integration (Week 15)
1. **Secure Connect Bundle Support**
   - SCB file handling
   - Certificate extraction
   - Cloud configuration

2. **Astra-Specific Features**
   - Rate limiting detection and backoff
   - Astra token authentication
   - Region-aware routing
   - Astra-optimized defaults

3. **Connection Management**
   - Astra connection pooling
   - Automatic retry with backoff
   - Connection health monitoring
   - Failover handling

#### 5.2 Astra Optimizations (Week 16)
1. **Performance Tuning**
   - Astra-specific parallelism limits
   - Adaptive rate limiting
   - Burst handling
   - Cost optimization

2. **Monitoring & Observability**
   - Astra metrics integration
   - Operation tracking dashboard
   - Cost monitoring
   - Performance analytics

3. **Testing & Documentation**
   - Astra-specific test suite
   - Performance benchmarks
   - Cost analysis tools
   - Migration guide from on-prem

## Success Criteria

### Phase 1
- [ ] Monorepo structure working
- [ ] Both packages build independently
- [ ] TestPyPI publication successful
- [ ] No regression in core library
- [ ] Hello world test passes

### Phase 2
- [ ] CSV export/import fully functional
- [ ] All Cassandra 5 types supported
- [ ] Performance meets or exceeds DSBulk
- [ ] 100% test coverage
- [ ] Production-ready error handling

### Phase 3
- [ ] JSON format complete with tests
- [ ] Parquet format complete with tests
- [ ] Iceberg format complete with tests
- [ ] Format comparison benchmarks
- [ ] Documentation for each format

### Phase 4
- [ ] S3 support with S3 Tables
- [ ] Azure Blob support
- [ ] Google Cloud Storage support
- [ ] Unified storage API
- [ ] Cloud cost optimization guide

### Phase 5
- [ ] DataStax Astra support
- [ ] Secure Connect Bundle (SCB) integration
- [ ] Astra-specific optimizations
- [ ] Rate limiting handling
- [ ] Astra streaming support

## Next Steps

1. **Decision**: Confirm monorepo approach with Iceberg as primary format
2. **Restructure**: Migrate to monorepo structure
3. **Tooling**: Set up Poetry/Pants for workspace management
4. **Development**: Begin bulk package implementation
5. **Testing**: Establish cross-package integration tests

This monorepo approach provides the best of both worlds: clean separation of concerns with the benefits of coordinated development and releases.

## Observability & Callback Framework

### Core Design Principles

1. **Structured Logging**
   - Every operation gets a unique operation_id
   - Contextual information (keyspace, table, token range, node)
   - Log levels: DEBUG (detailed), INFO (progress), WARN (issues), ERROR (failures)
   - JSON structured logs for easy parsing

2. **Metrics Collection**
   - Prometheus-compatible metrics
   - Key metrics: rows_processed, bytes_processed, errors, latency_p99
   - Per-operation and global aggregates
   - Integration with async-cassandra's existing metrics

3. **Progress Callback System**
   - Async-friendly callback interface
   - Composable callbacks (chain multiple callbacks)
   - Backpressure-aware (callbacks can slow down processing)
   - Error handling in callbacks doesn't affect main operation

### Built-in Callback Library

```python
# Core callback interface
class BulkOperationCallback(Protocol):
    async def on_progress(self, stats: BulkOperationStats) -> None:
        """Called periodically with progress updates"""

    async def on_range_complete(self, range: TokenRange, rows: int) -> None:
        """Called when a token range is completed"""

    async def on_error(self, error: Exception, range: TokenRange) -> None:
        """Called when an error occurs processing a range"""

    async def on_complete(self, final_stats: BulkOperationStats) -> None:
        """Called when the entire operation completes"""

# Built-in callbacks
class ProgressBarCallback(BulkOperationCallback):
    """Rich progress bar with ETA and throughput"""
    def __init__(self, description: str = "Processing"):
        self.progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TimeRemainingColumn(),
            TransferSpeedColumn(),
        )

class LoggingCallback(BulkOperationCallback):
    """Structured logging of progress"""
    def __init__(self, logger: Logger, log_interval: int = 1000):
        self.logger = logger
        self.log_interval = log_interval

class MetricsCallback(BulkOperationCallback):
    """Prometheus metrics collection"""
    def __init__(self, registry: CollectorRegistry = None):
        self.rows_processed = Counter('bulk_rows_processed_total')
        self.bytes_processed = Counter('bulk_bytes_processed_total')
        self.errors = Counter('bulk_errors_total')
        self.duration = Histogram('bulk_operation_duration_seconds')

class FileProgressCallback(BulkOperationCallback):
    """Write progress to file for external monitoring"""
    def __init__(self, progress_file: Path):
        self.progress_file = progress_file

class WebhookCallback(BulkOperationCallback):
    """Send progress updates to webhook"""
    def __init__(self, webhook_url: str, auth_token: str = None):
        self.webhook_url = webhook_url
        self.auth_token = auth_token

class ThrottlingCallback(BulkOperationCallback):
    """Adaptive throttling based on cluster metrics"""
    def __init__(self, target_cpu: float = 0.7, check_interval: int = 100):
        self.target_cpu = target_cpu
        self.check_interval = check_interval

class CheckpointCallback(BulkOperationCallback):
    """Save progress for resume capability"""
    def __init__(self, checkpoint_file: Path, save_interval: int = 1000):
        self.checkpoint_file = checkpoint_file
        self.save_interval = save_interval

class CompositeCallback(BulkOperationCallback):
    """Combine multiple callbacks"""
    def __init__(self, *callbacks: BulkOperationCallback):
        self.callbacks = callbacks

    async def on_progress(self, stats: BulkOperationStats) -> None:
        await asyncio.gather(*[cb.on_progress(stats) for cb in self.callbacks])
```

### Usage Examples

```python
# Simple progress bar
await operator.export_to_csv(
    'keyspace.table',
    'output.csv',
    progress_callback=ProgressBarCallback("Exporting data")
)

# Production setup with multiple callbacks
callbacks = CompositeCallback(
    ProgressBarCallback("Exporting to S3"),
    LoggingCallback(logger, log_interval=10000),
    MetricsCallback(prometheus_registry),
    CheckpointCallback(Path("export.checkpoint")),
    ThrottlingCallback(target_cpu=0.6)
)

await operator.export_to_s3(
    'keyspace.table',
    's3://bucket/data.parquet',
    progress_callback=callbacks
)

# Custom callback
class SlackNotificationCallback(BulkOperationCallback):
    def __init__(self, webhook_url: str, notify_every: int = 1000000):
        self.webhook_url = webhook_url
        self.notify_every = notify_every
        self.last_notified = 0

    async def on_progress(self, stats: BulkOperationStats) -> None:
        if stats.rows_processed - self.last_notified >= self.notify_every:
            await self._send_slack_message(
                f"Processed {stats.rows_processed:,} rows "
                f"({stats.progress_percentage:.1f}% complete)"
            )
            self.last_notified = stats.rows_processed
```

### Logging Structure

```json
{
  "timestamp": "2024-01-15T10:30:45.123Z",
  "level": "INFO",
  "operation_id": "bulk_export_123456",
  "operation_type": "export",
  "keyspace": "my_keyspace",
  "table": "my_table",
  "format": "parquet",
  "destination": "s3://bucket/data.parquet",
  "token_range": {
    "start": -9223372036854775808,
    "end": -4611686018427387904
  },
  "progress": {
    "rows_processed": 1500000,
    "bytes_processed": 536870912,
    "ranges_completed": 45,
    "total_ranges": 128,
    "percentage": 35.2,
    "rows_per_second": 125000,
    "eta_seconds": 240
  },
  "node": "10.0.0.5",
  "message": "Completed token range"
}
```

## Writetime and TTL Support

### Overview

Writetime (and TTL) support is essential for:
- Data migrations preserving original timestamps
- Backup and restore operations
- Compliance with data retention policies
- Maintaining data lineage

### Cassandra Writetime Limitations

1. **Writetime is per-column**: Not per-row, each non-primary key column can have different writetimes
2. **Not supported on**:
   - Primary key columns
   - Collections (list, set, map) - entire collection
   - Counter columns
   - Static columns in some contexts
3. **Collection elements**: Individual elements can have writetimes (e.g., map entries)
4. **Precision**: Microseconds since epoch (not milliseconds)

### Implementation Design

#### Export with Writetime

```python
# API Design
await operator.export_to_csv(
    'keyspace.table',
    'output.csv',
    include_writetime=True,  # Add writetime columns
    writetime_suffix='_writetime',  # Column naming
    include_ttl=True,  # Also export TTL
    ttl_suffix='_ttl'
)

# Output CSV structure
# id,name,email,name_writetime,email_writetime,name_ttl,email_ttl
# 123,John,john@example.com,1705325400000000,1705325400000000,86400,86400
```

#### Import with Writetime

```python
# API Design
await operator.import_from_csv(
    'keyspace.table',
    'input.csv',
    writetime_column='_writetime',  # Use this column for writetime
    writetime_value=1705325400000000,  # Or fixed writetime
    ttl_column='_ttl',  # Use this column for TTL
    ttl_value=86400  # Or fixed TTL
)

# Advanced: Per-column writetime mapping
await operator.import_from_csv(
    'keyspace.table',
    'input.csv',
    writetime_mapping={
        'name': 'name_writetime',
        'email': 'email_writetime',
        'profile': 1705325400000000  # Fixed writetime
    }
)
```

### Query Patterns

#### Export Queries
```sql
-- Standard export
SELECT * FROM keyspace.table

-- Export with writetime/TTL (dynamically built)
SELECT
    id, name, email,
    WRITETIME(name) as name_writetime,
    WRITETIME(email) as email_writetime,
    TTL(name) as name_ttl,
    TTL(email) as email_ttl
FROM keyspace.table
```

#### Import Statements
```sql
-- Import with writetime
INSERT INTO keyspace.table (id, name, email)
VALUES (?, ?, ?)
USING TIMESTAMP ?

-- Import with both writetime and TTL
INSERT INTO keyspace.table (id, name, email)
VALUES (?, ?, ?)
USING TIMESTAMP ? AND TTL ?

-- Update with writetime (for null handling)
UPDATE keyspace.table
USING TIMESTAMP ?
SET name = ?, email = ?
WHERE id = ?
```

### Type-Specific Handling

```python
# Writetime support matrix
WRITETIME_SUPPORT = {
    # Primitive types - SUPPORTED
    'ascii': True, 'bigint': True, 'blob': True, 'boolean': True,
    'date': True, 'decimal': True, 'double': True, 'float': True,
    'inet': True, 'int': True, 'text': True, 'time': True,
    'timestamp': True, 'timeuuid': True, 'uuid': True, 'varchar': True,
    'varint': True, 'smallint': True, 'tinyint': True,

    # Complex types - LIMITED/NO SUPPORT
    'list': False,  # No writetime on entire list
    'set': False,   # No writetime on entire set
    'map': False,   # No writetime on entire map
    'frozen': True,  # Frozen collections supported
    'tuple': True,   # Frozen tuples supported
    'udt': True,     # Frozen UDTs supported

    # Special types - NO SUPPORT
    'counter': False,  # Counters don't support writetime
}

# Collection element handling
class CollectionWritetimeHandler:
    """Handle writetime for collection elements"""

    def export_map_with_writetime(self, row, column):
        """Export map with per-entry writetime"""
        # SELECT map_column, writetime(map_column['key']) FROM table
        pass

    def import_map_with_writetime(self, data, writetimes):
        """Import map entries with individual writetimes"""
        # UPDATE table SET map_column['key'] = 'value' USING TIMESTAMP ?
        pass
```

### Format-Specific Implementations

#### CSV Format
- Additional columns for writetime/TTL
- Configurable column naming
- Handle missing writetime values

#### JSON Format
```json
{
  "id": 123,
  "name": "John",
  "email": "john@example.com",
  "_metadata": {
    "writetime": {
      "name": 1705325400000000,
      "email": 1705325400000000
    },
    "ttl": {
      "name": 86400,
      "email": 86400
    }
  }
}
```

#### Parquet Format
- Store writetime/TTL as additional columns
- Use column metadata for identification
- Efficient storage with column compression

#### Iceberg Format
- Use Iceberg metadata columns
- Track writetime in table properties
- Enable time-travel with original timestamps

### Best Practices

1. **Default Behavior**: Don't include writetime by default (performance impact)
2. **Validation**: Warn when writetime requested on unsupported columns
3. **Performance**: Batch columns to minimize query overhead
4. **Precision**: Always use microseconds, convert from other formats
5. **Null Handling**: Clear documentation on NULL writetime behavior
6. **Schema Evolution**: Handle schema changes between export/import

## Critical Design: Testing and Parallelization

### Testing as a First-Class Requirement

This is a **production database driver** - testing is not optional, it's fundamental. Every feature must be thoroughly tested before it can be considered complete.

#### Testing Hierarchy

1. **Unit Tests** (Fastest, Run Most Often)
   - Mock Cassandra interactions
   - Test type conversions in isolation
   - Verify parallelization logic
   - Test error handling paths
   - Must run in <30 seconds total

2. **Integration Tests** (Real Cassandra)
   - Single-node Cassandra tests
   - Multi-node cluster tests
   - Test actual data operations
   - Verify token range calculations
   - Test failure scenarios

3. **Performance Tests** (Benchmarks)
   - Establish baseline performance metrics
   - Test various parallelization levels
   - Memory usage profiling
   - CPU utilization monitoring
   - Network saturation tests

4. **Chaos Tests** (Production Scenarios)
   - Node failures during operations
   - Network partitions
   - Disk full scenarios
   - OOM conditions
   - Concurrent operations

#### Test Matrix for Each Feature

```python
# Every feature must be tested across this matrix
TEST_MATRIX = {
    "cluster_sizes": [1, 3, 5],  # Single and multi-node
    "data_sizes": ["1K", "1M", "100M", "1B"],  # Rows
    "parallelization": [1, 4, 16, 64, 256],  # Concurrent operations
    "cassandra_versions": ["4.0", "4.1", "5.0"],
    "consistency_levels": ["ONE", "QUORUM", "ALL"],
    "failure_modes": ["node_down", "network_slow", "disk_full"],
}
```

### Parallelization Configuration

Parallelization is critical for performance but must be configurable to prevent overwhelming production clusters.

#### Configuration Hierarchy

```python
@dataclass
class ParallelizationConfig:
    """Fine-grained control over parallelization"""

    # Token range parallelism
    max_concurrent_ranges: int = 16  # How many token ranges to process in parallel
    ranges_per_node: int = 4  # Ranges to process per Cassandra node

    # Query parallelism
    max_concurrent_queries: int = 32  # Total concurrent queries
    queries_per_range: int = 1  # Concurrent queries per token range

    # Resource limits
    max_memory_mb: int = 1024  # Memory limit for buffering
    max_connections_per_node: int = 4  # Connection pool size per node

    # Adaptive throttling
    enable_adaptive_throttling: bool = True
    target_coordinator_cpu: float = 0.7  # Target CPU on coordinator
    target_node_cpu: float = 0.8  # Target CPU on data nodes

    # Backpressure
    buffer_size_per_range: int = 10000  # Rows to buffer per range
    backpressure_threshold: float = 0.9  # Slow down at 90% buffer

    # Retry configuration
    max_retries_per_range: int = 3
    retry_backoff_ms: int = 1000
    retry_backoff_multiplier: float = 2.0

    def validate(self):
        """Validate configuration for safety"""
        assert self.max_concurrent_ranges <= 256, "Too many concurrent ranges"
        assert self.max_memory_mb <= 8192, "Memory limit too high"
        assert self.queries_per_range <= 4, "Too many queries per range"
```

#### Parallelization Patterns

```python
class ParallelizationStrategy:
    """Different strategies for different scenarios"""

    @staticmethod
    def conservative() -> ParallelizationConfig:
        """For production clusters under load"""
        return ParallelizationConfig(
            max_concurrent_ranges=4,
            max_concurrent_queries=8,
            queries_per_range=1,
            target_coordinator_cpu=0.5
        )

    @staticmethod
    def balanced() -> ParallelizationConfig:
        """Default for most use cases"""
        return ParallelizationConfig(
            max_concurrent_ranges=16,
            max_concurrent_queries=32,
            queries_per_range=1,
            target_coordinator_cpu=0.7
        )

    @staticmethod
    def aggressive() -> ParallelizationConfig:
        """For dedicated clusters or off-hours"""
        return ParallelizationConfig(
            max_concurrent_ranges=64,
            max_concurrent_queries=128,
            queries_per_range=2,
            target_coordinator_cpu=0.9
        )

    @staticmethod
    def adaptive(cluster_metrics: ClusterMetrics) -> ParallelizationConfig:
        """Dynamically adjust based on cluster health"""
        # Start conservative
        config = ParallelizationStrategy.conservative()

        # Scale up based on available resources
        if cluster_metrics.avg_cpu < 0.3:
            config.max_concurrent_ranges *= 2
        if cluster_metrics.pending_compactions < 10:
            config.max_concurrent_queries *= 2

        return config
```

### Testing Parallelization

```python
class ParallelizationTests:
    """Critical tests for parallelization logic"""

    async def test_token_range_coverage(self):
        """Ensure no data is missed or duplicated"""
        # Test with various split counts
        for splits in [1, 8, 32, 128, 1024]:
            await self._verify_complete_coverage(splits)

    async def test_concurrent_range_limit(self):
        """Verify concurrent range limits are respected"""
        config = ParallelizationConfig(max_concurrent_ranges=4)
        # Monitor actual concurrency during operation

    async def test_backpressure(self):
        """Test backpressure slows down producers"""
        # Simulate slow consumer
        # Verify production rate adapts

    async def test_node_aware_parallelism(self):
        """Test queries are distributed across nodes"""
        # Verify no single node is overwhelmed
        # Check replica-aware routing

    async def test_adaptive_throttling(self):
        """Test throttling based on cluster metrics"""
        # Simulate high CPU
        # Verify operation slows down
        # Simulate recovery
        # Verify operation speeds up
```

### Production Safety Features

1. **Circuit Breakers**
   ```python
   class CircuitBreaker:
       """Stop operations if cluster is unhealthy"""
       def __init__(self,
                    max_errors: int = 10,
                    error_window_seconds: int = 60,
                    cooldown_seconds: int = 300):
           self.max_errors = max_errors
           self.error_window = error_window_seconds
           self.cooldown = cooldown_seconds
   ```

2. **Resource Monitoring**
   ```python
   class ResourceMonitor:
       """Monitor and limit resource usage"""
       async def check_limits(self):
           if self.memory_usage > self.config.max_memory_mb:
               await self.trigger_backpressure()
           if self.open_connections > self.config.max_connections:
               await self.pause_new_operations()
   ```

3. **Cluster Health Checks**
   ```python
   class ClusterHealthMonitor:
       """Continuous cluster health monitoring"""
       async def is_healthy_for_bulk_ops(self) -> bool:
           metrics = await self.get_cluster_metrics()
           return (
               metrics.avg_cpu < 0.8 and
               metrics.pending_compactions < 100 and
               metrics.dropped_mutations == 0
           )
   ```

### Testing Requirements by Phase

#### Phase 1: Foundation
- [ ] Monorepo test infrastructure works
- [ ] Both packages have independent test suites
- [ ] CI runs all tests on every commit

#### Phase 2: CSV Implementation
- [ ] 100% code coverage for type conversions
- [ ] Parallelization tests with 1-256 concurrent operations
- [ ] Memory leak tests over 1B+ rows
- [ ] Crash recovery tests
- [ ] Multi-node failure scenarios

#### Phase 3: Additional Formats
- [ ] Format-specific edge cases
- [ ] Large file handling (>100GB)
- [ ] Compression/decompression correctness
- [ ] Format conversion accuracy

#### Phase 4: Cloud Storage
- [ ] Network failure handling
- [ ] Partial upload recovery
- [ ] Cost optimization validation
- [ ] Multi-region testing

### Performance Testing Approach

1. **Establish Baselines**
   - Measure performance in our test environment
   - Document throughput, latency, and resource usage
   - Create reproducible benchmark scenarios

2. **Continuous Monitoring**
   - Track performance across releases
   - Identify regressions early
   - Document performance characteristics

3. **Real-World Scenarios**
   - Test with actual production data patterns
   - Various data types and sizes
   - Different cluster configurations

The focus is on building a reliable, well-tested bulk operations library with configurable parallelization suitable for production database clusters. Performance targets will be established through actual testing and user feedback.

## Failure Handling, Retries, and Resume Capability

### Core Principle: Bulk Operations Must Be Resumable

In production, bulk operations processing billions of rows WILL encounter failures. The library must handle these gracefully and allow operations to resume from where they failed.

### Failure Types and Handling

```python
class FailureType(Enum):
    """Types of failures in bulk operations"""
    TRANSIENT = "transient"  # Network blip, timeout
    NODE_DOWN = "node_down"  # Cassandra node failure
    RANGE_ERROR = "range_error"  # Specific token range issue
    DATA_ERROR = "data_error"  # Bad data, type conversion
    RESOURCE_LIMIT = "resource_limit"  # OOM, disk full
    FATAL = "fatal"  # Unrecoverable error

@dataclass
class RangeFailure:
    """Track failures at token range level"""
    range: TokenRange
    failure_type: FailureType
    error: Exception
    attempt_count: int
    first_failure: datetime
    last_failure: datetime
    rows_processed_before_failure: int
```

### Retry Strategy

```python
@dataclass
class RetryConfig:
    """Configurable retry behavior"""
    # Per-range retries
    max_retries_per_range: int = 3
    initial_backoff_ms: int = 1000
    max_backoff_ms: int = 60000
    backoff_multiplier: float = 2.0

    # Failure thresholds
    max_failed_ranges: int = 10  # Abort if too many ranges fail
    max_failure_percentage: float = 0.05  # Abort if >5% ranges fail

    # Retry strategies by failure type
    retry_strategies: Dict[FailureType, RetryStrategy] = field(default_factory=lambda: {
        FailureType.TRANSIENT: RetryStrategy(max_retries=3, backoff=True),
        FailureType.NODE_DOWN: RetryStrategy(max_retries=5, backoff=True, wait_for_node=True),
        FailureType.RANGE_ERROR: RetryStrategy(max_retries=1, split_range=True),
        FailureType.DATA_ERROR: RetryStrategy(max_retries=0, skip_bad_data=True),
        FailureType.RESOURCE_LIMIT: RetryStrategy(max_retries=2, reduce_batch_size=True),
        FailureType.FATAL: RetryStrategy(max_retries=0, abort=True),
    })

class RetryStrategy:
    """How to retry specific failure types"""
    max_retries: int
    backoff: bool = True
    wait_for_node: bool = False
    split_range: bool = False  # Split range into smaller chunks
    skip_bad_data: bool = False
    reduce_batch_size: bool = False
    abort: bool = False
```

### Checkpoint and Resume System

```python
@dataclass
class OperationCheckpoint:
    """Checkpoint for resumable operations"""
    operation_id: str
    operation_type: str  # export, import, count
    keyspace: str
    table: str
    started_at: datetime
    last_checkpoint: datetime

    # Progress tracking
    total_ranges: int
    completed_ranges: List[TokenRange]
    failed_ranges: List[RangeFailure]
    in_progress_ranges: List[TokenRange]

    # Statistics
    rows_processed: int
    bytes_processed: int
    errors_encountered: int

    # Configuration snapshot
    config: Dict[str, Any]  # Parallelization, retry config, etc.

    def save(self, checkpoint_path: Path):
        """Atomic checkpoint save"""
        temp_path = checkpoint_path.with_suffix('.tmp')
        with open(temp_path, 'w') as f:
            json.dump(self.to_dict(), f, indent=2)
        temp_path.rename(checkpoint_path)  # Atomic on POSIX

    @classmethod
    def load(cls, checkpoint_path: Path) -> 'OperationCheckpoint':
        """Load checkpoint for resume"""
        with open(checkpoint_path) as f:
            return cls.from_dict(json.load(f))

    def get_remaining_ranges(self) -> List[TokenRange]:
        """Calculate ranges that still need processing"""
        completed_set = {(r.start, r.end) for r in self.completed_ranges}
        return [r for r in self.all_ranges if (r.start, r.end) not in completed_set]
```

### Resume Operation API

```python
# Resume from checkpoint
checkpoint = OperationCheckpoint.load("export_checkpoint.json")
await operator.resume_export(
    checkpoint=checkpoint,
    output_path="s3://bucket/data.parquet",
    progress_callback=ProgressBarCallback("Resuming export")
)

# Or auto-checkpoint during operation
await operator.export_to_csv(
    'keyspace.table',
    'output.csv',
    checkpoint_interval=1000,  # Checkpoint every 1000 ranges
    checkpoint_path='export_checkpoint.json',
    auto_resume=True  # Automatically resume if checkpoint exists
)
```

### Failure Handling During Operations

```python
class BulkOperationExecutor:
    """Core execution engine with failure handling"""

    async def execute_with_retry(self,
                                 ranges: List[TokenRange],
                                 operation: Callable,
                                 config: RetryConfig) -> OperationResult:
        """Execute operation with comprehensive failure handling"""

        checkpoint = OperationCheckpoint(...)
        failed_ranges: List[RangeFailure] = []

        # Process ranges with retry logic
        async with self._create_retry_pool() as pool:
            for range in ranges:
                result = await self._process_range_with_retry(
                    range, operation, config
                )

                if result.success:
                    checkpoint.completed_ranges.append(range)
                else:
                    failed_ranges.append(result.failure)

                    # Check failure thresholds
                    if self._should_abort(failed_ranges, checkpoint):
                        raise BulkOperationAborted(
                            "Too many failures",
                            checkpoint=checkpoint
                        )

                # Periodic checkpoint
                if len(checkpoint.completed_ranges) % config.checkpoint_interval == 0:
                    checkpoint.save(self.checkpoint_path)

        # Handle failed ranges
        if failed_ranges:
            await self._handle_failed_ranges(failed_ranges, checkpoint)

        return OperationResult(checkpoint=checkpoint, failed_ranges=failed_ranges)

    async def _process_range_with_retry(self,
                                        range: TokenRange,
                                        operation: Callable,
                                        config: RetryConfig) -> RangeResult:
        """Process single range with retry logic"""

        attempts = 0
        last_error = None
        backoff = config.initial_backoff_ms

        while attempts < config.max_retries_per_range:
            try:
                result = await operation(range)
                return RangeResult(success=True, data=result)

            except Exception as e:
                attempts += 1
                last_error = e
                failure_type = self._classify_failure(e)

                # Apply retry strategy
                strategy = config.retry_strategies[failure_type]

                if not strategy.should_retry(attempts):
                    break

                if strategy.wait_for_node:
                    await self._wait_for_node_recovery(range.replica_nodes)

                if strategy.split_range and range.is_splittable():
                    # Retry with smaller ranges
                    sub_ranges = self._split_range(range, parts=4)
                    return await self._process_subranges(sub_ranges, operation, config)

                if strategy.reduce_batch_size:
                    operation = self._reduce_batch_size(operation)

                # Backoff before retry
                await asyncio.sleep(backoff / 1000)
                backoff = min(backoff * config.backoff_multiplier, config.max_backoff_ms)

        # All retries failed
        return RangeResult(
            success=False,
            failure=RangeFailure(
                range=range,
                failure_type=self._classify_failure(last_error),
                error=last_error,
                attempt_count=attempts,
                first_failure=datetime.now(),
                last_failure=datetime.now(),
                rows_processed_before_failure=0  # TODO: Track partial progress
            )
        )
```

### Handling Partial Range Failures

```python
class PartialRangeHandler:
    """Handle failures within a token range"""

    async def process_range_with_savepoints(self,
                                           range: TokenRange,
                                           batch_size: int = 1000):
        """Process range in batches with savepoints"""

        cursor = range.start
        rows_processed = 0

        while cursor < range.end:
            try:
                # Process batch
                batch_end = min(cursor + batch_size, range.end)
                rows = await self._process_batch(cursor, batch_end)

                # Save progress
                await self._save_range_progress(range, cursor, rows_processed)

                cursor = batch_end
                rows_processed += len(rows)

            except Exception as e:
                # Can resume from cursor position
                raise PartialRangeFailure(
                    range=range,
                    completed_until=cursor,
                    rows_processed=rows_processed,
                    error=e
                )
```

### Error Reporting and Diagnostics

```python
@dataclass
class BulkOperationReport:
    """Comprehensive operation report"""
    operation_id: str
    success: bool
    total_rows: int
    successful_rows: int
    failed_rows: int
    duration: timedelta

    # Detailed failure information
    failures_by_type: Dict[FailureType, List[RangeFailure]]
    failure_samples: List[Dict[str, Any]]  # Sample of failed rows

    # Recovery information
    checkpoint_path: Path
    resume_command: str

    def generate_report(self) -> str:
        """Human-readable failure report"""
        return f"""
Bulk Operation Report
====================
Operation ID: {self.operation_id}
Status: {'PARTIAL SUCCESS' if self.failed_rows > 0 else 'SUCCESS'}
Rows Processed: {self.successful_rows:,} / {self.total_rows:,}
Failed Rows: {self.failed_rows:,}
Duration: {self.duration}

Failure Summary:
{self._format_failures()}

To resume this operation:
{self.resume_command}

Checkpoint saved to: {self.checkpoint_path}
        """
```

### Testing Failure Scenarios

```python
class FailureHandlingTests:
    """Test failure handling and resume capabilities"""

    async def test_resume_after_failure(self):
        """Test operation can resume from checkpoint"""
        # Start operation
        # Simulate failure midway
        # Load checkpoint
        # Resume operation
        # Verify no data loss or duplication

    async def test_node_failure_handling(self):
        """Test handling of node failures"""
        # Start operation
        # Kill Cassandra node
        # Verify operation retries and completes

    async def test_partial_range_recovery(self):
        """Test recovery from partial range failures"""
        # Process large range
        # Fail after processing some rows
        # Resume from savepoint
        # Verify exactly-once processing

    async def test_corruption_handling(self):
        """Test handling of data corruption"""
        # Insert corrupted data
        # Run operation
        # Verify bad data is logged but operation continues
```

This comprehensive failure handling ensures bulk operations are production-ready with proper retry logic, checkpointing, and resume capabilities essential for processing large datasets reliably.
