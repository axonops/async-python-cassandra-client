# Bulk Operations Implementation Progress

## Overview
This document tracks the implementation progress of the token-aware bulk operations example for async-cassandra, including all major decisions, issues encountered, and solutions applied.

## Phase 1: Basic Token Range Operations ✅ COMPLETED

### Initial Requirements
- Fix Cassandra warning: "USE <keyspace> with prepared statements is considered to be an anti-pattern"
- Create a comprehensive token-aware bulk operations example similar to DSBulk
- Follow TDD practices with test documentation standards from CLAUDE.md
- Use Cassandra 5.0 with proper memory configuration
- Support both Docker and Podman
- Properly handle vnodes (256 tokens per node)
- Integration testing against real Cassandra cluster

### Key Decisions Made

#### 1. Architecture Design
- **Decision**: Implement thin wrapper pattern following async-cassandra principles
- **Rationale**: Maintain consistency with main project, avoid reinventing the wheel
- **Implementation**:
  - `TokenAwareBulkOperator` class wraps async session
  - `TokenRange` dataclass for range representation
  - `TokenRangeSplitter` for proportional splitting

#### 2. Token Range Handling
- **Decision**: Use Murmur3 token range (-2^63 to 2^63-1)
- **Special Case**: MIN_TOKEN uses >= instead of > to avoid missing data
- **Wraparound**: Properly handle ranges that cross the ring boundary
- **Implementation**: See `token_utils.py` for size calculations
- **Vnode Support**: Dynamically discovers all vnodes (tested with 256 per node)

#### 3. Import Structure Fix
- **Issue**: Conflict between ruff and isort on import ordering
- **Solution**:
  - Disabled ruff's import sorting ("I" rule)
  - Configured isort to match main project settings
  - Marked `async_cassandra` as first-party import
- **Result**: Stable pre-commit hooks

#### 4. Testing Standards
- **Decision**: Follow CLAUDE.md documentation format for all tests
- **Format**:
  ```python
  """
  Brief description.

  What this tests:
  ---------------
  1. Specific behaviors

  Why this matters:
  ----------------
  - Real-world implications
  """
  ```
- **Coverage**: Achieved 94% test coverage

#### 5. Docker Configuration
- **Upgraded to Cassandra 5.0** (from 4.1)
- **Memory Settings**:
  - Single node: 1G heap (for limited resources)
  - Multi-node: 2G heap per node
  - Container limits adjusted for stability
- **Added init container** for automatic keyspace/table creation
- **Health checks** using nodetool and cqlsh
- **Sequential startup** for multi-node cluster

### Technical Implementation Details

#### Core Components Created
1. **bulk_operations/bulk_operator.py**
   - `TokenAwareBulkOperator`: Main class for bulk operations
   - `BulkOperationStats`: Statistics tracking
   - `BulkOperationError`: Custom exception with partial results
   - Methods: `count_by_token_ranges`, `export_by_token_ranges`
   - **Wraparound Range Fix**: Split wraparound ranges into two queries
   - **Prepared Statements**: All token queries use cached prepared statements

2. **bulk_operations/token_utils.py**
   - `TokenRange`: Dataclass with size/fraction calculations
   - `TokenRangeSplitter`: Proportional and replica-aware splitting
   - `discover_token_ranges`: Extract ranges from cluster metadata
   - `generate_token_range_query`: CQL query generation
   - **Fixed**: Access cluster via session._session.cluster

3. **example_count.py**
   - Demonstrates token-aware counting
   - Uses Rich for beautiful console output
   - Shows performance comparison with different split counts
   - Includes progress tracking

#### Key Algorithms

1. **Proportional Splitting**:
   ```python
   range_fraction = token_range.size / total_size
   range_splits = max(1, round(range_fraction * target_splits))
   ```

2. **Token Range Query (with Prepared Statements)**:
   ```python
   # Prepared once per table
   prepared_stmt = await session.prepare("""
       SELECT * FROM keyspace.table
       WHERE token(partition_key) > ?
       AND token(partition_key) <= ?
   """)

   # Executed many times with different token values
   result = await session.execute(prepared_stmt, (start_token, end_token))
   ```

3. **Wraparound Range Handling**:
   ```python
   # Split into two queries since CQL doesn't support OR
   if token_range.end < token_range.start:
       # Use prepared statements for each part
       result1 = await session.execute(stmt_gt, (token_range.start,))
       result2 = await session.execute(stmt_lte, (token_range.end,))
   ```

4. **Parallelism Control**:
   - Default split_count: 4 × number of nodes
   - Default parallelism: 2 × number of nodes
   - Uses asyncio.Semaphore for concurrency limiting

### Issues Encountered and Solutions

1. **AsyncSession vs AsyncCassandraSession**
   - **Issue**: Import error on AsyncSession
   - **Solution**: Use AsyncCassandraSession from async_cassandra

2. **Mock Setup for Tests**
   - **Issue**: Complex metadata structure needed for tests
   - **Solution**: Properly mock cluster.metadata.keyspaces hierarchy

3. **Type Checking**
   - **Issue**: mypy errors on session.cluster attribute
   - **Solution**: Access via session._session.cluster with type ignore

4. **Linting Conflicts**
   - **Issue**: ruff and isort fighting over import order
   - **Solution**: Disabled ruff's import sorting, configured isort

5. **Loop Variable Binding (B023)**
   - **Issue**: Closure variable in loop
   - **Solution**: Added default parameter to inner function

6. **Wraparound Range Queries**
   - **Issue**: CQL doesn't support OR in token range queries
   - **Solution**: Split wraparound ranges into two separate queries
   - **Result**: All 10,000 test rows now correctly counted

7. **Memory Issues with 3-Node Cluster**
   - **Issue**: Exit code 137 (OOM) when starting all nodes
   - **Solution**:
     - Reduced heap sizes (2G instead of 8G)
     - Sequential node startup with health checks
     - Created single-node alternative for testing

8. **Not Using Prepared Statements**
   - **Issue**: Using simple statements with string formatting for token queries
   - **Solution**:
     - Added `_get_prepared_statements()` method to prepare all queries once
     - Cache prepared statements per table
     - Pass token boundaries as parameters, not in query string
   - **Result**: Following CLAUDE.md best practices, better performance and security

### Test Coverage Summary
- **Unit Tests**: 34 tests, all passing
- **Integration Tests**: 20 tests across 4 modules
- **Coverage**: 88% (unit tests only)
  - bulk_operator.py: 82%
  - token_utils.py: 98%
- **Test Categories**:
  - Token range calculations
  - Parallel execution
  - Error handling
  - Progress tracking
  - Streaming exports
  - Vnode handling
  - Wraparound ranges

### Test Organization
```
tests/
├── unit/                    # Unit tests with mocks
│   ├── __init__.py
│   ├── test_helpers.py      # Shared test utilities
│   ├── test_bulk_operator.py
│   ├── test_token_utils.py
│   └── test_token_ranges.py
├── integration/             # Integration tests
│   ├── __init__.py
│   ├── conftest.py         # Auto-starts Cassandra
│   ├── test_token_discovery.py
│   ├── test_bulk_count.py
│   ├── test_bulk_export.py
│   ├── test_token_splitting.py
│   └── README.md
└── README.md               # Test documentation
```

### Project Structure
```
examples/bulk_operations/
├── bulk_operations/          # Core implementation (94% coverage)
│   ├── __init__.py
│   ├── bulk_operator.py      # Main operator class
│   └── token_utils.py        # Token range utilities
├── tests/                    # Unit tests
│   ├── test_bulk_operator.py
│   ├── test_token_utils.py
│   └── test_token_ranges.py
├── tests/integration/        # Integration tests (NEW)
│   ├── conftest.py          # Auto-starts Cassandra
│   ├── test_token_discovery.py
│   ├── test_bulk_count.py
│   ├── test_bulk_export.py
│   └── test_token_splitting.py
├── scripts/
│   └── init.cql             # Cassandra initialization
├── docker-compose.yml        # 3-node Cassandra cluster
├── docker-compose-single.yml # Single-node alternative (NEW)
├── Makefile                  # Development commands
├── pyproject.toml           # Project configuration
├── README.md                # User documentation
├── IMPLEMENTATION_PLAN.md   # Original plan
├── progress.md              # This file
└── example_count.py         # Demo script
```

### Key Insights About Vnodes
- Each node's vnodes are scattered across the token ring
- Number of vnodes is configurable (default 256 in Cassandra 4.0+)
- Code dynamically discovers actual vnode count from cluster
- Token ranges don't start at MIN_TOKEN due to random distribution
- Last range wraps around (positive to negative values)
- Wraparound ranges require special handling (split queries)
- All data must be accounted for across all ranges

### Phase 1 Success Metrics Achieved
- ✅ All unit tests passing with 94% coverage
- ✅ Pre-commit hooks passing (after configuration fixes)
- ✅ Docker Compose with Cassandra 5.0
- ✅ Example script demonstrating functionality
- ✅ Comprehensive documentation
- ✅ Follows all project conventions and CLAUDE.md standards
- ✅ Integration tests against real cluster
- ✅ Vnode token range validation
- ✅ Wraparound range handling
- ✅ No missing or duplicate data
- ✅ Performance scaling with parallelism

## Next Phase Planning

### Phase 2: Export Functionality (Foundation for Iceberg)
- Streaming export already has basic implementation
- Need to add:
  - File format options (CSV, JSON, Parquet)
  - Compression support
  - Resume capability
  - Better error recovery
- **Note**: Parquet export is critical as it's the underlying format for Iceberg

### Phase 3: Apache Iceberg Integration (PRIMARY GOAL)
- **This is the key deliverable** - Apache Iceberg is the modern, sexy data lakehouse format
- Build on Phase 2's Parquet export capability
- Use filesystem-based catalog (no S3/MinIO needed initially)
- PyIceberg with PyArrow backend
- Schema mapping from Cassandra to Iceberg types
- Partition strategy configuration
- Table evolution support
- Time travel capabilities
- **Why Iceberg?**
  - Production-ready table format used by Netflix, Apple, Adobe
  - ACID transactions on data lakes
  - Schema evolution without rewriting data
  - Hidden partitioning for better performance
  - Time travel and rollback capabilities

### Phase 4: Import from Iceberg
- Read Iceberg tables
- Batch insert to Cassandra
- Data validation
- Progress tracking

### Phase 5: Production Features
- Comprehensive benchmarking
- Performance optimization
- CLI tool with argparse
- Configuration file support
- Monitoring/metrics integration

## Important Notes for Resuming Work

1. **Always run from project root** for pre-commit hooks
2. **Unit tests**: `cd examples/bulk_operations && pytest tests/ -k unit`
3. **Integration tests**: `pytest tests/integration --integration`
4. **Linting**: `ruff check bulk_operations tests`
5. **Docker**: Uses cassandra-net network, bulk-cassandra-* containers
6. **Type access**: Use session._session.cluster for metadata

## Known Limitations

1. **Thread Pool**: Still subject to cassandra-driver's thread pool limits
2. **Memory**: While streaming, still need memory for concurrent operations
3. **Token Distribution**: Assumes even data distribution (real clusters vary)
4. **Single DC**: Current implementation assumes single datacenter

## References for Future Work

- [DSBulk Source](https://github.com/datastax/dsbulk) - Studied for design patterns
- [Cassandra Token Ranges](https://cassandra.apache.org/doc/latest/cassandra/architecture/dynamo.html)
- [PyIceberg Docs](https://py.iceberg.apache.org/) - For Phase 3
- [async-cassandra Patterns](../../CLAUDE.md) - Project conventions

---

## Recent Updates (2025-01-02)

### Prepared Statements Implementation
Following user feedback, updated all token range queries to use prepared statements:
- **What Changed**: Replaced string-formatted queries with prepared statements
- **Implementation**: Added statement caching in `_get_prepared_statements()`
- **Benefits**: Better performance, security, and follows CLAUDE.md best practices
- **Test Impact**: All tests updated and passing

### Test Reorganization
Reorganized test structure per user request:
- **Unit Tests**: Moved to `tests/unit/` subdirectory
- **Integration Tests**: Already in `tests/integration/`
- **Added**: `test_helpers.py` for shared test utilities
- **Coverage**: Maintained at 88% with improved organization

### Data Integrity Tests Added
Per user request, created comprehensive integration tests to verify data integrity:
- **test_data_integrity.py**: New test suite with 4 comprehensive tests
- **Tests Created**:
  1. Simple data round trip - verifies basic data is exactly preserved
  2. Complex data types - tests UUID, timestamp, collections, decimal, blob
  3. Large dataset (50K rows) - ensures no data loss at scale
  4. Wraparound ranges - specifically tests extreme token values
- **Key Finding**: Cassandra treats empty collections as NULL (fixed test expectations)
- **Result**: All data integrity tests passing - confirms Phase 1 is truly complete

*Last Updated: 2025-01-02*
*Phase 1 COMPLETED with full integration testing, prepared statements, and data integrity verification*
