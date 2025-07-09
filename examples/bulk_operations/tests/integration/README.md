# Integration Tests for Bulk Operations

This directory contains integration tests that validate bulk operations against a real Cassandra cluster.

## Test Organization

The integration tests are organized into logical modules:

- **test_token_discovery.py** - Tests for token range discovery with vnodes
  - Validates token range discovery matches cluster configuration
  - Compares with nodetool describering output
  - Ensures complete ring coverage without gaps

- **test_bulk_count.py** - Tests for bulk count operations
  - Validates full data coverage (no missing/duplicate rows)
  - Tests wraparound range handling
  - Performance testing with different parallelism levels

- **test_bulk_export.py** - Tests for bulk export operations
  - Validates streaming export completeness
  - Tests memory efficiency for large exports
  - Handles different CQL data types

- **test_token_splitting.py** - Tests for token range splitting strategies
  - Tests proportional splitting based on range sizes
  - Handles small vnode ranges appropriately
  - Validates replica-aware clustering

## Running Integration Tests

Integration tests require a running Cassandra cluster. They are skipped by default.

### Run all integration tests:
```bash
pytest tests/integration --integration
```

### Run specific test module:
```bash
pytest tests/integration/test_bulk_count.py --integration -v
```

### Run specific test:
```bash
pytest tests/integration/test_bulk_count.py::TestBulkCount::test_full_table_coverage_with_token_ranges --integration -v
```

## Test Infrastructure

### Automatic Cassandra Startup

The tests will automatically start a single-node Cassandra container if one is not already running, using either:
- `docker-compose-single.yml` (via docker-compose or podman-compose)

### Manual Cassandra Setup

You can also manually start Cassandra:

```bash
# Single node (recommended for basic tests)
podman-compose -f docker-compose-single.yml up -d

# Multi-node cluster (for advanced tests)
podman-compose -f docker-compose.yml up -d
```

### Test Fixtures

Common fixtures are defined in `conftest.py`:
- `ensure_cassandra` - Session-scoped fixture that ensures Cassandra is running
- `cluster` - Creates AsyncCluster connection
- `session` - Creates test session with keyspace

## Test Requirements

- Cassandra 4.0+ (or ScyllaDB)
- Docker or Podman with compose
- Python packages: pytest, pytest-asyncio, async-cassandra

## Debugging Tips

1. **View Cassandra logs:**
   ```bash
   podman logs bulk-cassandra-1
   ```

2. **Check token ranges manually:**
   ```bash
   podman exec bulk-cassandra-1 nodetool describering bulk_test
   ```

3. **Run with verbose output:**
   ```bash
   pytest tests/integration --integration -v -s
   ```

4. **Run with coverage:**
   ```bash
   pytest tests/integration --integration --cov=bulk_operations
   ```
