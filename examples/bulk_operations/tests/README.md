# Tests for Bulk Operations

This directory contains comprehensive tests for the bulk operations example, organized into unit and integration tests.

## Test Structure

```
tests/
├── unit/                 # Unit tests with mocked dependencies
│   ├── __init__.py
│   ├── test_helpers.py   # Shared test utilities
│   ├── test_bulk_operator.py
│   ├── test_token_utils.py
│   └── test_token_ranges.py
└── integration/          # Integration tests against real Cassandra
    ├── __init__.py
    ├── conftest.py       # Fixtures and configuration
    ├── test_token_discovery.py
    ├── test_bulk_count.py
    ├── test_bulk_export.py
    ├── test_token_splitting.py
    └── README.md
```

## Running Tests

### Unit Tests

Unit tests use mocks and don't require a Cassandra instance:

```bash
# Run all unit tests
pytest tests/unit -v

# Run specific test file
pytest tests/unit/test_bulk_operator.py -v

# Run with coverage
pytest tests/unit --cov=bulk_operations --cov-report=html
```

### Integration Tests

Integration tests require a running Cassandra cluster:

```bash
# Run all integration tests (starts Cassandra automatically)
pytest tests/integration --integration -v

# Run specific integration test
pytest tests/integration/test_bulk_count.py --integration -v
```

## Test Categories

### Unit Tests

- **test_bulk_operator.py** - Tests for `TokenAwareBulkOperator`
  - Count operations with mocked token ranges
  - Export streaming functionality
  - Error handling and recovery
  - Progress tracking and callbacks

- **test_token_utils.py** - Tests for token utilities
  - Token range calculations
  - Range splitting algorithms
  - Query generation
  - Token discovery mocking

- **test_token_ranges.py** - Additional token range tests
  - Wraparound range handling
  - Full ring coverage
  - Proportional splitting

### Integration Tests

- **test_token_discovery.py** - Token range discovery
  - Vnode handling (256 tokens per node)
  - Comparison with nodetool output
  - Ring coverage validation

- **test_bulk_count.py** - Count operations
  - Full table coverage
  - Wraparound range handling
  - Performance with parallelism

- **test_bulk_export.py** - Export operations
  - Streaming completeness
  - Memory efficiency
  - Data type handling

- **test_token_splitting.py** - Splitting strategies
  - Proportional splitting
  - Replica clustering
  - Small range handling

## Test Standards

All tests follow the documentation standards from CLAUDE.md:

```python
"""
Brief description of test.

What this tests:
---------------
1. Specific behavior being tested
2. Edge cases covered
3. Expected outcomes

Why this matters:
----------------
- Real-world implications
- Common use cases
- Potential bugs prevented
"""
```

## Coverage Goals

- Unit tests: 90%+ coverage
- Integration tests: Validate all critical paths
- Combined: 95%+ coverage

Current coverage: 86% (unit tests only)
