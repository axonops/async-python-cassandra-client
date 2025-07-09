# Test Organization

This directory contains all tests for async-python-cassandra-client, organized by test type:

## Directory Structure

### `/unit`
Pure unit tests with mocked dependencies. No external services required.
- Fast execution
- Test individual components in isolation
- All Cassandra interactions are mocked

### `/integration`
Integration tests that require a real Cassandra instance.
- Test actual database operations
- Verify driver behavior with real Cassandra
- Marked with `@pytest.mark.integration`

### `/bdd`
Cucumber-based Behavior Driven Development tests.
- Feature files in `/bdd/features`
- Step definitions in `/bdd/steps`
- Focus on user scenarios and requirements

### `/fastapi_integration`
FastAPI-specific integration tests.
- Test the example FastAPI application
- Verify async-cassandra works correctly with FastAPI
- Requires both Cassandra and the FastAPI app running
- No mocking - tests real-world scenarios

### `/benchmarks`
Performance benchmarks and stress tests.
- Measure performance characteristics
- Identify performance regressions

### `/utils`
Shared test utilities and helpers.

### `/_fixtures`
Test fixtures and sample data.

## Running Tests

```bash
# Unit tests (fast, no external dependencies)
make test-unit

# Integration tests (requires Cassandra)
make test-integration

# FastAPI integration tests (requires Cassandra + FastAPI app)
make test-fastapi

# BDD tests (requires Cassandra)
make test-bdd

# All tests
make test-all
```

## Test Isolation

- Each test type is completely isolated
- No shared code between test types
- Each directory has its own conftest.py if needed
- Tests should not import from other test directories
