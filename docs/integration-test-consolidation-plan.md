# Integration Test Consolidation Plan

## Current State
- 19 integration test files with significant overlap
- Multiple files testing the same functionality (prepared statements, consistency levels, etc.)
- Duplicate test patterns across files

## Consolidation Strategy

### 1. **test_crud_operations.py** (NEW)
Consolidate basic CRUD operations from:
- `test_basic_operations.py` - Basic insert/select/update/delete
- `test_select_operations.py` - SELECT query variations
- Error handling tests
- Non-existent data queries

**Remove from original files:**
- Duplicate CRUD tests
- Basic prepared statement tests (move to dedicated file)
- Basic error handling

### 2. **test_concurrent_operations.py** (KEEP AS IS)
Already well-focused on concurrency testing. Just ensure no duplication.

### 3. **test_batch_and_lwt_operations.py** (NEW)
Combine atomic operations:
- All tests from `test_batch_operations.py`
- All tests from `test_lwt_operations.py`
- Conditional batch operations

**Rationale:** Both deal with atomic operations and often used together

### 4. **test_data_types_and_counters.py** (NEW)
Combine data type testing:
- All tests from `test_cassandra_data_types.py`
- All tests from `test_counters.py`
- Remove counter test from data types file

**Rationale:** Counters are a special data type

### 5. **test_streaming_operations.py** (KEEP AS IS)
Well-focused on streaming functionality.

### 6. **test_simple_statements.py** (KEEP AS IS)
Specifically tests SimpleStatement usage (discouraged but needs testing).

### 7. **test_consistency_and_prepared_statements.py** (NEW)
Focus on:
- Consistency level tests from multiple files
- Prepared statement lifecycle and reuse
- Performance comparisons
- Custom payloads

### 8. **Other files to keep as is:**
- `test_network_failures.py` - Specific failure scenarios
- `test_reconnection_behavior.py` - Connection recovery
- `test_long_lived_connections.py` - Connection stability
- `test_protocol_version.py` - Protocol compatibility
- `test_driver_compatibility.py` - Driver API compatibility
- `test_empty_resultsets.py` - Edge case testing
- `test_stress.py` - Performance testing
- `test_context_manager_safety_integration.py` - Resource management
- `test_fastapi_reconnection_isolation.py` - FastAPI specific

## Expected Impact
- Reduce from 19 to ~15 files
- Eliminate duplicate test coverage
- More logical organization
- Easier to maintain and understand

## Implementation Order
1. Create `test_crud_operations.py` consolidating basic operations
2. Create `test_batch_and_lwt_operations.py` combining atomic operations
3. Create `test_data_types_and_counters.py` combining type tests
4. Create `test_consistency_and_prepared_statements.py` for advanced features
5. Remove duplicates from original files
6. Update conftest.py if needed
