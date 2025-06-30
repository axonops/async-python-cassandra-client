# Integration Test Placeholder Fix Summary

## Overview
Fixed placeholder usage in integration tests to follow the correct pattern:
- Use `%s` placeholders for non-prepared statements
- Use `?` placeholders for prepared statements only

## Files Fixed

### 1. test_network_failures.py
- Kept SimpleStatement with %s placeholder for consistency level testing (line 57)
- Fixed other non-prepared statements to use %s placeholders
- Total changes: 3 replacements

### 2. test_select_operations.py
- Kept SimpleStatement with %s placeholder for consistency level testing (line 41)
- Fixed all other non-prepared statements to use %s placeholders
- Total changes: 4 replacements

### 3. test_cassandra_data_types.py
- Fixed all non-prepared statements to use %s placeholders
- Kept ? placeholders only for prepared statements
- Total changes: 12 replacements

### 4. test_long_lived_connections.py
- Fixed non-prepared statements to use %s placeholders
- Total changes: 2 replacements

### 5. test_streaming_operations.py
- Fixed non-prepared statement to use %s placeholder
- Total changes: 1 replacement

### 6. test_stress.py
- Fixed non-prepared statement to use %s placeholder
- Total changes: 1 replacement

## Key Pattern
The correct usage pattern is:
```python
# Non-prepared statements use %s
await session.execute("INSERT INTO table (id, name) VALUES (%s, %s)", [id, name])

# Prepared statements use ?
prepared = await session.prepare("INSERT INTO table (id, name) VALUES (?, ?)")
await session.execute(prepared, [id, name])

# SimpleStatement with consistency level uses %s
stmt = SimpleStatement("SELECT * FROM table WHERE id = %s", consistency_level=ConsistencyLevel.QUORUM)
await session.execute(stmt, [id])
```

## Linting
All files have been fixed and pass linting checks:
- ✅ ruff check (fixed whitespace issues)
- ✅ black formatting
- ✅ isort import ordering

## Testing Status
Integration tests are running correctly with the fixed placeholders.
