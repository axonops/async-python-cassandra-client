# Consistency Level Support in Bulk Operations

## ✅ FULLY IMPLEMENTED AND WORKING

Consistency level support has been successfully added to all bulk operation methods and is working correctly with the 3-node Cassandra cluster.

## Implementation Details

### How DSBulk Handles Consistency

DSBulk (DataStax Bulk Loader) handles consistency levels as a configuration parameter:
- Default: `LOCAL_ONE`
- Cloud deployments (Astra): Automatically changes to `LOCAL_QUORUM`
- Configurable via:
  - Command line: `-cl LOCAL_QUORUM` or `--driver.query.consistency`
  - Config file: `datastax-java-driver.basic.request.consistency = LOCAL_QUORUM`

### Our Implementation

Following Cassandra driver patterns, consistency levels are set on the prepared statement objects before execution:

```python
# Example usage
from cassandra import ConsistencyLevel

# Count with QUORUM consistency
count = await operator.count_by_token_ranges(
    keyspace="my_keyspace",
    table="my_table",
    consistency_level=ConsistencyLevel.QUORUM
)

# Export with LOCAL_QUORUM consistency
await operator.export_to_csv(
    keyspace="my_keyspace",
    table="my_table",
    output_path="data.csv",
    consistency_level=ConsistencyLevel.LOCAL_QUORUM
)
```

## How It Works

The implementation sets the consistency level on prepared statements before execution:

```python
stmt = prepared_stmts["count_range"]
if consistency_level is not None:
    stmt.consistency_level = consistency_level
result = await self.session.execute(stmt, (token_range.start, token_range.end))
```

This follows the same pattern used in async-cassandra's test suite.

## Test Results

All consistency levels have been tested and verified working with a 3-node cluster:

| Consistency Level | Count Operation | Export Operation |
|------------------|-----------------|------------------|
| ONE              | ✓ Success       | ✓ Success        |
| TWO              | ✓ Success       | ✓ Success        |
| THREE            | ✓ Success       | ✓ Success        |
| QUORUM           | ✓ Success       | ✓ Success        |
| ALL              | ✓ Success       | ✓ Success        |
| LOCAL_ONE        | ✓ Success       | ✓ Success        |
| LOCAL_QUORUM     | ✓ Success       | ✓ Success        |

## Supported Operations

Consistency level parameter is available on:
- `count_by_token_ranges()`
- `export_by_token_ranges()`
- `export_to_csv()`
- `export_to_json()`
- `export_to_parquet()`
- `export_to_iceberg()`

## Code Changes Made

1. **bulk_operator.py**:
   - Added `consistency_level: ConsistencyLevel | None = None` to all relevant methods
   - Set consistency level on prepared statements before execution
   - Updated method documentation

2. **exporters/base.py**:
   - Added consistency_level parameter to abstract export method

3. **exporters/csv_exporter.py, json_exporter.py, parquet_exporter.py**:
   - Updated export methods to accept and pass consistency_level

The implementation is complete, tested, and ready for production use.
