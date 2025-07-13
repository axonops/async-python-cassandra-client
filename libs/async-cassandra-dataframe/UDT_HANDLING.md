# UDT (User Defined Type) Handling in async-cassandra-dataframe

## Overview

User Defined Types (UDTs) in Cassandra are custom data structures that can be used as column types. This document explains how async-cassandra-dataframe handles UDTs and the current limitations.

## How UDTs Work

### In Cassandra Driver

The cassandra-driver returns UDTs as namedtuple-like objects:
```python
# Raw cassandra-driver
row = session.execute("SELECT address FROM users WHERE id = 1").one()
print(row.address.city)  # Direct attribute access
# Output: "New York"
```

### In async-cassandra-dataframe

We convert UDTs to dictionaries for better pandas compatibility:
```python
df = await cdf.read_cassandra_table("users", session=session)
row = df.iloc[0]
print(row['address']['city'])  # Dict access
# Output: "New York"
```

## Dask Serialization Limitation

**IMPORTANT**: Dask has a known limitation where dict objects are converted to string representations during serialization. This affects UDT columns when using Dask delayed execution.

### The Issue

```python
# With Dask delayed execution (multiple partitions)
df = await cdf.read_cassandra_table(
    "users",
    session=session,
    partition_count=10,  # Multiple partitions
    use_parallel_execution=False  # Dask delayed
)

result = df.compute()
# UDT columns are now strings!
print(type(result.iloc[0]['address']))  # <class 'str'>
print(result.iloc[0]['address'])  # "{'street': '123 Main St', 'city': 'NYC'}"
```

### Root Cause

This is NOT a bug in async-cassandra-dataframe. It's a Dask limitation:
- Dask uses PyArrow for serialization
- PyArrow converts Python dict objects to strings
- This happens during the compute() operation

## Workarounds

### 1. Use Parallel Execution (Recommended)

For best UDT support, use parallel execution which bypasses Dask:

```python
df = await cdf.read_cassandra_table(
    "users",
    session=session,
    partition_count=10,
    use_parallel_execution=True  # ✅ Preserves UDTs as dicts
)

# df is already computed, UDTs are preserved
print(type(df.iloc[0]['address']))  # <class 'dict'>
```

### 2. Parse String Representations

If you must use Dask delayed execution, parse the string representations:

```python
import ast

df = await cdf.read_cassandra_table(
    "users",
    session=session,
    partition_count=10,
    use_parallel_execution=False
)

result = df.compute()

# Parse UDT strings back to dicts
for col in ['address', 'contact_info']:  # Your UDT columns
    result[col] = result[col].apply(
        lambda x: ast.literal_eval(x) if isinstance(x, str) else x
    )
```

### 3. Single Partition Reads

For small tables, use a single partition to avoid serialization:

```python
df = await cdf.read_cassandra_table(
    "users",
    session=session,
    partition_count=1  # Single partition avoids serialization issues
)
```

## Best Practices

### 1. Identify UDT Columns

Know which columns contain UDTs:
```python
from async_cassandra_dataframe.metadata import TableMetadataExtractor

extractor = TableMetadataExtractor(session)
metadata = await extractor.get_table_metadata("keyspace", "table")

# Find UDT columns
udt_columns = []
for col in metadata['columns']:
    col_type = str(col['type'])
    if col_type.startswith('frozen<') and 'address' in col_type:
        udt_columns.append(col['name'])
```

### 2. Use Type Hints

Document UDT structure in your code:
```python
from typing import TypedDict

class Address(TypedDict):
    street: str
    city: str
    state: str
    zip_code: int

# After reading and parsing
addresses: list[Address] = df['addresses'].tolist()
```

### 3. Frozen vs Non-Frozen UDTs

- **Frozen UDTs**: Can be used in primary keys, sets, and as map keys
- **Non-Frozen UDTs**: Cannot be used in collections or predicates

Both are converted to dicts in DataFrames.

## Examples

### Complete Example with UDT Handling

```python
import async_cassandra_dataframe as cdf
from async_cassandra import AsyncCluster
import ast

async def read_users_with_udts():
    async with AsyncCluster(['localhost']) as cluster:
        async with cluster.connect() as session:
            # Use parallel execution for best UDT support
            df = await cdf.read_cassandra_table(
                "myks.users",
                session=session,
                partition_count=20,
                use_parallel_execution=True,  # Preserves UDTs
                columns=['id', 'name', 'home_address', 'work_addresses']
            )

            # UDTs are preserved as dicts
            for idx, row in df.iterrows():
                home = row['home_address']
                print(f"User {row['name']} lives in {home['city']}")

                # Handle collections of UDTs
                for work_addr in row['work_addresses']:
                    print(f"  Works in {work_addr['city']}")
```

### Handling String Serialized UDTs

```python
def parse_udt_string(value):
    """Parse UDT string representation back to dict."""
    if isinstance(value, str) and value.startswith('{'):
        try:
            return ast.literal_eval(value)
        except:
            return value
    return value

# Apply to DataFrame
df['address'] = df['address'].apply(parse_udt_string)
```

## Performance Considerations

1. **Parallel Execution**: Faster and preserves UDTs correctly
2. **Dask Delayed**: May be needed for very large tables but requires UDT parsing
3. **Memory Usage**: UDTs as dicts use more memory than strings

## Future Improvements

We're investigating options to better handle UDT serialization with Dask, including:
- Custom Dask serializers for UDT objects
- Alternative DataFrame backends that preserve complex types
- Automatic UDT detection and parsing

## Summary

- UDTs are converted from namedtuples to dicts for pandas compatibility ✅
- Parallel execution (`use_parallel_execution=True`) preserves UDTs correctly ✅
- Dask delayed execution converts UDTs to strings (Dask limitation) ⚠️
- Parse string representations when using Dask delayed execution
- This is a known limitation of Dask, not a bug in async-cassandra-dataframe
