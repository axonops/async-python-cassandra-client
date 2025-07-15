# Cassandra Vector Type Support

async-cassandra-dataframe fully supports Cassandra 5.0+ vector types for similarity search and AI/ML workloads.

## Overview

Cassandra's `VECTOR` type stores fixed-dimensional arrays of floating-point numbers, typically used for:
- Machine learning embeddings
- Similarity search
- Feature vectors
- AI/ML applications

## Features

✅ **Full Support**
- Reading vector columns
- Writing vector data
- Preserving dimension integrity
- Maintaining float32 precision
- NULL vector handling
- Collections of vectors

## Usage

```python
import async_cassandra_dataframe as cdf
import numpy as np

# Create table with vector column
await session.execute("""
    CREATE TABLE embeddings (
        id INT PRIMARY KEY,
        content TEXT,
        embedding VECTOR<FLOAT, 1536>,  -- OpenAI embedding dimension
        metadata MAP<TEXT, TEXT>
    )
""")

# Insert vector data
embedding = [0.1, 0.2, 0.3, ...]  # 1536 dimensions
await session.execute(
    "INSERT INTO embeddings (id, content, embedding) VALUES (?, ?, ?)",
    (1, "Sample text", embedding)
)

# Read vector data
df = await cdf.read_cassandra_table("keyspace.embeddings", session=session)
pdf = df.compute()

# Vector is returned as a list
vector = pdf.iloc[0]['embedding']
print(f"Vector dimension: {len(vector)}")
print(f"Vector type: {type(vector)}")  # list

# Convert to numpy if needed
np_vector = np.array(vector, dtype=np.float32)
```

## Supported Vector Operations

### Different Dimensions
```python
# Small vectors (3D)
VECTOR<FLOAT, 3>

# Medium vectors (384D - sentence transformers)
VECTOR<FLOAT, 384>

# Large vectors (1536D - OpenAI embeddings)
VECTOR<FLOAT, 1536>
```

### Collections of Vectors
```python
# List of vectors
LIST<FROZEN<VECTOR<FLOAT, 128>>>

# Map with vector values
MAP<TEXT, FROZEN<VECTOR<FLOAT, 768>>>
```

## Type Precision

Cassandra `VECTOR<FLOAT>` uses 32-bit floating-point precision:
- Values are stored as `float32`
- Some precision loss is expected (e.g., 0.1 → 0.10000000149011612)
- This is normal and matches Cassandra's storage format

## Integration Tests

Comprehensive tests ensure vector support works correctly:
- `tests/integration/test_vector_type.py` - Vector-specific tests
- `tests/integration/test_all_types_comprehensive.py` - Part of all-types testing

## Notes

- Vector support requires Cassandra 5.0 or later
- Vectors are returned as Python lists, not numpy arrays
- Empty vectors are stored as NULL in Cassandra
- Special float values (NaN, Inf) are preserved
