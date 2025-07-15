# SPLIT Partitioning Strategy

The SPLIT strategy provides manual control over Dask partition count by splitting each Cassandra token range into N sub-partitions.

## When to Use

Use the SPLIT strategy when:
- Automatic partition calculations are too conservative
- You need more parallelism for large datasets
- Token ranges contain uneven data distribution
- You want fine-grained control over partition count

## Usage

```python
import async_cassandra_dataframe as cdf

# Split each token range into 3 sub-partitions
df = await cdf.read_cassandra_table(
    "my_table",
    session=session,
    partitioning_strategy="split",  # Use SPLIT strategy
    split_factor=3,                  # Split each range into 3
)

# Example: 17 token ranges * 3 splits = 51 Dask partitions
```

## How It Works

1. Discovers natural token ranges from Cassandra cluster
2. Splits each token range into N equal sub-ranges
3. Creates one Dask partition per sub-range

## Examples

### Basic Usage
```python
# Default AUTO strategy (conservative)
df_auto = await cdf.read_cassandra_table("my_table", session=session)
# Result: 2 partitions for medium dataset

# SPLIT strategy with factor 5
df_split = await cdf.read_cassandra_table(
    "my_table",
    session=session,
    partitioning_strategy="split",
    split_factor=5,
)
# Result: 85 partitions (17 ranges * 5)
```

### High Parallelism
```python
# For CPU-intensive processing, increase parallelism
df = await cdf.read_cassandra_table(
    "large_table",
    session=session,
    partitioning_strategy="split",
    split_factor=10,  # 10x more partitions
)

# Process with Dask
result = df.map_partitions(expensive_computation).compute()
```

### Comparison with Other Strategies

| Strategy | Use Case | Partition Count |
|----------|----------|-----------------|
| AUTO | General purpose | Conservative (2-10) |
| NATURAL | Maximum parallelism | One per token range |
| COMPACT | Memory-bounded | Based on target size |
| FIXED | Specific count | User-specified |
| SPLIT | Manual control | Token ranges * split_factor |

## Performance Considerations

- Higher split_factor = more parallelism but also more overhead
- Each partition requires a separate Cassandra query
- Optimal split_factor depends on:
  - Data volume per token range
  - Available CPU cores
  - Processing complexity
  - Network latency

## Recommendations

- Start with split_factor=2-5 for most cases
- Use 10+ for CPU-intensive processing on large clusters
- Monitor partition sizes with logging
- Adjust based on performance measurements
