# Partition Strategy Design

## Overview

This document outlines the new partitioning strategy that properly aligns Cassandra token ranges with Dask DataFrame partitions while providing intelligent defaults.

## Core Principles

1. **Respect Cassandra's Architecture**: Never split natural token ranges
2. **Maintain Lazy Evaluation**: Use Dask delayed execution exclusively
3. **Intelligent Defaults**: Auto-detect optimal partitioning based on cluster topology
4. **Flexible User Control**: Allow override when users know better

## Partitioning Strategies

### 1. AUTO (Default)
Intelligently determines partition count based on:
- Cluster topology (nodes, vnodes, replication factor)
- Estimated table size
- Available memory

```python
# Heuristics:
- High vnode count (256): Group aggressively (10-50 partitions per node)
- Low vnode count (1-16): Close to natural ranges
- Single node: Based on data size estimates
```

### 2. NATURAL
One Dask partition per Cassandra token range
- Maximum parallelism
- Higher overhead for high vnode clusters
- Best for compute-intensive operations

### 3. COMPACT
Balance between parallelism and overhead
- Groups small ranges together
- Target partition size (default 1GB)
- Respects natural boundaries

### 4. FIXED
User specifies exact partition count
- Maps to closest achievable count
- Never exceeds natural token ranges

## Implementation Plan

### Phase 1: Core Changes

1. **Remove Parallel Execution Path**
   - Delete the parallel execution code that creates single partition
   - Make delayed execution the only path

2. **Enhance Token Range Grouping**
   ```python
   def group_token_ranges(
       natural_ranges: List[TokenRange],
       strategy: PartitioningStrategy,
       target_count: Optional[int] = None,
       target_size_mb: int = 1024
   ) -> List[List[TokenRange]]:
       """Group natural token ranges into Dask partitions."""
   ```

3. **Update Reader Interface**
   ```python
   async def read(
       self,
       columns: List[str] = None,
       partition_strategy: str = "auto",  # New parameter
       partition_count: Optional[int] = None,
       target_partition_size_mb: int = 1024,
       # Remove use_parallel_execution parameter
   ) -> dd.DataFrame:
   ```

### Phase 2: Smart Grouping Algorithm

```python
class TokenRangeGrouper:
    """Groups token ranges into optimal Dask partitions."""

    def group_by_locality(self, ranges: List[TokenRange]) -> Dict[str, List[TokenRange]]:
        """Group ranges by primary replica for data locality."""

    def balance_partition_sizes(self, groups: Dict[str, List[TokenRange]]) -> List[List[TokenRange]]:
        """Balance groups to create evenly sized partitions."""

    def respect_memory_limits(self, groups: List[List[TokenRange]]) -> List[List[TokenRange]]:
        """Ensure no partition exceeds memory limits."""
```

### Phase 3: Partition Execution

Each Dask partition will:
1. Receive a list of token ranges to query
2. Execute queries in parallel within the partition
3. Stream results with memory management
4. Return combined pandas DataFrame

```python
def read_partition_ranges(
    session: AsyncSession,
    table: str,
    keyspace: str,
    ranges: List[TokenRange],
    columns: List[str],
    predicates: Dict[str, Any]
) -> pd.DataFrame:
    """Read multiple token ranges for a single Dask partition."""
    # This runs in a thread via dask.delayed
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        return loop.run_until_complete(
            _read_ranges_async(session, table, keyspace, ranges, columns, predicates)
        )
    finally:
        loop.close()

async def _read_ranges_async(...) -> pd.DataFrame:
    """Async implementation of range reading."""
    tasks = [
        stream_token_range(session, table, range, columns, predicates)
        for range in ranges
    ]
    dfs = await asyncio.gather(*tasks)
    return pd.concat(dfs, ignore_index=True)
```

## Configuration

### Environment Variables
```bash
CASSANDRA_DF_DEFAULT_STRATEGY=auto  # auto, natural, compact, fixed
CASSANDRA_DF_TARGET_PARTITION_SIZE_MB=1024
CASSANDRA_DF_MAX_PARTITIONS_PER_NODE=50
```

### Runtime Configuration
```python
reader = CassandraDataFrameReader(
    session,
    table,
    default_partition_strategy="auto"
)

df = await reader.read(
    partition_strategy="compact",
    target_partition_size_mb=2048
)
```

## Migration Path

1. **Deprecation Warning**: Add warning when `use_parallel_execution=True`
2. **Default Change**: Switch default to delayed execution
3. **Remove Parameter**: Remove `use_parallel_execution` in next major version

## Testing Strategy

1. **Unit Tests**: Token range grouping algorithms
2. **Integration Tests**: Various cluster topologies
3. **Performance Tests**: Compare strategies on real data
4. **Memory Tests**: Verify lazy evaluation and streaming

## Success Metrics

1. **Multiple Dask Partitions**: Always creates appropriate number of partitions
2. **Lazy Evaluation**: No data loaded until compute()
3. **Memory Efficiency**: Can handle tables larger than RAM
4. **Performance**: Better or equal to current implementation
5. **Compatibility**: Works with existing code (with deprecation warnings)
