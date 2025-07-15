# Fixes Applied to async-cassandra-dataframe

## Problem
The library had a critical bug where parallel execution (the default) was creating Dask DataFrames with only 1 partition, completely defeating the purpose of using Dask for distributed computing.

## Solution
1. **Removed Parallel Execution Path**
   - The parallel execution code was fundamentally broken - it combined all partitions into a single DataFrame
   - Now always uses delayed execution which properly maintains multiple Dask partitions

2. **Added Intelligent Partitioning Strategies**
   - Created `partition_strategy.py` with AUTO, NATURAL, COMPACT, and FIXED strategies
   - Strategies consider Cassandra's token ring architecture and vnode configuration
   - Note: Full implementation still TODO - currently calculates ideal grouping but doesn't apply it

3. **Added Predicate Pushdown Validation**
   - Prevents full table scans by ensuring partition keys are in predicates
   - Provides clear error messages when `require_partition_key_predicate=True`
   - Can be disabled for special cases

## Files Changed
- `src/async_cassandra_dataframe/reader.py` - Main fixes
- `src/async_cassandra_dataframe/partition_strategy.py` - New file
- Tests added for all new functionality

## Result
- Dask DataFrames now correctly have multiple partitions
- Each Cassandra partition becomes a Dask partition
- Proper lazy evaluation and distributed computing preserved
