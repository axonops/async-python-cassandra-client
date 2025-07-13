# Stupid Code - Issues and Improvements

This file tracks inefficient or problematic code patterns that need improvement.

## 1. Memory Inefficiency in DataFrame Construction

**Current Issue**: We collect ALL rows in memory before converting to DataFrame
```python
# Current inefficient pattern in partition.py and streaming.py:
rows = []
async for row in stream:
    rows.append(row)  # Collecting all rows in memory!

# Only then convert to DataFrame
df = pd.DataFrame(rows)
```

**Why This is Stupid**:
- Uses 2x memory (rows list + DataFrame)
- Can't process data until ALL rows are collected
- No early termination possible
- Memory limit checks are inaccurate

**Better Approach**: Use streaming callbacks to build DataFrame incrementally
- async-cassandra supports callbacks during streaming
- Could build DataFrame in chunks
- Better memory efficiency
- Progressive processing

## 2. Not Using Parallel Stream Processing

**Current Issue**: Sequential stream processing
```python
# Current approach - one stream at a time
for token_range in token_ranges:
    df = await stream_token_range(...)
    dfs.append(df)
```

**Why This is Stupid**:
- Doesn't leverage async-cassandra's parallel streaming
- Slower than necessary
- Not utilizing available I/O concurrency

**Better Approach**: Use async-cassandra's parallel stream processing pattern
- Process multiple streams concurrently
- Better I/O utilization
- Faster overall execution

## 3. Token Pagination Implementation

**Previous Issue**: Only fetched ONE page of data!
```python
# TODO: Implement proper token extraction for pagination
break  # For now, just get one page
```

**Status**: FIXED - but the implementation is complex and could be cleaner

## 4. Thread Pool Management

**Current Issue**: Threads accumulate over time
- CDF threads not always cleaned up
- Dask threads persist
- No automatic cleanup

**Why This is Stupid**:
- Resource leaks in production
- Eventually exhausts system resources
- Manual cleanup is error-prone

## 5. UDT Handling with Dask

**Current Issue**: Dask converts dicts to strings
- We identified this is a Dask limitation
- Current workaround is to avoid Dask or parse strings

**Why This is Stupid**:
- Loses type information
- Requires extra parsing
- Not elegant

## 6. Consistency Level Implementation

**Current Issue**: Creates new ExecutionProfile for each query
```python
if consistency_level:
    execution_profile = create_execution_profile(consistency_level)
```

**Why This is Stupid**:
- Creates objects unnecessarily
- Could cache profiles
- Minor but inefficient

## Investigation Results

### 1. Parallel Stream Processing
After investigating async-cassandra's source:
- No built-in "parallel stream processing" pattern found
- We can implement it using asyncio.gather() with multiple streams
- Created `parallel_stream_to_dataframe` in incremental_builder.py

### 2. Streaming Callbacks
async-cassandra supports:
- `page_callback` in StreamConfig for progress tracking
- Callbacks are called after each page is fetched
- Can be used for progress reporting but NOT for data processing

### 3. Incremental DataFrame Building
Created `IncrementalDataFrameBuilder` which:
- Builds DataFrame in chunks as rows arrive
- More memory efficient than collecting all rows first
- Allows early termination on memory limits
- Better type conversion handling

## Action Items

1. **High Priority**:
   - [x] Investigate async-cassandra parallel stream processing
   - [ ] Implement incremental DataFrame building in main code
   - [ ] Fix thread pool cleanup
   - [ ] Replace current row collection with incremental builder

2. **Medium Priority**:
   - [ ] Cache execution profiles
   - [ ] Simplify token pagination logic
   - [ ] Add automatic thread cleanup
   - [ ] Benchmark incremental vs batch DataFrame building

3. **Low Priority**:
   - [ ] Find better solution for Dask UDT serialization
   - [ ] Add performance benchmarks

## Implementation Plan

1. **Replace row collection in streaming.py**:
   - Use IncrementalDataFrameBuilder instead of rows list
   - Stream directly into DataFrame chunks
   - Better memory efficiency

2. **Add parallel streaming to partition.py**:
   - Execute multiple token ranges concurrently
   - Use asyncio.gather for parallelism
   - Respect max_concurrent_partitions

3. **Fix thread cleanup**:
   - Ensure all executors are properly shutdown
   - Add context managers for thread pools
   - Implement automatic cleanup on idle

## Notes

- The codebase has improved significantly from initial state
- Main issues now are efficiency rather than correctness
- async-cassandra has advanced features we're not fully utilizing
