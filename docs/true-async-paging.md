# True Async Paging in async-cassandra

## Key Concepts

### 1. Always Use Context Managers (CRITICAL)

```python
# ✅ CORRECT - Prevents resource leaks
async with await session.execute_stream("SELECT * FROM table") as result:
    async for row in result:
        await process_row(row)

# ❌ WRONG - Will leak resources!
result = await session.execute_stream("SELECT * FROM table")
async for row in result:  # Missing context manager!
    await process_row(row)
```

### 2. How Paging Actually Works

The Cassandra driver implements **true streaming** with these characteristics:

- **On-Demand Fetching**: Pages are fetched as you consume data, NOT all at once
- **Async Fetching**: While you process page N, the driver can fetch page N+1
- **Memory Efficient**: Only one page is held in memory at a time
- **No Pre-fetching All Data**: The driver doesn't load the entire result set

### 3. Page Size Recommendations

```python
# Small Pages (1000-5000 rows)
# ✅ Best for: Real-time processing, low memory usage, better responsiveness
# ❌ Trade-off: More network round trips
config = StreamConfig(fetch_size=1000)

# Medium Pages (5000-10000 rows)
# ✅ Best for: General purpose, good balance
config = StreamConfig(fetch_size=5000)

# Large Pages (10000-50000 rows)
# ✅ Best for: Bulk exports, batch processing, fewer round trips
# ❌ Trade-off: Higher memory usage, slower first results
config = StreamConfig(fetch_size=20000)
```

### 4. LIMIT vs Paging

**You don't need LIMIT with paging!**

```python
# ❌ UNNECESSARY - fetch_size already controls data flow
stmt = await session.prepare("SELECT * FROM users LIMIT ?")
async with await session.execute_stream(stmt, [1000]) as result:
    # This limits total results, not page size!

# ✅ CORRECT - Let paging handle the data flow
stmt = await session.prepare("SELECT * FROM users")
config = StreamConfig(fetch_size=1000)  # This controls page size
async with await session.execute_stream(stmt, stream_config=config) as result:
    # Process all data efficiently, page by page
```

### 5. Processing Patterns

#### Row-by-Row Processing
```python
# Process each row as it arrives
async with await session.execute_stream("SELECT * FROM large_table") as result:
    async for row in result:
        await process_row(row)  # Non-blocking, pages fetched as needed
```

#### Page-by-Page Processing
```python
# Process entire pages at once (e.g., for batch operations)
config = StreamConfig(fetch_size=5000)
async with await session.execute_stream("SELECT * FROM large_table", stream_config=config) as result:
    async for page in result.pages():
        # Process entire page (list of rows)
        await bulk_insert_to_warehouse(page)
```

### 6. Common Misconceptions

**Myth**: "The driver pre-fetches all pages"
**Reality**: Pages are fetched on-demand as you consume data

**Myth**: "I need LIMIT to control memory usage"
**Reality**: `fetch_size` controls memory usage, LIMIT just limits total results

**Myth**: "Larger pages are always better"
**Reality**: It depends on your use case - see recommendations above

**Myth**: "I can skip the context manager"
**Reality**: Context managers are MANDATORY to prevent resource leaks

### 7. Performance Tips

1. **Match fetch_size to your processing speed**
   - Fast processing → larger pages
   - Slow processing → smaller pages

2. **Use page callbacks for monitoring**
   ```python
   config = StreamConfig(
       fetch_size=5000,
       page_callback=lambda page_num, total_rows:
           logger.info(f"Processing page {page_num}, total: {total_rows:,}")
   )
   ```

3. **Consider network latency**
   - High latency → larger pages (fewer round trips)
   - Low latency → smaller pages are fine

4. **Monitor memory usage**
   - Each page holds `fetch_size` rows in memory
   - Adjust based on row size and available memory
