# Troubleshooting Guide

This guide helps you diagnose and fix common issues with async-cassandra.

## Table of Contents

- [Connection Issues](#connection-issues)
- [Query Execution Problems](#query-execution-problems)
- [Performance Issues](#performance-issues)
- [Async/Await Problems](#asyncawait-problems)
- [Integration Issues](#integration-issues)
- [Common Errors](#common-errors)

## Connection Issues

### Cannot Connect to Cassandra

**Symptoms:**
- `NoHostAvailable` exception
- Connection timeouts
- "Connection refused" errors

**Solutions:**

1. **Verify Cassandra is running:**
   ```bash
   # Check if Cassandra is listening
   netstat -an | grep 9042

   # Or with ss
   ss -tlnp | grep 9042
   ```

2. **Check contact points:**
   ```python
   # Wrong - using hostname without DNS
   cluster = AsyncCluster(['cassandra-server'])

   # Right - use IP or ensure DNS works
   cluster = AsyncCluster(['192.168.1.10'])
   ```

3. **Verify port configuration:**
   ```python
   # Default port is 9042
   cluster = AsyncCluster(['localhost'], port=9042)

   # If using custom port
   cluster = AsyncCluster(['localhost'], port=9043)
   ```

4. **Check firewall rules:**
   ```bash
   # Allow Cassandra port
   sudo ufw allow 9042/tcp
   ```

### Authentication Failures

**Symptoms:**
- `AuthenticationFailed` exception
- "Username and/or password are incorrect" errors

**Solutions:**

```python
# Use proper authentication
cluster = AsyncCluster.create_with_auth(
    contact_points=['localhost'],
    username='cassandra',
    password='cassandra'
)

# Or with PlainTextAuthProvider
from cassandra.auth import PlainTextAuthProvider

auth = PlainTextAuthProvider(username='user', password='pass')
cluster = AsyncCluster(
    contact_points=['localhost'],
    auth_provider=auth
)
```

## Query Execution Problems

### Query Timeout

**Symptoms:**
- `OperationTimedOut` exception
- Queries hang indefinitely

**Solutions:**

1. **Increase timeout:**
   ```python
   # Set custom timeout (in seconds)
   result = await session.execute(
       "SELECT * FROM large_table",
       timeout=30.0
   )
   ```

2. **Use paging for large results:**
   ```python
   from cassandra.query import SimpleStatement

   statement = SimpleStatement(
       "SELECT * FROM large_table",
       fetch_size=100  # Fetch 100 rows at a time
   )
   result = await session.execute(statement)
   ```

3. **Optimize your query:**
   - Add appropriate WHERE clauses
   - Use LIMIT for testing
   - Ensure proper indexes exist

### Keyspace Does Not Exist

**Symptoms:**
- `InvalidRequest: Keyspace 'xxx' does not exist`

**Solutions:**

```python
# Create keyspace if it doesn't exist
await session.execute("""
    CREATE KEYSPACE IF NOT EXISTS my_keyspace
    WITH REPLICATION = {
        'class': 'SimpleStrategy',
        'replication_factor': 1
    }
""")

# Then use it
await session.set_keyspace('my_keyspace')
```

## Performance Issues

### Slow Query Performance

**Symptoms:**
- High latency on queries
- Poor throughput
- Application timeouts

**Solutions:**

1. **Use prepared statements:**
   ```python
   # Prepare once
   prepared = await session.prepare(
       "SELECT * FROM users WHERE id = ?"
   )

   # Execute many times
   for user_id in user_ids:
       result = await session.execute(prepared, [user_id])
   ```

2. **Batch related operations:**
   ```python
   from cassandra.query import BatchStatement

   # Prepare the statement first
   insert_stmt = await session.prepare(
       "INSERT INTO table (id, data) VALUES (?, ?)"
   )

   batch = BatchStatement()
   for item in items:
       batch.add(insert_stmt, [item.id, item.data])
   await session.execute(batch)  # Note: use execute(), not execute_batch()
   ```

3. **Use connection warmup:**
   ```python
   from async_cassandra import ConnectionMonitor

   monitor = ConnectionMonitor(session)
   await monitor.warmup_connections()
   ```

### High Memory Usage

**Symptoms:**
- Memory keeps growing
- Out of memory errors
- Process gets killed

**Solutions:**

1. **Use async iteration instead of `.all()`:**
   ```python
   # Bad - loads all rows into memory
   all_rows = result.all()
   for row in all_rows:
       process(row)

   # Good - processes one row at a time
   async for row in result:
       process(row)
   ```

2. **Limit fetch size:**
   ```python
   statement = SimpleStatement(
       "SELECT * FROM large_table",
       fetch_size=100  # Small batches
   )
   ```

## Async/Await Problems

### "Cannot Run Coroutine" Errors

**Symptoms:**
- `RuntimeError: This event loop is already running`
- `RuntimeWarning: coroutine was never awaited`

**Solutions:**

1. **Always await async functions:**
   ```python
   # Wrong
   result = session.execute("SELECT * FROM users")

   # Right
   result = await session.execute("SELECT * FROM users")
   ```

2. **Use asyncio.run() properly:**
   ```python
   # Wrong - in Jupyter/IPython
   asyncio.run(main())

   # Right - in Jupyter/IPython
   await main()

   # Right - in scripts
   if __name__ == "__main__":
       asyncio.run(main())
   ```

### Deadlocks with sync code

**Symptoms:**
- Application hangs
- No progress on queries

**Solutions:**

```python
# Don't mix sync and async incorrectly
# Wrong
def sync_function():
    # This will deadlock!
    result = asyncio.run(session.execute("SELECT * FROM users"))

# Right - keep everything async
async def async_function():
    result = await session.execute("SELECT * FROM users")
```

## Integration Issues

### FastAPI Integration

**Common setup:**

```python
from fastapi import FastAPI, Depends
from contextlib import asynccontextmanager
from async_cassandra import AsyncCluster

cluster = None
session = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global cluster, session
    # Startup
    cluster = AsyncCluster(['localhost'])
    session = await cluster.connect()
    yield
    # Shutdown
    if session:
        await session.close()
    if cluster:
        await cluster.shutdown()

app = FastAPI(lifespan=lifespan)

async def get_session():
    return session

@app.get("/users/{user_id}")
async def get_user(user_id: str, session=Depends(get_session)):
    result = await session.execute(
        "SELECT * FROM users WHERE id = ?",
        [user_id]
    )
    return result.one()
```

## Common Errors

### TypeError: 'NoneType' object is not iterable

**Cause:** Trying to iterate over None result

**Solution:**
```python
result = await session.execute(query)
row = result.one()

# Check if row exists
if row:
    process(row)
else:
    print("No results found")
```

### CassandraError: Session is closed

**Cause:** Using session after closing it

**Solution:**
```python
# Use context managers
async with AsyncCluster(['localhost']) as cluster:
    async with await cluster.connect() as session:
        # Session is automatically closed after this block
        result = await session.execute(query)
```

### ImportError: cannot import name 'AsyncCassandraSession'

**Cause:** Incorrect import

**Solution:**
```python
# Correct imports
from async_cassandra import AsyncCluster, AsyncCassandraSession

# Or import everything
import async_cassandra
```

## Getting Help

If you're still having issues:

1. **Check the logs** - Enable debug logging:
   ```python
   import logging
   logging.basicConfig(level=logging.DEBUG)
   ```

2. **Minimal reproduction** - Create a small script that reproduces the issue

3. **Report the issue** - Include:
   - Python version
   - async-cassandra version
   - Cassandra version
   - Full error traceback
   - Minimal code to reproduce

**Contact:**
- GitHub Issues: https://github.com/axonops/async-python-cassandra-client/issues
- Website: https://axonops.com
