# Getting Started with async-cassandra

This guide walks you through the basics of using async-cassandra. For complete API documentation, see the [API Reference](api.md).

## Installation

```bash
pip install async-cassandra
```

## Requirements

- **Cassandra 4.0+** - Required for CQL protocol v5 support
- **Python 3.12+** - For modern async features
- **CQL Protocol v5+** - Older protocols are not supported

> **Note on Protocol Versions**: async-cassandra requires CQL protocol v5 or higher for optimal async performance. If you're using Cassandra 3.x or older, you'll need to upgrade. See the [protocol version documentation](#protocol-version-configuration) for details.

## Quick Start

### Your First async-cassandra Program

This example shows the minimum code needed to connect to Cassandra and run a query:

```python
import asyncio
from async_cassandra import AsyncCluster

async def main():
    # 1. Create a cluster object (doesn't connect yet)
    cluster = AsyncCluster(['localhost'])

    # 2. Connect and get a session (this is where connection happens)
    session = await cluster.connect('my_keyspace')

    # 3. Execute a query (waits for results without blocking)
    result = await session.execute("SELECT * FROM users LIMIT 10")

    # 4. Process results (rows are like dictionaries)
    for row in result:
        print(f"User: {row.name}, Email: {row.email}")

    # 5. Clean up (IMPORTANT: always close connections)
    await cluster.shutdown()

# Run the async function
asyncio.run(main())
```

### What's Different from Regular Cassandra Driver?

1. **Import AsyncCluster instead of Cluster**
2. **Use `await` for all database operations**
3. **Wrap your code in an async function**
4. **No blocking** - your app stays responsive

## Basic Usage Patterns

### Using Context Managers (Recommended)

Context managers ensure that resources are properly cleaned up after use, even if errors occur. When you use `async with`, Python guarantees that cleanup code runs no matter what happens.

**Why this matters**: async-cassandra's streaming operations can leak memory if not properly closed due to circular references in the callback system. Context managers prevent this.

> 📖 **Want to understand how context managers work?** See our [detailed explanation of context managers](context-managers-explained.md) to learn what happens behind the scenes.

**Without context manager (manual cleanup):**
```python
cluster = AsyncCluster(['localhost'])
session = await cluster.connect('my_keyspace')
try:
    result = await session.execute("SELECT * FROM users")
finally:
    await session.close()      # You must remember this
    await cluster.shutdown()   # And this!
```

**With context manager (automatic cleanup):**
```python
async with AsyncCluster(['localhost']) as cluster:
    async with await cluster.connect('my_keyspace') as session:
        result = await session.execute("SELECT * FROM users")
        # Python automatically calls close() and shutdown() for you
```

Benefits:
- No forgotten cleanup (prevents connection leaks)
- Cleaner code
- Exception safe (cleanup happens even if query fails)

### Authentication

If your Cassandra cluster requires authentication (most production clusters do), use the `create_with_auth` helper:

```python
cluster = AsyncCluster.create_with_auth(
    contact_points=['localhost'],
    username='cassandra',
    password='cassandra'
)

# Then connect as usual
session = await cluster.connect('my_keyspace')
```

**Note:** Never hardcode credentials in your code. Use environment variables or a secrets manager:

```python
import os

cluster = AsyncCluster.create_with_auth(
    contact_points=['localhost'],
    username=os.environ['CASSANDRA_USER'],
    password=os.environ['CASSANDRA_PASS']
)
```

### Prepared Statements

#### Why Use Prepared Statements?

Prepared statements are **required** for parameterized queries in async-cassandra. They provide:

1. **Security** - Prevents CQL injection attacks (like SQL injection)
2. **Performance** - Query is parsed once, executed many times
3. **Type Safety** - Cassandra validates parameter types

#### How They Work

```python
# ❌ This will NOT work - direct parameters not supported
await session.execute("SELECT * FROM users WHERE id = ?", [user_id])

# ✅ This works - prepare first, then execute
prepared = await session.prepare("SELECT * FROM users WHERE id = ?")
result = await session.execute(prepared, [user_id])
```

#### Example Usage

```python
# Prepare once (usually at startup)
user_query = await session.prepare("SELECT * FROM users WHERE id = ?")
insert_stmt = await session.prepare(
    "INSERT INTO users (id, name, email) VALUES (?, ?, ?)"
)

# Execute many times with different parameters
for user_id in user_ids:
    result = await session.execute(user_query, [user_id])
    user = result.one()
    print(user)

# Insert new users
await session.execute(insert_stmt, [uuid.uuid4(), "Alice", "alice@example.com"])
```

### Error Handling

#### Why Proper Error Handling Matters

In distributed systems like Cassandra, failures are normal, not exceptions. Network issues, node failures, and timeouts happen regularly. Your application needs to handle these gracefully.

#### Common Error Types

```python
from async_cassandra import ConnectionError, QueryError

try:
    result = await session.execute("SELECT * FROM users")
except ConnectionError as e:
    # Happens when: Can't reach Cassandra, node is down, network issues
    print(f"Connection failed: {e}")
    # What to do: Retry, use a different node, alert ops team

except QueryError as e:
    # Happens when: Bad CQL syntax, table doesn't exist, permissions issue
    print(f"Query failed: {e}")
    # What to do: Fix the query, check schema, verify permissions
```

#### Real-World Error Handling

```python
import asyncio
from async_cassandra import ConnectionError, QueryError

async def get_user_with_retry(session, user_id, max_retries=3):
    """Get user with automatic retry on connection errors."""
    for attempt in range(max_retries):
        try:
            prepared = await session.prepare("SELECT * FROM users WHERE id = ?")
            result = await session.execute(prepared, [user_id])
            return result.one()

        except ConnectionError as e:
            if attempt == max_retries - 1:
                # Final attempt failed, re-raise
                raise
            # Wait before retry (exponential backoff)
            wait_time = 2 ** attempt
            print(f"Connection failed, retrying in {wait_time}s...")
            await asyncio.sleep(wait_time)

        except QueryError as e:
            # Don't retry query errors - they won't fix themselves
            print(f"Query error (not retrying): {e}")
            raise
```

### Streaming Large Result Sets

#### The Memory Problem with Large Queries

When you query millions of rows, loading them all into memory will crash your application:

```python
# ❌ BAD: This loads ALL rows into memory at once
result = await session.execute("SELECT * FROM billion_row_table")
for row in result:  # 💥 OutOfMemoryError
    process(row)
```

#### The Streaming Solution

Streaming fetches rows in small batches (pages) as you need them:

```python
from async_cassandra.streaming import StreamConfig

# Configure streaming with page size
config = StreamConfig(
    fetch_size=1000  # Fetch 1000 rows at a time
)

# ⚠️ CRITICAL: Always use context manager to prevent memory leaks!
# Streaming results MUST be properly closed or memory will leak
async with await session.execute_stream(
    "SELECT * FROM billion_row_table",
    stream_config=config
) as result:
    # Process rows one at a time - only 1000 in memory at once
    async for row in result:
        await process_row(row)
        # Previous rows are garbage collected
# Result automatically closed when exiting context
```

#### ⚠️ CRITICAL: Proper Resource Cleanup

**IMPORTANT**: Streaming result sets create internal callbacks that can cause memory leaks if not properly closed. You MUST use one of these patterns:

**✅ BEST PRACTICE - Context Manager (Recommended):**
```python
# This ensures cleanup even if exceptions occur
async with await session.execute_stream(query, stream_config=config) as result:
    async for row in result:
        await process_row(row)
# Automatically closed, preventing memory leaks
```

**✅ Alternative - try/finally:**
```python
# Only use if context manager isn't possible
result = await session.execute_stream(query, stream_config=config)
try:
    async for row in result:
        await process_row(row)
finally:
    await result.close()  # CRITICAL: Must always close!
```

**❌ NEVER DO THIS - Memory Leak:**
```python
# This will leak memory!
result = await session.execute_stream(query, stream_config=config)
async for row in result:
    process_row(row)
# Result not closed - callbacks remain in memory forever!
```

#### Why This Happens

The streaming implementation uses callbacks to coordinate with the Cassandra driver. These callbacks create circular references that Python's garbage collector cannot clean up automatically. Without explicit cleanup via `close()` or a context manager, these references persist indefinitely, causing memory leaks.

> 📖 **Learn more**: For a detailed explanation of how context managers prevent these memory leaks, see [Understanding Context Managers](context-managers-explained.md).

#### When to Use Streaming

- ✅ Exporting data
- ✅ ETL processes
- ✅ Generating reports from large datasets
- ✅ Any query returning thousands+ of rows
- ❌ Don't use for small queries (adds overhead)

#### Complete Streaming Example

Here's a real-world example showing proper streaming usage with error handling:

```python
import asyncio
from async_cassandra import AsyncCluster
from async_cassandra.streaming import StreamConfig

async def export_user_data(session, output_file):
    """Export all users to a CSV file using streaming."""
    config = StreamConfig(
        fetch_size=5000,  # Process 5000 rows at a time
        page_callback=lambda page_num, rows: print(f"Processing page {page_num}...")
    )

    # CRITICAL: Use context manager for automatic cleanup
    async with await session.execute_stream(
        "SELECT id, name, email, created_at FROM users",
        stream_config=config
    ) as result:
        with open(output_file, 'w') as f:
            f.write("id,name,email,created_at\n")

            row_count = 0
            async for row in result:
                f.write(f"{row.id},{row.name},{row.email},{row.created_at}\n")
                row_count += 1

                # Progress update every 10,000 rows
                if row_count % 10000 == 0:
                    print(f"Exported {row_count:,} users...")

    # Result is automatically closed here, preventing memory leaks
    print(f"Export complete! Total users: {row_count:,}")
    return row_count

# Usage
async def main():
    async with AsyncCluster(['localhost']) as cluster:
        async with await cluster.connect('my_keyspace') as session:
            await export_user_data(session, 'users_export.csv')

asyncio.run(main())
```

**Key Points:**
1. Context manager (`async with`) ensures cleanup even if errors occur
2. Large `fetch_size` (5000) for better performance with small rows
3. Progress tracking without keeping all data in memory
4. File operations inside the streaming context for consistency

## Integration with Web Frameworks

### Why async-cassandra with FastAPI?

FastAPI is an async web framework. If you use the regular Cassandra driver, it will **block your entire web server** during database queries. This means your API can't handle other requests while waiting for Cassandra to respond.

### FastAPI Example

#### The Setup

```python
from fastapi import FastAPI, HTTPException
from async_cassandra import AsyncCluster
import uuid

app = FastAPI()

# Global variables for cluster and session
cluster = None
session = None
prepared_statements = {}

@app.on_event("startup")
async def startup():
    """Initialize Cassandra connection when server starts."""
    global cluster, session, prepared_statements

    # Create cluster connection
    cluster = AsyncCluster(['localhost'])
    session = await cluster.connect('my_keyspace')

    # Prepare statements once at startup (more efficient)
    prepared_statements['get_user'] = await session.prepare(
        "SELECT * FROM users WHERE id = ?"
    )
    prepared_statements['create_user'] = await session.prepare(
        "INSERT INTO users (id, name, email) VALUES (?, ?, ?)"
    )

@app.on_event("shutdown")
async def shutdown():
    """Clean up when server stops."""
    if cluster:
        await cluster.shutdown()
```

#### API Endpoints

```python
@app.get("/users/{user_id}")
async def get_user(user_id: str):
    """Get a user by ID."""
    try:
        # Convert string to UUID
        user_uuid = uuid.UUID(user_id)

        # Execute prepared statement
        result = await session.execute(
            prepared_statements['get_user'],
            [user_uuid]
        )

        user = result.one()
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        return {
            "id": str(user.id),
            "name": user.name,
            "email": user.email
        }

    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid user ID format")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/users")
async def create_user(name: str, email: str):
    """Create a new user."""
    user_id = uuid.uuid4()

    await session.execute(
        prepared_statements['create_user'],
        [user_id, name, email]
    )

    return {"id": str(user_id), "name": name, "email": email}
```

#### Why This Pattern?

1. **Global session**: Creating connections is expensive - do it once at startup
2. **Prepared statements at startup**: Better performance, prepare once, use many times
3. **Proper error handling**: Convert Cassandra errors to HTTP errors
4. **UUID handling**: Cassandra uses UUIDs, web uses strings - convert properly

## Protocol Version Configuration

### Understanding Protocol Versions

CQL (Cassandra Query Language) protocol versions define how clients communicate with Cassandra. async-cassandra requires **protocol v5 or higher** because:

- **Better streaming**: v5+ has improved streaming capabilities for large result sets
- **Enhanced features**: Modern error handling, better metadata, improved performance
- **Async alignment**: Features that work better with async patterns

### Configuring Protocol Version

```python
# Option 1: Automatic negotiation (RECOMMENDED)
# Driver negotiates to highest available version
cluster = AsyncCluster(['localhost'])  # Gets v5 (highest currently supported)

# Option 2: Explicitly specify v5
cluster = AsyncCluster(['localhost'], protocol_version=5)  # Forces v5 exactly

# ❌ This will raise ConfigurationError
cluster = AsyncCluster(['localhost'], protocol_version=4)
# Error: Protocol version 4 is not supported. async-cassandra requires CQL protocol v5 or higher...
```

**Important**: async-cassandra verifies protocol v5+ **after connection**:
- Driver negotiates to the highest available version (currently v5)
- Fails with clear error if server only supports v4 or lower
- Ensures compatibility with modern Cassandra features

> **Note**: As of Cassandra 5.0, the maximum supported protocol version is v5. Protocol v5 is considered stable and is the recommended version for production use.

### Upgrading from Older Cassandra

If you're currently using Cassandra 3.x (protocol v4) or older:

1. **Upgrade Cassandra**: Version 4.0+ supports protocol v5
   ```bash
   # Check your current Cassandra version
   nodetool version
   ```

2. **Cloud Services**: Most cloud providers already support v5+
   - AWS Keyspaces: Supports v4 and v5
   - Azure Cosmos DB: Check current documentation
   - DataStax Astra: Supports v5+

3. **Migration Path**:
   - Test with Cassandra 4.x in development first
   - Protocol v5 is backward compatible with v4 features
   - No code changes needed beyond the upgrade

### Common Protocol Version Errors

```python
# Error when using old protocol
try:
    cluster = AsyncCluster(['localhost'], protocol_version=3)
except ConfigurationError as e:
    print(e)
    # Output: Protocol version 3 is not supported. async-cassandra requires
    # CQL protocol v5 or higher for optimal async performance...
```

## Next Steps

- [API Reference](api.md) - Complete API documentation
- [Connection Pooling](connection-pooling.md) - Understanding connection behavior
- [Streaming](streaming.md) - Handling large result sets
- [Performance](performance.md) - Optimization tips
- [FastAPI Example](../examples/fastapi_app/) - Full production example
