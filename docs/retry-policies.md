# Retry Policies in async-cassandra

## Why Do We Have Our Own Retry Policy?

You might wonder why async-cassandra implements its own retry policy when the cassandra-driver already provides several. The answer is **safety and correctness** - our retry policy adds critical idempotency checking that prevents data corruption.

## The Problem with Default Retry Policies

The cassandra-driver provides these retry policies:
- `RetryPolicy` - The default, but it has limitations
- `FallthroughRetryPolicy` - Never retries anything
- `DowngradingConsistencyRetryPolicy` - Deprecated, downgrades consistency

**None of these check whether a query is idempotent before retrying writes!**

### What's the Risk?

Without idempotency checking, retrying write operations can cause:

1. **Duplicate Inserts**
   ```python
   # If this times out and gets retried...
   INSERT INTO users (id, email) VALUES (123, 'user@example.com')
   # You could end up with duplicate records!
   ```

2. **Multiple Counter Updates**
   ```python
   # If this times out and gets retried...
   UPDATE stats SET views = views + 1 WHERE page_id = 456
   # The counter could be incremented multiple times!
   ```

3. **Data Corruption**
   ```python
   # If this times out and gets retried...
   UPDATE accounts SET balance = balance - 100 WHERE id = 789
   # The account could be debited multiple times!
   ```

## How async-cassandra's Retry Policy Works

Our `AsyncRetryPolicy` handles different operation types intelligently:

### For READ Operations (SELECTs)

SELECTs are inherently idempotent - running the same query multiple times doesn't change data. The retry policy automatically retries read timeouts when it makes sense:

```python
# From async_cassandra/retry_policy.py
def on_read_timeout(self, ...):
    if retry_num >= self.max_retries:
        return self.RETHROW, None

    # If we got some data, retry might succeed
    if data_retrieved:
        return self.RETRY, consistency

    # If we got enough responses, retry at same consistency
    if received_responses >= required_responses:
        return self.RETRY, consistency

    return self.RETHROW, None
```

This means:
- **SELECTs are automatically retried** on timeout (up to max_retries)
- **No idempotency check needed** for reads - they're always safe to retry
- **Smart retry logic** - only retries if there's a good chance of success

### For WRITE Operations (INSERT, UPDATE, DELETE)

Writes are where idempotency becomes critical:

```python
# From async_cassandra/retry_policy.py
def on_write_timeout(self, query, ...):
    if retry_num >= self.max_retries:
        return self.RETHROW, None

    # CRITICAL: Only retry if query is explicitly marked as idempotent
    if getattr(query, "is_idempotent", None) is not True:
        # Query is not idempotent - do not retry
        return self.RETHROW, None

    # Only retry simple and batch writes that are explicitly idempotent
    if write_type in ("SIMPLE", "BATCH"):
        return self.RETRY, consistency

    return self.RETHROW, None
This means:
- **Safe writes are retried** - If you mark a query as idempotent, it will be retried on timeout
- **Unsafe writes are not retried** - Non-idempotent writes fail fast to prevent corruption
- **Explicit opt-in** - You must explicitly mark write queries as idempotent

## How to Use It

### Read Queries (SELECTs)

No special marking needed - SELECTs are automatically retried:

```python
# Automatically retried on timeout - no need to mark as idempotent
result = await session.execute("SELECT * FROM users WHERE id = ?", [123])

# Also automatically retried
stmt = SimpleStatement("SELECT name, email FROM users WHERE active = true")
result = await session.execute(stmt)
```

### Write Queries (INSERT, UPDATE, DELETE)

For writes, you must explicitly mark idempotent queries:

```python
from cassandra.query import SimpleStatement

# Safe to retry - using IF NOT EXISTS makes it idempotent
stmt = SimpleStatement(
    "INSERT INTO users (id, email) VALUES (?, ?) IF NOT EXISTS",
    is_idempotent=True
)
result = await session.execute(stmt, [123, 'user@example.com'])

# Safe to retry - setting a value is idempotent
stmt = SimpleStatement(
    "UPDATE users SET last_login = ? WHERE id = ?",
    is_idempotent=True
)
result = await session.execute(stmt, [datetime.now(), 123])

# NOT safe to retry - incrementing is not idempotent
stmt = SimpleStatement(
    "UPDATE counters SET views = views + 1 WHERE page_id = ?"
    # Note: is_idempotent is NOT set (defaults to False)
)
result = await session.execute(stmt, [456])
```

### Using the Retry Policy

The `AsyncRetryPolicy` is automatically used by default:

```python
from async_cassandra import AsyncCluster

# Uses AsyncRetryPolicy automatically
cluster = AsyncCluster(['localhost'])
session = await cluster.connect()

# Or specify explicitly with custom settings
from async_cassandra.retry_policy import AsyncRetryPolicy

cluster = AsyncCluster(
    ['localhost'],
    retry_policy=AsyncRetryPolicy(max_retries=5)
)
```

## Comparison with Driver's Default Behavior

| Scenario | Driver's RetryPolicy | async-cassandra's AsyncRetryPolicy |
|----------|---------------------|-----------------------------------|
| **Read timeout (SELECT)** | Retries if data retrieved | Same, with max retry limit |
| Write timeout (BATCH_LOG) | Always retries | Only if marked idempotent |
| Write timeout (SIMPLE/BATCH) | Never retries | Only if marked idempotent |
| Unavailable | Retries with next host | Same, with max retry limit |

The key difference is for write operations - we add safety by checking idempotency.

## Best Practices

1. **Always consider idempotency** - Think about whether your write can be safely retried
2. **Use IF NOT EXISTS/IF EXISTS** - These make INSERTs and DELETEs idempotent
3. **Set absolute values, not increments** - `SET count = 5` is idempotent, `SET count = count + 1` is not
4. **Use prepared statements** - They can be marked as idempotent once and reused

## Example: Different Query Types

```python
# ✅ SELECTs - Always safe to retry (automatically handled)
async def get_user(user_id):
    # No need to mark as idempotent - SELECTs are always retried
    return await session.execute(
        "SELECT * FROM users WHERE id = ?",
        [user_id]
    )

async def search_users(status):
    # Also automatically retried on timeout
    return await session.execute(
        "SELECT * FROM users WHERE status = ? ALLOW FILTERING",
        [status]
    )

# ✅ IDEMPOTENT WRITES - Must be explicitly marked
async def create_user_idempotent(user_id, email):
    stmt = SimpleStatement(
        "INSERT INTO users (id, email) VALUES (?, ?) IF NOT EXISTS",
        is_idempotent=True  # Safe because of IF NOT EXISTS
    )
    return await session.execute(stmt, [user_id, email])

# ❌ NON-IDEMPOTENT WRITES - Should NOT be marked
async def increment_login_count(user_id):
    # This MUST NOT be marked as idempotent - could increment multiple times
    return await session.execute(
        "UPDATE users SET login_count = login_count + 1 WHERE id = ?",
        [user_id]
    )
```

## More Examples: Idempotent vs Non-Idempotent Writes

```python
# ✅ IDEMPOTENT - Safe to retry
async def create_user_idempotent(user_id, email):
    stmt = SimpleStatement(
        "INSERT INTO users (id, email) VALUES (?, ?) IF NOT EXISTS",
        is_idempotent=True
    )
    return await session.execute(stmt, [user_id, email])

# ❌ NOT IDEMPOTENT - Could create duplicates if retried
async def create_user_unsafe(user_id, email):
    # Without IF NOT EXISTS, retrying could insert multiple times
    return await session.execute(
        "INSERT INTO users (id, email) VALUES (?, ?)",
        [user_id, email]
    )

# ✅ IDEMPOTENT - Setting to absolute value
async def update_user_status_idempotent(user_id, status):
    stmt = SimpleStatement(
        "UPDATE users SET status = ?, updated_at = ? WHERE id = ?",
        is_idempotent=True
    )
    return await session.execute(stmt, [status, datetime.now(), user_id])

# ❌ NOT IDEMPOTENT - Incrementing counter
async def increment_login_count(user_id):
    # This MUST NOT be retried - could increment multiple times
    return await session.execute(
        "UPDATE users SET login_count = login_count + 1 WHERE id = ?",
        [user_id]
    )
```

## Summary

async-cassandra's retry policy provides intelligent retry behavior:

1. **SELECTs are automatically retried** - The retry policy's `on_read_timeout` method returns `RETRY` when appropriate conditions are met (data retrieved or sufficient responses received)
2. **Writes require explicit idempotency marking** - Prevents accidental data corruption by checking `is_idempotent=True`
3. **Configurable retry limits** - Default is 3 retries, avoiding infinite retry loops
4. **Smart retry decisions** - Only retries when there's a good chance of success

The key insight is that while the cassandra-driver's retry policy works, it doesn't distinguish between safe and unsafe write operations. Our retry policy adds this critical safety check while maintaining all the benefits of automatic retries for read operations.

## Technical Details

The retry behavior is handled by the underlying cassandra-driver:
- When a timeout exception occurs, the driver calls the retry policy's appropriate method
- If the policy returns `RETRY`, the driver automatically retries the query
- This happens transparently - your code just sees either success or final failure

The `AsyncRetryPolicy` is automatically set as the default retry policy when creating an `AsyncCluster`, so all queries benefit from this behavior without any additional configuration.
