# API Reference

## Table of Contents

- [AsyncCluster](#asynccluster)
- [AsyncCassandraSession](#asynccassandrasession)
- [AsyncResultSet](#asyncresultset)
- [AsyncRetryPolicy](#asyncretrypolicy)
- [Exceptions](#exceptions)

## AsyncCluster

Manages cluster configuration and connection lifecycle.

### Constructor

```python
AsyncCluster(
    contact_points: Optional[List[str]] = None,
    port: int = 9042,
    auth_provider: Optional[AuthProvider] = None,
    load_balancing_policy: Optional[LoadBalancingPolicy] = None,
    reconnection_policy: Optional[ReconnectionPolicy] = None,
    retry_policy: Optional[RetryPolicy] = None,
    ssl_context: Optional[SSLContext] = None,
    protocol_version: Optional[int] = None,
    executor_threads: int = 2,
    max_schema_agreement_wait: int = 10,
    control_connection_timeout: float = 2.0,
    idle_heartbeat_interval: float = 30.0,
    schema_event_refresh_window: float = 2.0,
    topology_event_refresh_window: float = 10.0,
    status_event_refresh_window: float = 2.0,
    **kwargs
)
```

**Parameters:**
- `contact_points`: List of contact points (default: ["127.0.0.1"])
- `port`: Port to connect to (default: 9042)
- `auth_provider`: Authentication provider
- `load_balancing_policy`: Load balancing policy
- `reconnection_policy`: Reconnection policy
- `retry_policy`: Retry policy (default: AsyncRetryPolicy)
- `ssl_context`: SSL context for secure connections
- `protocol_version`: CQL protocol version (must be 5 or higher if specified). If omitted, driver negotiates the highest available version. Connection fails if negotiated version < 5.
- `executor_threads`: Number of threads for I/O operations (default: 2)
- `max_schema_agreement_wait`: Max time to wait for schema agreement (default: 10)
- `control_connection_timeout`: Timeout for control connection (default: 2.0)
- `idle_heartbeat_interval`: Interval for connection heartbeat (default: 30.0)
- `schema_event_refresh_window`: Window for schema event refresh (default: 2.0)
- `topology_event_refresh_window`: Window for topology event refresh (default: 10.0)
- `status_event_refresh_window`: Window for status event refresh (default: 2.0)
- `**kwargs`: Additional cluster options passed to underlying driver

### Class Methods

#### `create_with_auth`

```python
@classmethod
def create_with_auth(
    cls,
    contact_points: List[str],
    username: str,
    password: str,
    **kwargs
) -> AsyncCluster
```

Create cluster with username/password authentication.

### Methods

#### `connect`

```python
async def connect(
    keyspace: Optional[str] = None,
    timeout: Optional[float] = None
) -> AsyncCassandraSession
```

Connect to the cluster and create a session.

**Parameters:**
- `keyspace`: Optional keyspace to use
- `timeout`: Connection timeout in seconds

**Example:**
```python
# Recommended: Let driver negotiate to highest available
cluster = AsyncCluster(['localhost'])  # Negotiates to v5 (highest currently supported)
session = await cluster.connect('my_keyspace')  # Fails if < v5

# Explicit protocol version (must be 5+)
cluster = AsyncCluster(['localhost'], protocol_version=5)
session = await cluster.connect('my_keyspace')

# Connection to old Cassandra will fail after negotiation
try:
    cluster = AsyncCluster(['cassandra-3.x-server'])
    session = await cluster.connect()  # Negotiates v4, then fails
except ConnectionError as e:
    print(e)  # "Connected with protocol v4 but v5+ is required..."
```

#### `shutdown`

```python
async def shutdown() -> None
```

Shutdown the cluster and release all resources.

#### `register_user_type`

```python
def register_user_type(
    keyspace: str,
    user_type: str,
    cls: Type
) -> None
```

Register a user-defined type with the cluster.

**Parameters:**
- `keyspace`: Keyspace containing the user type
- `user_type`: Name of the user type in Cassandra
- `cls`: Python class to map the type to

### Properties

- `is_closed`: Check if cluster is closed
- `metadata`: Get cluster metadata

### Context Manager

```python
async with AsyncCluster(['localhost']) as cluster:
    session = await cluster.connect()
    # Use session
```

## AsyncCassandraSession

Provides async interface for executing CQL queries.

### Constructor

The session is created by calling `cluster.connect()` and accepts an optional metrics middleware:

```python
# Created internally by cluster.connect()
AsyncCassandraSession(
    session: Session,
    metrics: Optional[MetricsMiddleware] = None
)
```

### Methods

#### `execute`

```python
async def execute(
    query: Union[str, SimpleStatement, PreparedStatement],
    parameters: Optional[Union[List, Dict]] = None,
    trace: bool = False,
    custom_payload: Optional[Dict[str, bytes]] = None,
    timeout: Any = None,
    execution_profile: Any = EXEC_PROFILE_DEFAULT,
    paging_state: Optional[bytes] = None,
    host: Optional[Any] = None,
    execute_as: Optional[str] = None
) -> AsyncResultSet
```

Execute a CQL query asynchronously.

**Example:**
```python
# Simple query
result = await session.execute("SELECT * FROM users")

# Query with parameters (must prepare first)
prepared = await session.prepare("SELECT * FROM users WHERE id = ?")
result = await session.execute(prepared, [user_id])

# Query with named parameters (must prepare first)
prepared = await session.prepare("SELECT * FROM users WHERE name = :name")
result = await session.execute(prepared, {"name": "John"})
```

#### `execute_batch`

```python
async def execute_batch(
    batch_statement: BatchStatement,
    trace: bool = False,
    custom_payload: Optional[Dict[str, bytes]] = None,
    timeout: Any = None,
    execution_profile: Any = EXEC_PROFILE_DEFAULT
) -> AsyncResultSet
```

Execute a batch statement asynchronously.

**Example:**
```python
from cassandra.query import BatchStatement

batch = BatchStatement()
batch.add("INSERT INTO users (id, name) VALUES (?, ?)", [id1, "Alice"])
batch.add("INSERT INTO users (id, name) VALUES (?, ?)", [id2, "Bob"])

await session.execute_batch(batch)
```

#### `execute_stream`

```python
async def execute_stream(
    query: Union[str, SimpleStatement, PreparedStatement, BoundStatement],
    parameters: Optional[Union[list, tuple, dict]] = None,
    stream_config: Optional[StreamConfig] = None,
    **kwargs
) -> AsyncStreamingResultSet
```

Execute a query and return results as an async stream for memory-efficient processing of large result sets.

**⚠️ CRITICAL**: Streaming result sets MUST be properly closed to prevent memory leaks. The streaming implementation uses callbacks that create circular references. Always use a context manager or ensure proper cleanup.

**Parameters:**
- `query`: The CQL query to execute
- `parameters`: Query parameters (for prepared statements)
- `stream_config`: Configuration for streaming (fetch size, max pages, etc.)
- `**kwargs`: Additional keyword arguments passed to execute

**Returns:** `AsyncStreamingResultSet` - An async iterator over the results

**Example:**
```python
from async_cassandra.streaming import StreamConfig

# ✅ BEST PRACTICE: Always use context manager
config = StreamConfig(fetch_size=1000)
async with await session.execute_stream(
    "SELECT * FROM large_table",
    stream_config=config
) as result:
    # Process rows one at a time without loading all into memory
    async for row in result:
        await process_row(row)
# Result automatically closed, preventing memory leaks

# ✅ Alternative: Manual cleanup with try/finally
result = await session.execute_stream("SELECT * FROM large_table")
try:
    async for row in result:
        await process_row(row)
finally:
    await result.close()  # CRITICAL: Must close!

# ❌ NEVER DO THIS - Memory leak!
result = await session.execute_stream("SELECT * FROM large_table")
async for row in result:
    process_row(row)
# Result not closed - callbacks remain in memory forever!
```

**Processing by Pages:**
```python
# Context manager works with pages() too
async with await session.execute_stream(
    "SELECT * FROM large_table",
    stream_config=StreamConfig(fetch_size=5000)
) as result:
    async for page in result.pages():
        await process_page(page)  # Process 5000 rows at a time
```

#### `prepare`

```python
async def prepare(
    query: str,
    custom_payload: Optional[Dict[str, bytes]] = None,
    timeout: Optional[float] = None
) -> PreparedStatement
```

Prepare a CQL statement asynchronously.

**Parameters:**
- `query`: The CQL query to prepare
- `custom_payload`: Optional custom payload
- `timeout`: Optional timeout in seconds

**Example:**
```python
prepared = await session.prepare(
    "INSERT INTO users (id, name, email) VALUES (?, ?, ?)"
)

# Use prepared statement multiple times
await session.execute(prepared, [id1, "Alice", "alice@example.com"])
await session.execute(prepared, [id2, "Bob", "bob@example.com"])
```

#### `set_keyspace`

```python
async def set_keyspace(keyspace: str) -> None
```

Set the current keyspace.

#### `close`

```python
async def close() -> None
```

Close the session and release resources.

### Properties

- `is_closed`: Check if session is closed
- `keyspace`: Get current keyspace

### Context Manager

```python
async with await cluster.connect() as session:
    result = await session.execute("SELECT * FROM users")
```

## AsyncResultSet

Represents the result of a query execution.

### Methods

#### `one`

```python
def one() -> Optional[Any]
```

Get the first row or None if empty.

**Example:**
```python
# Must prepare the statement first
stmt = await session.prepare("SELECT * FROM users WHERE id = ?")
result = await session.execute(stmt, [user_id])
user = result.one()
if user:
    print(f"Found user: {user.name}")  # Access as attribute, not dict
```

#### `all`

```python
def all() -> List[Any]
```

Get all rows as a list.

**Example:**
```python
result = await session.execute("SELECT * FROM users")
users = result.all()
for user in users:
    print(user['name'])
```

### Properties

- `rows`: Get all rows as a list

### Async Iteration

```python
result = await session.execute("SELECT * FROM users")
async for row in result:
    print(row['name'])
```

### Length

```python
result = await session.execute("SELECT * FROM users")
print(f"Found {len(result)} users")
```

## AsyncRetryPolicy

Retry policy for async operations with idempotency safety checks.

### Constructor

```python
AsyncRetryPolicy(max_retries: int = 3)
```

### Methods

#### `on_read_timeout`
```python
def on_read_timeout(
    query, consistency, required_responses,
    received_responses, data_retrieved, retry_num
) -> Tuple[int, Optional[ConsistencyLevel]]
```

Handle read timeout with retry logic.

#### `on_write_timeout`
```python
def on_write_timeout(
    query, consistency, write_type,
    required_responses, received_responses, retry_num
) -> Tuple[int, Optional[ConsistencyLevel]]
```

Handle write timeout with idempotency checks.

#### `on_unavailable`
```python
def on_unavailable(
    query, consistency, required_replicas,
    alive_replicas, retry_num
) -> Tuple[int, Optional[ConsistencyLevel]]
```

Handle unavailable exception.

#### `on_request_error`
```python
def on_request_error(
    query, consistency, error, retry_num
) -> Tuple[int, Optional[ConsistencyLevel]]
```

Handle request errors.

### Retry Behavior

- **Read Timeout**: Retries if data was retrieved or enough responses received
- **Write Timeout**: Retries for SIMPLE and BATCH writes only if marked as idempotent
- **Unavailable**: Tries next host on first attempt, then retries
- **Request Error**: Always tries next host

### Idempotency Safety

The retry policy includes critical safety checks for write operations:

```python
# Safe to retry - marked as idempotent
stmt = SimpleStatement(
    "INSERT INTO users (id, name) VALUES (?, ?) IF NOT EXISTS",
    is_idempotent=True
)

# NOT safe to retry - will not be retried
stmt = SimpleStatement(
    "INSERT INTO users (id, name) VALUES (?, ?)"
    # is_idempotent defaults to None - treated as non-idempotent
)

# Prepared statements also need explicit marking
prepared = await session.prepare(
    "DELETE FROM users WHERE id = ?"
)
prepared.is_idempotent = True  # Mark as safe to retry

# Batch statements can be marked idempotent if all operations are safe
batch = BatchStatement()
batch.is_idempotent = True  # Only if all statements in batch are idempotent
```

**Important**: Write operations (INSERT, UPDATE, DELETE) are ONLY retried if the statement is explicitly marked with `is_idempotent=True`. Statements without this attribute or with `is_idempotent=False/None` will NOT be retried. This strict policy prevents:
- Duplicate data insertions
- Multiple increments/decrements
- Unintended side effects from retrying non-idempotent operations

Note: By default, Cassandra driver statements have `is_idempotent=None`, which is treated as non-idempotent for safety.

## Exceptions

### AsyncCassandraError

Base exception for all async-cassandra errors.

```python
class AsyncCassandraError(Exception):
    cause: Optional[Exception]  # Original exception if any
```

### ConnectionError

Raised when connection to Cassandra fails.

```python
try:
    session = await cluster.connect()
except ConnectionError as e:
    print(f"Failed to connect: {e}")
```

### QueryError

Raised when a non-Cassandra exception occurs during query execution. Most Cassandra driver exceptions (like `InvalidRequest`, `Unauthorized`, `AlreadyExists`, etc.) are passed through directly without wrapping.

```python
# Cassandra exceptions pass through directly
from cassandra import InvalidRequest, Unauthorized

try:
    result = await session.execute("SELECT * FROM invalid_table")
except InvalidRequest as e:
    print(f"Invalid query: {e}")  # Cassandra exception passed through
except QueryError as e:
    print(f"Unexpected error: {e}")  # Only non-Cassandra exceptions wrapped
    if e.cause:
        print(f"Caused by: {e.cause}")
```

### Cassandra Driver Exceptions

The following Cassandra driver exceptions are passed through directly without wrapping:
- `InvalidRequest` - Invalid query syntax or schema issues
- `Unauthorized` - Permission/authorization failures
- `AuthenticationFailed` - Authentication failures
- `AlreadyExists` - Schema already exists errors
- `NoHostAvailable` - No Cassandra hosts available
- `Unavailable`, `ReadTimeout`, `WriteTimeout` - Consistency/timeout errors
- `OperationTimedOut` - Query timeout
- Protocol exceptions like `SyntaxException`, `ServerError`

### Other Exceptions

The library defines `ConnectionError` for connection-related issues and `QueryError` for wrapping unexpected non-Cassandra exceptions. Most of the time, you should catch specific Cassandra exceptions for proper error handling.

## Complete Example

```python
import asyncio
import uuid
from async_cassandra import AsyncCluster, AsyncCassandraSession
from async_cassandra.exceptions import ConnectionError
from cassandra import InvalidRequest, AlreadyExists

async def main():
    # Create cluster with authentication
    cluster = AsyncCluster.create_with_auth(
        contact_points=['localhost'],
        username='cassandra',
        password='cassandra'
    )

    try:
        # Connect to cluster
        session = await cluster.connect()

        # Create keyspace
        await session.execute("""
            CREATE KEYSPACE IF NOT EXISTS example
            WITH REPLICATION = {
                'class': 'SimpleStrategy',
                'replication_factor': 1
            }
        """)

        # Use keyspace
        await session.set_keyspace('example')

        # Create table
        await session.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id UUID PRIMARY KEY,
                name TEXT,
                email TEXT
            )
        """)

        # Prepare statement
        insert_stmt = await session.prepare(
            "INSERT INTO users (id, name, email) VALUES (?, ?, ?)"
        )

        # Insert data
        user_id = uuid.uuid4()
        await session.execute(
            insert_stmt,
            [user_id, "John Doe", "john@example.com"]
        )

        # Query data (prepare the statement first)
        select_stmt = await session.prepare("SELECT * FROM users WHERE id = ?")
        result = await session.execute(select_stmt, [user_id])

        user = result.one()
        print(f"User: {user['name']} ({user['email']})")

    except ConnectionError as e:
        print(f"Connection failed: {e}")
    except InvalidRequest as e:
        print(f"Invalid query: {e}")
    except AlreadyExists as e:
        print(f"Schema already exists: {e.keyspace}.{e.table}")
    finally:
        await cluster.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
```

## Additional Components

For documentation on monitoring, metrics, and streaming components, see:

- [Monitoring and Metrics API Reference](api-monitoring.md) - ConnectionMonitor, MetricsMiddleware, streaming classes
- [Streaming Guide](streaming.md) - Detailed streaming usage and examples
- [Metrics and Monitoring Guide](metrics-monitoring.md) - Setting up monitoring
