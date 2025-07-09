# FastAPI Example Application

This example demonstrates how to use async-cassandra with FastAPI to build a high-performance REST API backed by Cassandra.

## 🎯 Purpose

**This example serves a dual purpose:**
1. **Production Template**: A real-world example of how to integrate async-cassandra with FastAPI
2. **CI Integration Test**: This application is used in our CI/CD pipeline to validate that async-cassandra works correctly in a real async web framework environment

## Overview

The example showcases all the key features of async-cassandra:
- **Thread Safety**: Handles concurrent requests without data corruption
- **Memory Efficiency**: Streaming endpoints for large datasets
- **Error Handling**: Consistent error responses across all operations
- **Performance**: Async operations preventing event loop blocking
- **Monitoring**: Health checks and metrics endpoints
- **Production Patterns**: Proper lifecycle management, prepared statements, and error handling

## What You'll Learn

This example teaches essential patterns for production Cassandra applications:

1. **Connection Management**: How to properly manage cluster and session lifecycle
2. **Prepared Statements**: Reusing prepared statements for performance and security
3. **Error Handling**: Converting Cassandra errors to appropriate HTTP responses
4. **Streaming**: Processing large datasets without memory exhaustion
5. **Concurrency**: Leveraging async for high-throughput operations
6. **Context Managers**: Ensuring resources are properly cleaned up
7. **Monitoring**: Building observable applications with health and metrics
8. **Testing**: Comprehensive test patterns for async applications

## API Endpoints

### 1. Basic CRUD Operations
- `POST /users` - Create a new user
  - **Purpose**: Demonstrates basic insert operations with prepared statements
  - **Validates**: UUID generation, timestamp handling, data validation
- `GET /users/{user_id}` - Get user by ID
  - **Purpose**: Shows single-row query patterns
  - **Validates**: UUID parsing, error handling for non-existent users
- `PUT /users/{user_id}` - Full update of user
  - **Purpose**: Demonstrates full record replacement
  - **Validates**: Update operations, timestamp updates
- `PATCH /users/{user_id}` - Partial update of user
  - **Purpose**: Shows selective field updates
  - **Validates**: Optional field handling, partial updates
- `DELETE /users/{user_id}` - Delete user
  - **Purpose**: Demonstrates delete operations
  - **Validates**: Idempotent deletes, cleanup
- `GET /users` - List users with pagination
  - **Purpose**: Shows basic pagination patterns
  - **Query params**: `limit` (default: 10, max: 100)

### 2. Streaming Operations
- `GET /users/stream` - Stream large datasets efficiently
  - **Purpose**: Demonstrates memory-efficient streaming for large result sets
  - **Query params**:
    - `limit`: Total rows to stream
    - `fetch_size`: Rows per page (controls memory usage)
    - `age_filter`: Filter users by minimum age
  - **Validates**: Memory efficiency, streaming context managers
- `GET /users/stream/pages` - Page-by-page streaming
  - **Purpose**: Shows manual page iteration for client-controlled paging
  - **Query params**: Same as above
  - **Validates**: Page-by-page processing, fetch more pages pattern

### 3. Batch Operations
- `POST /users/batch` - Create multiple users in a single batch
  - **Purpose**: Demonstrates batch insert performance benefits
  - **Validates**: Batch size limits, atomic batch operations

### 4. Performance Testing
- `GET /performance/async` - Test async performance with concurrent queries
  - **Purpose**: Demonstrates concurrent query execution benefits
  - **Query params**: `requests` (number of concurrent queries)
  - **Validates**: Thread pool handling, concurrent execution
- `GET /performance/sync` - Compare with sequential execution
  - **Purpose**: Shows performance difference vs sequential execution
  - **Query params**: `requests` (number of sequential queries)
  - **Validates**: Performance improvement metrics

### 5. Error Simulation & Resilience Testing
- `GET /slow_query` - Simulates slow query with timeout handling
  - **Purpose**: Tests timeout behavior and client timeout headers
  - **Headers**: `X-Request-Timeout` (timeout in seconds)
  - **Validates**: Timeout propagation, graceful timeout handling
- `GET /long_running_query` - Simulates very long operation (10s)
  - **Purpose**: Tests long-running query behavior
  - **Validates**: Long operation handling without blocking

### 6. Context Manager Safety Testing
These endpoints validate critical safety properties of context managers:

- `POST /context_manager_safety/query_error`
  - **Purpose**: Verifies query errors don't close the session
  - **Tests**: Executes invalid query, then valid query
  - **Validates**: Error isolation, session stability after errors

- `POST /context_manager_safety/streaming_error`
  - **Purpose**: Ensures streaming errors don't affect the session
  - **Tests**: Attempts invalid streaming, then valid streaming
  - **Validates**: Streaming context cleanup without session impact

- `POST /context_manager_safety/concurrent_streams`
  - **Purpose**: Tests multiple concurrent streams don't interfere
  - **Tests**: Runs 3 concurrent streams with different filters
  - **Validates**: Stream isolation, independent lifecycles

- `POST /context_manager_safety/nested_contexts`
  - **Purpose**: Verifies proper cleanup order in nested contexts
  - **Tests**: Creates cluster → session → stream nested contexts
  - **Validates**:
    - Innermost (stream) closes first
    - Middle (session) closes without affecting cluster
    - Outer (cluster) closes last
    - Main app session unaffected

- `POST /context_manager_safety/cancellation`
  - **Purpose**: Tests cancelled streaming operations clean up properly
  - **Tests**: Starts stream, cancels mid-flight, verifies cleanup
  - **Validates**:
    - No resource leaks on cancellation
    - Session remains usable
    - New streams can be started

- `GET /context_manager_safety/status`
  - **Purpose**: Monitor resource state
  - **Returns**: Current state of session, cluster, and keyspace
  - **Validates**: Resource tracking and monitoring

### 7. Monitoring & Operations
- `GET /` - Welcome message with API information
- `GET /health` - Health check with Cassandra connectivity test
  - **Purpose**: Load balancer health checks, monitoring
  - **Returns**: Status and Cassandra connectivity
- `GET /metrics` - Application metrics
  - **Purpose**: Performance monitoring, debugging
  - **Returns**: Query counts, error counts, performance stats
- `POST /shutdown` - Graceful shutdown simulation
  - **Purpose**: Tests graceful shutdown patterns
  - **Note**: In production, use process managers

## Running the Example

### Prerequisites

1. **Cassandra** running on localhost:9042 (or use Docker/Podman):
   ```bash
   # Using Docker
   docker run -d --name cassandra-test -p 9042:9042 cassandra:5

   # OR using Podman
   podman run -d --name cassandra-test -p 9042:9042 cassandra:5
   ```

2. **Python 3.12+** with dependencies:
   ```bash
   cd examples/fastapi_app
   pip install -r requirements.txt
   ```

### Start the Application

```bash
# Development mode with auto-reload
uvicorn main:app --reload

# Production mode
uvicorn main:app --host 0.0.0.0 --port 8000 --workers 1
```

**Note**: Use only 1 worker to ensure proper connection management. For scaling, run multiple instances behind a load balancer.

### Environment Variables

- `CASSANDRA_HOSTS` - Comma-separated list of Cassandra hosts (default: localhost)
- `CASSANDRA_PORT` - Cassandra port (default: 9042)
- `CASSANDRA_KEYSPACE` - Keyspace name (default: test_keyspace)

Example:
```bash
export CASSANDRA_HOSTS=node1,node2,node3
export CASSANDRA_PORT=9042
export CASSANDRA_KEYSPACE=production
```

## Testing the Application

### Automated Test Suite

The test suite validates all functionality and serves as integration tests in CI:

```bash
# Run all tests
pytest tests/test_fastapi_app.py -v

# Or run all tests in the tests directory
pytest tests/ -v
```

Tests cover:
- ✅ Thread safety under high concurrency
- ✅ Memory efficiency with streaming
- ✅ Error handling consistency
- ✅ Performance characteristics
- ✅ All endpoint functionality
- ✅ Timeout handling
- ✅ Connection lifecycle
- ✅ **Context manager safety**
  - Query error isolation
  - Streaming error containment
  - Concurrent stream independence
  - Nested context cleanup order
  - Cancellation handling

### Manual Testing Examples

#### Welcome and health check:
```bash
# Check if API is running
curl http://localhost:8000/
# Returns: {"message": "FastAPI + async-cassandra example is running!"}

# Detailed health check
curl http://localhost:8000/health
# Returns health status and Cassandra connectivity
```

#### Create a user:
```bash
curl -X POST http://localhost:8000/users \
  -H "Content-Type: application/json" \
  -d '{"name": "John Doe", "email": "john@example.com", "age": 30}'

# Response includes auto-generated UUID and timestamps:
# {
#   "id": "123e4567-e89b-12d3-a456-426614174000",
#   "name": "John Doe",
#   "email": "john@example.com",
#   "age": 30,
#   "created_at": "2024-01-01T12:00:00",
#   "updated_at": "2024-01-01T12:00:00"
# }
```

#### Get a user:
```bash
# Replace with actual UUID from create response
curl http://localhost:8000/users/550e8400-e29b-41d4-a716-446655440000

# Returns 404 if user not found with proper error message
```

#### Update operations:
```bash
# Full update (PUT) - all fields required
curl -X PUT http://localhost:8000/users/550e8400-e29b-41d4-a716-446655440000 \
  -H "Content-Type: application/json" \
  -d '{"name": "Jane Doe", "email": "jane@example.com", "age": 31}'

# Partial update (PATCH) - only specified fields updated
curl -X PATCH http://localhost:8000/users/550e8400-e29b-41d4-a716-446655440000 \
  -H "Content-Type: application/json" \
  -d '{"age": 32}'
```

#### Delete a user:
```bash
# Returns 204 No Content on success
curl -X DELETE http://localhost:8000/users/550e8400-e29b-41d4-a716-446655440000

# Idempotent - deleting non-existent user also returns 204
```

#### List users with pagination:
```bash
# Default limit is 10, max is 100
curl "http://localhost:8000/users?limit=10"

# Response includes list of users
```

#### Stream large dataset:
```bash
# Stream users with age > 25, 100 rows per page
curl "http://localhost:8000/users/stream?age_filter=25&fetch_size=100&limit=10000"

# Streams JSON array of users without loading all in memory
# fetch_size controls memory usage (rows per Cassandra page)
```

#### Page-by-page streaming:
```bash
# Get one page at a time with state tracking
curl "http://localhost:8000/users/stream/pages?age_filter=25&fetch_size=50"

# Returns:
# {
#   "users": [...],
#   "has_more": true,
#   "page_state": "encoded_state_for_next_page"
# }
```

#### Batch operations:
```bash
# Create multiple users atomically
curl -X POST http://localhost:8000/users/batch \
  -H "Content-Type: application/json" \
  -d '[
    {"name": "User 1", "email": "user1@example.com", "age": 25},
    {"name": "User 2", "email": "user2@example.com", "age": 30},
    {"name": "User 3", "email": "user3@example.com", "age": 35}
  ]'

# Returns count of created users
```

#### Test performance:
```bash
# Run 500 concurrent queries (async)
curl "http://localhost:8000/performance/async?requests=500"

# Compare with sequential execution
curl "http://localhost:8000/performance/sync?requests=500"

# Response shows timing and requests/second
```

#### Check health:
```bash
curl http://localhost:8000/health

# Returns:
# {
#   "status": "healthy",
#   "cassandra": "connected",
#   "keyspace": "example"
# }

# Returns 503 if Cassandra is not available
```

#### View metrics:
```bash
curl http://localhost:8000/metrics

# Returns application metrics:
# {
#   "total_queries": 1234,
#   "active_connections": 10,
#   "queries_per_second": 45.2,
#   "average_query_time_ms": 12.5,
#   "errors_count": 0
# }
```

#### Test error scenarios:
```bash
# Test timeout handling with short timeout
curl -H "X-Request-Timeout: 0.1" http://localhost:8000/slow_query
# Returns 504 Gateway Timeout

# Test with adequate timeout
curl -H "X-Request-Timeout: 10" http://localhost:8000/slow_query
# Returns success after 5 seconds
```

#### Test context manager safety:
```bash
# Test query error isolation
curl -X POST http://localhost:8000/context_manager_safety/query_error

# Test streaming error containment
curl -X POST http://localhost:8000/context_manager_safety/streaming_error

# Test concurrent streams
curl -X POST http://localhost:8000/context_manager_safety/concurrent_streams

# Test nested context managers
curl -X POST http://localhost:8000/context_manager_safety/nested_contexts

# Test cancellation handling
curl -X POST http://localhost:8000/context_manager_safety/cancellation

# Check resource status
curl http://localhost:8000/context_manager_safety/status
```

## Key Concepts Explained

For in-depth explanations of the core concepts used in this example:

- **[Why Async Matters for Cassandra](../../docs/why-async-wrapper.md)** - Understand the benefits of async operations for database drivers
- **[Streaming Large Datasets](../../docs/streaming.md)** - Learn about memory-efficient data processing
- **[Context Manager Safety](../../docs/context-managers-explained.md)** - Critical patterns for resource management
- **[Connection Pooling](../../docs/connection-pooling.md)** - How connections are managed efficiently

For prepared statements best practices, see the examples in the code above and the [main documentation](../../README.md#prepared-statements).

## Key Implementation Patterns

This example demonstrates several critical implementation patterns. For detailed documentation, see:

- **[Architecture Overview](../../docs/architecture.md)** - How async-cassandra works internally
- **[API Reference](../../docs/api.md)** - Complete API documentation
- **[Getting Started Guide](../../docs/getting-started.md)** - Basic usage patterns

Key patterns implemented in this example:

### Application Lifecycle Management
- FastAPI's lifespan context manager for proper setup/teardown
- Single cluster and session instance shared across the application
- Graceful shutdown handling

### Prepared Statements
- All parameterized queries use prepared statements
- Statements prepared once and reused for better performance
- Protection against CQL injection attacks

### Streaming for Large Results
- Memory-efficient processing using `execute_stream()`
- Configurable fetch size for memory control
- Automatic cleanup with context managers

### Error Handling
- Consistent error responses with proper HTTP status codes
- Cassandra exceptions mapped to appropriate HTTP errors
- Validation errors handled with 422 responses

### Context Manager Safety
- **[Context Manager Safety Documentation](../../docs/context-managers-explained.md)**

### Concurrent Request Handling
- Safe concurrent query execution using `asyncio.gather()`
- Thread pool executor manages concurrent operations
- No data corruption or connection issues under load

## Common Patterns and Best Practices

For comprehensive patterns and best practices when using async-cassandra:
- **[Getting Started Guide](../../docs/getting-started.md)** - Basic usage patterns
- **[Troubleshooting Guide](../../docs/troubleshooting.md)** - Common issues and solutions
- **[Streaming Documentation](../../docs/streaming.md)** - Memory-efficient data processing
- **[Performance Guide](../../docs/performance.md)** - Optimization strategies

The code in this example demonstrates these patterns in action. Key takeaways:
- Use a single global session shared across all requests
- Handle specific Cassandra errors and convert to appropriate HTTP responses
- Use streaming for large datasets to prevent memory exhaustion
- Always use context managers for proper resource cleanup

## Production Considerations

For detailed production deployment guidance, see:
- **[Connection Pooling](../../docs/connection-pooling.md)** - Connection management strategies
- **[Performance Guide](../../docs/performance.md)** - Optimization techniques
- **[Monitoring Guide](../../docs/metrics-monitoring.md)** - Metrics and observability
- **[Thread Pool Configuration](../../docs/thread-pool-configuration.md)** - Tuning for your workload

Key production patterns demonstrated in this example:
- Single global session shared across all requests
- Health check endpoints for load balancers
- Proper error handling and timeout management
- Input validation and security best practices

## CI/CD Integration

This example is automatically tested in our CI pipeline to ensure:
- async-cassandra integrates correctly with FastAPI
- All async operations work as expected
- No event loop blocking occurs
- Memory usage remains bounded with streaming
- Error handling works correctly

## Extending the Example

To add new features:

1. **New Endpoints**: Follow existing patterns for consistency
2. **Authentication**: Add FastAPI middleware for auth
3. **Rate Limiting**: Use FastAPI middleware or Redis
4. **Caching**: Add Redis for frequently accessed data
5. **API Versioning**: Use FastAPI's APIRouter for versioning

## Troubleshooting

For comprehensive troubleshooting guidance, see:
- **[Troubleshooting Guide](../../docs/troubleshooting.md)** - Common issues and solutions

Quick troubleshooting tips:
- **Connection issues**: Check Cassandra is running and environment variables are correct
- **Memory issues**: Use streaming endpoints and adjust `fetch_size`
- **Resource leaks**: Run `/context_manager_safety/*` endpoints to diagnose
- **Performance issues**: See the [Performance Guide](../../docs/performance.md)

## Complete Example Workflow

Here's a typical workflow demonstrating all key features:

```bash
# 1. Check system health
curl http://localhost:8000/health

# 2. Create some users
curl -X POST http://localhost:8000/users -H "Content-Type: application/json" \
  -d '{"name": "Alice", "email": "alice@example.com", "age": 28}'

curl -X POST http://localhost:8000/users -H "Content-Type: application/json" \
  -d '{"name": "Bob", "email": "bob@example.com", "age": 35}'

# 3. Create users in batch
curl -X POST http://localhost:8000/users/batch -H "Content-Type: application/json" \
  -d '[
    {"name": "Charlie", "email": "charlie@example.com", "age": 42},
    {"name": "Diana", "email": "diana@example.com", "age": 28},
    {"name": "Eve", "email": "eve@example.com", "age": 35}
  ]'

# 4. List all users
curl http://localhost:8000/users?limit=10

# 5. Stream users with age > 30
curl "http://localhost:8000/users/stream?age_filter=30&fetch_size=2"

# 6. Test performance
curl http://localhost:8000/performance/async?requests=100

# 7. Test context manager safety
curl -X POST http://localhost:8000/context_manager_safety/concurrent_streams

# 8. View metrics
curl http://localhost:8000/metrics

# 9. Clean up (delete a user)
curl -X DELETE http://localhost:8000/users/{user-id-from-create}
```

This example serves as both a learning resource and a production-ready template for building FastAPI applications with Cassandra using async-cassandra.
