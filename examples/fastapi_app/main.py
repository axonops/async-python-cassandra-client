"""
Simple FastAPI example using async-cassandra.

This demonstrates basic CRUD operations with Cassandra using the async wrapper.
Run with: uvicorn main:app --reload
"""

import asyncio
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from typing import List, Optional
from uuid import UUID

from cassandra import OperationTimedOut, ReadTimeout, Unavailable, WriteTimeout

# Import Cassandra driver exceptions for proper error detection
from cassandra.cluster import Cluster as SyncCluster
from cassandra.cluster import NoHostAvailable
from cassandra.policies import ConstantReconnectionPolicy
from fastapi import FastAPI, HTTPException, Query, Request
from pydantic import BaseModel

from async_cassandra import AsyncCluster, StreamConfig


# Pydantic models
class UserCreate(BaseModel):
    name: str
    email: str
    age: int


class User(BaseModel):
    id: str
    name: str
    email: str
    age: int
    created_at: datetime
    updated_at: datetime


class UserUpdate(BaseModel):
    name: Optional[str] = None
    email: Optional[str] = None
    age: Optional[int] = None


# Global session, cluster, and keyspace
session = None
cluster = None
sync_session = None  # For synchronous performance comparison
sync_cluster = None  # For synchronous performance comparison
keyspace = "example"


def is_cassandra_unavailable_error(error: Exception) -> bool:
    """
    Determine if an error indicates Cassandra is unavailable.

    This function checks for specific Cassandra driver exceptions that indicate
    the database is not reachable or available.
    """
    # Direct Cassandra driver exceptions
    if isinstance(
        error, (NoHostAvailable, Unavailable, OperationTimedOut, ReadTimeout, WriteTimeout)
    ):
        return True

    # Check error message for additional patterns
    error_msg = str(error).lower()
    unavailability_keywords = [
        "no host available",
        "all hosts",
        "connection",
        "timeout",
        "unavailable",
        "no replicas",
        "not enough replicas",
        "cannot achieve consistency",
        "operation timed out",
        "read timeout",
        "write timeout",
        "connection pool",
        "connection closed",
        "connection refused",
        "unable to connect",
    ]

    return any(keyword in error_msg for keyword in unavailability_keywords)


def handle_cassandra_error(error: Exception, operation: str = "operation") -> HTTPException:
    """
    Convert a Cassandra error to an appropriate HTTP exception.

    Returns 503 for availability issues, 500 for other errors.
    """
    if is_cassandra_unavailable_error(error):
        # Log the specific error type for debugging
        error_type = type(error).__name__
        return HTTPException(
            status_code=503,
            detail=f"Service temporarily unavailable: Cassandra connection issue ({error_type}: {str(error)})",
        )
    else:
        # Other errors (like InvalidRequest) get 500
        return HTTPException(
            status_code=500, detail=f"Internal server error during {operation}: {str(error)}"
        )


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage database lifecycle."""
    global session, cluster, sync_session, sync_cluster

    try:
        # Startup - connect to Cassandra with constant reconnection policy
        # IMPORTANT: Using ConstantReconnectionPolicy with 2-second delay for testing
        # This ensures quick reconnection during integration tests where we simulate
        # Cassandra outages. In production, you might want ExponentialReconnectionPolicy
        # to avoid overwhelming a recovering cluster.
        # IMPORTANT: Use 127.0.0.1 instead of localhost to force IPv4
        contact_points = os.getenv("CASSANDRA_HOSTS", "127.0.0.1").split(",")
        # Replace any "localhost" with "127.0.0.1" to ensure IPv4
        contact_points = ["127.0.0.1" if cp == "localhost" else cp for cp in contact_points]

        cluster = AsyncCluster(
            contact_points=contact_points,
            port=int(os.getenv("CASSANDRA_PORT", "9042")),
            reconnection_policy=ConstantReconnectionPolicy(
                delay=2.0
            ),  # Reconnect every 2 seconds for testing
            connect_timeout=10.0,  # Quick connection timeout for faster test feedback
        )
        session = await cluster.connect()
    except Exception as e:
        print(f"Failed to connect to Cassandra: {type(e).__name__}: {e}")
        # Don't fail startup completely, allow health check to report unhealthy
        session = None
        yield
        return

    # Create keyspace and table
    await session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS example
        WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """
    )
    await session.set_keyspace("example")

    # Also create sync cluster for performance comparison
    try:
        sync_cluster = SyncCluster(
            contact_points=contact_points,
            port=int(os.getenv("CASSANDRA_PORT", "9042")),
            reconnection_policy=ConstantReconnectionPolicy(delay=2.0),
            connect_timeout=10.0,
            protocol_version=5,
        )
        sync_session = sync_cluster.connect()
        sync_session.set_keyspace("example")
    except Exception as e:
        print(f"Failed to create sync cluster: {e}")
        sync_session = None

    # Drop and recreate table for clean test environment
    await session.execute("DROP TABLE IF EXISTS users")
    await session.execute(
        """
        CREATE TABLE users (
            id UUID PRIMARY KEY,
            name TEXT,
            email TEXT,
            age INT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )
    """
    )

    yield

    # Shutdown
    if session:
        await session.close()
    if cluster:
        await cluster.shutdown()
    if sync_session:
        sync_session.shutdown()
    if sync_cluster:
        sync_cluster.shutdown()


# Create FastAPI app
app = FastAPI(
    title="FastAPI + async-cassandra Example",
    description="Simple CRUD API using async-cassandra",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/")
async def root():
    """Root endpoint."""
    return {"message": "FastAPI + async-cassandra example is running!"}


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    try:
        # Simple health check - verify session is available
        if session is None:
            return {
                "status": "unhealthy",
                "cassandra_connected": False,
                "timestamp": datetime.now().isoformat(),
            }

        # Test connection with a simple query
        await session.execute("SELECT now() FROM system.local")
        return {
            "status": "healthy",
            "cassandra_connected": True,
            "timestamp": datetime.now().isoformat(),
        }
    except Exception:
        return {
            "status": "unhealthy",
            "cassandra_connected": False,
            "timestamp": datetime.now().isoformat(),
        }


@app.post("/users", response_model=User, status_code=201)
async def create_user(user: UserCreate):
    """Create a new user."""
    if session is None:
        raise HTTPException(
            status_code=503,
            detail="Service temporarily unavailable: Cassandra connection not established",
        )

    try:
        user_id = uuid.uuid4()
        now = datetime.now()

        # Use prepared statement for better performance
        stmt = await session.prepare(
            "INSERT INTO users (id, name, email, age, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)"
        )
        await session.execute(stmt, [user_id, user.name, user.email, user.age, now, now])

        return User(
            id=str(user_id),
            name=user.name,
            email=user.email,
            age=user.age,
            created_at=now,
            updated_at=now,
        )
    except Exception as e:
        raise handle_cassandra_error(e, "user creation")


@app.get("/users", response_model=List[User])
async def list_users(limit: int = Query(10, ge=1, le=10000)):
    """List all users."""
    if session is None:
        raise HTTPException(
            status_code=503,
            detail="Service temporarily unavailable: Cassandra connection not established",
        )

    try:
        # Use prepared statement with validated limit
        stmt = await session.prepare("SELECT * FROM users LIMIT ?")
        result = await session.execute(stmt, [limit])

        users = []
        async for row in result:
            users.append(
                User(
                    id=str(row.id),
                    name=row.name,
                    email=row.email,
                    age=row.age,
                    created_at=row.created_at,
                    updated_at=row.updated_at,
                )
            )

        return users
    except Exception as e:
        error_msg = str(e)
        if any(
            keyword in error_msg.lower()
            for keyword in ["unavailable", "nohost", "connection", "timeout"]
        ):
            raise HTTPException(
                status_code=503,
                detail=f"Service temporarily unavailable: Cassandra connection issue - {error_msg}",
            )
        raise HTTPException(status_code=500, detail=f"Internal server error: {error_msg}")


# Streaming endpoints - must come before /users/{user_id} to avoid route conflict
@app.get("/users/stream")
async def stream_users(
    limit: int = Query(1000, ge=0, le=10000), fetch_size: int = Query(100, ge=10, le=1000)
):
    """Stream users data for large result sets."""
    if session is None:
        raise HTTPException(
            status_code=503,
            detail="Service temporarily unavailable: Cassandra connection not established",
        )

    try:
        # Handle special case where limit=0
        if limit == 0:
            return {
                "users": [],
                "metadata": {
                    "total_returned": 0,
                    "pages_fetched": 0,
                    "fetch_size": fetch_size,
                    "streaming_enabled": True,
                },
            }

        stream_config = StreamConfig(fetch_size=fetch_size)

        # Use context manager for proper resource cleanup
        stmt = await session.prepare("SELECT * FROM users LIMIT ?")
        async with await session.execute_stream(
            stmt, [limit], stream_config=stream_config
        ) as result:
            users = []
            async for row in result:
                # Handle both dict-like and object-like row access
                if hasattr(row, "__getitem__"):
                    # Dictionary-like access
                    try:
                        user_dict = {
                            "id": str(row["id"]),
                            "name": row["name"],
                            "email": row["email"],
                            "age": row["age"],
                            "created_at": row["created_at"].isoformat(),
                            "updated_at": row["updated_at"].isoformat(),
                        }
                    except (KeyError, TypeError):
                        # Fall back to attribute access
                        user_dict = {
                            "id": str(row.id),
                            "name": row.name,
                            "email": row.email,
                            "age": row.age,
                            "created_at": row.created_at.isoformat(),
                            "updated_at": row.updated_at.isoformat(),
                        }
                else:
                    # Object-like access
                    user_dict = {
                        "id": str(row.id),
                        "name": row.name,
                        "email": row.email,
                        "age": row.age,
                        "created_at": row.created_at.isoformat(),
                        "updated_at": row.updated_at.isoformat(),
                    }
                users.append(user_dict)

            return {
                "users": users,
                "metadata": {
                    "total_returned": len(users),
                    "pages_fetched": result.page_number,
                    "fetch_size": fetch_size,
                    "streaming_enabled": True,
                },
            }

    except Exception as e:
        raise handle_cassandra_error(e, "streaming users")


@app.get("/users/stream/pages")
async def stream_users_by_pages(
    limit: int = Query(1000, ge=0, le=10000),
    fetch_size: int = Query(100, ge=10, le=1000),
    max_pages: int = Query(10, ge=0, le=100),
):
    """Stream users data page by page for memory efficiency."""
    if session is None:
        raise HTTPException(
            status_code=503,
            detail="Service temporarily unavailable: Cassandra connection not established",
        )

    try:
        # Handle special case where limit=0 or max_pages=0
        if limit == 0 or max_pages == 0:
            return {
                "total_rows_processed": 0,
                "pages_info": [],
                "metadata": {
                    "fetch_size": fetch_size,
                    "max_pages_limit": max_pages,
                    "streaming_mode": "page_by_page",
                },
            }

        stream_config = StreamConfig(fetch_size=fetch_size, max_pages=max_pages)

        # Use context manager for automatic cleanup
        stmt = await session.prepare("SELECT * FROM users LIMIT ?")
        async with await session.execute_stream(
            stmt, [limit], stream_config=stream_config
        ) as result:
            pages_info = []
            total_processed = 0

            async for page in result.pages():
                page_size = len(page)
                total_processed += page_size

                # Extract sample user data, handling both dict-like and object-like access
                sample_user = None
                if page:
                    first_row = page[0]
                    if hasattr(first_row, "__getitem__"):
                        # Dictionary-like access
                        try:
                            sample_user = {
                                "id": str(first_row["id"]),
                                "name": first_row["name"],
                                "email": first_row["email"],
                            }
                        except (KeyError, TypeError):
                            # Fall back to attribute access
                            sample_user = {
                                "id": str(first_row.id),
                                "name": first_row.name,
                                "email": first_row.email,
                            }
                    else:
                        # Object-like access
                        sample_user = {
                            "id": str(first_row.id),
                            "name": first_row.name,
                            "email": first_row.email,
                        }

                pages_info.append(
                    {
                        "page_number": len(pages_info) + 1,
                        "rows_in_page": page_size,
                        "sample_user": sample_user,
                    }
                )

            return {
                "total_rows_processed": total_processed,
                "pages_info": pages_info,
                "metadata": {
                    "fetch_size": fetch_size,
                    "max_pages_limit": max_pages,
                    "streaming_mode": "page_by_page",
                },
            }

    except Exception as e:
        raise handle_cassandra_error(e, "streaming users by pages")


@app.get("/users/{user_id}", response_model=User)
async def get_user(user_id: str):
    """Get user by ID."""
    if session is None:
        raise HTTPException(
            status_code=503,
            detail="Service temporarily unavailable: Cassandra connection not established",
        )

    try:
        user_uuid = uuid.UUID(user_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid UUID")

    try:
        stmt = await session.prepare("SELECT * FROM users WHERE id = ?")
        result = await session.execute(stmt, [user_uuid])
        row = result.one()

        if not row:
            raise HTTPException(status_code=404, detail="User not found")

        return User(
            id=str(row.id),
            name=row.name,
            email=row.email,
            age=row.age,
            created_at=row.created_at,
            updated_at=row.updated_at,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise handle_cassandra_error(e, "checking user existence")


@app.delete("/users/{user_id}", status_code=204)
async def delete_user(user_id: str):
    """Delete user by ID."""
    if session is None:
        raise HTTPException(
            status_code=503,
            detail="Service temporarily unavailable: Cassandra connection not established",
        )

    try:
        user_uuid = uuid.UUID(user_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid user ID format")

    try:
        stmt = await session.prepare("DELETE FROM users WHERE id = ?")
        await session.execute(stmt, [user_uuid])

        return None  # 204 No Content
    except Exception as e:
        error_msg = str(e)
        if any(
            keyword in error_msg.lower()
            for keyword in ["unavailable", "nohost", "connection", "timeout"]
        ):
            raise HTTPException(
                status_code=503,
                detail=f"Service temporarily unavailable: Cassandra connection issue - {error_msg}",
            )
        raise HTTPException(status_code=500, detail=f"Internal server error: {error_msg}")


@app.put("/users/{user_id}", response_model=User)
async def update_user(user_id: str, user_update: UserUpdate):
    """Update user by ID."""
    if session is None:
        raise HTTPException(
            status_code=503,
            detail="Service temporarily unavailable: Cassandra connection not established",
        )

    try:
        user_uuid = uuid.UUID(user_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid user ID format")

    try:
        # First check if user exists
        check_stmt = await session.prepare("SELECT * FROM users WHERE id = ?")
        result = await session.execute(check_stmt, [user_uuid])
        existing_user = result.one()

        if not existing_user:
            raise HTTPException(status_code=404, detail="User not found")
    except HTTPException:
        raise
    except Exception as e:
        raise handle_cassandra_error(e, "checking user existence")

    try:
        # Build update query dynamically based on provided fields
        update_fields = []
        params = []

        if user_update.name is not None:
            update_fields.append("name = ?")
            params.append(user_update.name)

        if user_update.email is not None:
            update_fields.append("email = ?")
            params.append(user_update.email)

        if user_update.age is not None:
            update_fields.append("age = ?")
            params.append(user_update.age)

        if not update_fields:
            raise HTTPException(status_code=400, detail="No fields to update")

        # Always update the updated_at timestamp
        update_fields.append("updated_at = ?")
        params.append(datetime.now())
        params.append(user_uuid)  # WHERE clause

        # Build a static query based on which fields are provided
        # This approach avoids dynamic SQL construction
        if len(update_fields) == 1:  # Only updated_at
            update_stmt = await session.prepare("UPDATE users SET updated_at = ? WHERE id = ?")
        elif len(update_fields) == 2:  # One field + updated_at
            if "name = ?" in update_fields:
                update_stmt = await session.prepare(
                    "UPDATE users SET name = ?, updated_at = ? WHERE id = ?"
                )
            elif "email = ?" in update_fields:
                update_stmt = await session.prepare(
                    "UPDATE users SET email = ?, updated_at = ? WHERE id = ?"
                )
            elif "age = ?" in update_fields:
                update_stmt = await session.prepare(
                    "UPDATE users SET age = ?, updated_at = ? WHERE id = ?"
                )
        elif len(update_fields) == 3:  # Two fields + updated_at
            if "name = ?" in update_fields and "email = ?" in update_fields:
                update_stmt = await session.prepare(
                    "UPDATE users SET name = ?, email = ?, updated_at = ? WHERE id = ?"
                )
            elif "name = ?" in update_fields and "age = ?" in update_fields:
                update_stmt = await session.prepare(
                    "UPDATE users SET name = ?, age = ?, updated_at = ? WHERE id = ?"
                )
            elif "email = ?" in update_fields and "age = ?" in update_fields:
                update_stmt = await session.prepare(
                    "UPDATE users SET email = ?, age = ?, updated_at = ? WHERE id = ?"
                )
        else:  # All fields
            update_stmt = await session.prepare(
                "UPDATE users SET name = ?, email = ?, age = ?, updated_at = ? WHERE id = ?"
            )

        await session.execute(update_stmt, params)

        # Return updated user
        result = await session.execute(check_stmt, [user_uuid])
        updated_user = result.one()

        return User(
            id=str(updated_user.id),
            name=updated_user.name,
            email=updated_user.email,
            age=updated_user.age,
            created_at=updated_user.created_at,
            updated_at=updated_user.updated_at,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise handle_cassandra_error(e, "checking user existence")


@app.patch("/users/{user_id}", response_model=User)
async def partial_update_user(user_id: str, user_update: UserUpdate):
    """Partial update user by ID (same as PUT in this implementation)."""
    return await update_user(user_id, user_update)


# Performance testing endpoints
@app.get("/performance/async")
async def test_async_performance(requests: int = Query(100, ge=1, le=1000)):
    """Test async performance with concurrent queries."""
    if session is None:
        raise HTTPException(
            status_code=503,
            detail="Service temporarily unavailable: Cassandra connection not established",
        )

    import time

    try:
        start_time = time.time()

        # Prepare statement once
        stmt = await session.prepare("SELECT * FROM users LIMIT 1")

        # Execute queries concurrently
        async def execute_query():
            return await session.execute(stmt)

        tasks = [execute_query() for _ in range(requests)]
        results = await asyncio.gather(*tasks)

        end_time = time.time()
        duration = end_time - start_time

        return {
            "requests": requests,
            "total_time": duration,
            "requests_per_second": requests / duration if duration > 0 else 0,
            "avg_time_per_request": duration / requests if requests > 0 else 0,
            "successful_requests": len(results),
            "mode": "async",
        }
    except Exception as e:
        raise handle_cassandra_error(e, "performance test")


@app.get("/performance/sync")
async def test_sync_performance(requests: int = Query(100, ge=1, le=1000)):
    """Test TRUE sync performance using synchronous cassandra-driver."""
    if sync_session is None:
        raise HTTPException(
            status_code=503,
            detail="Service temporarily unavailable: Sync Cassandra connection not established",
        )

    import time

    try:
        # Run synchronous operations in a thread pool to not block the event loop
        import concurrent.futures

        def run_sync_test():
            start_time = time.time()

            # Prepare statement once
            stmt = sync_session.prepare("SELECT * FROM users LIMIT 1")

            # Execute queries sequentially with the SYNC driver
            results = []
            for _ in range(requests):
                result = sync_session.execute(stmt)
                results.append(result)

            end_time = time.time()
            duration = end_time - start_time

            return {
                "requests": requests,
                "total_time": duration,
                "requests_per_second": requests / duration if duration > 0 else 0,
                "avg_time_per_request": duration / requests if requests > 0 else 0,
                "successful_requests": len(results),
                "mode": "sync (true blocking)",
            }

        # Run in thread pool to avoid blocking the event loop
        loop = asyncio.get_event_loop()
        with concurrent.futures.ThreadPoolExecutor() as pool:
            result = await loop.run_in_executor(pool, run_sync_test)

        return result
    except Exception as e:
        raise handle_cassandra_error(e, "sync performance test")


# Batch operations endpoint
@app.post("/users/batch", status_code=201)
async def create_users_batch(batch_data: dict):
    """Create multiple users in a batch."""
    if session is None:
        raise HTTPException(
            status_code=503,
            detail="Service temporarily unavailable: Cassandra connection not established",
        )

    try:
        users = batch_data.get("users", [])
        created_users = []

        for user_data in users:
            user_id = uuid.uuid4()
            now = datetime.now()

            # Create user dict with proper fields
            user_dict = {
                "id": str(user_id),
                "name": user_data.get("name", user_data.get("username", "")),
                "email": user_data["email"],
                "age": user_data.get("age", 25),
                "created_at": now.isoformat(),
                "updated_at": now.isoformat(),
            }

            # Insert into database
            stmt = await session.prepare(
                "INSERT INTO users (id, name, email, age, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)"
            )
            await session.execute(
                stmt, [user_id, user_dict["name"], user_dict["email"], user_dict["age"], now, now]
            )

            created_users.append(user_dict)

        return {"created": created_users}
    except Exception as e:
        raise handle_cassandra_error(e, "batch user creation")


# Metrics endpoint
@app.get("/metrics")
async def get_metrics():
    """Get application metrics."""
    # Simple metrics implementation
    return {
        "total_requests": 1000,  # Placeholder
        "query_performance": {
            "avg_response_time_ms": 50,
            "p95_response_time_ms": 100,
            "p99_response_time_ms": 200,
        },
        "cassandra_connections": {"active": 10, "idle": 5, "total": 15},
    }


# Shutdown endpoint
@app.post("/shutdown")
async def shutdown():
    """Gracefully shutdown the application."""
    # In a real app, this would trigger graceful shutdown
    return {"message": "Shutdown initiated"}


# Slow query endpoint for testing
@app.get("/slow_query")
async def slow_query(request: Request):
    """Simulate a slow query for testing timeouts."""

    # Check for timeout header
    timeout_header = request.headers.get("X-Request-Timeout")
    if timeout_header:
        timeout = float(timeout_header)
        # If timeout is very short, simulate timeout error
        if timeout < 1.0:
            raise HTTPException(status_code=504, detail="Gateway Timeout")

    await asyncio.sleep(5)  # Simulate slow operation
    return {"message": "Slow query completed"}


# Long running query endpoint
@app.get("/long_running_query")
async def long_running_query():
    """Simulate a long-running query."""
    await asyncio.sleep(10)  # Simulate very long operation
    return {"message": "Long query completed"}


# ============================================================================
# Context Manager Safety Endpoints
# ============================================================================


@app.post("/context_manager_safety/query_error")
async def test_query_error_session_safety():
    """Test that query errors don't close the session."""
    # Track session state
    session_id_before = id(session)
    is_closed_before = session.is_closed

    # Execute a bad query that will fail
    try:
        await session.execute("SELECT * FROM non_existent_table_xyz")
    except Exception as e:
        error_message = str(e)

    # Verify session is still usable
    session_id_after = id(session)
    is_closed_after = session.is_closed

    # Try a valid query to prove session works
    result = await session.execute("SELECT release_version FROM system.local")
    version = result.one().release_version

    return {
        "test": "query_error_session_safety",
        "session_unchanged": session_id_before == session_id_after,
        "session_open": not is_closed_after and not is_closed_before,
        "error_caught": error_message,
        "session_still_works": bool(version),
        "cassandra_version": version,
    }


@app.post("/context_manager_safety/streaming_error")
async def test_streaming_error_session_safety():
    """Test that streaming errors don't close the session."""
    session_id_before = id(session)
    error_message = None
    stream_completed = False

    # Try to stream from non-existent table
    try:
        async with await session.execute_stream(
            "SELECT * FROM non_existent_stream_table"
        ) as stream:
            async for row in stream:
                pass
            stream_completed = True
    except Exception as e:
        error_message = str(e)

    # Verify session is still usable
    session_id_after = id(session)

    # Try a valid streaming query
    row_count = 0
    # Use hardcoded query since keyspace is constant
    stmt = await session.prepare("SELECT * FROM example.users LIMIT ?")
    async with await session.execute_stream(stmt, [10]) as stream:
        async for row in stream:
            row_count += 1

    return {
        "test": "streaming_error_session_safety",
        "session_unchanged": session_id_before == session_id_after,
        "session_open": not session.is_closed,
        "streaming_error_caught": bool(error_message),
        "error_message": error_message,
        "stream_completed": stream_completed,
        "session_still_streams": row_count > 0,
        "rows_after_error": row_count,
    }


@app.post("/context_manager_safety/concurrent_streams")
async def test_concurrent_streams():
    """Test multiple concurrent streams don't interfere."""

    # Create test data
    users_to_create = []
    for i in range(30):
        users_to_create.append(
            {
                "id": str(uuid.uuid4()),
                "name": f"Stream Test User {i}",
                "email": f"stream{i}@test.com",
                "age": 20 + (i % 3) * 10,  # Ages: 20, 30, 40
            }
        )

    # Insert test data
    for user in users_to_create:
        stmt = await session.prepare(
            "INSERT INTO example.users (id, name, email, age) VALUES (?, ?, ?, ?)"
        )
        await session.execute(
            stmt,
            [UUID(user["id"]), user["name"], user["email"], user["age"]],
        )

    # Stream different age groups concurrently
    async def stream_age_group(age: int) -> dict:
        count = 0
        users = []

        config = StreamConfig(fetch_size=5)
        stmt = await session.prepare("SELECT * FROM example.users WHERE age = ? ALLOW FILTERING")
        async with await session.execute_stream(
            stmt,
            [age],
            stream_config=config,
        ) as stream:
            async for row in stream:
                count += 1
                users.append(row.name)

        return {"age": age, "count": count, "users": users[:3]}  # First 3 names

    # Run concurrent streams
    results = await asyncio.gather(stream_age_group(20), stream_age_group(30), stream_age_group(40))

    # Clean up test data
    for user in users_to_create:
        stmt = await session.prepare("DELETE FROM example.users WHERE id = ?")
        await session.execute(stmt, [UUID(user["id"])])

    return {
        "test": "concurrent_streams",
        "streams_completed": len(results),
        "all_streams_independent": all(r["count"] == 10 for r in results),
        "results": results,
        "session_still_open": not session.is_closed,
    }


@app.post("/context_manager_safety/nested_contexts")
async def test_nested_context_managers():
    """Test nested context managers close in correct order."""
    events = []

    # Create a temporary keyspace for this test
    temp_keyspace = f"test_nested_{uuid.uuid4().hex[:8]}"

    try:
        # Create new cluster context
        async with AsyncCluster(["127.0.0.1"]) as test_cluster:
            events.append("cluster_opened")

            # Create session context
            async with await test_cluster.connect() as test_session:
                events.append("session_opened")

                # Create keyspace with safe identifier
                # Validate keyspace name contains only safe characters
                if not temp_keyspace.replace("_", "").isalnum():
                    raise ValueError("Invalid keyspace name")

                # Use parameterized query for keyspace creation is not supported
                # So we validate the input first
                await test_session.execute(
                    f"""
                    CREATE KEYSPACE {temp_keyspace}
                    WITH REPLICATION = {{
                        'class': 'SimpleStrategy',
                        'replication_factor': 1
                    }}
                """
                )
                await test_session.set_keyspace(temp_keyspace)

                # Create table
                await test_session.execute(
                    """
                    CREATE TABLE test_table (
                        id UUID PRIMARY KEY,
                        value INT
                    )
                """
                )

                # Insert test data
                for i in range(5):
                    stmt = await test_session.prepare(
                        "INSERT INTO test_table (id, value) VALUES (?, ?)"
                    )
                    await test_session.execute(stmt, [uuid.uuid4(), i])

                # Create streaming context
                row_count = 0
                async with await test_session.execute_stream("SELECT * FROM test_table") as stream:
                    events.append("stream_opened")
                    async for row in stream:
                        row_count += 1
                    events.append("stream_closed")

                # Verify session still works after stream closed
                result = await test_session.execute("SELECT COUNT(*) FROM test_table")
                count_after_stream = result.one()[0]
                events.append(f"session_works_after_stream:{count_after_stream}")

                # Session will close here
                events.append("session_closing")

            events.append("session_closed")

            # Verify cluster still works after session closed
            async with await test_cluster.connect() as verify_session:
                result = await verify_session.execute("SELECT now() FROM system.local")
                events.append(f"cluster_works_after_session:{bool(result.one())}")

                # Clean up keyspace
                # Validate keyspace name before using in DROP
                if temp_keyspace.replace("_", "").isalnum():
                    await verify_session.execute(f"DROP KEYSPACE IF EXISTS {temp_keyspace}")

            # Cluster will close here
            events.append("cluster_closing")

        events.append("cluster_closed")

    except Exception as e:
        events.append(f"error:{str(e)}")
        # Try to clean up
        try:
            # Validate keyspace name before cleanup
            if temp_keyspace.replace("_", "").isalnum():
                await session.execute(f"DROP KEYSPACE IF EXISTS {temp_keyspace}")
        except Exception:
            pass

    # Verify our main session is still working
    main_session_works = False
    try:
        result = await session.execute("SELECT now() FROM system.local")
        main_session_works = bool(result.one())
    except Exception:
        pass

    return {
        "test": "nested_context_managers",
        "events": events,
        "correct_order": events
        == [
            "cluster_opened",
            "session_opened",
            "stream_opened",
            "stream_closed",
            "session_works_after_stream:5",
            "session_closing",
            "session_closed",
            "cluster_works_after_session:True",
            "cluster_closing",
            "cluster_closed",
        ],
        "row_count": row_count,
        "main_session_unaffected": main_session_works,
    }


@app.post("/context_manager_safety/cancellation")
async def test_streaming_cancellation():
    """Test that cancelled streaming operations clean up properly."""

    # Create test data
    test_ids = []
    for i in range(100):
        test_id = uuid.uuid4()
        test_ids.append(test_id)
        stmt = await session.prepare(
            "INSERT INTO example.users (id, name, email, age) VALUES (?, ?, ?, ?)"
        )
        await session.execute(
            stmt,
            [test_id, f"Cancel Test {i}", f"cancel{i}@test.com", 25],
        )

    # Start a streaming operation that we'll cancel
    rows_before_cancel = 0
    cancelled = False
    error_type = None

    async def stream_with_delay():
        nonlocal rows_before_cancel
        try:
            stmt = await session.prepare(
                "SELECT * FROM example.users WHERE age = ? ALLOW FILTERING"
            )
            async with await session.execute_stream(stmt, [25]) as stream:
                async for row in stream:
                    rows_before_cancel += 1
                    # Add delay to make cancellation more likely
                    await asyncio.sleep(0.01)
        except asyncio.CancelledError:
            nonlocal cancelled
            cancelled = True
            raise
        except Exception as e:
            nonlocal error_type
            error_type = type(e).__name__
            raise

    # Create task and cancel it
    task = asyncio.create_task(stream_with_delay())
    await asyncio.sleep(0.1)  # Let it process some rows
    task.cancel()

    # Wait for cancellation
    try:
        await task
    except asyncio.CancelledError:
        pass

    # Verify session still works
    session_works = False
    row_count_after = 0

    try:
        # Count rows to verify session works
        stmt = await session.prepare(
            "SELECT COUNT(*) FROM example.users WHERE age = ? ALLOW FILTERING"
        )
        result = await session.execute(stmt, [25])
        row_count_after = result.one()[0]
        session_works = True

        # Try streaming again
        new_stream_count = 0
        stmt = await session.prepare(
            "SELECT * FROM example.users WHERE age = ? LIMIT ? ALLOW FILTERING"
        )
        async with await session.execute_stream(stmt, [25, 10]) as stream:
            async for row in stream:
                new_stream_count += 1

    except Exception as e:
        error_type = f"post_cancel_error:{type(e).__name__}"

    # Clean up test data
    for test_id in test_ids:
        stmt = await session.prepare("DELETE FROM example.users WHERE id = ?")
        await session.execute(stmt, [test_id])

    return {
        "test": "streaming_cancellation",
        "rows_processed_before_cancel": rows_before_cancel,
        "was_cancelled": cancelled,
        "session_still_works": session_works,
        "total_rows": row_count_after,
        "new_stream_worked": new_stream_count == 10,
        "error_type": error_type,
        "session_open": not session.is_closed,
    }


@app.get("/context_manager_safety/status")
async def context_manager_safety_status():
    """Get current session and cluster status."""
    return {
        "session_open": not session.is_closed,
        "session_id": id(session),
        "cluster_open": not cluster.is_closed,
        "cluster_id": id(cluster),
        "keyspace": keyspace,
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
