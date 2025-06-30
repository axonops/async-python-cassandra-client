"""
Enhanced FastAPI example demonstrating all async-cassandra features.

This comprehensive example demonstrates:
- Timeout handling
- Streaming with memory management
- Connection monitoring
- Rate limiting
- Error handling
- Metrics collection

Run with: uvicorn main_enhanced:app --reload
"""

import asyncio
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from typing import List, Optional

from fastapi import BackgroundTasks, FastAPI, HTTPException, Query
from pydantic import BaseModel

from async_cassandra import AsyncCluster, StreamConfig
from async_cassandra.constants import MAX_CONCURRENT_QUERIES
from async_cassandra.metrics import create_metrics_system
from async_cassandra.monitoring import RateLimitedSession, create_monitored_session


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


class ConnectionHealth(BaseModel):
    status: str
    healthy_hosts: int
    unhealthy_hosts: int
    total_connections: int
    avg_latency_ms: Optional[float]
    timestamp: datetime


class UserBatch(BaseModel):
    users: List[UserCreate]


# Global resources
session = None
monitor = None
metrics = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle with enhanced features."""
    global session, monitor, metrics

    # Create metrics system
    metrics = create_metrics_system(backend="memory", prometheus_enabled=False)

    # Create monitored session with rate limiting
    contact_points = os.getenv("CASSANDRA_HOSTS", "localhost").split(",")
    # port = int(os.getenv("CASSANDRA_PORT", "9042"))  # Not used in create_monitored_session

    # Use create_monitored_session for automatic monitoring setup
    session, monitor = await create_monitored_session(
        contact_points=contact_points,
        max_concurrent=MAX_CONCURRENT_QUERIES,  # Rate limiting
        warmup=True,  # Pre-establish connections
    )

    # Add metrics to session
    session.session._metrics = metrics  # For rate limited session

    # Set up keyspace and tables
    await session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS example
        WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
        """
    )
    await session.session.set_keyspace("example")

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

    # Start continuous monitoring
    asyncio.create_task(monitor.start_monitoring(interval=30))

    yield

    # Graceful shutdown
    await monitor.stop_monitoring()
    await session.session.close()


# Create FastAPI app
app = FastAPI(
    title="Enhanced FastAPI + async-cassandra",
    description="Comprehensive example with all features",
    version="2.0.0",
    lifespan=lifespan,
)


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "message": "Enhanced FastAPI + async-cassandra example",
        "features": [
            "Timeout handling",
            "Memory-efficient streaming",
            "Connection monitoring",
            "Rate limiting",
            "Metrics collection",
            "Error handling",
        ],
    }


@app.get("/health", response_model=ConnectionHealth)
async def health_check():
    """Enhanced health check with connection monitoring."""
    try:
        # Get cluster metrics
        cluster_metrics = await monitor.get_cluster_metrics()

        # Calculate average latency
        latencies = [h.latency_ms for h in cluster_metrics.hosts if h.latency_ms]
        avg_latency = sum(latencies) / len(latencies) if latencies else None

        return ConnectionHealth(
            status="healthy" if cluster_metrics.healthy_hosts > 0 else "unhealthy",
            healthy_hosts=cluster_metrics.healthy_hosts,
            unhealthy_hosts=cluster_metrics.unhealthy_hosts,
            total_connections=cluster_metrics.total_connections,
            avg_latency_ms=avg_latency,
            timestamp=cluster_metrics.timestamp,
        )
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Health check failed: {str(e)}")


@app.get("/monitoring/hosts")
async def get_host_status():
    """Get detailed host status from monitoring."""
    cluster_metrics = await monitor.get_cluster_metrics()

    return {
        "cluster_name": cluster_metrics.cluster_name,
        "protocol_version": cluster_metrics.protocol_version,
        "hosts": [
            {
                "address": host.address,
                "datacenter": host.datacenter,
                "rack": host.rack,
                "status": host.status,
                "latency_ms": host.latency_ms,
                "last_check": host.last_check.isoformat() if host.last_check else None,
                "error": host.last_error,
            }
            for host in cluster_metrics.hosts
        ],
    }


@app.get("/monitoring/summary")
async def get_connection_summary():
    """Get connection summary."""
    return monitor.get_connection_summary()


@app.post("/users", response_model=User, status_code=201)
async def create_user(user: UserCreate, background_tasks: BackgroundTasks):
    """Create a new user with timeout handling."""
    user_id = uuid.uuid4()
    now = datetime.now()

    try:
        # Prepare with timeout
        stmt = await session.session.prepare(
            "INSERT INTO users (id, name, email, age, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
            timeout=10.0,  # 10 second timeout for prepare
        )

        # Execute with timeout (using statement's default timeout)
        await session.execute(stmt, [user_id, user.name, user.email, user.age, now, now])

        # Background task to update metrics
        background_tasks.add_task(update_user_count)

        return User(
            id=str(user_id),
            name=user.name,
            email=user.email,
            age=user.age,
            created_at=now,
            updated_at=now,
        )
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Query timeout")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create user: {str(e)}")


async def update_user_count():
    """Background task to update user count."""
    try:
        result = await session.execute("SELECT COUNT(*) FROM users")
        count = result.one()[0]
        # In a real app, this would update a cache or metrics
        print(f"Total users: {count}")
    except Exception:
        pass  # Don't fail background tasks


@app.get("/users", response_model=List[User])
async def list_users(
    limit: int = Query(10, ge=1, le=100),
    timeout: float = Query(30.0, ge=1.0, le=60.0),
):
    """List users with configurable timeout."""
    try:
        # Execute with custom timeout using prepared statement
        stmt = await session.session.prepare("SELECT * FROM users LIMIT ?")
        result = await session.execute(
            stmt,
            [limit],
            timeout=timeout,
        )

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
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail=f"Query timeout after {timeout}s")


@app.get("/users/stream/advanced")
async def stream_users_advanced(
    limit: int = Query(1000, ge=0, le=100000),
    fetch_size: int = Query(100, ge=10, le=5000),
    max_pages: Optional[int] = Query(None, ge=1, le=1000),
    timeout_seconds: Optional[float] = Query(None, ge=1.0, le=300.0),
):
    """Advanced streaming with all configuration options."""
    try:
        # Create stream config with all options
        stream_config = StreamConfig(
            fetch_size=fetch_size,
            max_pages=max_pages,
            timeout_seconds=timeout_seconds,
        )

        # Track streaming progress
        progress = {
            "pages_fetched": 0,
            "rows_processed": 0,
            "start_time": datetime.now(),
        }

        def page_callback(page_number: int, page_size: int):
            progress["pages_fetched"] = page_number
            progress["rows_processed"] += page_size

        stream_config.page_callback = page_callback

        # Execute streaming query with prepared statement
        stmt = await session.session.prepare("SELECT * FROM users LIMIT ?")
        result = await session.session.execute_stream(
            stmt,
            [limit],
            stream_config=stream_config,
        )

        users = []

        # Use context manager for proper cleanup
        async with result as stream:
            async for row in stream:
                users.append(
                    {
                        "id": str(row.id),
                        "name": row.name,
                        "email": row.email,
                    }
                )

                # Check if we've reached the limit
                if limit and len(users) >= limit:
                    break

        end_time = datetime.now()
        duration = (end_time - progress["start_time"]).total_seconds()

        return {
            "users": users,
            "metadata": {
                "total_returned": len(users),
                "pages_fetched": progress["pages_fetched"],
                "rows_processed": progress["rows_processed"],
                "duration_seconds": duration,
                "rows_per_second": progress["rows_processed"] / duration if duration > 0 else 0,
                "config": {
                    "fetch_size": fetch_size,
                    "max_pages": max_pages,
                    "timeout_seconds": timeout_seconds,
                },
            },
        }
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Streaming timeout")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Streaming failed: {str(e)}")


@app.get("/users/{user_id}", response_model=User)
async def get_user(user_id: str):
    """Get user by ID with proper error handling."""
    try:
        user_uuid = uuid.UUID(user_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid UUID format")

    try:
        stmt = await session.session.prepare("SELECT * FROM users WHERE id = ?")
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
        # Check for NoHostAvailable
        if "NoHostAvailable" in str(type(e)):
            raise HTTPException(status_code=503, detail="No Cassandra hosts available")
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")


@app.get("/metrics/queries")
async def get_query_metrics():
    """Get query performance metrics."""
    if not metrics or not hasattr(metrics, "collectors"):
        return {"error": "Metrics not available"}

    # Get stats from in-memory collector
    for collector in metrics.collectors:
        if hasattr(collector, "get_stats"):
            stats = await collector.get_stats()
            return stats

    return {"error": "No stats available"}


@app.get("/rate_limit/status")
async def get_rate_limit_status():
    """Get rate limiting status."""
    if isinstance(session, RateLimitedSession):
        return {
            "rate_limiting_enabled": True,
            "metrics": session.get_metrics(),
            "max_concurrent": session.semaphore._value,
        }
    return {"rate_limiting_enabled": False}


@app.post("/test/timeout")
async def test_timeout_handling(
    operation: str = Query("connect", pattern="^(connect|prepare|execute)$"),
    timeout: float = Query(5.0, ge=0.1, le=30.0),
):
    """Test timeout handling for different operations."""
    try:
        if operation == "connect":
            # Test connection timeout
            cluster = AsyncCluster(["nonexistent.host"])
            await cluster.connect(timeout=timeout)

        elif operation == "prepare":
            # Test prepare timeout (simulate with sleep)
            await asyncio.wait_for(asyncio.sleep(timeout + 1), timeout=timeout)

        elif operation == "execute":
            # Test execute timeout
            await session.execute("SELECT * FROM users", timeout=timeout)

        return {"message": f"{operation} completed within {timeout}s"}

    except asyncio.TimeoutError:
        return {
            "error": "timeout",
            "operation": operation,
            "timeout_seconds": timeout,
            "message": f"{operation} timed out after {timeout}s",
        }
    except Exception as e:
        return {
            "error": "exception",
            "operation": operation,
            "message": str(e),
        }


@app.post("/test/concurrent_load")
async def test_concurrent_load(
    concurrent_requests: int = Query(50, ge=1, le=500),
    query_type: str = Query("read", pattern="^(read|write)$"),
):
    """Test system under concurrent load."""
    start_time = datetime.now()

    async def execute_query(i: int):
        try:
            if query_type == "read":
                await session.execute("SELECT * FROM users LIMIT 1")
                return {"success": True, "index": i}
            else:
                user_id = uuid.uuid4()
                stmt = await session.session.prepare(
                    "INSERT INTO users (id, name, email, age, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)"
                )
                await session.execute(
                    stmt,
                    [
                        user_id,
                        f"LoadTest{i}",
                        f"load{i}@test.com",
                        25,
                        datetime.now(),
                        datetime.now(),
                    ],
                )
                return {"success": True, "index": i, "user_id": str(user_id)}
        except Exception as e:
            return {"success": False, "index": i, "error": str(e)}

    # Execute queries concurrently
    tasks = [execute_query(i) for i in range(concurrent_requests)]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Analyze results
    successful = sum(1 for r in results if isinstance(r, dict) and r.get("success"))
    failed = len(results) - successful

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    # Get rate limit metrics if available
    rate_limit_metrics = {}
    if isinstance(session, RateLimitedSession):
        rate_limit_metrics = session.get_metrics()

    return {
        "test_summary": {
            "concurrent_requests": concurrent_requests,
            "query_type": query_type,
            "successful": successful,
            "failed": failed,
            "duration_seconds": duration,
            "requests_per_second": concurrent_requests / duration if duration > 0 else 0,
        },
        "rate_limit_metrics": rate_limit_metrics,
        "timestamp": datetime.now().isoformat(),
    }


@app.post("/users/batch")
async def create_users_batch(batch: UserBatch):
    """Create multiple users in a batch operation."""
    try:
        # Prepare the insert statement
        stmt = await session.session.prepare(
            "INSERT INTO users (id, name, email, age, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)"
        )

        created_users = []
        now = datetime.now()

        # Execute batch inserts
        for user_data in batch.users:
            user_id = uuid.uuid4()
            await session.execute(
                stmt, [user_id, user_data.name, user_data.email, user_data.age, now, now]
            )
            created_users.append(
                {
                    "id": str(user_id),
                    "name": user_data.name,
                    "email": user_data.email,
                    "age": user_data.age,
                    "created_at": now.isoformat(),
                    "updated_at": now.isoformat(),
                }
            )

        return {"created": len(created_users), "users": created_users}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Batch creation failed: {str(e)}")


@app.delete("/users/cleanup")
async def cleanup_test_users():
    """Clean up test users created during load testing."""
    try:
        # Delete all users with LoadTest prefix
        # Note: LIKE is not supported in Cassandra, we need to fetch all and filter
        result = await session.execute("SELECT id, name FROM users")

        deleted_count = 0
        async for row in result:
            if row.name and row.name.startswith("LoadTest"):
                # Use prepared statement for delete
                delete_stmt = await session.session.prepare("DELETE FROM users WHERE id = ?")
                await session.execute(delete_stmt, [row.id])
                deleted_count += 1

        return {"deleted": deleted_count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cleanup failed: {str(e)}")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
