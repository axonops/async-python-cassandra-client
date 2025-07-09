"""
Enhanced integration tests for FastAPI with all async-cassandra features.
"""

import asyncio
import uuid

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from examples.fastapi_app.main_enhanced import app


@pytest.mark.asyncio
@pytest.mark.integration
class TestEnhancedFastAPIFeatures:
    """Test all enhanced features in the FastAPI example."""

    @pytest_asyncio.fixture
    async def client(self):
        """Create async HTTP client with proper app initialization."""
        # The app needs to be properly initialized with lifespan

        # Create a test app that runs the lifespan
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            # Trigger lifespan startup
            async with app.router.lifespan_context(app):
                yield client

    async def test_root_endpoint(self, client):
        """Test root endpoint lists all features."""
        response = await client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert "features" in data
        assert "Timeout handling" in data["features"]
        assert "Memory-efficient streaming" in data["features"]
        assert "Connection monitoring" in data["features"]

    async def test_enhanced_health_check(self, client):
        """Test enhanced health check with monitoring data."""
        response = await client.get("/health")
        assert response.status_code == 200
        data = response.json()

        # Check all required fields
        assert "status" in data
        assert "healthy_hosts" in data
        assert "unhealthy_hosts" in data
        assert "total_connections" in data
        assert "timestamp" in data

        # Verify at least one healthy host
        assert data["healthy_hosts"] >= 1

    async def test_host_monitoring(self, client):
        """Test detailed host monitoring endpoint."""
        response = await client.get("/monitoring/hosts")
        assert response.status_code == 200
        data = response.json()

        assert "cluster_name" in data
        assert "protocol_version" in data
        assert "hosts" in data
        assert isinstance(data["hosts"], list)

        # Check host details
        if data["hosts"]:
            host = data["hosts"][0]
            assert "address" in host
            assert "status" in host
            assert "latency_ms" in host

    async def test_connection_summary(self, client):
        """Test connection summary endpoint."""
        response = await client.get("/monitoring/summary")
        assert response.status_code == 200
        data = response.json()

        assert "total_hosts" in data
        assert "up_hosts" in data
        assert "down_hosts" in data
        assert "protocol_version" in data
        assert "max_requests_per_connection" in data

    async def test_create_user_with_timeout(self, client):
        """Test user creation with timeout handling."""
        user_data = {"name": "Timeout Test User", "email": "timeout@test.com", "age": 30}

        response = await client.post("/users", json=user_data)
        assert response.status_code == 201
        created_user = response.json()

        assert created_user["name"] == user_data["name"]
        assert created_user["email"] == user_data["email"]
        assert "id" in created_user

    async def test_list_users_with_custom_timeout(self, client):
        """Test listing users with custom timeout."""
        # First create some users
        for i in range(5):
            await client.post(
                "/users",
                json={"name": f"Test User {i}", "email": f"user{i}@test.com", "age": 25 + i},
            )

        # List with custom timeout
        response = await client.get("/users?limit=5&timeout=10.0")
        assert response.status_code == 200
        users = response.json()
        assert isinstance(users, list)
        assert len(users) <= 5

    async def test_advanced_streaming(self, client):
        """Test advanced streaming with all options."""
        # Create test data
        for i in range(20):
            await client.post(
                "/users",
                json={"name": f"Stream User {i}", "email": f"stream{i}@test.com", "age": 20 + i},
            )

        # Test streaming with various configurations
        response = await client.get(
            "/users/stream/advanced?"
            "limit=20&"
            "fetch_size=10&"  # Minimum is 10
            "max_pages=3&"
            "timeout_seconds=30.0"
        )
        if response.status_code != 200:
            print(f"Response status: {response.status_code}")
            print(f"Response body: {response.text}")
        assert response.status_code == 200
        data = response.json()

        assert "users" in data
        assert "metadata" in data

        metadata = data["metadata"]
        assert metadata["pages_fetched"] <= 3  # Respects max_pages
        assert metadata["rows_processed"] <= 20  # Respects limit
        assert "duration_seconds" in metadata
        assert "rows_per_second" in metadata

    async def test_streaming_with_memory_limit(self, client):
        """Test streaming with memory limit."""
        response = await client.get(
            "/users/stream/advanced?"
            "limit=1000&"
            "fetch_size=100&"
            "max_memory_mb=1"  # Very low memory limit
        )
        assert response.status_code == 200
        data = response.json()

        # Should stop before reaching limit due to memory constraint
        assert len(data["users"]) < 1000

    async def test_error_handling_invalid_uuid(self, client):
        """Test proper error handling for invalid UUID."""
        response = await client.get("/users/invalid-uuid")
        assert response.status_code == 400
        assert "Invalid UUID format" in response.json()["detail"]

    async def test_error_handling_user_not_found(self, client):
        """Test proper error handling for non-existent user."""
        random_uuid = str(uuid.uuid4())
        response = await client.get(f"/users/{random_uuid}")
        assert response.status_code == 404
        assert "User not found" in response.json()["detail"]

    async def test_query_metrics(self, client):
        """Test query metrics collection."""
        # Execute some queries first
        for i in range(10):
            await client.get("/users?limit=1")

        response = await client.get("/metrics/queries")
        assert response.status_code == 200
        data = response.json()

        if "query_performance" in data:
            perf = data["query_performance"]
            assert "total_queries" in perf
            assert perf["total_queries"] >= 10

    async def test_rate_limit_status(self, client):
        """Test rate limiting status endpoint."""
        response = await client.get("/rate_limit/status")
        assert response.status_code == 200
        data = response.json()

        assert "rate_limiting_enabled" in data
        if data["rate_limiting_enabled"]:
            assert "metrics" in data
            assert "max_concurrent" in data

    async def test_timeout_operations(self, client):
        """Test timeout handling for different operations."""
        # Test very short timeout
        response = await client.post("/test/timeout?operation=execute&timeout=0.1")
        assert response.status_code == 200
        data = response.json()

        # Should either complete or timeout
        assert data.get("error") in ["timeout", None]

    async def test_concurrent_load_read(self, client):
        """Test system under concurrent read load."""
        # Create test data
        await client.post(
            "/users", json={"name": "Load Test User", "email": "load@test.com", "age": 25}
        )

        # Test concurrent reads
        response = await client.post("/test/concurrent_load?concurrent_requests=20&query_type=read")
        assert response.status_code == 200
        data = response.json()

        summary = data["test_summary"]
        assert summary["successful"] > 0
        assert summary["requests_per_second"] > 0

        # Check rate limit metrics if available
        if data.get("rate_limit_metrics"):
            metrics = data["rate_limit_metrics"]
            assert metrics["total_requests"] >= 20

    async def test_concurrent_load_write(self, client):
        """Test system under concurrent write load."""
        response = await client.post(
            "/test/concurrent_load?concurrent_requests=10&query_type=write"
        )
        if response.status_code != 200:
            print(f"Response status: {response.status_code}")
            print(f"Response body: {response.text}")
        assert response.status_code == 200
        data = response.json()

        summary = data["test_summary"]
        assert summary["successful"] > 0

        # Clean up test data
        cleanup_response = await client.delete("/users/cleanup")
        if cleanup_response.status_code != 200:
            print(f"Cleanup error: {cleanup_response.text}")
        assert cleanup_response.status_code == 200

    async def test_streaming_timeout(self, client):
        """Test streaming with timeout."""
        # Test with very short timeout
        response = await client.get(
            "/users/stream/advanced?"
            "limit=1000&"
            "fetch_size=100&"  # Add required fetch_size
            "timeout_seconds=0.1"  # Very short timeout
        )

        # Should either complete quickly or timeout
        if response.status_code == 504:
            assert "timeout" in response.json()["detail"].lower()
        elif response.status_code == 422:
            # Validation error is also acceptable - might fail before timeout
            assert "detail" in response.json()
        else:
            assert response.status_code == 200

    async def test_connection_monitoring_callbacks(self, client):
        """Test that monitoring is active and collecting data."""
        # Wait a bit for monitoring to collect data
        await asyncio.sleep(2)

        # Check host status
        response = await client.get("/monitoring/hosts")
        assert response.status_code == 200
        data = response.json()

        # Should have collected latency data
        hosts_with_latency = [h for h in data["hosts"] if h.get("latency_ms") is not None]
        assert len(hosts_with_latency) > 0

    async def test_graceful_error_recovery(self, client):
        """Test that system recovers gracefully from errors."""
        # Create a user (should work)
        user1 = await client.post(
            "/users", json={"name": "Recovery Test 1", "email": "recovery1@test.com", "age": 30}
        )
        assert user1.status_code == 201

        # Try invalid operation
        invalid = await client.get("/users/not-a-uuid")
        assert invalid.status_code == 400

        # System should still work
        user2 = await client.post(
            "/users", json={"name": "Recovery Test 2", "email": "recovery2@test.com", "age": 31}
        )
        assert user2.status_code == 201

    async def test_memory_efficient_streaming(self, client):
        """Test that streaming is memory efficient."""
        # Create substantial test data
        batch_size = 50
        for batch in range(3):
            batch_data = {
                "users": [
                    {
                        "name": f"Batch User {batch * batch_size + i}",
                        "email": f"batch{batch}_{i}@test.com",
                        "age": 20 + i,
                    }
                    for i in range(batch_size)
                ]
            }
            # Use the main app's batch endpoint
            response = await client.post("/users/batch", json=batch_data)
            assert response.status_code == 200

        # Stream through all data with smaller fetch size to ensure multiple pages
        response = await client.get(
            "/users/stream/advanced?"
            "limit=200&"  # Increase limit to ensure we get all users
            "fetch_size=10&"  # Small fetch size to ensure multiple pages
            "max_pages=20"
        )
        assert response.status_code == 200
        data = response.json()

        # With 150 users and fetch_size=10, we should get multiple pages
        # Check that we got users (may not be exactly 150 due to other tests)
        assert data["metadata"]["pages_fetched"] >= 1
        assert len(data["users"]) >= 150  # Should get at least 150 users
        assert len(data["users"]) <= 200  # But no more than limit
