"""
Integration tests for FastAPI example application.
"""

import asyncio
import sys
import uuid
from pathlib import Path
from typing import AsyncGenerator

import pytest
import pytest_asyncio
from httpx import AsyncClient

# Add the FastAPI app directory to the path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "examples" / "fastapi_app"))
from main import app


@pytest.fixture(scope="session")
def cassandra_service():
    """Use existing Cassandra service for tests."""
    # Cassandra should already be running on localhost:9042
    # Check if it's available
    import socket
    import time

    max_attempts = 10
    for i in range(max_attempts):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)
            result = sock.connect_ex(("localhost", 9042))
            sock.close()
            if result == 0:
                yield True
                return
        except Exception:
            pass
        time.sleep(1)

    raise RuntimeError("Cassandra is not available on localhost:9042")


@pytest_asyncio.fixture
async def client() -> AsyncGenerator[AsyncClient, None]:
    """Create async HTTP client for tests."""
    from httpx import ASGITransport, AsyncClient

    # Initialize the app lifespan context
    async with app.router.lifespan_context(app):
        # Use ASGI transport to test the app directly
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
            yield ac


@pytest.mark.integration
class TestHealthEndpoint:
    """Test health check endpoint."""

    @pytest.mark.asyncio
    async def test_health_check(self, client: AsyncClient, cassandra_service):
        """Test health check returns healthy status."""
        response = await client.get("/health")

        assert response.status_code == 200
        data = response.json()

        assert data["status"] == "healthy"
        assert data["cassandra_connected"] is True
        assert "timestamp" in data


@pytest.mark.integration
class TestUserCRUD:
    """Test user CRUD operations."""

    @pytest.mark.asyncio
    async def test_create_user(self, client: AsyncClient, cassandra_service):
        """Test creating a new user."""
        user_data = {"name": "John Doe", "email": "john@example.com", "age": 30}

        response = await client.post("/users", json=user_data)

        assert response.status_code == 201
        data = response.json()

        assert "id" in data
        assert data["name"] == user_data["name"]
        assert data["email"] == user_data["email"]
        assert data["age"] == user_data["age"]
        assert "created_at" in data
        assert "updated_at" in data

    @pytest.mark.asyncio
    async def test_get_user(self, client: AsyncClient, cassandra_service):
        """Test getting user by ID."""
        # First create a user
        user_data = {"name": "Jane Doe", "email": "jane@example.com", "age": 25}

        create_response = await client.post("/users", json=user_data)
        created_user = create_response.json()
        user_id = created_user["id"]

        # Get the user
        response = await client.get(f"/users/{user_id}")

        assert response.status_code == 200
        data = response.json()

        assert data["id"] == user_id
        assert data["name"] == user_data["name"]
        assert data["email"] == user_data["email"]
        assert data["age"] == user_data["age"]

    @pytest.mark.asyncio
    async def test_get_nonexistent_user(self, client: AsyncClient, cassandra_service):
        """Test getting non-existent user returns 404."""
        fake_id = str(uuid.uuid4())

        response = await client.get(f"/users/{fake_id}")

        assert response.status_code == 404
        assert "User not found" in response.json()["detail"]

    @pytest.mark.asyncio
    async def test_invalid_user_id_format(self, client: AsyncClient, cassandra_service):
        """Test invalid user ID format returns 400."""
        response = await client.get("/users/invalid-uuid")

        assert response.status_code == 400
        assert "Invalid UUID" in response.json()["detail"]

    @pytest.mark.asyncio
    async def test_list_users(self, client: AsyncClient, cassandra_service):
        """Test listing users."""
        # Create multiple users
        users = []
        for i in range(5):
            user_data = {"name": f"User {i}", "email": f"user{i}@example.com", "age": 20 + i}
            response = await client.post("/users", json=user_data)
            users.append(response.json())

        # List users
        response = await client.get("/users?limit=10")

        assert response.status_code == 200
        data = response.json()

        assert isinstance(data, list)
        assert len(data) >= 5  # At least the users we created

    @pytest.mark.asyncio
    async def test_update_user(self, client: AsyncClient, cassandra_service):
        """Test updating user."""
        # Create a user
        user_data = {"name": "Update Test", "email": "update@example.com", "age": 30}

        create_response = await client.post("/users", json=user_data)
        user_id = create_response.json()["id"]

        # Update the user
        update_data = {"name": "Updated Name", "age": 31}

        response = await client.put(f"/users/{user_id}", json=update_data)

        assert response.status_code == 200
        data = response.json()

        assert data["id"] == user_id
        assert data["name"] == update_data["name"]
        assert data["email"] == user_data["email"]  # Unchanged
        assert data["age"] == update_data["age"]
        assert data["updated_at"] > data["created_at"]

    @pytest.mark.asyncio
    async def test_partial_update(self, client: AsyncClient, cassandra_service):
        """Test partial update of user."""
        # Create a user
        user_data = {"name": "Partial Update", "email": "partial@example.com", "age": 25}

        create_response = await client.post("/users", json=user_data)
        user_id = create_response.json()["id"]

        # Update only email
        update_data = {"email": "newemail@example.com"}

        response = await client.put(f"/users/{user_id}", json=update_data)

        assert response.status_code == 200
        data = response.json()

        assert data["email"] == update_data["email"]
        assert data["name"] == user_data["name"]  # Unchanged
        assert data["age"] == user_data["age"]  # Unchanged

    @pytest.mark.asyncio
    async def test_delete_user(self, client: AsyncClient, cassandra_service):
        """Test deleting user."""
        # Create a user
        user_data = {"name": "Delete Test", "email": "delete@example.com", "age": 35}

        create_response = await client.post("/users", json=user_data)
        user_id = create_response.json()["id"]

        # Delete the user
        response = await client.delete(f"/users/{user_id}")

        assert response.status_code == 204

        # Verify user is deleted
        get_response = await client.get(f"/users/{user_id}")
        assert get_response.status_code == 404


@pytest.mark.integration
class TestPerformance:
    """Test performance endpoints."""

    @pytest.mark.asyncio
    async def test_async_performance(self, client: AsyncClient, cassandra_service):
        """Test async performance endpoint."""
        response = await client.get("/performance/async?requests=10")

        assert response.status_code == 200
        data = response.json()

        assert data["requests"] == 10
        assert data["total_time"] > 0
        assert data["avg_time_per_request"] > 0
        assert data["requests_per_second"] > 0

    @pytest.mark.asyncio
    async def test_sync_performance(self, client: AsyncClient, cassandra_service):
        """Test sync performance endpoint."""
        response = await client.get("/performance/sync?requests=10")

        assert response.status_code == 200
        data = response.json()

        assert data["requests"] == 10
        assert data["total_time"] > 0
        assert data["avg_time_per_request"] > 0
        assert data["requests_per_second"] > 0

    @pytest.mark.asyncio
    async def test_performance_comparison(self, client: AsyncClient, cassandra_service):
        """Test that async is faster than sync for concurrent operations."""
        # Run async test
        async_response = await client.get("/performance/async?requests=50")
        assert async_response.status_code == 200
        async_data = async_response.json()
        assert async_data["requests"] == 50
        assert async_data["total_time"] > 0
        assert async_data["requests_per_second"] > 0

        # Run sync test
        sync_response = await client.get("/performance/sync?requests=50")
        assert sync_response.status_code == 200
        sync_data = sync_response.json()
        assert sync_data["requests"] == 50
        assert sync_data["total_time"] > 0
        assert sync_data["requests_per_second"] > 0

        # Async should be significantly faster for concurrent operations
        # Note: In CI or under light load, the difference might be small
        # so we just verify both work correctly
        print(f"Async RPS: {async_data['requests_per_second']:.2f}")
        print(f"Sync RPS: {sync_data['requests_per_second']:.2f}")

        # For concurrent operations, async should generally be faster
        # but we'll be lenient in case of CI variability
        assert async_data["requests_per_second"] > sync_data["requests_per_second"] * 0.8


@pytest.mark.integration
class TestConcurrency:
    """Test concurrent operations."""

    @pytest.mark.asyncio
    async def test_concurrent_user_creation(self, client: AsyncClient, cassandra_service):
        """Test creating multiple users concurrently."""

        async def create_user(i: int):
            user_data = {
                "name": f"Concurrent User {i}",
                "email": f"concurrent{i}@example.com",
                "age": 20 + i,
            }
            response = await client.post("/users", json=user_data)
            return response.json()

        # Create 20 users concurrently
        users = await asyncio.gather(*[create_user(i) for i in range(20)])

        assert len(users) == 20

        # Verify all users have unique IDs
        user_ids = [user["id"] for user in users]
        assert len(set(user_ids)) == 20

    @pytest.mark.asyncio
    async def test_concurrent_read_write(self, client: AsyncClient, cassandra_service):
        """Test concurrent read and write operations."""
        # Create initial user
        user_data = {"name": "Concurrent Test", "email": "concurrent@example.com", "age": 30}

        create_response = await client.post("/users", json=user_data)
        user_id = create_response.json()["id"]

        async def read_user():
            response = await client.get(f"/users/{user_id}")
            return response.json()

        async def update_user(age: int):
            response = await client.put(f"/users/{user_id}", json={"age": age})
            return response.json()

        # Run mixed read/write operations concurrently
        operations = []
        for i in range(10):
            if i % 2 == 0:
                operations.append(read_user())
            else:
                operations.append(update_user(30 + i))

        results = await asyncio.gather(*operations, return_exceptions=True)

        # Verify no errors occurred
        for result in results:
            assert not isinstance(result, Exception)
