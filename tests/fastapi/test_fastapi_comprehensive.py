"""
Comprehensive integration tests for FastAPI application.

Following TDD principles, these tests are written FIRST to define
the expected behavior of the async-cassandra framework when used
with FastAPI - its primary use case.
"""

import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient


@pytest.mark.integration
class TestFastAPIComprehensive:
    """Comprehensive tests for FastAPI integration following TDD principles."""

    @pytest.fixture
    def test_client(self):
        """Create FastAPI test client."""
        # Import here to ensure app is created fresh
        from examples.fastapi_app.main import app

        # TestClient properly handles lifespan in newer FastAPI versions
        with TestClient(app) as client:
            yield client

    def test_health_check_endpoint(self, test_client):
        """
        GIVEN a FastAPI application with async-cassandra
        WHEN the health endpoint is called
        THEN it should return healthy status without blocking
        """
        response = test_client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["cassandra_connected"] is True
        assert "timestamp" in data

    def test_concurrent_request_handling(self, test_client):
        """
        GIVEN a FastAPI application handling multiple concurrent requests
        WHEN many requests are sent simultaneously
        THEN all requests should be handled without blocking or data corruption
        """

        # Create multiple users concurrently
        def create_user(i):
            user_data = {
                "name": f"concurrent_user_{i}",  # Changed from username to name
                "email": f"user{i}@example.com",
                "age": 25 + (i % 50),  # Add required age field
            }
            return test_client.post("/users", json=user_data)

        # Send 50 concurrent requests
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(create_user, i) for i in range(50)]
            responses = [f.result() for f in futures]

        # All should succeed
        assert all(r.status_code == 201 for r in responses)

        # Verify no data corruption - all users should be unique
        user_ids = [r.json()["id"] for r in responses]
        assert len(set(user_ids)) == 50  # All IDs should be unique

    def test_streaming_large_datasets(self, test_client):
        """
        GIVEN a large dataset in Cassandra
        WHEN streaming data through FastAPI
        THEN memory usage should remain constant and not accumulate
        """
        # First create some users to stream
        for i in range(100):
            user_data = {
                "name": f"stream_user_{i}",
                "email": f"stream{i}@example.com",
                "age": 20 + (i % 60),
            }
            test_client.post("/users", json=user_data)

        # Test streaming endpoint - currently fails due to route ordering bug in FastAPI app
        # where /users/{user_id} matches before /users/stream
        response = test_client.get("/users/stream?limit=100&fetch_size=10")

        # This test expects the streaming functionality to work
        # Currently it fails with 400 due to route ordering issue
        assert response.status_code == 200
        data = response.json()
        assert "users" in data
        assert "metadata" in data
        assert data["metadata"]["streaming_enabled"] is True
        assert len(data["users"]) >= 100  # Should have at least the users we created

    def test_error_handling_and_recovery(self, test_client):
        """
        GIVEN various error conditions
        WHEN errors occur during request processing
        THEN the application should handle them gracefully and recover
        """
        # Test 1: Invalid UUID
        response = test_client.get("/users/invalid-uuid")
        assert response.status_code == 400
        assert "Invalid UUID" in response.json()["detail"]

        # Test 2: Non-existent resource
        non_existent_id = str(uuid.uuid4())
        response = test_client.get(f"/users/{non_existent_id}")
        assert response.status_code == 404
        assert "User not found" in response.json()["detail"]

        # Test 3: Invalid data
        response = test_client.post("/users", json={"invalid": "data"})
        assert response.status_code == 422  # FastAPI validation error

        # Test 4: Verify app still works after errors
        health_response = test_client.get("/health")
        assert health_response.status_code == 200

    def test_connection_pool_behavior(self, test_client):
        """
        GIVEN limited connection pool resources
        WHEN many requests exceed pool capacity
        THEN requests should queue appropriately without failing
        """
        # Create a burst of requests that exceed typical pool size
        start_time = time.time()

        def make_request(i):
            return test_client.get("/users")

        # Send 100 requests with limited concurrency
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = [executor.submit(make_request, i) for i in range(100)]
            responses = [f.result() for f in futures]

        duration = time.time() - start_time

        # All should eventually succeed
        assert all(r.status_code == 200 for r in responses)

        # Should complete in reasonable time (not hung)
        assert duration < 30  # 30 seconds for 100 requests is reasonable

    def test_prepared_statement_caching(self, test_client):
        """
        GIVEN repeated identical queries
        WHEN executed multiple times
        THEN prepared statements should be cached and reused
        """
        # Create a user first
        user_data = {"name": "test_user", "email": "test@example.com", "age": 25}
        create_response = test_client.post("/users", json=user_data)
        user_id = create_response.json()["id"]

        # Get the same user multiple times
        responses = []
        for _ in range(10):
            response = test_client.get(f"/users/{user_id}")
            responses.append(response)

        # All should succeed and return same data
        assert all(r.status_code == 200 for r in responses)
        assert all(r.json()["id"] == user_id for r in responses)

        # Performance should improve after first query (prepared statement cached)
        # This is more of a performance characteristic than functional test

    def test_batch_operations(self, test_client):
        """
        GIVEN multiple operations to perform
        WHEN executed as a batch
        THEN all operations should succeed atomically
        """
        # Create multiple users in a batch
        batch_data = {
            "users": [
                {"name": f"batch_user_{i}", "email": f"batch{i}@example.com", "age": 25 + i}
                for i in range(5)
            ]
        }

        response = test_client.post("/users/batch", json=batch_data)
        assert response.status_code == 201

        created_users = response.json()["created"]
        assert len(created_users) == 5

        # Verify all were created
        for user in created_users:
            get_response = test_client.get(f"/users/{user['id']}")
            assert get_response.status_code == 200

    def test_async_context_manager_usage(self, test_client):
        """
        GIVEN async context manager pattern
        WHEN used in request handlers
        THEN resources should be properly managed
        """
        # This tests that sessions are properly closed even with errors
        # Make multiple requests that might fail
        for i in range(10):
            if i % 2 == 0:
                # Valid request
                test_client.get("/users")
            else:
                # Invalid request
                test_client.get("/users/invalid-uuid")

        # Verify system still healthy
        health = test_client.get("/health")
        assert health.status_code == 200

    def test_monitoring_and_metrics(self, test_client):
        """
        GIVEN monitoring endpoints
        WHEN metrics are requested
        THEN accurate metrics should be returned
        """
        # Make some requests to generate metrics
        for _ in range(5):
            test_client.get("/users")

        # Get metrics
        response = test_client.get("/metrics")
        assert response.status_code == 200

        metrics = response.json()
        assert "total_requests" in metrics
        assert metrics["total_requests"] >= 5
        assert "query_performance" in metrics

    @pytest.mark.parametrize("consistency_level", ["ONE", "QUORUM", "ALL"])
    def test_consistency_levels(self, test_client, consistency_level):
        """
        GIVEN different consistency level requirements
        WHEN operations are performed
        THEN the appropriate consistency should be used
        """
        # Create user with specific consistency level
        user_data = {
            "name": f"consistency_test_{consistency_level}",
            "email": f"test_{consistency_level}@example.com",
            "age": 25,
        }

        response = test_client.post(
            "/users", json=user_data, headers={"X-Consistency-Level": consistency_level}
        )

        assert response.status_code == 201

        # Verify it was created
        user_id = response.json()["id"]
        get_response = test_client.get(
            f"/users/{user_id}", headers={"X-Consistency-Level": consistency_level}
        )
        assert get_response.status_code == 200

    def test_timeout_handling(self, test_client):
        """
        GIVEN timeout constraints
        WHEN operations exceed timeout
        THEN appropriate timeout errors should be returned
        """
        # Create a slow query endpoint (would need to be added to FastAPI app)
        response = test_client.get(
            "/slow_query", headers={"X-Request-Timeout": "0.1"}  # 100ms timeout
        )

        # Should timeout
        assert response.status_code == 504  # Gateway timeout

    def test_no_blocking_of_event_loop(self, test_client):
        """
        GIVEN async operations running
        WHEN Cassandra operations are performed
        THEN the event loop should not be blocked
        """
        # Start a long-running query
        import threading

        long_query_done = threading.Event()

        def long_query():
            test_client.get("/long_running_query")
            long_query_done.set()

        # Start long query in background
        thread = threading.Thread(target=long_query)
        thread.start()

        # Meanwhile, other quick queries should still work
        start_time = time.time()
        for _ in range(5):
            response = test_client.get("/health")
            assert response.status_code == 200

        quick_queries_time = time.time() - start_time

        # Quick queries should complete fast even with long query running
        assert quick_queries_time < 1.0  # Should take less than 1 second

        # Wait for long query to complete
        thread.join(timeout=5)

    def test_graceful_shutdown(self, test_client):
        """
        GIVEN an active FastAPI application
        WHEN shutdown is initiated
        THEN all connections should be properly closed
        """
        # Make some requests
        for _ in range(3):
            test_client.get("/users")

        # Trigger shutdown (this would need shutdown endpoint)
        response = test_client.post("/shutdown")
        assert response.status_code == 200

        # Verify connections were closed properly
        # (Would need to check connection metrics)
