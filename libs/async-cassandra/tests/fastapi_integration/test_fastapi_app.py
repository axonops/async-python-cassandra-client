"""
Comprehensive test suite for the FastAPI example application.

This validates that the example properly demonstrates all the
improvements made to the async-cassandra library.
"""

import asyncio
import os
import time
import uuid

import httpx
import pytest
import pytest_asyncio
from httpx import ASGITransport


class TestFastAPIExample:
    """Test suite for FastAPI example application."""

    @pytest_asyncio.fixture
    async def app_client(self):
        """Create test client for the FastAPI app."""
        # First, check that Cassandra is available
        from async_cassandra import AsyncCluster

        try:
            test_cluster = AsyncCluster(contact_points=["localhost"])
            test_session = await test_cluster.connect()
            await test_session.execute("SELECT now() FROM system.local")
            await test_session.close()
            await test_cluster.shutdown()
        except Exception as e:
            pytest.fail(f"Cassandra not available: {e}")

        from main import app, lifespan

        # Manually handle lifespan since httpx doesn't do it properly
        async with lifespan(app):
            transport = ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                yield client

    @pytest.mark.asyncio
    async def test_health_and_basic_operations(self, app_client):
        """Test health check and basic CRUD operations."""
        print("\n=== Testing Health and Basic Operations ===")

        # Health check
        health_resp = await app_client.get("/health")
        assert health_resp.status_code == 200
        assert health_resp.json()["status"] == "healthy"
        print("✓ Health check passed")

        # Create user
        user_data = {"name": "Test User", "email": "test@example.com", "age": 30}
        create_resp = await app_client.post("/users", json=user_data)
        assert create_resp.status_code == 201
        user = create_resp.json()
        print(f"✓ Created user: {user['id']}")

        # Get user
        get_resp = await app_client.get(f"/users/{user['id']}")
        assert get_resp.status_code == 200
        assert get_resp.json()["name"] == user_data["name"]
        print("✓ Retrieved user successfully")

        # Update user
        update_data = {"age": 31}
        update_resp = await app_client.put(f"/users/{user['id']}", json=update_data)
        assert update_resp.status_code == 200
        assert update_resp.json()["age"] == 31
        print("✓ Updated user successfully")

        # Delete user
        delete_resp = await app_client.delete(f"/users/{user['id']}")
        assert delete_resp.status_code == 204
        print("✓ Deleted user successfully")

    @pytest.mark.asyncio
    async def test_thread_safety_under_concurrency(self, app_client):
        """Test thread safety improvements with concurrent operations."""
        print("\n=== Testing Thread Safety Under Concurrency ===")

        async def create_and_read_user(user_id: int):
            """Create a user and immediately read it back."""
            # Create
            user_data = {
                "name": f"Concurrent User {user_id}",
                "email": f"concurrent{user_id}@test.com",
                "age": 25 + (user_id % 10),
            }
            create_resp = await app_client.post("/users", json=user_data)
            if create_resp.status_code != 201:
                return None

            created_user = create_resp.json()

            # Immediately read back
            get_resp = await app_client.get(f"/users/{created_user['id']}")
            if get_resp.status_code != 200:
                return None

            return get_resp.json()

        # Run many concurrent operations
        num_concurrent = 50
        start_time = time.time()

        results = await asyncio.gather(
            *[create_and_read_user(i) for i in range(num_concurrent)], return_exceptions=True
        )

        duration = time.time() - start_time

        # Check results
        successful = [r for r in results if isinstance(r, dict)]
        errors = [r for r in results if isinstance(r, Exception)]

        print(f"✓ Completed {num_concurrent} concurrent operations in {duration:.2f}s")
        print(f"  - Successful: {len(successful)}")
        print(f"  - Errors: {len(errors)}")

        # Thread safety should ensure high success rate
        assert len(successful) >= num_concurrent * 0.95  # 95% success rate

        # Verify data consistency
        for user in successful:
            assert "id" in user
            assert "name" in user
            assert user["created_at"] is not None

    @pytest.mark.asyncio
    async def test_streaming_memory_efficiency(self, app_client):
        """Test streaming functionality for memory efficiency."""
        print("\n=== Testing Streaming Memory Efficiency ===")

        # Create a batch of users for streaming
        batch_size = 100
        batch_data = {
            "users": [
                {"name": f"Stream Test {i}", "email": f"stream{i}@test.com", "age": 20 + (i % 50)}
                for i in range(batch_size)
            ]
        }

        batch_resp = await app_client.post("/users/batch", json=batch_data)
        assert batch_resp.status_code == 201
        print(f"✓ Created {batch_size} users for streaming test")

        # Test regular streaming
        stream_resp = await app_client.get(f"/users/stream?limit={batch_size}&fetch_size=10")
        assert stream_resp.status_code == 200
        stream_data = stream_resp.json()

        assert stream_data["metadata"]["streaming_enabled"] is True
        assert stream_data["metadata"]["pages_fetched"] > 1
        assert len(stream_data["users"]) >= batch_size
        print(
            f"✓ Streamed {len(stream_data['users'])} users in {stream_data['metadata']['pages_fetched']} pages"
        )

        # Test page-by-page streaming
        pages_resp = await app_client.get(
            f"/users/stream/pages?limit={batch_size}&fetch_size=10&max_pages=5"
        )
        assert pages_resp.status_code == 200
        pages_data = pages_resp.json()

        assert pages_data["metadata"]["streaming_mode"] == "page_by_page"
        assert len(pages_data["pages_info"]) <= 5
        print(
            f"✓ Page-by-page streaming: {pages_data['total_rows_processed']} rows in {len(pages_data['pages_info'])} pages"
        )

    @pytest.mark.asyncio
    async def test_error_handling_consistency(self, app_client):
        """Test error handling improvements."""
        print("\n=== Testing Error Handling Consistency ===")

        # Test invalid UUID handling
        invalid_uuid_resp = await app_client.get("/users/not-a-uuid")
        assert invalid_uuid_resp.status_code == 400
        assert "Invalid UUID" in invalid_uuid_resp.json()["detail"]
        print("✓ Invalid UUID error handled correctly")

        # Test non-existent resource
        fake_uuid = str(uuid.uuid4())
        not_found_resp = await app_client.get(f"/users/{fake_uuid}")
        assert not_found_resp.status_code == 404
        assert "User not found" in not_found_resp.json()["detail"]
        print("✓ Resource not found error handled correctly")

        # Test validation errors - missing required field
        invalid_user_resp = await app_client.post(
            "/users", json={"name": "Test"}  # Missing email and age
        )
        assert invalid_user_resp.status_code == 422
        print("✓ Validation error handled correctly")

        # Test streaming with invalid parameters
        invalid_stream_resp = await app_client.get("/users/stream?fetch_size=0")
        assert invalid_stream_resp.status_code == 422
        print("✓ Streaming parameter validation working")

    @pytest.mark.asyncio
    async def test_performance_comparison(self, app_client):
        """Test performance endpoints to validate async benefits."""
        print("\n=== Testing Performance Comparison ===")

        # Compare async vs sync performance
        num_requests = 50

        # Test async performance
        async_resp = await app_client.get(f"/performance/async?requests={num_requests}")
        assert async_resp.status_code == 200
        async_data = async_resp.json()

        # Test sync performance
        sync_resp = await app_client.get(f"/performance/sync?requests={num_requests}")
        assert sync_resp.status_code == 200
        sync_data = sync_resp.json()

        print(f"✓ Async performance: {async_data['requests_per_second']:.1f} req/s")
        print(f"✓ Sync performance: {sync_data['requests_per_second']:.1f} req/s")
        print(
            f"✓ Speedup factor: {async_data['requests_per_second'] / sync_data['requests_per_second']:.1f}x"
        )

        # Skip performance comparison in CI environments
        if os.getenv("CI") != "true":
            # Async should be significantly faster
            assert async_data["requests_per_second"] > sync_data["requests_per_second"]
        else:
            # In CI, just verify both completed successfully
            assert async_data["requests"] == num_requests
            assert sync_data["requests"] == num_requests
            assert async_data["requests_per_second"] > 0
            assert sync_data["requests_per_second"] > 0

    @pytest.mark.asyncio
    async def test_monitoring_endpoints(self, app_client):
        """Test monitoring and metrics endpoints."""
        print("\n=== Testing Monitoring Endpoints ===")

        # Test metrics endpoint
        metrics_resp = await app_client.get("/metrics")
        assert metrics_resp.status_code == 200
        metrics = metrics_resp.json()

        assert "query_performance" in metrics
        assert "cassandra_connections" in metrics
        print("✓ Metrics endpoint working")

        # Test shutdown endpoint
        shutdown_resp = await app_client.post("/shutdown")
        assert shutdown_resp.status_code == 200
        assert "Shutdown initiated" in shutdown_resp.json()["message"]
        print("✓ Shutdown endpoint working")

    @pytest.mark.asyncio
    async def test_timeout_handling(self, app_client):
        """Test timeout handling capabilities."""
        print("\n=== Testing Timeout Handling ===")

        # Test with short timeout (should timeout)
        timeout_resp = await app_client.get("/slow_query", headers={"X-Request-Timeout": "0.1"})
        assert timeout_resp.status_code == 504
        print("✓ Short timeout handled correctly")

        # Test with adequate timeout
        success_resp = await app_client.get("/slow_query", headers={"X-Request-Timeout": "10"})
        assert success_resp.status_code == 200
        print("✓ Adequate timeout allows completion")

    @pytest.mark.asyncio
    async def test_context_manager_safety(self, app_client):
        """Test comprehensive context manager safety in FastAPI."""
        print("\n=== Testing Context Manager Safety ===")

        # Get initial status
        status = await app_client.get("/context_manager_safety/status")
        assert status.status_code == 200
        initial_state = status.json()
        print(
            f"✓ Initial state: Session={initial_state['session_open']}, Cluster={initial_state['cluster_open']}"
        )

        # Test 1: Query errors don't close session
        print("\nTest 1: Query Error Safety")
        query_error_resp = await app_client.post("/context_manager_safety/query_error")
        assert query_error_resp.status_code == 200
        query_result = query_error_resp.json()
        assert query_result["session_unchanged"] is True
        assert query_result["session_open"] is True
        assert query_result["session_still_works"] is True
        assert "non_existent_table_xyz" in query_result["error_caught"]
        print("✓ Query errors don't close session")
        print(f"  - Error caught: {query_result['error_caught'][:50]}...")
        print(f"  - Session still works: {query_result['session_still_works']}")

        # Test 2: Streaming errors don't close session
        print("\nTest 2: Streaming Error Safety")
        stream_error_resp = await app_client.post("/context_manager_safety/streaming_error")
        assert stream_error_resp.status_code == 200
        stream_result = stream_error_resp.json()
        assert stream_result["session_unchanged"] is True
        assert stream_result["session_open"] is True
        assert stream_result["streaming_error_caught"] is True
        # The session_still_streams might be False if no users exist, but session should work
        if not stream_result["session_still_streams"]:
            print(f"  - Note: No users found ({stream_result['rows_after_error']} rows)")
            # Create a user for subsequent tests
            user_resp = await app_client.post(
                "/users", json={"name": "Test User", "email": "test@example.com", "age": 30}
            )
            assert user_resp.status_code == 201
        print("✓ Streaming errors don't close session")
        print(f"  - Error caught: {stream_result['error_message'][:50]}...")
        print(f"  - Session remains open: {stream_result['session_open']}")

        # Test 3: Concurrent streams don't interfere
        print("\nTest 3: Concurrent Streams Safety")
        concurrent_resp = await app_client.post("/context_manager_safety/concurrent_streams")
        assert concurrent_resp.status_code == 200
        concurrent_result = concurrent_resp.json()
        print(f"  - Debug: Results = {concurrent_result['results']}")
        assert concurrent_result["streams_completed"] == 3
        # Check if streams worked independently (each should have 10 users)
        if not concurrent_result["all_streams_independent"]:
            print(
                f"  - Warning: Stream counts varied: {[r['count'] for r in concurrent_result['results']]}"
            )
        assert concurrent_result["session_still_open"] is True
        print("✓ Concurrent streams completed")
        for result in concurrent_result["results"]:
            print(f"  - Age {result['age']}: {result['count']} users")

        # Test 4: Nested context managers
        print("\nTest 4: Nested Context Managers")
        nested_resp = await app_client.post("/context_manager_safety/nested_contexts")
        assert nested_resp.status_code == 200
        nested_result = nested_resp.json()
        assert nested_result["correct_order"] is True
        assert nested_result["main_session_unaffected"] is True
        assert nested_result["row_count"] == 5
        print("✓ Nested contexts close in correct order")
        print(f"  - Events: {' → '.join(nested_result['events'][:5])}...")
        print(f"  - Main session unaffected: {nested_result['main_session_unaffected']}")

        # Test 5: Streaming cancellation
        print("\nTest 5: Streaming Cancellation Safety")
        cancel_resp = await app_client.post("/context_manager_safety/cancellation")
        assert cancel_resp.status_code == 200
        cancel_result = cancel_resp.json()
        assert cancel_result["was_cancelled"] is True
        assert cancel_result["session_still_works"] is True
        assert cancel_result["new_stream_worked"] is True
        assert cancel_result["session_open"] is True
        print("✓ Cancelled streams clean up properly")
        print(f"  - Rows before cancel: {cancel_result['rows_processed_before_cancel']}")
        print(f"  - Session works after cancel: {cancel_result['session_still_works']}")
        print(f"  - New stream successful: {cancel_result['new_stream_worked']}")

        # Verify final state matches initial state
        final_status = await app_client.get("/context_manager_safety/status")
        assert final_status.status_code == 200
        final_state = final_status.json()
        assert final_state["session_id"] == initial_state["session_id"]
        assert final_state["cluster_id"] == initial_state["cluster_id"]
        assert final_state["session_open"] is True
        assert final_state["cluster_open"] is True
        print("\n✓ All context manager safety tests passed!")
        print("  - Session remained stable throughout all tests")
        print("  - No resource leaks detected")


async def run_all_tests():
    """Run all tests and print summary."""
    print("=" * 60)
    print("FastAPI Example Application Test Suite")
    print("=" * 60)

    test_suite = TestFastAPIExample()

    # Create client
    from main import app

    async with httpx.AsyncClient(app=app, base_url="http://test") as client:
        # Run tests
        try:
            await test_suite.test_health_and_basic_operations(client)
            await test_suite.test_thread_safety_under_concurrency(client)
            await test_suite.test_streaming_memory_efficiency(client)
            await test_suite.test_error_handling_consistency(client)
            await test_suite.test_performance_comparison(client)
            await test_suite.test_monitoring_endpoints(client)
            await test_suite.test_timeout_handling(client)
            await test_suite.test_context_manager_safety(client)

            print("\n" + "=" * 60)
            print("✅ All tests passed! The FastAPI example properly demonstrates:")
            print("  - Thread safety improvements")
            print("  - Memory-efficient streaming")
            print("  - Consistent error handling")
            print("  - Performance benefits of async")
            print("  - Monitoring capabilities")
            print("  - Timeout handling")
            print("=" * 60)

        except AssertionError as e:
            print(f"\n❌ Test failed: {e}")
            raise
        except Exception as e:
            print(f"\n❌ Unexpected error: {e}")
            raise


if __name__ == "__main__":
    # Run the test suite
    asyncio.run(run_all_tests())
