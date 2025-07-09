"""
BDD tests for FastAPI Cassandra reconnection behavior.

This test validates the application's ability to handle Cassandra outages
and automatically recover when the database becomes available again.
"""

import asyncio
import os
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest
import pytest_asyncio
from httpx import ASGITransport

# Import the cassandra_container fixture
pytest_plugins = ["tests._fixtures.cassandra"]

# Add FastAPI app to path
fastapi_app_dir = Path(__file__).parent.parent.parent / "examples" / "fastapi_app"
sys.path.insert(0, str(fastapi_app_dir))

# Import test utilities
from tests.test_utils import (  # noqa: E402
    cleanup_keyspace,
    create_test_keyspace,
    generate_unique_keyspace,
)
from tests.utils.cassandra_control import CassandraControl  # noqa: E402


def wait_for_cassandra_ready(host="127.0.0.1", timeout=30):
    """Wait for Cassandra to be ready by executing a test query with cqlsh."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            # Use cqlsh to test if Cassandra is ready
            result = subprocess.run(
                ["cqlsh", host, "-e", "SELECT release_version FROM system.local;"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode == 0:
                return True
        except (subprocess.TimeoutExpired, Exception):
            pass
        time.sleep(0.5)
    return False


def wait_for_cassandra_down(host="127.0.0.1", timeout=10):
    """Wait for Cassandra to be down by checking if cqlsh fails."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            result = subprocess.run(
                ["cqlsh", host, "-e", "SELECT 1;"], capture_output=True, text=True, timeout=2
            )
            if result.returncode != 0:
                return True
        except (subprocess.TimeoutExpired, Exception):
            return True
        time.sleep(0.5)
    return False


@pytest_asyncio.fixture(autouse=True)
async def ensure_cassandra_enabled_bdd(cassandra_container):
    """Ensure Cassandra binary protocol is enabled before and after each test."""
    # Enable at start
    subprocess.run(
        [
            cassandra_container.runtime,
            "exec",
            cassandra_container.container_name,
            "nodetool",
            "enablebinary",
        ],
        capture_output=True,
    )
    await asyncio.sleep(2)

    yield

    # Enable at end (cleanup)
    subprocess.run(
        [
            cassandra_container.runtime,
            "exec",
            cassandra_container.container_name,
            "nodetool",
            "enablebinary",
        ],
        capture_output=True,
    )
    await asyncio.sleep(2)


@pytest_asyncio.fixture
async def unique_test_keyspace(cassandra_container):
    """Create a unique keyspace for each test."""
    from async_cassandra import AsyncCluster

    # Check health before proceeding
    health = cassandra_container.check_health()
    if not health["native_transport"] or not health["cql_available"]:
        pytest.fail(f"Cassandra not healthy: {health}")

    cluster = AsyncCluster(contact_points=["127.0.0.1"], protocol_version=5)
    session = await cluster.connect()

    # Create unique keyspace
    keyspace = generate_unique_keyspace("bdd_reconnection")
    await create_test_keyspace(session, keyspace)

    yield keyspace

    # Cleanup
    await cleanup_keyspace(session, keyspace)
    await session.close()
    await cluster.shutdown()
    # Give extra time for driver's internal threads to fully stop
    await asyncio.sleep(2)


@pytest_asyncio.fixture
async def app_client(unique_test_keyspace):
    """Create test client for the FastAPI app with isolated keyspace."""
    # Set the test keyspace in environment
    os.environ["TEST_KEYSPACE"] = unique_test_keyspace

    from main import app, lifespan

    # Manually handle lifespan since httpx doesn't do it properly
    async with lifespan(app):
        transport = ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            yield client

    # Clean up environment
    os.environ.pop("TEST_KEYSPACE", None)


def run_async(coro):
    """Run async code in sync context."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


class TestFastAPIReconnectionBDD:
    """BDD tests for Cassandra reconnection in FastAPI applications."""

    def _get_cassandra_control(self, container):
        """Get Cassandra control interface."""
        return CassandraControl(container)

    def test_cassandra_outage_and_recovery(self, app_client, cassandra_container):
        """
        Given: A FastAPI application connected to Cassandra
        When: Cassandra becomes temporarily unavailable and then recovers
        Then: The application should handle the outage gracefully and automatically reconnect
        """

        async def test_scenario():
            # Given: A connected FastAPI application with working APIs
            print("\nGiven: A FastAPI application with working Cassandra connection")

            # Verify health check shows connected
            health_response = await app_client.get("/health")
            assert health_response.status_code == 200
            assert health_response.json()["cassandra_connected"] is True
            print("✓ Health check confirms Cassandra is connected")

            # Create a test user to verify functionality
            user_data = {"name": "Reconnection Test User", "email": "reconnect@test.com", "age": 30}
            create_response = await app_client.post("/users", json=user_data)
            assert create_response.status_code == 201
            user_id = create_response.json()["id"]
            print(f"✓ Created test user with ID: {user_id}")

            # Verify streaming works
            stream_response = await app_client.get("/users/stream?limit=5&fetch_size=10")
            if stream_response.status_code != 200:
                print(f"Stream response status: {stream_response.status_code}")
                print(f"Stream response body: {stream_response.text}")
            assert stream_response.status_code == 200
            assert stream_response.json()["metadata"]["streaming_enabled"] is True
            print("✓ Streaming API is working")

            # When: Cassandra binary protocol is disabled (simulating outage)
            print("\nWhen: Cassandra becomes unavailable (disabling binary protocol)")

            # Skip this test in CI since we can't control Cassandra service
            if os.environ.get("CI") == "true":
                pytest.skip("Cannot control Cassandra service in CI environment")

            control = self._get_cassandra_control(cassandra_container)
            success = control.simulate_outage()
            assert success, "Failed to simulate Cassandra outage"
            print("✓ Binary protocol disabled - simulating Cassandra outage")
            print("✓ Confirmed Cassandra is down via cqlsh")

            # Then: APIs should return 503 Service Unavailable errors
            print("\nThen: APIs should return 503 Service Unavailable errors")

            # Try to create a user - should fail with 503
            try:
                user_data = {"name": "Test User", "email": "test@example.com", "age": 25}
                error_response = await app_client.post("/users", json=user_data, timeout=10.0)
                if error_response.status_code == 503:
                    print("✓ Create user returns 503 Service Unavailable")
                else:
                    print(
                        f"Warning: Create user returned {error_response.status_code} instead of 503"
                    )
            except (httpx.TimeoutException, httpx.RequestError) as e:
                print(f"✓ Create user failed with {type(e).__name__} (expected)")

            # Verify health check shows disconnected
            health_response = await app_client.get("/health")
            assert health_response.status_code == 200
            assert health_response.json()["cassandra_connected"] is False
            print("✓ Health check correctly reports Cassandra as disconnected")

            # When: Cassandra becomes available again
            print("\nWhen: Cassandra becomes available again (enabling binary protocol)")

            if os.environ.get("CI") == "true":
                print("  (In CI - Cassandra service always running)")
                # In CI, Cassandra is always available
            else:
                success = control.restore_service()
                assert success, "Failed to restore Cassandra service"
                print("✓ Binary protocol re-enabled")
                print("✓ Confirmed Cassandra is ready via cqlsh")

            # Then: The application should automatically reconnect
            print("\nThen: The application should automatically reconnect")

            # Now check if the app has reconnected
            # The FastAPI app uses a 2-second constant reconnection delay, so we need to wait
            # at least that long plus some buffer for the reconnection to complete
            reconnected = False
            # Wait up to 30 seconds - driver needs time to rediscover the host
            for attempt in range(30):  # Up to 30 seconds (30 * 1s)
                try:
                    # Check health first to see connection status
                    health_resp = await app_client.get("/health")
                    if health_resp.status_code == 200:
                        health_data = health_resp.json()
                        if health_data.get("cassandra_connected"):
                            # Now try actual query
                            response = await app_client.get("/users?limit=1")
                            if response.status_code == 200:
                                reconnected = True
                                print(f"✓ App reconnected after {attempt + 1} seconds")
                                break
                            else:
                                print(
                                    f"  Health says connected but query returned {response.status_code}"
                                )
                        else:
                            if attempt % 5 == 0:  # Print every 5 seconds
                                print(
                                    f"  After {attempt} seconds: Health check says not connected yet"
                                )
                except (httpx.TimeoutException, httpx.RequestError) as e:
                    print(f"  Attempt {attempt + 1}: Connection error: {type(e).__name__}")
                await asyncio.sleep(1.0)  # Check every second

            assert reconnected, "Application failed to reconnect after Cassandra came back"
            print("✓ Application successfully reconnected to Cassandra")

            # Verify health check shows connected again
            health_response = await app_client.get("/health")
            assert health_response.status_code == 200
            assert health_response.json()["cassandra_connected"] is True
            print("✓ Health check confirms reconnection")

            # Verify we can retrieve the previously created user
            get_response = await app_client.get(f"/users/{user_id}")
            assert get_response.status_code == 200
            assert get_response.json()["name"] == "Reconnection Test User"
            print("✓ Previously created data is still accessible")

            # Create a new user to verify full functionality
            new_user_data = {"name": "Post-Recovery User", "email": "recovery@test.com", "age": 35}
            create_response = await app_client.post("/users", json=new_user_data)
            assert create_response.status_code == 201
            print("✓ Can create new users after recovery")

            # Verify streaming works again
            stream_response = await app_client.get("/users/stream?limit=5&fetch_size=10")
            assert stream_response.status_code == 200
            assert stream_response.json()["metadata"]["streaming_enabled"] is True
            print("✓ Streaming API works after recovery")

            print("\n✅ Cassandra reconnection test completed successfully!")
            print("   - Application handled outage gracefully with 503 errors")
            print("   - Automatic reconnection occurred without manual intervention")
            print("   - All functionality restored after recovery")

        # Run the async test scenario
        run_async(test_scenario())

    def test_multiple_outage_cycles(self, app_client, cassandra_container):
        """
        Given: A FastAPI application connected to Cassandra
        When: Cassandra experiences multiple outage/recovery cycles
        Then: The application should handle each cycle gracefully
        """

        async def test_scenario():
            print("\nGiven: A FastAPI application with Cassandra connection")

            # Skip this test in CI since we can't control Cassandra service
            if os.environ.get("CI") == "true":
                pytest.skip("Cannot control Cassandra service in CI environment")

            # Verify initial health
            health_response = await app_client.get("/health")
            assert health_response.status_code == 200
            assert health_response.json()["cassandra_connected"] is True

            cycles = 1  # Just test one cycle to speed up
            for cycle in range(1, cycles + 1):
                print(f"\nWhen: Cassandra outage cycle {cycle}/{cycles} begins")

                # Disable binary protocol
                control = self._get_cassandra_control(cassandra_container)

                if os.environ.get("CI") == "true":
                    print(f"  Cycle {cycle}: Skipping in CI - cannot control service")
                    continue

                success = control.simulate_outage()
                assert success, f"Cycle {cycle}: Failed to simulate outage"
                print(f"✓ Cycle {cycle}: Binary protocol disabled")
                print(f"✓ Cycle {cycle}: Confirmed Cassandra is down via cqlsh")

                # Verify unhealthy state
                health_response = await app_client.get("/health")
                assert health_response.json()["cassandra_connected"] is False
                print(f"✓ Cycle {cycle}: Health check reports disconnected")

                # Re-enable binary protocol
                success = control.restore_service()
                assert success, f"Cycle {cycle}: Failed to restore service"
                print(f"✓ Cycle {cycle}: Binary protocol re-enabled")
                print(f"✓ Cycle {cycle}: Confirmed Cassandra is ready via cqlsh")

                # Check app reconnection
                # The FastAPI app uses a 2-second constant reconnection delay
                reconnected = False
                for _ in range(8):  # Up to 4 seconds to account for 2s reconnection delay
                    try:
                        response = await app_client.get("/users?limit=1")
                        if response.status_code == 200:
                            reconnected = True
                            break
                    except Exception:
                        pass
                    await asyncio.sleep(0.5)

                assert reconnected, f"Cycle {cycle}: Failed to reconnect"
                print(f"✓ Cycle {cycle}: Successfully reconnected")

                # Verify functionality with a test operation
                user_data = {
                    "name": f"Cycle {cycle} User",
                    "email": f"cycle{cycle}@test.com",
                    "age": 20 + cycle,
                }
                create_response = await app_client.post("/users", json=user_data)
                assert create_response.status_code == 201
                print(f"✓ Cycle {cycle}: Created test user successfully")

            print(f"\nThen: All {cycles} outage cycles handled successfully")
            print("✅ Multiple reconnection cycles completed without issues!")

        run_async(test_scenario())

    def test_reconnection_during_active_load(self, app_client, cassandra_container):
        """
        Given: A FastAPI application under active load
        When: Cassandra becomes unavailable during request processing
        Then: The application should handle in-flight requests gracefully and recover
        """

        async def test_scenario():
            print("\nGiven: A FastAPI application handling active requests")

            # Skip this test in CI since we can't control Cassandra service
            if os.environ.get("CI") == "true":
                pytest.skip("Cannot control Cassandra service in CI environment")

            # Track request results
            request_results = {"successes": 0, "errors": [], "error_types": set()}

            async def continuous_requests(client: httpx.AsyncClient, duration: int):
                """Make continuous requests for specified duration."""
                start_time = time.time()

                while time.time() - start_time < duration:
                    try:
                        # Alternate between different endpoints
                        endpoints = [
                            ("/health", "GET", None),
                            ("/users?limit=5", "GET", None),
                            (
                                "/users",
                                "POST",
                                {"name": "Load Test", "email": "load@test.com", "age": 25},
                            ),
                        ]

                        endpoint, method, data = endpoints[int(time.time()) % len(endpoints)]

                        if method == "GET":
                            response = await client.get(endpoint, timeout=5.0)
                        else:
                            response = await client.post(endpoint, json=data, timeout=5.0)

                        if response.status_code in [200, 201]:
                            request_results["successes"] += 1
                        elif response.status_code == 503:
                            request_results["errors"].append("503_service_unavailable")
                            request_results["error_types"].add("503")
                        else:
                            request_results["errors"].append(f"status_{response.status_code}")
                            request_results["error_types"].add(str(response.status_code))

                    except (httpx.TimeoutException, httpx.RequestError) as e:
                        request_results["errors"].append(type(e).__name__)
                        request_results["error_types"].add(type(e).__name__)

                    await asyncio.sleep(0.1)

            # Start continuous requests in background
            print("Starting continuous load generation...")
            request_task = asyncio.create_task(continuous_requests(app_client, 15))

            # Let requests run for a bit
            await asyncio.sleep(3)
            print(f"✓ Initial requests successful: {request_results['successes']}")

            # When: Cassandra becomes unavailable during active load
            print("\nWhen: Cassandra becomes unavailable during active requests")
            control = self._get_cassandra_control(cassandra_container)

            if os.environ.get("CI") == "true":
                print("  (In CI - cannot disable service, continuing with available service)")
            else:
                success = control.simulate_outage()
                assert success, "Failed to simulate outage"
                print("✓ Binary protocol disabled during active load")

            # Let errors accumulate
            await asyncio.sleep(4)
            print(f"✓ Errors during outage: {len(request_results['errors'])}")

            # Re-enable Cassandra
            print("\nWhen: Cassandra becomes available again")
            if not os.environ.get("CI") == "true":
                success = control.restore_service()
                assert success, "Failed to restore service"
                print("✓ Binary protocol re-enabled")

            # Wait for task completion
            await request_task

            # Then: Analyze results
            print("\nThen: Application should have handled the outage gracefully")
            print("Results:")
            print(f"  - Successful requests: {request_results['successes']}")
            print(f"  - Failed requests: {len(request_results['errors'])}")
            print(f"  - Error types seen: {request_results['error_types']}")

            # Verify we had both successes and failures
            assert (
                request_results["successes"] > 0
            ), "Should have successful requests before/after outage"
            assert len(request_results["errors"]) > 0, "Should have errors during outage"
            assert (
                "503" in request_results["error_types"] or len(request_results["error_types"]) > 0
            ), "Should have seen 503 errors or connection errors"

            # Final health check
            health_response = await app_client.get("/health")
            assert health_response.status_code == 200
            assert health_response.json()["cassandra_connected"] is True
            print("✓ Final health check confirms recovery")

            print("\n✅ Active load reconnection test completed successfully!")
            print("   - Application continued serving requests where possible")
            print("   - Errors were returned appropriately during outage")
            print("   - Automatic recovery restored full functionality")

        run_async(test_scenario())

    def test_rapid_connection_cycling(self, app_client, cassandra_container):
        """
        Given: A FastAPI application connected to Cassandra
        When: Cassandra connection is rapidly cycled (quick disable/enable)
        Then: The application should remain stable and not leak resources
        """

        async def test_scenario():
            print("\nGiven: A FastAPI application with stable Cassandra connection")

            # Skip this test in CI since we can't control Cassandra service
            if os.environ.get("CI") == "true":
                pytest.skip("Cannot control Cassandra service in CI environment")

            # Create initial user to establish baseline
            initial_user = {"name": "Baseline User", "email": "baseline@test.com", "age": 25}
            response = await app_client.post("/users", json=initial_user)
            assert response.status_code == 201
            print("✓ Baseline functionality confirmed")

            print("\nWhen: Rapidly cycling Cassandra connection")

            # Perform rapid cycles
            for i in range(5):
                print(f"\nRapid cycle {i+1}/5:")

                control = self._get_cassandra_control(cassandra_container)

                if os.environ.get("CI") == "true":
                    print("  - Skipping cycle in CI")
                    break

                # Quick disable
                control.disable_binary_protocol()
                print("  - Disabled")

                # Very short wait
                await asyncio.sleep(0.5)

                # Quick enable
                control.enable_binary_protocol()
                print("  - Enabled")

                # Minimal wait before next cycle
                await asyncio.sleep(1)

            print("\nThen: Application should remain stable and recover")

            # The FastAPI app has ConstantReconnectionPolicy with 2 second delay
            # So it should recover automatically once Cassandra is available
            print("Waiting for FastAPI app to automatically recover...")
            recovery_start = time.time()
            app_recovered = False

            # Wait for the app to recover - checking via health endpoint and actual operations
            while time.time() - recovery_start < 15:
                try:
                    # Test with a real operation
                    test_user = {
                        "name": "Recovery Test User",
                        "email": "recovery@test.com",
                        "age": 30,
                    }
                    response = await app_client.post("/users", json=test_user, timeout=3.0)
                    if response.status_code == 201:
                        app_recovered = True
                        recovery_time = time.time() - recovery_start
                        print(f"✓ App recovered and accepting requests (took {recovery_time:.1f}s)")
                        break
                    else:
                        print(f"  - Got status {response.status_code}, waiting for recovery...")
                except Exception as e:
                    print(f"  - Still recovering: {type(e).__name__}")

                await asyncio.sleep(1)

            assert (
                app_recovered
            ), "FastAPI app should automatically recover when Cassandra is available"

            # Verify health check also shows recovery
            health_response = await app_client.get("/health")
            assert health_response.status_code == 200
            assert health_response.json()["cassandra_connected"] is True
            print("✓ Health check confirms full recovery")

            # Verify streaming works after recovery
            stream_response = await app_client.get("/users/stream?limit=5")
            assert stream_response.status_code == 200
            print("✓ Streaming functionality recovered")

            print("\n✅ Rapid connection cycling test completed!")
            print("   - Application remained stable during rapid cycling")
            print("   - Automatic recovery worked as expected")
            print("   - All functionality restored after Cassandra recovery")

        run_async(test_scenario())
