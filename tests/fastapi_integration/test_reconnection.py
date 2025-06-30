"""
Test FastAPI app reconnection behavior when Cassandra is stopped and restarted.

This test demonstrates that the cassandra-driver's ExponentialReconnectionPolicy
handles reconnection automatically, which is critical for rolling restarts and DC outages.
"""

import asyncio
import os
import time

import httpx
import pytest
import pytest_asyncio

from tests.utils.cassandra_control import CassandraControl


@pytest_asyncio.fixture(autouse=True)
async def ensure_cassandra_enabled(cassandra_container):
    """Ensure Cassandra binary protocol is enabled before and after each test."""
    control = CassandraControl(cassandra_container)

    # Enable at start
    control.enable_binary_protocol()
    await asyncio.sleep(2)

    yield

    # Enable at end (cleanup)
    control.enable_binary_protocol()
    await asyncio.sleep(2)


class TestFastAPIReconnection:
    """Test suite for FastAPI reconnection behavior."""

    async def _wait_for_api_health(
        self, client: httpx.AsyncClient, healthy: bool, timeout: int = 30
    ):
        """Wait for API health check to reach desired state."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                response = await client.get("/health")
                if response.status_code == 200:
                    data = response.json()
                    if data["cassandra_connected"] == healthy:
                        return True
            except httpx.RequestError:
                # Connection errors during reconnection
                if not healthy:
                    return True
            await asyncio.sleep(0.5)
        return False

    async def _verify_apis_working(self, client: httpx.AsyncClient):
        """Verify all APIs are working correctly."""
        # 1. Health check
        health_resp = await client.get("/health")
        assert health_resp.status_code == 200
        assert health_resp.json()["status"] == "healthy"
        assert health_resp.json()["cassandra_connected"] is True

        # 2. Create user
        user_data = {"name": "Reconnection Test User", "email": "reconnect@test.com", "age": 25}
        create_resp = await client.post("/users", json=user_data)
        assert create_resp.status_code == 201
        user_id = create_resp.json()["id"]

        # 3. Read user back
        get_resp = await client.get(f"/users/{user_id}")
        assert get_resp.status_code == 200
        assert get_resp.json()["name"] == user_data["name"]

        # 4. Test streaming
        stream_resp = await client.get("/users/stream?limit=10&fetch_size=10")
        assert stream_resp.status_code == 200
        stream_data = stream_resp.json()
        assert stream_data["metadata"]["streaming_enabled"] is True

        return user_id

    async def _verify_apis_return_errors(self, client: httpx.AsyncClient):
        """Verify APIs return appropriate errors when Cassandra is down."""
        # Wait a bit for existing connections to fail
        await asyncio.sleep(3)

        # Try to create a user - should fail
        user_data = {"name": "Should Fail", "email": "fail@test.com", "age": 30}
        error_occurred = False
        try:
            create_resp = await client.post("/users", json=user_data, timeout=10.0)
            print(f"Create user response during outage: {create_resp.status_code}")
            if create_resp.status_code >= 500:
                error_detail = create_resp.json().get("detail", "")
                print(f"Got expected error: {error_detail}")
                error_occurred = True
            else:
                # Might succeed if connection is still cached
                print(
                    f"Warning: Create succeeded with status {create_resp.status_code} - connection might be cached"
                )
        except (httpx.TimeoutException, httpx.RequestError) as e:
            print(f"Create user failed with {type(e).__name__} - this is expected")
            error_occurred = True

        # At least one operation should fail to confirm outage is detected
        if not error_occurred:
            # Try another operation that should fail
            try:
                # Force a new query that requires active connection
                list_resp = await client.get("/users?limit=100", timeout=10.0)
                if list_resp.status_code >= 500:
                    print(f"List users failed with {list_resp.status_code}")
                    error_occurred = True
            except (httpx.TimeoutException, httpx.RequestError) as e:
                print(f"List users failed with {type(e).__name__}")
                error_occurred = True

        assert error_occurred, "Expected at least one operation to fail during Cassandra outage"

    def _get_cassandra_control(self, container):
        """Get Cassandra control interface."""
        return CassandraControl(container)

    @pytest.mark.asyncio
    async def test_cassandra_reconnection_behavior(self, app_client, cassandra_container):
        """Test reconnection when Cassandra is stopped and restarted."""
        print("\n=== Testing Cassandra Reconnection Behavior ===")

        # Step 1: Verify everything works initially
        print("\n1. Verifying all APIs work initially...")
        user_id = await self._verify_apis_working(app_client)
        print("✓ All APIs working correctly")

        # Step 2: Disable binary protocol (simulate Cassandra outage)
        print("\n2. Disabling Cassandra binary protocol to simulate outage...")
        control = self._get_cassandra_control(cassandra_container)

        if os.environ.get("CI") == "true":
            print("  (In CI - cannot control service, skipping outage simulation)")
            print("\n✓ Test completed (CI environment)")
            return

        success, msg = control.disable_binary_protocol()
        if not success:
            pytest.fail(msg)
        print("✓ Binary protocol disabled")

        # Give it a moment for binary protocol to be disabled
        await asyncio.sleep(3)

        # Step 3: Verify APIs return appropriate errors
        print("\n3. Verifying APIs return appropriate errors during outage...")
        await self._verify_apis_return_errors(app_client)
        print("✓ APIs returning appropriate error responses")

        # Step 4: Re-enable binary protocol
        print("\n4. Re-enabling Cassandra binary protocol...")
        success, msg = control.enable_binary_protocol()
        if not success:
            pytest.fail(msg)
        print("✓ Binary protocol re-enabled")

        # Step 5: Wait for reconnection
        reconnect_timeout = 30  # seconds - give enough time for exponential backoff
        print(f"\n5. Waiting up to {reconnect_timeout} seconds for reconnection...")

        # Instead of checking health, try actual operations
        reconnected = False
        start_time = time.time()
        while time.time() - start_time < reconnect_timeout:
            try:
                # Try a simple query
                test_resp = await app_client.get("/users?limit=1", timeout=5.0)
                if test_resp.status_code == 200:
                    print("✓ Reconnection successful!")
                    reconnected = True
                    break
            except (httpx.TimeoutException, httpx.RequestError):
                pass
            await asyncio.sleep(2)

        if not reconnected:
            pytest.fail(f"Failed to reconnect within {reconnect_timeout} seconds")

        # Step 6: Verify all APIs work again
        print("\n6. Verifying all APIs work after recovery...")
        # Verify the user we created earlier still exists
        get_resp = await app_client.get(f"/users/{user_id}")
        assert get_resp.status_code == 200
        assert get_resp.json()["name"] == "Reconnection Test User"
        print("✓ Previously created user still accessible")

        # Create a new user to verify full functionality
        await self._verify_apis_working(app_client)
        print("✓ All APIs fully functional after recovery")

        print("\n✅ Reconnection test completed successfully!")
        print("   - APIs handled outage gracefully with appropriate errors")
        print("   - Automatic reconnection occurred after service restoration")
        print("   - No manual intervention required")

    @pytest.mark.asyncio
    async def test_multiple_reconnection_cycles(self, app_client, cassandra_container):
        """Test multiple disconnect/reconnect cycles to ensure stability."""
        print("\n=== Testing Multiple Reconnection Cycles ===")

        cycles = 3
        for cycle in range(1, cycles + 1):
            print(f"\n--- Cycle {cycle}/{cycles} ---")

            control = self._get_cassandra_control(cassandra_container)

            if os.environ.get("CI") == "true":
                print(f"Cycle {cycle}: Skipping in CI environment")
                continue

            # Disable
            print("Disabling binary protocol...")
            success, msg = control.disable_binary_protocol()
            if not success:
                pytest.fail(f"Cycle {cycle}: {msg}")

            await asyncio.sleep(2)

            # Verify unhealthy
            health_resp = await app_client.get("/health")
            assert health_resp.json()["cassandra_connected"] is False
            print("✓ Cassandra reported as disconnected")

            # Re-enable
            print("Re-enabling binary protocol...")
            success, msg = control.enable_binary_protocol()
            if not success:
                pytest.fail(f"Cycle {cycle}: {msg}")

            # Wait for reconnection
            if not await self._wait_for_api_health(app_client, healthy=True, timeout=10):
                pytest.fail(f"Cycle {cycle}: Failed to reconnect")
            print("✓ Reconnected successfully")

            # Verify functionality
            user_data = {
                "name": f"Cycle {cycle} User",
                "email": f"cycle{cycle}@test.com",
                "age": 20 + cycle,
            }
            create_resp = await app_client.post("/users", json=user_data)
            assert create_resp.status_code == 201
            print(f"✓ Created user for cycle {cycle}")

        print(f"\n✅ Successfully completed {cycles} reconnection cycles!")

    @pytest.mark.asyncio
    async def test_reconnection_during_active_requests(self, app_client, cassandra_container):
        """Test reconnection behavior when requests are active during outage."""
        print("\n=== Testing Reconnection During Active Requests ===")

        async def continuous_requests(client: httpx.AsyncClient, duration: int):
            """Make continuous requests for specified duration."""
            errors = []
            successes = 0
            start_time = time.time()

            while time.time() - start_time < duration:
                try:
                    resp = await client.get("/health")
                    if resp.status_code == 200 and resp.json()["cassandra_connected"]:
                        successes += 1
                    else:
                        errors.append("unhealthy")
                except Exception as e:
                    errors.append(str(type(e).__name__))
                await asyncio.sleep(0.1)

            return successes, errors

        # Start continuous requests in background
        request_task = asyncio.create_task(continuous_requests(app_client, 15))

        # Wait a bit for requests to start
        await asyncio.sleep(2)

        control = self._get_cassandra_control(cassandra_container)

        if os.environ.get("CI") == "true":
            print("Skipping outage simulation in CI environment")
            # Just let the requests run without outage
        else:
            # Disable binary protocol
            print("Disabling binary protocol during active requests...")
            control.disable_binary_protocol()

            # Wait for errors to accumulate
            await asyncio.sleep(3)

            # Re-enable binary protocol
            print("Re-enabling binary protocol...")
            control.enable_binary_protocol()

        # Wait for task to complete
        successes, errors = await request_task

        print("\nResults:")
        print(f"  - Successful requests: {successes}")
        print(f"  - Failed requests: {len(errors)}")
        print(f"  - Error types: {set(errors)}")

        # Should have both successes and failures
        assert successes > 0, "Should have successful requests before and after outage"
        assert len(errors) > 0, "Should have errors during outage"

        # Final health check should be healthy
        health_resp = await app_client.get("/health")
        assert health_resp.json()["cassandra_connected"] is True

        print("\n✅ Active requests handled reconnection gracefully!")
