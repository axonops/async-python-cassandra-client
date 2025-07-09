"""
Test to isolate why FastAPI app doesn't reconnect after Cassandra comes back.
"""

import asyncio
import os
import time

import pytest
from cassandra.policies import ConstantReconnectionPolicy

from async_cassandra import AsyncCluster
from tests.utils.cassandra_control import CassandraControl


class TestFastAPIReconnectionIsolation:
    """Isolate FastAPI reconnection issue."""

    def _get_cassandra_control(self, container=None):
        """Get Cassandra control interface."""
        return CassandraControl(container)

    @pytest.mark.integration
    @pytest.mark.asyncio
    @pytest.mark.skip(reason="Requires container control not available in CI")
    async def test_session_health_check_pattern(self):
        """
        Test the FastAPI health check pattern that might prevent reconnection.

        What this tests:
        ---------------
        1. Health check pattern
        2. Failure detection
        3. Recovery behavior
        4. Session reuse

        Why this matters:
        ----------------
        FastAPI patterns:
        - Health endpoints common
        - Global session reuse
        - Must handle outages

        Verifies reconnection works
        with app patterns.
        """
        pytest.skip("This test requires container control capabilities")
        print("\n=== Testing FastAPI Health Check Pattern ===")

        # Skip this test in CI since we can't control Cassandra service
        if os.environ.get("CI") == "true":
            pytest.skip("Cannot control Cassandra service in CI environment")

        # Simulate FastAPI startup
        cluster = None
        session = None

        try:
            # Initial connection (like FastAPI startup)
            cluster = AsyncCluster(
                contact_points=["127.0.0.1"],
                protocol_version=5,
                reconnection_policy=ConstantReconnectionPolicy(delay=2.0),
                connect_timeout=10.0,
            )
            session = await cluster.connect()
            print("✓ Initial connection established")

            # Create keyspace
            await session.execute(
                """
                CREATE KEYSPACE IF NOT EXISTS fastapi_test
                WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
            """
            )
            await session.set_keyspace("fastapi_test")

            # Simulate health check function
            async def health_check():
                """Simulate FastAPI health check."""
                try:
                    if session is None:
                        return False
                    await session.execute("SELECT now() FROM system.local")
                    return True
                except Exception:
                    return False

            # Initial health check should pass
            assert await health_check(), "Initial health check failed"
            print("✓ Initial health check passed")

            # Disable Cassandra
            print("\nDisabling Cassandra...")
            control = self._get_cassandra_control()

            if os.environ.get("CI") == "true":
                # Still test that health check works with available service
                print("✓ Skipping outage simulation in CI")
            else:
                success = control.simulate_outage()
                assert success, "Failed to simulate outage"
                print("✓ Cassandra is down")

            # Health check behavior depends on environment
            if os.environ.get("CI") == "true":
                # In CI, Cassandra is always up
                assert await health_check(), "Health check should pass in CI"
                print("✓ Health check passes (CI environment)")
            else:
                # In local env, should fail when down
                assert not await health_check(), "Health check should fail when Cassandra is down"
                print("✓ Health check correctly reports failure")

            # Re-enable Cassandra
            print("\nRe-enabling Cassandra...")
            if not os.environ.get("CI") == "true":
                success = control.restore_service()
                assert success, "Failed to restore service"
                print("✓ Cassandra is ready")

            # Test health check recovery
            print("\nTesting health check recovery...")
            recovered = False
            start_time = time.time()

            for attempt in range(30):
                if await health_check():
                    recovered = True
                    elapsed = time.time() - start_time
                    print(f"✓ Health check recovered after {elapsed:.1f} seconds")
                    break
                await asyncio.sleep(1)
                if attempt % 5 == 0:
                    print(f"  After {attempt} seconds: Health check still failing")

            if not recovered:
                # Try a direct query to see if session works
                print("\nTesting direct query...")
                try:
                    await session.execute("SELECT now() FROM system.local")
                    print("✓ Direct query works! Health check pattern may be caching errors")
                except Exception as e:
                    print(f"✗ Direct query also fails: {type(e).__name__}: {e}")

            assert recovered, "Health check never recovered"

        finally:
            if session:
                await session.close()
            if cluster:
                await cluster.shutdown()

    @pytest.mark.integration
    @pytest.mark.asyncio
    @pytest.mark.skip(reason="Requires container control not available in CI")
    async def test_global_session_reconnection(self):
        """
        Test reconnection with global session variable like FastAPI.

        What this tests:
        ---------------
        1. Global session pattern
        2. Reconnection works
        3. No session replacement
        4. Automatic recovery

        Why this matters:
        ----------------
        Global state common:
        - FastAPI apps
        - Flask apps
        - Service patterns

        Must reconnect without
        manual intervention.
        """
        pytest.skip("This test requires container control capabilities")
        print("\n=== Testing Global Session Reconnection ===")

        # Skip this test in CI since we can't control Cassandra service
        if os.environ.get("CI") == "true":
            pytest.skip("Cannot control Cassandra service in CI environment")

        # Global variables like in FastAPI
        global session, cluster
        session = None
        cluster = None

        try:
            # Startup
            cluster = AsyncCluster(
                contact_points=["127.0.0.1"],
                protocol_version=5,
                reconnection_policy=ConstantReconnectionPolicy(delay=2.0),
                connect_timeout=10.0,
            )
            session = await cluster.connect()
            print("✓ Global session created")

            # Create keyspace
            await session.execute(
                """
                CREATE KEYSPACE IF NOT EXISTS global_test
                WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
            """
            )
            await session.set_keyspace("global_test")

            # Test query
            await session.execute("SELECT now() FROM system.local")
            print("✓ Initial query works")

            # Get control interface
            control = self._get_cassandra_control()

            if os.environ.get("CI") == "true":
                print("\nSkipping outage simulation in CI")
                # In CI, just test that the session works
                await session.execute("SELECT now() FROM system.local")
                print("✓ Session works in CI environment")
            else:
                # Disable Cassandra
                print("\nDisabling Cassandra...")
                control.simulate_outage()

                # Re-enable Cassandra
                print("Re-enabling Cassandra...")
                control.restore_service()

            # Test recovery with global session
            print("\nTesting global session recovery...")
            recovered = False
            for attempt in range(30):
                try:
                    await session.execute("SELECT now() FROM system.local")
                    recovered = True
                    print(f"✓ Global session recovered after {attempt + 1} seconds")
                    break
                except Exception as e:
                    if attempt % 5 == 0:
                        print(f"  After {attempt} seconds: {type(e).__name__}")
                await asyncio.sleep(1)

            assert recovered, "Global session never recovered"

        finally:
            if session:
                await session.close()
            if cluster:
                await cluster.shutdown()
