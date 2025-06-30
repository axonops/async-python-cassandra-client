"""
Test to isolate why FastAPI app doesn't reconnect after Cassandra comes back.
"""

import asyncio
import subprocess
import time

import pytest
from cassandra.policies import ConstantReconnectionPolicy

from async_cassandra import AsyncCluster


class TestFastAPIReconnectionIsolation:
    """Isolate FastAPI reconnection issue."""

    def _wait_for_cassandra_ready(self, timeout=30):
        """Wait for Cassandra to be ready using cqlsh."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                result = subprocess.run(
                    ["cqlsh", "127.0.0.1", "-e", "SELECT release_version FROM system.local;"],
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

    def _wait_for_cassandra_down(self, timeout=10):
        """Wait for Cassandra to be down."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                result = subprocess.run(
                    ["cqlsh", "127.0.0.1", "-e", "SELECT 1;"],
                    capture_output=True,
                    text=True,
                    timeout=2,
                )
                if result.returncode != 0:
                    return True
            except (subprocess.TimeoutExpired, Exception):
                return True
            time.sleep(0.5)
        return False

    def _execute_nodetool_command(self, command):
        """Execute a nodetool command in the Cassandra container."""
        container_runtime = (
            "podman"
            if subprocess.run(["which", "podman"], capture_output=True).returncode == 0
            else "docker"
        )
        return subprocess.run(
            [container_runtime, "exec", "async-cassandra-test", "nodetool", command],
            capture_output=True,
            text=True,
        )

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_session_health_check_pattern(self):
        """Test the FastAPI health check pattern that might prevent reconnection."""
        print("\n=== Testing FastAPI Health Check Pattern ===")

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
            self._execute_nodetool_command("disablebinary")
            assert self._wait_for_cassandra_down()
            print("✓ Cassandra is down")

            # Health check should fail
            assert not await health_check(), "Health check should fail when Cassandra is down"
            print("✓ Health check correctly reports failure")

            # Re-enable Cassandra
            print("\nRe-enabling Cassandra...")
            self._execute_nodetool_command("enablebinary")
            assert self._wait_for_cassandra_ready()
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
    async def test_global_session_reconnection(self):
        """Test reconnection with global session variable like FastAPI."""
        print("\n=== Testing Global Session Reconnection ===")

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

            # Disable Cassandra
            print("\nDisabling Cassandra...")
            self._execute_nodetool_command("disablebinary")
            assert self._wait_for_cassandra_down()

            # Re-enable Cassandra
            print("Re-enabling Cassandra...")
            self._execute_nodetool_command("enablebinary")
            assert self._wait_for_cassandra_ready()

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
