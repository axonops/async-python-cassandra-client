"""
Integration tests comparing reconnection behavior between raw driver and async wrapper.

This test verifies that our wrapper doesn't interfere with the driver's reconnection logic.
"""

import asyncio
import subprocess
import time

import pytest
from cassandra.cluster import Cluster
from cassandra.policies import ConstantReconnectionPolicy

from async_cassandra import AsyncCluster


class TestReconnectionBehavior:
    """Test reconnection behavior of raw driver vs async wrapper."""

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

    @pytest.mark.integration
    def test_raw_driver_reconnection(self):
        """Test reconnection with raw Cassandra driver (synchronous)."""
        print("\n=== Testing Raw Driver Reconnection ===")

        # Create cluster with constant reconnection policy
        cluster = Cluster(
            contact_points=["127.0.0.1"],
            protocol_version=5,
            reconnection_policy=ConstantReconnectionPolicy(delay=2.0),
            connect_timeout=10.0,
        )

        session = cluster.connect()

        # Create test keyspace and table
        session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS reconnect_test_sync
            WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
        """
        )
        session.set_keyspace("reconnect_test_sync")
        session.execute("DROP TABLE IF EXISTS test_table")
        session.execute(
            """
            CREATE TABLE test_table (
                id INT PRIMARY KEY,
                value TEXT
            )
        """
        )

        # Insert initial data
        session.execute("INSERT INTO test_table (id, value) VALUES (1, 'before_outage')")
        result = session.execute("SELECT * FROM test_table WHERE id = 1")
        assert result.one().value == "before_outage"
        print("✓ Initial connection working")

        # Disable Cassandra
        print("Disabling Cassandra binary protocol...")
        disable_result = self._execute_nodetool_command("disablebinary")
        assert disable_result.returncode == 0
        assert self._wait_for_cassandra_down(), "Cassandra did not go down"
        print("✓ Cassandra is down")

        # Try query - should fail
        try:
            session.execute("SELECT * FROM test_table", timeout=2.0)
            assert False, "Query should have failed"
        except Exception as e:
            print(f"✓ Query failed as expected: {type(e).__name__}")

        # Re-enable Cassandra
        print("Re-enabling Cassandra binary protocol...")
        enable_result = self._execute_nodetool_command("enablebinary")
        assert enable_result.returncode == 0
        assert self._wait_for_cassandra_ready(), "Cassandra did not come back"
        print("✓ Cassandra is ready")

        # Test reconnection - try for up to 30 seconds
        reconnected = False
        start_time = time.time()
        while time.time() - start_time < 30:
            try:
                result = session.execute("SELECT * FROM test_table WHERE id = 1")
                if result.one().value == "before_outage":
                    reconnected = True
                    elapsed = time.time() - start_time
                    print(f"✓ Raw driver reconnected after {elapsed:.1f} seconds")
                    break
            except Exception:
                pass
            time.sleep(1)

        assert reconnected, "Raw driver failed to reconnect within 30 seconds"

        # Insert new data to verify full functionality
        session.execute("INSERT INTO test_table (id, value) VALUES (2, 'after_reconnect')")
        result = session.execute("SELECT * FROM test_table WHERE id = 2")
        assert result.one().value == "after_reconnect"
        print("✓ Can insert and query after reconnection")

        cluster.shutdown()

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_async_wrapper_reconnection(self):
        """Test reconnection with async wrapper."""
        print("\n=== Testing Async Wrapper Reconnection ===")

        # Create cluster with constant reconnection policy
        cluster = AsyncCluster(
            contact_points=["127.0.0.1"],
            protocol_version=5,
            reconnection_policy=ConstantReconnectionPolicy(delay=2.0),
            connect_timeout=10.0,
        )

        session = await cluster.connect()

        # Create test keyspace and table
        await session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS reconnect_test_async
            WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
        """
        )
        await session.set_keyspace("reconnect_test_async")
        await session.execute("DROP TABLE IF EXISTS test_table")
        await session.execute(
            """
            CREATE TABLE test_table (
                id INT PRIMARY KEY,
                value TEXT
            )
        """
        )

        # Insert initial data
        await session.execute("INSERT INTO test_table (id, value) VALUES (1, 'before_outage')")
        result = await session.execute("SELECT * FROM test_table WHERE id = 1")
        assert result.one().value == "before_outage"
        print("✓ Initial connection working")

        # Disable Cassandra
        print("Disabling Cassandra binary protocol...")
        disable_result = self._execute_nodetool_command("disablebinary")
        assert disable_result.returncode == 0
        assert self._wait_for_cassandra_down(), "Cassandra did not go down"
        print("✓ Cassandra is down")

        # Try query - should fail
        try:
            await session.execute("SELECT * FROM test_table", timeout=2.0)
            assert False, "Query should have failed"
        except Exception as e:
            print(f"✓ Query failed as expected: {type(e).__name__}")

        # Re-enable Cassandra
        print("Re-enabling Cassandra binary protocol...")
        enable_result = self._execute_nodetool_command("enablebinary")
        assert enable_result.returncode == 0
        assert self._wait_for_cassandra_ready(), "Cassandra did not come back"
        print("✓ Cassandra is ready")

        # Test reconnection - try for up to 30 seconds
        reconnected = False
        start_time = time.time()
        while time.time() - start_time < 30:
            try:
                result = await session.execute("SELECT * FROM test_table WHERE id = 1")
                if result.one().value == "before_outage":
                    reconnected = True
                    elapsed = time.time() - start_time
                    print(f"✓ Async wrapper reconnected after {elapsed:.1f} seconds")
                    break
            except Exception:
                pass
            await asyncio.sleep(1)

        assert reconnected, "Async wrapper failed to reconnect within 30 seconds"

        # Insert new data to verify full functionality
        await session.execute("INSERT INTO test_table (id, value) VALUES (2, 'after_reconnect')")
        result = await session.execute("SELECT * FROM test_table WHERE id = 2")
        assert result.one().value == "after_reconnect"
        print("✓ Can insert and query after reconnection")

        await session.close()
        await cluster.shutdown()

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_reconnection_timing_comparison(self):
        """Compare reconnection timing between raw driver and async wrapper."""
        print("\n=== Comparing Reconnection Timing ===")

        # Test both in parallel to ensure fair comparison
        raw_reconnect_time = None
        async_reconnect_time = None

        def test_raw_driver():
            nonlocal raw_reconnect_time
            cluster = Cluster(
                contact_points=["127.0.0.1"],
                protocol_version=5,
                reconnection_policy=ConstantReconnectionPolicy(delay=2.0),
                connect_timeout=10.0,
            )
            session = cluster.connect()
            session.execute("SELECT now() FROM system.local")

            # Wait for Cassandra to be down
            time.sleep(2)  # Give time for Cassandra to be disabled

            # Measure reconnection time
            start_time = time.time()
            while time.time() - start_time < 30:
                try:
                    session.execute("SELECT now() FROM system.local")
                    raw_reconnect_time = time.time() - start_time
                    break
                except Exception:
                    time.sleep(0.5)

            cluster.shutdown()

        async def test_async_wrapper():
            nonlocal async_reconnect_time
            cluster = AsyncCluster(
                contact_points=["127.0.0.1"],
                protocol_version=5,
                reconnection_policy=ConstantReconnectionPolicy(delay=2.0),
                connect_timeout=10.0,
            )
            session = await cluster.connect()
            await session.execute("SELECT now() FROM system.local")

            # Wait for Cassandra to be down
            await asyncio.sleep(2)  # Give time for Cassandra to be disabled

            # Measure reconnection time
            start_time = time.time()
            while time.time() - start_time < 30:
                try:
                    await session.execute("SELECT now() FROM system.local")
                    async_reconnect_time = time.time() - start_time
                    break
                except Exception:
                    await asyncio.sleep(0.5)

            await session.close()
            await cluster.shutdown()

        # Ensure Cassandra is up
        assert self._wait_for_cassandra_ready(), "Cassandra not ready at start"

        # Start both tests
        import threading

        raw_thread = threading.Thread(target=test_raw_driver)
        raw_thread.start()
        async_task = asyncio.create_task(test_async_wrapper())

        # Disable Cassandra after connections are established
        await asyncio.sleep(1)
        print("Disabling Cassandra...")
        self._execute_nodetool_command("disablebinary")
        assert self._wait_for_cassandra_down()

        # Re-enable after a few seconds
        await asyncio.sleep(3)
        print("Re-enabling Cassandra...")
        self._execute_nodetool_command("enablebinary")
        assert self._wait_for_cassandra_ready()

        # Wait for both tests to complete
        raw_thread.join(timeout=35)
        await asyncio.wait_for(async_task, timeout=35)

        # Compare results
        print("\nReconnection times:")
        print(
            f"  Raw driver: {raw_reconnect_time:.1f}s"
            if raw_reconnect_time
            else "  Raw driver: Failed to reconnect"
        )
        print(
            f"  Async wrapper: {async_reconnect_time:.1f}s"
            if async_reconnect_time
            else "  Async wrapper: Failed to reconnect"
        )

        # Both should reconnect
        assert raw_reconnect_time is not None, "Raw driver failed to reconnect"
        assert async_reconnect_time is not None, "Async wrapper failed to reconnect"

        # Times should be similar (within 5 seconds)
        time_diff = abs(raw_reconnect_time - async_reconnect_time)
        assert time_diff < 5.0, f"Reconnection time difference too large: {time_diff:.1f}s"
        print(f"✓ Reconnection times are similar (difference: {time_diff:.1f}s)")
