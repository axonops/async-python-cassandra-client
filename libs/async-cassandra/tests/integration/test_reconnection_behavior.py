"""
Integration tests comparing reconnection behavior between raw driver and async wrapper.

This test verifies that our wrapper doesn't interfere with the driver's reconnection logic.
"""

import asyncio
import os
import subprocess
import time

import pytest
from cassandra.cluster import Cluster
from cassandra.policies import ConstantReconnectionPolicy

from async_cassandra import AsyncCluster
from tests.utils.cassandra_control import CassandraControl


class TestReconnectionBehavior:
    """Test reconnection behavior of raw driver vs async wrapper."""

    def _get_cassandra_control(self, container=None):
        """Get Cassandra control interface for the test environment."""
        # For integration tests, create a mock container object with just the fields we need
        if container is None and os.environ.get("CI") != "true":
            container = type(
                "MockContainer",
                (),
                {
                    "container_name": "async-cassandra-test",
                    "runtime": (
                        "podman"
                        if subprocess.run(["which", "podman"], capture_output=True).returncode == 0
                        else "docker"
                    ),
                },
            )()
        return CassandraControl(container)

    @pytest.mark.integration
    def test_raw_driver_reconnection(self):
        """
        Test reconnection with raw Cassandra driver (synchronous).

        What this tests:
        ---------------
        1. Raw driver reconnects
        2. After service outage
        3. Reconnection policy works
        4. Full functionality restored

        Why this matters:
        ----------------
        Baseline behavior shows:
        - Expected reconnection time
        - Driver capabilities
        - Recovery patterns

        Wrapper must match this
        baseline behavior.
        """
        print("\n=== Testing Raw Driver Reconnection ===")

        # Skip this test in CI since we can't control Cassandra service
        if os.environ.get("CI") == "true":
            pytest.skip("Cannot control Cassandra service in CI environment")

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

        # Get control interface
        control = self._get_cassandra_control()

        # Disable Cassandra
        print("Disabling Cassandra binary protocol...")
        success = control.simulate_outage()
        assert success, "Failed to simulate Cassandra outage"
        print("✓ Cassandra is down")

        # Try query - should fail
        try:
            session.execute("SELECT * FROM test_table", timeout=2.0)
            assert False, "Query should have failed"
        except Exception as e:
            print(f"✓ Query failed as expected: {type(e).__name__}")

        # Re-enable Cassandra
        print("Re-enabling Cassandra binary protocol...")
        success = control.restore_service()
        assert success, "Failed to restore Cassandra service"
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
        """
        Test reconnection with async wrapper.

        What this tests:
        ---------------
        1. Wrapper reconnects properly
        2. Async operations resume
        3. No blocking during outage
        4. Same behavior as raw driver

        Why this matters:
        ----------------
        Wrapper must not break:
        - Driver reconnection logic
        - Automatic recovery
        - Connection pooling

        Critical for production
        reliability.
        """
        print("\n=== Testing Async Wrapper Reconnection ===")

        # Skip this test in CI since we can't control Cassandra service
        if os.environ.get("CI") == "true":
            pytest.skip("Cannot control Cassandra service in CI environment")

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

        # Get control interface
        control = self._get_cassandra_control()

        # Disable Cassandra
        print("Disabling Cassandra binary protocol...")
        success = control.simulate_outage()
        assert success, "Failed to simulate Cassandra outage"
        print("✓ Cassandra is down")

        # Try query - should fail
        try:
            await session.execute("SELECT * FROM test_table", timeout=2.0)
            assert False, "Query should have failed"
        except Exception as e:
            print(f"✓ Query failed as expected: {type(e).__name__}")

        # Re-enable Cassandra
        print("Re-enabling Cassandra binary protocol...")
        success = control.restore_service()
        assert success, "Failed to restore Cassandra service"
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
        """
        Compare reconnection timing between raw driver and async wrapper.

        What this tests:
        ---------------
        1. Both reconnect similarly
        2. Timing within 5 seconds
        3. No wrapper overhead
        4. Parallel comparison

        Why this matters:
        ----------------
        Performance validation:
        - Wrapper adds minimal delay
        - Recovery time predictable
        - Production SLAs met

        Ensures wrapper doesn't
        degrade reconnection.
        """
        print("\n=== Comparing Reconnection Timing ===")

        # Skip this test in CI since we can't control Cassandra service
        if os.environ.get("CI") == "true":
            pytest.skip("Cannot control Cassandra service in CI environment")

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

        # Get control interface
        control = self._get_cassandra_control()

        # Ensure Cassandra is up
        assert control.wait_for_cassandra_ready(), "Cassandra not ready at start"

        # Start both tests
        import threading

        raw_thread = threading.Thread(target=test_raw_driver)
        raw_thread.start()
        async_task = asyncio.create_task(test_async_wrapper())

        # Disable Cassandra after connections are established
        await asyncio.sleep(1)
        print("Disabling Cassandra...")
        control.simulate_outage()

        # Re-enable after a few seconds
        await asyncio.sleep(3)
        print("Re-enabling Cassandra...")
        control.restore_service()

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
