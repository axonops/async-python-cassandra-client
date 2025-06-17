"""
Pytest configuration for integration tests.
"""

import os
import sys
from pathlib import Path

import pytest
import pytest_asyncio

from async_cassandra import AsyncCluster

# Add parent directory to path for test_utils import
sys.path.insert(0, str(Path(__file__).parent.parent))
from test_utils import (  # noqa: E402
    TestTableManager,
    cleanup_keyspace,
    create_test_keyspace,
    generate_unique_keyspace,
)

# Add integration test directory to path for container_manager import
sys.path.insert(0, str(Path(__file__).parent))
from container_manager import ContainerManager  # noqa: E402


def pytest_configure(config):
    """Configure pytest for integration tests."""
    # Skip if explicitly disabled
    if os.environ.get("SKIP_INTEGRATION_TESTS", "").lower() in ("1", "true", "yes"):
        return

    # Initialize container manager
    config.container_manager = ContainerManager()

    # Check if we're running as part of integration tests specifically
    # This prevents conflicts when running all tests together
    if hasattr(config, "_integration_tests_initialized"):
        return
    config._integration_tests_initialized = True

    # Start containers if not already running
    if not config.container_manager.is_running():
        try:
            config.container_manager.start_containers()
            config.containers_started = True
        except Exception as e:
            pytest.exit(f"Failed to start test containers: {e}", 1)
    else:
        config.containers_started = False
        print("Using existing Cassandra service on localhost:9042")


def pytest_unconfigure(config):
    """Clean up after tests."""
    # Only stop containers if we started them
    if (
        hasattr(config, "container_manager")
        and hasattr(config, "containers_started")
        and config.containers_started
        and os.environ.get("KEEP_CONTAINERS", "").lower() not in ("1", "true", "yes")
    ):
        try:
            config.container_manager.stop_containers()
        except Exception as e:
            print(f"Warning: Failed to stop containers: {e}")

        # Also try to clean up using the shell scripts as a backup
        try:
            import subprocess

            subprocess.run(
                ["./scripts/manage_test_containers.sh", "kill"], capture_output=True, timeout=10
            )
        except Exception:
            pass


@pytest_asyncio.fixture(scope="function")
async def cassandra_cluster(pytestconfig):
    """Create an async Cassandra cluster for testing."""
    # Check Cassandra health before creating cluster
    if hasattr(pytestconfig, "container_manager"):
        health = pytestconfig.container_manager.check_health()
        if not health["native_transport"] or not health["cql_available"]:
            pytest.fail(f"Cassandra not healthy: {health}")
    else:
        # If no container manager, just check if Cassandra is available
        import socket

        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)
            result = sock.connect_ex(("localhost", 9042))
            sock.close()
            if result != 0:
                pytest.fail("Cassandra not available on localhost:9042")
        except Exception as e:
            pytest.fail(f"Cannot connect to Cassandra: {e}")

    # Set protocol_version to 5 to avoid negotiation issues
    # Use shorter timeout for tests
    cluster = AsyncCluster(contact_points=["localhost"], protocol_version=5, connect_timeout=5.0)
    yield cluster
    await cluster.shutdown()


@pytest_asyncio.fixture(scope="function", autouse=True)
async def ensure_cassandra_healthy(pytestconfig):
    """Ensure Cassandra is healthy before each test."""
    if hasattr(pytestconfig, "container_manager"):
        # Check health before test
        try:
            health = pytestconfig.container_manager.check_health()
            if not health["native_transport"] or not health["cql_available"]:
                # Try to wait a bit and check again
                import asyncio

                await asyncio.sleep(2)
                health = pytestconfig.container_manager.check_health()
                if not health["native_transport"] or not health["cql_available"]:
                    pytest.fail(f"Cassandra not healthy before test: {health}")
        except Exception as e:
            pytest.fail(f"Error checking Cassandra health: {e}")
    else:
        # Minimal health check if no container manager
        import socket

        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)
            result = sock.connect_ex(("localhost", 9042))
            sock.close()
            if result != 0:
                pytest.fail("Cassandra not available on localhost:9042")
        except Exception as e:
            pytest.fail(f"Cannot connect to Cassandra: {e}")

    yield

    # Optional: Check health after test
    if hasattr(pytestconfig, "container_manager"):
        try:
            health = pytestconfig.container_manager.check_health()
            if not health["native_transport"]:
                print(f"Warning: Cassandra health degraded after test: {health}")
        except Exception:
            pass  # Don't fail on post-test health check


@pytest_asyncio.fixture(scope="function")
async def cassandra_session(cassandra_cluster):
    """Create an async Cassandra session with isolated keyspace."""
    session = await cassandra_cluster.connect()

    # Create unique keyspace for this test
    keyspace = generate_unique_keyspace("test_integ")
    await create_test_keyspace(session, keyspace)
    await session.set_keyspace(keyspace)

    # Create users table for tests that expect it
    await session.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id UUID PRIMARY KEY,
            name TEXT,
            email TEXT,
            age INT
        )
    """
    )

    yield session

    # Cleanup
    try:
        await session.close()
    except Exception:
        pass

    # Clean up keyspace after test
    try:
        admin_session = await cassandra_cluster.connect()
        await cleanup_keyspace(admin_session, keyspace)
        await admin_session.close()
    except Exception:
        pass


@pytest_asyncio.fixture(scope="function")
async def test_table_manager(cassandra_cluster):
    """Provide a test table manager for isolated table creation."""
    session = await cassandra_cluster.connect()

    async with TestTableManager(session) as manager:
        yield manager

    await session.close()


@pytest.fixture
def unique_keyspace():
    """Generate a unique keyspace name for test isolation."""
    return generate_unique_keyspace()


@pytest_asyncio.fixture(scope="function")
async def session_with_keyspace(cassandra_cluster):
    """Create a session with a unique keyspace already set."""
    session = await cassandra_cluster.connect()
    keyspace = generate_unique_keyspace("test_session")

    await create_test_keyspace(session, keyspace)
    await session.set_keyspace(keyspace)

    yield session, keyspace

    # Cleanup
    try:
        await cleanup_keyspace(session, keyspace)
    except Exception:
        pass

    try:
        await session.close()
    except Exception:
        pass
