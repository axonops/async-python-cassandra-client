"""
Pytest configuration for integration tests.
"""

import os
import socket
import sys
from pathlib import Path

import pytest
import pytest_asyncio

from async_cassandra import AsyncCluster

# Add parent directory to path for test_utils import
sys.path.insert(0, str(Path(__file__).parent.parent))
from test_utils import (  # noqa: E402
    TestTableManager,
    generate_unique_keyspace,
    generate_unique_table,
)


def pytest_configure(config):
    """Configure pytest for integration tests."""
    # Skip if explicitly disabled
    if os.environ.get("SKIP_INTEGRATION_TESTS", "").lower() in ("1", "true", "yes"):
        pytest.exit("Skipping integration tests (SKIP_INTEGRATION_TESTS is set)", 0)

    # Store shared keyspace name
    config.shared_test_keyspace = "integration_test"

    # Get contact points from environment
    contact_points = os.environ.get("CASSANDRA_CONTACT_POINTS", "localhost").split(",")
    config.cassandra_contact_points = [cp.strip() for cp in contact_points]

    # Check if Cassandra is available
    cassandra_port = int(os.environ.get("CASSANDRA_PORT", "9042"))
    available = False
    for contact_point in config.cassandra_contact_points:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2)
            result = sock.connect_ex((contact_point, cassandra_port))
            sock.close()
            if result == 0:
                available = True
                print(f"Found Cassandra on {contact_point}:{cassandra_port}")
                break
        except Exception:
            pass

    if not available:
        pytest.exit(
            f"Cassandra is not available on {config.cassandra_contact_points}:{cassandra_port}\n"
            f"Please start Cassandra using: make cassandra-start\n"
            f"Or set CASSANDRA_CONTACT_POINTS environment variable to point to your Cassandra instance",
            1,
        )


@pytest_asyncio.fixture(scope="session")
async def shared_keyspace_setup(pytestconfig):
    """Create shared keyspace for all integration tests."""
    # Create a cluster and session for setting up the shared keyspace
    cluster = AsyncCluster(
        contact_points=pytestconfig.cassandra_contact_points,
        protocol_version=5,
        connect_timeout=10.0,
    )
    session = await cluster.connect()

    try:
        # Create the shared keyspace
        keyspace_name = pytestconfig.shared_test_keyspace
        await session.execute(
            f"""
            CREATE KEYSPACE IF NOT EXISTS {keyspace_name}
            WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
            """
        )
        print(f"Created shared keyspace: {keyspace_name}")

        yield keyspace_name

    finally:
        # Clean up the keyspace after all tests
        try:
            await session.execute(f"DROP KEYSPACE IF EXISTS {pytestconfig.shared_test_keyspace}")
            print(f"Dropped shared keyspace: {pytestconfig.shared_test_keyspace}")
        except Exception as e:
            print(f"Warning: Failed to drop shared keyspace: {e}")

        await session.close()
        await cluster.shutdown()


@pytest_asyncio.fixture(scope="function")
async def cassandra_cluster(pytestconfig):
    """Create an async Cassandra cluster for testing."""
    # Set protocol_version to 5 to avoid negotiation issues
    # Use reasonable timeout for tests
    cluster = AsyncCluster(
        contact_points=pytestconfig.cassandra_contact_points,
        protocol_version=5,
        connect_timeout=10.0,
    )
    yield cluster
    await cluster.shutdown()


@pytest_asyncio.fixture(scope="function")
async def cassandra_session(cassandra_cluster, shared_keyspace_setup, pytestconfig):
    """Create an async Cassandra session using shared keyspace with isolated tables."""
    session = await cassandra_cluster.connect()

    # Use the shared keyspace
    keyspace = pytestconfig.shared_test_keyspace
    await session.set_keyspace(keyspace)

    # Track tables created for this test
    created_tables = []

    # Create a unique users table for tests that expect it
    users_table = generate_unique_table("users")
    await session.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {users_table} (
            id UUID PRIMARY KEY,
            name TEXT,
            email TEXT,
            age INT
        )
    """
    )
    created_tables.append(users_table)

    # Store the table name in session for tests to use
    session._test_users_table = users_table
    session._created_tables = created_tables

    yield session

    # Cleanup tables after test
    try:
        for table in created_tables:
            await session.execute(f"DROP TABLE IF EXISTS {table}")
    except Exception:
        pass

    # Close session
    try:
        await session.close()
    except Exception:
        pass


@pytest_asyncio.fixture(scope="function")
async def test_table_manager(cassandra_cluster, shared_keyspace_setup, pytestconfig):
    """Provide a test table manager for isolated table creation."""
    session = await cassandra_cluster.connect()

    # Use the shared keyspace
    keyspace = pytestconfig.shared_test_keyspace
    await session.set_keyspace(keyspace)

    async with TestTableManager(session, keyspace=keyspace, use_shared_keyspace=True) as manager:
        yield manager

    await session.close()


@pytest.fixture
def unique_keyspace():
    """Generate a unique keyspace name for test isolation."""
    return generate_unique_keyspace()


@pytest_asyncio.fixture(scope="function")
async def session_with_keyspace(cassandra_cluster, shared_keyspace_setup, pytestconfig):
    """Create a session with shared keyspace already set."""
    session = await cassandra_cluster.connect()
    keyspace = pytestconfig.shared_test_keyspace

    await session.set_keyspace(keyspace)

    # Track tables created for cleanup
    session._created_tables = []

    yield session, keyspace

    # Cleanup tables
    try:
        for table in getattr(session, "_created_tables", []):
            await session.execute(f"DROP TABLE IF EXISTS {table}")
    except Exception:
        pass

    try:
        await session.close()
    except Exception:
        pass
