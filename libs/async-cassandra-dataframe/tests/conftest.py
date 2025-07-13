"""
Pytest configuration and shared fixtures for all tests.

Follows the same pattern as async-cassandra for consistency.
"""

import os
import socket

import pytest
import pytest_asyncio
from async_cassandra import AsyncCluster


def pytest_configure(config):
    """Configure pytest for dataframe tests."""
    # Skip if explicitly disabled
    if os.environ.get("SKIP_INTEGRATION_TESTS", "").lower() in ("1", "true", "yes"):
        pytest.exit("Skipping integration tests (SKIP_INTEGRATION_TESTS is set)", 0)

    # Store shared keyspace name
    config.shared_test_keyspace = "test_dataframe"

    # Get contact points from environment
    # Force IPv4 by replacing localhost with 127.0.0.1
    contact_points = os.environ.get("CASSANDRA_CONTACT_POINTS", "127.0.0.1").split(",")
    config.cassandra_contact_points = [
        "127.0.0.1" if cp.strip() == "localhost" else cp.strip() for cp in contact_points
    ]

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
async def async_cluster(pytestconfig):
    """Create a shared cluster for all integration tests."""
    cluster = AsyncCluster(
        contact_points=pytestconfig.cassandra_contact_points,
        protocol_version=5,
        connect_timeout=10.0,
    )
    yield cluster
    await cluster.shutdown()


@pytest_asyncio.fixture(scope="session")
async def shared_keyspace(async_cluster, pytestconfig):
    """Create shared keyspace for all integration tests."""
    session = await async_cluster.connect()

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


@pytest_asyncio.fixture(scope="function")
async def session(async_cluster, shared_keyspace):
    """Create an async Cassandra session using shared keyspace."""
    session = await async_cluster.connect()

    # Use the shared keyspace
    await session.set_keyspace(shared_keyspace)

    # Track tables created for this test
    session._created_tables = []

    yield session

    # Cleanup tables after test
    try:
        for table in getattr(session, "_created_tables", []):
            await session.execute(f"DROP TABLE IF EXISTS {table}")
    except Exception:
        pass


@pytest.fixture
def test_table_name():
    """Generate a unique table name for each test."""
    import random
    import string

    suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=8))
    return f"test_table_{suffix}"


# For unit tests that don't need Cassandra
@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    import asyncio

    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()
