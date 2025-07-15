"""
Integration test configuration and fixtures.

Provides real Cassandra cluster setup for testing bulk operations
with actual database interactions.
"""

import asyncio
import os
import socket
import time
from typing import AsyncGenerator

import pytest
import pytest_asyncio
from async_cassandra import AsyncCassandraSession


def pytest_configure(config):
    """Configure pytest for integration tests."""
    # Skip if explicitly disabled
    if os.environ.get("SKIP_INTEGRATION_TESTS", "").lower() in ("1", "true", "yes"):
        pytest.exit("Skipping integration tests (SKIP_INTEGRATION_TESTS is set)", 0)

    # Get contact points from environment
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


@pytest.fixture(scope="session")
def event_loop():
    """Create event loop for async tests."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def cassandra_host(pytestconfig) -> str:
    """Get Cassandra host for connections."""
    return pytestconfig.cassandra_contact_points[0]


@pytest.fixture(scope="session")
def cassandra_port() -> int:
    """Get Cassandra port for connections."""
    return int(os.environ.get("CASSANDRA_PORT", "9042"))


@pytest_asyncio.fixture(scope="session")
async def cluster(pytestconfig):
    """Create async cluster for tests."""
    from async_cassandra import AsyncCluster

    cluster = AsyncCluster(
        contact_points=pytestconfig.cassandra_contact_points,
        port=int(os.environ.get("CASSANDRA_PORT", "9042")),
        connect_timeout=10.0,
    )
    yield cluster
    await cluster.shutdown()


@pytest_asyncio.fixture(scope="session")
async def session(cluster) -> AsyncGenerator[AsyncCassandraSession, None]:
    """Create async session with test keyspace."""
    session = await cluster.connect()

    # Create test keyspace
    await session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS test_bulk
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """
    )

    await session.set_keyspace("test_bulk")

    yield session

    # Cleanup
    await session.execute("DROP KEYSPACE IF EXISTS test_bulk")
    await session.close()


@pytest_asyncio.fixture
async def test_table(session: AsyncCassandraSession):
    """
    Create test table for each test.

    Provides a fresh table with sample schema for testing
    bulk operations. Table is dropped after test.
    """
    table_name = f"test_table_{int(time.time() * 1000)}"

    # Create table with various data types
    await session.execute(
        f"""
        CREATE TABLE {table_name} (
            id uuid PRIMARY KEY,
            name text,
            age int,
            active boolean,
            score double,
            created_at timestamp,
            metadata map<text, text>,
            tags set<text>
        )
    """
    )

    yield table_name

    # Cleanup
    await session.execute(f"DROP TABLE IF EXISTS {table_name}")


@pytest_asyncio.fixture
async def populated_table(session: AsyncCassandraSession, test_table: str):
    """
    Create and populate test table with sample data.

    Inserts 1000 rows with various data types for testing
    export operations at scale.
    """
    from datetime import datetime, timezone
    from uuid import uuid4

    # Prepare insert statement
    insert_stmt = await session.prepare(
        f"""
        INSERT INTO {test_table}
        (id, name, age, active, score, created_at, metadata, tags)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
    )

    # Insert test data
    for i in range(1000):
        await session.execute(
            insert_stmt,
            (
                uuid4(),
                f"User {i}",
                20 + (i % 50),
                i % 2 == 0,
                i * 0.5,
                datetime.now(timezone.utc),
                {"key": f"value{i}", "index": str(i)},
                {f"tag{i % 5}", f"group{i % 10}"},
            ),
        )

    return test_table
