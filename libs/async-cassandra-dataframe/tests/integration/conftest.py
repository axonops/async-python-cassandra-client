"""
Integration test configuration and shared fixtures.

CRITICAL: Integration tests require a real Cassandra instance.
NO MOCKS ALLOWED in integration tests - they must test against real Cassandra.
"""

import os
import socket
from collections.abc import AsyncGenerator
from datetime import UTC

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


@pytest_asyncio.fixture(scope="function")
async def basic_test_table(session, test_table_name):
    """Create a basic test table with sample data for integration tests."""
    from datetime import datetime

    # Create table
    await session.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {test_table_name} (
            id INT PRIMARY KEY,
            name TEXT,
            value DOUBLE,
            created_at TIMESTAMP,
            is_active BOOLEAN
        )
    """
    )

    # Track for cleanup
    session._created_tables.append(test_table_name)

    # Insert sample data
    insert_stmt = await session.prepare(
        f"""
        INSERT INTO {test_table_name} (id, name, value, created_at, is_active)
        VALUES (?, ?, ?, ?, ?)
    """
    )

    # Insert 1000 rows
    for i in range(1000):
        await session.execute(
            insert_stmt, (i, f"name_{i}", float(i), datetime.now(UTC), i % 2 == 0)
        )

    return test_table_name


@pytest_asyncio.fixture
async def all_types_table(session, test_table_name: str) -> AsyncGenerator[str, None]:
    """
    Create table with ALL Cassandra data types for comprehensive testing.

    CRITICAL: Tests type mapping, NULL handling, and serialization.
    """
    table_name = test_table_name

    await session.execute(
        f"""
        CREATE TABLE {table_name} (
            -- Primary key
            id INT PRIMARY KEY,

            -- String types
            ascii_col ASCII,
            text_col TEXT,
            varchar_col VARCHAR,

            -- Numeric types
            tinyint_col TINYINT,
            smallint_col SMALLINT,
            int_col INT,
            bigint_col BIGINT,
            varint_col VARINT,
            float_col FLOAT,
            double_col DOUBLE,
            decimal_col DECIMAL,

            -- Temporal types
            date_col DATE,
            time_col TIME,
            timestamp_col TIMESTAMP,
            duration_col DURATION,

            -- Binary
            blob_col BLOB,

            -- Other types
            boolean_col BOOLEAN,
            inet_col INET,
            uuid_col UUID,
            timeuuid_col TIMEUUID,

            -- Collection types
            list_col LIST<TEXT>,
            set_col SET<INT>,
            map_col MAP<TEXT, INT>,

            -- Counter (special table needed)
            -- counter_col COUNTER,

            -- Tuple
            tuple_col TUPLE<TEXT, INT, BOOLEAN>
        )
        """
    )

    # Track for cleanup
    session._created_tables.append(table_name)

    yield f"test_dataframe.{table_name}"


@pytest_asyncio.fixture
async def wide_table(session, test_table_name: str) -> AsyncGenerator[str, None]:
    """Create a wide table with many columns for testing."""
    table_name = test_table_name

    # Create table with 100 columns
    columns = ["id INT PRIMARY KEY"]
    for i in range(99):
        columns.append(f"col_{i} TEXT")

    create_stmt = f"CREATE TABLE {table_name} ({', '.join(columns)})"
    await session.execute(create_stmt)

    # Track for cleanup
    session._created_tables.append(table_name)

    yield f"test_dataframe.{table_name}"


@pytest_asyncio.fixture
async def large_rows_table(session, test_table_name: str) -> AsyncGenerator[str, None]:
    """Create table with large rows (BLOBs) for memory testing."""
    table_name = test_table_name

    await session.execute(
        f"""
        CREATE TABLE {table_name} (
            id INT PRIMARY KEY,
            large_data BLOB,
            metadata TEXT
        )
        """
    )

    # Insert rows with 1MB blobs
    large_data = b"x" * (1024 * 1024)  # 1MB
    insert_stmt = await session.prepare(
        f"INSERT INTO {table_name} (id, large_data, metadata) VALUES (?, ?, ?)"
    )

    for i in range(10):
        await session.execute(insert_stmt, (i, large_data, f"metadata_{i}"))

    # Track for cleanup
    session._created_tables.append(table_name)

    yield f"test_dataframe.{table_name}"


@pytest_asyncio.fixture
async def sparse_table(session, test_table_name: str) -> AsyncGenerator[str, None]:
    """Create table with sparse data (many NULLs)."""
    table_name = test_table_name

    await session.execute(
        f"""
        CREATE TABLE {table_name} (
            id INT PRIMARY KEY,
            col1 TEXT,
            col2 TEXT,
            col3 TEXT,
            col4 TEXT,
            col5 TEXT
        )
        """
    )

    # Insert sparse data - most columns NULL
    for i in range(1000):
        # Only populate 1-2 columns besides ID
        if i % 5 == 0:
            await session.execute(f"INSERT INTO {table_name} (id, col1) VALUES ({i}, 'value_{i}')")
        elif i % 3 == 0:
            await session.execute(
                f"INSERT INTO {table_name} (id, col2, col3) VALUES ({i}, 'val2_{i}', 'val3_{i}')"
            )
        else:
            await session.execute(f"INSERT INTO {table_name} (id) VALUES ({i})")

    # Track for cleanup
    session._created_tables.append(table_name)

    yield f"test_dataframe.{table_name}"


# For unit tests that don't need Cassandra
@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    import asyncio

    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()
