"""
Shared fixtures for integration tests.

Provides Cassandra connection, session management, and test data utilities.
"""

import asyncio
import os
import uuid
from collections.abc import AsyncGenerator, Generator

import pytest
import pytest_asyncio
from async_cassandra import AsyncCluster


@pytest.fixture(scope="session")
def event_loop() -> Generator:
    """Create event loop for session scope."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def cassandra_host() -> str:
    """Get Cassandra host from environment or default."""
    return os.environ.get("CASSANDRA_HOST", "localhost")


@pytest.fixture(scope="session")
def cassandra_port() -> int:
    """Get Cassandra port from environment or default."""
    return int(os.environ.get("CASSANDRA_PORT", "9042"))


@pytest.fixture(scope="session")
def dask_scheduler() -> str:
    """Get Dask scheduler address from environment."""
    return os.environ.get("DASK_SCHEDULER", "tcp://localhost:8786")


@pytest_asyncio.fixture(scope="session")
async def async_cluster(cassandra_host: str, cassandra_port: int) -> AsyncGenerator:
    """Create async cluster for session scope."""
    cluster = AsyncCluster(
        contact_points=[cassandra_host],
        port=cassandra_port,
        protocol_version=5,
    )
    yield cluster
    await cluster.shutdown()


@pytest_asyncio.fixture(scope="session")
async def session(async_cluster: AsyncCluster) -> AsyncGenerator:
    """Create session with test keyspace."""
    session = await async_cluster.connect()

    # Create test keyspace
    await session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS test_dataframe
        WITH replication = {
            'class': 'SimpleStrategy',
            'replication_factor': 1
        }
        """
    )

    # Use test keyspace
    await session.set_keyspace("test_dataframe")

    yield session

    # Cleanup is handled by cluster shutdown


@pytest.fixture
def test_table_name() -> str:
    """Generate unique table name for each test."""
    return f"test_{uuid.uuid4().hex[:8]}"


@pytest_asyncio.fixture
async def basic_test_table(session, test_table_name: str) -> AsyncGenerator[str, None]:
    """Create a basic test table with various data types."""
    table_name = test_table_name

    # Create table with common data types
    await session.execute(
        f"""
        CREATE TABLE {table_name} (
            id INT,
            name TEXT,
            value DOUBLE,
            created_at TIMESTAMP,
            is_active BOOLEAN,
            PRIMARY KEY (id)
        )
        """
    )

    # Insert test data
    insert_stmt = await session.prepare(
        f"""
        INSERT INTO {table_name} (id, name, value, created_at, is_active)
        VALUES (?, ?, ?, ?, ?)
        """
    )

    # Insert 1000 rows for testing
    from datetime import datetime

    for i in range(1000):
        await session.execute(
            insert_stmt,
            (
                i,
                f"name_{i}",
                float(i * 1.5),
                datetime(2024, 1, (i % 28) + 1, 12, 0, 0),
                i % 2 == 0,
            ),
        )

    yield f"test_dataframe.{table_name}"

    # Cleanup
    await session.execute(f"DROP TABLE IF EXISTS {table_name}")


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

    yield f"test_dataframe.{table_name}"

    await session.execute(f"DROP TABLE IF EXISTS {table_name}")


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

    yield f"test_dataframe.{table_name}"

    await session.execute(f"DROP TABLE IF EXISTS {table_name}")


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

    yield f"test_dataframe.{table_name}"

    await session.execute(f"DROP TABLE IF EXISTS {table_name}")


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

    yield f"test_dataframe.{table_name}"

    await session.execute(f"DROP TABLE IF EXISTS {table_name}")
