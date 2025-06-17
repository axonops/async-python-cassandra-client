"""Test utilities for isolating tests and managing test resources."""

import asyncio
import uuid
from typing import Optional, Set

# Track created keyspaces for cleanup
_created_keyspaces: Set[str] = set()


def generate_unique_keyspace(prefix: str = "test") -> str:
    """Generate a unique keyspace name for test isolation."""
    unique_id = str(uuid.uuid4()).replace("-", "")[:8]
    keyspace = f"{prefix}_{unique_id}"
    _created_keyspaces.add(keyspace)
    return keyspace


def generate_unique_table(prefix: str = "table") -> str:
    """Generate a unique table name for test isolation."""
    unique_id = str(uuid.uuid4()).replace("-", "")[:8]
    return f"{prefix}_{unique_id}"


async def create_test_table(
    session, table_name: Optional[str] = None, schema: str = "(id int PRIMARY KEY, data text)"
) -> str:
    """Create a test table with the given schema and register it for cleanup."""
    if table_name is None:
        table_name = generate_unique_table()

    await session.execute(f"CREATE TABLE IF NOT EXISTS {table_name} {schema}")

    # Register table for cleanup if session tracks created tables
    if hasattr(session, "_created_tables"):
        session._created_tables.append(table_name)

    return table_name


async def create_test_keyspace(session, keyspace: Optional[str] = None) -> str:
    """Create a test keyspace with proper replication."""
    if keyspace is None:
        keyspace = generate_unique_keyspace()

    await session.execute(
        f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
    """
    )
    return keyspace


async def cleanup_keyspace(session, keyspace: str) -> None:
    """Clean up a test keyspace."""
    try:
        await session.execute(f"DROP KEYSPACE IF EXISTS {keyspace}")
        _created_keyspaces.discard(keyspace)
    except Exception:
        # Ignore cleanup errors
        pass


async def cleanup_all_test_keyspaces(session) -> None:
    """Clean up all tracked test keyspaces."""
    for keyspace in list(_created_keyspaces):
        await cleanup_keyspace(session, keyspace)


def get_test_timeout(base_timeout: float = 5.0) -> float:
    """Get appropriate timeout for tests based on environment."""
    # Increase timeout in CI environments or when running under coverage
    import os

    if os.environ.get("CI") or os.environ.get("COVERAGE_RUN"):
        return base_timeout * 3
    return base_timeout


async def wait_for_schema_agreement(session, timeout: float = 10.0) -> None:
    """Wait for schema agreement across the cluster."""
    start_time = asyncio.get_event_loop().time()
    while asyncio.get_event_loop().time() - start_time < timeout:
        try:
            result = await session.execute("SELECT schema_version FROM system.local")
            if result:
                return
        except Exception:
            pass
        await asyncio.sleep(0.1)


async def ensure_keyspace_exists(session, keyspace: str) -> None:
    """Ensure a keyspace exists before using it."""
    await session.execute(
        f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
    """
    )
    await wait_for_schema_agreement(session)


async def ensure_table_exists(session, keyspace: str, table: str, schema: str) -> None:
    """Ensure a table exists with the given schema."""
    await ensure_keyspace_exists(session, keyspace)
    await session.execute(f"USE {keyspace}")
    await session.execute(f"CREATE TABLE IF NOT EXISTS {table} {schema}")
    await wait_for_schema_agreement(session)


def get_container_timeout() -> int:
    """Get timeout for container operations."""
    import os

    # Longer timeout in CI environments
    if os.environ.get("CI"):
        return 120
    return 60


async def run_with_timeout(coro, timeout: float):
    """Run a coroutine with a timeout."""
    try:
        return await asyncio.wait_for(coro, timeout=timeout)
    except asyncio.TimeoutError:
        raise TimeoutError(f"Operation timed out after {timeout} seconds")


class TestTableManager:
    """Context manager for creating and cleaning up test tables."""

    def __init__(self, session, keyspace: Optional[str] = None, use_shared_keyspace: bool = False):
        self.session = session
        self.keyspace = keyspace or generate_unique_keyspace()
        self.tables = []
        self.use_shared_keyspace = use_shared_keyspace

    async def __aenter__(self):
        if not self.use_shared_keyspace:
            await create_test_keyspace(self.session, self.keyspace)
            await self.session.execute(f"USE {self.keyspace}")
        # If using shared keyspace, assume it's already set on the session
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # Clean up tables
        for table in self.tables:
            try:
                await self.session.execute(f"DROP TABLE IF EXISTS {table}")
            except Exception:
                pass

        # Only clean up keyspace if we created it
        if not self.use_shared_keyspace:
            try:
                await cleanup_keyspace(self.session, self.keyspace)
            except Exception:
                pass

    async def create_table(
        self, table_name: Optional[str] = None, schema: str = "(id int PRIMARY KEY, data text)"
    ) -> str:
        """Create a test table with the given schema."""
        if table_name is None:
            table_name = generate_unique_table()

        await self.session.execute(f"CREATE TABLE IF NOT EXISTS {table_name} {schema}")
        self.tables.append(table_name)
        return table_name
