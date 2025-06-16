"""
Pytest configuration for FastAPI example app tests.
"""

import sys
from pathlib import Path

import httpx
import pytest
import pytest_asyncio
from httpx import ASGITransport

# Add parent directories to path
sys.path.insert(0, str(Path(__file__).parent.parent))  # fastapi_app dir
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))  # project root

# Import test utils
from tests.test_utils import cleanup_keyspace, create_test_keyspace, generate_unique_keyspace


@pytest_asyncio.fixture
async def unique_test_keyspace():
    """Create a unique keyspace for each test."""
    from async_cassandra import AsyncCluster

    cluster = AsyncCluster(contact_points=["localhost"], protocol_version=5)
    session = await cluster.connect()

    # Create unique keyspace
    keyspace = generate_unique_keyspace("fastapi_test")
    await create_test_keyspace(session, keyspace)

    yield keyspace

    # Cleanup
    await cleanup_keyspace(session, keyspace)
    await session.close()
    await cluster.shutdown()


@pytest_asyncio.fixture
async def app_client(unique_test_keyspace):
    """Create test client for the FastAPI app with isolated keyspace."""
    # First, check that Cassandra is available
    from async_cassandra import AsyncCluster

    try:
        test_cluster = AsyncCluster(contact_points=["localhost"], protocol_version=5)
        test_session = await test_cluster.connect()
        await test_session.execute("SELECT now() FROM system.local")
        await test_session.close()
        await test_cluster.shutdown()
    except Exception as e:
        pytest.skip(f"Cassandra not available: {e}")

    # Set the test keyspace in environment
    import os

    os.environ["TEST_KEYSPACE"] = unique_test_keyspace

    from main import app, lifespan

    # Manually handle lifespan since httpx doesn't do it properly
    async with lifespan(app):
        transport = ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            yield client

    # Clean up environment
    os.environ.pop("TEST_KEYSPACE", None)
