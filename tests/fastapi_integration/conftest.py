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
fastapi_app_dir = Path(__file__).parent.parent.parent / "examples" / "fastapi_app"
sys.path.insert(0, str(fastapi_app_dir))  # fastapi_app dir
sys.path.insert(0, str(Path(__file__).parent.parent.parent))  # project root

# Import test utils
from tests.test_utils import (  # noqa: E402
    cleanup_keyspace,
    create_test_keyspace,
    generate_unique_keyspace,
)

# Note: We don't import cassandra_container here to avoid conflicts with integration tests


@pytest.fixture(scope="session")
def cassandra_container():
    """Provide access to the running Cassandra container."""
    import subprocess

    # Find running container on port 9042
    for runtime in ["podman", "docker"]:
        try:
            result = subprocess.run(
                [runtime, "ps", "--format", "{{.Names}} {{.Ports}}"],
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                for line in result.stdout.strip().split("\n"):
                    if "9042" in line:
                        container_name = line.split()[0]

                        # Create a simple container object
                        class Container:
                            def __init__(self, name, runtime_cmd):
                                self.container_name = name
                                self.runtime = runtime_cmd

                            def check_health(self):
                                # Run nodetool info
                                result = subprocess.run(
                                    [self.runtime, "exec", self.container_name, "nodetool", "info"],
                                    capture_output=True,
                                    text=True,
                                )

                                health_status = {
                                    "native_transport": False,
                                    "gossip": False,
                                    "cql_available": False,
                                }

                                if result.returncode == 0:
                                    info = result.stdout
                                    health_status["native_transport"] = (
                                        "Native Transport active: true" in info
                                    )
                                    health_status["gossip"] = (
                                        "Gossip active" in info
                                        and "true" in info.split("Gossip active")[1].split("\n")[0]
                                    )

                                    # Check CQL availability
                                    cql_result = subprocess.run(
                                        [
                                            self.runtime,
                                            "exec",
                                            self.container_name,
                                            "cqlsh",
                                            "-e",
                                            "SELECT now() FROM system.local",
                                        ],
                                        capture_output=True,
                                    )
                                    health_status["cql_available"] = cql_result.returncode == 0

                                return health_status

                        return Container(container_name, runtime)
        except Exception:
            pass

    pytest.fail("No Cassandra container found running on port 9042")


@pytest_asyncio.fixture
async def unique_test_keyspace(cassandra_container):  # noqa: F811
    """Create a unique keyspace for each test."""
    from async_cassandra import AsyncCluster

    # Check health before proceeding
    health = cassandra_container.check_health()
    if not health["native_transport"] or not health["cql_available"]:
        pytest.fail(f"Cassandra not healthy: {health}")

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
        pytest.fail(f"Cassandra not available: {e}")

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


@pytest.fixture(scope="function", autouse=True)
async def ensure_cassandra_healthy_fastapi(cassandra_container):
    """Ensure Cassandra is healthy before each FastAPI test."""
    # Check health before test
    health = cassandra_container.check_health()
    if not health["native_transport"] or not health["cql_available"]:
        # Try to wait a bit and check again
        import asyncio

        await asyncio.sleep(2)
        health = cassandra_container.check_health()
        if not health["native_transport"] or not health["cql_available"]:
            pytest.fail(f"Cassandra not healthy before test: {health}")

    yield

    # Optional: Check health after test
    health = cassandra_container.check_health()
    if not health["native_transport"]:
        print(f"Warning: Cassandra health degraded after test: {health}")
