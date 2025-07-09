"""Pytest configuration for BDD tests."""

import asyncio
import sys
from pathlib import Path

import pytest

from tests._fixtures.cassandra import cassandra_container  # noqa: F401

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Import test utils for isolation
sys.path.insert(0, str(Path(__file__).parent.parent))
from test_utils import (  # noqa: E402
    cleanup_keyspace,
    create_test_keyspace,
    generate_unique_keyspace,
    get_test_timeout,
)


@pytest.fixture(scope="session")
def event_loop():
    """Create an event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def anyio_backend():
    """Use asyncio backend for async tests."""
    return "asyncio"


@pytest.fixture
def connection_parameters():
    """Provide connection parameters for BDD tests."""
    return {"contact_points": ["127.0.0.1"], "port": 9042}


@pytest.fixture
def driver_configured():
    """Provide driver configuration for BDD tests."""
    return {"contact_points": ["127.0.0.1"], "port": 9042, "thread_pool_max_workers": 32}


@pytest.fixture
def cassandra_cluster_running(cassandra_container):  # noqa: F811
    """Ensure Cassandra container is running and healthy."""
    assert cassandra_container.is_running()

    # Check health before proceeding
    health = cassandra_container.check_health()
    if not health["native_transport"] or not health["cql_available"]:
        pytest.fail(f"Cassandra not healthy: {health}")

    return cassandra_container


@pytest.fixture
async def cassandra_cluster(cassandra_container):  # noqa: F811
    """Provide an async Cassandra cluster for BDD tests."""
    from async_cassandra import AsyncCluster

    # Ensure Cassandra is healthy before creating cluster
    health = cassandra_container.check_health()
    if not health["native_transport"] or not health["cql_available"]:
        pytest.fail(f"Cassandra not healthy: {health}")

    cluster = AsyncCluster(["127.0.0.1"], protocol_version=5)
    yield cluster
    await cluster.shutdown()
    # Give extra time for driver's internal threads to fully stop
    # This prevents "cannot schedule new futures after shutdown" errors
    await asyncio.sleep(2)


@pytest.fixture
async def isolated_session(cassandra_cluster):
    """Provide an isolated session with unique keyspace for BDD tests."""
    session = await cassandra_cluster.connect()

    # Create unique keyspace for this test
    keyspace = generate_unique_keyspace("test_bdd")
    await create_test_keyspace(session, keyspace)
    await session.set_keyspace(keyspace)

    yield session

    # Cleanup
    await cleanup_keyspace(session, keyspace)
    await session.close()
    # Give time for session cleanup
    await asyncio.sleep(1)


@pytest.fixture
def test_context():
    """Shared context for BDD tests with isolation helpers."""
    return {
        "keyspaces_created": [],
        "tables_created": [],
        "get_unique_keyspace": lambda: generate_unique_keyspace("bdd"),
        "get_test_timeout": get_test_timeout,
    }


@pytest.fixture
def bdd_test_timeout():
    """Get appropriate timeout for BDD tests."""
    return get_test_timeout(10.0)


# BDD-specific configuration
def pytest_bdd_step_error(request, feature, scenario, step, step_func, step_func_args, exception):
    """Enhanced error reporting for BDD steps."""
    print(f"\n{'='*60}")
    print(f"STEP FAILED: {step.keyword} {step.name}")
    print(f"Feature: {feature.name}")
    print(f"Scenario: {scenario.name}")
    print(f"Error: {exception}")
    print(f"{'='*60}\n")


# Markers for BDD tests
def pytest_configure(config):
    """Configure custom markers for BDD tests."""
    config.addinivalue_line("markers", "bdd: mark test as BDD test")
    config.addinivalue_line("markers", "critical: mark test as critical for production")
    config.addinivalue_line("markers", "concurrency: mark test as concurrency test")
    config.addinivalue_line("markers", "performance: mark test as performance test")
    config.addinivalue_line("markers", "memory: mark test as memory test")
    config.addinivalue_line("markers", "fastapi: mark test as FastAPI integration test")
    config.addinivalue_line("markers", "startup_shutdown: mark test as startup/shutdown test")
    config.addinivalue_line(
        "markers", "dependency_injection: mark test as dependency injection test"
    )
    config.addinivalue_line("markers", "streaming: mark test as streaming test")
    config.addinivalue_line("markers", "pagination: mark test as pagination test")
    config.addinivalue_line("markers", "caching: mark test as caching test")
    config.addinivalue_line("markers", "prepared_statements: mark test as prepared statements test")
    config.addinivalue_line("markers", "monitoring: mark test as monitoring test")
    config.addinivalue_line("markers", "connection_reuse: mark test as connection reuse test")
    config.addinivalue_line("markers", "background_tasks: mark test as background tasks test")
    config.addinivalue_line("markers", "graceful_shutdown: mark test as graceful shutdown test")
    config.addinivalue_line("markers", "middleware: mark test as middleware test")
    config.addinivalue_line("markers", "connection_failure: mark test as connection failure test")
    config.addinivalue_line("markers", "websocket: mark test as websocket test")
    config.addinivalue_line("markers", "memory_pressure: mark test as memory pressure test")
    config.addinivalue_line("markers", "auth: mark test as authentication test")
    config.addinivalue_line("markers", "error_handling: mark test as error handling test")


@pytest.fixture(scope="function", autouse=True)
async def ensure_cassandra_healthy_bdd(cassandra_container):  # noqa: F811
    """Ensure Cassandra is healthy before each BDD test."""
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


# Automatically mark all BDD tests
def pytest_collection_modifyitems(items):
    """Automatically add markers to BDD tests."""
    for item in items:
        # Mark all tests in bdd directory
        if "bdd" in str(item.fspath):
            item.add_marker(pytest.mark.bdd)

        # Add markers based on tags in feature files
        if hasattr(item, "scenario"):
            for tag in item.scenario.tags:
                # Remove @ and convert hyphens to underscores
                marker_name = tag.lstrip("@").replace("-", "_")
                if hasattr(pytest.mark, marker_name):
                    marker = getattr(pytest.mark, marker_name)
                    item.add_marker(marker)
