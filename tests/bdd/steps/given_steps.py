"""Given step definitions for BDD tests."""

import subprocess
import time

from pytest_bdd import given, parsers

from async_cassandra import AsyncCassandraSession as AsyncSession
from async_cassandra import AsyncCluster


@given("a Cassandra cluster is running")
def cassandra_cluster_running(cassandra_container):
    """Ensure Cassandra container is running."""
    assert cassandra_container.is_running()
    return cassandra_container


@given("the async-cassandra driver is configured")
def driver_configured():
    """Ensure driver is properly configured."""
    return {"contact_points": ["127.0.0.1"], "port": 9042, "thread_pool_max_workers": 32}


@given("I have connection parameters for the local cluster")
def connection_parameters():
    """Provide connection parameters."""
    return {"contact_points": ["127.0.0.1"], "port": 9042}


@given(parsers.parse("I need to handle {count:d} concurrent requests"))
def concurrent_request_count(count):
    """Set up concurrent request count."""
    return count


@given("the Cassandra cluster is unavailable")
def cassandra_unavailable(cassandra_container):
    """Make Cassandra unavailable."""
    cassandra_container.stop()
    return cassandra_container


@given(parsers.parse("I configure a connection pool with {pool_size:d} connections"))
def configure_pool_size(pool_size):
    """Configure connection pool size."""
    return {"thread_pool_max_workers": pool_size}


@given("I have an active async session")
async def active_async_session(cassandra_cluster_running, driver_configured):
    """Provide an active async session."""
    cluster = AsyncCluster(**driver_configured)
    session = await cluster.connect()
    yield session
    await session.close()
    await cluster.shutdown()


@given(parsers.parse('a test keyspace "{keyspace_name}" exists'))
async def test_keyspace_exists(active_async_session, keyspace_name):
    """Ensure test keyspace exists."""
    await active_async_session.execute(
        f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace_name}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
    """
    )
    await active_async_session.set_keyspace(keyspace_name)
    return keyspace_name


@given(parsers.parse('a table "{table_name}" with sample data'))
async def table_with_sample_data(active_async_session, table_name):
    """Create table with sample data."""
    # Create table
    await active_async_session.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id int PRIMARY KEY,
            name text,
            email text,
            created_at timestamp
        )
    """
    )

    # Insert sample data
    for i in range(1, 6):
        await active_async_session.execute(
            f"INSERT INTO {table_name} (id, name, email, created_at) VALUES (?, ?, ?, toTimestamp(now()))",
            [i, f"User {i}", f"user{i}@example.com"],
        )

    # Special test user
    await active_async_session.execute(
        f"INSERT INTO {table_name} (id, name, email, created_at) VALUES (?, ?, ?, toTimestamp(now()))",
        [123, "Test User", "test@example.com"],
    )

    return table_name


@given(parsers.parse('a table "{table_name}" exists'))
async def table_exists(active_async_session, table_name):
    """Create an empty table."""
    await active_async_session.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id int PRIMARY KEY,
            name text,
            data text,
            total decimal
        )
    """
    )
    return table_name


@given("a FastAPI application with async-cassandra")
def fastapi_app_with_cassandra():
    """Create a FastAPI app with async-cassandra integration."""
    from contextlib import asynccontextmanager

    from fastapi import Depends, FastAPI

    # Global cluster instance
    cluster = None

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Startup
        global cluster
        cluster = AsyncCluster()
        yield
        # Shutdown
        if cluster:
            await cluster.shutdown()

    app = FastAPI(lifespan=lifespan)

    async def get_session():
        """Dependency to get Cassandra session."""
        session = await cluster.connect()
        try:
            yield session
        finally:
            await session.close()

    @app.get("/users/{user_id}")
    async def get_user(user_id: int, session: AsyncSession = Depends(get_session)):
        result = await session.execute("SELECT * FROM users WHERE id = ?", [user_id])
        row = await result.one()
        if row:
            return {"id": row.id, "name": row.name, "email": row.email}
        return {"error": "User not found"}

    return app


@given("a running Cassandra cluster with test data")
def cassandra_with_test_data(cassandra_cluster_running):
    """Ensure Cassandra has test data."""
    container = cassandra_cluster_running

    # Create test keyspace and tables
    container.execute_cql(
        """
        CREATE KEYSPACE IF NOT EXISTS test_app
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """
    )

    container.execute_cql(
        """
        CREATE TABLE IF NOT EXISTS test_app.users (
            id int PRIMARY KEY,
            name text,
            email text
        )
    """
    )

    # Insert test data
    for i in range(1, 11):
        container.execute_cql(
            f"""
            INSERT INTO test_app.users (id, name, email)
            VALUES ({i}, 'User {i}', 'user{i}@example.com')
        """
        )

    return container


@given("the FastAPI test client is initialized")
def fastapi_test_client(fastapi_app_with_cassandra):
    """Initialize FastAPI test client."""
    from fastapi.testclient import TestClient

    return TestClient(fastapi_app_with_cassandra)


@given("a production-like Cassandra cluster with 3 nodes")
def production_cassandra_cluster():
    """Mock a 3-node Cassandra cluster."""
    # In real tests, this would set up a multi-node cluster
    # For now, we'll mock it
    return {
        "nodes": ["node1", "node2", "node3"],
        "replication_factor": 3,
        "consistency_level": "QUORUM",
    }


@given("async-cassandra configured with production settings")
def production_async_config():
    """Production configuration for async-cassandra."""
    return {
        "thread_pool_max_workers": 50,
        "default_timeout": 10.0,
        "retry_policy": "production",
        "connection_pool_size": 20,
    }


@given("comprehensive monitoring is enabled")
def monitoring_enabled():
    """Enable comprehensive monitoring."""
    from async_cassandra.monitoring import MetricsCollector

    return MetricsCollector()


@given(parsers.parse("a Cassandra {version} cluster is running on port {port:d}"))
def cassandra_version_cluster_running(version, port, context):
    """Start a specific version of Cassandra on a given port."""
    # Check if docker or podman is available
    docker_cmd = None
    try:
        subprocess.run(["docker", "--version"], capture_output=True, check=True)
        docker_cmd = "docker"
    except (subprocess.CalledProcessError, FileNotFoundError):
        try:
            subprocess.run(["podman", "--version"], capture_output=True, check=True)
            docker_cmd = "podman"
        except (subprocess.CalledProcessError, FileNotFoundError):
            raise RuntimeError("Neither docker nor podman is available")

    # Map version to specific Cassandra image
    version_map = {"3.11": "cassandra:3.11", "5.0": "cassandra:5.0"}

    image = version_map.get(version)
    if not image:
        raise ValueError(f"Unsupported Cassandra version: {version}")

    container_name = f"cassandra-{version.replace('.', '')}-test-{port}"

    # Stop any existing container with this name
    subprocess.run([docker_cmd, "stop", container_name], capture_output=True)
    subprocess.run([docker_cmd, "rm", container_name], capture_output=True)

    # Start the container
    cmd = [
        docker_cmd,
        "run",
        "-d",
        "--name",
        container_name,
        "-p",
        f"{port}:9042",
        "-e",
        "CASSANDRA_CLUSTER_NAME=TestCluster",
        "-e",
        "CASSANDRA_DC=dc1",
        "-e",
        "CASSANDRA_ENDPOINT_SNITCH=SimpleSnitch",
        image,
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"Failed to start Cassandra {version}: {result.stderr}")

    # Store container info in context for cleanup
    if not hasattr(context, "containers"):
        context.containers = []
    context.containers.append((docker_cmd, container_name))

    # Wait for Cassandra to be ready
    max_attempts = 60  # 60 seconds timeout
    for attempt in range(max_attempts):
        try:
            # Try to connect using cqlsh to check if Cassandra is ready
            check_cmd = [docker_cmd, "exec", container_name, "cqlsh", "-e", "DESCRIBE CLUSTER"]
            result = subprocess.run(check_cmd, capture_output=True, text=True)
            if result.returncode == 0:
                break
        except Exception:
            pass
        time.sleep(1)
    else:
        raise RuntimeError(f"Cassandra {version} failed to start within 60 seconds")

    return {"version": version, "port": port, "container": container_name}
