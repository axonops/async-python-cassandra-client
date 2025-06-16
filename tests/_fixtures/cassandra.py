"""Cassandra test fixtures supporting both Docker and Podman.

This module provides fixtures for managing Cassandra containers
in tests, with support for both Docker and Podman runtimes.
"""

import os
import subprocess
import time
from typing import Optional

import pytest


def get_container_runtime() -> str:
    """Detect available container runtime (docker or podman)."""
    for runtime in ["docker", "podman"]:
        try:
            subprocess.run([runtime, "--version"], capture_output=True, check=True)
            return runtime
        except (subprocess.CalledProcessError, FileNotFoundError):
            continue
    raise RuntimeError("Neither docker nor podman found. Please install one.")


class CassandraContainer:
    """Manages a Cassandra container for testing."""

    def __init__(self, runtime: str = None):
        self.runtime = runtime or get_container_runtime()
        self.container_name = f"test-cassandra-{os.getpid()}"
        self.container_id: Optional[str] = None

    def start(self):
        """Start the Cassandra container."""
        # First check if port 9042 is already in use
        port_check = subprocess.run(
            [self.runtime, "ps", "--format", "{{.Names}} {{.Ports}}"],
            capture_output=True,
            text=True,
        )

        if port_check.stdout.strip():
            # Check each running container for port 9042
            for line in port_check.stdout.strip().split("\n"):
                if "9042" in line:
                    # Extract container name
                    existing_container = line.split()[0]
                    if existing_container != self.container_name:
                        print(f"Using existing Cassandra container: {existing_container}")
                        self.container_id = existing_container
                        self.container_name = existing_container
                        # Verify it's ready
                        self._wait_for_cassandra()
                        return

        # Check if our container already exists
        result = subprocess.run(
            [
                self.runtime,
                "ps",
                "-a",
                "--filter",
                f"name={self.container_name}",
                "--format",
                "{{.ID}}",
            ],
            capture_output=True,
            text=True,
        )

        if result.stdout.strip():
            # Container exists, start it
            self.container_id = result.stdout.strip()
            subprocess.run([self.runtime, "start", self.container_id], check=True)
        else:
            # Create new container
            result = subprocess.run(
                [
                    self.runtime,
                    "run",
                    "-d",
                    "--name",
                    self.container_name,
                    "-p",
                    "9042:9042",
                    "-e",
                    "CASSANDRA_CLUSTER_NAME=TestCluster",
                    "-e",
                    "CASSANDRA_DC=datacenter1",
                    "-e",
                    "CASSANDRA_ENDPOINT_SNITCH=GossipingPropertyFileSnitch",
                    "cassandra:5.0",
                ],
                capture_output=True,
                text=True,
                check=True,
            )
            self.container_id = result.stdout.strip()

        # Wait for Cassandra to be ready
        self._wait_for_cassandra()

    def stop(self):
        """Stop the Cassandra container."""
        if self.container_id:
            subprocess.run([self.runtime, "stop", self.container_id], capture_output=True)

    def remove(self):
        """Remove the Cassandra container."""
        if self.container_id:
            subprocess.run([self.runtime, "rm", "-f", self.container_id], capture_output=True)

    def _wait_for_cassandra(self, timeout: int = 60):
        """Wait for Cassandra to be ready to accept connections."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            # Use container name instead of ID for exec
            container_ref = self.container_name if self.container_name else self.container_id
            result = subprocess.run(
                [
                    self.runtime,
                    "exec",
                    container_ref,
                    "cqlsh",
                    "-e",
                    "SELECT release_version FROM system.local",
                ],
                capture_output=True,
            )
            if result.returncode == 0:
                return
            time.sleep(2)
        raise TimeoutError(f"Cassandra did not start within {timeout} seconds")

    def execute_cql(self, cql: str):
        """Execute CQL statement in the container."""
        return subprocess.run(
            [self.runtime, "exec", self.container_id, "cqlsh", "-e", cql],
            capture_output=True,
            text=True,
            check=True,
        )

    def is_running(self) -> bool:
        """Check if container is running."""
        if not self.container_id:
            return False
        result = subprocess.run(
            [self.runtime, "inspect", "-f", "{{.State.Running}}", self.container_id],
            capture_output=True,
            text=True,
        )
        return result.stdout.strip() == "true"


@pytest.fixture(scope="session")
def cassandra_container():
    """Provide a Cassandra container for the test session."""
    # First check if there's already a running container we can use
    runtime = get_container_runtime()
    port_check = subprocess.run(
        [runtime, "ps", "--format", "{{.Names}} {{.Ports}}"],
        capture_output=True,
        text=True,
    )

    if port_check.stdout.strip():
        # Check for container using port 9042
        for line in port_check.stdout.strip().split("\n"):
            if "9042" in line:
                existing_container = line.split()[0]
                print(f"Using existing Cassandra container: {existing_container}")

                container = CassandraContainer()
                container.container_name = existing_container
                container.container_id = existing_container
                container.runtime = runtime

                # Ensure test keyspace exists
                container.execute_cql(
                    """
                    CREATE KEYSPACE IF NOT EXISTS test_keyspace
                    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
                """
                )

                yield container
                # Don't stop/remove containers we didn't create
                return

    # No existing container, create new one
    container = CassandraContainer()
    container.start()

    # Create test keyspace
    container.execute_cql(
        """
        CREATE KEYSPACE IF NOT EXISTS test_keyspace
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """
    )

    yield container

    # Cleanup based on environment variable
    if os.environ.get("KEEP_CONTAINERS") != "1":
        container.stop()
        container.remove()


@pytest.fixture(scope="function")
def cassandra_session(cassandra_container):
    """Provide a Cassandra session connected to test keyspace."""
    from cassandra.cluster import Cluster

    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()
    session.set_keyspace("test_keyspace")

    yield session

    # Cleanup tables created during test
    rows = session.execute(
        """
        SELECT table_name FROM system_schema.tables
        WHERE keyspace_name = 'test_keyspace'
    """
    )
    for row in rows:
        session.execute(f"DROP TABLE IF EXISTS {row.table_name}")

    cluster.shutdown()


@pytest.fixture(scope="function")
async def async_cassandra_session(cassandra_container):
    """Provide an async Cassandra session."""
    from async_cassandra import AsyncCluster

    cluster = AsyncCluster(["127.0.0.1"])
    session = await cluster.connect()
    await session.set_keyspace("test_keyspace")

    yield session

    # Cleanup
    await session.close()
    await cluster.shutdown()
