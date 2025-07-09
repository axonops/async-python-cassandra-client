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
        self.container_name = "async-cassandra-test"
        self.container_id: Optional[str] = None

    def start(self):
        """Start the Cassandra container."""
        # Stop and remove any existing container with our name
        print(f"Cleaning up any existing container named {self.container_name}...")
        subprocess.run(
            [self.runtime, "stop", self.container_name],
            capture_output=True,
            stderr=subprocess.DEVNULL,
        )
        subprocess.run(
            [self.runtime, "rm", "-f", self.container_name],
            capture_output=True,
            stderr=subprocess.DEVNULL,
        )

        # Create new container with proper resources
        print(f"Starting fresh Cassandra container: {self.container_name}")
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
                "-e",
                "HEAP_NEWSIZE=512M",
                "-e",
                "MAX_HEAP_SIZE=3G",
                "-e",
                "JVM_OPTS=-XX:+UseG1GC -XX:G1RSetUpdatingPauseTimePercent=5 -XX:MaxGCPauseMillis=300",
                "--memory=4g",
                "--memory-swap=4g",
                "cassandra:5",
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
        if self.container_id or self.container_name:
            container_ref = self.container_id or self.container_name
            subprocess.run([self.runtime, "stop", container_ref], capture_output=True)

    def remove(self):
        """Remove the Cassandra container."""
        if self.container_id or self.container_name:
            container_ref = self.container_id or self.container_name
            subprocess.run([self.runtime, "rm", "-f", container_ref], capture_output=True)

    def _wait_for_cassandra(self, timeout: int = 90):
        """Wait for Cassandra to be ready to accept connections."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            # Use container name instead of ID for exec
            container_ref = self.container_name if self.container_name else self.container_id

            # First check if native transport is active
            health_result = subprocess.run(
                [
                    self.runtime,
                    "exec",
                    container_ref,
                    "nodetool",
                    "info",
                ],
                capture_output=True,
                text=True,
            )

            if (
                health_result.returncode == 0
                and "Native Transport active: true" in health_result.stdout
            ):
                # Now check if CQL is responsive
                cql_result = subprocess.run(
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
                if cql_result.returncode == 0:
                    return
            time.sleep(3)
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

    def check_health(self) -> dict:
        """Check Cassandra health using nodetool info."""
        if not self.container_id:
            return {
                "native_transport": False,
                "gossip": False,
                "cql_available": False,
            }

        container_ref = self.container_name if self.container_name else self.container_id

        # Run nodetool info
        result = subprocess.run(
            [
                self.runtime,
                "exec",
                container_ref,
                "nodetool",
                "info",
            ],
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
            health_status["native_transport"] = "Native Transport active: true" in info
            health_status["gossip"] = (
                "Gossip active" in info and "true" in info.split("Gossip active")[1].split("\n")[0]
            )

            # Check CQL availability
            cql_result = subprocess.run(
                [
                    self.runtime,
                    "exec",
                    container_ref,
                    "cqlsh",
                    "-e",
                    "SELECT now() FROM system.local",
                ],
                capture_output=True,
            )
            health_status["cql_available"] = cql_result.returncode == 0

        return health_status


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
