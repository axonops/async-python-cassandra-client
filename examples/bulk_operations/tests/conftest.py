"""
Pytest configuration for bulk operations tests.

Handles test markers and Docker/Podman support.
"""

import os
import subprocess
from pathlib import Path

import pytest


def get_container_runtime():
    """Detect whether to use docker or podman."""
    # Check environment variable first
    runtime = os.environ.get("CONTAINER_RUNTIME", "").lower()
    if runtime in ["docker", "podman"]:
        return runtime

    # Auto-detect
    for cmd in ["docker", "podman"]:
        try:
            subprocess.run([cmd, "--version"], capture_output=True, check=True)
            return cmd
        except (subprocess.CalledProcessError, FileNotFoundError):
            continue

    raise RuntimeError("Neither docker nor podman found. Please install one.")


# Set container runtime globally
CONTAINER_RUNTIME = get_container_runtime()
os.environ["CONTAINER_RUNTIME"] = CONTAINER_RUNTIME


def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line("markers", "unit: Unit tests that don't require external services")
    config.addinivalue_line("markers", "integration: Integration tests requiring Cassandra cluster")
    config.addinivalue_line("markers", "slow: Tests that take a long time to run")


def pytest_collection_modifyitems(config, items):
    """Automatically skip integration tests if not explicitly requested."""
    if config.getoption("markexpr"):
        # User specified markers, respect their choice
        return

    # Check if Cassandra is available
    cassandra_available = check_cassandra_available()

    skip_integration = pytest.mark.skip(
        reason="Integration tests require running Cassandra cluster. Use -m integration to run."
    )

    for item in items:
        if "integration" in item.keywords and not cassandra_available:
            item.add_marker(skip_integration)


def check_cassandra_available():
    """Check if Cassandra cluster is available."""
    try:
        # Try to connect to the first node
        import socket

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        result = sock.connect_ex(("127.0.0.1", 9042))
        sock.close()
        return result == 0
    except Exception:
        return False


@pytest.fixture(scope="session")
def container_runtime():
    """Get the container runtime being used."""
    return CONTAINER_RUNTIME


@pytest.fixture(scope="session")
def docker_compose_file():
    """Path to docker-compose file."""
    return Path(__file__).parent.parent / "docker-compose.yml"


@pytest.fixture(scope="session")
def docker_compose_command(container_runtime):
    """Get the appropriate docker-compose command."""
    if container_runtime == "podman":
        return ["podman-compose"]
    else:
        return ["docker-compose"]
