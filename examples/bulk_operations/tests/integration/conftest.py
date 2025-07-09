"""
Shared configuration and fixtures for integration tests.
"""

import os
import subprocess
import time

import pytest


def is_cassandra_running():
    """Check if Cassandra is accessible on localhost."""
    try:
        from cassandra.cluster import Cluster

        cluster = Cluster(["localhost"])
        session = cluster.connect()
        session.shutdown()
        cluster.shutdown()
        return True
    except Exception:
        return False


def start_cassandra_if_needed():
    """Start Cassandra using docker-compose if not already running."""
    if is_cassandra_running():
        return True

    # Try to start single-node Cassandra
    compose_file = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "docker-compose-single.yml"
    )

    if not os.path.exists(compose_file):
        return False

    print("\nStarting Cassandra container for integration tests...")

    # Try podman first, then docker
    for cmd in ["podman-compose", "docker-compose"]:
        try:
            subprocess.run([cmd, "-f", compose_file, "up", "-d"], check=True, capture_output=True)
            break
        except (subprocess.CalledProcessError, FileNotFoundError):
            continue
    else:
        print("Could not start Cassandra - neither podman-compose nor docker-compose found")
        return False

    # Wait for Cassandra to be ready
    print("Waiting for Cassandra to be ready...")
    for _i in range(60):  # Wait up to 60 seconds
        if is_cassandra_running():
            print("Cassandra is ready!")
            return True
        time.sleep(1)

    print("Cassandra failed to start in time")
    return False


@pytest.fixture(scope="session", autouse=True)
def ensure_cassandra():
    """Ensure Cassandra is running for integration tests."""
    if not start_cassandra_if_needed():
        pytest.skip("Cassandra is not available for integration tests")


# Skip integration tests if not explicitly requested
def pytest_collection_modifyitems(config, items):
    """Skip integration tests unless --integration flag is passed."""
    if not config.getoption("--integration", default=False):
        skip_integration = pytest.mark.skip(
            reason="Integration tests not requested (use --integration flag)"
        )
        for item in items:
            if "integration" in item.keywords:
                item.add_marker(skip_integration)


def pytest_addoption(parser):
    """Add custom command line options."""
    parser.addoption(
        "--integration", action="store_true", default=False, help="Run integration tests"
    )
