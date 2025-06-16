"""BDD tests for protocol version enforcement."""

import subprocess
import time

import pytest
from pytest_bdd import given, parsers, scenario, then, when

from async_cassandra import AsyncCluster

# Import the cassandra_container fixture for Cassandra 5 tests
pytest_plugins = ["tests._fixtures.cassandra"]


# Define each scenario explicitly
@scenario(
    "features/protocol_version_enforcement.feature",
    "Connection to Cassandra 3.11 should fail with clear error",
)
def test_cassandra_311_connection_fails():
    """Test connection to Cassandra 3.11 fails."""
    pass


@scenario(
    "features/protocol_version_enforcement.feature",
    "Explicit protocol v5 with Cassandra 3.11 should fail immediately",
)
def test_cassandra_311_explicit_v5_fails():
    """Test explicit v5 with Cassandra 3.11 fails."""
    pass


@scenario(
    "features/protocol_version_enforcement.feature",
    "Explicit protocol v4 should be rejected at configuration time",
)
def test_explicit_v4_rejected():
    """Test explicit v4 is rejected."""
    pass


@scenario(
    "features/protocol_version_enforcement.feature",
    "Connection to Cassandra 5 should succeed with protocol v5 or higher",
)
def test_cassandra_5_connection_succeeds():
    """Test connection to Cassandra 5 succeeds."""
    pass


@scenario(
    "features/protocol_version_enforcement.feature",
    "Explicit protocol v5 with Cassandra 5 should succeed",
)
def test_cassandra_5_explicit_v5_succeeds():
    """Test explicit v5 with Cassandra 5 succeeds."""
    pass


@scenario(
    "features/protocol_version_enforcement.feature",
    "Explicit protocol v6 with Cassandra 5 should fail due to beta status",
)
def test_cassandra_5_explicit_v6_fails_beta():
    """Test explicit v6 with Cassandra 5 fails due to beta status."""
    pass


@pytest.fixture
def protocol_context():
    """Context for protocol version tests."""
    return {
        "cluster": None,
        "session": None,
        "container_info": None,
        "containers": [],
        "connection_result": None,
        "cluster_creation_result": None,
        "error": None,
    }


def run_async(coro, loop):
    """Run async code in sync context."""
    return loop.run_until_complete(coro)


# Add step for Cassandra 5.0 using existing fixture
@given(parsers.parse("a Cassandra 5.0 cluster is running on port 9042"))
def cassandra_5_running(cassandra_container, protocol_context):
    """Use existing Cassandra 5 container."""
    assert cassandra_container.is_running()
    protocol_context["container_info"] = {"version": "5.0", "port": 9042, "container": "existing"}
    return cassandra_container


# Override the given steps from given_steps.py for this specific test
@given(parsers.parse("a Cassandra {version} cluster is running on port {port:d}"))
def cassandra_version_running(version, port, protocol_context):
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

    # Store container info for cleanup
    protocol_context["containers"].append((docker_cmd, container_name))
    protocol_context["container_info"] = {
        "version": version,
        "port": port,
        "container": container_name,
    }

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


# When steps
@when("I try to connect without specifying protocol version")
def try_connect_no_version(protocol_context, event_loop):
    """Try to connect without protocol version."""

    async def _connect():
        try:
            cluster = AsyncCluster(
                contact_points=["127.0.0.1"], port=protocol_context["container_info"]["port"]
            )
            session = await cluster.connect()
            protocol_context["cluster"] = cluster
            protocol_context["session"] = session
            protocol_context["connection_result"] = {
                "success": True,
                "cluster": cluster,
                "session": session,
                "protocol_version": cluster._cluster.protocol_version,
            }
        except Exception as e:
            protocol_context["connection_result"] = {
                "success": False,
                "error": e,
                "error_type": type(e).__name__,
            }

    run_async(_connect(), event_loop)


@when(parsers.parse("I try to connect with protocol version {version:d}"))
def try_connect_with_version(version, protocol_context, event_loop):
    """Try to connect with specific protocol version."""

    async def _connect():
        try:
            cluster = AsyncCluster(
                contact_points=["127.0.0.1"],
                port=protocol_context["container_info"]["port"],
                protocol_version=version,
            )
            session = await cluster.connect()
            protocol_context["cluster"] = cluster
            protocol_context["session"] = session
            protocol_context["connection_result"] = {
                "success": True,
                "cluster": cluster,
                "session": session,
                "protocol_version": cluster._cluster.protocol_version,
            }
        except Exception as e:
            protocol_context["connection_result"] = {
                "success": False,
                "error": e,
                "error_type": type(e).__name__,
            }

    run_async(_connect(), event_loop)


@when(parsers.parse("I try to create a cluster with protocol version {version:d}"))
def try_create_cluster_with_version(version, protocol_context):
    """Try to create cluster with specific protocol version."""
    # For configuration tests, we don't need an actual container
    port = (
        protocol_context["container_info"]["port"]
        if protocol_context.get("container_info")
        else 9042
    )

    try:
        cluster = AsyncCluster(contact_points=["127.0.0.1"], port=port, protocol_version=version)
        protocol_context["cluster_creation_result"] = {
            "success": True,
            "cluster": cluster,
            "protocol_version": version,
        }
    except Exception as e:
        protocol_context["cluster_creation_result"] = {
            "success": False,
            "error": e,
            "error_type": type(e).__name__,
        }


@when("I connect without specifying protocol version")
def connect_no_version(protocol_context, event_loop):
    """Connect without protocol version (for Cassandra 5)."""
    try_connect_no_version(protocol_context, event_loop)


@when(parsers.parse("I connect with protocol version {version:d}"))
def connect_with_version(version, protocol_context, event_loop):
    """Connect with protocol version (for Cassandra 5)."""
    try_connect_with_version(version, protocol_context, event_loop)


# Then steps are already defined in then_steps.py, but we need to use protocol_context
@then("the connection should fail with protocol version error")
def verify_protocol_error(protocol_context):
    """Verify protocol version error."""
    result = protocol_context["connection_result"]
    assert not result["success"], "Connection should have failed"
    assert result["error_type"] == "ConnectionError"
    error_msg = str(result["error"])
    assert "protocol v4 but v5+ is required" in error_msg or "protocol v5" in error_msg.lower()


@then(parsers.parse('the error message should mention "{text}"'))
def verify_error_contains(text, protocol_context):
    """Verify error contains text."""
    # Check both connection_result and cluster_creation_result
    result = protocol_context["connection_result"] or protocol_context["cluster_creation_result"]
    assert result is not None, "No result found in context"
    assert not result["success"], "Operation should have failed"
    error_msg = str(result["error"])
    assert text in error_msg, f"Error message should contain '{text}', but was: {error_msg}"


@then("the connection should fail with NoHostAvailable error")
def verify_no_host_error(protocol_context):
    """Verify NoHostAvailable error."""
    result = protocol_context["connection_result"]
    assert not result["success"], "Connection should have failed"
    error_msg = str(result["error"])
    # Accept NoHostAvailable, protocol errors, or cluster shutdown (which happens after NoHostAvailable)
    acceptable_errors = [
        "NoHostAvailable",
        "protocol",
        "Protocol",
        "Beta version",
        "Cluster is already shut down",
    ]
    assert any(
        err in error_msg for err in acceptable_errors
    ), f"Expected protocol/NoHostAvailable error, got: {error_msg}"


@then("the error message should mention protocol incompatibility")
def verify_protocol_incompatibility(protocol_context):
    """Verify protocol incompatibility."""
    result = protocol_context["connection_result"]
    assert not result["success"], "Connection should have failed"
    error_msg = str(result["error"])
    # Accept various protocol-related errors including beta version, cluster shutdown after failure
    protocol_keywords = [
        "protocol",
        "version",
        "v5",
        "Cassandra 4.0",
        "Beta version",
        "Cluster is already shut down",
    ]
    assert any(
        keyword in error_msg for keyword in protocol_keywords
    ), f"Expected protocol error, got: {error_msg}"


@then("I should get a ConfigurationError immediately")
def verify_config_error(protocol_context):
    """Verify ConfigurationError."""
    result = protocol_context["cluster_creation_result"]
    assert not result["success"], "Cluster creation should have failed"
    assert result["error_type"] == "ConfigurationError"


@then("the connection should succeed")
def verify_success(protocol_context):
    """Verify connection succeeded."""
    result = protocol_context["connection_result"]
    assert result[
        "success"
    ], f"Connection should have succeeded but failed with: {result.get('error')}"


@then(parsers.parse("the negotiated protocol version should be {version:d} or higher"))
def verify_version_min(version, protocol_context):
    """Verify minimum protocol version."""
    result = protocol_context["connection_result"]
    assert result["success"], "Connection should have succeeded"
    actual = result["protocol_version"]
    assert actual >= version, f"Protocol version should be {version}+, but was {actual}"


@then(parsers.parse("the negotiated protocol version should be exactly {version:d}"))
def verify_version_exact(version, protocol_context):
    """Verify exact protocol version."""
    result = protocol_context["connection_result"]
    assert result["success"], "Connection should have succeeded"
    actual = result["protocol_version"]
    assert actual == version, f"Protocol version should be {version}, but was {actual}"


# Cleanup
@pytest.fixture(autouse=True)
def cleanup_after_test(protocol_context, event_loop):
    """Clean up resources after each test."""
    yield

    async def _cleanup():
        if protocol_context.get("session"):
            try:
                await protocol_context["session"].close()
            except Exception:
                pass

        if protocol_context.get("cluster"):
            try:
                await protocol_context["cluster"].shutdown()
            except Exception:
                pass

    if protocol_context.get("session") or protocol_context.get("cluster"):
        run_async(_cleanup(), event_loop)

    # Clean up containers
    for docker_cmd, container_name in protocol_context.get("containers", []):
        try:
            subprocess.run([docker_cmd, "stop", container_name], capture_output=True)
            subprocess.run([docker_cmd, "rm", container_name], capture_output=True)
        except Exception:
            pass
