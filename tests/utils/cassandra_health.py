"""
Shared utilities for Cassandra health checks across test suites.
"""

import subprocess
import time
from typing import Dict, Optional


def check_cassandra_health(
    runtime: str, container_name_or_id: str, timeout: float = 5.0
) -> Dict[str, bool]:
    """
    Check Cassandra health using nodetool info.

    Args:
        runtime: Container runtime (docker or podman)
        container_name_or_id: Container name or ID
        timeout: Timeout for each command

    Returns:
        Dictionary with health status:
        - native_transport: Whether native transport is active
        - gossip: Whether gossip is active
        - cql_available: Whether CQL queries work
    """
    health_status = {
        "native_transport": False,
        "gossip": False,
        "cql_available": False,
    }

    try:
        # Run nodetool info
        result = subprocess.run(
            [runtime, "exec", container_name_or_id, "nodetool", "info"],
            capture_output=True,
            text=True,
            timeout=timeout,
        )

        if result.returncode == 0:
            info = result.stdout
            health_status["native_transport"] = "Native Transport active: true" in info

            # Parse gossip status more carefully
            if "Gossip active" in info:
                gossip_line = info.split("Gossip active")[1].split("\n")[0]
                health_status["gossip"] = "true" in gossip_line

            # Check CQL availability
            cql_result = subprocess.run(
                [
                    runtime,
                    "exec",
                    container_name_or_id,
                    "cqlsh",
                    "-e",
                    "SELECT now() FROM system.local",
                ],
                capture_output=True,
                timeout=timeout,
            )
            health_status["cql_available"] = cql_result.returncode == 0
    except subprocess.TimeoutExpired:
        pass
    except Exception:
        pass

    return health_status


def wait_for_cassandra_health(
    runtime: str,
    container_name_or_id: str,
    timeout: int = 90,
    check_interval: float = 3.0,
    required_checks: Optional[list] = None,
) -> bool:
    """
    Wait for Cassandra to be healthy.

    Args:
        runtime: Container runtime (docker or podman)
        container_name_or_id: Container name or ID
        timeout: Maximum time to wait in seconds
        check_interval: Time between health checks
        required_checks: List of required health checks (default: native_transport and cql_available)

    Returns:
        True if healthy within timeout, False otherwise
    """
    if required_checks is None:
        required_checks = ["native_transport", "cql_available"]

    start_time = time.time()
    while time.time() - start_time < timeout:
        health = check_cassandra_health(runtime, container_name_or_id)

        if all(health.get(check, False) for check in required_checks):
            return True

        time.sleep(check_interval)

    return False


def ensure_cassandra_healthy(runtime: str, container_name_or_id: str) -> Dict[str, bool]:
    """
    Ensure Cassandra is healthy, raising an exception if not.

    Args:
        runtime: Container runtime (docker or podman)
        container_name_or_id: Container name or ID

    Returns:
        Health status dictionary

    Raises:
        RuntimeError: If Cassandra is not healthy
    """
    health = check_cassandra_health(runtime, container_name_or_id)

    if not health["native_transport"] or not health["cql_available"]:
        raise RuntimeError(
            f"Cassandra is not healthy: Native Transport={health['native_transport']}, "
            f"CQL Available={health['cql_available']}"
        )

    return health
