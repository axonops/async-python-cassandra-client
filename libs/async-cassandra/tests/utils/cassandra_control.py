"""Unified Cassandra control interface for tests.

This module provides a unified interface for controlling Cassandra in tests,
supporting both local container environments and CI service environments.
"""

import os
import subprocess
import time
from typing import Tuple

import pytest


class CassandraControl:
    """Provides unified control interface for Cassandra in different environments."""

    def __init__(self, container=None):
        """Initialize with optional container reference."""
        self.container = container
        self.is_ci = os.environ.get("CI") == "true"

    def execute_nodetool_command(self, command: str) -> subprocess.CompletedProcess:
        """Execute a nodetool command, handling both container and CI environments.

        In CI environments where Cassandra runs as a service, this will skip the test.

        Args:
            command: The nodetool command to execute (e.g., "disablebinary", "enablebinary")

        Returns:
            CompletedProcess with returncode, stdout, and stderr
        """
        if self.is_ci:
            # In CI, we can't control the Cassandra service
            pytest.skip("Cannot control Cassandra service in CI environment")

        # In local environment, execute in container
        if not self.container:
            raise ValueError("Container reference required for non-CI environments")

        container_ref = (
            self.container.container_name
            if hasattr(self.container, "container_name") and self.container.container_name
            else self.container.container_id
        )

        return subprocess.run(
            [self.container.runtime, "exec", container_ref, "nodetool", command],
            capture_output=True,
            text=True,
        )

    def wait_for_cassandra_ready(self, host: str = "127.0.0.1", timeout: int = 30) -> bool:
        """Wait for Cassandra to be ready by executing a test query with cqlsh.

        This works in both container and CI environments.
        """
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                result = subprocess.run(
                    ["cqlsh", host, "-e", "SELECT release_version FROM system.local;"],
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
                if result.returncode == 0:
                    return True
            except (subprocess.TimeoutExpired, Exception):
                pass
            time.sleep(0.5)
        return False

    def wait_for_cassandra_down(self, host: str = "127.0.0.1", timeout: int = 10) -> bool:
        """Wait for Cassandra to be down by checking if cqlsh fails.

        This works in both container and CI environments.
        """
        if self.is_ci:
            # In CI, Cassandra service is always running
            pytest.skip("Cannot control Cassandra service in CI environment")

        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                result = subprocess.run(
                    ["cqlsh", host, "-e", "SELECT 1;"],
                    capture_output=True,
                    text=True,
                    timeout=2,
                )
                if result.returncode != 0:
                    return True
            except (subprocess.TimeoutExpired, Exception):
                return True
            time.sleep(0.5)
        return False

    def disable_binary_protocol(self) -> Tuple[bool, str]:
        """Disable Cassandra binary protocol.

        Returns:
            Tuple of (success, message)
        """
        result = self.execute_nodetool_command("disablebinary")
        if result.returncode == 0:
            return True, "Binary protocol disabled"
        return False, f"Failed to disable binary protocol: {result.stderr}"

    def enable_binary_protocol(self) -> Tuple[bool, str]:
        """Enable Cassandra binary protocol.

        Returns:
            Tuple of (success, message)
        """
        result = self.execute_nodetool_command("enablebinary")
        if result.returncode == 0:
            return True, "Binary protocol enabled"
        return False, f"Failed to enable binary protocol: {result.stderr}"

    def simulate_outage(self) -> bool:
        """Simulate a Cassandra outage.

        In CI, this will skip the test.
        """
        if self.is_ci:
            # In CI, we can't actually create an outage
            pytest.skip("Cannot control Cassandra service in CI environment")

        success, _ = self.disable_binary_protocol()
        if success:
            return self.wait_for_cassandra_down()
        return False

    def restore_service(self) -> bool:
        """Restore Cassandra service after simulated outage.

        In CI, this will skip the test.
        """
        if self.is_ci:
            # In CI, service is always running
            pytest.skip("Cannot control Cassandra service in CI environment")

        success, _ = self.enable_binary_protocol()
        if success:
            return self.wait_for_cassandra_ready()
        return False
