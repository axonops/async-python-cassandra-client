"""
Container management utilities for tests.

Provides a consistent interface for managing test containers across
Docker and Podman, with proper cleanup and naming conventions.
"""

import random
import shutil
import subprocess
import time
from typing import Dict, List, Optional, Tuple

# Project naming convention for test containers
PROJECT_PREFIX = "async-cassandra-test"


class ContainerManager:
    """Manages test containers with support for Docker and Podman."""

    def __init__(self):
        """Initialize container manager and detect runtime."""
        self.runtime = self._detect_runtime()
        if not self.runtime:
            raise RuntimeError("Neither docker nor podman found in PATH")

    def _detect_runtime(self) -> Optional[str]:
        """Detect available container runtime."""
        if shutil.which("podman"):
            return "podman"
        elif shutil.which("docker"):
            return "docker"
        return None

    def _run_command(self, args: List[str], check: bool = True) -> subprocess.CompletedProcess:
        """Run a container command."""
        cmd = [self.runtime] + args
        return subprocess.run(cmd, capture_output=True, text=True, check=check)

    def generate_container_name(self, test_name: str = "") -> str:
        """Generate a unique container name following project convention."""
        timestamp = int(time.time())
        random_id = random.randint(1000, 9999)

        if test_name:
            # Sanitize test name
            safe_name = test_name.replace(".", "-").replace("_", "-").lower()[:20]
            return f"{PROJECT_PREFIX}-{safe_name}-{timestamp}-{random_id}"
        else:
            return f"{PROJECT_PREFIX}-{timestamp}-{random_id}"

    def list_project_containers(self) -> List[Dict[str, str]]:
        """List all containers with our project prefix."""
        result = self._run_command(["ps", "-a", "--format", "json"], check=False)

        if result.returncode != 0:
            return []

        containers = []
        for line in result.stdout.strip().split("\n"):
            if line:
                import json

                try:
                    container = json.loads(line)
                    # Check for containers with our prefix
                    names = container.get("Names", [])
                    if isinstance(names, str):
                        names = [names]

                    for name in names:
                        if name.startswith(PROJECT_PREFIX):
                            containers.append(
                                {
                                    "id": container.get("ID", container.get("Id", "")),
                                    "name": name,
                                    "status": container.get("Status", ""),
                                    "image": container.get("Image", ""),
                                }
                            )
                            break
                except json.JSONDecodeError:
                    continue

        return containers

    def kill_project_containers(self) -> int:
        """Kill all containers with our project prefix. Returns count of killed containers."""
        containers = self.list_project_containers()
        killed_count = 0

        for container in containers:
            container_id = container["id"]
            if container_id:
                result = self._run_command(["rm", "-f", container_id], check=False)
                if result.returncode == 0:
                    killed_count += 1

        return killed_count

    def start_cassandra_container(
        self,
        name: Optional[str] = None,
        version: str = "5.0",
        port: int = 9042,
        extra_args: Optional[List[str]] = None,
    ) -> Tuple[str, int]:
        """
        Start a Cassandra container for testing.
        Args:
            name: Container name (will be auto-generated if not provided)
            version: Cassandra version
            port: Host port to bind to (will find free port if 0)
            extra_args: Additional arguments to pass to container
        Returns:
            Tuple of (container_name, actual_port)
        """
        if name is None:
            name = self.generate_container_name("cassandra")

        # Find a free port if needed
        if port == 0:
            import socket

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(("", 0))
                port = s.getsockname()[1]

        # Build container run command
        cmd = ["run", "-d", "--name", name, "-p", f"{port}:9042"]

        # Add extra arguments if provided
        if extra_args:
            cmd.extend(extra_args)

        # Use appropriate image based on version
        if version == "3.11":
            image = "cassandra:3.11"
        elif version.startswith("4"):
            image = f"cassandra:{version}"
        elif version.startswith("5"):
            # Use AxonOps image for Cassandra 5
            image = "registry.axonops.com/axonops-public/axonops-docker/cassandra:5.0"
        else:
            image = f"cassandra:{version}"

        cmd.append(image)

        # Run the container
        result = self._run_command(cmd)
        if result.returncode != 0:
            raise RuntimeError(f"Failed to start container: {result.stderr}")

        return name, port

    def wait_for_cassandra(self, container_name: str, timeout: int = 60) -> bool:
        """
        Wait for Cassandra to be ready in the container.
        Args:
            container_name: Name of the container
            timeout: Maximum time to wait in seconds
        Returns:
            True if Cassandra is ready, False if timeout
        """
        start_time = time.time()

        while time.time() - start_time < timeout:
            # Check if container is running
            result = self._run_command(
                ["ps", "--filter", f"name={container_name}", "--format", "{{.Status}}"], check=False
            )

            if "Up" not in result.stdout:
                time.sleep(1)
                continue

            # Try to connect with cqlsh
            result = self._run_command(
                ["exec", container_name, "cqlsh", "-e", "SELECT release_version FROM system.local"],
                check=False,
            )

            if result.returncode == 0:
                return True

            time.sleep(1)

        return False

    def stop_container(self, container_name: str) -> bool:
        """Stop and remove a container."""
        result = self._run_command(["rm", "-f", container_name], check=False)
        return result.returncode == 0

    def get_container_logs(self, container_name: str, tail: Optional[int] = None) -> str:
        """Get logs from a container."""
        cmd = ["logs"]
        if tail:
            cmd.extend(["--tail", str(tail)])
        cmd.append(container_name)

        result = self._run_command(cmd, check=False)
        return result.stdout + result.stderr


# Singleton instance
_manager = None


def get_container_manager() -> ContainerManager:
    """Get the singleton container manager instance."""
    global _manager
    if _manager is None:
        _manager = ContainerManager()
    return _manager


# Pytest fixture support
def pytest_fixture():
    """
    Pytest fixture that provides a container manager and ensures cleanup.
    Usage:
        @pytest.fixture
        async def cassandra_container(container_manager):
            name, port = container_manager.start_cassandra_container()
            container_manager.wait_for_cassandra(name)
            yield name, port
            container_manager.stop_container(name)
    """
    import pytest

    @pytest.fixture
    def container_manager():
        manager = get_container_manager()
        containers_before = {c["name"] for c in manager.list_project_containers()}

        yield manager

        # Cleanup any containers created during the test
        containers_after = {c["name"] for c in manager.list_project_containers()}
        new_containers = containers_after - containers_before

        for container_name in new_containers:
            manager.stop_container(container_name)

    return container_manager
