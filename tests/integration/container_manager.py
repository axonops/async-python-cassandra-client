"""
Container management for integration tests.
Supports both Docker and Podman.
"""

import subprocess
import sys
import time
from pathlib import Path


class ContainerManager:
    """Manages containers for integration tests."""

    def __init__(self):
        self.container_runtime = None
        self.compose_command = None
        self.compose_file = Path(__file__).parent / "docker-compose.yml"
        self._detect_runtime()

    def _detect_runtime(self):
        """Detect available container runtime."""
        # Check for Docker
        if self._command_exists("docker"):
            try:
                subprocess.run(["docker", "version"], capture_output=True, check=True, timeout=5)
                self.container_runtime = "docker"

                # Check for docker compose (plugin) vs docker-compose (standalone)
                try:
                    subprocess.run(
                        ["docker", "compose", "version"], capture_output=True, check=True, timeout=5
                    )
                    self.compose_command = ["docker", "compose"]
                except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
                    if self._command_exists("docker-compose"):
                        self.compose_command = ["docker-compose"]
                    else:
                        raise RuntimeError(
                            "Docker found but neither 'docker compose' nor 'docker-compose' available"
                        )
                return
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
                pass

        # Check for Podman
        if self._command_exists("podman"):
            self.container_runtime = "podman"
            if self._command_exists("podman-compose"):
                self.compose_command = ["podman-compose"]
            else:
                raise RuntimeError(
                    "Podman found but podman-compose not available. "
                    "Install it with: pip install podman-compose"
                )
            return

        raise RuntimeError(
            "Neither Docker nor Podman found. Please install one of them.\n"
            "For Docker: https://docs.docker.com/get-docker/\n"
            "For Podman: https://podman.io/getting-started/installation"
        )

    def _command_exists(self, cmd):
        """Check if a command exists."""
        return subprocess.run(["which", cmd], capture_output=True, timeout=5).returncode == 0

    def start_containers(self):
        """Start the test containers."""
        print(f"Starting Cassandra container using {self.container_runtime}...")

        # Start containers
        subprocess.run(
            self.compose_command + ["-f", str(self.compose_file), "up", "-d"], check=True
        )

        # Wait for Cassandra to be ready
        print("Waiting for Cassandra to be ready...")
        max_attempts = 30
        for attempt in range(max_attempts):
            try:
                # Check if Cassandra is responding
                result = subprocess.run(
                    self.compose_command
                    + [
                        "-f",
                        str(self.compose_file),
                        "exec",
                        "cassandra",
                        "cqlsh",
                        "-e",
                        "describe keyspaces",
                    ],
                    capture_output=True,
                    timeout=10,
                )
                if result.returncode == 0:
                    print("Cassandra is ready!")
                    return
            except subprocess.TimeoutExpired:
                pass

            time.sleep(2)
            print(f"Waiting for Cassandra... ({attempt + 1}/{max_attempts})")

        raise RuntimeError("Cassandra failed to start within timeout period")

    def stop_containers(self):
        """Stop and remove test containers."""
        print(f"Stopping containers using {self.container_runtime}...")
        subprocess.run(
            self.compose_command + ["-f", str(self.compose_file), "down", "-v"], check=True
        )

    def is_running(self):
        """Check if Cassandra is available (either via container or existing service)."""
        # First check if Cassandra is available on the port
        import socket

        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)
            result = sock.connect_ex(("localhost", 9042))
            sock.close()
            if result == 0:
                # Cassandra is available on the port
                return True
        except Exception:
            pass

        # If not available on port, check if our container is running
        try:
            result = subprocess.run(
                self.compose_command + ["-f", str(self.compose_file), "ps", "-q", "cassandra"],
                capture_output=True,
                timeout=5,
            )
            return bool(result.stdout.strip())
        except subprocess.TimeoutExpired:
            return False


# CLI interface
if __name__ == "__main__":
    manager = ContainerManager()

    if len(sys.argv) < 2:
        print(f"Container runtime detected: {manager.container_runtime}")
        print(f"Compose command: {' '.join(manager.compose_command)}")
        sys.exit(0)

    command = sys.argv[1]

    if command == "start":
        manager.start_containers()
    elif command == "stop":
        manager.stop_containers()
    elif command == "status":
        if manager.is_running():
            print("Cassandra container is running")
        else:
            print("Cassandra container is not running")
    else:
        print(f"Unknown command: {command}")
        print("Usage: python container_manager.py [start|stop|status]")
        sys.exit(1)
