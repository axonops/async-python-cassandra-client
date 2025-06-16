#!/bin/bash
# Detect and use available container runtime (Docker or Podman)

# Function to check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Detect container runtime
if command_exists docker && docker version >/dev/null 2>&1; then
    CONTAINER_RUNTIME="docker"
    COMPOSE_COMMAND="docker compose"
    # Check for older docker-compose command
    if ! docker compose version >/dev/null 2>&1; then
        if command_exists docker-compose; then
            COMPOSE_COMMAND="docker-compose"
        else
            echo "Error: docker compose plugin or docker-compose not found" >&2
            exit 1
        fi
    fi
elif command_exists podman; then
    CONTAINER_RUNTIME="podman"
    if command_exists podman-compose; then
        COMPOSE_COMMAND="podman-compose"
    else
        echo "Error: podman-compose not found. Please install it with: pip install podman-compose" >&2
        exit 1
    fi
else
    echo "Error: Neither Docker nor Podman found. Please install one of them." >&2
    exit 1
fi

# Export for use in other scripts
export CONTAINER_RUNTIME
export COMPOSE_COMMAND

# If sourced with an argument, execute the command
if [ $# -gt 0 ]; then
    $COMPOSE_COMMAND "$@"
fi
