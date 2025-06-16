#!/bin/bash
# Script to manage test containers for async-python-cassandra-client project
# This script supports both Docker and Podman

# Naming convention for our test containers: async-cassandra-test-*
PROJECT_PREFIX="async-cassandra-test"

# Detect container runtime (docker or podman)
if command -v podman &> /dev/null; then
    CONTAINER_CMD="podman"
elif command -v docker &> /dev/null; then
    CONTAINER_CMD="docker"
else
    echo "Error: Neither docker nor podman found in PATH"
    exit 1
fi

echo "Using container runtime: $CONTAINER_CMD"

# Function to list all project test containers
list_containers() {
    echo "Listing all ${PROJECT_PREFIX} containers:"
    $CONTAINER_CMD ps -a --format "table {{.ID}}\t{{.Names}}\t{{.Status}}\t{{.Image}}" | grep -E "(CONTAINER ID|${PROJECT_PREFIX})" || echo "No test containers found"
}

# Function to kill all project test containers
kill_containers() {
    echo "Finding and killing all ${PROJECT_PREFIX} containers..."
    local containers=$($CONTAINER_CMD ps -a --format "{{.Names}}" | grep "^${PROJECT_PREFIX}" || true)
    
    if [ -z "$containers" ]; then
        echo "No test containers to kill"
    else
        echo "Killing containers:"
        echo "$containers"
        echo "$containers" | xargs -r $CONTAINER_CMD rm -f
        echo "All test containers killed"
    fi
}

# Function to clean up volumes associated with test containers
clean_volumes() {
    echo "Cleaning up orphaned volumes..."
    if [ "$CONTAINER_CMD" = "docker" ]; then
        docker volume prune -f
    else
        # Podman handles volumes differently
        podman volume prune -f
    fi
}

# Function to show container logs (useful for debugging)
show_logs() {
    local container_name=$1
    if [ -z "$container_name" ]; then
        echo "Usage: $0 logs <container_name>"
        exit 1
    fi
    
    echo "Showing logs for $container_name:"
    $CONTAINER_CMD logs "$container_name"
}

# Main script logic
case "$1" in
    list)
        list_containers
        ;;
    kill)
        kill_containers
        ;;
    clean)
        kill_containers
        clean_volumes
        ;;
    logs)
        show_logs "$2"
        ;;
    *)
        echo "Usage: $0 {list|kill|clean|logs <container_name>}"
        echo ""
        echo "Commands:"
        echo "  list  - List all test containers"
        echo "  kill  - Kill all test containers"
        echo "  clean - Kill all test containers and clean up volumes"
        echo "  logs  - Show logs for a specific container"
        echo ""
        echo "Container naming convention: ${PROJECT_PREFIX}-<test-id>"
        exit 1
        ;;
esac