#!/bin/bash
# Quick Cassandra container management for tests

set -e

CONTAINER_NAME="async-cassandra-test-quick"
CASSANDRA_IMAGE="cassandra:4.1"
PORT=9042

# Detect container runtime
if command -v podman &> /dev/null; then
    CONTAINER_CMD="podman"
elif command -v docker &> /dev/null; then
    CONTAINER_CMD="docker"
else
    echo "Error: Neither docker nor podman found"
    exit 1
fi

# Function to check if Cassandra is ready
check_cassandra() {
    # Quick port check using Python (more portable than nc)
    if python3 "$(dirname "$0")/check_port.py" localhost $PORT; then
        # If port is open, assume Cassandra is ready
        # We can't reliably test with cqlsh as it may not be installed
        return 0
    fi
    return 1
}

# Function to start Cassandra
start_cassandra() {
    echo "Checking for Cassandra..."
    
    # First check if ANY Cassandra is available
    if check_cassandra; then
        echo "Cassandra is already running and ready!"
        exit 0
    fi
    
    # Check if our container exists but isn't responding
    if $CONTAINER_CMD ps --format "{{.Names}}" | grep -q "^${CONTAINER_NAME}$"; then
        echo "Container exists but not responding, removing..."
        $CONTAINER_CMD rm -f $CONTAINER_NAME 2>/dev/null || true
    fi
    
    # Remove any existing container
    $CONTAINER_CMD rm -f $CONTAINER_NAME 2>/dev/null || true
    
    # Start new container
    $CONTAINER_CMD run -d \
        --name $CONTAINER_NAME \
        -p $PORT:9042 \
        -e CASSANDRA_CLUSTER_NAME=TestCluster \
        -e CASSANDRA_DC=datacenter1 \
        -e CASSANDRA_ENDPOINT_SNITCH=SimpleSnitch \
        $CASSANDRA_IMAGE
    
    # Wait for readiness
    echo "Waiting for Cassandra to be ready..."
    for i in {1..30}; do
        if check_cassandra; then
            echo "Cassandra is ready!"
            exit 0
        fi
        echo -n "."
        sleep 1
    done
    
    echo -e "\nTimeout waiting for Cassandra"
    exit 1
}

# Function to stop Cassandra
stop_cassandra() {
    echo "Stopping Cassandra container..."
    $CONTAINER_CMD stop $CONTAINER_NAME 2>/dev/null || true
    $CONTAINER_CMD rm $CONTAINER_NAME 2>/dev/null || true
    echo "Cassandra container stopped"
}

# Main logic
case "${1:-start}" in
    start)
        start_cassandra
        ;;
    stop)
        stop_cassandra
        ;;
    check)
        if check_cassandra; then
            echo "Cassandra is running and ready"
            exit 0
        else
            echo "Cassandra is not ready"
            exit 1
        fi
        ;;
    *)
        echo "Usage: $0 {start|stop|check}"
        exit 1
        ;;
esac