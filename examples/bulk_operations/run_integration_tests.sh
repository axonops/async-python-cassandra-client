#!/bin/bash
# Integration test runner for bulk operations

echo "🚀 Bulk Operations Integration Test Runner"
echo "========================================="

# Check if docker or podman is available
if command -v podman &> /dev/null; then
    CONTAINER_TOOL="podman"
elif command -v docker &> /dev/null; then
    CONTAINER_TOOL="docker"
else
    echo "❌ Error: Neither docker nor podman found. Please install one."
    exit 1
fi

echo "Using container tool: $CONTAINER_TOOL"

# Function to wait for cluster to be ready
wait_for_cluster() {
    echo "⏳ Waiting for Cassandra cluster to be ready..."
    local max_attempts=60
    local attempt=0

    while [ $attempt -lt $max_attempts ]; do
        if $CONTAINER_TOOL exec bulk-cassandra-1 nodetool status 2>/dev/null | grep -q "UN"; then
            echo "✅ Cassandra cluster is ready!"
            return 0
        fi
        attempt=$((attempt + 1))
        echo -n "."
        sleep 5
    done

    echo "❌ Timeout waiting for cluster to be ready"
    return 1
}

# Function to show cluster status
show_cluster_status() {
    echo ""
    echo "📊 Cluster Status:"
    echo "=================="
    $CONTAINER_TOOL exec bulk-cassandra-1 nodetool status || true
    echo ""
}

# Main execution
echo ""
echo "1️⃣ Starting Cassandra cluster..."
$CONTAINER_TOOL-compose up -d

if wait_for_cluster; then
    show_cluster_status

    echo "2️⃣ Running integration tests..."
    echo ""

    # Run pytest with integration markers
    pytest tests/test_integration.py -v -s -m integration
    TEST_RESULT=$?

    echo ""
    echo "3️⃣ Cluster token information:"
    echo "=============================="
    echo "Sample output from nodetool describering:"
    $CONTAINER_TOOL exec bulk-cassandra-1 nodetool describering bulk_test 2>/dev/null | head -20 || true

    echo ""
    echo "4️⃣ Test Summary:"
    echo "================"
    if [ $TEST_RESULT -eq 0 ]; then
        echo "✅ All integration tests passed!"
    else
        echo "❌ Some tests failed. Please check the output above."
    fi

    echo ""
    read -p "Press Enter to stop the cluster, or Ctrl+C to keep it running..."

    echo "Stopping cluster..."
    $CONTAINER_TOOL-compose down
else
    echo "❌ Failed to start cluster. Check container logs:"
    $CONTAINER_TOOL-compose logs
    $CONTAINER_TOOL-compose down
    exit 1
fi

echo ""
echo "✨ Done!"
