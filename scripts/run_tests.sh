#!/bin/bash
# Smart test runner for async-python-cassandra-client

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    local status=$1
    local message=$2
    case $status in
        "success")
            echo -e "${GREEN}✓${NC} $message"
            ;;
        "error")
            echo -e "${RED}✗${NC} $message"
            ;;
        "info")
            echo -e "${YELLOW}→${NC} $message"
            ;;
    esac
}

# Function to ensure Cassandra is running
ensure_cassandra() {
    if ! ./scripts/quick_cassandra.sh check 2>/dev/null; then
        print_status "info" "Cassandra not running, starting container..."
        ./scripts/quick_cassandra.sh start
    else
        print_status "success" "Cassandra is already running"
    fi
}

# Function to run a test suite
run_test_suite() {
    local suite_name=$1
    local test_command=$2
    
    print_status "info" "Running $suite_name..."
    
    if eval "$test_command"; then
        print_status "success" "$suite_name passed"
        return 0
    else
        print_status "error" "$suite_name failed"
        return 1
    fi
}

# Main test execution
case "${1:-all}" in
    unit)
        run_test_suite "Unit Tests" "pytest tests/unit/ tests/_core/ tests/_resilience/ tests/_features/ -v"
        ;;
    
    integration)
        ensure_cassandra
        run_test_suite "Integration Tests" "pytest tests/integration/ -v -m integration"
        ;;
    
    fastapi)
        ensure_cassandra
        run_test_suite "FastAPI Tests (tests/fastapi)" "pytest tests/fastapi -v || true"
        run_test_suite "FastAPI Example Tests" "cd examples/fastapi_app && pytest test_fastapi_app.py -v"
        ;;
    
    bdd)
        ensure_cassandra
        run_test_suite "BDD Tests" "pytest tests/bdd -v"
        ;;
    
    critical)
        ensure_cassandra
        print_status "info" "Running critical tests as per..."
        run_test_suite "Core Critical Tests" "pytest tests/_core -v -x -m 'critical'"
        run_test_suite "FastAPI Tests" "pytest tests/fastapi -v || true"
        run_test_suite "FastAPI Example" "cd examples/fastapi_app && pytest test_fastapi_app.py -v"
        run_test_suite "BDD Critical Tests" "pytest tests/bdd -m 'critical' -v || true"
        ;;
    
    all)
        print_status "info" "Running complete test suite..."
        
        # Lint first
        print_status "info" "Running linters..."
        make lint
        
        # Then run all tests
        ensure_cassandra
        
        failed=0
        
        run_test_suite "Unit Tests" "pytest tests/unit/ tests/_core/ tests/_resilience/ tests/_features/ -v" || failed=1
        run_test_suite "Integration Tests" "pytest tests/integration/ -v -m integration" || failed=1
        run_test_suite "FastAPI Tests" "cd examples/fastapi_app && pytest test_fastapi_app.py -v" || failed=1
        run_test_suite "BDD Tests" "pytest tests/bdd -v" || failed=1
        
        if [ $failed -eq 0 ]; then
            print_status "success" "All tests passed!"
        else
            print_status "error" "Some tests failed"
            exit 1
        fi
        ;;
    
    clean)
        print_status "info" "Cleaning up test containers..."
        ./scripts/manage_test_containers.sh kill
        ./scripts/quick_cassandra.sh stop 2>/dev/null || true
        print_status "success" "Cleanup complete"
        ;;
    
    *)
        echo "Usage: $0 {unit|integration|fastapi|bdd|critical|all|clean}"
        echo ""
        echo "Test suites:"
        echo "  unit        - Run unit tests (no Cassandra needed)"
        echo "  integration - Run integration tests"
        echo "  fastapi     - Run FastAPI tests"
        echo "  bdd         - Run BDD tests"
        echo "  critical    - Run critical tests (per CLAUDE.md)"
        echo "  all         - Run all tests with linting"
        echo "  clean       - Clean up test containers"
        exit 1
        ;;
esac