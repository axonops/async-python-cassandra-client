#!/bin/bash
# Health check and test validation script

set -e

echo "==================================="
echo "Test Suite Health Check"
echo "==================================="

# Check Cassandra
echo -n "Checking Cassandra... "
if scripts/quick_cassandra.sh check >/dev/null 2>&1; then
    echo "✅ Running"
else
    echo "❌ Not running"
    echo "Starting Cassandra..."
    scripts/quick_cassandra.sh start
fi

# Run each test suite with proper isolation
echo ""
echo "Running test suites..."
echo ""

# Core tests
echo "1. Core Tests..."
if pytest tests/_core tests/_resilience -v --tb=short -x --timeout=60 > /tmp/core_tests.log 2>&1; then
    echo "   ✅ Core tests PASSED"
else
    echo "   ❌ Core tests FAILED"
    tail -20 /tmp/core_tests.log
fi

# Unit tests
echo "2. Unit Tests..."
if pytest tests/unit/ -v --tb=short -x --timeout=60 > /tmp/unit_tests.log 2>&1; then
    echo "   ✅ Unit tests PASSED"
else
    echo "   ❌ Unit tests FAILED"
    tail -20 /tmp/unit_tests.log
fi

# Integration tests
echo "3. Integration Tests (subset)..."
if pytest tests/integration/test_basic_operations.py tests/integration/test_cassandra_data_types.py -v --tb=short --timeout=60 > /tmp/integ_tests.log 2>&1; then
    echo "   ✅ Integration tests PASSED"
else
    echo "   ❌ Integration tests FAILED"
    tail -20 /tmp/integ_tests.log
fi

# FastAPI tests
echo "4. FastAPI Tests (quick check)..."
if pytest tests/fastapi/test_fastapi_advanced.py::TestFastAPIAdvancedScenarios::test_memory_leak_detection_in_streaming -v --tb=short --timeout=60 > /tmp/fastapi_tests.log 2>&1; then
    echo "   ✅ FastAPI test PASSED"
else
    echo "   ❌ FastAPI test FAILED"
    tail -20 /tmp/fastapi_tests.log
fi

echo ""
echo "==================================="
echo "Health Check Complete"
echo "==================================="
