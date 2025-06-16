.PHONY: help install install-dev test test-quick test-core test-critical test-progressive test-all test-unit test-integration test-integration-keep test-stress test-bdd lint format type-check build clean container-start container-stop container-status container-clean container-list

help:
	@echo "Available commands:"
	@echo ""
	@echo "Installation:"
	@echo "  install        Install the package"
	@echo "  install-dev    Install with development dependencies"
	@echo ""
	@echo "Quick Test Commands:"
	@echo "  test-quick     Run quick validation tests (~30s)"
	@echo "  test-core      Run core functionality tests only (~1m)"
	@echo "  test-critical  Run critical tests (core + FastAPI) (~2m)"
	@echo "  test-progressive Run tests in fail-fast order"
	@echo ""
	@echo "Test Suites:"
	@echo "  test           Run all tests (excluding stress tests)"
	@echo "  test-unit      Run unit tests only"
	@echo "  test-integration Run integration tests (auto-manages containers)"
	@echo "  test-integration-keep Run integration tests (keeps containers running)"
	@echo "  test-stress    Run stress tests"
	@echo "  test-bdd       Run BDD tests"
	@echo "  test-all       Run ALL tests (unit, integration, stress, and BDD)"
	@echo ""
	@echo "Test Categories:"
	@echo "  test-resilience Run error handling and resilience tests"
	@echo "  test-features  Run advanced feature tests"
	@echo "  test-fastapi   Run FastAPI integration tests"
	@echo "  test-performance Run performance and benchmark tests"
	@echo ""
	@echo "Container Management:"
	@echo "  container-start Start Cassandra container manually"
	@echo "  container-stop  Stop Cassandra container"
	@echo "  container-status Check if Cassandra container is running"
	@echo "  container-list  List all test containers"
	@echo "  container-clean Kill all test containers"
	@echo ""
	@echo "Code Quality:"
	@echo "  lint           Run linters"
	@echo "  format         Format code"
	@echo "  type-check     Run type checking"
	@echo ""
	@echo "Build:"
	@echo "  build          Build distribution packages"
	@echo "  clean          Clean build artifacts"
	@echo ""
	@echo "Environment variables:"
	@echo "  SKIP_INTEGRATION_TESTS=1  Skip integration tests"
	@echo "  KEEP_CONTAINERS=1         Keep containers running after tests"

install:
	pip install -e .

install-dev:
	pip install -e ".[dev,test]"
	pip install -r requirements-lint.txt
	pre-commit install

# Quick validation (30s)
test-quick:
	@echo "Running quick validation tests..."
	pytest tests/_core -v -x -m "quick"

# Core tests only (1m)
test-core:
	@echo "Running core functionality tests..."
	pytest tests/_core tests/_resilience -v -x

# Critical path - MUST ALL PASS
test-critical:
	@echo "Running critical tests (including FastAPI)..."
	@if ! ./scripts/quick_cassandra.sh check 2>/dev/null; then \
		echo "Starting Cassandra container for critical tests..."; \
		./scripts/quick_cassandra.sh start; \
	fi
	pytest tests/_core -v -x -m "critical"
	pytest tests/fastapi -v
	cd examples/fastapi_app && pytest test_fastapi_app.py -v
	pytest tests/bdd -m "critical" -v

# Progressive execution - FAIL FAST
test-progressive:
	@echo "Running tests in fail-fast order..."
	@if ! ./scripts/quick_cassandra.sh check 2>/dev/null; then \
		echo "Starting Cassandra container..."; \
		./scripts/quick_cassandra.sh start; \
	fi
	@echo "=== Running Core Tests ==="
	@pytest tests/_core -v -x || exit 1
	@echo "=== Running Resilience Tests ==="
	@pytest tests/_resilience -v -x || exit 1
	@echo "=== Running Feature Tests ==="
	@pytest tests/_features -v || exit 1
	@echo "=== Running Integration Tests ==="
	@pytest tests/integration -v || exit 1
	@echo "=== Running FastAPI Tests ==="
	@pytest tests/fastapi -v || exit 1
	@echo "=== Running FastAPI Example App Tests ==="
	@cd examples/fastapi_app && pytest test_fastapi_app.py -v || exit 1
	@echo "=== Running BDD Tests ==="
	@pytest tests/bdd -v || exit 1

# Test suite commands
test-resilience:
	@echo "Running resilience tests..."
	pytest tests/_resilience -v

test-features:
	@echo "Running feature tests..."
	pytest tests/_features -v


test-performance:
	@echo "Running performance tests..."
	pytest tests/performance -v

# BDD tests - MUST PASS
test-bdd:
	@echo "Running BDD tests..."
	@if ! ./scripts/quick_cassandra.sh check 2>/dev/null; then \
		echo "Starting Cassandra container for BDD tests..."; \
		./scripts/quick_cassandra.sh start; \
	fi
	@mkdir -p reports
	pytest tests/bdd -v --cucumber-json=reports/bdd.json

# Standard test command - runs everything except stress
test:
	@echo "Running standard test suite..."
	@if ! ./scripts/quick_cassandra.sh check 2>/dev/null; then \
		echo "Starting Cassandra container..."; \
		./scripts/quick_cassandra.sh start; \
	fi
	pytest tests/ -v -m "not stress"

test-unit:
	@echo "Running unit tests (no Cassandra required)..."
	pytest tests/unit/ -v --cov=async_cassandra --cov-report=html
	@echo "Unit tests completed."

test-integration:
	@echo "Running integration tests..."
	@if ! ./scripts/quick_cassandra.sh check 2>/dev/null; then \
		echo "Starting Cassandra container..."; \
		./scripts/quick_cassandra.sh start; \
	fi
	pytest tests/integration/ -v -m integration
	@echo "Integration tests completed."

test-integration-keep:
	@echo "Running integration tests (keeping containers after tests)..."
	KEEP_CONTAINERS=1 pytest tests/integration/ -v -m integration
	@echo "Integration tests completed. Containers are still running."

test-fastapi:
	@echo "Running FastAPI integration tests with real app and Cassandra..."
	@if ! ./scripts/quick_cassandra.sh check 2>/dev/null; then \
		echo "Starting Cassandra container..."; \
		./scripts/quick_cassandra.sh start; \
	fi
	cd examples/fastapi_app && pytest ../../tests/fastapi_integration/ -v
	@echo "FastAPI integration tests completed."

test-stress:
	@echo "Running stress tests..."
	pytest tests/integration/ tests/performance/ -v -m stress
	@echo "Stress tests completed."

# Full test suite - EVERYTHING MUST PASS
test-all: lint
	@echo "Running complete test suite..."
	@./scripts/run_tests.sh all

# Code quality - MUST PASS
lint:
	@echo "=== Running ruff ==="
	ruff check src/ tests/
	@echo "=== Running black ==="
	black --check src/ tests/
	@echo "=== Running isort ==="
	isort --check-only src/ tests/
	@echo "=== Running mypy ==="
	mypy src/

format:
	black src/ tests/
	isort src/ tests/

type-check:
	mypy src/

# Build
build:
	python -m build

# Container management
container-start:
	@echo "Starting test containers..."
	@./scripts/quick_cassandra.sh start

container-stop:
	@echo "Stopping test containers..."
	@./scripts/quick_cassandra.sh stop

container-status:
	@./scripts/quick_cassandra.sh check

container-list:
	@./scripts/manage_test_containers.sh list

container-clean:
	@echo "Cleaning up test containers..."
	@./scripts/manage_test_containers.sh kill
	@./scripts/quick_cassandra.sh stop 2>/dev/null || true

# Cleanup
clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info
	rm -rf .coverage
	rm -rf htmlcov/
	rm -rf .pytest_cache/
	rm -rf .mypy_cache/
	rm -rf reports/*.json reports/*.html reports/*.xml
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
