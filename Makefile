.PHONY: help install install-dev test test-quick test-core test-critical test-progressive test-all test-unit test-integration test-integration-keep test-stress test-bdd lint format type-check build clean cassandra-start cassandra-stop cassandra-status cassandra-wait

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
	@echo "Cassandra Management:"
	@echo "  cassandra-start Start Cassandra container"
	@echo "  cassandra-stop  Stop Cassandra container"
	@echo "  cassandra-status Check if Cassandra is running"
	@echo "  cassandra-wait  Wait for Cassandra to be ready"
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
	@echo "  CASSANDRA_CONTACT_POINTS  Cassandra contact points (default: localhost)"
	@echo "  SKIP_INTEGRATION_TESTS=1  Skip integration tests"
	@echo "  KEEP_CONTAINERS=1         Keep containers running after tests"

install:
	pip install -e .

install-dev:
	pip install -e ".[dev,test]"
	pip install -r requirements-lint.txt
	pre-commit install

# Environment setup
CONTAINER_RUNTIME ?= $(shell command -v podman >/dev/null 2>&1 && echo podman || echo docker)
CASSANDRA_CONTACT_POINTS ?= localhost
CASSANDRA_PORT ?= 9042
CASSANDRA_IMAGE ?= cassandra:5
CASSANDRA_CONTAINER_NAME ?= async-cassandra-test

# Quick validation (30s)
test-quick:
	@echo "Running quick validation tests..."
	pytest tests/_core -v -x -m "quick"

# Core tests only (1m)
test-core:
	@echo "Running core functionality tests..."
	pytest tests/_core tests/_resilience -v -x

# Critical path - MUST ALL PASS
test-critical: cassandra-wait
	@echo "Running critical tests (including FastAPI)..."
	pytest tests/_core -v -x -m "critical"
	pytest tests/fastapi -v
	cd examples/fastapi_app && pytest test_fastapi_app.py -v
	pytest tests/bdd -m "critical" -v

# Progressive execution - FAIL FAST
test-progressive: cassandra-wait
	@echo "Running tests in fail-fast order..."
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
test-bdd: cassandra-wait
	@echo "Running BDD tests..."
	@mkdir -p reports
	pytest tests/bdd -v --cucumber-json=reports/bdd.json

# Standard test command - runs everything except stress
test: cassandra-wait
	@echo "Running standard test suite..."
	pytest tests/ -v -m "not stress"

test-unit:
	@echo "Running unit tests (no Cassandra required)..."
	pytest tests/unit/ -v --cov=async_cassandra --cov-report=html
	@echo "Unit tests completed."

test-integration: cassandra-wait
	@echo "Running integration tests..."
	CASSANDRA_CONTACT_POINTS=$(CASSANDRA_CONTACT_POINTS) pytest tests/integration/ -v -m integration
	@echo "Integration tests completed."

test-integration-keep: cassandra-wait
	@echo "Running integration tests (keeping containers after tests)..."
	KEEP_CONTAINERS=1 CASSANDRA_CONTACT_POINTS=$(CASSANDRA_CONTACT_POINTS) pytest tests/integration/ -v -m integration
	@echo "Integration tests completed. Containers are still running."

test-fastapi: cassandra-wait
	@echo "Running FastAPI integration tests with real app and Cassandra..."
	cd examples/fastapi_app && CASSANDRA_CONTACT_POINTS=$(CASSANDRA_CONTACT_POINTS) pytest ../../tests/fastapi_integration/ -v
	@echo "FastAPI integration tests completed."

test-stress: cassandra-wait
	@echo "Running stress tests..."
	CASSANDRA_CONTACT_POINTS=$(CASSANDRA_CONTACT_POINTS) pytest tests/integration/ tests/performance/ -v -m stress
	@echo "Stress tests completed."

# Full test suite - EVERYTHING MUST PASS
test-all: lint cassandra-wait
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

# Cassandra management
cassandra-start:
	@echo "Starting Cassandra container..."
	@$(CONTAINER_RUNTIME) rm -f $(CASSANDRA_CONTAINER_NAME) 2>/dev/null || true
	@$(CONTAINER_RUNTIME) run -d \
		--name $(CASSANDRA_CONTAINER_NAME) \
		-p $(CASSANDRA_PORT):9042 \
		-e CASSANDRA_CLUSTER_NAME=TestCluster \
		-e CASSANDRA_DC=datacenter1 \
		-e CASSANDRA_ENDPOINT_SNITCH=SimpleSnitch \
		$(CASSANDRA_IMAGE)
	@echo "Cassandra container started"

cassandra-stop:
	@echo "Stopping Cassandra container..."
	@$(CONTAINER_RUNTIME) stop $(CASSANDRA_CONTAINER_NAME) 2>/dev/null || true
	@$(CONTAINER_RUNTIME) rm $(CASSANDRA_CONTAINER_NAME) 2>/dev/null || true
	@echo "Cassandra container stopped"

cassandra-status:
	@if $(CONTAINER_RUNTIME) ps --format "{{.Names}}" | grep -q "^$(CASSANDRA_CONTAINER_NAME)$$"; then \
		echo "Cassandra container is running"; \
		if $(CONTAINER_RUNTIME) exec $(CASSANDRA_CONTAINER_NAME) nodetool info 2>&1 | grep -q "Native Transport active: true"; then \
			if $(CONTAINER_RUNTIME) exec $(CASSANDRA_CONTAINER_NAME) cqlsh -e "SELECT release_version FROM system.local" 2>&1 | grep -q "[0-9]"; then \
				echo "Cassandra is ready and accepting CQL queries"; \
			else \
				echo "Cassandra native transport is active but CQL not ready yet"; \
			fi; \
		else \
			echo "Cassandra is starting up..."; \
		fi; \
	else \
		echo "Cassandra container is not running"; \
		exit 1; \
	fi

cassandra-wait:
	@echo "Ensuring Cassandra is ready..."
	@if ! nc -z $(CASSANDRA_CONTACT_POINTS) $(CASSANDRA_PORT) 2>/dev/null; then \
		echo "Cassandra not running on $(CASSANDRA_CONTACT_POINTS):$(CASSANDRA_PORT), starting container..."; \
		$(MAKE) cassandra-start; \
		echo "Waiting for Cassandra to be ready..."; \
		for i in $$(seq 1 60); do \
			if $(CONTAINER_RUNTIME) exec $(CASSANDRA_CONTAINER_NAME) nodetool info 2>&1 | grep -q "Native Transport active: true"; then \
				if $(CONTAINER_RUNTIME) exec $(CASSANDRA_CONTAINER_NAME) cqlsh -e "SELECT release_version FROM system.local" 2>&1 | grep -q "[0-9]"; then \
					echo "Cassandra is ready! (verified with SELECT query)"; \
					exit 0; \
				fi; \
			fi; \
			printf "."; \
			sleep 2; \
		done; \
		echo ""; \
		echo "Timeout waiting for Cassandra"; \
		exit 1; \
	else \
		echo "Checking if Cassandra on $(CASSANDRA_CONTACT_POINTS):$(CASSANDRA_PORT) can accept queries..."; \
		if [ "$(CASSANDRA_CONTACT_POINTS)" = "localhost" ] && $(CONTAINER_RUNTIME) ps --format "{{.Names}}" | grep -q "^$(CASSANDRA_CONTAINER_NAME)$$"; then \
			if ! $(CONTAINER_RUNTIME) exec $(CASSANDRA_CONTAINER_NAME) cqlsh -e "SELECT release_version FROM system.local" 2>&1 | grep -q "[0-9]"; then \
				echo "Cassandra is running but not accepting queries yet, waiting..."; \
				for i in $$(seq 1 30); do \
					if $(CONTAINER_RUNTIME) exec $(CASSANDRA_CONTAINER_NAME) cqlsh -e "SELECT release_version FROM system.local" 2>&1 | grep -q "[0-9]"; then \
						echo "Cassandra is ready! (verified with SELECT query)"; \
						exit 0; \
					fi; \
					printf "."; \
					sleep 2; \
				done; \
				echo ""; \
				echo "Timeout waiting for Cassandra to accept queries"; \
				exit 1; \
			fi; \
		fi; \
		echo "Cassandra is already running and accepting queries"; \
	fi

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

clean-all: clean cassandra-stop
	@echo "All cleaned up"
