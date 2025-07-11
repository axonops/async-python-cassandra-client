.PHONY: help install install-dev test test-unit test-integration test-stress lint format type-check build clean cassandra-start cassandra-stop cassandra-status cassandra-wait

# Environment setup
CONTAINER_RUNTIME ?= $(shell command -v podman >/dev/null 2>&1 && echo podman || echo docker)
CASSANDRA_CONTACT_POINTS ?= 127.0.0.1
CASSANDRA_PORT ?= 9042
CASSANDRA_IMAGE ?= cassandra:4.1
CASSANDRA_CONTAINER_NAME ?= async-cassandra-bulk-test

help:
	@echo "Available commands:"
	@echo ""
	@echo "Installation:"
	@echo "  install        Install the package"
	@echo "  install-dev    Install with development dependencies"
	@echo ""
	@echo "Testing:"
	@echo "  test           Run all tests (unit + integration)"
	@echo "  test-unit      Run unit tests only"
	@echo "  test-integration Run integration tests (auto-manages Cassandra)"
	@echo "  test-stress    Run stress tests"
	@echo ""
	@echo "Cassandra Management:"
	@echo "  cassandra-start Start Cassandra container"
	@echo "  cassandra-stop  Stop Cassandra container"
	@echo "  cassandra-status Check if Cassandra is running"
	@echo "  cassandra-wait  Wait for Cassandra to be ready"
	@echo ""
	@echo "Code Quality:"
	@echo "  lint           Run linters (ruff, black, isort, mypy)"
	@echo "  format         Format code"
	@echo "  type-check     Run type checking"
	@echo ""
	@echo "Build:"
	@echo "  build          Build distribution packages"
	@echo "  clean          Clean build artifacts"
	@echo ""
	@echo "Environment variables:"
	@echo "  CASSANDRA_CONTACT_POINTS  Cassandra contact points (default: 127.0.0.1)"
	@echo "  CASSANDRA_PORT            Cassandra port (default: 9042)"
	@echo "  SKIP_INTEGRATION_TESTS=1  Skip integration tests"

install:
	pip install -e .

install-dev:
	pip install -e ".[dev,test]"

# Standard test command - runs everything
test:
	@echo "Running standard test suite..."
	@echo "=== Running Unit Tests (No Cassandra Required) ==="
	pytest tests/unit/ -v
	@echo "=== Starting Cassandra for Integration Tests ==="
	$(MAKE) cassandra-wait
	@echo "=== Running Integration Tests ==="
	pytest tests/integration/ -v
	@echo "=== Cleaning up Cassandra ==="
	$(MAKE) cassandra-stop

test-unit:
	@echo "Running unit tests (no Cassandra required)..."
	pytest tests/unit/ -v --cov=async_cassandra_bulk --cov-report=html

test-integration: cassandra-wait
	@echo "Running integration tests..."
	CASSANDRA_CONTACT_POINTS=$(CASSANDRA_CONTACT_POINTS) pytest tests/integration/ -v

test-stress: cassandra-wait
	@echo "Running stress tests..."
	CASSANDRA_CONTACT_POINTS=$(CASSANDRA_CONTACT_POINTS) pytest tests/integration/test_stress.py -v

# Code quality
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
	ruff check --fix src/ tests/

type-check:
	mypy src/

# Build
build: clean
	python -m build

clean:
	rm -rf dist/ build/ *.egg-info/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

# Cassandra management
cassandra-start:
	@echo "Starting Cassandra container..."
	@echo "Stopping any existing Cassandra container..."
	@$(CONTAINER_RUNTIME) stop $(CASSANDRA_CONTAINER_NAME) 2>/dev/null || true
	@$(CONTAINER_RUNTIME) rm -f $(CASSANDRA_CONTAINER_NAME) 2>/dev/null || true
	@$(CONTAINER_RUNTIME) run -d \
		--name $(CASSANDRA_CONTAINER_NAME) \
		-p $(CASSANDRA_PORT):9042 \
		-e CASSANDRA_CLUSTER_NAME=TestCluster \
		-e CASSANDRA_DC=datacenter1 \
		-e CASSANDRA_ENDPOINT_SNITCH=GossipingPropertyFileSnitch \
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
		echo "Timeout waiting for Cassandra to be ready"; \
		exit 1; \
	else \
		echo "Cassandra is already running on $(CASSANDRA_CONTACT_POINTS):$(CASSANDRA_PORT)"; \
	fi
