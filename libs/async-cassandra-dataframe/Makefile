.PHONY: help install install-dev test test-unit test-integration test-distributed lint format clean docker-up docker-down cassandra-start cassandra-stop cassandra-status cassandra-wait

# Environment setup
CONTAINER_RUNTIME ?= $(shell command -v podman >/dev/null 2>&1 && echo podman || echo docker)
CASSANDRA_CONTACT_POINTS ?= 127.0.0.1
CASSANDRA_PORT ?= 9042
CASSANDRA_CONTAINER_NAME ?= cassandra-dataframe-test

help:
	@echo "Available commands:"
	@echo "  install         Install the package"
	@echo "  install-dev     Install with development dependencies"
	@echo "  test           Run all tests"
	@echo "  test-unit      Run unit tests only"
	@echo "  test-integration Run integration tests"
	@echo "  test-distributed Run distributed tests with Dask cluster"
	@echo "  lint           Run linters"
	@echo "  format         Format code"
	@echo "  clean          Clean build artifacts"
	@echo ""
	@echo "Cassandra Management:"
	@echo "  cassandra-start Start Cassandra container"
	@echo "  cassandra-stop  Stop Cassandra container"
	@echo "  cassandra-status Check if Cassandra is running"
	@echo "  cassandra-wait  Wait for Cassandra to be ready"
	@echo ""
	@echo "  docker-up      Start test containers (deprecated, use cassandra-start)"
	@echo "  docker-down    Stop test containers (deprecated, use cassandra-stop)"

install:
	pip install -e .

install-dev:
	pip install -e ".[dev,test]"

test: test-unit test-integration

test-unit:
	pytest tests/unit -v

test-integration: cassandra-start cassandra-wait
	CASSANDRA_CONTACT_POINTS=$(CASSANDRA_CONTACT_POINTS) pytest tests/integration -v -m "not distributed"
	$(MAKE) cassandra-stop

test-distributed: cassandra-start cassandra-wait
	CASSANDRA_CONTACT_POINTS=$(CASSANDRA_CONTACT_POINTS) DASK_SCHEDULER=tcp://localhost:8786 \
		pytest tests/integration -v -m "distributed"
	$(MAKE) cassandra-stop

lint:
	ruff check src tests
	black --check src tests
	isort --check-only src tests
	mypy src

format:
	black src tests
	isort src tests
	ruff check --fix src tests

clean:
	rm -rf build dist *.egg-info
	rm -rf .pytest_cache .ruff_cache .mypy_cache
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

docker-up:
	docker-compose -f docker-compose.test.yml up -d
	@echo "Waiting for services to be ready..."
	@sleep 10

docker-down:
	docker-compose -f docker-compose.test.yml down

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
		cassandra:5
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
				echo "Cassandra is running but not accepting queries yet"; \
			fi; \
		else \
			echo "Cassandra is starting up..."; \
		fi; \
	else \
		echo "Cassandra container is not running"; \
	fi

cassandra-wait:
	@echo "Waiting for Cassandra to be ready..."
	@for i in $$(seq 1 60); do \
		if $(CONTAINER_RUNTIME) exec $(CASSANDRA_CONTAINER_NAME) cqlsh -e "SELECT release_version FROM system.local" 2>&1 | grep -q "[0-9]"; then \
			echo "Cassandra is ready! (verified with SELECT query)"; \
			exit 0; \
		fi; \
		echo "Waiting for Cassandra... ($$i/60)"; \
		sleep 2; \
	done; \
	echo "Timeout waiting for Cassandra to be ready"; \
	exit 1
