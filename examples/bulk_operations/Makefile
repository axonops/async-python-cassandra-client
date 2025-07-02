.PHONY: help install dev-install test test-unit test-integration lint format type-check clean docker-up docker-down run-example

# Default target
.DEFAULT_GOAL := help

help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install production dependencies
	pip install -e .

dev-install: ## Install development dependencies
	pip install -e ".[dev]"

test: ## Run all tests
	pytest -v

test-unit: ## Run unit tests only
	pytest -v -m unit

test-integration: ## Run integration tests (requires Cassandra cluster)
	pytest -v -m integration

test-slow: ## Run slow tests
	pytest -v -m slow

lint: ## Run linting checks
	ruff check .
	black --check .

format: ## Format code
	black .
	ruff check --fix .

type-check: ## Run type checking
	mypy bulk_operations tests

clean: ## Clean up generated files
	rm -rf build/ dist/ *.egg-info/
	rm -rf .pytest_cache/ .coverage htmlcov/
	rm -rf iceberg_warehouse/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

# Container runtime detection
CONTAINER_RUNTIME ?= $(shell which docker >/dev/null 2>&1 && echo docker || which podman >/dev/null 2>&1 && echo podman)
ifeq ($(CONTAINER_RUNTIME),podman)
    COMPOSE_CMD = podman-compose
else
    COMPOSE_CMD = docker-compose
endif

docker-up: ## Start 3-node Cassandra cluster
	$(COMPOSE_CMD) up -d
	@echo "Waiting for Cassandra cluster to be ready..."
	@sleep 30
	@$(CONTAINER_RUNTIME) exec cassandra-1 cqlsh -e "DESCRIBE CLUSTER" || (echo "Cluster not ready, waiting more..." && sleep 30)
	@echo "Cassandra cluster is ready!"

docker-down: ## Stop and remove Cassandra cluster
	$(COMPOSE_CMD) down -v

docker-logs: ## Show Cassandra logs
	$(COMPOSE_CMD) logs -f

# Cassandra cluster management
cassandra-up: ## Start 3-node Cassandra cluster
	$(COMPOSE_CMD) up -d

cassandra-down: ## Stop and remove Cassandra cluster
	$(COMPOSE_CMD) down -v

cassandra-wait: ## Wait for Cassandra to be ready
	@echo "Waiting for Cassandra cluster to be ready..."
	@for i in {1..30}; do \
		if $(CONTAINER_RUNTIME) exec bulk-cassandra-1 cqlsh -e "SELECT now() FROM system.local" >/dev/null 2>&1; then \
			echo "Cassandra is ready!"; \
			break; \
		fi; \
		echo "Waiting for Cassandra... ($$i/30)"; \
		sleep 5; \
	done

cassandra-logs: ## Show Cassandra logs
	$(COMPOSE_CMD) logs -f

# Example commands
example-count: ## Run bulk count example
	@echo "Running bulk count example..."
	python example_count.py

example-export: ## Run export to Iceberg example (not yet implemented)
	@echo "Export example not yet implemented"
	# python example_export.py

example-import: ## Run import from Iceberg example (not yet implemented)
	@echo "Import example not yet implemented"
	# python example_import.py

# Quick demo
demo: cassandra-up cassandra-wait example-count ## Run quick demo with count example

# Development workflow
dev-setup: dev-install docker-up ## Complete development setup

ci: lint type-check test-unit ## Run CI checks (no integration tests)
