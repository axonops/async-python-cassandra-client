# Developer Guide

This guide contains all information needed for developing, testing, and contributing to async-python-cassandra-client.

## Table of Contents

- [Development Setup](#development-setup)
- [Running Tests](#running-tests)
- [Code Quality](#code-quality)
- [CI/CD Process](#cicd-process)
- [Release Process](#release-process)
- [Troubleshooting](#troubleshooting)

## Development Setup

### Prerequisites

- Python 3.12+
- Docker or Podman (for integration tests)
- Make (optional, for convenience commands)

### Setting Up Your Environment

```bash
# Clone the repository
git clone https://github.com/axonops/async-python-cassandra-client.git
cd async-python-cassandra-client

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install development dependencies
pip install -e ".[dev]"
```

## Running Tests

### Test Categories

1. **Unit Tests** - Fast tests with mocked dependencies
2. **Integration Tests** - Tests against real Cassandra using containers
3. **BDD Tests** - Behavior-driven tests (partially implemented)
4. **Benchmarks** - Performance tests

### Running All Tests

```bash
# Using make
make test

# Or manually
pytest tests/
```

### Running Specific Test Categories

```bash
# Unit tests only
make test-unit
# Or: pytest tests/unit/

# Integration tests only (requires Docker/Podman)
make test-integration
# Or: pytest tests/integration/

# Specific test file
pytest tests/unit/test_session.py -v

# Run with coverage
make test-coverage
```

### FastAPI Example Tests

The FastAPI example app serves as our primary integration test for real-world usage:

```bash
cd examples/fastapi_app

# Start Cassandra (if not running)
docker-compose up -d cassandra

# Run FastAPI tests
pytest test_fastapi_app.py -v
```

## Code Quality

### Linting and Formatting

All code must pass linting checks before merge:

```bash
# Run all checks
make lint

# Or run individually:
ruff check src/ tests/
black --check src/ tests/
isort --check-only src/ tests/
mypy src/
```

### Auto-formatting

```bash
# Format code automatically
make format

# Or manually:
black src/ tests/
isort src/ tests/
ruff check --fix src/ tests/
```

### Pre-commit Hooks

We recommend setting up pre-commit hooks:

```bash
pip install pre-commit
pre-commit install
```

## CI/CD Process

### GitHub Actions Workflows

1. **tests.yml** - Runs on every push and PR
   - Unit tests
   - Integration tests
   - Linting checks
   - Coverage reporting

2. **publish.yml** - Runs on release tags
   - Builds packages
   - Publishes to PyPI

### PR Requirements

Before merging, all PRs must:
- ✅ Pass all unit tests
- ✅ Pass all integration tests
- ✅ Pass all linting checks (ruff, black, isort, mypy)
- ✅ Maintain or improve code coverage
- ✅ Update documentation if needed
- ✅ Include tests for new features

## Release Process

1. Update version in `pyproject.toml`
2. Update CHANGELOG.md
3. Create and push tag:
   ```bash
   git tag -a v0.1.0 -m "Release version 0.1.0"
   git push origin v0.1.0
   ```
4. GitHub Actions will automatically publish to PyPI

## Troubleshooting

### Common Issues

#### Container Runtime Issues
If integration tests fail with container errors:
```bash
# Check if Docker/Podman is running
docker ps  # or: podman ps

# Use Podman instead of Docker
export USE_PODMAN=1
make test-integration
```

#### Import Errors
Ensure you've installed development dependencies:
```bash
pip install -e ".[dev]"
```

#### Test Timeouts
Some tests may timeout on slower systems. Increase timeout:
```bash
pytest tests/ --timeout=300
```

### Debug Mode

Run tests with more verbose output:
```bash
pytest tests/ -vv -s --log-cli-level=DEBUG
```

### Getting Help

- Check existing issues on GitHub
- Review test output carefully
- Enable debug logging in tests
- Ask questions in GitHub Discussions
