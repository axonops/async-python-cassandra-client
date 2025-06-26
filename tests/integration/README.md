# Integration Tests

This directory contains integration tests for the async-python-cassandra-client library. The tests run against a real Cassandra instance.

## Prerequisites

You need a running Cassandra instance on your machine. The tests expect Cassandra to be available on `localhost:9042` by default.

## Running Tests

### Quick Start

```bash
# Start Cassandra (if not already running)
make cassandra-start

# Run integration tests
make test-integration

# Stop Cassandra when done
make cassandra-stop
```

### Using Existing Cassandra

If you already have Cassandra running elsewhere:

```bash
# Set the contact points
export CASSANDRA_CONTACT_POINTS=10.0.0.1,10.0.0.2
export CASSANDRA_PORT=9042  # optional, defaults to 9042

# Run tests
make test-integration
```

## Makefile Targets

- `make cassandra-start` - Start a Cassandra container using Docker or Podman
- `make cassandra-stop` - Stop and remove the Cassandra container
- `make cassandra-status` - Check if Cassandra is running and ready
- `make cassandra-wait` - Wait for Cassandra to be ready (starts it if needed)
- `make test-integration` - Run integration tests (waits for Cassandra automatically)
- `make test-integration-keep` - Run tests but keep containers running

## Environment Variables

- `CASSANDRA_CONTACT_POINTS` - Comma-separated list of Cassandra contact points (default: localhost)
- `CASSANDRA_PORT` - Cassandra port (default: 9042)
- `CONTAINER_RUNTIME` - Container runtime to use (auto-detected, can be docker or podman)
- `CASSANDRA_IMAGE` - Cassandra Docker image (default: cassandra:5)
- `CASSANDRA_CONTAINER_NAME` - Container name (default: async-cassandra-test)
- `SKIP_INTEGRATION_TESTS=1` - Skip integration tests entirely
- `KEEP_CONTAINERS=1` - Keep containers running after tests complete

## Container Configuration

When using `make cassandra-start`, the container is configured with:
- Image: `cassandra:5` (latest Cassandra 5.x)
- Port: `9042` (default Cassandra port)
- Cluster name: `TestCluster`
- Datacenter: `datacenter1`
- Snitch: `SimpleSnitch`

## Writing Integration Tests

Integration tests should:
1. Use the `cassandra_session` fixture for a ready-to-use session
2. Clean up any test data they create
3. Be marked with `@pytest.mark.integration`
4. Handle transient network errors gracefully

Example:
```python
@pytest.mark.integration
@pytest.mark.asyncio
async def test_example(cassandra_session):
    result = await cassandra_session.execute("SELECT * FROM system.local")
    assert result.one() is not None
```

## Troubleshooting

### Cassandra Not Available

If tests fail with "Cassandra is not available":

1. Check if Cassandra is running: `make cassandra-status`
2. Start Cassandra: `make cassandra-start`
3. Wait for it to be ready: `make cassandra-wait`

### Port Conflicts

If port 9042 is already in use by another service:
1. Stop the conflicting service, or
2. Use a different Cassandra instance and set `CASSANDRA_CONTACT_POINTS`

### Container Issues

If using containers and having issues:
1. Check container logs: `docker logs async-cassandra-test` or `podman logs async-cassandra-test`
2. Ensure you have enough available memory (at least 1GB free)
3. Try removing and recreating: `make cassandra-stop && make cassandra-start`

### Docker vs Podman

The Makefile automatically detects whether you have Docker or Podman installed. If you have both and want to force one:

```bash
export CONTAINER_RUNTIME=podman  # or docker
make cassandra-start
```
