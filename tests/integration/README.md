# Integration Tests

This directory contains integration tests for the async-python-cassandra-client library. The tests run against a real Cassandra instance using either Docker or Podman.

## Container Runtime Support

The integration tests automatically detect and use the available container runtime:
- **Docker** (with docker-compose or docker compose plugin)
- **Podman** (with podman-compose)

## Running Tests

### Automatic Container Management (Recommended)

Simply run:
```bash
make test-integration
```

This will:
1. Automatically detect Docker or Podman
2. Start a Cassandra 5.0 container if not already running
3. Wait for Cassandra to be ready
4. Run all integration tests
5. Stop and remove the container when done

### Keep Containers Running

To keep containers running after tests (useful for debugging):
```bash
make test-integration-keep
```

### Manual Container Management

Start containers:
```bash
make container-start
```

Check container status:
```bash
make container-status
```

Stop containers:
```bash
make container-stop
```

## Environment Variables

- `SKIP_INTEGRATION_TESTS=1` - Skip integration tests entirely
- `KEEP_CONTAINERS=1` - Keep containers running after tests complete

## Container Configuration

The Cassandra container is configured with:
- Image: `cassandra:5.0`
- Port: `9042` (default Cassandra port)
- Memory: 512MB heap (suitable for testing)
- Health checks for automatic readiness detection

## Troubleshooting

### Container Runtime Not Found

If you get an error about Docker or Podman not being found:

1. Install Docker: https://docs.docker.com/get-docker/
2. Or install Podman: https://podman.io/getting-started/installation

For Podman users, also install podman-compose:
```bash
pip install podman-compose
```

### Port Conflicts

If port 9042 is already in use, you can:
1. Stop any existing Cassandra instances
2. Or modify `docker-compose.yml` to use a different port

### Container Startup Issues

If Cassandra fails to start:
1. Check container logs: `docker logs async-cassandra-test`
2. Ensure you have enough available memory (at least 1GB free)
3. Try manually starting with: `make container-start`

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