# Troubleshooting Integration Tests

## Common Issues

### Connection Reset by Peer

**Symptoms:**
- `ConnectionResetError: [Errno 54] Connection reset by peer`
- `NoHostAvailable` errors
- `Cluster is already shut down` on retry attempts

**Root Cause:**
The Cassandra container is not running or not ready to accept connections.

**Solution:**
1. Check if Cassandra is running:
   ```bash
   python tests/integration/container_manager.py status
   ```

2. If not running, start it:
   ```bash
   cd tests/integration
   python container_manager.py start
   ```

3. Verify port 9042 is accessible:
   ```bash
   nc -zv localhost 9042
   ```

### Container Management

The integration tests use a container manager that supports both Docker and Podman. The container is automatically started by pytest when running integration tests, but sometimes manual intervention is needed.

**Manual Container Commands:**
```bash
# Check status
python tests/integration/container_manager.py status

# Start containers
python tests/integration/container_manager.py start

# Stop containers
python tests/integration/container_manager.py stop
```

**Keep Containers Running:**
To prevent containers from being stopped after tests:
```bash
KEEP_CONTAINERS=1 pytest tests/integration/
```

### Protocol Version Issues

The async-cassandra driver requires Cassandra 4.0+ (protocol v5). The test container uses Cassandra 5.0 which supports this. If you see protocol version errors, ensure:

1. The Cassandra container image is version 4.0 or higher
2. Protocol version 5 is specified in tests when needed
3. The container has fully started and is ready

### Debugging Connection Issues

If tests fail with connection errors:

1. Check container logs:
   ```bash
   podman logs async-cassandra-test
   # or
   docker logs async-cassandra-test
   ```

2. Test basic connectivity:
   ```bash
   # Using cqlsh
   podman exec async-cassandra-test cqlsh -e "DESCRIBE KEYSPACES"
   # or
   docker exec async-cassandra-test cqlsh -e "DESCRIBE KEYSPACES"
   ```

3. Check for port conflicts:
   ```bash
   lsof -i :9042
   ```

### IPv6 Issues

The Cassandra driver tries both IPv4 (127.0.0.1) and IPv6 (::1) addresses. You may see warnings about IPv6 connections failing - this is normal if your system doesn't have IPv6 enabled. The driver will fall back to IPv4.
