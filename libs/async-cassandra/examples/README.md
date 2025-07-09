# async-cassandra Examples

This directory contains working examples demonstrating various features and use cases of async-cassandra.

## 📍 Important: Directory Context

All examples must be run from the `libs/async-cassandra` directory, not from this examples directory:

```bash
# Navigate to the async-cassandra library directory first
cd libs/async-cassandra

# Then run examples using make
make example-streaming
```

## Quick Start

### Running Examples with Make

The easiest way to run examples is using the provided Make targets from the `libs/async-cassandra` directory:

```bash
# From the libs/async-cassandra directory:
cd libs/async-cassandra

# Run a specific example (automatically starts Cassandra if needed)
make example-streaming
make example-export-csv
make example-export-parquet
make example-realtime
make example-metrics
make example-non-blocking
make example-context

# Run all examples in sequence
make examples-all

# Use external Cassandra cluster
CASSANDRA_CONTACT_POINTS=node1.example.com,node2.example.com make example-streaming
```

### Installing Example Dependencies

Some examples require additional dependencies:

```bash
# From the libs/async-cassandra directory:
cd libs/async-cassandra

# Install all example dependencies (including pyarrow for Parquet export)
make install-examples

# Or manually
pip install -r examples/requirements.txt
```

### Environment Variables

All examples support these environment variables:
- `CASSANDRA_CONTACT_POINTS`: Comma-separated list of contact points (default: localhost)
- `CASSANDRA_PORT`: Port number (default: 9042)
- `EXAMPLE_OUTPUT_DIR`: Directory for output files like CSV and Parquet exports (default: examples/exampleoutput)

## Available Examples

### 1. [FastAPI Integration](fastapi_app/)

A complete REST API application demonstrating:
- Full CRUD operations with async Cassandra
- Update operations (PUT/PATCH endpoints)
- Streaming endpoints for large datasets
- Performance comparison endpoints (async vs sync)
- Connection lifecycle management with lifespan
- Docker Compose setup for easy development
- Comprehensive integration tests

**Run the FastAPI app:**
```bash
cd fastapi_app
docker-compose up  # Starts Cassandra and the app
# Or manually:
pip install -r requirements.txt
python main.py
```

### 2. [Basic Streaming](streaming_basic.py)

Demonstrates streaming functionality for large result sets:
- Basic streaming with `execute_stream()`
- Page-based processing for batch operations
- Progress tracking with callbacks
- Filtering and parameterized streaming queries
- Memory-efficient data processing

**Run:**
```bash
# From libs/async-cassandra directory:
make example-streaming

# Or run directly (from this examples directory):
python streaming_basic.py
```

### 3. [Export Large Tables](export_large_table.py)

Shows how to export large Cassandra tables to CSV:
- Memory-efficient streaming export
- Progress tracking during export
- Both async and sync file I/O examples
- Handling of various Cassandra data types
- Configurable fetch sizes for optimization

**Run:**
```bash
# From libs/async-cassandra directory:
make example-export-large-table

# Or run directly (from this examples directory):
python export_large_table.py
# Exports will be saved in examples/exampleoutput/ directory (default)

# Or with custom output directory:
EXAMPLE_OUTPUT_DIR=/tmp/my-exports python export_large_table.py
```

### 4. [Export to Parquet Format](export_to_parquet.py)

Advanced example of exporting large Cassandra tables to Parquet format:
- Memory-efficient streaming with page-by-page processing
- Automatic schema inference from Cassandra data types
- Multiple compression options (snappy, gzip, lz4)
- Progress tracking during export
- Handles all Cassandra data types including collections
- Configurable row group sizes for optimization
- Export statistics and performance metrics

**Run:**
```bash
python export_to_parquet.py
# Exports will be saved in examples/exampleoutput/ directory (default)

# Or with custom output directory:
EXAMPLE_OUTPUT_DIR=/tmp/my-parquet-exports python export_to_parquet.py
```

**Note:** Requires PyArrow to be installed:
```bash
pip install pyarrow
```

### 5. [Real-time Data Processing](realtime_processing.py)

Example of processing time-series data in real-time:
- Sliding window analytics
- Real-time aggregations
- Alert triggering based on thresholds
- Handling continuous data ingestion
- Sensor data monitoring simulation

**Run:**
```bash
python realtime_processing.py
```

### 6. [Metrics Collection](metrics_simple.py)

Simple example of metrics collection:
- Query performance tracking
- Connection health monitoring
- Error rate calculation
- Performance statistics summary

**Run:**
```bash
python metrics_simple.py
```

### 7. [Advanced Metrics](metrics_example.py)

Comprehensive metrics and observability example:
- Multiple metrics collectors setup
- Query performance monitoring
- Connection health tracking
- Prometheus integration example
- FastAPI integration patterns

**Run:**
```bash
python metrics_example.py
```

### 8. [Non-Blocking Streaming Demo](streaming_non_blocking_demo.py)

Visual demonstration that streaming doesn't block the event loop:
- Heartbeat monitoring to detect event loop blocking
- Concurrent queries during streaming
- Visual feedback showing event loop responsiveness
- Performance analysis of concurrent operations
- Proves the async wrapper truly keeps the event loop free

**Run:**
```bash
python streaming_non_blocking_demo.py
```

### 9. [Context Manager Safety](context_manager_safety_demo.py)

Demonstrates proper context manager usage:
- Context manager isolation
- Error safety in queries and streaming
- Concurrent operations with shared resources
- Resource cleanup guarantees

**Run:**
```bash
python context_manager_safety_demo.py
```

### 8. [Monitoring Configuration](monitoring/)

Production-ready monitoring configurations:
- **alerts.yml** - Prometheus alerting rules for:
  - High query latency
  - Connection failures
  - Error rate thresholds
- **grafana_dashboard.json** - Grafana dashboard for visualizing:
  - Query performance metrics
  - Connection health status
  - Error rates and trends

## Output Files

Examples that generate output files (CSV exports, Parquet exports, etc.) save them to a configurable directory:

- **Default location**: `examples/exampleoutput/`
- **Configure via environment variable**: `EXAMPLE_OUTPUT_DIR=/path/to/output`
- **Git ignored**: All files in the default output directory are ignored by Git (except README.md and .gitignore)
- **Cleanup**: Files are not automatically deleted; clean up manually when needed:
  ```bash
  rm -f examples/exampleoutput/*.csv
  rm -f examples/exampleoutput/*.parquet
  ```

## Prerequisites

All examples require:

1. **Python 3.12 or higher**
2. **Apache Cassandra** running locally on port 9042
   - For FastAPI example: Use the included docker-compose.yml
   - For others: Install and run Cassandra locally or use Docker:
     ```bash
     docker run -d -p 9042:9042 cassandra:5
     ```
3. **Install async-cassandra**:
   ```bash
   pip install -e ..  # From the examples directory
   # Or when published to PyPI:
   # pip install async-cassandra
   ```

## Best Practices Demonstrated

### MANDATORY: Always Use Context Managers
All examples follow the required pattern:
```python
# ALWAYS use context managers for resource management
async with AsyncCluster(["localhost"]) as cluster:
    async with await cluster.connect() as session:
        # For streaming, ALWAYS use context manager:
        async with await session.execute_stream("SELECT * FROM table") as result:
            async for row in result:
                # Process row
                pass
```

**⚠️ CRITICAL**: See [True Async Paging](../docs/true-async-paging.md) for important details about streaming patterns and common mistakes.

### MANDATORY: Always Use PreparedStatements
For any query with parameters:
```python
# Prepare statement once
stmt = await session.prepare(
    "INSERT INTO users (id, name) VALUES (?, ?)"
)
# Execute many times
await session.execute(stmt, [user_id, name])
```

### Common Patterns Demonstrated

#### Connection Management
- Using context managers for automatic cleanup (REQUIRED)
- Proper cluster and session lifecycle
- Connection health monitoring

#### Error Handling
- Catching and handling Cassandra exceptions
- Retry strategies with idempotency
- Graceful degradation

#### Performance Optimization
- Prepared statements for repeated queries (REQUIRED)
- Concurrent query execution
- Streaming for large datasets with context managers
- Appropriate fetch sizes

#### Monitoring & Observability
- Metrics collection
- Performance tracking
- Health checks

## Running Multiple Examples

Each example is self-contained and creates its own keyspace. They clean up after themselves, so you can run them in any order.

## Troubleshooting

### Connection Refused
Ensure Cassandra is running and accessible on localhost:9042

### Module Not Found
Install async-cassandra from the parent directory:
```bash
cd ..
pip install -e .
```

### Performance Issues
Examples use local Cassandra by default. Network latency may vary with remote clusters.

## Contributing

We welcome new examples! When contributing:
- **MUST use context managers** for all cluster/session/streaming operations
- **MUST use PreparedStatements** for all parameterized queries
- Include clear documentation in the code
- Handle errors appropriately
- Clean up resources (drop keyspaces/tables)
- Test with Python 3.12
- Update this README
- Follow the patterns shown in existing examples

## Support

- GitHub Issues: https://github.com/axonops/async-python-cassandra-client/issues
- Discussions: https://github.com/axonops/async-python-cassandra-client/discussions
- Website: https://axonops.com
