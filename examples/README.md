# async-cassandra Examples

This directory contains working examples demonstrating various features and use cases of async-cassandra.

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
python export_large_table.py
# Exports will be saved in ./exports/ directory
```

### 4. [Real-time Data Processing](realtime_processing.py)

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

### 5. [Metrics Collection](metrics_simple.py)

Simple example of metrics collection:
- Query performance tracking
- Connection health monitoring
- Error rate calculation
- Performance statistics summary

**Run:**
```bash
python metrics_simple.py
```

### 6. [Advanced Metrics](metrics_example.py)

More comprehensive metrics example (requires updates to work with current API).

### 7. [Monitoring Configuration](monitoring/)

Production-ready monitoring configurations:
- **alerts.yml** - Prometheus alerting rules for:
  - High query latency
  - Connection failures
  - Error rate thresholds
- **grafana_dashboard.json** - Grafana dashboard for visualizing:
  - Query performance metrics
  - Connection health status
  - Error rates and trends

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

## Common Patterns Demonstrated

### Connection Management
- Using context managers for automatic cleanup
- Proper cluster and session lifecycle
- Connection health monitoring

### Error Handling
- Catching and handling Cassandra exceptions
- Retry strategies with idempotency
- Graceful degradation

### Performance Optimization
- Prepared statements for repeated queries
- Concurrent query execution
- Streaming for large datasets
- Appropriate fetch sizes

### Monitoring & Observability
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
- Include clear documentation in the code
- Handle errors appropriately
- Clean up resources (drop keyspaces/tables)
- Test with Python 3.12
- Update this README

## Support

- GitHub Issues: https://github.com/axonops/async-python-cassandra-client/issues
- Discussions: https://github.com/axonops/async-python-cassandra-client/discussions
- Website: https://axonops.com
