# Async Python Cassandra© Client

[![CI Status](https://github.com/axonops/async-python-cassandra-client/workflows/Main%20Branch%20CI/badge.svg)](https://github.com/axonops/async-python-cassandra-client/actions)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python Version](https://img.shields.io/pypi/pyversions/async-cassandra)](https://pypi.org/project/async-cassandra/)
[![PyPI Version](https://img.shields.io/pypi/v/async-cassandra)](https://pypi.org/project/async-cassandra/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Checked with mypy](https://img.shields.io/badge/mypy-checked-blue)](http://mypy-lang.org/)

> 📢 **Early Release**: This is an early release of async-cassandra. While it has been tested extensively, you may encounter edge cases. We welcome your feedback and contributions! Please report any issues on our [GitHub Issues](https://github.com/axonops/async-python-cassandra-client/issues) page.

## ✨ Overview

A Python library that enables the Cassandra driver to work seamlessly with async frameworks like FastAPI, aiohttp, and Quart. It provides an async/await interface that prevents blocking your application's event loop while maintaining full compatibility with the DataStax Python driver.

When using the standard Cassandra driver in async applications, blocking operations can freeze your entire service. This wrapper solves that critical issue by bridging the gap between Cassandra's thread-based I/O and Python's async ecosystem, ensuring your web services remain responsive under load.

## 🏗️ Why create this framework?

### Understanding Async vs Sync

In async Python applications, an **event loop** manages all operations in a single thread. Think of it like a smart traffic controller - it efficiently switches between tasks whenever one is waiting for I/O (like a database query). This allows handling thousands of concurrent requests without creating thousands of threads.

**The key issue**: When you use the standard Cassandra driver (which is synchronous) inside an async web framework like FastAPI or aiohttp, you create a blocking problem. The synchronous driver operations block the event loop, preventing your async application from handling other requests.

**Important clarification**: This blocking issue only occurs when:
1. You're building an async application (using FastAPI, aiohttp, Quart, etc.)
2. You use synchronous database operations inside async handlers

If you're building a traditional synchronous application (Flask, Django without async views), the standard Cassandra driver works fine and you don't need this wrapper.

Here's a concrete example showing the problem and solution:

```python
# ❌ Synchronous code in an async handler - BLOCKS the event loop
from fastapi import FastAPI
from cassandra.cluster import Cluster

app = FastAPI()
cluster = Cluster(['localhost'])
session = cluster.connect()

@app.get("/users/{user_id}")
async def get_user(user_id: str):
    # This blocks the event loop! While this query runs,
    # your FastAPI app cannot process ANY other requests
    result = session.execute("SELECT * FROM users WHERE id = %s", [user_id])
    return {"user": result.one()}
```

```python
# ✅ Async code with our wrapper - NON-BLOCKING
from fastapi import FastAPI
from async_cassandra import AsyncCluster

app = FastAPI()
cluster = AsyncCluster(['localhost'])
session = None

@app.on_event("startup")
async def startup():
    global session
    session = await cluster.connect()

@app.get("/users/{user_id}")
async def get_user(user_id: str):
    # This doesn't block! The event loop remains free to handle
    # other requests while waiting for the database response
    result = await session.execute("SELECT * FROM users WHERE id = %s", [user_id])
    return {"user": result.one()}
```

The key difference: with the sync driver, each database query blocks your entire async application from handling other requests. With async-cassandra, the event loop remains free to process other work while waiting for database responses.

### The Benefits

This library provides true async/await support, enabling:

- **Non-blocking Operations**: Prevents your async application from freezing during database queries
- **Framework Compatibility**: Works naturally with FastAPI, aiohttp, and other async frameworks
- **Clean Async Code**: Use async/await syntax throughout your application
- **Better Concurrency Management**: Leverage the event loop for handling concurrent requests

See our [Architecture Overview](docs/architecture.md) for technical details, or learn more about [What This Wrapper Actually Solves (And What It Doesn't)](docs/why-async-wrapper.md#what-this-wrapper-actually-solves-and-what-it-doesnt).

### 🔄 True Async Paging

The standard Cassandra driver's manual paging (`fetch_next_page()`) is synchronous, which blocks your entire async application:

```python
# ❌ With standard driver - blocks the event loop
result = await session.execute("SELECT * FROM large_table")
while result.has_more_pages:
    result.fetch_next_page()  # This blocks! Your app freezes here

# ✅ With async-cassandra streaming - truly async
result = await session.execute_stream("SELECT * FROM large_table")
async for row in result:
    await process_row(row)  # Non-blocking, other requests keep flowing
```

This is critical for web applications where blocking the event loop means all other requests stop being processed. For a detailed explanation of this issue, see our [streaming documentation](docs/streaming.md#the-async-problem-with-manual-paging).

## ⚠️ Important Limitations

This wrapper makes the cassandra-driver compatible with async Python applications, but it's important to understand what it does and doesn't do:

**What it DOES**:
- ✅ Prevents blocking the event loop
- ✅ Provides async/await syntax
- ✅ Enables use with async frameworks (FastAPI, aiohttp)
- ✅ Allows concurrent operations via event loop

**What it DOESN'T do**:
- ❌ Make the underlying I/O truly asynchronous (still uses threads)
- ❌ Provide performance improvements over the sync driver
- ❌ Remove thread pool limitations (concurrency still bounded by driver's [thread pool size](docs/thread-pool-configuration.md))
- ❌ Eliminate thread overhead

The cassandra-driver uses blocking sockets and thread pools internally. This wrapper provides a compatibility layer but cannot change the fundamental architecture. For a detailed technical analysis, see our [Why Async Wrapper](docs/why-async-wrapper.md) documentation.

## 🚀 Key Features

- **Async/await interface** for all Cassandra operations
- **Streaming support** for memory-efficient processing of large result sets
- **Automatic retry logic** for SELECT queries, with idempotency checking for writes
- **Connection monitoring** and health checking capabilities
- **Metrics collection** with pluggable backends (in-memory, Prometheus)
- **Type hints** throughout the codebase
- **Compatible** with standard cassandra-driver types (Statement, PreparedStatement, etc.)
- **Context manager support** for proper resource cleanup

## 🔀 Alternative Libraries

Several other async Cassandra drivers exist for Python, each with different design approaches:

- **[ScyllaPy](https://github.com/Intreecom/scyllapy)**: Rust-based driver with Python bindings
- **[Acsylla](https://github.com/acsylla/acsylla)**: C++ driver wrapper using Cython
- **[DataStax AsyncioReactor](https://github.com/datastax/python-driver)**: Experimental asyncio support in the official driver

See our [comparison guide](docs/alternatives-comparison.md) for technical differences between these libraries.


## 📋 Requirements

- Python 3.12 or higher
- Apache Cassandra 4.0+ (for CQL protocol v5 support)
- cassandra-driver 3.29.2+
- CQL Protocol v5 or higher (see below)

### 🔌 CQL Protocol Version Requirement

**async-cassandra requires CQL protocol v5 or higher** for all connections. We verify this requirement after connection to ensure you get the best available protocol version.

```python
# Recommended: Let driver negotiate to highest available
cluster = AsyncCluster(['localhost'])  # Negotiates to highest (currently v5)
await cluster.connect()  # Fails if negotiated < v5

# Explicit versions (v5+):
cluster = AsyncCluster(['localhost'], protocol_version=5)  # Forces v5 exactly

# This raises ConfigurationError immediately:
cluster = AsyncCluster(['localhost'], protocol_version=4)  # ❌ Not supported
```

**Why We Enforce v5+ (and not v4 or older):**

1. **Async Performance**: Protocol v5 introduced features that significantly improve async operations:
   - Better streaming control for large result sets
   - Improved connection management per host
   - More efficient prepared statement handling

2. **Testing & Maintenance**: Supporting older protocols would require:
   - Testing against Cassandra 2.x/3.x (v3/v4 protocols)
   - Handling protocol-specific quirks and limitations
   - Maintaining compatibility code for deprecated features

3. **Security & Features**: Older protocols lack:
   - Modern authentication mechanisms
   - Proper error reporting for async contexts
   - Features required for cloud-native deployments

4. **Industry Standards**:
   - Cassandra 4.0 (with v5) was released in July 2021
   - Major cloud providers default to v5+
   - Cassandra 3.x reached EOL in 2023

**What This Means for You:**

When connecting:
- **Cassandra 4.0+**: Automatically uses v5 (highest currently supported)
- **Cassandra 3.x or older**: Connection fails with:
```
ConnectionError: Connected with protocol v4 but v5+ is required.
Your Cassandra server only supports up to protocol v4.
async-cassandra requires CQL protocol v5 or higher (Cassandra 4.0+).
Please upgrade your Cassandra cluster to version 4.0 or newer.
```

**Upgrade Options:**
- **Self-hosted**: Upgrade to Cassandra 4.0+ or 5.0 and consider AxonOps to help manage your cluster
- **AxonOps**: Supports Cassandra 4.0+ (and earlier releases also)
- **AWS Keyspaces**: Already supports v5
- **Azure Cosmos DB**: Check current documentation
- **DataStax Astra**: Supports v5+ by default

We understand this requirement may be inconvenient for some users, but it allows us to provide a better, more maintainable async experience while focusing our testing and development efforts on modern Cassandra deployments.

## 🔧 Installation

```bash
# From PyPI
pip install async-cassandra

# From source
pip install -e .
```

## 📚 Quick Start

```python
import asyncio
from async_cassandra import AsyncCluster

async def main():
    # Connect to Cassandra
    cluster = AsyncCluster(['localhost'])
    session = await cluster.connect()

    # Execute queries
    result = await session.execute("SELECT * FROM system.local")
    print(f"Connected to: {result.one().cluster_name}")

    # Clean up
    await session.close()
    await cluster.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
```

For more detailed examples, see our [Getting Started Guide](docs/getting-started.md).

## 🤝 Contributing

We welcome contributions! Please see:
- [Contributing Guidelines](CONTRIBUTING.md) - How to contribute
- [Developer Documentation](developerdocs/) - Development setup, testing, and architecture

**Important**: All contributors must sign our [Contributor License Agreement (CLA)](CLA.md) before their pull request can be merged.

## 📞 Support

- **Issues**: Please report bugs and feature requests on our [GitHub Issues](https://github.com/axonops/async-python-cassandra-client/issues) page
- **Community**: For questions and discussions, visit our [GitHub Discussions](https://github.com/axonops/async-python-cassandra-client/discussions)
- **Company**: Learn more about AxonOps at [https://axonops.com](https://axonops.com)

## 📖 Documentation

### Getting Started
- [Getting Started Guide](docs/getting-started.md) - **Start here!**
- [API Reference](docs/api.md) - Detailed API documentation
- [Troubleshooting Guide](docs/troubleshooting.md) - Common issues and solutions
- [Understanding Context Managers](docs/context-managers-explained.md) - Deep dive into Python context managers

### Advanced Topics
- [Architecture Overview](docs/architecture.md) - Technical deep dive
- [Connection Pooling Guide](docs/connection-pooling.md) - Understanding Python driver limitations
- [Thread Pool Configuration](docs/thread-pool-configuration.md) - Tuning the driver's executor
- [Streaming Large Result Sets](docs/streaming.md) - Efficiently handle large datasets
- [Performance Guide](docs/performance.md) - Optimization tips and benchmarks
- [Retry Policies](docs/retry-policies.md) - Why we have our own retry policy
- [Metrics and Monitoring](docs/metrics-monitoring.md) - Track performance and health

### Examples
- [FastAPI Integration](examples/fastapi_app/README.md) - Complete REST API example
- [More Examples](examples/) - Additional usage patterns

## ⚡ Performance

async-cassandra enables your async Python application to work with Cassandra without blocking the event loop. While it doesn't eliminate the underlying driver's thread pool, it prevents those blocking operations from freezing your entire application. This is crucial for web servers where a blocked event loop means no requests can be processed.

The wrapper's primary benefit is **compatibility**, not raw performance. It allows you to use Cassandra in async applications like FastAPI without sacrificing the responsiveness of your service.

For performance optimization tips and understanding the limitations, see our [Performance Guide](docs/performance.md).

## 📝 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- DataStax™ for the [Python Driver for Apache Cassandra](https://github.com/datastax/python-driver)
- The Python asyncio community for inspiration and best practices
- All contributors who help make this project better

## ⚖️ Legal Notices

*This project may contain trademarks or logos for projects, products, or services. Any use of third-party trademarks or logos are subject to those third-party's policies.*

**Important**: This project is not affiliated with, endorsed by, or sponsored by the Apache Software Foundation or the Apache Cassandra project. It is an independent framework developed by [AxonOps](https://axonops.com).

- **AxonOps** is a registered trademark of AxonOps Limited.
- **Apache**, **Apache Cassandra**, **Cassandra**, **Apache Spark**, **Spark**, **Apache TinkerPop**, **TinkerPop**, **Apache Kafka** and **Kafka** are either registered trademarks or trademarks of the Apache Software Foundation or its subsidiaries in Canada, the United States and/or other countries.
- **DataStax** is a registered trademark of DataStax, Inc. and its subsidiaries in the United States and/or other countries.

### Copyright

This project is an independent work and has not been authorized, sponsored, or otherwise approved by the Apache Software Foundation.

### License Compliance

This project uses the Apache License 2.0, which is compatible with the Apache Cassandra project. We acknowledge and respect all applicable licenses of dependencies used in this project.

---

<p align="center">
  Made with ❤️ by the <a href="https://axonops.com">AxonOps</a> Team
</p>
