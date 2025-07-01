# Async Python Cassandra© Client

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python Version](https://img.shields.io/pypi/pyversions/async-cassandra)](https://pypi.org/project/async-cassandra/)
[![PyPI Version](https://img.shields.io/pypi/v/async-cassandra)](https://pypi.org/project/async-cassandra/)

> 📢 **Early Release**: This is an early release of async-cassandra. While it has been tested extensively, you may encounter edge cases. We welcome your feedback and contributions! Please report any issues on our [GitHub Issues](https://github.com/axonops/async-python-cassandra-client/issues) page.

## 🎯 Overview

A Python library that enables true async/await support for Cassandra database operations. This package wraps the official DataStax™ Cassandra driver to make it compatible with async frameworks like **FastAPI**, **aiohttp**, and **Quart**.

When using the standard Cassandra driver in async applications, blocking operations can freeze your entire service. This wrapper solves that critical issue by bridging Cassandra's thread-based operations with Python's async ecosystem.

## ✨ Key Features

- 🚀 **True async/await interface** for all Cassandra operations
- 🛡️ **Prevents event loop blocking** in async applications
- ✅ **100% compatible** with the official cassandra-driver types
- 📊 **Streaming support** for memory-efficient processing of large datasets
- 🔄 **Automatic retry logic** for failed queries
- 📡 **Connection monitoring** and health checking
- 📈 **Metrics collection** with Prometheus support
- 🎯 **Type hints** throughout the codebase

## 📋 Requirements

- Python 3.12 or higher
- Apache Cassandra 4.0+ (or compatible distributions)
- Requires CQL protocol v5 or higher

## 📦 Installation

```bash
pip install async-cassandra
```

## 🚀 Quick Start

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

### 🌐 FastAPI Integration

```python
from fastapi import FastAPI
from async_cassandra import AsyncCluster
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    cluster = AsyncCluster(['localhost'])
    app.state.session = await cluster.connect()
    yield
    # Shutdown
    await app.state.session.close()
    await cluster.shutdown()

app = FastAPI(lifespan=lifespan)

@app.get("/users/{user_id}")
async def get_user(user_id: str):
    query = "SELECT * FROM users WHERE id = ?"
    result = await app.state.session.execute(query, [user_id])
    return result.one()
```

## 🤔 Why Use This Library?

The official `cassandra-driver` uses a thread pool for I/O operations, which can cause problems in async applications:

- 🚫 **Event Loop Blocking**: Synchronous operations block the event loop, freezing your entire application
- 🐌 **Poor Concurrency**: Thread pool limits prevent efficient handling of many concurrent requests
- ⚡ **Framework Incompatibility**: Doesn't integrate naturally with async frameworks

This library provides true async/await support while maintaining full compatibility with the official driver.

## ⚠️ Important Limitations

This wrapper makes the cassandra-driver compatible with async Python, but it's important to understand what it does and doesn't do:

**What it DOES:**
- ✅ Prevents blocking the event loop in async applications
- ✅ Provides async/await syntax for all operations
- ✅ Enables use with FastAPI, aiohttp, and other async frameworks
- ✅ Allows concurrent operations via the event loop

**What it DOESN'T do:**
- ❌ Make the underlying I/O truly asynchronous (still uses threads internally)
- ❌ Provide performance improvements over the sync driver
- ❌ Remove thread pool limitations (concurrency still bounded by driver's thread pool size)
- ❌ Eliminate thread overhead - there's still a context switch cost

**Key Understanding:** The official cassandra-driver uses blocking sockets and a thread pool for all I/O operations. This wrapper provides an async interface by running those blocking operations in a thread pool and coordinating with your event loop. This is a compatibility layer, not a reimplementation.

For a detailed technical explanation, see [What This Wrapper Actually Solves (And What It Doesn't)](https://github.com/axonops/async-python-cassandra-client/blob/main/docs/why-async-wrapper.md) in our documentation.

## 📚 Documentation

For comprehensive documentation, examples, and advanced usage, please visit our GitHub repository:

### 🔗 **[Full Documentation on GitHub](https://github.com/axonops/async-python-cassandra-client)**

Key documentation sections:
- 📖 [Getting Started Guide](https://github.com/axonops/async-python-cassandra-client/blob/main/docs/getting-started.md)
- 🔧 [API Reference](https://github.com/axonops/async-python-cassandra-client/blob/main/docs/api.md)
- 🚀 [FastAPI Integration Example](https://github.com/axonops/async-python-cassandra-client/tree/main/examples/fastapi_app)
- ⚡ [Performance Guide](https://github.com/axonops/async-python-cassandra-client/blob/main/docs/performance.md)
- 🔍 [Troubleshooting](https://github.com/axonops/async-python-cassandra-client/blob/main/docs/troubleshooting.md)

## 📄 License

This project is licensed under the Apache License 2.0. See the [LICENSE](https://github.com/axonops/async-python-cassandra-client/blob/main/LICENSE) file for details.

## 🏢 About

Developed and maintained by [AxonOps](https://axonops.com). We're committed to providing high-quality tools for the Cassandra community.

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](https://github.com/axonops/async-python-cassandra-client/blob/main/CONTRIBUTING.md) on GitHub.

## 💬 Support

- **Issues**: [GitHub Issues](https://github.com/axonops/async-python-cassandra-client/issues)
- **Discussions**: [GitHub Discussions](https://github.com/axonops/async-python-cassandra-client/discussions)

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

---

<p align="center">
  Made with ❤️ by the <a href="https://axonops.com">AxonOps</a> Team
</p>
