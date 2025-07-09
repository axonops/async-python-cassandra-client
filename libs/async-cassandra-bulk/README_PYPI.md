# async-cassandra-bulk (🚧 Active Development)

[![PyPI version](https://badge.fury.io/py/async-cassandra-bulk.svg)](https://badge.fury.io/py/async-cassandra-bulk)
[![Python versions](https://img.shields.io/pypi/pyversions/async-cassandra-bulk.svg)](https://pypi.org/project/async-cassandra-bulk/)
[![License](https://img.shields.io/pypi/l/async-cassandra-bulk.svg)](https://github.com/axonops/async-python-cassandra-client/blob/main/LICENSE)

High-performance bulk operations extension for Apache Cassandra, built on [async-cassandra](https://pypi.org/project/async-cassandra/).

> 🚧 **Active Development**: This package is currently under active development and not yet feature-complete. The API may change as we work towards a stable release. For production use, we recommend using [async-cassandra](https://pypi.org/project/async-cassandra/) directly.

## 🎯 Overview

**async-cassandra-bulk** will provide high-performance data import/export capabilities for Apache Cassandra databases. Once complete, it will leverage token-aware parallel processing to achieve optimal throughput while maintaining memory efficiency.

## ✨ Key Features (Coming Soon)

- 🚀 **Token-aware parallel processing** for maximum throughput
- 📊 **Memory-efficient streaming** for large datasets
- 🔄 **Resume capability** with checkpointing
- 📁 **Multiple formats**: CSV, JSON, Parquet, Apache Iceberg
- ☁️ **Cloud storage support**: S3, GCS, Azure Blob
- 📈 **Progress tracking** with customizable callbacks

## 📦 Installation

```bash
pip install async-cassandra-bulk
```

## 🚀 Quick Start

```python
import asyncio
from async_cassandra_bulk import hello

async def main():
    # This is a placeholder function for testing
    message = await hello()
    print(message)  # "Hello from async-cassandra-bulk!"

if __name__ == "__main__":
    asyncio.run(main())
```

> **Note**: Full functionality is coming soon! This is currently a skeleton package in active development.

## 📖 Documentation

See the [project documentation](https://github.com/axonops/async-python-cassandra-client) for detailed information.

## 🤝 Related Projects

- [async-cassandra](https://pypi.org/project/async-cassandra/) - The async Cassandra driver this package builds upon

## 📄 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](https://github.com/axonops/async-python-cassandra-client/blob/main/LICENSE) file for details.
