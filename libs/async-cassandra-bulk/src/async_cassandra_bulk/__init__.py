"""async-cassandra-bulk - High-performance bulk operations for Apache Cassandra."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("async-cassandra-bulk")
except PackageNotFoundError:
    # Package is not installed
    __version__ = "0.0.0+unknown"


async def hello() -> str:
    """Simple hello world for Phase 1 testing."""
    return "Hello from async-cassandra-bulk!"


__all__ = ["hello", "__version__"]
