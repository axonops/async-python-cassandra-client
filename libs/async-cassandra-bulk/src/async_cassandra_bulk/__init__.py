"""async-cassandra-bulk - High-performance bulk operations for Apache Cassandra."""

from importlib.metadata import PackageNotFoundError, version

from .exporters import BaseExporter, CSVExporter, JSONExporter
from .operators import BulkOperator
from .parallel_export import ParallelExporter
from .utils.stats import BulkOperationStats
from .utils.token_utils import TokenRange, discover_token_ranges

try:
    __version__ = version("async-cassandra-bulk")
except PackageNotFoundError:
    # Package is not installed
    __version__ = "0.0.0+unknown"


async def hello() -> str:
    """Simple hello world for Phase 1 testing."""
    return "Hello from async-cassandra-bulk!"


__all__ = [
    "BulkOperator",
    "BaseExporter",
    "CSVExporter",
    "JSONExporter",
    "ParallelExporter",
    "BulkOperationStats",
    "TokenRange",
    "discover_token_ranges",
    "hello",
    "__version__",
]
