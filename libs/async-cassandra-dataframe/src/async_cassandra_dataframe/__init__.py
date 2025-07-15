"""
async-cassandra-dataframe: Dask DataFrame integration for Apache Cassandra.

This library provides distributed processing capabilities for Cassandra data
using Dask DataFrames, built on top of async-cassandra.
"""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("async-cassandra-dataframe")
except PackageNotFoundError:
    __version__ = "unknown"

# Main API
from .reader import read_cassandra_table, stream_cassandra_table

__all__ = [
    "__version__",
    "read_cassandra_table",
    "stream_cassandra_table",
]
