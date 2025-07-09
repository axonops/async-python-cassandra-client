"""
Token-aware bulk operations for Apache Cassandra using async-cassandra.

This package provides efficient, parallel bulk operations by leveraging
Cassandra's token ranges for data distribution.
"""

__version__ = "0.1.0"

from .bulk_operator import BulkOperationStats, TokenAwareBulkOperator
from .token_utils import TokenRange, TokenRangeSplitter

__all__ = [
    "TokenAwareBulkOperator",
    "BulkOperationStats",
    "TokenRange",
    "TokenRangeSplitter",
]
