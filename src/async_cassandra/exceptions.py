"""
Exception classes for async-cassandra.
"""

from typing import Optional


class AsyncCassandraError(Exception):
    """Base exception for all async-cassandra errors."""

    def __init__(self, message: str, cause: Optional[Exception] = None):
        super().__init__(message)
        self.cause = cause


class ConnectionError(AsyncCassandraError):
    """Raised when connection to Cassandra fails."""

    pass


class QueryError(AsyncCassandraError):
    """Raised when a query execution fails."""

    pass


class TimeoutError(AsyncCassandraError):
    """Raised when an operation times out."""

    pass


class AuthenticationError(AsyncCassandraError):
    """Raised when authentication fails."""

    pass


class ConfigurationError(AsyncCassandraError):
    """Raised when configuration is invalid."""

    pass
