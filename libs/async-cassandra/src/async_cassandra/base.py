"""
Simplified base classes for async-cassandra.

This module provides minimal functionality needed for the async wrapper,
avoiding over-engineering and complex locking patterns.
"""

from typing import Any, TypeVar

T = TypeVar("T")


class AsyncContextManageable:
    """
    Simple mixin to add async context manager support.

    Classes using this mixin must implement an async close() method.
    """

    async def __aenter__(self: T) -> T:
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.close()  # type: ignore
