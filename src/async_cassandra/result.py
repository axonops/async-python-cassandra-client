"""
Simplified async result handling for Cassandra queries.

This implementation focuses on essential functionality without
complex state tracking.
"""

import asyncio
import threading
from typing import Any, AsyncIterator, List, Optional

from cassandra.cluster import ResponseFuture


class AsyncResultHandler:
    """
    Simplified handler for asynchronous results from Cassandra queries.

    This class wraps ResponseFuture callbacks in asyncio Futures,
    providing async/await support with minimal complexity.
    """

    def __init__(self, response_future: ResponseFuture):
        self.response_future = response_future
        self.rows: List[Any] = []
        self._future: Optional[asyncio.Future[AsyncResultSet]] = None
        # Thread lock is necessary since callbacks come from driver threads
        self._lock = threading.Lock()
        # Store early results/errors if callbacks fire before get_result
        self._early_result: Optional[AsyncResultSet] = None
        self._early_error: Optional[Exception] = None

        # Set up callbacks
        self.response_future.add_callbacks(callback=self._handle_page, errback=self._handle_error)

    def _cleanup_callbacks(self) -> None:
        """Clean up response future callbacks to prevent memory leaks."""
        try:
            # Clear callbacks if the method exists
            if hasattr(self.response_future, "clear_callbacks"):
                self.response_future.clear_callbacks()
        except Exception:
            # Ignore errors during cleanup
            pass

    def _handle_page(self, rows: List[Any]) -> None:
        """Handle successful page retrieval.

        This method is called from driver threads, so we need thread safety.
        """
        with self._lock:
            if rows is not None:
                # Create a defensive copy to avoid cross-thread data issues
                self.rows.extend(list(rows))

            if self.response_future.has_more_pages:
                self.response_future.start_fetching_next_page()
            else:
                # All pages fetched
                # Create a copy of rows to avoid reference issues
                final_result = AsyncResultSet(list(self.rows), self.response_future)

                if self._future and not self._future.done():
                    loop = getattr(self, "_loop", None)
                    if loop:
                        loop.call_soon_threadsafe(self._future.set_result, final_result)
                else:
                    # Store for later if future doesn't exist yet
                    self._early_result = final_result

                # Clean up callbacks after completion
                self._cleanup_callbacks()

    def _handle_error(self, exc: Exception) -> None:
        """Handle query execution error."""
        with self._lock:
            if self._future and not self._future.done():
                loop = getattr(self, "_loop", None)
                if loop:
                    loop.call_soon_threadsafe(self._future.set_exception, exc)
            else:
                # Store for later if future doesn't exist yet
                self._early_error = exc

        # Clean up callbacks to prevent memory leaks
        self._cleanup_callbacks()

    async def get_result(self, timeout: Optional[float] = None) -> "AsyncResultSet":
        """
        Wait for the query to complete and return the result.

        Args:
            timeout: Optional timeout in seconds.

        Returns:
            AsyncResultSet containing all rows from the query.

        Raises:
            asyncio.TimeoutError: If the query doesn't complete within the timeout.
        """
        # Create future in the current event loop
        loop = asyncio.get_running_loop()
        self._future = loop.create_future()
        self._loop = loop  # Store loop for callbacks

        # Check if result/error is already available (callback might have fired early)
        with self._lock:
            if self._early_error:
                self._future.set_exception(self._early_error)
            elif self._early_result:
                self._future.set_result(self._early_result)
            # Remove the early check for empty results - let callbacks handle it

        # Use query timeout if no explicit timeout provided
        if (
            timeout is None
            and hasattr(self.response_future, "timeout")
            and self.response_future.timeout is not None
        ):
            timeout = self.response_future.timeout

        try:
            if timeout is not None:
                return await asyncio.wait_for(self._future, timeout=timeout)
            else:
                return await self._future
        except asyncio.TimeoutError:
            # Clean up on timeout
            self._cleanup_callbacks()
            raise
        except Exception:
            # Clean up on any error
            self._cleanup_callbacks()
            raise


class AsyncResultSet:
    """
    Async wrapper for Cassandra query results.

    Provides async iteration over result rows and metadata access.
    """

    def __init__(self, rows: List[Any], response_future: Any = None):
        self._rows = rows
        self._index = 0
        self._response_future = response_future

    def __aiter__(self) -> AsyncIterator[Any]:
        """Return async iterator for the result set."""
        self._index = 0  # Reset index for each iteration
        return self

    async def __anext__(self) -> Any:
        """Get next row from the result set."""
        if self._index >= len(self._rows):
            raise StopAsyncIteration

        row = self._rows[self._index]
        self._index += 1
        return row

    def __len__(self) -> int:
        """Return number of rows in the result set."""
        return len(self._rows)

    def __getitem__(self, index: int) -> Any:
        """Get row by index."""
        return self._rows[index]

    @property
    def rows(self) -> List[Any]:
        """Get all rows as a list."""
        return self._rows

    def one(self) -> Optional[Any]:
        """
        Get the first row or None if empty.

        Returns:
            First row from the result set or None.
        """
        return self._rows[0] if self._rows else None

    def all(self) -> List[Any]:
        """
        Get all rows.

        Returns:
            List of all rows in the result set.
        """
        return self._rows

    def get_query_trace(self) -> Any:
        """
        Get the query trace if available.

        Returns:
            Query trace object or None if tracing wasn't enabled.
        """
        if self._response_future and hasattr(self._response_future, "get_query_trace"):
            return self._response_future.get_query_trace()
        return None
