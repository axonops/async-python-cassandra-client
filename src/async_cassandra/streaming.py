"""
Simplified streaming support for large result sets in async-cassandra.

This implementation focuses on essential streaming functionality
without complex state tracking.
"""

import asyncio
import logging
import threading
from dataclasses import dataclass
from typing import Any, AsyncIterator, Callable, List, Optional

from cassandra.cluster import ResponseFuture
from cassandra.query import ConsistencyLevel, SimpleStatement

logger = logging.getLogger(__name__)


@dataclass
class StreamConfig:
    """Configuration for streaming results."""

    fetch_size: int = 1000  # Number of rows per page
    max_pages: Optional[int] = None  # Limit number of pages (None = no limit)
    page_callback: Optional[Callable[[int, int], None]] = None  # Progress callback
    timeout_seconds: Optional[float] = None  # Timeout for the entire streaming operation


class AsyncStreamingResultSet:
    """
    Simplified streaming result set that fetches pages on demand.

    This class provides memory-efficient iteration over large result sets
    by fetching pages as needed rather than loading all results at once.
    """

    def __init__(self, response_future: ResponseFuture, config: Optional[StreamConfig] = None):
        """
        Initialize streaming result set.

        Args:
            response_future: The Cassandra response future
            config: Streaming configuration
        """
        self.response_future = response_future
        self.config = config or StreamConfig()

        self._current_page: List[Any] = []
        self._current_index = 0
        self._page_number = 0
        self._total_rows = 0
        self._exhausted = False
        self._error: Optional[Exception] = None
        self._closed = False

        # Thread lock for thread-safe operations (necessary for driver callbacks)
        self._lock = threading.Lock()

        # Event to signal when a page is ready
        self._page_ready: Optional[asyncio.Event] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None

        # Start fetching the first page
        self._setup_callbacks()

    def _cleanup_callbacks(self) -> None:
        """Clean up response future callbacks to prevent memory leaks."""
        try:
            # Clear callbacks if the method exists
            if hasattr(self.response_future, "clear_callbacks"):
                self.response_future.clear_callbacks()
        except Exception:
            # Ignore errors during cleanup
            pass

    def __del__(self) -> None:
        """Ensure callbacks are cleaned up when object is garbage collected."""
        # Clean up callbacks to break circular references
        self._cleanup_callbacks()

    def _setup_callbacks(self) -> None:
        """Set up callbacks for the current page."""
        self.response_future.add_callbacks(callback=self._handle_page, errback=self._handle_error)

        # Check if the response_future already has an error
        # This can happen with very short timeouts
        if (
            hasattr(self.response_future, "_final_exception")
            and self.response_future._final_exception
        ):
            self._handle_error(self.response_future._final_exception)

    def _handle_page(self, rows: Optional[List[Any]]) -> None:
        """Handle successful page retrieval.

        This method is called from driver threads, so we need thread safety.
        """
        with self._lock:
            if rows is not None:
                # Replace the current page (don't accumulate)
                self._current_page = list(rows)  # Defensive copy
                self._current_index = 0
                self._page_number += 1
                self._total_rows += len(rows)

                # Check if we've reached the page limit
                if self.config.max_pages and self._page_number >= self.config.max_pages:
                    self._exhausted = True
            else:
                self._current_page = []
                self._exhausted = True

        # Call progress callback if configured
        if self.config.page_callback:
            try:
                self.config.page_callback(self._page_number, len(rows) if rows else 0)
            except Exception as e:
                logger.warning(f"Page callback error: {e}")

        # Signal that the page is ready
        if self._page_ready and self._loop:
            self._loop.call_soon_threadsafe(self._page_ready.set)

    def _handle_error(self, exc: Exception) -> None:
        """Handle query execution error."""
        with self._lock:
            self._error = exc
            self._exhausted = True
            # Clear current page to prevent memory leak
            self._current_page = []
            self._current_index = 0

        if self._page_ready and self._loop:
            self._loop.call_soon_threadsafe(self._page_ready.set)

        # Clean up callbacks to prevent memory leaks
        self._cleanup_callbacks()

    async def _fetch_next_page(self) -> bool:
        """
        Fetch the next page of results.

        Returns:
            True if a page was fetched, False if no more pages.
        """
        if self._exhausted:
            return False

        if not self.response_future.has_more_pages:
            self._exhausted = True
            return False

        # Initialize event if needed
        if self._page_ready is None:
            self._page_ready = asyncio.Event()
            self._loop = asyncio.get_running_loop()

        # Clear the event before fetching
        self._page_ready.clear()

        # Start fetching the next page
        self.response_future.start_fetching_next_page()

        # Wait for the page to be ready
        if self.config.timeout_seconds:
            await asyncio.wait_for(self._page_ready.wait(), timeout=self.config.timeout_seconds)
        else:
            await self._page_ready.wait()

        # Check for errors
        if self._error:
            raise self._error

        return len(self._current_page) > 0

    def __aiter__(self) -> AsyncIterator[Any]:
        """Return async iterator for streaming results."""
        return self

    async def __anext__(self) -> Any:
        """Get next row from the streaming result set."""
        # Initialize event if needed
        if self._page_ready is None:
            self._page_ready = asyncio.Event()
            self._loop = asyncio.get_running_loop()

        # Wait for first page if needed
        if self._page_number == 0 and not self._current_page:
            # Use timeout from config if available
            if self.config.timeout_seconds:
                await asyncio.wait_for(self._page_ready.wait(), timeout=self.config.timeout_seconds)
            else:
                await self._page_ready.wait()

        # Check for errors first
        if self._error:
            raise self._error

        # If we have rows in the current page, return one
        if self._current_index < len(self._current_page):
            row = self._current_page[self._current_index]
            self._current_index += 1
            return row

        # If current page is exhausted, try to fetch next page
        if not self._exhausted and await self._fetch_next_page():
            # Recursively call to get the first row from new page
            return await self.__anext__()

        # No more rows
        raise StopAsyncIteration

    async def pages(self) -> AsyncIterator[List[Any]]:
        """
        Iterate over pages instead of individual rows.

        Yields:
            Lists of row objects (pages).
        """
        # Initialize event if needed
        if self._page_ready is None:
            self._page_ready = asyncio.Event()
            self._loop = asyncio.get_running_loop()

        # Wait for first page if needed
        if self._page_number == 0 and not self._current_page:
            if self.config.timeout_seconds:
                await asyncio.wait_for(self._page_ready.wait(), timeout=self.config.timeout_seconds)
            else:
                await self._page_ready.wait()

        # Yield the current page if it has data
        if self._current_page:
            yield self._current_page

        # Fetch and yield subsequent pages
        while await self._fetch_next_page():
            if self._current_page:
                yield self._current_page

    @property
    def page_number(self) -> int:
        """Get the current page number."""
        return self._page_number

    @property
    def total_rows_fetched(self) -> int:
        """Get the total number of rows fetched so far."""
        return self._total_rows

    async def cancel(self) -> None:
        """Cancel the streaming operation."""
        self._exhausted = True
        self._cleanup_callbacks()

    async def __aenter__(self) -> "AsyncStreamingResultSet":
        """Enter async context manager."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit async context manager and clean up resources."""
        await self.close()

    async def close(self) -> None:
        """Close the streaming result set and clean up resources."""
        if self._closed:
            return

        self._closed = True
        self._exhausted = True

        # Clean up callbacks
        self._cleanup_callbacks()

        # Clear current page to free memory
        with self._lock:
            self._current_page = []
            self._current_index = 0

        # Signal any waiters
        if self._page_ready is not None:
            self._page_ready.set()


class StreamingResultHandler:
    """
    Handler for creating streaming result sets.

    This is an alternative to AsyncResultHandler that doesn't
    load all results into memory.
    """

    def __init__(self, response_future: ResponseFuture, config: Optional[StreamConfig] = None):
        """
        Initialize streaming result handler.

        Args:
            response_future: The Cassandra response future
            config: Streaming configuration
        """
        self.response_future = response_future
        self.config = config or StreamConfig()

    async def get_streaming_result(self) -> AsyncStreamingResultSet:
        """
        Get the streaming result set.

        Returns:
            AsyncStreamingResultSet for efficient iteration.
        """
        # Simply create and return the streaming result set
        # It will handle its own callbacks
        return AsyncStreamingResultSet(self.response_future, self.config)


def create_streaming_statement(
    query: str, fetch_size: int = 1000, consistency_level: Optional[ConsistencyLevel] = None
) -> SimpleStatement:
    """
    Create a statement configured for streaming.

    Args:
        query: The CQL query
        fetch_size: Number of rows per page
        consistency_level: Optional consistency level

    Returns:
        SimpleStatement configured for streaming
    """
    statement = SimpleStatement(query, fetch_size=fetch_size)

    if consistency_level is not None:
        statement.consistency_level = consistency_level

    return statement
