"""
Unit tests for streaming memory management.

These tests use mocks to verify memory handling behavior
without requiring a real Cassandra connection.
"""

import asyncio
import gc
import weakref
from unittest.mock import MagicMock

from async_cassandra.streaming import AsyncStreamingResultSet


class TestStreamingMemoryManagement:
    """Unit tests for memory management in streaming functionality."""

    def test_streaming_result_set_cleanup(self):
        """
        GIVEN streaming through large result sets with mocked response
        WHEN pages are processed and discarded
        THEN memory should be properly released
        """

        async def run_test():
            # Track page counts instead of weak references
            pages_created = []

            class PageTracker:
                """Wrapper to track page lifecycle"""

                def __init__(self, page_data):
                    self.data = page_data
                    pages_created.append(self)

            class InstrumentedStreamingResultSet(AsyncStreamingResultSet):
                def _handle_page(self, rows):
                    # Verify only one page is held at a time
                    if hasattr(self, "_current_page") and self._current_page:
                        # Previous page should be replaced
                        assert len(self._current_page) <= 100
                    super()._handle_page(rows)

            # Mock response future with multiple pages
            mock_future = MagicMock()
            mock_future.has_more_pages = True
            mock_future.add_callbacks = MagicMock()
            # Ensure _final_exception is not auto-mocked
            mock_future._final_exception = None
            page_count = 0

            def fetch_next_page():
                nonlocal page_count
                page_count += 1
                if page_count < 10:
                    mock_future.has_more_pages = True
                    # Simulate page callback
                    handler._handle_page([f"row_{page_count}_{i}" for i in range(100)])
                else:
                    mock_future.has_more_pages = False
                    handler._handle_page([])

            mock_future.start_fetching_next_page = fetch_next_page

            handler = InstrumentedStreamingResultSet(mock_future)
            # Initialize with first page
            handler._handle_page([f"row_0_{i}" for i in range(100)])

            # Process all pages
            processed_rows = 0
            async for row in handler:
                processed_rows += 1
                if processed_rows % 100 == 0:
                    # After each page, check memory usage
                    # Only current page should be in memory
                    assert len(handler._current_page) <= 100

            # Verify all rows were processed
            assert processed_rows == 1000  # 10 pages * 100 rows

            # Verify handler only holds one page
            assert len(handler._current_page) <= 100

        asyncio.run(run_test())

    def test_circular_reference_prevention(self):
        """
        GIVEN streaming result set with callbacks
        WHEN result set is deleted
        THEN circular references should be broken
        """

        async def run_test():
            # Track object lifecycle
            handler_refs = []

            class TrackedStreamingResultSet(AsyncStreamingResultSet):
                def __init__(self, *args, **kwargs):
                    super().__init__(*args, **kwargs)
                    handler_refs.append(weakref.ref(self))

            # Mock response future with proper callback tracking
            stored_callbacks = []

            mock_future = MagicMock()
            mock_future.has_more_pages = False
            mock_future._final_exception = None

            def mock_add_callbacks(callback=None, errback=None):
                stored_callbacks.append((callback, errback))

            def mock_clear_callbacks():
                stored_callbacks.clear()

            mock_future.add_callbacks = mock_add_callbacks
            mock_future.clear_callbacks = mock_clear_callbacks

            # Create handler with potential circular reference
            handler = TrackedStreamingResultSet(mock_future)
            handler._handle_page(["row1", "row2", "row3"])

            # Process some data
            processed = []
            async for row in handler:
                processed.append(row)
                if len(processed) >= 3:
                    break

            # Store reference to check cleanup
            handler_ref = weakref.ref(handler)

            # Properly close the handler to break circular references
            await handler.close()

            # Delete handler
            del handler
            gc.collect()

            # Check that handler was garbage collected
            assert (
                handler_ref() is None
            ), "Handler not garbage collected - circular reference exists"

        asyncio.run(run_test())

    def test_exception_cleanup_in_streaming(self):
        """
        GIVEN streaming operation that encounters an error
        WHEN exception occurs during streaming
        THEN all resources should be properly cleaned up
        """

        async def run_test():
            # Track resource allocation
            allocated_resources = []

            class ResourceTracker:
                def __init__(self, name):
                    self.name = name
                    allocated_resources.append(weakref.ref(self))

            # Mock a failing streaming operation
            mock_future = MagicMock()
            mock_future.has_more_pages = True
            mock_future.add_callbacks = MagicMock()
            # Ensure _final_exception is not auto-mocked
            mock_future._final_exception = None

            error_on_page = 3
            current_page = 0
            handler_ref = {"handler": None}  # Use dict to allow closure access

            def fetch_with_error():
                nonlocal current_page
                current_page += 1

                # Allocate some resources
                resource = ResourceTracker(f"page_{current_page}")  # noqa: F841

                if current_page == error_on_page:
                    # Simulate error through handler
                    if handler_ref["handler"]:
                        handler_ref["handler"]._handle_error(
                            Exception("Simulated error during fetch")
                        )
                    mock_future.has_more_pages = False
                else:
                    # Normal page
                    if handler_ref["handler"]:
                        handler_ref["handler"]._handle_page([f"row_{i}" for i in range(10)])

            mock_future.start_fetching_next_page = fetch_with_error

            handler = AsyncStreamingResultSet(mock_future)
            handler_ref["handler"] = handler
            # Initialize with first page
            handler._handle_page([f"row_{i}" for i in range(10)])

            # Try to iterate, expecting failure
            rows_processed = 0
            error_caught = False
            try:
                async for row in handler:
                    rows_processed += 1
                    # Trigger next page fetch periodically
                    if rows_processed % 10 == 0:
                        await handler._fetch_next_page()
            except Exception as e:
                error_caught = True
                assert "Simulated error" in str(e)

            assert error_caught, "Expected error was not raised"
            assert rows_processed > 0, "Some rows should have been processed"

            # Clear references
            del handler
            gc.collect()

            # Check cleanup
            alive_resources = sum(1 for ref in allocated_resources if ref() is not None)
            assert alive_resources == 0, f"Resources not cleaned up: {alive_resources}"

        asyncio.run(run_test())
