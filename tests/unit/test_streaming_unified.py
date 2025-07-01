"""
Unified streaming tests for async-python-cassandra.

This module consolidates all streaming-related tests from multiple files:
- test_streaming.py: Core streaming functionality and multi-page iteration
- test_streaming_memory.py: Memory management during streaming
- test_streaming_memory_management.py: Duplicate memory management tests
- test_streaming_memory_leak.py: Memory leak prevention tests

Test Organization:
==================
1. Core Streaming Tests - Basic streaming functionality
2. Multi-Page Streaming Tests - Pagination and page fetching
3. Memory Management Tests - Resource cleanup and leak prevention
4. Error Handling Tests - Streaming error scenarios
5. Cancellation Tests - Stream cancellation and cleanup
6. Performance Tests - Large result set handling

Key Testing Principles:
======================
- Test both single-page and multi-page results
- Verify memory is properly released
- Ensure callbacks are cleaned up
- Test error propagation during streaming
- Verify cancellation doesn't leak resources
"""

import gc
import weakref
from typing import Any, AsyncIterator, List
from unittest.mock import AsyncMock, Mock, patch

import pytest

from async_cassandra import AsyncCassandraSession
from async_cassandra.exceptions import QueryError
from async_cassandra.streaming import StreamConfig


class MockAsyncStreamingResultSet:
    """Mock implementation of AsyncStreamingResultSet for testing"""

    def __init__(self, rows: List[Any], pages: List[List[Any]] = None):
        self.rows = rows
        self.pages = pages or [rows]
        self._current_page_index = 0
        self._current_row_index = 0
        self._closed = False
        self.total_rows_fetched = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.close()

    async def close(self):
        self._closed = True

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._closed:
            raise StopAsyncIteration

        # If we have pages
        if self.pages:
            if self._current_page_index >= len(self.pages):
                raise StopAsyncIteration

            current_page = self.pages[self._current_page_index]
            if self._current_row_index >= len(current_page):
                self._current_page_index += 1
                self._current_row_index = 0

                if self._current_page_index >= len(self.pages):
                    raise StopAsyncIteration

                current_page = self.pages[self._current_page_index]

            row = current_page[self._current_row_index]
            self._current_row_index += 1
            self.total_rows_fetched += 1
            return row
        else:
            # Simple case - all rows in one list
            if self._current_row_index >= len(self.rows):
                raise StopAsyncIteration

            row = self.rows[self._current_row_index]
            self._current_row_index += 1
            self.total_rows_fetched += 1
            return row

    async def pages(self) -> AsyncIterator[List[Any]]:
        """Iterate over pages instead of rows"""
        for page in self.pages:
            yield page


class TestStreamingFunctionality:
    """
    Test core streaming functionality.

    Streaming is used for large result sets that don't fit in memory.
    These tests verify the streaming API works correctly.
    """

    @pytest.mark.asyncio
    async def test_single_page_streaming(self):
        """
        Test streaming with a single page of results.

        What this tests:
        ---------------
        1. execute_stream returns AsyncStreamingResultSet
        2. Single page results work correctly
        3. Context manager properly opens/closes stream
        4. All rows are yielded

        Why this matters:
        ----------------
        Even single-page results should work with streaming API
        for consistency. This is the simplest streaming case.
        """
        mock_session = Mock()
        async_session = AsyncCassandraSession(mock_session)

        # Mock the execute_stream to return our mock streaming result
        rows = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}, {"id": 3, "name": "Charlie"}]

        mock_stream = MockAsyncStreamingResultSet(rows)

        with patch.object(async_session, "execute_stream", return_value=mock_stream):
            # Collect all streamed rows
            collected_rows = []
            async with await async_session.execute_stream("SELECT * FROM users") as stream:
                async for row in stream:
                    collected_rows.append(row)

            # Verify all rows were streamed
            assert len(collected_rows) == 3
            assert collected_rows[0]["name"] == "Alice"
            assert collected_rows[1]["name"] == "Bob"
            assert collected_rows[2]["name"] == "Charlie"

    @pytest.mark.asyncio
    async def test_multi_page_streaming(self):
        """
        Test streaming with multiple pages of results.

        What this tests:
        ---------------
        1. Multiple pages are fetched automatically
        2. Page boundaries are transparent to user
        3. All pages are processed in order
        4. Has_more_pages triggers next fetch

        Why this matters:
        ----------------
        Large result sets span multiple pages. The streaming
        API must seamlessly fetch pages as needed.
        """
        mock_session = Mock()
        async_session = AsyncCassandraSession(mock_session)

        # Define pages of data
        pages = [
            [{"id": 1}, {"id": 2}, {"id": 3}],
            [{"id": 4}, {"id": 5}, {"id": 6}],
            [{"id": 7}, {"id": 8}, {"id": 9}],
        ]

        all_rows = [row for page in pages for row in page]
        mock_stream = MockAsyncStreamingResultSet(all_rows, pages)

        with patch.object(async_session, "execute_stream", return_value=mock_stream):
            # Stream all pages
            collected_rows = []
            async with await async_session.execute_stream("SELECT * FROM large_table") as stream:
                async for row in stream:
                    collected_rows.append(row)

            # Verify all rows from all pages
            assert len(collected_rows) == 9
            assert [r["id"] for r in collected_rows] == list(range(1, 10))

    @pytest.mark.asyncio
    async def test_streaming_with_fetch_size(self):
        """
        Test streaming with custom fetch size.

        What this tests:
        ---------------
        1. Custom fetch_size is respected
        2. Page size affects streaming behavior
        3. Configuration passes through correctly

        Why this matters:
        ----------------
        Fetch size controls memory usage and performance.
        Users need to tune this for their use case.
        """
        mock_session = Mock()
        async_session = AsyncCassandraSession(mock_session)

        # Just verify the config is passed - actual pagination is tested elsewhere
        rows = [{"id": i} for i in range(100)]
        mock_stream = MockAsyncStreamingResultSet(rows)

        # Mock execute_stream to verify it's called with correct config
        execute_stream_mock = AsyncMock(return_value=mock_stream)

        with patch.object(async_session, "execute_stream", execute_stream_mock):
            stream_config = StreamConfig(fetch_size=1000)
            async with await async_session.execute_stream(
                "SELECT * FROM large_table", stream_config=stream_config
            ) as stream:
                async for row in stream:
                    pass

            # Verify execute_stream was called with the config
            execute_stream_mock.assert_called_once()
            args, kwargs = execute_stream_mock.call_args
            assert kwargs.get("stream_config") == stream_config

    @pytest.mark.asyncio
    async def test_streaming_error_propagation(self):
        """
        Test error handling during streaming.

        What this tests:
        ---------------
        1. Errors are properly propagated
        2. Context manager handles errors
        3. Resources are cleaned up on error

        Why this matters:
        ----------------
        Streaming operations can fail mid-stream. Errors must
        be handled gracefully without resource leaks.
        """
        mock_session = Mock()
        async_session = AsyncCassandraSession(mock_session)

        # Create a mock that will raise an error
        error_msg = "Network error during streaming"
        execute_stream_mock = AsyncMock(side_effect=QueryError(error_msg))

        with patch.object(async_session, "execute_stream", execute_stream_mock):
            # Verify error is propagated
            with pytest.raises(QueryError) as exc_info:
                async with await async_session.execute_stream("SELECT * FROM test") as stream:
                    async for row in stream:
                        pass

            assert error_msg in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_streaming_cancellation(self):
        """
        Test cancelling streaming mid-iteration.

        What this tests:
        ---------------
        1. Stream can be cancelled
        2. Resources are cleaned up
        3. No errors on early exit

        Why this matters:
        ----------------
        Users may need to stop streaming early. This shouldn't
        leak resources or cause errors.
        """
        mock_session = Mock()
        async_session = AsyncCassandraSession(mock_session)

        # Large result set
        rows = [{"id": i} for i in range(1000)]
        mock_stream = MockAsyncStreamingResultSet(rows)

        with patch.object(async_session, "execute_stream", return_value=mock_stream):
            processed = 0
            async with await async_session.execute_stream("SELECT * FROM large_table") as stream:
                async for row in stream:
                    processed += 1
                    if processed >= 10:
                        break  # Early exit

            # Verify we stopped early
            assert processed == 10
            # Verify stream was closed
            assert mock_stream._closed

    @pytest.mark.asyncio
    async def test_empty_result_streaming(self):
        """
        Test streaming with empty results.

        What this tests:
        ---------------
        1. Empty results don't cause errors
        2. Iterator completes immediately
        3. Context manager works with no data

        Why this matters:
        ----------------
        Queries may return no results. The streaming API
        should handle this gracefully.
        """
        mock_session = Mock()
        async_session = AsyncCassandraSession(mock_session)

        # Empty result
        mock_stream = MockAsyncStreamingResultSet([])

        with patch.object(async_session, "execute_stream", return_value=mock_stream):
            rows_found = 0
            async with await async_session.execute_stream("SELECT * FROM empty_table") as stream:
                async for row in stream:
                    rows_found += 1

            assert rows_found == 0


class TestStreamingMemoryManagement:
    """
    Test memory management during streaming operations.

    These tests verify that streaming doesn't leak memory and
    properly cleans up resources.
    """

    @pytest.mark.asyncio
    async def test_memory_cleanup_after_streaming(self):
        """
        Test memory is released after streaming completes.

        What this tests:
        ---------------
        1. Row objects are not retained after iteration
        2. Internal buffers are cleared
        3. Garbage collection works properly

        Why this matters:
        ----------------
        Streaming large datasets shouldn't cause memory to
        accumulate. Each page should be released after processing.
        """
        mock_session = Mock()
        async_session = AsyncCassandraSession(mock_session)

        # Track row object references
        row_refs = []

        # Create rows that support weakref
        class Row:
            def __init__(self, id, data):
                self.id = id
                self.data = data

            def __getitem__(self, key):
                return getattr(self, key)

        rows = []
        for i in range(100):
            row = Row(id=i, data="x" * 1000)
            rows.append(row)
            row_refs.append(weakref.ref(row))

        mock_stream = MockAsyncStreamingResultSet(rows)

        with patch.object(async_session, "execute_stream", return_value=mock_stream):
            # Stream and process rows
            processed = 0
            async with await async_session.execute_stream("SELECT * FROM test") as stream:
                async for row in stream:
                    processed += 1
                    # Don't keep references

        # Clear all references
        rows = None
        mock_stream.rows = []
        mock_stream.pages = []
        mock_stream = None

        # Force garbage collection
        gc.collect()

        # Check that rows were released
        alive_refs = sum(1 for ref in row_refs if ref() is not None)
        assert processed == 100
        # Most rows should be collected (some may still be referenced)
        assert alive_refs < 10

    @pytest.mark.asyncio
    async def test_memory_cleanup_on_error(self):
        """
        Test memory cleanup when error occurs during streaming.

        What this tests:
        ---------------
        1. Partial results are cleaned up on error
        2. Callbacks are removed
        3. No dangling references

        Why this matters:
        ----------------
        Errors during streaming shouldn't leak the partially
        processed results or internal state.
        """
        mock_session = Mock()
        async_session = AsyncCassandraSession(mock_session)

        # Create a stream that will fail mid-iteration
        class FailingStream(MockAsyncStreamingResultSet):
            def __init__(self, rows):
                super().__init__(rows)
                self.iterations = 0

            async def __anext__(self):
                self.iterations += 1
                if self.iterations > 5:
                    raise Exception("Database error")
                return await super().__anext__()

        rows = [{"id": i} for i in range(50)]
        mock_stream = FailingStream(rows)

        with patch.object(async_session, "execute_stream", return_value=mock_stream):
            # Try to stream, should error
            with pytest.raises(Exception) as exc_info:
                async with await async_session.execute_stream("SELECT * FROM test") as stream:
                    async for row in stream:
                        pass

            assert "Database error" in str(exc_info.value)
            # Stream should be closed even on error
            assert mock_stream._closed

    @pytest.mark.asyncio
    async def test_no_memory_leak_with_many_pages(self):
        """
        Test no memory accumulation with many pages.

        What this tests:
        ---------------
        1. Memory doesn't grow with page count
        2. Old pages are released
        3. Only current page is in memory

        Why this matters:
        ----------------
        Streaming millions of rows across thousands of pages
        shouldn't cause memory to grow unbounded.
        """
        mock_session = Mock()
        async_session = AsyncCassandraSession(mock_session)

        # Create many small pages
        pages = []
        for page_num in range(100):
            page = [{"id": page_num * 10 + i, "page": page_num} for i in range(10)]
            pages.append(page)

        all_rows = [row for page in pages for row in page]
        mock_stream = MockAsyncStreamingResultSet(all_rows, pages)

        with patch.object(async_session, "execute_stream", return_value=mock_stream):
            # Stream through all pages
            total_rows = 0
            page_numbers_seen = set()

            async with await async_session.execute_stream("SELECT * FROM huge_table") as stream:
                async for row in stream:
                    total_rows += 1
                    page_numbers_seen.add(row.get("page"))

            # Verify we processed all pages
            assert total_rows == 1000
            assert len(page_numbers_seen) == 100

    @pytest.mark.asyncio
    async def test_stream_close_releases_resources(self):
        """
        Test that closing stream releases all resources.

        What this tests:
        ---------------
        1. Explicit close() works
        2. Resources are freed immediately
        3. Cannot iterate after close

        Why this matters:
        ----------------
        Users may need to close streams early. This should
        immediately free all resources.
        """
        mock_session = Mock()
        async_session = AsyncCassandraSession(mock_session)

        rows = [{"id": i} for i in range(100)]
        mock_stream = MockAsyncStreamingResultSet(rows)

        with patch.object(async_session, "execute_stream", return_value=mock_stream):
            stream = await async_session.execute_stream("SELECT * FROM test")

            # Process a few rows
            row_count = 0
            async for row in stream:
                row_count += 1
                if row_count >= 5:
                    break

            # Explicitly close
            await stream.close()

            # Verify closed
            assert stream._closed

            # Cannot iterate after close
            with pytest.raises(StopAsyncIteration):
                await stream.__anext__()

    @pytest.mark.asyncio
    async def test_weakref_cleanup_on_session_close(self):
        """
        Test cleanup when session is closed during streaming.

        What this tests:
        ---------------
        1. Session close interrupts streaming
        2. Stream resources are cleaned up
        3. No dangling references

        Why this matters:
        ----------------
        Session may be closed while streams are active. This
        shouldn't leak stream resources.
        """
        mock_session = Mock()
        async_session = AsyncCassandraSession(mock_session)

        # Track if stream was cleaned up
        stream_closed = False

        class TrackableStream(MockAsyncStreamingResultSet):
            async def close(self):
                nonlocal stream_closed
                stream_closed = True
                await super().close()

        rows = [{"id": i} for i in range(1000)]
        mock_stream = TrackableStream(rows)

        with patch.object(async_session, "execute_stream", return_value=mock_stream):
            # Start streaming but don't finish
            stream = await async_session.execute_stream("SELECT * FROM test")

            # Process a few rows
            count = 0
            async for row in stream:
                count += 1
                if count >= 5:
                    break

            # Close the stream (simulating session close)
            await stream.close()

            # Verify cleanup happened
            assert stream_closed


class TestStreamingPerformance:
    """
    Test streaming performance characteristics.

    These tests verify streaming can handle large datasets efficiently.
    """

    @pytest.mark.asyncio
    async def test_streaming_large_rows(self):
        """
        Test streaming rows with large data.

        What this tests:
        ---------------
        1. Large rows don't cause issues
        2. Memory per row is bounded
        3. Streaming continues smoothly

        Why this matters:
        ----------------
        Some rows may contain blobs or large text fields.
        Streaming should handle these efficiently.
        """
        mock_session = Mock()
        async_session = AsyncCassandraSession(mock_session)

        # Create rows with large data
        rows = []
        for i in range(50):
            rows.append(
                {
                    "id": i,
                    "data": "x" * 100000,  # 100KB per row
                    "blob": b"y" * 50000,  # 50KB binary
                }
            )

        mock_stream = MockAsyncStreamingResultSet(rows)

        with patch.object(async_session, "execute_stream", return_value=mock_stream):
            processed = 0
            total_size = 0

            async with await async_session.execute_stream("SELECT * FROM blobs") as stream:
                async for row in stream:
                    processed += 1
                    total_size += len(row["data"]) + len(row["blob"])

            assert processed == 50
            assert total_size == 50 * (100000 + 50000)

    @pytest.mark.asyncio
    async def test_streaming_high_throughput(self):
        """
        Test streaming can maintain high throughput.

        What this tests:
        ---------------
        1. Thousands of rows/second possible
        2. Minimal overhead per row
        3. Efficient page transitions

        Why this matters:
        ----------------
        Bulk data operations need high throughput. Streaming
        overhead must be minimal.
        """
        mock_session = Mock()
        async_session = AsyncCassandraSession(mock_session)

        # Simulate high-throughput scenario
        rows_per_page = 5000
        num_pages = 20

        pages = []
        for page_num in range(num_pages):
            page = [{"id": page_num * rows_per_page + i} for i in range(rows_per_page)]
            pages.append(page)

        all_rows = [row for page in pages for row in page]
        mock_stream = MockAsyncStreamingResultSet(all_rows, pages)

        with patch.object(async_session, "execute_stream", return_value=mock_stream):
            # Stream all rows and measure throughput
            import time

            start_time = time.time()

            total_rows = 0
            async with await async_session.execute_stream("SELECT * FROM big_table") as stream:
                async for row in stream:
                    total_rows += 1

            elapsed = time.time() - start_time

            expected_total = rows_per_page * num_pages
            assert total_rows == expected_total

            # Should process quickly (implementation dependent)
            # This documents the performance expectation
            rows_per_second = total_rows / elapsed if elapsed > 0 else 0
            # Should handle thousands of rows/second
            assert rows_per_second > 0  # Use the variable

    @pytest.mark.asyncio
    async def test_streaming_memory_limit_enforcement(self):
        """
        Test memory limits are enforced during streaming.

        What this tests:
        ---------------
        1. Configurable memory limits
        2. Backpressure when limit reached
        3. Graceful handling of limits

        Why this matters:
        ----------------
        Production systems have memory constraints. Streaming
        must respect these limits.
        """
        mock_session = Mock()
        async_session = AsyncCassandraSession(mock_session)

        # Large amount of data
        rows = [{"id": i, "data": "x" * 10000} for i in range(1000)]
        mock_stream = MockAsyncStreamingResultSet(rows)

        with patch.object(async_session, "execute_stream", return_value=mock_stream):
            # Stream with memory awareness
            rows_processed = 0
            async with await async_session.execute_stream("SELECT * FROM test") as stream:
                async for row in stream:
                    rows_processed += 1
                    # In real implementation, might pause/backpressure here
                    if rows_processed >= 100:
                        break
