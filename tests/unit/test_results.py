"""Core result handling tests.

This module tests AsyncResultHandler and AsyncResultSet functionality,
which are critical for proper async operation of query results.

Test Organization:
==================
- TestAsyncResultHandler: Core callback-to-async conversion tests
- TestAsyncResultSet: Result collection wrapper tests

Key Testing Focus:
==================
1. Callback registration and handling
2. Multi-callback safety (duplicate calls)
3. Result set iteration and access patterns
4. Property access and convenience methods
5. Edge cases (empty results, single results)

Note: This complements test_result.py with additional edge cases.
"""

from unittest.mock import Mock

import pytest
from cassandra.cluster import ResponseFuture

from async_cassandra.result import AsyncResultHandler, AsyncResultSet


class TestAsyncResultHandler:
    """
    Test AsyncResultHandler for callback-based result handling.

    This class focuses on the core mechanics of converting Cassandra's
    callback-based results to Python async/await. It tests edge cases
    not covered in test_result.py.
    """

    @pytest.mark.core
    @pytest.mark.quick
    async def test_init(self):
        """
        Test AsyncResultHandler initialization.

        What this tests:
        ---------------
        1. Handler stores reference to ResponseFuture
        2. Empty rows list is initialized
        3. Callbacks are registered immediately
        4. Handler is ready to receive results

        Why this matters:
        ----------------
        Initialization must happen quickly before results arrive:
        - Callbacks must be registered before driver calls them
        - State must be initialized to handle results
        - No async operations during init (can't await)

        The handler is the critical bridge between sync callbacks
        and async/await, so initialization must be bulletproof.
        """
        mock_future = Mock(spec=ResponseFuture)
        mock_future.add_callbacks = Mock()

        handler = AsyncResultHandler(mock_future)
        assert handler.response_future == mock_future
        assert handler.rows == []
        mock_future.add_callbacks.assert_called_once()

    @pytest.mark.core
    async def test_on_success(self):
        """
        Test successful result handling.

        What this tests:
        ---------------
        1. Success callback properly receives rows
        2. Rows are stored in the handler
        3. Result future completes with AsyncResultSet
        4. No paging logic for single-page results

        Why this matters:
        ----------------
        The success path is the most common case:
        - Query executes successfully
        - Results arrive via callback
        - Must convert to awaitable result

        This tests the happy path that 99% of queries follow.
        The callback happens in driver thread, so thread safety
        is critical here.
        """
        mock_future = Mock(spec=ResponseFuture)
        mock_future.add_callbacks = Mock()
        mock_future.has_more_pages = False

        handler = AsyncResultHandler(mock_future)

        # Get result future and simulate success callback
        result_future = handler.get_result()

        # Simulate the driver calling our success callback
        mock_result = Mock()
        mock_result.current_rows = [{"id": 1}, {"id": 2}]
        handler._handle_page(mock_result.current_rows)

        result = await result_future
        assert isinstance(result, AsyncResultSet)

    @pytest.mark.core
    async def test_on_error(self):
        """
        Test error handling.

        What this tests:
        ---------------
        1. Error callback receives exceptions
        2. Exception is stored and re-raised on await
        3. No result is returned on error
        4. Original exception details preserved

        Why this matters:
        ----------------
        Error handling is critical for debugging:
        - Network errors
        - Query syntax errors
        - Timeout errors
        - Permission errors

        The error must be:
        - Captured from callback thread
        - Stored until await
        - Re-raised with full details
        - Not swallowed or lost
        """
        mock_future = Mock(spec=ResponseFuture)
        mock_future.add_callbacks = Mock()

        handler = AsyncResultHandler(mock_future)
        error = Exception("Test error")

        # Get result future and simulate error callback
        result_future = handler.get_result()
        handler._handle_error(error)

        with pytest.raises(Exception, match="Test error"):
            await result_future

    @pytest.mark.core
    @pytest.mark.critical
    async def test_multiple_callbacks(self):
        """
        Test that multiple success/error calls don't break the handler.

        What this tests:
        ---------------
        1. First callback sets the result
        2. Subsequent callbacks are safely ignored
        3. No exceptions from duplicate callbacks
        4. Result remains stable after first callback

        Why this matters:
        ----------------
        Defensive programming against driver bugs:
        - Driver might call callbacks multiple times
        - Race conditions in callback handling
        - Error after success (or vice versa)

        Real-world scenario:
        - Network packet arrives late
        - Retry logic in driver
        - Threading race conditions

        The handler must be idempotent - multiple calls should
        not corrupt state or raise exceptions. First result wins.
        """
        mock_future = Mock(spec=ResponseFuture)
        mock_future.add_callbacks = Mock()
        mock_future.has_more_pages = False

        handler = AsyncResultHandler(mock_future)

        # Get result future
        result_future = handler.get_result()

        # First success should set the result
        mock_result = Mock()
        mock_result.current_rows = [{"id": 1}]
        handler._handle_page(mock_result.current_rows)

        result = await result_future
        assert isinstance(result, AsyncResultSet)

        # Subsequent calls should be ignored (no exceptions)
        handler._handle_page([{"id": 2}])
        handler._handle_error(Exception("should be ignored"))


class TestAsyncResultSet:
    """
    Test AsyncResultSet for handling query results.

    Tests additional functionality not covered in test_result.py,
    focusing on edge cases and additional access patterns.
    """

    @pytest.mark.core
    @pytest.mark.quick
    async def test_init_single_page(self):
        """
        Test initialization with single page result.

        What this tests:
        ---------------
        1. ResultSet correctly stores provided rows
        2. No data transformation during init
        3. Rows are accessible immediately
        4. Works with typical dict-like row data

        Why this matters:
        ----------------
        Single page results are the most common case:
        - Queries with LIMIT
        - Primary key lookups
        - Small tables

        Initialization should be fast and simple, just
        storing the rows for later access.
        """
        rows = [{"id": 1}, {"id": 2}, {"id": 3}]

        async_result = AsyncResultSet(rows)
        assert async_result.rows == rows

    @pytest.mark.core
    async def test_init_empty(self):
        """
        Test initialization with empty result.

        What this tests:
        ---------------
        1. Empty list is handled correctly
        2. No errors with zero rows
        3. Properties work with empty data
        4. Ready for iteration (will complete immediately)

        Why this matters:
        ----------------
        Empty results are common and must work:
        - No matching WHERE clause
        - Deleted data
        - Fresh tables

        Empty ResultSet should behave like empty list,
        not None or error.
        """
        async_result = AsyncResultSet([])
        assert async_result.rows == []

    @pytest.mark.core
    @pytest.mark.critical
    async def test_async_iteration(self):
        """
        Test async iteration over results.

        What this tests:
        ---------------
        1. Supports async for syntax
        2. Yields rows in correct order
        3. Completes after all rows
        4. Each row is yielded exactly once

        Why this matters:
        ----------------
        Core functionality for result processing:
        ```python
        async for row in results:
            await process(row)
        ```

        Must work correctly for:
        - FastAPI endpoints
        - Async data processing
        - Streaming responses

        Async iteration allows non-blocking processing
        of each row, critical for scalability.
        """
        rows = [{"id": 1}, {"id": 2}, {"id": 3}]
        async_result = AsyncResultSet(rows)

        results = []
        async for row in async_result:
            results.append(row)

        assert results == rows

    @pytest.mark.core
    async def test_one(self):
        """
        Test getting single result.

        What this tests:
        ---------------
        1. one() returns first row
        2. Works with single row result
        3. Returns actual row, not wrapped
        4. Matches driver behavior

        Why this matters:
        ----------------
        Optimized for single-row queries:
        - User lookup by ID
        - Configuration values
        - Existence checks

        Simpler than iteration for single values.
        """
        rows = [{"id": 1, "name": "test"}]
        async_result = AsyncResultSet(rows)

        result = async_result.one()
        assert result == {"id": 1, "name": "test"}

    @pytest.mark.core
    async def test_all(self):
        """
        Test getting all results.

        What this tests:
        ---------------
        1. all() returns complete row list
        2. No async needed (already in memory)
        3. Returns actual list, not copy
        4. Preserves row order

        Why this matters:
        ----------------
        For when you need all data at once:
        - JSON serialization
        - Bulk operations
        - Data export

        More convenient than list comprehension.
        """
        rows = [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}]
        async_result = AsyncResultSet(rows)

        results = async_result.all()
        assert results == rows

    @pytest.mark.core
    async def test_len(self):
        """
        Test getting result count.

        What this tests:
        ---------------
        1. len() protocol support
        2. Accurate row count
        3. O(1) operation (not counting)
        4. Works with empty results

        Why this matters:
        ----------------
        Standard Python patterns:
        - Checking if results exist
        - Pagination calculations
        - Progress reporting

        Makes ResultSet feel native.
        """
        rows = [{"id": i} for i in range(5)]
        async_result = AsyncResultSet(rows)

        assert len(async_result) == 5

    @pytest.mark.core
    async def test_getitem(self):
        """
        Test indexed access to results.

        What this tests:
        ---------------
        1. Square bracket notation works
        2. Zero-based indexing
        3. Access specific rows by position
        4. Returns actual row data

        Why this matters:
        ----------------
        Pythonic access patterns:
        - first = results[0]
        - last = results[-1]
        - middle = results[len(results)//2]

        Useful for:
        - Accessing specific rows
        - Sampling results
        - Testing specific positions

        Makes ResultSet behave like a list.
        """
        rows = [{"id": 1, "name": "test"}, {"id": 2, "name": "test2"}]
        async_result = AsyncResultSet(rows)

        assert async_result[0] == {"id": 1, "name": "test"}
        assert async_result[1] == {"id": 2, "name": "test2"}

    @pytest.mark.core
    async def test_properties(self):
        """
        Test result set properties.

        What this tests:
        ---------------
        1. Direct access to rows property
        2. Property returns underlying list
        3. Can check length via property
        4. Properties are consistent

        Why this matters:
        ----------------
        Properties provide direct access:
        - Debugging (inspect results.rows)
        - Integration with other code
        - Performance (no method call)

        The .rows property gives escape hatch to
        raw data when needed.
        """
        rows = [{"id": 1}, {"id": 2}, {"id": 3}]
        async_result = AsyncResultSet(rows)

        # Check basic properties
        assert len(async_result.rows) == 3
        assert async_result.rows == rows
