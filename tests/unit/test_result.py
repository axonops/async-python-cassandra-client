"""
Unit tests for async result handling.

This module tests the core result handling mechanisms that convert
Cassandra driver's callback-based results into Python async/await
compatible results.

Test Organization:
==================
- TestAsyncResultHandler: Tests the callback-to-async conversion
- TestAsyncResultSet: Tests the result set wrapper functionality

Key Testing Focus:
==================
1. Single and multi-page result handling
2. Error propagation from callbacks
3. Async iteration protocol
4. Result set convenience methods (one(), all())
5. Empty result handling
"""

from unittest.mock import Mock

import pytest

from async_cassandra.result import AsyncResultHandler, AsyncResultSet


class TestAsyncResultHandler:
    """
    Test cases for AsyncResultHandler.

    AsyncResultHandler is the bridge between Cassandra driver's callback-based
    ResponseFuture and Python's async/await. It registers callbacks that get
    called when results are ready and converts them to awaitable results.
    """

    @pytest.fixture
    def mock_response_future(self):
        """
        Create a mock ResponseFuture.

        ResponseFuture is the driver's async result object that uses
        callbacks. We mock it to test our handler without real queries.
        """
        future = Mock()
        future.has_more_pages = False
        future.add_callbacks = Mock()
        future.timeout = None  # Add timeout attribute for new timeout handling
        return future

    @pytest.mark.asyncio
    async def test_single_page_result(self, mock_response_future):
        """
        Test handling single page of results.

        What this tests:
        ---------------
        1. Handler correctly receives page callback
        2. Single page results are wrapped in AsyncResultSet
        3. get_result() returns when page is complete
        4. No pagination logic triggered for single page

        Why this matters:
        ----------------
        Most queries return a single page of results. This is the
        common case that must work efficiently:
        - Small result sets
        - Queries with LIMIT
        - Single row lookups

        The handler should not add overhead for simple cases.
        """
        handler = AsyncResultHandler(mock_response_future)

        # Simulate successful page callback
        test_rows = [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}]
        handler._handle_page(test_rows)

        # Get result
        result = await handler.get_result()

        assert isinstance(result, AsyncResultSet)
        assert len(result) == 2
        assert result.rows == test_rows

    @pytest.mark.asyncio
    async def test_multi_page_result(self, mock_response_future):
        """
        Test handling multiple pages of results.

        What this tests:
        ---------------
        1. Multi-page results are handled correctly
        2. Next page fetch is triggered automatically
        3. All pages are accumulated into final result
        4. has_more_pages flag controls pagination

        Why this matters:
        ----------------
        Large result sets are split into pages to:
        - Prevent memory exhaustion
        - Allow incremental processing
        - Control network bandwidth

        The handler must:
        - Automatically fetch all pages
        - Accumulate results correctly
        - Handle page boundaries transparently

        Common with:
        - Large table scans
        - No LIMIT queries
        - Analytics workloads
        """
        # Configure mock for multiple pages
        mock_response_future.has_more_pages = True
        mock_response_future.start_fetching_next_page = Mock()

        handler = AsyncResultHandler(mock_response_future)

        # First page
        first_page = [{"id": 1}, {"id": 2}]
        handler._handle_page(first_page)

        # Verify next page fetch was triggered
        mock_response_future.start_fetching_next_page.assert_called_once()

        # Second page (final)
        mock_response_future.has_more_pages = False
        second_page = [{"id": 3}, {"id": 4}]
        handler._handle_page(second_page)

        # Get result
        result = await handler.get_result()

        assert len(result) == 4
        assert result.rows == first_page + second_page

    @pytest.mark.asyncio
    async def test_error_handling(self, mock_response_future):
        """
        Test error handling in result handler.

        What this tests:
        ---------------
        1. Errors from callbacks are captured
        2. Errors are propagated when get_result() is called
        3. Original exception is preserved
        4. No results are returned on error

        Why this matters:
        ----------------
        Many things can go wrong during query execution:
        - Network failures
        - Query syntax errors
        - Timeout exceptions
        - Server overload

        The handler must:
        - Capture errors from callbacks
        - Propagate them at the right time
        - Preserve error details for debugging

        Without proper error handling, errors could be:
        - Silently swallowed
        - Raised at callback time (wrong thread)
        - Lost without stack trace
        """
        handler = AsyncResultHandler(mock_response_future)

        # Simulate error callback
        test_error = Exception("Query failed")
        handler._handle_error(test_error)

        # Should raise the exception
        with pytest.raises(Exception) as exc_info:
            await handler.get_result()

        assert str(exc_info.value) == "Query failed"

    @pytest.mark.asyncio
    async def test_callback_registration(self, mock_response_future):
        """
        Test that callbacks are properly registered.

        What this tests:
        ---------------
        1. Callbacks are registered on ResponseFuture
        2. Both success and error callbacks are set
        3. Correct handler methods are used
        4. Registration happens during init

        Why this matters:
        ----------------
        The callback registration is the critical link between
        driver and our async wrapper:
        - Must register before results arrive
        - Must handle both success and error paths
        - Must use correct method signatures

        If registration fails:
        - Results would never arrive
        - Queries would hang forever
        - Errors would be lost

        This test ensures the "wiring" is correct.
        """
        handler = AsyncResultHandler(mock_response_future)

        # Verify callbacks were registered
        mock_response_future.add_callbacks.assert_called_once()
        call_args = mock_response_future.add_callbacks.call_args

        assert call_args.kwargs["callback"] == handler._handle_page
        assert call_args.kwargs["errback"] == handler._handle_error


class TestAsyncResultSet:
    """
    Test cases for AsyncResultSet.

    AsyncResultSet wraps query results to provide async iteration
    and convenience methods. It's what users interact with after
    executing a query.
    """

    @pytest.fixture
    def sample_rows(self):
        """
        Create sample row data.

        Simulates typical query results with multiple rows
        and columns. Used across multiple tests.
        """
        return [
            {"id": 1, "name": "Alice", "age": 30},
            {"id": 2, "name": "Bob", "age": 25},
            {"id": 3, "name": "Charlie", "age": 35},
        ]

    @pytest.mark.asyncio
    async def test_async_iteration(self, sample_rows):
        """
        Test async iteration over result set.

        What this tests:
        ---------------
        1. AsyncResultSet supports 'async for' syntax
        2. All rows are yielded in order
        3. Iteration completes normally
        4. Each row is accessible during iteration

        Why this matters:
        ----------------
        Async iteration is the primary way to process results:
        ```python
        async for row in result:
            await process_row(row)
        ```

        This enables:
        - Non-blocking result processing
        - Integration with async frameworks
        - Natural Python syntax

        Without this, users would need callbacks or blocking calls.
        """
        result_set = AsyncResultSet(sample_rows)

        collected_rows = []
        async for row in result_set:
            collected_rows.append(row)

        assert collected_rows == sample_rows

    def test_len(self, sample_rows):
        """
        Test length of result set.

        What this tests:
        ---------------
        1. len() works on AsyncResultSet
        2. Returns correct count of rows
        3. Works with standard Python functions

        Why this matters:
        ----------------
        Users expect Pythonic behavior:
        - if len(result) > 0:
        - print(f"Found {len(result)} rows")
        - assert len(result) == expected_count

        This makes AsyncResultSet feel like a normal collection.
        """
        result_set = AsyncResultSet(sample_rows)
        assert len(result_set) == 3

    def test_one_with_results(self, sample_rows):
        """
        Test one() method with results.

        What this tests:
        ---------------
        1. one() returns first row when results exist
        2. Only the first row is returned (not a list)
        3. Remaining rows are ignored

        Why this matters:
        ----------------
        Common pattern for single-row queries:
        ```python
        user = result.one()
        if user:
            print(f"Found user: {user.name}")
        ```

        Useful for:
        - Primary key lookups
        - COUNT queries
        - Existence checks

        Mirrors driver's ResultSet.one() behavior.
        """
        result_set = AsyncResultSet(sample_rows)
        assert result_set.one() == sample_rows[0]

    def test_one_empty(self):
        """
        Test one() method with empty results.

        What this tests:
        ---------------
        1. one() returns None for empty results
        2. No exception is raised
        3. Safe to use without checking length first

        Why this matters:
        ----------------
        Handles the "not found" case gracefully:
        ```python
        user = result.one()
        if not user:
            raise NotFoundError("User not found")
        ```

        No need for try/except or length checks.
        """
        result_set = AsyncResultSet([])
        assert result_set.one() is None

    def test_all(self, sample_rows):
        """
        Test all() method.

        What this tests:
        ---------------
        1. all() returns complete list of rows
        2. Original row order is preserved
        3. Returns actual list (not iterator)

        Why this matters:
        ----------------
        Sometimes you need all results immediately:
        - Converting to JSON
        - Passing to templates
        - Batch processing

        Convenience method avoids:
        ```python
        rows = [row async for row in result]  # More complex
        ```
        """
        result_set = AsyncResultSet(sample_rows)
        assert result_set.all() == sample_rows

    def test_rows_property(self, sample_rows):
        """
        Test rows property.

        What this tests:
        ---------------
        1. Direct access to underlying rows list
        2. Returns same data as all()
        3. Property access (no parentheses)

        Why this matters:
        ----------------
        Provides flexibility:
        - result.rows for property access
        - result.all() for method call
        - Both return same data

        Some users prefer property syntax for data access.
        """
        result_set = AsyncResultSet(sample_rows)
        assert result_set.rows == sample_rows

    @pytest.mark.asyncio
    async def test_empty_iteration(self):
        """
        Test iteration over empty result set.

        What this tests:
        ---------------
        1. Empty result sets can be iterated
        2. No rows are yielded
        3. Iteration completes immediately
        4. No errors or hangs occur

        Why this matters:
        ----------------
        Empty results are common and must work correctly:
        - No matching rows
        - Deleted data
        - Fresh tables

        The iteration should complete gracefully without
        special handling:
        ```python
        async for row in result:  # Should not error if empty
            process(row)
        ```
        """
        result_set = AsyncResultSet([])

        collected_rows = []
        async for row in result_set:
            collected_rows.append(row)

        assert collected_rows == []

    @pytest.mark.asyncio
    async def test_multiple_iterations(self, sample_rows):
        """
        Test that result set can be iterated multiple times.

        What this tests:
        ---------------
        1. Same result set can be iterated repeatedly
        2. Each iteration yields all rows
        3. Order is consistent across iterations
        4. No state corruption between iterations

        Why this matters:
        ----------------
        Unlike generators, AsyncResultSet allows re-iteration:
        - Processing results multiple ways
        - Retry logic after errors
        - Debugging (print then process)

        This differs from streaming results which can only
        be consumed once. AsyncResultSet holds all data in
        memory, allowing multiple passes.

        Example use case:
        ----------------
        # First pass: validation
        async for row in result:
            validate(row)

        # Second pass: processing
        async for row in result:
            await process(row)
        """
        result_set = AsyncResultSet(sample_rows)

        # First iteration
        first_iter = []
        async for row in result_set:
            first_iter.append(row)

        # Second iteration
        second_iter = []
        async for row in result_set:
            second_iter.append(row)

        assert first_iter == sample_rows
        assert second_iter == sample_rows
