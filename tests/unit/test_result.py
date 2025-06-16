"""
Unit tests for async result handling.
"""

from unittest.mock import Mock

import pytest

from async_cassandra.result import AsyncResultHandler, AsyncResultSet


class TestAsyncResultHandler:
    """Test cases for AsyncResultHandler."""

    @pytest.fixture
    def mock_response_future(self):
        """Create a mock ResponseFuture."""
        future = Mock()
        future.has_more_pages = False
        future.add_callbacks = Mock()
        future.timeout = None  # Add timeout attribute for new timeout handling
        return future

    @pytest.mark.asyncio
    async def test_single_page_result(self, mock_response_future):
        """Test handling single page of results."""
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
        """Test handling multiple pages of results."""
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
        """Test error handling in result handler."""
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
        """Test that callbacks are properly registered."""
        handler = AsyncResultHandler(mock_response_future)

        # Verify callbacks were registered
        mock_response_future.add_callbacks.assert_called_once()
        call_args = mock_response_future.add_callbacks.call_args

        assert call_args.kwargs["callback"] == handler._handle_page
        assert call_args.kwargs["errback"] == handler._handle_error


class TestAsyncResultSet:
    """Test cases for AsyncResultSet."""

    @pytest.fixture
    def sample_rows(self):
        """Create sample row data."""
        return [
            {"id": 1, "name": "Alice", "age": 30},
            {"id": 2, "name": "Bob", "age": 25},
            {"id": 3, "name": "Charlie", "age": 35},
        ]

    @pytest.mark.asyncio
    async def test_async_iteration(self, sample_rows):
        """Test async iteration over result set."""
        result_set = AsyncResultSet(sample_rows)

        collected_rows = []
        async for row in result_set:
            collected_rows.append(row)

        assert collected_rows == sample_rows

    def test_len(self, sample_rows):
        """Test length of result set."""
        result_set = AsyncResultSet(sample_rows)
        assert len(result_set) == 3

    def test_one_with_results(self, sample_rows):
        """Test one() method with results."""
        result_set = AsyncResultSet(sample_rows)
        assert result_set.one() == sample_rows[0]

    def test_one_empty(self):
        """Test one() method with empty results."""
        result_set = AsyncResultSet([])
        assert result_set.one() is None

    def test_all(self, sample_rows):
        """Test all() method."""
        result_set = AsyncResultSet(sample_rows)
        assert result_set.all() == sample_rows

    def test_rows_property(self, sample_rows):
        """Test rows property."""
        result_set = AsyncResultSet(sample_rows)
        assert result_set.rows == sample_rows

    @pytest.mark.asyncio
    async def test_empty_iteration(self):
        """Test iteration over empty result set."""
        result_set = AsyncResultSet([])

        collected_rows = []
        async for row in result_set:
            collected_rows.append(row)

        assert collected_rows == []

    @pytest.mark.asyncio
    async def test_multiple_iterations(self, sample_rows):
        """Test that result set can be iterated multiple times."""
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
