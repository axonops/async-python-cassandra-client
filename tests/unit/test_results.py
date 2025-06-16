"""Core result handling tests.

This module tests AsyncResultHandler and AsyncResultSet functionality,
which are critical for proper async operation of query results.
"""

from unittest.mock import Mock

import pytest
from cassandra.cluster import ResponseFuture

from async_cassandra.result import AsyncResultHandler, AsyncResultSet


class TestAsyncResultHandler:
    """Test AsyncResultHandler for callback-based result handling."""

    @pytest.mark.core
    @pytest.mark.quick
    async def test_init(self):
        """Test AsyncResultHandler initialization."""
        mock_future = Mock(spec=ResponseFuture)
        mock_future.add_callbacks = Mock()

        handler = AsyncResultHandler(mock_future)
        assert handler.response_future == mock_future
        assert handler.rows == []
        mock_future.add_callbacks.assert_called_once()

    @pytest.mark.core
    async def test_on_success(self):
        """Test successful result handling."""
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
        """Test error handling."""
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
        """Test that multiple success/error calls don't break the handler."""
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
    """Test AsyncResultSet for handling query results."""

    @pytest.mark.core
    @pytest.mark.quick
    async def test_init_single_page(self):
        """Test initialization with single page result."""
        rows = [{"id": 1}, {"id": 2}, {"id": 3}]

        async_result = AsyncResultSet(rows)
        assert async_result.rows == rows

    @pytest.mark.core
    async def test_init_empty(self):
        """Test initialization with empty result."""
        async_result = AsyncResultSet([])
        assert async_result.rows == []

    @pytest.mark.core
    @pytest.mark.critical
    async def test_async_iteration(self):
        """Test async iteration over results."""
        rows = [{"id": 1}, {"id": 2}, {"id": 3}]
        async_result = AsyncResultSet(rows)

        results = []
        async for row in async_result:
            results.append(row)

        assert results == rows

    @pytest.mark.core
    async def test_one(self):
        """Test getting single result."""
        rows = [{"id": 1, "name": "test"}]
        async_result = AsyncResultSet(rows)

        result = async_result.one()
        assert result == {"id": 1, "name": "test"}

    @pytest.mark.core
    async def test_all(self):
        """Test getting all results."""
        rows = [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}]
        async_result = AsyncResultSet(rows)

        results = async_result.all()
        assert results == rows

    @pytest.mark.core
    async def test_len(self):
        """Test getting result count."""
        rows = [{"id": i} for i in range(5)]
        async_result = AsyncResultSet(rows)

        assert len(async_result) == 5

    @pytest.mark.core
    async def test_getitem(self):
        """Test indexed access to results."""
        rows = [{"id": 1, "name": "test"}, {"id": 2, "name": "test2"}]
        async_result = AsyncResultSet(rows)

        assert async_result[0] == {"id": 1, "name": "test"}
        assert async_result[1] == {"id": 2, "name": "test2"}

    @pytest.mark.core
    async def test_properties(self):
        """Test result set properties."""
        rows = [{"id": 1}, {"id": 2}, {"id": 3}]
        async_result = AsyncResultSet(rows)

        # Check basic properties
        assert len(async_result.rows) == 3
        assert async_result.rows == rows
