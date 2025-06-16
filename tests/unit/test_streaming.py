"""
Unit tests for streaming functionality.
"""

import asyncio
from unittest.mock import Mock

import pytest
from cassandra import ConsistencyLevel
from cassandra.cluster import ResponseFuture

from async_cassandra.streaming import (
    AsyncStreamingResultSet,
    StreamConfig,
    StreamingResultHandler,
    create_streaming_statement,
)


class TestAsyncStreamingResultSet:
    """Test cases for AsyncStreamingResultSet."""

    @pytest.fixture
    def mock_response_future(self):
        """Create a mock ResponseFuture."""
        future = Mock(spec=ResponseFuture)
        future.has_more_pages = True
        future._final_exception = None
        future.add_callbacks = Mock()
        future.start_fetching_next_page = Mock()
        return future

    @pytest.mark.asyncio
    async def test_single_page_iteration(self, mock_response_future):
        """Test iterating through a single page of results."""
        mock_response_future.has_more_pages = False

        result_set = AsyncStreamingResultSet(mock_response_future)

        # Get callback from add_callbacks
        args = mock_response_future.add_callbacks.call_args
        callback = args[1]["callback"]

        # Simulate page callback from a thread
        test_rows = [{"id": 1}, {"id": 2}, {"id": 3}]
        import threading

        def thread_callback():
            callback(test_rows)

        thread = threading.Thread(target=thread_callback)
        thread.start()

        # Iterate through results
        collected = []
        async for row in result_set:
            collected.append(row)

        assert collected == test_rows
        assert result_set.total_rows_fetched == 3
        assert result_set.page_number == 1

    @pytest.mark.asyncio
    async def test_multi_page_iteration(self, mock_response_future):
        """Test iterating through multiple pages."""
        result_set = AsyncStreamingResultSet(mock_response_future)

        # Get callbacks
        args = mock_response_future.add_callbacks.call_args
        callback = args[1]["callback"]

        # Track pages fetched
        pages_fetched = []

        def mock_start_fetching():
            pages_fetched.append(True)
            # Simulate async callback from thread
            if len(pages_fetched) == 1:
                # Second page
                import threading

                def thread_callback():
                    callback([{"id": 4}, {"id": 5}])
                    mock_response_future.has_more_pages = False

                thread = threading.Thread(target=thread_callback)
                thread.start()

        mock_response_future.start_fetching_next_page = mock_start_fetching

        # First page from thread
        import threading

        def first_page_callback():
            callback([{"id": 1}, {"id": 2}, {"id": 3}])

        thread = threading.Thread(target=first_page_callback)
        thread.start()

        # Iterate through all results
        collected = []
        async for row in result_set:
            collected.append(row)

        assert len(collected) == 5
        assert collected[0]["id"] == 1
        assert collected[4]["id"] == 5
        assert result_set.total_rows_fetched == 5
        assert result_set.page_number == 2

    @pytest.mark.asyncio
    async def test_page_iteration(self, mock_response_future):
        """Test iterating by pages instead of rows."""
        result_set = AsyncStreamingResultSet(mock_response_future)

        pages_to_simulate = [[{"id": 1}, {"id": 2}], [{"id": 3}, {"id": 4}], [{"id": 5}]]

        current_page_idx = [0]

        def mock_start_fetching():
            current_page_idx[0] += 1
            if current_page_idx[0] < len(pages_to_simulate):
                result_set._handle_page(pages_to_simulate[current_page_idx[0]])
                if current_page_idx[0] == len(pages_to_simulate) - 1:
                    mock_response_future.has_more_pages = False

        mock_response_future.start_fetching_next_page = mock_start_fetching

        # First page
        result_set._handle_page(pages_to_simulate[0])

        # Collect pages
        collected_pages = []
        async for page in result_set.pages():
            collected_pages.append(page)

        assert len(collected_pages) == 3
        assert collected_pages[0] == pages_to_simulate[0]
        assert collected_pages[2] == pages_to_simulate[2]

    @pytest.mark.asyncio
    async def test_error_handling(self, mock_response_future):
        """Test error handling during streaming."""
        result_set = AsyncStreamingResultSet(mock_response_future)

        # Initialize the event loop and page_ready event
        result_set._loop = asyncio.get_running_loop()
        result_set._page_ready = asyncio.Event()

        # Simulate error which will set the page_ready event
        test_error = Exception("Query failed")
        result_set._handle_error(test_error)

        # Should raise error when iterating
        with pytest.raises(Exception) as exc_info:
            async for _ in result_set:
                pass

        assert str(exc_info.value) == "Query failed"
        assert result_set._exhausted

    @pytest.mark.asyncio
    async def test_stream_config(self, mock_response_future):
        """Test streaming with custom configuration."""
        callback_calls = []

        def progress_callback(page_num, row_count):
            callback_calls.append((page_num, row_count))

        config = StreamConfig(fetch_size=100, max_pages=2, page_callback=progress_callback)

        result_set = AsyncStreamingResultSet(mock_response_future, config)

        # Simulate pages
        result_set._handle_page([{"id": i} for i in range(100)])

        # Should have called callback
        assert callback_calls == [(1, 100)]

        # Add another page
        result_set._handle_page([{"id": i} for i in range(100, 150)])

        # Should stop after max_pages
        assert result_set._exhausted
        assert len(callback_calls) == 2

    @pytest.mark.asyncio
    async def test_cancel_streaming(self, mock_response_future):
        """Test canceling a streaming operation."""
        result_set = AsyncStreamingResultSet(mock_response_future)

        # Start with some data
        result_set._handle_page([{"id": 1}, {"id": 2}])

        # Cancel streaming
        await result_set.cancel()

        assert result_set._exhausted

        # Should stop iteration
        collected = []
        async for row in result_set:
            collected.append(row)

        # Should only get the initial page
        assert len(collected) == 2


class TestStreamingResultHandler:
    """Test cases for StreamingResultHandler."""

    @pytest.fixture
    def mock_response_future(self):
        """Create a mock ResponseFuture."""
        future = Mock(spec=ResponseFuture)
        future.add_callbacks = Mock()
        return future

    @pytest.mark.asyncio
    async def test_get_streaming_result(self, mock_response_future):
        """Test getting streaming result from handler."""
        handler = StreamingResultHandler(mock_response_future)

        # Get streaming result
        result = await handler.get_streaming_result()

        # Should return an AsyncStreamingResultSet
        assert isinstance(result, AsyncStreamingResultSet)
        assert result.response_future == mock_response_future
        assert result.config is not None

    @pytest.mark.asyncio
    async def test_initial_error_handling(self, mock_response_future):
        """Test error handling in initial response."""
        handler = StreamingResultHandler(mock_response_future)

        # The handler now just creates and returns an AsyncStreamingResultSet
        # Error handling happens within the AsyncStreamingResultSet itself
        result = await handler.get_streaming_result()

        # Verify the result is properly initialized
        assert isinstance(result, AsyncStreamingResultSet)
        assert result.response_future == mock_response_future

        # Initialize the event loop and page_ready event
        result._loop = asyncio.get_running_loop()
        result._page_ready = asyncio.Event()

        # Test error handling by simulating an error in the result set
        test_error = Exception("Connection failed")
        result._handle_error(test_error)

        # Should raise error when iterating
        with pytest.raises(Exception) as exc_info:
            async for _ in result:
                pass

        assert str(exc_info.value) == "Connection failed"


class TestCreateStreamingStatement:
    """Test cases for create_streaming_statement helper."""

    def test_create_basic_statement(self):
        """Test creating a basic streaming statement."""
        query = "SELECT * FROM users"
        statement = create_streaming_statement(query)

        assert statement.query_string == query
        assert statement.fetch_size == 1000

    def test_create_with_custom_fetch_size(self):
        """Test creating statement with custom fetch size."""
        query = "SELECT * FROM large_table"
        statement = create_streaming_statement(query, fetch_size=5000)

        assert statement.query_string == query
        assert statement.fetch_size == 5000

    def test_create_with_consistency_level(self):
        """Test creating statement with consistency level."""
        query = "SELECT * FROM users"
        statement = create_streaming_statement(query, consistency_level=ConsistencyLevel.QUORUM)

        assert statement.consistency_level == ConsistencyLevel.QUORUM
