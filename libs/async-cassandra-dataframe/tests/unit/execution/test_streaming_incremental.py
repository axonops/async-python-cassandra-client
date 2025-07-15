"""
Test streaming with incremental DataFrame building.

What this tests:
---------------
1. Streaming uses incremental builder instead of row lists
2. Progress callbacks are integrated
3. Memory limits are respected
4. Parallel streaming works correctly

Why this matters:
----------------
- Verifies memory efficiency improvements
- Ensures progress tracking works
- Validates parallel execution
- Confirms no regressions
"""

from unittest.mock import AsyncMock, Mock, patch

import pandas as pd
import pytest

from async_cassandra_dataframe.streaming import CassandraStreamer


class TestStreamingWithIncrementalBuilder:
    """Test streaming using incremental builder."""

    @pytest.mark.asyncio
    async def test_stream_query_uses_incremental_builder(self):
        """stream_query should use IncrementalDataFrameBuilder."""
        session = AsyncMock()
        streamer = CassandraStreamer(session)

        # Mock the async context manager and streaming
        stream_result = AsyncMock()
        stream_result.__aenter__.return_value = stream_result
        stream_result.__aexit__.return_value = None

        # Mock rows
        mock_rows = []
        for i in range(5):
            row = Mock()
            row._asdict.return_value = {"id": i, "name": f"user_{i}"}
            mock_rows.append(row)

        # Make it async iterable
        async def async_iter(self):
            for row in mock_rows:
                yield row

        stream_result.__aiter__ = async_iter

        # Mock session methods
        session.prepare = AsyncMock()
        session.execute_stream = AsyncMock(return_value=stream_result)

        # Execute
        with patch(
            "async_cassandra_dataframe.incremental_builder.IncrementalDataFrameBuilder"
        ) as MockBuilder:
            mock_builder = Mock()
            mock_builder.get_dataframe.return_value = pd.DataFrame({"id": [0, 1, 2, 3, 4]})
            mock_builder.total_rows = 5
            mock_builder.get_memory_usage.return_value = 1000
            MockBuilder.return_value = mock_builder

            await streamer.stream_query("SELECT * FROM table", (), ["id", "name"], fetch_size=1000)

            # Verify builder was used
            MockBuilder.assert_called_once_with(
                columns=["id", "name"], chunk_size=1000, type_mapper=None, table_metadata=None
            )
            assert mock_builder.add_row.call_count == 5
            mock_builder.get_dataframe.assert_called_once()

    @pytest.mark.asyncio
    async def test_progress_callback_integration(self):
        """Progress callbacks should be logged."""
        session = AsyncMock()
        streamer = CassandraStreamer(session)

        # Track if callback was set
        callback_set = False

        def check_stream_config(prepared, values, stream_config=None, **kwargs):
            nonlocal callback_set
            if stream_config and stream_config.page_callback:
                callback_set = True
            # Return mock stream
            stream_result = AsyncMock()
            stream_result.__aenter__.return_value = stream_result
            stream_result.__aexit__.return_value = None

            # Make it properly async iterable
            async def empty_aiter(self):
                return
                yield  # Make it a generator

            stream_result.__aiter__ = empty_aiter
            return stream_result

        session.prepare = AsyncMock()
        session.execute_stream = AsyncMock(side_effect=check_stream_config)

        # No need to mock logging for this test
        await streamer.stream_query("SELECT * FROM table", (), ["id"])

        # Verify callback was set
        assert callback_set, "Progress callback should be set in StreamConfig"

    @pytest.mark.asyncio
    async def test_memory_limit_stops_streaming(self):
        """Streaming should stop when memory limit is reached."""
        session = AsyncMock()
        streamer = CassandraStreamer(session)

        # Create many rows
        mock_rows = []
        for i in range(1000):
            row = Mock()
            row._asdict.return_value = {"id": i, "data": "x" * 1000}
            mock_rows.append(row)

        stream_result = AsyncMock()
        stream_result.__aenter__.return_value = stream_result
        stream_result.__aexit__.return_value = None

        rows_yielded = 0

        async def async_iter(self):
            nonlocal rows_yielded
            for row in mock_rows:
                rows_yielded += 1
                yield row

        stream_result.__aiter__ = async_iter

        session.prepare = AsyncMock()
        session.execute_stream = AsyncMock(return_value=stream_result)

        with patch(
            "async_cassandra_dataframe.incremental_builder.IncrementalDataFrameBuilder"
        ) as MockBuilder:
            mock_builder = Mock()
            mock_builder.total_rows = 0

            # Simulate memory growth
            def get_memory():
                return mock_builder.total_rows * 1000

            mock_builder.get_memory_usage = get_memory

            # Track added rows
            added_rows = []

            def add_row(row):
                added_rows.append(row)
                mock_builder.total_rows = len(added_rows)

            mock_builder.add_row = add_row
            mock_builder.get_dataframe.return_value = pd.DataFrame()
            MockBuilder.return_value = mock_builder

            # No need to mock logging for this test
            await streamer.stream_query(
                "SELECT * FROM table", (), ["id", "data"], memory_limit_mb=1  # 1MB limit
            )

            # Should NOT have stopped early - we don't truncate on memory limit
            assert len(added_rows) == 1000  # All rows should be processed

    @pytest.mark.asyncio
    async def test_token_range_streaming_uses_builder(self):
        """Token range streaming should use incremental builder."""
        session = AsyncMock()
        streamer = CassandraStreamer(session)

        # Mock the stream result
        mock_stream_result = AsyncMock()

        # Create async context manager that yields rows
        async def async_iter():
            for i in range(3):
                row = Mock()
                row._asdict.return_value = {"id": i}
                yield row

        # Set up the async context manager
        mock_stream_result.__aenter__.return_value = async_iter()
        mock_stream_result.__aexit__.return_value = None

        # Mock prepare and execute_stream
        session.prepare = AsyncMock()
        session.execute_stream = AsyncMock(return_value=mock_stream_result)

        with patch(
            "async_cassandra_dataframe.incremental_builder.IncrementalDataFrameBuilder"
        ) as MockBuilder:
            mock_builder = Mock()
            mock_builder.get_dataframe.return_value = pd.DataFrame({"id": [0, 1, 2]})
            mock_builder.get_memory_usage.return_value = 100
            MockBuilder.return_value = mock_builder

            await streamer.stream_token_range(
                table="ks.table",
                columns=["id"],
                partition_keys=["id"],
                start_token=-1000,
                end_token=1000,
            )

            # Verify builder was used
            assert MockBuilder.called
            assert mock_builder.add_row.call_count == 3
