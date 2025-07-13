"""
Test that parallel query execution actually runs queries concurrently.

What this tests:
---------------
1. ParallelPartitionReader executes queries in parallel using asyncio.Semaphore
2. Concurrency limit is respected via semaphore
3. read_partitions properly manages concurrent execution
4. Error handling doesn't break parallelism
5. Proper integration with streaming partition strategy

Why this matters:
----------------
- User specifically requested verification of parallel execution
- Performance depends on concurrent queries to Cassandra
- Must ensure we're using asyncio.Semaphore correctly
- Verifies the actual implementation, not mocks
"""

import asyncio
import time
from unittest.mock import AsyncMock, Mock, patch

import pandas as pd
import pytest
from async_cassandra_dataframe.parallel import ParallelExecutionError, ParallelPartitionReader


class TestActualParallelExecution:
    """Test the actual ParallelPartitionReader implementation."""

    @pytest.mark.asyncio
    async def test_semaphore_controls_concurrency(self):
        """Verify asyncio.Semaphore properly limits concurrent execution."""
        # Track concurrent executions
        current_concurrent = 0
        max_concurrent_seen = 0
        execution_order = []

        async def mock_stream_partition(partition):
            """Mock that tracks concurrency."""
            nonlocal current_concurrent, max_concurrent_seen

            partition_id = partition["partition_id"]
            current_concurrent += 1
            max_concurrent_seen = max(max_concurrent_seen, current_concurrent)
            execution_order.append(f"start_{partition_id}")

            # Simulate query time
            await asyncio.sleep(0.05)

            current_concurrent -= 1
            execution_order.append(f"end_{partition_id}")

            return pd.DataFrame({"id": [partition_id]})

        # Mock the StreamingPartitionStrategy
        with patch(
            "async_cassandra_dataframe.partition.StreamingPartitionStrategy"
        ) as MockStrategy:
            mock_strategy = Mock()
            mock_strategy.stream_partition = mock_stream_partition
            MockStrategy.return_value = mock_strategy

            # Create reader with concurrency limit of 2
            reader = ParallelPartitionReader(session=Mock(), max_concurrent=2)

            # Create 6 partitions
            partitions = [{"partition_id": i, "session": Mock()} for i in range(6)]

            # Execute
            start_time = time.time()
            results = await reader.read_partitions(partitions)
            total_time = time.time() - start_time

            # Verify results
            assert len(results) == 6
            assert max_concurrent_seen == 2, f"Should respect limit, saw {max_concurrent_seen}"

            # Verify timing - with concurrency=2 and 0.05s per query:
            # Should take ~0.15s (3 batches) not 0.3s (sequential)
            assert total_time < 0.25, f"Should run in parallel, took {total_time}s"

            # Verify execution pattern shows parallelism
            # Should see start_0, start_1 before end_0
            execution_order.index("start_0")  # Just verify it exists
            start_1_idx = execution_order.index("start_1")
            end_0_idx = execution_order.index("end_0")

            assert start_1_idx < end_0_idx, "Should start partition 1 before partition 0 ends"

    @pytest.mark.asyncio
    async def test_progress_callback_integration(self):
        """Progress callback should be called correctly."""
        progress_updates = []

        async def progress_callback(completed, total, message):
            progress_updates.append({"completed": completed, "total": total, "message": message})

        # Mock StreamingPartitionStrategy
        with patch(
            "async_cassandra_dataframe.partition.StreamingPartitionStrategy"
        ) as MockStrategy:
            mock_strategy = Mock()
            mock_strategy.stream_partition = AsyncMock(return_value=pd.DataFrame({"id": [1]}))
            MockStrategy.return_value = mock_strategy

            reader = ParallelPartitionReader(
                session=Mock(), max_concurrent=2, progress_callback=progress_callback
            )

            partitions = [{"partition_id": i, "session": Mock()} for i in range(3)]
            await reader.read_partitions(partitions)

            # Should have 3 progress updates
            assert len(progress_updates) == 3
            assert progress_updates[-1]["completed"] == 3
            assert progress_updates[-1]["total"] == 3

    @pytest.mark.asyncio
    async def test_error_aggregation_with_parallel_execution(self):
        """Errors should be properly aggregated even with parallel execution."""

        async def mock_stream_with_errors(partition):
            partition_id = partition["partition_id"]
            if partition_id in [1, 3]:
                raise ValueError(f"Error in partition {partition_id}")
            return pd.DataFrame({"id": [partition_id]})

        with patch(
            "async_cassandra_dataframe.partition.StreamingPartitionStrategy"
        ) as MockStrategy:
            mock_strategy = Mock()
            mock_strategy.stream_partition = mock_stream_with_errors
            MockStrategy.return_value = mock_strategy

            reader = ParallelPartitionReader(
                session=Mock(), max_concurrent=2, allow_partial_results=False
            )

            partitions = [{"partition_id": i, "session": Mock()} for i in range(5)]

            with pytest.raises(ParallelExecutionError) as exc_info:
                await reader.read_partitions(partitions)

            error = exc_info.value
            assert error.failed_count == 2
            assert error.successful_count == 3
            assert len(error.errors) == 2
            assert "ValueError (2 occurrences)" in str(error)

    @pytest.mark.asyncio
    async def test_partition_metadata_addition(self):
        """Partition metadata should be added when requested."""

        async def mock_stream(partition):
            return pd.DataFrame({"id": [1, 2, 3]})

        with patch(
            "async_cassandra_dataframe.partition.StreamingPartitionStrategy"
        ) as MockStrategy:
            mock_strategy = Mock()
            mock_strategy.stream_partition = mock_stream
            MockStrategy.return_value = mock_strategy

            reader = ParallelPartitionReader(session=Mock())

            partitions = [{"partition_id": 42, "session": Mock(), "add_partition_metadata": True}]

            results = await reader.read_partitions(partitions)
            df = results[0]

            # Should have metadata columns
            assert "_partition_id" in df.columns
            assert df["_partition_id"].iloc[0] == 42
            assert "_read_duration_ms" in df.columns

    @pytest.mark.skip(reason="API has changed, need to update test")
    @pytest.mark.asyncio
    async def test_real_integration_with_reader_module(self):
        """Test integration with reader.py."""
        # This tests how read_cassandra_table actually uses ParallelPartitionReader
        from async_cassandra_dataframe.reader import CassandraDataFrameReader

        # Mock dependencies
        session = AsyncMock()
        session.keyspace = "test_ks"

        # Create reader
        reader = CassandraDataFrameReader(
            session=session, keyspace="test_ks", table="test_table", max_concurrent_partitions=5
        )

        # Mock the partition reader
        with patch.object(reader, "_create_partitions") as mock_create:
            mock_create.return_value = []  # No partitions means no parallel execution

            # Mock parallel reader if partitions were created
            with patch("async_cassandra_dataframe.parallel.ParallelPartitionReader") as MockReader:
                mock_reader_instance = Mock()
                mock_reader_instance.read_partitions = AsyncMock(return_value=[])
                MockReader.return_value = mock_reader_instance

                # Call read
                df = await reader.read()

                # Since we mocked no partitions, it should return empty dataframe
                assert isinstance(df, pd.DataFrame)

    @pytest.mark.asyncio
    async def test_concurrent_queries_complete_independently(self):
        """Queries should complete independently without blocking each other."""
        completion_times = {}

        async def mock_stream_with_varying_times(partition):
            partition_id = partition["partition_id"]
            # Different partitions take different times
            delay = 0.1 if partition_id % 2 == 0 else 0.05

            await asyncio.sleep(delay)
            completion_times[partition_id] = time.time()

            return pd.DataFrame({"id": [partition_id]})

        with patch(
            "async_cassandra_dataframe.partition.StreamingPartitionStrategy"
        ) as MockStrategy:
            mock_strategy = Mock()
            mock_strategy.stream_partition = mock_stream_with_varying_times
            MockStrategy.return_value = mock_strategy

            reader = ParallelPartitionReader(session=Mock(), max_concurrent=3)

            partitions = [{"partition_id": i, "session": Mock()} for i in range(6)]

            start_time = time.time()
            await reader.read_partitions(partitions)

            # Fast queries (odd IDs) should complete before slow queries
            fast_times = [completion_times[i] - start_time for i in [1, 3, 5]]
            slow_times = [completion_times[i] - start_time for i in [0, 2, 4]]

            # All fast queries should complete faster than slowest query
            assert all(fast < max(slow_times) for fast in fast_times)

    def test_semaphore_initialization(self):
        """Semaphore should be created with correct value."""
        reader = ParallelPartitionReader(session=Mock(), max_concurrent=7)

        assert reader._semaphore._value == 7
        assert reader.max_concurrent == 7

    @pytest.mark.asyncio
    async def test_as_completed_behavior(self):
        """Verify we're using asyncio.as_completed correctly."""
        # This tests that results are processed as they complete
        completion_order = []

        async def mock_stream(partition):
            partition_id = partition["partition_id"]
            # Reverse delay - higher IDs complete faster
            delay = (5 - partition_id) * 0.02
            await asyncio.sleep(delay)
            completion_order.append(partition_id)
            return pd.DataFrame({"id": [partition_id]})

        with patch(
            "async_cassandra_dataframe.partition.StreamingPartitionStrategy"
        ) as MockStrategy:
            mock_strategy = Mock()
            mock_strategy.stream_partition = mock_stream
            MockStrategy.return_value = mock_strategy

            reader = ParallelPartitionReader(session=Mock(), max_concurrent=5)

            partitions = [{"partition_id": i, "session": Mock()} for i in range(5)]
            await reader.read_partitions(partitions)

            # Should complete in reverse order (4, 3, 2, 1, 0)
            assert completion_order == [4, 3, 2, 1, 0]
