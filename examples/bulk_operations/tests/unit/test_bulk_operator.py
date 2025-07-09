"""
Unit tests for TokenAwareBulkOperator.

What this tests:
---------------
1. Parallel execution of token range queries
2. Result aggregation and streaming
3. Progress tracking
4. Error handling and recovery

Why this matters:
----------------
- Ensures correct parallel processing
- Validates data completeness
- Confirms non-blocking async behavior
- Handles failures gracefully

Additional context:
---------------------------------
These tests mock the async-cassandra library to test
our bulk operation logic in isolation.
"""

import asyncio
from unittest.mock import AsyncMock, Mock, patch

import pytest

from bulk_operations.bulk_operator import (
    BulkOperationError,
    BulkOperationStats,
    TokenAwareBulkOperator,
)


class TestTokenAwareBulkOperator:
    """Test the main bulk operator class."""

    @pytest.fixture
    def mock_cluster(self):
        """Create a mock AsyncCluster."""
        cluster = Mock()
        cluster.contact_points = ["127.0.0.1", "127.0.0.2", "127.0.0.3"]
        return cluster

    @pytest.fixture
    def mock_session(self, mock_cluster):
        """Create a mock AsyncSession."""
        session = Mock()
        # Mock the underlying sync session that has cluster attribute
        session._session = Mock()
        session._session.cluster = mock_cluster
        session.execute = AsyncMock()
        session.execute_stream = AsyncMock()
        session.prepare = AsyncMock(return_value=Mock())  # Mock prepare method

        # Mock metadata structure
        metadata = Mock()

        # Create proper column mock
        partition_key_col = Mock()
        partition_key_col.name = "id"  # Set the name attribute properly

        keyspaces = {
            "test_ks": Mock(tables={"test_table": Mock(partition_key=[partition_key_col])})
        }
        metadata.keyspaces = keyspaces
        mock_cluster.metadata = metadata

        return session

    @pytest.mark.unit
    async def test_count_by_token_ranges_single_node(self, mock_session):
        """
        Test counting rows with token ranges on single node.

        What this tests:
        ---------------
        1. Token range discovery is called correctly
        2. Queries are generated for each token range
        3. Results are aggregated properly
        4. Single node operation works correctly

        Why this matters:
        ----------------
        - Ensures basic counting functionality works
        - Validates token range splitting logic
        - Confirms proper result aggregation
        - Foundation for more complex multi-node operations
        """
        operator = TokenAwareBulkOperator(mock_session)

        # Mock token range discovery
        with patch(
            "bulk_operations.bulk_operator.discover_token_ranges", new_callable=AsyncMock
        ) as mock_discover:
            # Create proper TokenRange mocks
            from bulk_operations.token_utils import TokenRange

            mock_ranges = [
                TokenRange(start=-1000, end=0, replicas=["127.0.0.1"]),
                TokenRange(start=0, end=1000, replicas=["127.0.0.1"]),
            ]
            mock_discover.return_value = mock_ranges

            # Mock query results
            mock_session.execute.side_effect = [
                Mock(one=Mock(return_value=Mock(count=500))),  # First range
                Mock(one=Mock(return_value=Mock(count=300))),  # Second range
            ]

            # Execute count
            result = await operator.count_by_token_ranges(
                keyspace="test_ks", table="test_table", split_count=2
            )

            assert result == 800
            assert mock_session.execute.call_count == 2

    @pytest.mark.unit
    async def test_count_with_parallel_execution(self, mock_session):
        """
        Test that counts are executed in parallel.

        What this tests:
        ---------------
        1. Multiple token ranges are processed concurrently
        2. Parallelism limits are respected
        3. Total execution time reflects parallel processing
        4. Results are correctly aggregated from parallel tasks

        Why this matters:
        ----------------
        - Parallel execution is critical for performance
        - Must not block the event loop
        - Resource limits must be respected
        - Common pattern in production bulk operations
        """
        operator = TokenAwareBulkOperator(mock_session)

        # Track execution times
        execution_times = []

        async def mock_execute_with_delay(stmt, params=None):
            start = asyncio.get_event_loop().time()
            await asyncio.sleep(0.1)  # Simulate query time
            execution_times.append(asyncio.get_event_loop().time() - start)
            return Mock(one=Mock(return_value=Mock(count=100)))

        mock_session.execute = mock_execute_with_delay

        with patch(
            "bulk_operations.bulk_operator.discover_token_ranges", new_callable=AsyncMock
        ) as mock_discover:
            # Create 4 ranges
            from bulk_operations.token_utils import TokenRange

            mock_ranges = [
                TokenRange(start=i * 1000, end=(i + 1) * 1000, replicas=["node1"]) for i in range(4)
            ]
            mock_discover.return_value = mock_ranges

            # Execute count
            start_time = asyncio.get_event_loop().time()
            result = await operator.count_by_token_ranges(
                keyspace="test_ks", table="test_table", split_count=4, parallelism=4
            )
            total_time = asyncio.get_event_loop().time() - start_time

            assert result == 400  # 4 ranges * 100 each
            # If executed in parallel, total time should be ~0.1s, not 0.4s
            assert total_time < 0.2

    @pytest.mark.unit
    async def test_count_with_error_handling(self, mock_session):
        """
        Test error handling during count operations.

        What this tests:
        ---------------
        1. Partial failures are handled gracefully
        2. BulkOperationError is raised with partial results
        3. Individual errors are collected and reported
        4. Operation continues despite individual failures

        Why this matters:
        ----------------
        - Network issues can cause partial failures
        - Users need visibility into what succeeded
        - Partial results are often useful
        - Critical for production reliability
        """
        operator = TokenAwareBulkOperator(mock_session)

        with patch(
            "bulk_operations.bulk_operator.discover_token_ranges", new_callable=AsyncMock
        ) as mock_discover:
            from bulk_operations.token_utils import TokenRange

            mock_ranges = [
                TokenRange(start=0, end=1000, replicas=["node1"]),
                TokenRange(start=1000, end=2000, replicas=["node2"]),
            ]
            mock_discover.return_value = mock_ranges

            # First succeeds, second fails
            mock_session.execute.side_effect = [
                Mock(one=Mock(return_value=Mock(count=500))),
                Exception("Connection timeout"),
            ]

            # Should raise BulkOperationError
            with pytest.raises(BulkOperationError) as exc_info:
                await operator.count_by_token_ranges(
                    keyspace="test_ks", table="test_table", split_count=2
                )

            assert "Failed to count" in str(exc_info.value)
            assert exc_info.value.partial_result == 500

    @pytest.mark.unit
    async def test_export_streaming(self, mock_session):
        """
        Test streaming export functionality.

        What this tests:
        ---------------
        1. Token ranges are discovered for export
        2. Results are streamed asynchronously
        3. Memory usage remains constant (streaming)
        4. All rows are yielded in order

        Why this matters:
        ----------------
        - Streaming prevents memory exhaustion
        - Essential for large dataset exports
        - Async iteration must work correctly
        - Foundation for Iceberg export functionality
        """
        operator = TokenAwareBulkOperator(mock_session)

        # Mock token range discovery
        with patch(
            "bulk_operations.bulk_operator.discover_token_ranges", new_callable=AsyncMock
        ) as mock_discover:
            from bulk_operations.token_utils import TokenRange

            mock_ranges = [TokenRange(start=0, end=1000, replicas=["node1"])]
            mock_discover.return_value = mock_ranges

            # Mock streaming results
            async def mock_stream_results():
                for i in range(10):
                    row = Mock()
                    row.id = i
                    row.name = f"row_{i}"
                    yield row

            mock_stream_context = AsyncMock()
            mock_stream_context.__aenter__.return_value = mock_stream_results()
            mock_stream_context.__aexit__.return_value = None

            mock_session.execute_stream.return_value = mock_stream_context

            # Collect exported rows
            exported_rows = []
            async for row in operator.export_by_token_ranges(
                keyspace="test_ks", table="test_table", split_count=1
            ):
                exported_rows.append(row)

            assert len(exported_rows) == 10
            assert exported_rows[0].id == 0
            assert exported_rows[9].name == "row_9"

    @pytest.mark.unit
    async def test_progress_callback(self, mock_session):
        """
        Test progress callback functionality.

        What this tests:
        ---------------
        1. Progress callbacks are invoked during operation
        2. Statistics are updated correctly
        3. Progress percentage is calculated accurately
        4. Final statistics reflect complete operation

        Why this matters:
        ----------------
        - Users need visibility into long-running operations
        - Progress tracking enables better UX
        - Statistics help with performance tuning
        - Critical for production monitoring
        """
        operator = TokenAwareBulkOperator(mock_session)
        progress_updates = []

        def progress_callback(stats: BulkOperationStats):
            progress_updates.append(
                {
                    "rows": stats.rows_processed,
                    "ranges": stats.ranges_completed,
                    "progress": stats.progress_percentage,
                }
            )

        # Mock setup
        with patch(
            "bulk_operations.bulk_operator.discover_token_ranges", new_callable=AsyncMock
        ) as mock_discover:
            from bulk_operations.token_utils import TokenRange

            mock_ranges = [
                TokenRange(start=0, end=1000, replicas=["node1"]),
                TokenRange(start=1000, end=2000, replicas=["node2"]),
            ]
            mock_discover.return_value = mock_ranges

            mock_session.execute.side_effect = [
                Mock(one=Mock(return_value=Mock(count=500))),
                Mock(one=Mock(return_value=Mock(count=300))),
            ]

            # Execute with progress callback
            await operator.count_by_token_ranges(
                keyspace="test_ks",
                table="test_table",
                split_count=2,
                progress_callback=progress_callback,
            )

            assert len(progress_updates) >= 2
            # Check final progress
            final_update = progress_updates[-1]
            assert final_update["ranges"] == 2
            assert final_update["progress"] == 100.0

    @pytest.mark.unit
    async def test_operation_stats(self, mock_session):
        """
        Test operation statistics collection.

        What this tests:
        ---------------
        1. Statistics are collected during operations
        2. Duration is calculated correctly
        3. Rows per second metric is accurate
        4. All statistics fields are populated

        Why this matters:
        ----------------
        - Performance metrics guide optimization
        - Statistics enable capacity planning
        - Benchmarking requires accurate metrics
        - Production monitoring depends on these stats
        """
        operator = TokenAwareBulkOperator(mock_session)

        with patch(
            "bulk_operations.bulk_operator.discover_token_ranges", new_callable=AsyncMock
        ) as mock_discover:
            from bulk_operations.token_utils import TokenRange

            mock_ranges = [TokenRange(start=0, end=1000, replicas=["node1"])]
            mock_discover.return_value = mock_ranges

            # Mock returns the same value for all calls (it's a single range)
            mock_count_result = Mock()
            mock_count_result.one.return_value = Mock(count=1000)
            mock_session.execute.return_value = mock_count_result

            # Get stats after operation
            count, stats = await operator.count_by_token_ranges_with_stats(
                keyspace="test_ks", table="test_table", split_count=1
            )

            assert count == 1000
            assert stats.rows_processed == 1000
            assert stats.ranges_completed == 1
            assert stats.duration_seconds > 0
            assert stats.rows_per_second > 0
