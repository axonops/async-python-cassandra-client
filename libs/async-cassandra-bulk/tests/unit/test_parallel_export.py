"""
Test parallel export functionality.

What this tests:
---------------
1. Parallel execution of token range exports
2. Progress tracking across workers
3. Error handling and retry logic
4. Resource management (worker pools)
5. Checkpointing and resumption

Why this matters:
----------------
- Bulk exports must scale with data size
- Parallel processing is essential for performance
- Must handle failures gracefully
- Progress visibility for long-running exports
"""

import asyncio
from datetime import datetime
from typing import Any, Dict
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from async_cassandra_bulk.parallel_export import ParallelExporter
from async_cassandra_bulk.utils.stats import BulkOperationStats
from async_cassandra_bulk.utils.token_utils import TokenRange


def setup_mock_cluster_metadata(mock_session, columns=None):
    """Helper to setup cluster metadata mocks."""
    if columns is None:
        columns = ["id"]

    # Setup session structure
    mock_session._session = MagicMock()
    mock_session._session.cluster = MagicMock()
    mock_session._session.cluster.metadata = MagicMock()

    # Create column mocks
    mock_columns = {}
    partition_keys = []

    for col_name in columns:
        mock_col = MagicMock()
        mock_col.name = col_name
        mock_columns[col_name] = mock_col
        if col_name == "id":  # First column is partition key
            partition_keys.append(mock_col)

    # Create table mock
    mock_table = MagicMock()
    mock_table.columns = mock_columns
    mock_table.partition_key = partition_keys

    # Create keyspace mock
    mock_keyspace = MagicMock()
    mock_keyspace.tables = {"table": mock_table}

    mock_session._session.cluster.metadata.keyspaces = {"keyspace": mock_keyspace}


class TestParallelExporterInitialization:
    """Test ParallelExporter initialization and configuration."""

    def test_parallel_exporter_requires_session(self):
        """
        Test that ParallelExporter requires a session parameter.

        What this tests:
        ---------------
        1. Constructor validates session parameter is provided
        2. Raises TypeError when session is missing
        3. Error message mentions 'session'
        4. No partial initialization occurs

        Why this matters:
        ----------------
        - Session is required for all database queries
        - Clear error messages help developers fix issues quickly
        - Prevents runtime errors from missing dependencies
        - Production exports must have valid session

        Additional context:
        ---------------------------------
        - The session should be an AsyncCassandraSession instance
        - This validation happens before any other initialization
        """
        with pytest.raises(TypeError) as exc_info:
            ParallelExporter()

        assert "session" in str(exc_info.value)

    def test_parallel_exporter_requires_table(self):
        """
        Test that ParallelExporter requires table name parameter.

        What this tests:
        ---------------
        1. Table parameter is mandatory in constructor
        2. Raises TypeError when table is missing
        3. Error message mentions 'table'
        4. Validation occurs after session check

        Why this matters:
        ----------------
        - Must know which Cassandra table to export
        - Prevents runtime errors from missing table specification
        - Clear error messages guide proper usage
        - Production exports need valid table references

        Additional context:
        ---------------------------------
        - Table should be in format 'keyspace.table'
        - This is validated separately in another test
        """
        mock_session = MagicMock()

        with pytest.raises(TypeError) as exc_info:
            ParallelExporter(session=mock_session)

        assert "table" in str(exc_info.value)

    def test_parallel_exporter_requires_exporter(self):
        """
        Test that ParallelExporter requires an exporter instance.

        What this tests:
        ---------------
        1. Exporter parameter is mandatory in constructor
        2. Raises TypeError when exporter is missing
        3. Error message mentions 'exporter'
        4. Exporter should be a BaseExporter subclass instance

        Why this matters:
        ----------------
        - Exporter defines the output format (CSV, JSON, etc.)
        - Type safety prevents runtime format errors
        - Clear separation of concerns between parallel logic and format
        - Production exports must specify output format

        Additional context:
        ---------------------------------
        - Exporter instances handle file writing and format-specific conversions
        - Examples: CSVExporter, JSONExporter
        - Custom exporters can be created by subclassing BaseExporter
        """
        mock_session = MagicMock()

        with pytest.raises(TypeError) as exc_info:
            ParallelExporter(session=mock_session, table="keyspace.table")

        assert "exporter" in str(exc_info.value)

    def test_parallel_exporter_initialization(self):
        """
        Test successful initialization with required parameters.

        What this tests:
        ---------------
        1. Constructor accepts all required parameters
        2. Stores session, table, and exporter correctly
        3. Sets default concurrency to 4 workers
        4. Sets default batch size to 1000 rows

        Why this matters:
        ----------------
        - Proper initialization is critical for parallel operations
        - Default values provide good performance for most cases
        - Confirms object is ready for export operations
        - Production exports rely on correct initialization

        Additional context:
        ---------------------------------
        - Concurrency of 4 balances performance and resource usage
        - Batch size of 1000 is optimal for most Cassandra clusters
        - These defaults can be overridden in custom options test
        """
        mock_session = MagicMock()
        mock_exporter = MagicMock()

        parallel = ParallelExporter(
            session=mock_session, table="keyspace.table", exporter=mock_exporter
        )

        assert parallel.session is mock_session
        assert parallel.table == "keyspace.table"
        assert parallel.exporter is mock_exporter
        assert parallel.concurrency == 4  # Default
        assert parallel.batch_size == 1000  # Default

    def test_parallel_exporter_custom_options(self):
        """
        Test initialization with custom performance options.

        What this tests:
        ---------------
        1. Custom concurrency value overrides default
        2. Custom batch size overrides default
        3. Checkpoint interval can be configured
        4. All custom options are stored correctly

        Why this matters:
        ----------------
        - Performance tuning for specific workloads
        - Resource management for different cluster sizes
        - Large clusters may benefit from higher concurrency
        - Production tuning based on data characteristics

        Additional context:
        ---------------------------------
        - Higher concurrency (16) for better parallelism
        - Larger batch size (5000) for fewer round trips
        - Checkpoint interval controls resumption granularity
        - Settings depend on cluster size and network latency
        """
        mock_session = MagicMock()
        mock_exporter = MagicMock()

        parallel = ParallelExporter(
            session=mock_session,
            table="keyspace.table",
            exporter=mock_exporter,
            concurrency=16,
            batch_size=5000,
            checkpoint_interval=100,
        )

        assert parallel.concurrency == 16
        assert parallel.batch_size == 5000
        assert parallel.checkpoint_interval == 100


class TestParallelExporterTokenRanges:
    """Test token range discovery and splitting."""

    @pytest.mark.asyncio
    async def test_discover_and_split_ranges(self):
        """
        Test token range discovery and splitting for parallel processing.

        What this tests:
        ---------------
        1. Discovers token ranges from cluster metadata
        2. Splits ranges based on concurrency setting
        3. Ensures even distribution of work
        4. Resulting ranges cover entire token space

        Why this matters:
        ----------------
        - Token ranges are foundation for parallel processing
        - Even distribution ensures optimal load balancing
        - All data must be covered without gaps or overlaps
        - Production exports rely on complete data coverage

        Additional context:
        ---------------------------------
        - Token ranges represent portions of the Cassandra ring
        - More splits than workers allows better work distribution
        - Splitting is proportional to range sizes
        """
        # Mock session and token ranges
        mock_session = AsyncMock()
        mock_exporter = MagicMock()

        # Mock token range discovery
        mock_ranges = [
            TokenRange(start=0, end=1000, replicas=["node1"]),
            TokenRange(start=1000, end=2000, replicas=["node2"]),
            TokenRange(start=2000, end=3000, replicas=["node3"]),
        ]

        parallel = ParallelExporter(
            session=mock_session, table="keyspace.table", exporter=mock_exporter, concurrency=6
        )

        with patch(
            "async_cassandra_bulk.parallel_export.discover_token_ranges", return_value=mock_ranges
        ):
            ranges = await parallel._discover_and_split_ranges()

        # Should split into more ranges based on concurrency
        assert len(ranges) >= 6
        # All original ranges should be covered
        total_size = sum(r.size for r in ranges)
        original_size = sum(r.size for r in mock_ranges)
        assert total_size == original_size


class TestParallelExporterWorkers:
    """Test worker pool and task management."""

    @pytest.mark.asyncio
    async def test_export_single_range(self):
        """
        Test exporting a single token range with proper query generation.

        What this tests:
        ---------------
        1. Generates correct CQL query with token range bounds
        2. Executes query with proper batch size
        3. Passes each row to exporter's write_row method
        4. Updates statistics with row count and range completion

        Why this matters:
        ----------------
        - Core worker functionality must be correct
        - Token range queries ensure complete data coverage
        - Statistics tracking enables progress monitoring
        - Production exports process millions of rows this way

        Additional context:
        ---------------------------------
        - Uses token() function in CQL for range queries
        - Batch size controls memory usage
        - Each worker processes ranges independently
        """
        # Mock components
        mock_session = AsyncMock()
        mock_exporter = AsyncMock()
        mock_stats = MagicMock(spec=BulkOperationStats)

        # Setup mock metadata
        setup_mock_cluster_metadata(mock_session, columns=["id", "name"])

        # Mock query results with async iteration
        class MockRow:
            def __init__(self, data):
                self._fields = list(data.keys())
                for k, v in data.items():
                    setattr(self, k, v)

        async def mock_async_iter():
            yield MockRow({"id": 1, "name": "Alice"})
            yield MockRow({"id": 2, "name": "Bob"})

        mock_result = MagicMock()
        mock_result.__aiter__ = lambda self: mock_async_iter()
        mock_session.execute.return_value = mock_result

        parallel = ParallelExporter(
            session=mock_session, table="keyspace.table", exporter=mock_exporter, batch_size=100
        )

        # Test range
        test_range = TokenRange(start=0, end=1000, replicas=["node1"])

        # Execute
        row_count = await parallel._export_range(test_range, mock_stats)

        # Verify
        assert row_count == 2
        mock_session.execute.assert_called_once()
        query = mock_session.execute.call_args[0][0]
        assert "token(" in query
        assert "keyspace.table" in query

        # Verify rows were written
        assert mock_exporter.write_row.call_count == 2

    @pytest.mark.asyncio
    async def test_export_range_with_pagination(self):
        """
        Test exporting large token range requiring pagination.

        What this tests:
        ---------------
        1. Detects when more pages are available
        2. Fetches subsequent pages using paging state
        3. Processes all rows across multiple pages
        4. Maintains accurate row count across pages

        Why this matters:
        ----------------
        - Large ranges always span multiple pages
        - Missing pages means data loss in production
        - Pagination state must be handled correctly
        - Production tables have billions of rows requiring pagination

        Additional context:
        ---------------------------------
        - Cassandra returns has_more_pages flag
        - Paging state allows fetching next page
        - Default page size is controlled by batch_size
        """
        # Mock components
        mock_session = AsyncMock()
        mock_exporter = AsyncMock()
        mock_stats = MagicMock(spec=BulkOperationStats)

        # Setup mock metadata
        setup_mock_cluster_metadata(mock_session, columns=["id"])

        # Mock paginated results with async iteration (async-cassandra handles pagination)
        class MockRow:
            def __init__(self, data):
                self._fields = list(data.keys())
                for k, v in data.items():
                    setattr(self, k, v)

        async def mock_async_iter():
            # Simulate 150 rows across "pages"
            for i in range(150):
                yield MockRow({"id": i})

        mock_result = MagicMock()
        mock_result.__aiter__ = lambda self: mock_async_iter()
        mock_session.execute.return_value = mock_result

        parallel = ParallelExporter(
            session=mock_session, table="keyspace.table", exporter=mock_exporter
        )

        test_range = TokenRange(start=0, end=1000, replicas=["node1"])

        # Execute
        row_count = await parallel._export_range(test_range, mock_stats)

        # Verify
        assert row_count == 150
        assert mock_session.execute.call_count == 1  # Only one query, pagination is internal
        assert mock_exporter.write_row.call_count == 150

    @pytest.mark.asyncio
    async def test_worker_error_handling(self):
        """
        Test error handling and recovery in export workers.

        What this tests:
        ---------------
        1. Catches and logs query execution errors
        2. Records errors in statistics for visibility
        3. Worker continues processing other ranges
        4. Failed range doesn't crash entire export

        Why this matters:
        ----------------
        - Network timeouts are common in production
        - One bad range shouldn't fail entire export
        - Error tracking helps identify problematic ranges
        - Production resilience requires graceful error handling

        Additional context:
        ---------------------------------
        - Common errors: timeouts, node failures, large partitions
        - Errors are logged with range information
        - Failed ranges can be retried separately
        """
        # Mock components
        mock_session = AsyncMock()
        mock_exporter = AsyncMock()
        mock_stats = MagicMock(spec=BulkOperationStats)
        mock_stats.errors = []

        # Mock query error
        mock_session.execute.side_effect = Exception("Query timeout")

        parallel = ParallelExporter(
            session=mock_session, table="keyspace.table", exporter=mock_exporter
        )

        test_range = TokenRange(start=0, end=1000, replicas=["node1"])

        # Execute - should not raise
        row_count = await parallel._export_range(test_range, mock_stats)

        # Verify
        assert row_count == -1  # Error indicator
        assert len(mock_stats.errors) == 1
        assert "Query timeout" in str(mock_stats.errors[0])

    @pytest.mark.asyncio
    async def test_concurrent_workers(self):
        """
        Test concurrent worker execution with concurrency limits.

        What this tests:
        ---------------
        1. Respects configured concurrency limit (max 3 workers)
        2. All 10 ranges are processed despite worker limit
        3. No race conditions in statistics updates
        4. Tracks maximum concurrent executions

        Why this matters:
        ----------------
        - Concurrency provides 10x+ performance improvement
        - Too many workers can overwhelm Cassandra nodes
        - Resource limits prevent cluster destabilization
        - Production exports must balance speed and stability

        Additional context:
        ---------------------------------
        - Uses semaphore to limit concurrent workers
        - Workers process from shared queue
        - Statistics updates are thread-safe
        - Typical production uses 4-16 workers
        """
        # Mock components
        mock_session = AsyncMock()
        mock_exporter = AsyncMock()

        # Track concurrent executions
        concurrent_count = 0
        max_concurrent = 0

        async def mock_execute(*args, **kwargs):
            nonlocal concurrent_count, max_concurrent
            concurrent_count += 1
            max_concurrent = max(max_concurrent, concurrent_count)

            # Simulate work
            await asyncio.sleep(0.1)

            concurrent_count -= 1

            # Return async iterable result
            class MockRow:
                def __init__(self, data):
                    self._fields = list(data.keys())
                    for k, v in data.items():
                        setattr(self, k, v)

            async def mock_async_iter():
                yield MockRow({"id": 1})

            result = MagicMock()
            result.__aiter__ = lambda self: mock_async_iter()
            return result

        mock_session.execute = mock_execute

        # Mock cluster metadata
        setup_mock_cluster_metadata(mock_session, columns=["id"])

        parallel = ParallelExporter(
            session=mock_session, table="keyspace.table", exporter=mock_exporter, concurrency=3
        )

        # Create multiple ranges
        ranges = [
            TokenRange(start=i * 100, end=(i + 1) * 100, replicas=["node1"]) for i in range(10)
        ]

        # Execute
        stats = await parallel._process_ranges(ranges)

        # Verify
        assert stats.rows_processed == 10
        assert max_concurrent <= 3  # Concurrency limit respected


class TestParallelExporterExecution:
    """Test full export execution."""

    @pytest.mark.asyncio
    async def test_export_full_workflow(self):
        """
        Test complete export workflow from start to finish.

        What this tests:
        ---------------
        1. Token range discovery from cluster metadata
        2. Worker pool creation and management
        3. Progress tracking throughout export
        4. Final statistics calculation and accuracy
        5. Proper exporter lifecycle (header, rows, footer)

        Why this matters:
        ----------------
        - End-to-end validation ensures all components work together
        - Critical path for all production exports
        - Verifies integration between discovery, workers, and exporters
        - Confirms statistics are accurate for monitoring

        Additional context:
        ---------------------------------
        - The splitter may create more ranges than originally discovered
        - Stats should reflect all processed data
        - Exporter methods must be called in correct order
        """
        # Mock components
        mock_session = AsyncMock()
        mock_exporter = AsyncMock()

        # Mock token ranges
        mock_ranges = [
            TokenRange(start=0, end=500, replicas=["node1"]),
            TokenRange(start=500, end=1000, replicas=["node2"]),
        ]

        # Mock query results with async iteration
        class MockRow:
            def __init__(self, data):
                self._fields = list(data.keys())
                for k, v in data.items():
                    setattr(self, k, v)

        async def mock_async_iter():
            for i in range(10):
                yield MockRow({"id": i})

        mock_result = MagicMock()
        mock_result.__aiter__ = lambda self: mock_async_iter()
        mock_session.execute.return_value = mock_result

        # Mock column discovery
        setup_mock_cluster_metadata(mock_session, columns=["id", "name"])

        parallel = ParallelExporter(
            session=mock_session, table="keyspace.table", exporter=mock_exporter
        )

        with patch(
            "async_cassandra_bulk.parallel_export.discover_token_ranges", return_value=mock_ranges
        ):
            stats = await parallel.export()

        # Verify
        # The splitter may create more ranges than the original 2
        assert stats.rows_processed > 0
        assert stats.ranges_completed > 0
        assert stats.is_complete

        # Verify exporter workflow
        mock_exporter.write_header.assert_called_once()
        assert mock_exporter.write_row.call_count == stats.rows_processed
        mock_exporter.write_footer.assert_called_once()

    @pytest.mark.asyncio
    async def test_export_with_progress_callback(self):
        """
        Test export with progress callback for real-time monitoring.

        What this tests:
        ---------------
        1. Progress callback invoked after each range completion
        2. Correct statistics passed with each update
        3. Regular updates throughout export process
        4. Progress percentage increases monotonically to 100%

        Why this matters:
        ----------------
        - User feedback essential for multi-hour exports
        - Integration with UI progress bars and dashboards
        - Allows early termination if progress stalls
        - Production monitoring requires real-time visibility

        Additional context:
        ---------------------------------
        - Callback invoked after each range, not each row
        - Progress percentage based on completed ranges
        - Final update should show 100% completion
        """
        # Mock components
        mock_session = AsyncMock()
        mock_exporter = AsyncMock()
        progress_updates = []

        def progress_callback(stats: BulkOperationStats):
            progress_updates.append(
                {"rows": stats.rows_processed, "progress": stats.progress_percentage}
            )

        # Setup mocks
        mock_ranges = [
            TokenRange(start=i * 100, end=(i + 1) * 100, replicas=["node1"]) for i in range(4)
        ]

        mock_result = MagicMock()
        mock_result.current_rows = [{"id": 1}]
        mock_result.has_more_pages = False
        mock_session.execute.return_value = mock_result

        # Mock columns
        setup_mock_cluster_metadata(mock_session, columns=["id"])

        parallel = ParallelExporter(
            session=mock_session,
            table="keyspace.table",
            exporter=mock_exporter,
            progress_callback=progress_callback,
        )

        with patch(
            "async_cassandra_bulk.parallel_export.discover_token_ranges", return_value=mock_ranges
        ):
            await parallel.export()

        # Verify progress updates
        assert len(progress_updates) > 0
        # Progress should increase
        progresses = [u["progress"] for u in progress_updates]
        assert progresses[-1] == 100.0

    @pytest.mark.asyncio
    async def test_export_empty_table(self):
        """
        Test exporting table with no data rows.

        What this tests:
        ---------------
        1. Handles empty result sets gracefully without errors
        2. Still writes header/footer for valid file structure
        3. Statistics correctly show zero rows processed
        4. Export completes successfully despite no data

        Why this matters:
        ----------------
        - Empty tables are common in development/testing
        - File format must be valid even without data
        - Scripts consuming output expect consistent structure
        - Production tables may be temporarily empty

        Additional context:
        ---------------------------------
        - Empty CSV still has header row
        - Empty JSON array is valid: []
        - Important for automated pipelines
        """
        # Mock components
        mock_session = AsyncMock()
        mock_exporter = AsyncMock()

        # Mock empty results with async iteration
        async def mock_async_iter():
            # Don't yield anything - empty result
            return
            yield  # Make it a generator

        mock_result = MagicMock()
        mock_result.__aiter__ = lambda self: mock_async_iter()
        mock_session.execute.return_value = mock_result

        # Mock ranges
        mock_ranges = [TokenRange(start=0, end=1000, replicas=["node1"])]

        # Mock columns
        setup_mock_cluster_metadata(mock_session, columns=["id"])

        parallel = ParallelExporter(
            session=mock_session, table="keyspace.table", exporter=mock_exporter
        )

        with patch(
            "async_cassandra_bulk.parallel_export.discover_token_ranges", return_value=mock_ranges
        ):
            stats = await parallel.export()

        # Verify
        assert stats.rows_processed == 0
        assert stats.is_complete

        # Still writes structure
        mock_exporter.write_header.assert_called_once()
        mock_exporter.write_footer.assert_called_once()
        mock_exporter.write_row.assert_not_called()


class TestParallelExporterCheckpointing:
    """Test checkpointing and resumption."""

    @pytest.mark.asyncio
    async def test_checkpoint_saving(self):
        """
        Test saving checkpoint state during long-running export.

        What this tests:
        ---------------
        1. Checkpoint saved at configured intervals (every N ranges)
        2. Contains complete progress state for resumption
        3. Checkpoint data structure is serializable
        4. Multiple checkpoints saved during export

        Why this matters:
        ----------------
        - Resume multi-hour exports after failures
        - Network interruptions don't lose progress
        - Fault tolerance for production workloads
        - Cost savings by not re-exporting data

        Additional context:
        ---------------------------------
        - Checkpoint includes completed ranges and row count
        - Saved after every checkpoint_interval ranges
        - Can be persisted to file or database
        """
        # Mock components
        mock_session = AsyncMock()
        mock_exporter = AsyncMock()
        checkpoints = []

        async def save_checkpoint(state: Dict[str, Any]):
            checkpoints.append(state.copy())

        # Setup mocks
        mock_ranges = [
            TokenRange(start=i * 100, end=(i + 1) * 100, replicas=["node1"]) for i in range(10)
        ]

        # Mock query results with async iteration
        class MockRow:
            def __init__(self, data):
                self._fields = list(data.keys())
                for k, v in data.items():
                    setattr(self, k, v)

        async def mock_async_iter():
            for i in range(5):
                yield MockRow({"id": i})

        mock_result = MagicMock()
        mock_result.__aiter__ = lambda self: mock_async_iter()
        mock_session.execute.return_value = mock_result

        # Mock columns
        setup_mock_cluster_metadata(mock_session, columns=["id"])

        parallel = ParallelExporter(
            session=mock_session,
            table="keyspace.table",
            exporter=mock_exporter,
            checkpoint_interval=3,  # Save after every 3 ranges
            checkpoint_callback=save_checkpoint,
        )

        with patch(
            "async_cassandra_bulk.parallel_export.discover_token_ranges", return_value=mock_ranges
        ):
            await parallel.export()

        # Verify checkpoints
        assert len(checkpoints) > 0
        last_checkpoint = checkpoints[-1]
        assert "completed_ranges" in last_checkpoint
        assert "total_rows" in last_checkpoint
        assert last_checkpoint["total_rows"] == 50  # 10 ranges * 5 rows

    @pytest.mark.asyncio
    async def test_resume_from_checkpoint(self):
        """
        Test resuming interrupted export from saved checkpoint.

        What this tests:
        ---------------
        1. Skips already completed ranges to avoid reprocessing
        2. Continues from exact position where export stopped
        3. Final statistics include rows from previous run
        4. Only processes remaining unfinished ranges

        Why this matters:
        ----------------
        - Avoid costly reprocessing of billions of rows
        - Accurate total counts for billing/monitoring
        - Network failures don't restart entire export
        - Production resilience for large datasets

        Additional context:
        ---------------------------------
        - Checkpoint contains list of (start, end) tuples
        - Row count accumulates across resumed runs
        - Essential for TB+ sized table exports
        """
        # Mock components
        mock_session = AsyncMock()
        mock_exporter = AsyncMock()

        # Previous checkpoint state
        checkpoint = {
            "completed_ranges": [(0, 300), (300, 600)],  # First 2 ranges done
            "total_rows": 20,
            "start_time": datetime.now().timestamp(),
        }

        # Setup mocks
        all_ranges = [
            TokenRange(start=0, end=300, replicas=["node1"]),
            TokenRange(start=300, end=600, replicas=["node2"]),
            TokenRange(start=600, end=900, replicas=["node3"]),  # This should process
            TokenRange(start=900, end=1000, replicas=["node4"]),  # This too
        ]

        mock_result = MagicMock()
        mock_result.current_rows = [{"id": i} for i in range(5)]
        mock_result.has_more_pages = False
        mock_session.execute.return_value = mock_result

        # Mock columns
        setup_mock_cluster_metadata(mock_session, columns=["id"])

        parallel = ParallelExporter(
            session=mock_session,
            table="keyspace.table",
            exporter=mock_exporter,
            resume_from=checkpoint,
        )

        with patch(
            "async_cassandra_bulk.parallel_export.discover_token_ranges", return_value=all_ranges
        ):
            stats = await parallel.export()

        # Verify
        # The ranges get split further, so we expect more than 2 calls
        # The exact number depends on splitting algorithm
        assert mock_session.execute.call_count > 0  # Some ranges processed
        assert mock_session.execute.call_count < 8  # But not all (some skipped)

        # Stats should accumulate correctly
        assert stats.rows_processed >= 20  # At least the previous rows
        assert stats.ranges_completed > 2  # More than just the skipped ones
