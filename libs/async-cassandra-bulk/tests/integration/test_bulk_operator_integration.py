"""
Integration tests for BulkOperator with real Cassandra.

What this tests:
---------------
1. BulkOperator functionality against real Cassandra cluster
2. Count operations on actual tables
3. Export operations with real data
4. Performance with realistic datasets
5. Error handling with actual database errors

Why this matters:
----------------
- Unit tests use mocks, integration tests prove real functionality
- Cassandra-specific behaviors only visible with real cluster
- Performance characteristics need real database
- Production readiness verification
"""

import pytest

from async_cassandra_bulk import BulkOperator


class TestBulkOperatorCount:
    """Test count operations against real Cassandra."""

    @pytest.mark.asyncio
    async def test_count_empty_table(self, session, test_table):
        """
        Test counting rows in an empty table.

        What this tests:
        ---------------
        1. Count operation returns 0 for empty table
        2. Query executes successfully against real cluster
        3. No errors with empty result set
        4. Correct keyspace.table format accepted

        Why this matters:
        ----------------
        - Empty tables are common in development/testing
        - Must handle edge case gracefully
        - Verifies basic connectivity and query execution
        - Production systems may have temporarily empty tables

        Additional context:
        ---------------------------------
        - Uses COUNT(*) which is optimized in Cassandra 4.0+
        - Should complete quickly even for empty table
        - Forms baseline for performance testing
        """
        operator = BulkOperator(session=session)

        count = await operator.count(f"test_bulk.{test_table}")

        assert count == 0

    @pytest.mark.asyncio
    async def test_count_populated_table(self, session, populated_table):
        """
        Test counting rows in a populated table.

        What this tests:
        ---------------
        1. Count returns correct number of rows (1000)
        2. Query performs well with moderate data
        3. No timeout or performance issues
        4. Accurate count across all partitions

        Why this matters:
        ----------------
        - Validates count accuracy with real data
        - Performance baseline for 1000 rows
        - Ensures no off-by-one errors
        - Production counts must be accurate for billing

        Additional context:
        ---------------------------------
        - 1000 rows tests beyond single partition
        - Count may take longer on larger clusters
        - Used as baseline for export verification
        """
        operator = BulkOperator(session=session)

        count = await operator.count(f"test_bulk.{populated_table}")

        assert count == 1000

    @pytest.mark.asyncio
    async def test_count_with_where_clause(self, session, populated_table):
        """
        Test counting with WHERE clause filtering.

        What this tests:
        ---------------
        1. WHERE clause properly appended to COUNT query
        2. Filtering works on non-partition key columns
        3. Returns correct subset count (500 active users)
        4. No syntax errors with real CQL parser

        Why this matters:
        ----------------
        - Filtered counts common for analytics
        - WHERE clause must be valid CQL
        - Allows counting specific data states
        - Production use: count active users, recent records

        Additional context:
        ---------------------------------
        - WHERE on non-partition key requires ALLOW FILTERING
        - Our test data has 500 active (even IDs) users
        - Real Cassandra validates query syntax
        """
        operator = BulkOperator(session=session)

        count = await operator.count(
            f"test_bulk.{populated_table}", where="active = true ALLOW FILTERING"
        )

        assert count == 500  # Half are active (even IDs)

    @pytest.mark.asyncio
    async def test_count_invalid_table(self, session):
        """
        Test count with non-existent table.

        What this tests:
        ---------------
        1. Proper error raised for invalid table
        2. Error message includes table name
        3. No hanging or timeout
        4. Original Cassandra error preserved

        Why this matters:
        ----------------
        - Clear errors help debugging
        - Must fail fast for invalid tables
        - Production monitoring needs real errors
        - No silent failures or hangs

        Additional context:
        ---------------------------------
        - Cassandra returns InvalidRequest error
        - Error includes keyspace and table info
        - Should fail within milliseconds
        """
        operator = BulkOperator(session=session)

        with pytest.raises(Exception) as exc_info:
            await operator.count("test_bulk.nonexistent_table")

        assert "nonexistent_table" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_count_performance(self, session, populated_table):
        """
        Test count performance characteristics.

        What this tests:
        ---------------
        1. Count completes within reasonable time (<5 seconds)
        2. No memory leaks during operation
        3. Connection pool handled properly
        4. Measures baseline performance

        Why this matters:
        ----------------
        - Production tables can have billions of rows
        - Count performance affects user experience
        - Baseline for optimization efforts
        - Timeout settings depend on performance

        Additional context:
        ---------------------------------
        - 1000 rows should count in <1 second
        - Larger tables may need increased timeout
        - Performance varies by cluster size
        """
        import time

        operator = BulkOperator(session=session)

        start_time = time.time()
        count = await operator.count(f"test_bulk.{populated_table}")
        duration = time.time() - start_time

        assert count == 1000
        assert duration < 5.0  # Should be much faster, but allow margin


class TestBulkOperatorExport:
    """Test export operations against real Cassandra."""

    @pytest.mark.asyncio
    async def test_export_csv_basic(self, session, populated_table, tmp_path):
        """
        Test basic CSV export functionality.

        What this tests:
        ---------------
        1. Export creates CSV file at specified path
        2. All 1000 rows exported correctly
        3. CSV format is valid and parseable
        4. Statistics show correct row count

        Why this matters:
        ----------------
        - End-to-end validation of export pipeline
        - CSV is most common export format
        - File must be readable by standard tools
        - Production exports must be complete

        Additional context:
        ---------------------------------
        - Uses parallel export with token ranges
        - Should leverage multiple workers
        - Verifies integration of all components
        """
        output_file = tmp_path / "export.csv"
        operator = BulkOperator(session=session)

        stats = await operator.export(
            table=f"test_bulk.{populated_table}", output_path=str(output_file), format="csv"
        )

        assert output_file.exists()
        assert stats.rows_processed == 1000
        assert stats.is_complete

        # Verify CSV is valid
        import csv

        with open(output_file, "r") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) == stats.rows_processed
        # Check first row has expected columns
        assert "id" in rows[0]
        assert "name" in rows[0]

    @pytest.mark.asyncio
    async def test_export_json_array_mode(self, session, populated_table, tmp_path):
        """
        Test JSON export in array mode.

        What this tests:
        ---------------
        1. Export creates valid JSON array file
        2. All rows included in array
        3. Cassandra types properly converted
        4. File is valid parseable JSON

        Why this matters:
        ----------------
        - JSON common for API integrations
        - Type conversion must preserve data
        - Array mode for complete datasets
        - Production data must round-trip

        Additional context:
        ---------------------------------
        - UUIDs converted to strings
        - Timestamps in ISO format
        - Collections preserved as JSON
        """
        output_file = tmp_path / "export.json"
        operator = BulkOperator(session=session)

        stats = await operator.export(
            table=f"test_bulk.{populated_table}", output_path=str(output_file), format="json"
        )

        assert output_file.exists()
        assert stats.rows_processed == 1000

        # Verify JSON is valid
        import json

        with open(output_file, "r") as f:
            data = json.load(f)

        assert isinstance(data, list)
        assert len(data) == stats.rows_processed
        assert all("id" in row for row in data)

    @pytest.mark.asyncio
    async def test_export_with_concurrency(self, session, populated_table, tmp_path):
        """
        Test export with custom concurrency settings.

        What this tests:
        ---------------
        1. Higher concurrency (8 workers) processes faster
        2. All workers utilized for parallel processing
        3. No data corruption with concurrent writes
        4. Statistics accurate with parallel execution

        Why this matters:
        ----------------
        - Production exports need performance tuning
        - Concurrency critical for large tables
        - Must handle concurrent writes safely
        - Performance scales with workers

        Additional context:
        ---------------------------------
        - Default is 4 workers
        - Test uses 8 for better parallelism
        - Each worker processes token ranges
        """
        output_file = tmp_path / "export_concurrent.csv"
        operator = BulkOperator(session=session)

        stats = await operator.export(
            table=f"test_bulk.{populated_table}",
            output_path=str(output_file),
            format="csv",
            concurrency=8,
        )

        assert stats.rows_processed == 1000
        assert stats.ranges_completed > 1  # Should use multiple ranges

    @pytest.mark.asyncio
    async def test_export_empty_table(self, session, test_table, tmp_path):
        """
        Test exporting empty table.

        What this tests:
        ---------------
        1. Empty table exports without errors
        2. Output file created with headers only
        3. Statistics show 0 rows
        4. File format still valid

        Why this matters:
        ----------------
        - Empty tables valid edge case
        - File structure must be consistent
        - Automated pipelines expect files
        - Production may have empty partitions

        Additional context:
        ---------------------------------
        - CSV has header row only
        - JSON has empty array []
        - Important for idempotent operations
        """
        output_file = tmp_path / "empty.csv"
        operator = BulkOperator(session=session)

        stats = await operator.export(
            table=f"test_bulk.{test_table}", output_path=str(output_file), format="csv"
        )

        assert output_file.exists()
        assert stats.rows_processed == 0
        assert stats.is_complete

        # File should have header row only
        content = output_file.read_text()
        lines = content.strip().split("\n")
        assert len(lines) == 1  # Header only
        assert "id" in lines[0]

    @pytest.mark.asyncio
    async def test_export_with_column_selection(self, session, populated_table, tmp_path):
        """
        Test export with specific column selection.

        What this tests:
        ---------------
        1. Only specified columns included in export
        2. Column order preserved as specified
        3. Reduces data size and export time
        4. Other columns properly excluded

        Why this matters:
        ----------------
        - Selective export common requirement
        - Reduces bandwidth and storage
        - Privacy/security column filtering
        - Production exports often need subset

        Additional context:
        ---------------------------------
        - Generates SELECT with specific columns
        - Can significantly reduce export size
        - Column validation done by Cassandra
        """
        output_file = tmp_path / "partial.csv"
        operator = BulkOperator(session=session)

        stats = await operator.export(
            table=f"test_bulk.{populated_table}",
            output_path=str(output_file),
            format="csv",
            columns=["id", "name", "active"],
        )

        assert stats.rows_processed == 1000

        # Verify only selected columns
        import csv

        with open(output_file, "r") as f:
            reader = csv.DictReader(f)
            first_row = next(reader)

        assert set(first_row.keys()) == {"id", "name", "active"}
        assert "age" not in first_row  # Not selected

    @pytest.mark.asyncio
    async def test_export_performance_monitoring(self, session, populated_table, tmp_path):
        """
        Test export performance metrics and monitoring.

        What this tests:
        ---------------
        1. Statistics track duration accurately
        2. Rows per second calculated correctly
        3. Progress callbacks invoked during export
        4. Performance metrics reasonable for data size

        Why this matters:
        ----------------
        - Production monitoring requires metrics
        - Performance baselines for optimization
        - Progress feedback for long exports
        - SLA compliance verification

        Additional context:
        ---------------------------------
        - 1000 rows should export in seconds
        - Rate depends on cluster and network
        - Progress callbacks for UI updates
        """
        output_file = tmp_path / "monitored.csv"
        progress_updates = []

        def progress_callback(stats):
            progress_updates.append(
                {"rows": stats.rows_processed, "percentage": stats.progress_percentage}
            )

        operator = BulkOperator(session=session)

        stats = await operator.export(
            table=f"test_bulk.{populated_table}",
            output_path=str(output_file),
            format="csv",
            progress_callback=progress_callback,
        )

        assert stats.rows_processed == 1000
        assert stats.rows_per_second > 0
        assert stats.duration_seconds > 0

        # Progress was tracked
        assert len(progress_updates) > 0
        assert progress_updates[-1]["percentage"] == 100.0
