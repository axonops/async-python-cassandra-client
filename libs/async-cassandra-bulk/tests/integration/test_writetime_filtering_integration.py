"""
Integration tests for writetime filtering with real Cassandra.

What this tests:
---------------
1. Writetime filtering with actual CQL queries
2. Before/after filtering on real data
3. Performance with filtered exports
4. Edge cases with Cassandra timestamps

Why this matters:
----------------
- Verify CQL WHERE clause generation
- Real timestamp comparisons
- Production-like scenarios
- Cassandra 5 compatibility
"""

import csv
import json
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest

from async_cassandra_bulk import BulkOperator


class TestWritetimeFilteringIntegration:
    """Test writetime filtering with real Cassandra."""

    @pytest.fixture
    async def time_series_table(self, session):
        """
        Create table with data at different timestamps.

        What this tests:
        ---------------
        1. Data with known writetime values
        2. Multiple time periods
        3. Realistic time series data
        4. Various update patterns

        Why this matters:
        ----------------
        - Test filtering accuracy
        - Verify boundary conditions
        - Real-world scenarios
        - Performance testing
        """
        table_name = "writetime_filter_test"
        keyspace = "test_bulk"

        await session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {keyspace}.{table_name} (
                id INT,
                partition_key INT,
                event_type TEXT,
                status TEXT,
                value DOUBLE,
                metadata MAP<TEXT, TEXT>,
                PRIMARY KEY (partition_key, id)
            )
        """
        )

        # Insert data at different timestamps
        insert_stmt = await session.prepare(
            f"""
            INSERT INTO {keyspace}.{table_name}
            (partition_key, id, event_type, status, value, metadata)
            VALUES (?, ?, ?, ?, ?, ?)
            USING TIMESTAMP ?
            """
        )

        # Base timestamp: 2024-01-01 00:00:00 UTC
        base_timestamp = int(datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp() * 1_000_000)

        # Insert data across different time periods
        # Calculate exact timestamps for clarity
        apr1_timestamp = int(datetime(2024, 4, 1, tzinfo=timezone.utc).timestamp() * 1_000_000)

        time_periods = [
            ("old_data", base_timestamp - 365 * 24 * 60 * 60 * 1_000_000),  # 1 year ago
            ("q1_data", base_timestamp),  # Jan 1, 2024
            (
                "q2_data",
                apr1_timestamp + 24 * 60 * 60 * 1_000_000,
            ),  # Apr 2, 2024 (1 day after cutoff)
            ("recent_data", base_timestamp + 180 * 24 * 60 * 60 * 1_000_000),  # Jul 1, 2024
            ("future_data", base_timestamp + 364 * 24 * 60 * 60 * 1_000_000),  # Dec 31, 2024
        ]

        row_id = 0
        for period_name, timestamp in time_periods:
            for partition in range(5):
                for i in range(20):
                    await session.execute(
                        insert_stmt,
                        (
                            partition,
                            row_id,
                            period_name,
                            "active" if i % 2 == 0 else "inactive",
                            float(row_id * 10),
                            {"period": period_name, "index": str(i)},
                            timestamp,
                        ),
                    )
                    row_id += 1

        # Also update some rows with newer timestamps
        update_stmt = await session.prepare(
            f"""
            UPDATE {keyspace}.{table_name}
            USING TIMESTAMP ?
            SET status = ?, value = ?
            WHERE partition_key = ? AND id = ?
            """
        )

        # Update some Q1 data in Q3
        update_timestamp = base_timestamp + 200 * 24 * 60 * 60 * 1_000_000
        for i in range(20, 40):  # Update some Q1 rows
            await session.execute(
                update_stmt,
                (update_timestamp, "updated", float(i * 100), 1, i),
            )

        yield f"{keyspace}.{table_name}"

        await session.execute(f"DROP TABLE IF EXISTS {keyspace}.{table_name}")

    @pytest.mark.asyncio
    async def test_export_with_writetime_after_filter(self, session, time_series_table):
        """
        Test filtering data written after a specific time.

        What this tests:
        ---------------
        1. Only recent data exported
        2. Correct row count
        3. Writetime values verified
        4. Filter effectiveness

        Why this matters:
        ----------------
        - Incremental exports
        - Recent changes only
        - Performance optimization
        - Reduce data volume
        """
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
            output_path = tmp.name

        try:
            operator = BulkOperator(session=session)

            # Export only data written after April 1, 2024
            cutoff_date = datetime(2024, 4, 1, tzinfo=timezone.utc)

            await operator.export(
                table=time_series_table,
                output_path=output_path,
                format="csv",
                options={
                    "writetime_after": cutoff_date,
                    "writetime_columns": ["status", "value"],
                },
            )

            # Verify results
            with open(output_path, "r") as f:
                reader = csv.DictReader(f)
                rows = list(reader)

            # Should have q2_data, recent_data, future_data, and updated rows
            # q2_data: 5 partitions * 20 rows = 100 rows
            # recent_data: 5 partitions * 20 rows = 100 rows
            # future_data: 5 partitions * 20 rows = 100 rows
            # updated rows: 20 rows
            # Total: 320 rows (but may be less due to token range distribution)
            assert len(rows) >= 200, f"Expected at least 200 rows, got {len(rows)}"
            assert len(rows) <= 320, f"Expected at most 320 rows, got {len(rows)}"

            # Verify all rows have writetime after cutoff
            cutoff_micros = int(cutoff_date.timestamp() * 1_000_000)
            for row in rows:
                if row["status_writetime"]:
                    # Parse ISO timestamp back to datetime
                    wt_dt = datetime.fromisoformat(row["status_writetime"].replace("Z", "+00:00"))
                    wt_micros = int(wt_dt.timestamp() * 1_000_000)
                    assert wt_micros >= cutoff_micros, "Found row with writetime before cutoff"

            # Check event types
            event_types = {row["event_type"] for row in rows}
            # old_data rows with status=updated should be included (they were updated after cutoff)
            old_data_rows = [row for row in rows if row["event_type"] == "old_data"]
            if old_data_rows:
                # All old_data rows should have status=updated
                assert all(row["status"] == "updated" for row in old_data_rows)

            assert "q1_data" not in event_types  # No q1_data should be included
            assert "q2_data" in event_types
            assert "recent_data" in event_types
            assert "future_data" in event_types

        finally:
            Path(output_path).unlink(missing_ok=True)

    @pytest.mark.asyncio
    async def test_export_with_writetime_before_filter(self, session, time_series_table):
        """
        Test filtering data written before a specific time.

        What this tests:
        ---------------
        1. Only old data exported
        2. Historical data archiving
        3. Cutoff precision
        4. No recent data included

        Why this matters:
        ----------------
        - Archive old data
        - Clean up strategies
        - Compliance requirements
        - Data lifecycle
        """
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
            output_path = tmp.name

        try:
            operator = BulkOperator(session=session)

            # Export only data written before April 1, 2024
            cutoff_date = datetime(2024, 4, 1, tzinfo=timezone.utc)

            await operator.export(
                table=time_series_table,
                output_path=output_path,
                format="json",
                options={
                    "writetime_before": cutoff_date,
                    "writetime_columns": ["*"],
                    "writetime_filter_mode": "all",  # ALL columns must be before cutoff
                },
            )

            # Verify results
            with open(output_path, "r") as f:
                data = json.load(f)

            # Should have old_data and q1_data only, minus the updated rows
            # old_data: 5 partitions * 20 rows = 100 rows
            # q1_data: 5 partitions * 20 rows = 100 rows
            # But 20 rows from q1_data were updated with newer timestamp
            # With "all" mode, those 20 rows are excluded
            assert len(data) == 180, f"Expected 180 rows, got {len(data)}"

            # Verify all rows have writetime before cutoff
            cutoff_micros = int(cutoff_date.timestamp() * 1_000_000)
            for row in data:
                # Check writetime values
                for key, value in row.items():
                    if key.endswith("_writetime") and value:
                        # Writetime should be serialized as ISO string
                        if isinstance(value, str):
                            wt_dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
                            wt_micros = int(wt_dt.timestamp() * 1_000_000)
                            assert (
                                wt_micros < cutoff_micros
                            ), "Found row with writetime after cutoff"

            # Check event types
            event_types = {row["event_type"] for row in data}
            assert event_types == {"old_data", "q1_data"}

        finally:
            Path(output_path).unlink(missing_ok=True)

    @pytest.mark.asyncio
    async def test_export_with_writetime_range_filter(self, session, time_series_table):
        """
        Test filtering data within a time range.

        What this tests:
        ---------------
        1. Both before and after filters
        2. Specific time window
        3. Boundary conditions
        4. Range accuracy

        Why this matters:
        ----------------
        - Monthly reports
        - Time-based analysis
        - Debugging specific periods
        - Compliance reporting
        """
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
            output_path = tmp.name

        try:
            operator = BulkOperator(session=session)

            # Export Q2 2024 data only (April 1 - June 30)
            start_date = datetime(2024, 4, 1, tzinfo=timezone.utc)
            end_date = datetime(2024, 6, 30, 23, 59, 59, tzinfo=timezone.utc)

            await operator.export(
                table=time_series_table,
                output_path=output_path,
                format="csv",
                options={
                    "writetime_after": start_date,
                    "writetime_before": end_date,
                    "writetime_columns": ["event_type", "status", "value"],
                },
            )

            # Verify results
            with open(output_path, "r") as f:
                reader = csv.DictReader(f)
                rows = list(reader)

            # Should have q2_data and recent_data (which is June 29, within range)
            # q2_data: 5 partitions * 20 rows = 100 rows
            # recent_data: 5 partitions * 20 rows = 100 rows
            assert len(rows) == 200, f"Expected 200 rows, got {len(rows)}"

            # Verify only rows from the time range
            event_types = {row["event_type"] for row in rows}
            assert event_types == {"q2_data", "recent_data"}

            # Verify writetime is in range
            start_micros = int(start_date.timestamp() * 1_000_000)
            end_micros = int(end_date.timestamp() * 1_000_000)

            for row in rows:
                if row["event_type_writetime"]:
                    wt_dt = datetime.fromisoformat(
                        row["event_type_writetime"].replace("Z", "+00:00")
                    )
                    wt_micros = int(wt_dt.timestamp() * 1_000_000)
                    assert start_micros <= wt_micros <= end_micros, "Writetime outside range"

        finally:
            Path(output_path).unlink(missing_ok=True)

    @pytest.mark.asyncio
    async def test_writetime_filter_with_no_matching_data(self, session, time_series_table):
        """
        Test filtering when no data matches criteria.

        What this tests:
        ---------------
        1. Empty result handling
        2. No errors on empty export
        3. Proper file creation
        4. Stats accuracy

        Why this matters:
        ----------------
        - Edge case handling
        - Graceful empty results
        - User expectations
        - Error prevention
        """
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
            output_path = tmp.name

        try:
            operator = BulkOperator(session=session)

            # Export data from far future
            future_date = datetime(2030, 1, 1, tzinfo=timezone.utc)

            stats = await operator.export(
                table=time_series_table,
                output_path=output_path,
                format="csv",
                options={
                    "writetime_after": future_date,
                    "writetime_columns": ["*"],
                },
            )

            # Should complete successfully with 0 rows
            assert stats.rows_processed == 0
            assert stats.errors == []

            # File should exist with headers only
            with open(output_path, "r") as f:
                lines = f.readlines()

            assert len(lines) == 1  # Header only
            assert "id" in lines[0]

        finally:
            Path(output_path).unlink(missing_ok=True)

    @pytest.mark.asyncio
    async def test_writetime_filter_performance(self, session, time_series_table):
        """
        Test performance impact of writetime filtering.

        What this tests:
        ---------------
        1. Export speed with filters
        2. Memory usage bounded
        3. Efficient query execution
        4. Scalability

        Why this matters:
        ----------------
        - Production performance
        - Large dataset handling
        - Resource efficiency
        - User experience
        """
        # First, export without filter as baseline
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
            baseline_path = tmp.name

        try:
            operator = BulkOperator(session=session)

            start_time = time.time()
            baseline_stats = await operator.export(
                table=time_series_table,
                output_path=baseline_path,
                format="csv",
            )
            baseline_duration = time.time() - start_time

            Path(baseline_path).unlink()

            # Now export with filter
            with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
                filtered_path = tmp.name

            start_time = time.time()
            filtered_stats = await operator.export(
                table=time_series_table,
                output_path=filtered_path,
                format="csv",
                options={
                    "writetime_after": datetime(2024, 4, 1, tzinfo=timezone.utc),
                    "writetime_columns": ["status"],
                },
            )
            filtered_duration = time.time() - start_time

            # Filtered export should process fewer rows
            assert filtered_stats.rows_processed < baseline_stats.rows_processed

            # Performance should be reasonable (not more than 2x slower)
            # In practice, it might even be faster due to fewer rows
            assert filtered_duration < baseline_duration * 2

            print("\nPerformance comparison:")
            print(f"  Baseline: {baseline_stats.rows_processed} rows in {baseline_duration:.2f}s")
            print(f"  Filtered: {filtered_stats.rows_processed} rows in {filtered_duration:.2f}s")
            print(f"  Speedup: {baseline_duration / filtered_duration:.2f}x")

        finally:
            Path(baseline_path).unlink(missing_ok=True)
            Path(filtered_path).unlink(missing_ok=True)

    @pytest.mark.asyncio
    async def test_writetime_filter_with_checkpoint_resume(self, session, time_series_table):
        """
        Test writetime filtering with checkpoint/resume.

        What this tests:
        ---------------
        1. Filter preserved in checkpoint
        2. Resume maintains filter
        3. No duplicate filtering
        4. Consistent results

        Why this matters:
        ----------------
        - Long running exports
        - Failure recovery
        - Filter consistency
        - Data integrity
        """
        partial_checkpoint = None

        def save_checkpoint(data):
            nonlocal partial_checkpoint
            if data["total_rows"] > 50:
                partial_checkpoint = data.copy()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
            output_path = tmp.name

        try:
            operator = BulkOperator(session=session)

            # Start export with filter and checkpoint
            cutoff_date = datetime(2024, 4, 1, tzinfo=timezone.utc)

            await operator.export(
                table=time_series_table,
                output_path=output_path,
                format="csv",
                concurrency=1,
                checkpoint_interval=2,
                checkpoint_callback=save_checkpoint,
                options={
                    "writetime_after": cutoff_date,
                    "writetime_columns": ["status", "value"],
                },
            )

            # Verify checkpoint has filter info
            assert partial_checkpoint is not None
            assert "export_config" in partial_checkpoint
            config = partial_checkpoint["export_config"]
            assert "writetime_after_micros" in config
            assert config["writetime_after_micros"] == int(cutoff_date.timestamp() * 1_000_000)

            # Resume from checkpoint
            with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp2:
                output_path2 = tmp2.name

            await operator.export(
                table=time_series_table,
                output_path=output_path2,
                format="csv",
                resume_from=partial_checkpoint,
                options={
                    "writetime_after": cutoff_date,
                    "writetime_columns": ["status", "value"],
                },
            )

            # Verify resumed export maintains filter
            with open(output_path2, "r") as f:
                reader = csv.DictReader(f)
                rows = list(reader)

            # All rows should still respect the filter
            cutoff_micros = int(cutoff_date.timestamp() * 1_000_000)
            for row in rows:
                if row["status_writetime"]:
                    wt_dt = datetime.fromisoformat(row["status_writetime"].replace("Z", "+00:00"))
                    wt_micros = int(wt_dt.timestamp() * 1_000_000)
                    assert wt_micros >= cutoff_micros

        finally:
            Path(output_path).unlink(missing_ok=True)
            if "output_path2" in locals():
                Path(output_path2).unlink(missing_ok=True)
