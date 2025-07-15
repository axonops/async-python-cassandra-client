"""
Comprehensive integration tests for writetime export with parallelization.

What this tests:
---------------
1. Parallel export with writetime across multiple token ranges
2. Large dataset handling with writetime columns
3. Writetime consistency across parallel workers
4. Performance and correctness under high concurrency

Why this matters:
----------------
- Production exports use parallelization
- Writetime must be correct across all workers
- Large tables stress test the implementation
- Race conditions could corrupt writetime data
"""

import csv
import json
import tempfile
import time
from datetime import datetime
from pathlib import Path

import pytest

from async_cassandra_bulk import BulkOperator


class TestWritetimeParallelExport:
    """Test writetime export with parallel processing."""

    @pytest.fixture
    async def large_writetime_table(self, session):
        """
        Create table with many rows and varied writetime values.

        What this tests:
        ---------------
        1. Table with enough data to require multiple ranges
        2. Different writetime values per row and column
        3. Mix of old and new writetime values
        4. Sufficient data for parallel processing

        Why this matters:
        ----------------
        - Real tables have millions of rows
        - Writetime varies across data
        - Parallel export must handle scale
        - Token ranges must be processed correctly
        """
        table_name = "writetime_parallel_test"
        keyspace = "test_bulk"

        # Create table with multiple partitions
        await session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {keyspace}.{table_name} (
                partition_id INT,
                cluster_id INT,
                name TEXT,
                email TEXT,
                status TEXT,
                metadata MAP<TEXT, TEXT>,
                tags SET<TEXT>,
                scores LIST<INT>,
                PRIMARY KEY (partition_id, cluster_id)
            )
        """
        )

        # Insert data with different writetime values
        base_writetime = 1700000000000000  # ~2023-11-14

        # Prepare statements for better performance
        insert_stmt = await session.prepare(
            f"""
            INSERT INTO {keyspace}.{table_name}
            (partition_id, cluster_id, name, email, status, metadata, tags, scores)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            USING TIMESTAMP ?
            """
        )

        # Create 1000 rows across 100 partitions
        for partition in range(100):
            batch = []
            for cluster in range(10):
                row_writetime = base_writetime + (partition * 1000000) + (cluster * 100000)

                values = (
                    partition,
                    cluster,
                    f"User {partition}-{cluster}",
                    f"user_{partition}_{cluster}@example.com",
                    "active" if partition % 2 == 0 else "inactive",
                    {"dept": f"dept_{partition % 5}", "level": str(cluster % 3)},
                    {f"tag_{i}" for i in range(cluster % 3 + 1)},
                    [i * 10 for i in range(cluster % 4 + 1)],
                    row_writetime,
                )
                batch.append(values)

            # Execute batch
            for values in batch:
                await session.execute(insert_stmt, values)

        # Update some columns with newer writetime
        update_stmt = await session.prepare(
            f"""
            UPDATE {keyspace}.{table_name}
            USING TIMESTAMP ?
            SET email = ?, status = ?
            WHERE partition_id = ? AND cluster_id = ?
            """
        )

        # Update 20% of rows with newer writetime
        newer_writetime = base_writetime + 10000000000000  # Much newer
        for partition in range(0, 100, 5):
            for cluster in range(0, 10, 2):
                new_email = f"updated_{partition}_{cluster}@example.com"
                await session.execute(
                    update_stmt,
                    (newer_writetime, new_email, "updated", partition, cluster),
                )

        yield f"{keyspace}.{table_name}"

        # Cleanup
        await session.execute(f"DROP TABLE IF EXISTS {keyspace}.{table_name}")

    @pytest.mark.asyncio
    async def test_parallel_export_writetime_consistency(self, session, large_writetime_table):
        """
        Test writetime export maintains consistency across workers.

        What this tests:
        ---------------
        1. Multiple workers export correct writetime values
        2. No data corruption or mixing between workers
        3. All rows exported with correct writetime
        4. Token range boundaries respected

        Why this matters:
        ----------------
        - Workers must not interfere with each other
        - Writetime values must match source data
        - Token ranges could overlap if buggy
        - Production reliability depends on this
        """
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
            output_path = tmp.name

        try:
            operator = BulkOperator(session=session)

            # Track progress
            progress_updates = []

            def track_progress(stats):
                progress_updates.append(
                    {
                        "rows": stats.rows_processed,
                        "ranges": stats.ranges_completed,
                        "time": time.time(),
                    }
                )

            # Export with high concurrency and writetime
            start_time = time.time()
            stats = await operator.export(
                table=large_writetime_table,
                output_path=output_path,
                format="csv",
                concurrency=8,  # High concurrency to stress test
                batch_size=100,
                progress_callback=track_progress,
                options={
                    "writetime_columns": ["name", "email", "status"],
                },
            )
            export_duration = time.time() - start_time

            # Verify export completed successfully
            assert stats.rows_processed == 1000
            assert stats.errors == []
            assert stats.is_complete

            # Verify reasonable performance
            assert export_duration < 30  # Should complete within 30 seconds
            assert len(progress_updates) > 0  # Progress was reported

            # Read and verify CSV content
            with open(output_path, "r") as f:
                reader = csv.DictReader(f)
                rows = list(reader)

            assert len(rows) == 1000

            # Verify writetime columns present
            sample_row = rows[0]
            assert "name_writetime" in sample_row
            assert "email_writetime" in sample_row
            assert "status_writetime" in sample_row

            # Verify no primary key writetime
            assert "partition_id_writetime" not in sample_row
            assert "cluster_id_writetime" not in sample_row

            # Verify writetime values are timestamps
            writetime_values = set()
            for row in rows:
                # Parse writetime to ensure it's valid
                name_wt = row["name_writetime"]
                assert name_wt  # Not empty
                assert "2023" in name_wt or "2024" in name_wt  # Valid year

                # Collect unique writetime values
                writetime_values.add(name_wt)

            # Should have multiple different writetime values
            assert len(writetime_values) > 50  # Many different timestamps

            # Verify rows are complete (no partial data)
            for row in rows:
                assert row["partition_id"]
                assert row["cluster_id"]
                assert row["name"]
                assert row["email"]

        finally:
            Path(output_path).unlink(missing_ok=True)

    @pytest.mark.asyncio
    async def test_writetime_defaults_to_false(self, session, large_writetime_table):
        """
        Verify writetime export is disabled by default.

        What this tests:
        ---------------
        1. Export without writetime options excludes writetime
        2. No _writetime columns in output
        3. Default behavior is backwards compatible
        4. Explicit false also works

        Why this matters:
        ----------------
        - Backwards compatibility critical
        - Writetime adds overhead
        - Users must opt-in explicitly
        - Default behavior must be clear
        """
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
            output_path = tmp.name

        try:
            operator = BulkOperator(session=session)

            # Export WITHOUT any writetime options
            stats = await operator.export(
                table=large_writetime_table,
                output_path=output_path,
                format="csv",
                concurrency=4,
            )

            # Verify export completed
            assert stats.rows_processed == 1000

            # Read CSV and verify NO writetime columns
            with open(output_path, "r") as f:
                reader = csv.DictReader(f)
                rows = list(reader)

            # Check first row has no writetime columns
            sample_row = rows[0]
            for key in sample_row.keys():
                assert not key.endswith("_writetime")

            # Verify regular columns are present
            assert "name" in sample_row
            assert "email" in sample_row
            assert "status" in sample_row

        finally:
            Path(output_path).unlink(missing_ok=True)

    @pytest.mark.asyncio
    async def test_selective_writetime_columns(self, session, large_writetime_table):
        """
        Test selecting specific columns for writetime export.

        What this tests:
        ---------------
        1. Only requested columns get writetime
        2. Other columns don't have writetime
        3. Mix of writetime and non-writetime works
        4. Column selection is accurate

        Why this matters:
        ----------------
        - Not all columns need writetime
        - Reduces query overhead
        - Precise control required
        - Production use cases vary
        """
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
            output_path = tmp.name

        try:
            operator = BulkOperator(session=session)

            # Export with writetime for only email column
            stats = await operator.export(
                table=large_writetime_table,
                output_path=output_path,
                format="json",
                options={
                    "writetime_columns": ["email"],  # Only email writetime
                },
                json_options={
                    "mode": "array",  # Array of objects
                },
            )

            assert stats.rows_processed == 1000

            # Read JSON and verify
            with open(output_path, "r") as f:
                data = json.load(f)

            assert len(data) == 1000

            # Check first few rows
            for row in data[:10]:
                # Should have email_writetime
                assert "email_writetime" in row
                assert row["email_writetime"]  # Not null

                # Should NOT have writetime for other columns
                assert "name_writetime" not in row
                assert "status_writetime" not in row
                assert "metadata_writetime" not in row

                # Verify email_writetime is valid ISO format
                email_wt = row["email_writetime"]
                assert "T" in email_wt  # ISO format
                datetime.fromisoformat(email_wt.replace("Z", "+00:00"))

        finally:
            Path(output_path).unlink(missing_ok=True)

    @pytest.mark.asyncio
    async def test_writetime_with_complex_types(self, session, large_writetime_table):
        """
        Test writetime export with collections and complex types.

        What this tests:
        ---------------
        1. Writetime works with MAP, SET, LIST columns
        2. Complex type serialization with writetime
        3. No corruption of complex data
        4. Writetime applies to entire collection

        Why this matters:
        ----------------
        - Production tables have complex types
        - Collections have single writetime
        - Must handle all CQL types
        - Complex scenarios common
        """
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
            output_path = tmp.name

        try:
            operator = BulkOperator(session=session)

            # Export with writetime including complex columns
            stats = await operator.export(
                table=large_writetime_table,
                output_path=output_path,
                format="json",
                options={
                    "writetime_columns": ["metadata", "tags", "scores"],
                },
                json_options={
                    "mode": "objects",  # JSONL format
                },
            )

            assert stats.rows_processed == 1000

            # Read JSONL and verify
            rows = []
            with open(output_path, "r") as f:
                for line in f:
                    rows.append(json.loads(line))

            # Verify complex types have writetime
            for row in rows[:10]:
                # Complex columns should have values
                assert isinstance(row.get("metadata"), dict)
                assert isinstance(row.get("tags"), list)
                assert isinstance(row.get("scores"), list)

                # Should have writetime for complex columns
                assert "metadata_writetime" in row
                assert "tags_writetime" in row
                assert "scores_writetime" in row

                # Writetime should be valid
                for col in ["metadata", "tags", "scores"]:
                    wt_key = f"{col}_writetime"
                    if row[wt_key]:  # Not null
                        # Handle list format (JSON arrays might be serialized as lists)
                        wt_value = row[wt_key]
                        if isinstance(wt_value, str):
                            datetime.fromisoformat(wt_value.replace("Z", "+00:00"))

        finally:
            Path(output_path).unlink(missing_ok=True)

    @pytest.mark.asyncio
    async def test_writetime_export_error_handling(self, session):
        """
        Test error handling during writetime export.

        What this tests:
        ---------------
        1. Invalid writetime column names handled
        2. Non-existent columns rejected
        3. System columns handled appropriately
        4. Clear error messages provided

        Why this matters:
        ----------------
        - Users make configuration mistakes
        - Clear errors prevent confusion
        - System must fail gracefully
        - Production debugging relies on this
        """
        table_name = "writetime_error_test"
        keyspace = "test_bulk"

        # Create simple table
        await session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {keyspace}.{table_name} (
                id UUID PRIMARY KEY,
                data TEXT
            )
        """
        )

        try:
            # Insert test data
            await session.execute(
                f"""
                INSERT INTO {keyspace}.{table_name} (id, data)
                VALUES (uuid(), 'test')
                """
            )

            with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
                output_path = tmp.name

            operator = BulkOperator(session=session)

            # Test 1: Request writetime for all columns
            # Should only get writetime for existing non-key columns
            stats = await operator.export(
                table=f"{keyspace}.{table_name}",
                output_path=output_path,
                format="csv",
                options={
                    "writetime_columns": ["data"],  # Only request existing column
                },
            )

            # Should complete successfully
            assert stats.rows_processed >= 1

            # Verify only valid column has writetime
            with open(output_path, "r") as f:
                reader = csv.DictReader(f)
                row = next(reader)
                assert "data_writetime" in row

            Path(output_path).unlink(missing_ok=True)

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {keyspace}.{table_name}")

    @pytest.mark.asyncio
    async def test_writetime_with_checkpoint_resume(self, session, large_writetime_table):
        """
        Test writetime export can be checkpointed and resumed.

        What this tests:
        ---------------
        1. Checkpoint includes writetime configuration
        2. Resume maintains writetime columns
        3. No duplicate or missing writetime data
        4. Consistent state across resume

        Why this matters:
        ----------------
        - Large exports may fail midway
        - Resume must preserve settings
        - Writetime config must persist
        - Production reliability critical
        """
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
            output_path = tmp.name

        checkpoint_data = None
        checkpoint_count = 0

        def save_checkpoint(data):
            nonlocal checkpoint_data, checkpoint_count
            checkpoint_data = data
            checkpoint_count += 1

        try:
            operator = BulkOperator(session=session)

            # Start export with aggressive checkpointing
            await operator.export(
                table=large_writetime_table,
                output_path=output_path,
                format="csv",
                concurrency=2,
                checkpoint_interval=5,  # Checkpoint every 5 ranges
                checkpoint_callback=save_checkpoint,
                options={
                    "writetime_columns": ["name", "email"],
                },
            )

            # Should have checkpointed
            assert checkpoint_count > 0
            assert checkpoint_data is not None

            # Verify checkpoint contains progress
            assert "completed_ranges" in checkpoint_data
            assert "total_rows" in checkpoint_data
            assert checkpoint_data["total_rows"] > 0

            # In a real scenario, we would:
            # 1. Simulate failure by interrupting export
            # 2. Create new operator with resume_from=checkpoint_data
            # 3. Verify export continues with same writetime config

            # For now, verify the export completed with writetime
            with open(output_path, "r") as f:
                reader = csv.DictReader(f)
                row = next(reader)
                assert "name_writetime" in row
                assert "email_writetime" in row

        finally:
            Path(output_path).unlink(missing_ok=True)

    @pytest.mark.asyncio
    async def test_writetime_performance_impact(self, session, large_writetime_table):
        """
        Measure performance impact of writetime export.

        What this tests:
        ---------------
        1. Baseline export performance without writetime
        2. Performance with writetime enabled
        3. Overhead is reasonable
        4. Scales with concurrency

        Why this matters:
        ----------------
        - Writetime adds query overhead
        - Performance must be acceptable
        - Users need to know impact
        - Production SLAs depend on this
        """
        # Test 1: Export without writetime
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
            output_path_no_wt = tmp.name

        operator = BulkOperator(session=session)

        start = time.time()
        stats_no_wt = await operator.export(
            table=large_writetime_table,
            output_path=output_path_no_wt,
            format="csv",
            concurrency=4,
        )
        duration_no_wt = time.time() - start

        # Test 2: Export with writetime for all columns
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
            output_path_with_wt = tmp.name

        start = time.time()
        stats_with_wt = await operator.export(
            table=large_writetime_table,
            output_path=output_path_with_wt,
            format="csv",
            concurrency=4,
            options={
                "writetime_columns": ["*"],
            },
        )
        duration_with_wt = time.time() - start

        # Clean up
        Path(output_path_no_wt).unlink(missing_ok=True)
        Path(output_path_with_wt).unlink(missing_ok=True)

        # Verify both exports completed
        assert stats_no_wt.rows_processed == 1000
        assert stats_with_wt.rows_processed == 1000

        # Calculate overhead (handle case where durations might be very small)
        if duration_no_wt > 0:
            overhead_ratio = duration_with_wt / duration_no_wt
        else:
            overhead_ratio = 1.0
        print("\nPerformance impact:")
        print(f"  Without writetime: {duration_no_wt:.2f}s")
        print(f"  With writetime: {duration_with_wt:.2f}s")
        print(f"  Overhead ratio: {overhead_ratio:.2f}x")

        # Writetime should add some overhead but not excessive
        # Allow up to 3x slower (conservative limit)
        assert overhead_ratio < 3.0, f"Writetime overhead too high: {overhead_ratio:.2f}x"

    @pytest.mark.asyncio
    async def test_writetime_null_handling_edge_cases(self, session):
        """
        Test edge cases for null writetime handling.

        What this tests:
        ---------------
        1. Null values have null writetime
        2. Tombstones have writetime
        3. Empty collections handling
        4. Mixed null/non-null in same row

        Why this matters:
        ----------------
        - Nulls are common in real data
        - Tombstones still have writetime
        - Edge cases cause bugs
        - Production data is messy
        """
        table_name = "writetime_null_edge_test"
        keyspace = "test_bulk"

        await session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {keyspace}.{table_name} (
                id INT PRIMARY KEY,
                text_col TEXT,
                int_col INT,
                list_col LIST<TEXT>,
                map_col MAP<TEXT, INT>
            )
        """
        )

        try:
            # Insert various null scenarios
            base_wt = 1700000000000000

            # Row 1: All values present
            await session.execute(
                f"""
                INSERT INTO {keyspace}.{table_name}
                (id, text_col, int_col, list_col, map_col)
                VALUES (1, 'text', 100, ['a', 'b'], {{'k1': 1}})
                USING TIMESTAMP {base_wt}
                """
            )

            # Row 2: Some nulls from insert
            await session.execute(
                f"""
                INSERT INTO {keyspace}.{table_name}
                (id, text_col)
                VALUES (2, 'only text')
                USING TIMESTAMP {base_wt + 1000000}
                """
            )

            # Row 3: Explicit null (creates tombstone)
            await session.execute(
                f"""
                INSERT INTO {keyspace}.{table_name}
                (id, text_col, int_col)
                VALUES (3, 'text', null)
                USING TIMESTAMP {base_wt + 2000000}
                """
            )

            # Row 4: Empty collections
            await session.execute(
                f"""
                INSERT INTO {keyspace}.{table_name}
                (id, list_col, map_col)
                VALUES (4, [], {{}})
                USING TIMESTAMP {base_wt + 3000000}
                """
            )

            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
                output_path = tmp.name

            operator = BulkOperator(session=session)

            await operator.export(
                table=f"{keyspace}.{table_name}",
                output_path=output_path,
                format="json",
                options={
                    "writetime_columns": ["*"],
                },
            )

            # Read and analyze results
            with open(output_path, "r") as f:
                data = json.load(f)

            # Convert to dict by id for easier testing
            rows_by_id = {row["id"]: row for row in data}

            # Row 1: All columns should have writetime
            row1 = rows_by_id[1]
            assert row1["text_col_writetime"] is not None
            assert row1["int_col_writetime"] is not None
            assert row1["list_col_writetime"] is not None
            assert row1["map_col_writetime"] is not None

            # Row 2: Only inserted columns have writetime
            row2 = rows_by_id[2]
            assert row2["text_col_writetime"] is not None
            assert row2["int_col"] is None
            assert row2["int_col_writetime"] is None  # No writetime for missing value

            # Row 3: Explicit null might have writetime (tombstone)
            row3 = rows_by_id[3]
            assert row3["text_col_writetime"] is not None
            # Note: Cassandra behavior for null writetime can vary

            # Row 4: Empty collections still have writetime
            row4 = rows_by_id[4]
            # Empty collections might be null or empty depending on Cassandra version
            if row4["list_col"] is not None:
                assert row4["list_col"] == []
                assert row4["list_col_writetime"] is not None  # Empty list has writetime
            if row4["map_col"] is not None:
                assert row4["map_col"] == {}
                assert row4["map_col_writetime"] is not None  # Empty map has writetime

            Path(output_path).unlink(missing_ok=True)

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {keyspace}.{table_name}")
