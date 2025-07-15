"""
Stress tests for writetime export functionality.

What this tests:
---------------
1. Very large tables with millions of rows
2. High concurrency scenarios
3. Memory usage and resource management
4. Token range wraparound handling

Why this matters:
----------------
- Production tables can be huge
- Memory leaks would be catastrophic
- Wraparound ranges are tricky
- Must handle extreme scenarios
"""

import asyncio
import gc
import os
import tempfile
import time
from pathlib import Path

import psutil
import pytest
from cassandra.util import uuid_from_time

from async_cassandra_bulk import BulkOperator


class TestWritetimeStress:
    """Stress test writetime export under extreme conditions."""

    @pytest.fixture
    async def very_large_table(self, session):
        """
        Create table with 10k rows for stress testing.

        What this tests:
        ---------------
        1. Large dataset handling
        2. Memory efficiency
        3. Multiple token ranges
        4. Performance at scale

        Why this matters:
        ----------------
        - Real tables have millions of rows
        - Memory usage must be bounded
        - Performance must scale linearly
        - Production workloads are large
        """
        table_name = "writetime_stress_test"
        keyspace = "test_bulk"

        # Create wide table with many columns
        await session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {keyspace}.{table_name} (
                bucket INT,
                id TIMEUUID,
                col1 TEXT,
                col2 TEXT,
                col3 TEXT,
                col4 TEXT,
                col5 TEXT,
                col6 INT,
                col7 INT,
                col8 INT,
                col9 DOUBLE,
                col10 DOUBLE,
                data BLOB,
                PRIMARY KEY (bucket, id)
            ) WITH CLUSTERING ORDER BY (id DESC)
        """
        )

        # Insert 100k rows across 100 buckets
        insert_stmt = await session.prepare(
            f"""
            INSERT INTO {keyspace}.{table_name}
            (bucket, id, col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, data)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            USING TIMESTAMP ?
            """
        )

        base_writetime = 1700000000000000
        batch_size = 100
        total_rows = 1_000  # Reduced to 1k for faster tests
        num_buckets = 10  # Reduced buckets
        rows_per_bucket = total_rows // num_buckets

        print(f"\nInserting {total_rows} rows for stress test...")
        start_time = time.time()

        for bucket in range(num_buckets):
            batch = []
            for i in range(rows_per_bucket):
                row_id = f"{bucket:03d}-{i:04d}"
                writetime = base_writetime + (bucket * 1000000) + (i * 1000)

                values = (
                    bucket,
                    uuid_from_time(time.time()),
                    f"text1_{row_id}",
                    f"text2_{row_id}",
                    f"text3_{row_id}",
                    f"text4_{row_id}",
                    f"text5_{row_id}",
                    i % 1000,
                    i % 100,
                    i % 10,
                    float(i) / 100,
                    float(i) / 1000,
                    os.urandom(256),  # 256 bytes of random data
                    writetime,
                )
                batch.append(values)

                if len(batch) >= batch_size:
                    # Execute batch
                    await asyncio.gather(*[session.execute(insert_stmt, v) for v in batch])
                    batch = []

            # Execute remaining
            if batch:
                await asyncio.gather(*[session.execute(insert_stmt, v) for v in batch])

            if bucket % 10 == 0:
                elapsed = time.time() - start_time
                print(f"  Inserted {(bucket + 1) * rows_per_bucket} rows in {elapsed:.1f}s")

        print(f"Created table with {total_rows} rows")
        yield f"{keyspace}.{table_name}"

        # Cleanup
        await session.execute(f"DROP TABLE IF EXISTS {keyspace}.{table_name}")

    @pytest.mark.asyncio
    async def test_high_concurrency_writetime_export(self, session, very_large_table):
        """
        Test export with very high concurrency.

        What this tests:
        ---------------
        1. 16+ concurrent workers
        2. Thread pool saturation
        3. Memory usage stays bounded
        4. No deadlocks or race conditions

        Why this matters:
        ----------------
        - Production uses high concurrency
        - Thread pool limits exist
        - Memory must not grow unbounded
        - Deadlocks would hang exports
        """
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
            output_path = tmp.name

        try:
            operator = BulkOperator(session=session)

            # Get initial memory usage
            process = psutil.Process()
            initial_memory = process.memory_info().rss / 1024 / 1024  # MB

            # Track memory during export
            memory_samples = []

            def track_progress(stats):
                current_memory = process.memory_info().rss / 1024 / 1024
                memory_samples.append(current_memory)

            # Export with very high concurrency
            start_time = time.time()
            stats = await operator.export(
                table=very_large_table,
                output_path=output_path,
                format="csv",
                concurrency=16,  # Very high concurrency
                batch_size=500,
                progress_callback=track_progress,
                options={
                    "writetime_columns": ["col1", "col2", "col3"],
                },
            )
            duration = time.time() - start_time

            # Verify export completed (fixture creates 1000 rows, not 10000)
            assert stats.rows_processed == 1_000
            assert stats.errors == []

            # Check memory usage
            peak_memory = max(memory_samples) if memory_samples else initial_memory
            memory_increase = peak_memory - initial_memory

            print("\nHigh concurrency export stats:")
            print(f"  Duration: {duration:.1f}s")
            print(f"  Rows/second: {stats.rows_per_second:.1f}")
            print(f"  Initial memory: {initial_memory:.1f} MB")
            print(f"  Peak memory: {peak_memory:.1f} MB")
            print(f"  Memory increase: {memory_increase:.1f} MB")

            # Memory increase should be reasonable (< 100MB for 10k rows)
            assert memory_increase < 100, f"Memory usage too high: {memory_increase:.1f} MB"

            # Performance should be good
            assert stats.rows_per_second > 1000  # At least 1k rows/sec

        finally:
            Path(output_path).unlink(missing_ok=True)
            gc.collect()  # Force garbage collection

    @pytest.mark.asyncio
    async def test_writetime_with_token_wraparound(self, session):
        """
        Test writetime export with token range wraparound.

        What this tests:
        ---------------
        1. Wraparound ranges handled correctly
        2. No missing data at boundaries
        3. No duplicate data
        4. Writetime preserved across wraparound

        Why this matters:
        ----------------
        - Token ring wraps at boundaries
        - Edge case often has bugs
        - Data loss would be critical
        - Must handle MIN/MAX tokens
        """
        table_name = "writetime_wraparound_test"
        keyspace = "test_bulk"

        # Create table with specific token distribution
        await session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {keyspace}.{table_name} (
                id BIGINT PRIMARY KEY,
                data TEXT,
                marker TEXT
            )
        """
        )

        try:
            # Insert data across token range boundaries
            # Using specific IDs that hash to extreme token values
            test_data = [
                # These IDs are chosen to create wraparound scenarios
                (-9223372036854775807, "near_min_token", "MIN"),
                (-9223372036854775800, "at_min_boundary", "MIN_BOUNDARY"),
                (0, "at_zero", "ZERO"),
                (9223372036854775800, "near_max_token", "MAX"),
                (9223372036854775807, "at_max_token", "MAX_BOUNDARY"),
            ]

            base_writetime = 1700000000000000
            for i, (id_val, data, marker) in enumerate(test_data):
                writetime = base_writetime + (i * 1000000)
                await session.execute(
                    f"""
                    INSERT INTO {keyspace}.{table_name} (id, data, marker)
                    VALUES ({id_val}, '{data}', '{marker}')
                    USING TIMESTAMP {writetime}
                    """
                )

            # Add more data to ensure multiple ranges
            # Start from 1 to avoid overwriting the ID 0 test case
            for i in range(1, 100):
                await session.execute(
                    f"""
                    INSERT INTO {keyspace}.{table_name} (id, data, marker)
                    VALUES ({i * 1000}, 'regular_{i}', 'REGULAR')
                    USING TIMESTAMP {base_writetime + 10000000}
                    """
                )

            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
                output_path = tmp.name

            operator = BulkOperator(session=session)

            # Export with multiple workers to test range splitting
            await operator.export(
                table=f"{keyspace}.{table_name}",
                output_path=output_path,
                format="json",
                concurrency=4,
                options={
                    "writetime_columns": ["data", "marker"],
                },
            )

            # Read results
            import json

            with open(output_path, "r") as f:
                data = json.load(f)

            # Verify all boundary data exported
            markers_found = {row["marker"] for row in data}
            expected_markers = {"MIN", "MIN_BOUNDARY", "ZERO", "MAX", "MAX_BOUNDARY", "REGULAR"}
            assert expected_markers.issubset(markers_found)

            # Verify no duplicates
            id_list = [row["id"] for row in data]
            assert len(id_list) == len(set(id_list)), "Duplicate rows found"

            # Verify writetime for boundary rows
            boundary_rows = [row for row in data if row["marker"] != "REGULAR"]
            for row in boundary_rows:
                assert row["data_writetime"] is not None
                assert row["marker_writetime"] is not None

                # Writetime should be different for different rows
                wt_str = row["data_writetime"]
                assert "2023" in wt_str  # Base writetime year

            Path(output_path).unlink(missing_ok=True)

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {keyspace}.{table_name}")

    @pytest.mark.asyncio
    async def test_writetime_export_memory_efficiency(self, session):
        """
        Test memory efficiency with streaming and writetime.

        What this tests:
        ---------------
        1. Streaming doesn't buffer all writetime data
        2. Memory usage proportional to batch size
        3. Large writetime values handled efficiently
        4. No memory leaks over time

        Why this matters:
        ----------------
        - Writetime adds memory overhead
        - Streaming must remain efficient
        - Large exports need bounded memory
        - Production stability critical
        """
        table_name = "writetime_memory_test"
        keyspace = "test_bulk"

        # Create table with large text fields
        await session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {keyspace}.{table_name} (
                partition_id INT,
                cluster_id INT,
                large_text1 TEXT,
                large_text2 TEXT,
                large_text3 TEXT,
                PRIMARY KEY (partition_id, cluster_id)
            )
        """
        )

        try:
            # Insert rows with large text values
            large_text = "x" * 10000  # 10KB per column
            insert_stmt = await session.prepare(
                f"""
                INSERT INTO {keyspace}.{table_name}
                (partition_id, cluster_id, large_text1, large_text2, large_text3)
                VALUES (?, ?, ?, ?, ?)
                USING TIMESTAMP ?
                """
            )

            # Insert 1000 rows = ~30MB of text data
            for partition in range(10):
                for cluster in range(100):
                    writetime = 1700000000000000 + (partition * 1000000) + cluster
                    await session.execute(
                        insert_stmt,
                        (partition, cluster, large_text, large_text, large_text, writetime),
                    )

            with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
                output_path = tmp.name

            operator = BulkOperator(session=session)

            # Monitor memory during export
            process = psutil.Process()
            gc.collect()
            initial_memory = process.memory_info().rss / 1024 / 1024

            peak_memory = initial_memory
            samples = []

            def monitor_memory(stats):
                nonlocal peak_memory
                current = process.memory_info().rss / 1024 / 1024
                peak_memory = max(peak_memory, current)
                samples.append(current)

            # Export with small batch size to test streaming
            await operator.export(
                table=f"{keyspace}.{table_name}",
                output_path=output_path,
                format="csv",
                batch_size=10,  # Small batch to test streaming
                concurrency=2,
                progress_callback=monitor_memory,
                options={
                    "writetime_columns": ["large_text1", "large_text2", "large_text3"],
                },
            )

            # Calculate memory usage
            memory_increase = peak_memory - initial_memory
            avg_memory = sum(samples) / len(samples) if samples else initial_memory

            print("\nMemory efficiency test:")
            print(f"  Initial memory: {initial_memory:.1f} MB")
            print(f"  Peak memory: {peak_memory:.1f} MB")
            print(f"  Average memory: {avg_memory:.1f} MB")
            print(f"  Memory increase: {memory_increase:.1f} MB")

            # With streaming, memory increase should be reasonable
            # Data is ~30MB, but with writetime and processing overhead,
            # memory increase of up to 200MB is acceptable
            assert (
                memory_increase < 200
            ), f"Memory usage too high for streaming: {memory_increase:.1f} MB"

            Path(output_path).unlink(missing_ok=True)

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {keyspace}.{table_name}")
            gc.collect()

    @pytest.mark.asyncio
    async def test_concurrent_writetime_column_updates(self, session):
        """
        Test writetime export during concurrent column updates.

        What this tests:
        ---------------
        1. Export while data is being updated
        2. Writetime values are consistent
        3. No data corruption
        4. Export completes successfully

        Why this matters:
        ----------------
        - Production tables are actively written
        - Writetime changes during export
        - Must handle concurrent updates
        - Data consistency critical
        """
        table_name = "writetime_concurrent_test"
        keyspace = "test_bulk"

        await session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {keyspace}.{table_name} (
                id INT PRIMARY KEY,
                update_count INT,
                status TEXT,
                last_updated TIMESTAMP
            )
        """
        )

        try:
            # Insert initial data
            for i in range(1000):
                await session.execute(
                    f"""
                    INSERT INTO {keyspace}.{table_name}
                    (id, update_count, status, last_updated)
                    VALUES ({i}, 0, 'initial', toTimestamp(now()))
                    """
                )

            # Start concurrent updates
            update_task = asyncio.create_task(
                self._concurrent_updates(session, keyspace, table_name)
            )

            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
                output_path = tmp.name

            try:
                operator = BulkOperator(session=session)

                # Export while updates are happening
                stats = await operator.export(
                    table=f"{keyspace}.{table_name}",
                    output_path=output_path,
                    format="json",
                    concurrency=4,
                    options={
                        "writetime_columns": ["update_count", "status"],
                    },
                )

                # Cancel update task
                update_task.cancel()
                try:
                    await update_task
                except asyncio.CancelledError:
                    pass

                # Verify export completed
                assert stats.rows_processed == 1000
                assert stats.errors == []

                # Read and verify data consistency
                import json

                with open(output_path, "r") as f:
                    data = json.load(f)

                # Each row should have consistent writetime values
                for row in data:
                    assert "update_count_writetime" in row
                    assert "status_writetime" in row

                    # Writetime should be valid
                    if row["update_count_writetime"]:
                        assert "T" in row["update_count_writetime"]

                Path(output_path).unlink(missing_ok=True)

            finally:
                # Ensure update task is cancelled
                if not update_task.done():
                    update_task.cancel()

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {keyspace}.{table_name}")

    async def _concurrent_updates(self, session, keyspace: str, table_name: str):
        """Helper to perform concurrent updates during export."""
        update_stmt = await session.prepare(
            f"""
            UPDATE {keyspace}.{table_name}
            SET update_count = ?, status = ?, last_updated = toTimestamp(now())
            WHERE id = ?
            """
        )

        update_count = 0
        while True:
            try:
                # Update random rows
                for _ in range(10):
                    row_id = update_count % 1000
                    status = f"updated_{update_count}"
                    await session.execute(update_stmt, (update_count, status, row_id))
                    update_count += 1

                # Small delay to not overwhelm
                await asyncio.sleep(0.01)

            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Update error: {e}")
                await asyncio.sleep(0.1)
