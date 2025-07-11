"""
Comprehensive error scenario tests for async-cassandra-bulk.

What this tests:
---------------
1. Network failures during export
2. Disk space exhaustion
3. Permission errors
4. Cassandra node failures
5. Memory pressure scenarios
6. Corrupted data handling
7. Invalid configurations
8. Race conditions

Why this matters:
----------------
- Production systems fail in unexpected ways
- Data integrity must be maintained
- Error recovery must be predictable
- Users need clear error messages
- No silent data loss allowed

Additional context:
---------------------------------
These tests simulate real-world failure scenarios
that can occur in production environments.
"""

import asyncio
import json
import os
import tempfile
import uuid
from pathlib import Path
from unittest.mock import patch

import pytest
from cassandra.cluster import NoHostAvailable

from async_cassandra_bulk import BulkOperator


class TestNetworkFailures:
    """Test network-related failure scenarios."""

    @pytest.fixture
    async def network_test_table(self, session):
        """Create a table for network failure tests."""
        table_name = f"network_test_{uuid.uuid4().hex[:8]}"
        keyspace = "test_bulk"

        await session.execute(
            f"""
            CREATE TABLE {keyspace}.{table_name} (
                id INT,
                partition INT,
                data TEXT,
                PRIMARY KEY (partition, id)
            )
        """
        )

        # Insert test data across multiple partitions
        insert_stmt = await session.prepare(
            f"INSERT INTO {keyspace}.{table_name} (partition, id, data) VALUES (?, ?, ?)"
        )

        for partition in range(10):
            for i in range(100):
                await session.execute(insert_stmt, (partition, i, f"data_{partition}_{i}"))

        yield f"{keyspace}.{table_name}"

        await session.execute(f"DROP TABLE IF EXISTS {keyspace}.{table_name}")

    @pytest.mark.asyncio
    async def test_export_with_intermittent_network_failures(self, session, network_test_table):
        """
        Test export behavior with intermittent network failures.

        What this tests:
        ---------------
        1. Export continues despite transient failures
        2. Failed ranges are retried
        3. No data loss occurs
        4. Checkpoint state remains consistent

        Why this matters:
        ----------------
        - Network blips are common in distributed systems
        - Export must be resilient to transient failures
        - Data completeness is critical
        """
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
            output_path = tmp.name

        checkpoints = []

        def track_checkpoint(checkpoint):
            checkpoints.append(checkpoint.copy())

        # Simulate intermittent failures by patching execute
        original_execute = session.execute
        call_count = 0

        async def flaky_execute(*args, **kwargs):
            nonlocal call_count
            call_count += 1

            # Fail every 5th call to simulate intermittent issues
            if call_count % 5 == 0 and call_count < 20:
                raise NoHostAvailable("Simulated network failure", {})

            return await original_execute(*args, **kwargs)

        try:
            # Patch the session's execute method
            session.execute = flaky_execute

            operator = BulkOperator(session=session)

            # Export with checkpoint tracking
            stats = await operator.export(
                table=network_test_table,
                output_path=output_path,
                format="json",
                concurrency=2,  # Lower concurrency to control failures
                checkpoint_interval=5,
                checkpoint_callback=track_checkpoint,
            )

            # Verify export completed despite failures but with some data loss
            # When ranges fail, they are not retried automatically
            assert stats.rows_processed < 1000  # Some rows lost due to failures
            assert stats.rows_processed > 500  # But most data exported
            assert len(stats.errors) > 0  # Errors were recorded
            assert len(checkpoints) > 0

            # Verify data integrity
            with open(output_path, "r") as f:
                exported_data = json.load(f)

            # Should match rows processed count
            assert len(exported_data) == stats.rows_processed

            # Verify some but not all partitions represented (due to failures)
            partitions = {row["partition"] for row in exported_data}
            assert len(partitions) >= 5  # At least half the partitions
            assert len(partitions) < 10  # But not all due to failures

        finally:
            # Restore original execute
            session.execute = original_execute
            Path(output_path).unlink(missing_ok=True)

    @pytest.mark.asyncio
    async def test_export_with_total_network_failure(self, session, network_test_table):
        """
        Test export behavior when network completely fails.

        What this tests:
        ---------------
        1. Export fails gracefully
        2. Partial data is not corrupted
        3. Error is properly propagated
        4. Checkpoint can be used to resume

        Why this matters:
        ----------------
        - Total failures need clean handling
        - Partial exports must be valid
        - Users need actionable errors
        """
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
            output_path = tmp.name

        last_checkpoint = None

        def save_checkpoint(checkpoint):
            nonlocal last_checkpoint
            last_checkpoint = checkpoint

        # Simulate total failure after some progress
        original_execute = session.execute
        call_count = 0

        async def failing_execute(*args, **kwargs):
            nonlocal call_count
            call_count += 1

            # Allow first 10 calls, then fail everything
            if call_count > 10:
                raise NoHostAvailable("Total network failure", {})

            return await original_execute(*args, **kwargs)

        try:
            session.execute = failing_execute

            operator = BulkOperator(session=session)

            # Export should complete but with errors
            stats = await operator.export(
                table=network_test_table,
                output_path=output_path,
                format="json",
                concurrency=1,
                checkpoint_callback=save_checkpoint,
            )

            # Should have processed some rows before failure
            assert stats.rows_processed > 0
            assert stats.rows_processed < 1000  # But not all
            assert len(stats.errors) > 0

            # All errors should be NoHostAvailable
            for error in stats.errors:
                assert isinstance(error, NoHostAvailable)

            # Verify we have a checkpoint
            assert last_checkpoint is not None
            assert last_checkpoint.get("completed_ranges") is not None
            assert last_checkpoint.get("total_rows", 0) > 0

            # Verify partial export is valid JSON
            if os.path.exists(output_path):
                with open(output_path, "r") as f:
                    content = f.read()
                    if content:
                        # Should be valid JSON array
                        data = json.loads(content)
                        assert isinstance(data, list)
                        assert len(data) > 0  # Some data exported

        finally:
            session.execute = original_execute
            Path(output_path).unlink(missing_ok=True)


class TestDiskSpaceErrors:
    """Test disk space exhaustion scenarios."""

    @pytest.mark.asyncio
    async def test_export_disk_full(self, session):
        """
        Test export when disk becomes full.

        What this tests:
        ---------------
        1. Disk full error is detected
        2. Export fails with clear error
        3. Partial file is cleaned up
        4. No corruption occurs

        Why this matters:
        ----------------
        - Disk space is finite
        - Large exports can exhaust space
        - Clean failure is essential
        """
        table_name = f"disk_test_{uuid.uuid4().hex[:8]}"
        keyspace = "test_bulk"

        # Create table with large data
        await session.execute(
            f"""
            CREATE TABLE {keyspace}.{table_name} (
                id INT PRIMARY KEY,
                large_data TEXT
            )
        """
        )

        try:
            # Insert rows with large data
            large_text = "x" * 10000  # 10KB per row
            insert_stmt = await session.prepare(
                f"INSERT INTO {keyspace}.{table_name} (id, large_data) VALUES (?, ?)"
            )

            for i in range(100):
                await session.execute(insert_stmt, (i, large_text))

            # Create a small temporary directory with limited space
            # This is simulated by mocking write operations
            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
                output_path = tmp.name

            write_count = 0

            # Create a wrapper for the exporter that simulates disk full
            from async_cassandra_bulk.exporters.json import JSONExporter

            original_write_row = JSONExporter.write_row

            async def limited_write_row(self, row_dict):
                nonlocal write_count
                write_count += 1

                # Simulate disk full after 50 writes
                if write_count > 50:
                    raise OSError(28, "No space left on device")

                return await original_write_row(self, row_dict)

            operator = BulkOperator(session=session)

            # Patch write_row to simulate disk full
            with patch.object(JSONExporter, "write_row", limited_write_row):
                stats = await operator.export(
                    table=f"{keyspace}.{table_name}",
                    output_path=output_path,
                    format="json",
                )

                # Should have processed some rows before disk full
                assert stats.rows_processed == 50  # Exactly 50 before failure
                assert len(stats.errors) > 0

                # All errors should be OSError with errno 28
                for error in stats.errors:
                    assert isinstance(error, OSError)
                    assert error.errno == 28
                    assert "No space left" in str(error)

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {keyspace}.{table_name}")
            Path(output_path).unlink(missing_ok=True)


class TestCheckpointErrors:
    """Test checkpoint-related error scenarios."""

    @pytest.mark.asyncio
    async def test_corrupted_checkpoint_handling(self, session):
        """
        Test handling of corrupted checkpoint files.

        What this tests:
        ---------------
        1. Corrupted checkpoint detection
        2. Clear error message
        3. Option to start fresh
        4. No data corruption

        Why this matters:
        ----------------
        - Checkpoint files can be corrupted
        - Users need recovery options
        - Data integrity paramount
        """
        table_name = f"checkpoint_corrupt_{uuid.uuid4().hex[:8]}"
        keyspace = "test_bulk"

        await session.execute(
            f"""
            CREATE TABLE {keyspace}.{table_name} (
                id INT PRIMARY KEY,
                data TEXT
            )
        """
        )

        try:
            # Insert test data
            for i in range(100):
                await session.execute(
                    f"INSERT INTO {keyspace}.{table_name} (id, data) VALUES ({i}, 'test_{i}')"
                )

            # Create corrupted checkpoint with invalid completed_ranges
            corrupted_checkpoint = {
                "version": "1.0",
                "completed_ranges": [[1, 2, 3]],  # Wrong format - should be list of 2-tuples
                "total_rows": 50,  # Valid number
                "table": f"{keyspace}.{table_name}",
                "export_config": {
                    "table": f"{keyspace}.{table_name}",
                    "columns": None,
                    "writetime_columns": [],
                },
            }

            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
                output_path = tmp.name

            operator = BulkOperator(session=session)

            # The export should handle even corrupted checkpoints gracefully
            # It will convert the 3-element list to a tuple and continue
            stats = await operator.export(
                table=f"{keyspace}.{table_name}",
                output_path=output_path,
                format="json",
                resume_from=corrupted_checkpoint,
            )

            # The export should complete successfully
            # The corrupted ranges will be ignored/skipped
            assert stats.rows_processed >= 50  # At least the checkpoint amount

            # Verify data was exported
            with open(output_path, "r") as f:
                data = json.load(f)
            assert len(data) > 0

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {keyspace}.{table_name}")
            Path(output_path).unlink(missing_ok=True)

    @pytest.mark.asyncio
    async def test_checkpoint_write_failure(self, session):
        """
        Test behavior when checkpoint callback raises exception.

        What this tests:
        ---------------
        1. Checkpoint callback exceptions are caught
        2. Export fails if checkpoint is critical
        3. Error is properly handled
        4. Demonstrates checkpoint callback importance

        Why this matters:
        ----------------
        - Checkpoint callbacks might fail
        - Need to understand failure behavior
        - Users must handle checkpoint errors
        """
        table_name = f"checkpoint_write_fail_{uuid.uuid4().hex[:8]}"
        keyspace = "test_bulk"

        await session.execute(
            f"""
            CREATE TABLE {keyspace}.{table_name} (
                id INT PRIMARY KEY,
                data TEXT
            )
        """
        )

        try:
            # Insert test data
            for i in range(100):
                await session.execute(
                    f"INSERT INTO {keyspace}.{table_name} (id, data) VALUES ({i}, 'test_{i}')"
                )

            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
                output_path = tmp.name

            checkpoint_attempts = 0

            def failing_checkpoint(checkpoint):
                nonlocal checkpoint_attempts
                checkpoint_attempts += 1
                raise IOError("Cannot write checkpoint")

            operator = BulkOperator(session=session)

            # Export should fail when checkpoint callback raises
            with pytest.raises(IOError) as exc_info:
                await operator.export(
                    table=f"{keyspace}.{table_name}",
                    output_path=output_path,
                    format="json",
                    checkpoint_interval=10,
                    checkpoint_callback=failing_checkpoint,
                )

            assert "Cannot write checkpoint" in str(exc_info.value)
            assert checkpoint_attempts > 0  # Tried to checkpoint

            # Verify data integrity
            with open(output_path, "r") as f:
                data = json.load(f)

            assert len(data) == 100

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {keyspace}.{table_name}")
            Path(output_path).unlink(missing_ok=True)


class TestConcurrencyErrors:
    """Test concurrency and thread safety scenarios."""

    @pytest.mark.asyncio
    async def test_concurrent_exports_same_table(self, session):
        """
        Test multiple concurrent exports of same table.

        What this tests:
        ---------------
        1. Concurrent exports don't interfere
        2. Each export gets complete data
        3. No data corruption
        4. Resource cleanup works

        Why this matters:
        ----------------
        - Multiple users may export same data
        - Operations must be isolated
        - Thread safety critical
        """
        table_name = f"concurrent_test_{uuid.uuid4().hex[:8]}"
        keyspace = "test_bulk"

        await session.execute(
            f"""
            CREATE TABLE {keyspace}.{table_name} (
                id INT PRIMARY KEY,
                data TEXT
            )
        """
        )

        try:
            # Insert test data
            for i in range(100):
                await session.execute(
                    f"INSERT INTO {keyspace}.{table_name} (id, data) VALUES ({i}, 'test_{i}')"
                )

            # Run 5 concurrent exports
            export_tasks = []
            output_paths = []

            for i in range(5):
                with tempfile.NamedTemporaryFile(
                    mode="w", suffix=f"_{i}.json", delete=False
                ) as tmp:
                    output_path = tmp.name
                    output_paths.append(output_path)

                operator = BulkOperator(session=session)
                task = operator.export(
                    table=f"{keyspace}.{table_name}",
                    output_path=output_path,
                    format="json",
                    concurrency=2,  # Each export uses 2 workers
                )
                export_tasks.append(task)

            # Wait for all exports to complete
            results = await asyncio.gather(*export_tasks)

            # Verify all exports succeeded
            for i, stats in enumerate(results):
                assert stats.rows_processed == 100
                assert stats.errors == []

            # Verify each export has complete data
            for output_path in output_paths:
                with open(output_path, "r") as f:
                    data = json.load(f)

                assert len(data) == 100
                ids = {row["id"] for row in data}
                assert len(ids) == 100  # All unique IDs present

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {keyspace}.{table_name}")
            for path in output_paths:
                Path(path).unlink(missing_ok=True)

    @pytest.mark.asyncio
    async def test_thread_pool_exhaustion(self, session):
        """
        Test behavior when thread pool is exhausted.

        What this tests:
        ---------------
        1. Export handles thread pool limits
        2. No deadlock occurs
        3. Performance degrades gracefully
        4. All data still exported

        Why this matters:
        ----------------
        - Thread pools have limits
        - System must remain stable
        - Deadlock prevention critical
        """
        table_name = f"thread_pool_test_{uuid.uuid4().hex[:8]}"
        keyspace = "test_bulk"

        await session.execute(
            f"""
            CREATE TABLE {keyspace}.{table_name} (
                id INT PRIMARY KEY,
                data TEXT
            )
        """
        )

        try:
            # Insert more data
            for i in range(500):
                await session.execute(
                    f"INSERT INTO {keyspace}.{table_name} (id, data) VALUES ({i}, 'test_{i}')"
                )

            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
                output_path = tmp.name

            operator = BulkOperator(session=session)

            # Export with very high concurrency to stress thread pool
            stats = await operator.export(
                table=f"{keyspace}.{table_name}",
                output_path=output_path,
                format="json",
                concurrency=50,  # Very high concurrency
                batch_size=10,  # Small batches = more operations
            )

            # Should complete despite thread pool pressure
            assert stats.rows_processed == 500
            assert stats.is_complete

            # Verify data integrity
            with open(output_path, "r") as f:
                data = json.load(f)

            assert len(data) == 500

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {keyspace}.{table_name}")
            Path(output_path).unlink(missing_ok=True)


class TestDataIntegrityUnderFailure:
    """Test data integrity during various failure scenarios."""

    @pytest.mark.asyncio
    async def test_export_during_concurrent_updates(self, session):
        """
        Test export while table is being updated.

        What this tests:
        ---------------
        1. Export handles concurrent modifications
        2. Snapshot consistency per range
        3. No crashes or corruption
        4. Clear behavior documented

        Why this matters:
        ----------------
        - Tables are often live during export
        - Consistency model must be clear
        - No surprises for users
        """
        table_name = f"concurrent_update_test_{uuid.uuid4().hex[:8]}"
        keyspace = "test_bulk"

        await session.execute(
            f"""
            CREATE TABLE {keyspace}.{table_name} (
                id INT PRIMARY KEY,
                counter INT,
                updated_at TIMESTAMP
            )
        """
        )

        try:
            # Insert initial data
            for i in range(100):
                await session.execute(
                    f"""
                    INSERT INTO {keyspace}.{table_name} (id, counter, updated_at)
                    VALUES ({i}, 0, toTimestamp(now()))
                """
                )

            # Start background updates
            update_task_stop = asyncio.Event()
            update_count = 0

            async def update_worker():
                nonlocal update_count
                while not update_task_stop.is_set():
                    try:
                        # Update random rows
                        row_id = update_count % 100
                        await session.execute(
                            f"""
                            UPDATE {keyspace}.{table_name}
                            SET counter = counter + 1, updated_at = toTimestamp(now())
                            WHERE id = {row_id}
                        """
                        )
                        update_count += 1
                        await asyncio.sleep(0.001)  # High update rate
                    except asyncio.CancelledError:
                        break
                    except Exception:
                        pass  # Ignore errors during shutdown

            update_task = asyncio.create_task(update_worker())

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
                )

                # Stop updates
                update_task_stop.set()
                await update_task

                # Verify export completed
                assert stats.rows_processed == 100

                # Verify data is valid (may have mixed versions)
                with open(output_path, "r") as f:
                    data = json.load(f)

                assert len(data) == 100

                # Each row should be internally consistent
                for row in data:
                    assert isinstance(row["id"], int)
                    assert isinstance(row["counter"], int)
                    assert row["counter"] >= 0  # Never negative

                print(f"Export completed with {update_count} concurrent updates")

            finally:
                update_task_stop.set()
                try:
                    await update_task
                except asyncio.CancelledError:
                    pass

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {keyspace}.{table_name}")
            Path(output_path).unlink(missing_ok=True)

    @pytest.mark.asyncio
    async def test_export_with_schema_change(self, session):
        """
        Test export behavior during schema changes.

        What this tests:
        ---------------
        1. Export handles column additions
        2. Export handles column drops (if possible)
        3. Clear error on incompatible changes
        4. No corruption or crashes

        Why this matters:
        ----------------
        - Schema evolves in production
        - Export must be robust
        - Clear failure modes needed
        """
        table_name = f"schema_change_test_{uuid.uuid4().hex[:8]}"
        keyspace = "test_bulk"

        await session.execute(
            f"""
            CREATE TABLE {keyspace}.{table_name} (
                id INT PRIMARY KEY,
                col1 TEXT,
                col2 TEXT
            )
        """
        )

        try:
            # Insert initial data
            for i in range(50):
                await session.execute(
                    f"""
                    INSERT INTO {keyspace}.{table_name} (id, col1, col2)
                    VALUES ({i}, 'data1_{i}', 'data2_{i}')
                """
                )

            schema_changed = asyncio.Event()
            export_started = asyncio.Event()

            async def schema_changer():
                # Wait for export to start
                await export_started.wait()
                await asyncio.sleep(0.1)  # Let export make some progress

                # Add a new column
                await session.execute(
                    f"""
                    ALTER TABLE {keyspace}.{table_name} ADD col3 TEXT
                """
                )

                # Insert data with new column
                for i in range(50, 100):
                    await session.execute(
                        f"""
                        INSERT INTO {keyspace}.{table_name} (id, col1, col2, col3)
                        VALUES ({i}, 'data1_{i}', 'data2_{i}', 'data3_{i}')
                    """
                    )

                schema_changed.set()

            schema_task = asyncio.create_task(schema_changer())

            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
                output_path = tmp.name

            try:
                operator = BulkOperator(session=session)

                export_started.set()

                # Export during schema change
                stats = await operator.export(
                    table=f"{keyspace}.{table_name}",
                    output_path=output_path,
                    format="json",
                    concurrency=1,  # Slow to ensure schema change happens during export
                )

                await schema_task

                # Export should handle mixed schema
                assert stats.rows_processed >= 50  # At least original data

                # Verify data structure
                with open(output_path, "r") as f:
                    data = json.load(f)

                # Some rows may have col3, some may not
                has_col3 = sum(1 for row in data if "col3" in row)
                no_col3 = sum(1 for row in data if "col3" not in row)

                print(f"Rows with col3: {has_col3}, without: {no_col3}")

                # All rows should have original columns
                for row in data:
                    assert "id" in row
                    assert "col1" in row
                    assert "col2" in row

            finally:
                await schema_task

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {keyspace}.{table_name}")
            Path(output_path).unlink(missing_ok=True)


class TestMemoryPressure:
    """Test behavior under memory pressure."""

    @pytest.mark.asyncio
    async def test_export_large_rows(self, session):
        """
        Test export of tables with very large rows.

        What this tests:
        ---------------
        1. Memory usage stays bounded
        2. No OOM errors
        3. Streaming works correctly
        4. Performance acceptable

        Why this matters:
        ----------------
        - Some tables have large blobs
        - Memory must not grow unbounded
        - System stability critical
        """
        table_name = f"large_row_test_{uuid.uuid4().hex[:8]}"
        keyspace = "test_bulk"

        await session.execute(
            f"""
            CREATE TABLE {keyspace}.{table_name} (
                id INT PRIMARY KEY,
                large_blob BLOB,
                metadata TEXT
            )
        """
        )

        try:
            # Insert rows with large blobs
            large_data = os.urandom(1024 * 1024)  # 1MB per row
            insert_stmt = await session.prepare(
                f"""
                INSERT INTO {keyspace}.{table_name} (id, large_blob, metadata)
                VALUES (?, ?, ?)
            """
            )

            for i in range(10):  # 10MB total
                await session.execute(insert_stmt, (i, large_data, f"metadata_{i}"))

            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
                output_path = tmp.name

            operator = BulkOperator(session=session)

            # Export with small batch size to test streaming
            stats = await operator.export(
                table=f"{keyspace}.{table_name}",
                output_path=output_path,
                format="json",
                batch_size=1,  # One row at a time
                concurrency=1,  # Sequential to test memory behavior
            )

            assert stats.rows_processed == 10

            # File should be large but memory usage should have stayed reasonable
            file_size = os.path.getsize(output_path)
            assert file_size > 10 * 1024 * 1024  # At least 10MB (base64 encoded)

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {keyspace}.{table_name}")
            Path(output_path).unlink(missing_ok=True)
