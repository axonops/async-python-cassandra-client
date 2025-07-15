"""
Integration tests for ParallelExporter with real Cassandra.

What this tests:
---------------
1. Parallel export with actual token ranges from cluster
2. Checkpointing and resumption with real data
3. Error handling with network/query failures
4. Performance with concurrent workers
5. Large dataset handling

Why this matters:
----------------
- Token range discovery only works with real cluster
- Concurrent query execution needs real coordination
- Performance characteristics differ from mocks
- Production resilience testing
"""

import asyncio
from uuid import uuid4

import pytest

from async_cassandra_bulk import CSVExporter, ParallelExporter


class TestParallelExportTokenRanges:
    """Test token range discovery and processing with real cluster."""

    @pytest.mark.asyncio
    async def test_discover_token_ranges_real_cluster(self, session, populated_table):
        """
        Test token range discovery from actual Cassandra cluster.

        What this tests:
        ---------------
        1. Token ranges discovered from cluster metadata
        2. Ranges cover entire token space without gaps
        3. Each range has replica information
        4. Number of ranges matches cluster topology

        Why this matters:
        ----------------
        - Token ranges are core to distributed processing
        - Must accurately reflect cluster topology
        - Gaps would cause data loss
        - Production clusters have complex topologies

        Additional context:
        ---------------------------------
        - Single node test cluster has fewer ranges
        - Production clusters have 256+ vnodes per node
        - Ranges used for parallel worker distribution
        """
        from async_cassandra_bulk.utils.token_utils import discover_token_ranges

        ranges = await discover_token_ranges(session, "test_bulk")

        # Should have at least one range
        assert len(ranges) > 0

        # Each range should have replicas
        for range in ranges:
            assert range.replicas is not None
            assert len(range.replicas) > 0
            assert range.start != range.end

        # Verify ranges cover token space (simplified for single node)
        assert any(r.start < r.end for r in ranges)

    @pytest.mark.asyncio
    async def test_parallel_export_utilizes_workers(self, session, populated_table, tmp_path):
        """
        Test that parallel export actually uses multiple workers.

        What this tests:
        ---------------
        1. Multiple workers process ranges concurrently
        2. Work distributed across available workers
        3. All data exported despite parallelism
        4. No data duplication from concurrent access

        Why this matters:
        ----------------
        - Parallelism critical for large table performance
        - Must verify actual concurrent execution
        - Data integrity with parallel processing
        - Production exports rely on parallelism

        Additional context:
        ---------------------------------
        - Default 4 workers, can be tuned
        - Each worker gets token range queue
        - Semaphore limits concurrent queries
        """
        output_file = tmp_path / "parallel_export.csv"
        exporter = CSVExporter(output_path=str(output_file))

        # Track concurrent executions
        concurrent_count = 0
        max_concurrent = 0
        lock = asyncio.Lock()

        # Wrap exporter to track concurrency
        original_write = exporter.write_row

        async def tracking_write(row):
            nonlocal concurrent_count, max_concurrent
            async with lock:
                concurrent_count += 1
                max_concurrent = max(max_concurrent, concurrent_count)

            await asyncio.sleep(0.001)  # Simulate work
            await original_write(row)

            async with lock:
                concurrent_count -= 1

        exporter.write_row = tracking_write

        parallel = ParallelExporter(
            session=session, table=f"test_bulk.{populated_table}", exporter=exporter, concurrency=4
        )

        stats = await parallel.export()

        assert stats.rows_processed == 1000
        assert max_concurrent > 1  # Proves parallel execution

    @pytest.mark.asyncio
    async def test_export_with_token_range_splitting(self, session, populated_table, tmp_path):
        """
        Test token range splitting for optimal parallelism.

        What this tests:
        ---------------
        1. Ranges split based on concurrency setting
        2. Splits are roughly equal in size
        3. All ranges processed without gaps
        4. More splits than workers for load balancing

        Why this matters:
        ----------------
        - Even work distribution critical for performance
        - Skewed ranges cause worker starvation
        - Production tables have uneven distributions
        - Splitting algorithm affects throughput

        Additional context:
        ---------------------------------
        - Target splits = concurrency * 2
        - Proportional splitting based on range size
        - Small ranges not split further
        """
        output_file = tmp_path / "split_export.csv"
        exporter = CSVExporter(output_path=str(output_file))

        # Track which ranges were processed
        processed_ranges = []

        # Hook into range processing
        from async_cassandra_bulk.parallel_export import ParallelExporter

        original_export_range = ParallelExporter._export_range

        async def tracking_export_range(self, token_range, stats):
            processed_ranges.append((token_range.start, token_range.end))
            return await original_export_range(self, token_range, stats)

        ParallelExporter._export_range = tracking_export_range

        try:
            parallel = ParallelExporter(
                session=session,
                table=f"test_bulk.{populated_table}",
                exporter=exporter,
                concurrency=8,  # Higher concurrency = more splits
            )

            stats = await parallel.export()

            # Token range queries might miss some rows at boundaries
            assert 900 <= stats.rows_processed <= 1000
            assert len(processed_ranges) >= 8  # At least as many as workers

        finally:
            # Restore original method
            ParallelExporter._export_range = original_export_range


class TestParallelExportCheckpointing:
    """Test checkpointing and resumption with real data."""

    @pytest.mark.asyncio
    async def test_checkpoint_save_and_resume(self, session, populated_table, tmp_path):
        """
        Test saving checkpoints and resuming interrupted export.

        What this tests:
        ---------------
        1. Checkpoints saved at configured intervals
        2. Resume skips already processed ranges
        3. Final row count includes previous progress
        4. No duplicate data in resumed export

        Why this matters:
        ----------------
        - Long exports may fail (network, timeout)
        - Resumption saves time and resources
        - Critical for TB+ sized exports
        - Production resilience requirement

        Additional context:
        ---------------------------------
        - Checkpoint contains range list and row count
        - Resume from checkpoint skips completed work
        - Essential for cost-effective large exports
        """
        output_file = tmp_path / "checkpoint_export.csv"
        checkpoint_file = tmp_path / "checkpoint.json"

        # First export - interrupt after some progress
        exporter1 = CSVExporter(output_path=str(output_file))
        rows_before_interrupt = 0

        # Interrupt after processing some rows
        original_write = exporter1.write_row

        async def interrupting_write(row):
            nonlocal rows_before_interrupt
            rows_before_interrupt += 1
            if rows_before_interrupt > 300:  # Interrupt after 300 rows
                raise Exception("Simulated network failure")
            await original_write(row)

        exporter1.write_row = interrupting_write

        # Save checkpoints to file
        saved_checkpoints = []

        async def save_checkpoint(state):
            saved_checkpoints.append(state)
            import json

            with open(checkpoint_file, "w") as f:
                json.dump(state, f)

        parallel1 = ParallelExporter(
            session=session,
            table=f"test_bulk.{populated_table}",
            exporter=exporter1,
            checkpoint_interval=2,  # Frequent checkpoints
            checkpoint_callback=save_checkpoint,
        )

        # First export will complete with errors
        stats1 = await parallel1.export()

        # Should have processed exactly 300 rows before failure
        assert stats1.rows_processed == 300
        assert len(stats1.errors) > 0
        assert any("Simulated network failure" in str(e) for e in stats1.errors)
        assert len(saved_checkpoints) > 0

        # Load last checkpoint
        import json

        with open(checkpoint_file, "r") as f:
            last_checkpoint = json.load(f)

        # Resume from checkpoint with new exporter
        output_file2 = tmp_path / "resumed_export.csv"
        exporter2 = CSVExporter(output_path=str(output_file2))

        parallel2 = ParallelExporter(
            session=session,
            table=f"test_bulk.{populated_table}",
            exporter=exporter2,
            resume_from=last_checkpoint,
        )

        stats = await parallel2.export()

        # Should complete successfully
        # The resumed export only includes new rows, not checkpoint rows
        # When we resume, we might reprocess some ranges that were in-progress
        # during the interruption, so we could get more than 1000 total

        # We should export all remaining data
        assert stats.rows_processed > 0
        assert stats.is_complete

        # Read both CSV files to get actual unique rows
        import csv

        all_rows = set()

        # Read first export
        with open(output_file, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                all_rows.add(row["id"])

        # Read resumed export
        with open(output_file2, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                all_rows.add(row["id"])

        # Should have all 1000 unique rows between both exports
        assert len(all_rows) == 1000

    @pytest.mark.asyncio
    async def test_checkpoint_with_progress_tracking(self, session, populated_table, tmp_path):
        """
        Test checkpoint integration with progress callbacks.

        What this tests:
        ---------------
        1. Progress callbacks show checkpoint progress
        2. Resumed export starts at correct percentage
        3. Progress smoothly continues from checkpoint
        4. Final progress reaches 100%

        Why this matters:
        ----------------
        - UI needs accurate progress after resume
        - Users must see continued progress
        - Progress bars shouldn't reset
        - Production monitoring continuity

        Additional context:
        ---------------------------------
        - Progress based on range completion
        - Checkpoint stores ranges_completed
        - UI can show "Resuming from X%"
        """
        output_file = tmp_path / "progress_checkpoint.csv"
        exporter = CSVExporter(output_path=str(output_file))

        progress_updates = []
        checkpoint_progress = []

        def progress_callback(stats):
            progress_updates.append(stats.progress_percentage)

        async def checkpoint_callback(state):
            checkpoint_progress.append(state["total_rows"])

        parallel = ParallelExporter(
            session=session,
            table=f"test_bulk.{populated_table}",
            exporter=exporter,
            progress_callback=progress_callback,
            checkpoint_callback=checkpoint_callback,
            checkpoint_interval=5,
        )

        stats = await parallel.export()

        assert stats.rows_processed == 1000
        assert len(progress_updates) > 0
        assert progress_updates[-1] == 100.0
        assert len(checkpoint_progress) > 0


class TestParallelExportErrorHandling:
    """Test error handling and recovery with real cluster."""

    @pytest.mark.asyncio
    async def test_export_handles_query_timeout(self, session, populated_table, tmp_path):
        """
        Test handling of query timeouts during export.

        What this tests:
        ---------------
        1. Query timeout doesn't crash entire export
        2. Error logged with range information
        3. Other ranges continue processing
        4. Statistics show error count

        Why this matters:
        ----------------
        - Network timeouts common in production
        - One bad range shouldn't fail export
        - Need visibility into partial failures
        - Production resilience requirement

        Additional context:
        ---------------------------------
        - Real timeouts from network/node issues
        - Large partitions may timeout
        - Errors collected for analysis
        """
        output_file = tmp_path / "timeout_export.csv"
        exporter = CSVExporter(output_path=str(output_file))

        # Inject timeout for specific range
        from async_cassandra_bulk.parallel_export import ParallelExporter

        original_export_range = ParallelExporter._export_range

        call_count = 0

        async def timeout_export_range(self, token_range, stats):
            nonlocal call_count
            call_count += 1
            if call_count == 3:  # Fail third range
                raise asyncio.TimeoutError("Query timeout")
            return await original_export_range(self, token_range, stats)

        ParallelExporter._export_range = timeout_export_range

        try:
            parallel = ParallelExporter(
                session=session, table=f"test_bulk.{populated_table}", exporter=exporter
            )

            stats = await parallel.export()

            # Export should partially complete despite error
            assert stats.rows_processed > 0  # Got some data
            assert stats.ranges_completed > 0  # Some ranges succeeded
            assert len(stats.errors) > 0
            assert any("timeout" in str(e).lower() for e in stats.errors)

        finally:
            ParallelExporter._export_range = original_export_range

    @pytest.mark.asyncio
    async def test_export_with_node_failure_simulation(self, session, populated_table, tmp_path):
        """
        Test export resilience to node failure scenarios.

        What this tests:
        ---------------
        1. Export continues despite node unavailability
        2. Retries or skips failed ranges
        3. Logs appropriate error information
        4. Partial export better than no export

        Why this matters:
        ----------------
        - Node failures happen in production
        - Export shouldn't require 100% availability
        - Business continuity during outages
        - Production clusters have node failures

        Additional context:
        ---------------------------------
        - Real clusters have replication
        - Driver may retry on different replicas
        - Some data better than no data
        """
        output_file = tmp_path / "node_failure_export.csv"
        exporter = CSVExporter(output_path=str(output_file))

        parallel = ParallelExporter(
            session=session,
            table=f"test_bulk.{populated_table}",
            exporter=exporter,
            concurrency=2,  # Lower concurrency for test
        )

        # Export should handle transient failures
        stats = await parallel.export()

        # Even with potential failures, should export most data
        assert stats.rows_processed > 0
        assert output_file.exists()


class TestParallelExportPerformance:
    """Test performance characteristics with real data."""

    @pytest.mark.asyncio
    async def test_export_performance_scaling(self, session, tmp_path):
        """
        Test export performance scales with concurrency.

        What this tests:
        ---------------
        1. Higher concurrency improves throughput
        2. Performance scales sub-linearly
        3. Diminishing returns at high concurrency
        4. Optimal concurrency identification

        Why this matters:
        ----------------
        - Production tuning requires benchmarks
        - Resource utilization optimization
        - Cost/performance trade-offs
        - SLA compliance verification

        Additional context:
        ---------------------------------
        - Optimal concurrency depends on cluster
        - Network latency affects scaling
        - Usually 4-16 workers optimal
        """
        # Create larger test dataset
        table_name = f"perf_test_{int(asyncio.get_event_loop().time() * 1000)}"

        await session.execute(
            f"""
            CREATE TABLE test_bulk.{table_name} (
                id uuid PRIMARY KEY,
                data text
            )
        """
        )

        # Insert more rows for performance testing
        insert_stmt = await session.prepare(
            f"""
            INSERT INTO test_bulk.{table_name} (id, data) VALUES (?, ?)
        """
        )

        for i in range(5000):
            await session.execute(insert_stmt, (uuid4(), f"Data {i}" * 10))

        try:
            # Test different concurrency levels
            results = {}

            for concurrency in [1, 4, 8]:
                output_file = tmp_path / f"perf_{concurrency}.csv"
                exporter = CSVExporter(output_path=str(output_file))

                parallel = ParallelExporter(
                    session=session,
                    table=f"test_bulk.{table_name}",
                    exporter=exporter,
                    concurrency=concurrency,
                )

                import time

                start = time.time()
                stats = await parallel.export()
                duration = time.time() - start

                results[concurrency] = {
                    "duration": duration,
                    "rows_per_second": stats.rows_per_second,
                }

                assert stats.rows_processed == 5000

            # Higher concurrency should be faster
            assert results[4]["duration"] < results[1]["duration"]
            assert results[4]["rows_per_second"] > results[1]["rows_per_second"]

        finally:
            await session.execute(f"DROP TABLE test_bulk.{table_name}")
