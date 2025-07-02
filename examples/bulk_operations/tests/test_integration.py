"""
Integration tests for bulk operations with real Cassandra cluster.

What this tests:
---------------
1. End-to-end bulk operations with real data
2. Token range coverage and correctness
3. Performance with multi-node cluster
4. Iceberg export/import functionality

Why this matters:
----------------
- Validates the complete workflow
- Ensures no data loss or duplication
- Tests real cluster behavior
- Verifies Iceberg integration

Additional context:
---------------------------------
These tests require a 3-node Cassandra cluster running
via docker-compose. Use 'make test-integration' to run.
"""

import asyncio
import os
import tempfile
from datetime import datetime
from pathlib import Path
from uuid import uuid4

import pytest

from async_cassandra import AsyncCluster
from bulk_operations.bulk_operator import TokenAwareBulkOperator


@pytest.mark.integration
class TestBulkOperationsIntegration:
    """Integration tests with real Cassandra cluster."""

    @pytest.fixture
    async def cluster(self):
        """Create connection to test cluster."""
        contact_points = os.environ.get(
            "CASSANDRA_CONTACT_POINTS", "127.0.0.1,127.0.0.2,127.0.0.3"
        ).split(",")

        cluster = AsyncCluster(contact_points)
        yield cluster
        await cluster.shutdown()

    @pytest.fixture
    async def session(self, cluster):
        """Create test session."""
        session = await cluster.connect()
        yield session
        await session.close()

    @pytest.fixture
    async def test_keyspace(self, session):
        """Create test keyspace with RF=3."""
        keyspace = f"test_bulk_{uuid4().hex[:8]}"

        await session.execute(
            f"""
            CREATE KEYSPACE {keyspace}
            WITH REPLICATION = {{
                'class': 'SimpleStrategy',
                'replication_factor': 3
            }}
        """
        )

        yield keyspace

        # Cleanup
        await session.execute(f"DROP KEYSPACE {keyspace}")

    @pytest.fixture
    async def test_table(self, session, test_keyspace):
        """Create test table with sample data."""
        table = "test_data"

        # Create table
        await session.execute(
            f"""
            CREATE TABLE {test_keyspace}.{table} (
                partition_id int,
                cluster_id int,
                data text,
                created_at timestamp,
                PRIMARY KEY (partition_id, cluster_id)
            )
        """
        )

        # Insert test data across partitions
        insert_stmt = await session.prepare(
            f"""
            INSERT INTO {test_keyspace}.{table}
            (partition_id, cluster_id, data, created_at)
            VALUES (?, ?, ?, ?)
        """
        )

        # Create data that will be distributed across all nodes
        rows_inserted = 0
        for partition in range(100):  # 100 partitions
            for cluster in range(10):  # 10 rows per partition
                await session.execute(
                    insert_stmt, [partition, cluster, f"data_{partition}_{cluster}", datetime.now()]
                )
                rows_inserted += 1

        return table, rows_inserted

    @pytest.mark.slow
    async def test_count_all_data(self, session, test_keyspace, test_table):
        """Test counting all rows using token ranges."""
        table_name, expected_count = test_table
        operator = TokenAwareBulkOperator(session)

        # Count using token ranges
        actual_count = await operator.count_by_token_ranges(
            keyspace=test_keyspace, table=table_name, split_count=12  # 4 splits per node
        )

        assert actual_count == expected_count

    @pytest.mark.slow
    async def test_count_vs_regular_count(self, session, test_keyspace, test_table):
        """Compare token range count with regular COUNT(*)."""
        table_name, _ = test_table
        operator = TokenAwareBulkOperator(session)

        # Regular count (can timeout on large tables)
        result = await session.execute(f"SELECT COUNT(*) FROM {test_keyspace}.{table_name}")
        regular_count = result.one().count

        # Token range count
        token_count = await operator.count_by_token_ranges(
            keyspace=test_keyspace, table=table_name, split_count=24
        )

        assert token_count == regular_count

    @pytest.mark.slow
    async def test_export_completeness(self, session, test_keyspace, test_table):
        """Test that export captures all data."""
        table_name, expected_count = test_table
        operator = TokenAwareBulkOperator(session)

        # Export all data
        exported_rows = []
        async for row in operator.export_by_token_ranges(
            keyspace=test_keyspace, table=table_name, split_count=12
        ):
            exported_rows.append(row)

        assert len(exported_rows) == expected_count

        # Verify data integrity
        seen_keys = set()
        for row in exported_rows:
            key = (row.partition_id, row.cluster_id)
            assert key not in seen_keys, "Duplicate row found"
            seen_keys.add(key)

    @pytest.mark.slow
    async def test_progress_tracking(self, session, test_keyspace, test_table):
        """Test progress tracking during operations."""
        table_name, _ = test_table
        operator = TokenAwareBulkOperator(session)

        progress_updates = []

        def track_progress(stats):
            progress_updates.append(
                {
                    "percentage": stats.progress_percentage,
                    "ranges": stats.ranges_completed,
                    "rows": stats.rows_processed,
                }
            )

        # Run count with progress tracking
        await operator.count_by_token_ranges(
            keyspace=test_keyspace,
            table=table_name,
            split_count=6,
            progress_callback=track_progress,
        )

        # Verify progress updates
        assert len(progress_updates) > 0
        assert progress_updates[0]["percentage"] < progress_updates[-1]["percentage"]
        assert progress_updates[-1]["percentage"] == 100.0

    @pytest.mark.slow
    async def test_iceberg_export_import(self, session, test_keyspace, test_table):
        """Test full export/import cycle with Iceberg."""
        table_name, expected_count = test_table
        operator = TokenAwareBulkOperator(session)

        with tempfile.TemporaryDirectory() as temp_dir:
            warehouse_path = Path(temp_dir) / "iceberg_warehouse"

            # Export to Iceberg
            export_stats = await operator.export_to_iceberg(
                source_keyspace=test_keyspace,
                source_table=table_name,
                iceberg_warehouse_path=str(warehouse_path),
                iceberg_table="test_export",
                split_count=12,
            )

            assert export_stats.row_count == expected_count
            assert export_stats.success

            # Create new table for import
            import_table = "imported_data"
            await session.execute(
                f"""
                CREATE TABLE {test_keyspace}.{import_table} (
                    partition_id int,
                    cluster_id int,
                    data text,
                    created_at timestamp,
                    PRIMARY KEY (partition_id, cluster_id)
                )
            """
            )

            # Import from Iceberg
            import_stats = await operator.import_from_iceberg(
                iceberg_warehouse_path=str(warehouse_path),
                iceberg_table="test_export",
                target_keyspace=test_keyspace,
                target_table=import_table,
                parallelism=12,
            )

            assert import_stats.row_count == expected_count
            assert import_stats.success

            # Verify imported data
            result = await session.execute(f"SELECT COUNT(*) FROM {test_keyspace}.{import_table}")
            assert result.one().count == expected_count

    @pytest.mark.slow
    async def test_concurrent_operations(self, session, test_keyspace, test_table):
        """Test running multiple bulk operations concurrently."""
        table_name, expected_count = test_table
        operator = TokenAwareBulkOperator(session)

        # Run multiple counts concurrently
        tasks = [
            operator.count_by_token_ranges(keyspace=test_keyspace, table=table_name, split_count=8)
            for _ in range(3)
        ]

        results = await asyncio.gather(*tasks)

        # All should return same count
        assert all(count == expected_count for count in results)

    @pytest.mark.slow
    async def test_large_table_performance(self, session, test_keyspace):
        """Test performance with larger dataset."""
        table = "large_table"

        # Create table
        await session.execute(
            f"""
            CREATE TABLE {test_keyspace}.{table} (
                id uuid,
                data text,
                value double,
                created_at timestamp,
                PRIMARY KEY (id)
            )
        """
        )

        # Insert 10k rows
        insert_stmt = await session.prepare(
            f"""
            INSERT INTO {test_keyspace}.{table}
            (id, data, value, created_at)
            VALUES (?, ?, ?, ?)
        """
        )

        start_time = datetime.now()
        tasks = []
        for i in range(10000):
            task = session.execute(
                insert_stmt,
                [uuid4(), f"data_{i}" * 10, float(i), datetime.now()],  # Make rows bigger
            )
            tasks.append(task)

            # Batch inserts
            if len(tasks) >= 100:
                await asyncio.gather(*tasks)
                tasks = []

        if tasks:
            await asyncio.gather(*tasks)

        insert_duration = (datetime.now() - start_time).total_seconds()

        # Test count performance
        operator = TokenAwareBulkOperator(session)

        start_time = datetime.now()
        count = await operator.count_by_token_ranges(
            keyspace=test_keyspace, table=table, split_count=24
        )
        count_duration = (datetime.now() - start_time).total_seconds()

        assert count == 10000

        # Performance assertions
        rows_per_second = count / count_duration
        assert rows_per_second > 1000, f"Count too slow: {rows_per_second} rows/sec"

        print("\nPerformance stats:")
        print(f"  Insert: {10000/insert_duration:.0f} rows/sec")
        print(f"  Count: {rows_per_second:.0f} rows/sec")
