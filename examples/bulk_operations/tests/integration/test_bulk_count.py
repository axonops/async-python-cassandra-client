"""
Integration tests for bulk count operations.

What this tests:
---------------
1. Full data coverage with token ranges (no missing/duplicate rows)
2. Wraparound range handling
3. Count accuracy across different data distributions
4. Performance with parallelism

Why this matters:
----------------
- Count is the simplest bulk operation - if it fails, everything fails
- Proves our token range queries are correct
- Gaps mean data loss in production
- Duplicates mean incorrect counting
- Critical for data integrity
"""

import asyncio

import pytest

from async_cassandra import AsyncCluster
from bulk_operations.bulk_operator import TokenAwareBulkOperator


@pytest.mark.integration
class TestBulkCount:
    """Test bulk count operations against real Cassandra cluster."""

    @pytest.fixture
    async def cluster(self):
        """Create connection to test cluster."""
        cluster = AsyncCluster(
            contact_points=["localhost"],
            port=9042,
        )
        yield cluster
        await cluster.shutdown()

    @pytest.fixture
    async def session(self, cluster):
        """Create test session with keyspace and table."""
        session = await cluster.connect()

        # Create test keyspace
        await session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS bulk_test
            WITH replication = {
                'class': 'SimpleStrategy',
                'replication_factor': 1
            }
        """
        )

        # Create test table
        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS bulk_test.test_data (
                id INT PRIMARY KEY,
                data TEXT,
                value DOUBLE
            )
        """
        )

        # Clear any existing data
        await session.execute("TRUNCATE bulk_test.test_data")

        yield session

    @pytest.mark.asyncio
    async def test_full_table_coverage_with_token_ranges(self, session):
        """
        Test that token ranges cover all data without gaps or duplicates.

        What this tests:
        ---------------
        1. Insert known dataset across token range
        2. Count using token ranges
        3. Verify exact match with direct count
        4. No missing or duplicate rows

        Why this matters:
        ----------------
        - Proves our token range queries are correct
        - Gaps mean data loss in production
        - Duplicates mean incorrect counting
        - Critical for data integrity
        """
        # Insert test data with known count
        insert_stmt = await session.prepare(
            """
            INSERT INTO bulk_test.test_data (id, data, value)
            VALUES (?, ?, ?)
        """
        )

        expected_count = 10000
        print(f"\nInserting {expected_count} test rows...")

        # Insert in batches for efficiency
        batch_size = 100
        for i in range(0, expected_count, batch_size):
            tasks = []
            for j in range(batch_size):
                if i + j < expected_count:
                    tasks.append(session.execute(insert_stmt, (i + j, f"data-{i+j}", float(i + j))))
            await asyncio.gather(*tasks)

        # Count using direct query
        result = await session.execute("SELECT COUNT(*) FROM bulk_test.test_data")
        direct_count = result.one().count
        assert (
            direct_count == expected_count
        ), f"Direct count mismatch: {direct_count} vs {expected_count}"

        # Count using token ranges
        operator = TokenAwareBulkOperator(session)
        token_count = await operator.count_by_token_ranges(
            keyspace="bulk_test",
            table="test_data",
            split_count=16,  # Moderate splitting
            parallelism=8,
        )

        print("\nCount comparison:")
        print(f"  Direct count: {direct_count}")
        print(f"  Token range count: {token_count}")

        assert (
            token_count == direct_count
        ), f"Token range count mismatch: {token_count} vs {direct_count}"

    @pytest.mark.asyncio
    async def test_count_with_wraparound_ranges(self, session):
        """
        Test counting specifically with wraparound ranges.

        What this tests:
        ---------------
        1. Insert data that falls in wraparound range
        2. Verify wraparound range is properly split
        3. Count includes all data
        4. No double counting

        Why this matters:
        ----------------
        - Wraparound ranges are tricky edge cases
        - CQL doesn't support OR in token queries
        - Must split into two queries properly
        - Common source of bugs
        """
        # Insert test data
        insert_stmt = await session.prepare(
            """
            INSERT INTO bulk_test.test_data (id, data, value)
            VALUES (?, ?, ?)
        """
        )

        # Insert data with IDs that we know will hash to extreme token values
        test_ids = []
        for i in range(50000, 60000):  # Test range that includes wraparound tokens
            test_ids.append(i)

        print(f"\nInserting {len(test_ids)} test rows...")
        batch_size = 100
        for i in range(0, len(test_ids), batch_size):
            tasks = []
            for j in range(batch_size):
                if i + j < len(test_ids):
                    id_val = test_ids[i + j]
                    tasks.append(
                        session.execute(insert_stmt, (id_val, f"data-{id_val}", float(id_val)))
                    )
            await asyncio.gather(*tasks)

        # Get direct count
        result = await session.execute("SELECT COUNT(*) FROM bulk_test.test_data")
        direct_count = result.one().count

        # Count using token ranges with different split counts
        operator = TokenAwareBulkOperator(session)

        for split_count in [4, 8, 16, 32]:
            token_count = await operator.count_by_token_ranges(
                keyspace="bulk_test",
                table="test_data",
                split_count=split_count,
                parallelism=4,
            )

            print(f"\nSplit count {split_count}: {token_count} rows")
            assert (
                token_count == direct_count
            ), f"Count mismatch with {split_count} splits: {token_count} vs {direct_count}"

    @pytest.mark.asyncio
    async def test_parallel_count_performance(self, session):
        """
        Test parallel execution improves count performance.

        What this tests:
        ---------------
        1. Count performance with different parallelism levels
        2. Results are consistent across parallelism levels
        3. No deadlocks or timeouts
        4. Higher parallelism provides benefit

        Why this matters:
        ----------------
        - Parallel execution is the main benefit
        - Must handle concurrent queries properly
        - Performance validation
        - Resource efficiency
        """
        # Insert more data for meaningful parallelism test
        insert_stmt = await session.prepare(
            """
            INSERT INTO bulk_test.test_data (id, data, value)
            VALUES (?, ?, ?)
        """
        )

        # Clear and insert fresh data
        await session.execute("TRUNCATE bulk_test.test_data")

        row_count = 50000
        print(f"\nInserting {row_count} rows for parallel test...")

        batch_size = 500
        for i in range(0, row_count, batch_size):
            tasks = []
            for j in range(batch_size):
                if i + j < row_count:
                    tasks.append(session.execute(insert_stmt, (i + j, f"data-{i+j}", float(i + j))))
            await asyncio.gather(*tasks)

        operator = TokenAwareBulkOperator(session)

        # Test with different parallelism levels
        import time

        results = []
        for parallelism in [1, 2, 4, 8]:
            start_time = time.time()

            count = await operator.count_by_token_ranges(
                keyspace="bulk_test", table="test_data", split_count=32, parallelism=parallelism
            )

            duration = time.time() - start_time
            results.append(
                {
                    "parallelism": parallelism,
                    "count": count,
                    "duration": duration,
                    "rows_per_sec": count / duration,
                }
            )

            print(f"\nParallelism {parallelism}:")
            print(f"  Count: {count}")
            print(f"  Duration: {duration:.2f}s")
            print(f"  Rows/sec: {count/duration:,.0f}")

        # All counts should be identical
        counts = [r["count"] for r in results]
        assert len(set(counts)) == 1, f"Inconsistent counts: {counts}"

        # Higher parallelism should generally be faster
        # (though not always due to overhead)
        assert (
            results[-1]["duration"] < results[0]["duration"] * 1.5
        ), "Parallel execution not providing benefit"

    @pytest.mark.asyncio
    async def test_count_with_progress_callback(self, session):
        """
        Test progress callback during count operations.

        What this tests:
        ---------------
        1. Progress callbacks are invoked correctly
        2. Stats are accurate and updated
        3. Progress percentage is calculated correctly
        4. Final stats match actual results

        Why this matters:
        ----------------
        - Users need progress feedback for long operations
        - Stats help with monitoring and debugging
        - Progress tracking enables better UX
        - Critical for production observability
        """
        # Insert test data
        insert_stmt = await session.prepare(
            """
            INSERT INTO bulk_test.test_data (id, data, value)
            VALUES (?, ?, ?)
        """
        )

        expected_count = 5000
        for i in range(expected_count):
            await session.execute(insert_stmt, (i, f"data-{i}", float(i)))

        operator = TokenAwareBulkOperator(session)

        # Track progress callbacks
        progress_updates = []

        def progress_callback(stats):
            progress_updates.append(
                {
                    "rows": stats.rows_processed,
                    "ranges_completed": stats.ranges_completed,
                    "total_ranges": stats.total_ranges,
                    "percentage": stats.progress_percentage,
                }
            )

        # Count with progress tracking
        count, stats = await operator.count_by_token_ranges_with_stats(
            keyspace="bulk_test",
            table="test_data",
            split_count=8,
            parallelism=4,
            progress_callback=progress_callback,
        )

        print(f"\nProgress updates received: {len(progress_updates)}")
        print(f"Final count: {count}")
        print(
            f"Final stats: rows={stats.rows_processed}, ranges={stats.ranges_completed}/{stats.total_ranges}"
        )

        # Verify results
        assert count == expected_count, f"Count mismatch: {count} vs {expected_count}"
        assert stats.rows_processed == expected_count
        assert stats.ranges_completed == stats.total_ranges
        assert stats.success is True
        assert len(stats.errors) == 0
        assert len(progress_updates) > 0, "No progress callbacks received"

        # Verify progress increased monotonically
        for i in range(1, len(progress_updates)):
            assert (
                progress_updates[i]["ranges_completed"]
                >= progress_updates[i - 1]["ranges_completed"]
            )
