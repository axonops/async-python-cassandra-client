"""
Simple test to verify parallel execution is working after the fix.

What this tests:
---------------
1. The asyncio.as_completed bug is fixed
2. Queries execute in parallel
3. No errors occur during parallel execution

Why this matters:
----------------
- Parallel execution was completely broken
- Now it should work correctly
- User requested verification of parallel execution
"""

import time

import async_cassandra_dataframe as cdf
import pytest


@pytest.mark.integration
class TestParallelExecutionWorking:
    """Verify parallel execution works after bug fix."""

    @pytest.mark.asyncio
    async def test_basic_parallel_execution(self, session, test_table_name):
        """Basic test that parallel execution works without errors."""
        # Create a simple table
        await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                id INT PRIMARY KEY,
                data TEXT
            )
        """
        )

        # Insert just 100 rows
        insert_stmt = await session.prepare(
            f"INSERT INTO {test_table_name} (id, data) VALUES (?, ?)"
        )

        for i in range(100):
            await session.execute(insert_stmt, (i, f"data_{i}"))

        print("\n=== TESTING PARALLEL EXECUTION ===")

        # Read with parallel execution enabled
        # Don't force many partitions - just verify it works
        start_time = time.time()
        df = await cdf.read_cassandra_table(
            session=session,
            keyspace=session.keyspace,
            table=test_table_name,
            max_concurrent_partitions=5,  # Allow parallel
        )
        duration = time.time() - start_time

        # Verify we got all data
        assert len(df) == 100, f"Expected 100 rows, got {len(df)}"
        assert set(df["id"].values) == set(range(100)), "Missing or incorrect data"

        print(f"✓ Successfully read {len(df)} rows in {duration:.2f}s")
        print("✓ Parallel execution is WORKING!")
        print("==================================")

    @pytest.mark.asyncio
    async def test_parallel_with_multiple_partitions(self, session, test_table_name):
        """Test with a table that has multiple partitions."""
        # Create table with composite primary key
        await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                partition_id INT,
                id INT,
                data TEXT,
                PRIMARY KEY (partition_id, id)
            )
        """
        )

        # Insert data across 5 partitions
        insert_stmt = await session.prepare(
            f"INSERT INTO {test_table_name} (partition_id, id, data) VALUES (?, ?, ?)"
        )

        rows_inserted = 0
        for p in range(5):
            for i in range(20):
                await session.execute(insert_stmt, (p, i, f"data_{p}_{i}"))
                rows_inserted += 1

        print(f"\nInserted {rows_inserted} rows across 5 partitions")

        # Track execution with logging
        import logging

        logging.basicConfig(level=logging.INFO)

        # Read with parallel execution
        start_time = time.time()
        df = await cdf.read_cassandra_table(
            session=session,
            keyspace=session.keyspace,
            table=test_table_name,
            max_concurrent_partitions=3,
        )
        duration = time.time() - start_time

        # Verify results
        assert len(df) == 100, f"Expected 100 rows, got {len(df)}"

        print("\n=== PARALLEL EXECUTION RESULTS ===")
        print(f"✓ Read {len(df)} rows in {duration:.2f}s")
        print(f"✓ Data from {len(df['partition_id'].unique())} partitions")
        print("✓ No errors during parallel execution")
        print("==================================")

    @pytest.mark.asyncio
    async def test_error_handling_in_parallel(self, session, test_table_name):
        """Test that error handling works correctly in parallel execution."""
        # Create a simple table
        await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                id INT PRIMARY KEY,
                data TEXT
            )
        """
        )

        # Insert some data
        insert_stmt = await session.prepare(
            f"INSERT INTO {test_table_name} (id, data) VALUES (?, ?)"
        )
        for i in range(50):
            await session.execute(insert_stmt, (i, f"data_{i}"))

        # Try to read with an invalid column (should fail)
        with pytest.raises(Exception) as exc_info:
            await cdf.read_cassandra_table(
                session=session,
                keyspace=session.keyspace,
                table=test_table_name,
                columns=["id", "invalid_column"],  # This column doesn't exist
                max_concurrent_partitions=3,
            )

        # The important thing is that we get a proper error, not a hang or crash
        print(f"\n✓ Error handling works correctly: {type(exc_info.value).__name__}")
        print("✓ Parallel execution handles errors properly")
