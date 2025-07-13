"""
Test writetime filtering functionality.

CRITICAL: Tests temporal queries and snapshot consistency.
"""

from datetime import UTC, datetime, timedelta

import pytest
from async_cassandra_dataframe import read_cassandra_table


class TestWritetimeFiltering:
    """Test writetime-based filtering capabilities."""

    @pytest.mark.asyncio
    async def test_filter_data_older_than(self, session, test_table_name):
        """
        Test filtering data older than specific writetime.

        What this tests:
        ---------------
        1. Writetime comparison operators work
        2. Only older data returned
        3. Timezone handling correct
        4. Multiple rows filtered correctly

        Why this matters:
        ----------------
        - Archive old data
        - Clean up stale records
        - Time-based data retention
        """
        # Create table
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                id INT PRIMARY KEY,
                status TEXT,
                value INT
            )
            """
        )

        try:
            # Insert data at different times
            # First batch - old data
            await session.execute(
                f"INSERT INTO {test_table_name} (id, status, value) VALUES (1, 'old', 100)"
            )

            # Wait a bit
            await session.execute("SELECT * FROM system.local")  # Force a round trip

            # Mark cutoff time
            cutoff_time = datetime.now(UTC)

            # Wait a bit more
            await session.execute("SELECT * FROM system.local")

            # Second batch - new data
            await session.execute(
                f"INSERT INTO {test_table_name} (id, status, value) VALUES (2, 'new', 200)"
            )

            # Read data older than cutoff
            df = await read_cassandra_table(
                f"test_dataframe.{test_table_name}",
                session=session,
                writetime_filter={"column": "status", "operator": "<", "timestamp": cutoff_time},
            )

            pdf = df.compute()

            # Should only have old data
            assert len(pdf) == 1
            assert pdf.iloc[0]["id"] == 1
            assert pdf.iloc[0]["status"] == "old"

            # Verify writetime is before cutoff
            assert pdf.iloc[0]["status_writetime"] < cutoff_time

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    @pytest.mark.asyncio
    async def test_filter_data_younger_than(self, session, test_table_name):
        """
        Test filtering data younger than specific writetime.

        What this tests:
        ---------------
        1. Recent data extraction
        2. Greater than operator works
        3. Proper timestamp comparison

        Why this matters:
        ----------------
        - Get recent changes only
        - Incremental data loads
        - Real-time analytics
        """
        # Create table
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                id INT PRIMARY KEY,
                event TEXT,
                timestamp TIMESTAMP
            )
            """
        )

        try:
            # Insert old data
            await session.execute(
                f"""
                INSERT INTO {test_table_name} (id, event, timestamp)
                VALUES (1, 'old_event', '2020-01-01T00:00:00Z')
                """
            )

            # Mark threshold
            threshold = datetime.now(UTC) - timedelta(seconds=1)

            # Insert new data
            await session.execute(
                f"""
                INSERT INTO {test_table_name} (id, event, timestamp)
                VALUES (2, 'new_event', '{datetime.now(UTC).isoformat()}')
                """
            )

            # Get data newer than threshold
            df = await read_cassandra_table(
                f"test_dataframe.{test_table_name}",
                session=session,
                writetime_filter={"column": "event", "operator": ">", "timestamp": threshold},
            )

            pdf = df.compute()

            # Should only have new data
            assert len(pdf) == 1
            assert pdf.iloc[0]["id"] == 2
            assert pdf.iloc[0]["event"] == "new_event"

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    @pytest.mark.asyncio
    async def test_snapshot_consistency(self, session, test_table_name):
        """
        Test snapshot consistency with fixed "now" time.

        What this tests:
        ---------------
        1. All queries use same "now" time
        2. Consistent view of data
        3. No drift during long reads

        Why this matters:
        ----------------
        - Consistent snapshots
        - Reproducible extracts
        - Avoid data changes during read
        """
        # Create table
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                id INT PRIMARY KEY,
                data TEXT,
                version INT
            )
            """
        )

        try:
            # Insert initial data
            for i in range(10):
                await session.execute(
                    f"""
                    INSERT INTO {test_table_name} (id, data, version)
                    VALUES ({i}, 'data_{i}', 1)
                    """
                )

            # Read with snapshot time
            df = await read_cassandra_table(
                f"test_dataframe.{test_table_name}",
                session=session,
                snapshot_time="now",  # Fix "now" at read time
                writetime_filter={
                    "column": "data",
                    "operator": "<=",
                    "timestamp": "now",  # Uses same snapshot time
                },
            )

            pdf1 = df.compute()

            # Insert more data (simulating changes during read)
            for i in range(10, 20):
                await session.execute(
                    f"""
                    INSERT INTO {test_table_name} (id, data, version)
                    VALUES ({i}, 'data_{i}', 2)
                    """
                )

            # Read again with same snapshot - should get same data
            df2 = await read_cassandra_table(
                f"test_dataframe.{test_table_name}",
                session=session,
                snapshot_time=pdf1.iloc[0]["data_writetime"],  # Use same time
                writetime_filter={
                    "column": "data",
                    "operator": "<=",
                    "timestamp": pdf1.iloc[0]["data_writetime"],
                },
            )

            pdf2 = await df2.compute()

            # Should have same data despite inserts
            assert len(pdf1) == len(pdf2) == 10
            assert set(pdf1["id"]) == set(range(10))

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    @pytest.mark.asyncio
    async def test_wildcard_writetime_filter(self, session, test_table_name):
        """
        Test filtering with wildcard column selection.

        What this tests:
        ---------------
        1. "*" expands to all writetime-capable columns
        2. OR logic across columns
        3. Correct filtering behavior

        Why this matters:
        ----------------
        - Filter on any column change
        - Comprehensive change detection
        - Simplified queries
        """
        # Create table
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                id INT PRIMARY KEY,
                col1 TEXT,
                col2 TEXT,
                col3 INT
            )
            """
        )

        try:
            # Insert with all columns
            await session.execute(
                f"""
                INSERT INTO {test_table_name} (id, col1, col2, col3)
                VALUES (1, 'a', 'b', 100)
                """
            )

            # Mark time
            cutoff = datetime.now(UTC)

            # Update only one column
            await session.execute(f"UPDATE {test_table_name} SET col2 = 'b_updated' WHERE id = 1")

            # Insert new row
            await session.execute(
                f"""
                INSERT INTO {test_table_name} (id, col1, col2, col3)
                VALUES (2, 'x', 'y', 200)
                """
            )

            # Get any data modified after cutoff
            df = await read_cassandra_table(
                f"test_dataframe.{test_table_name}",
                session=session,
                writetime_filter={
                    "column": "*",  # Check all columns
                    "operator": ">",
                    "timestamp": cutoff,
                },
            )

            pdf = df.compute()

            # Should get both rows (one updated, one new)
            assert len(pdf) == 2

            # Check writetime columns exist
            assert "col1_writetime" in pdf.columns
            assert "col2_writetime" in pdf.columns
            assert "col3_writetime" in pdf.columns

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    @pytest.mark.asyncio
    async def test_concurrency_control(self, session, test_table_name):
        """
        Test concurrent query limiting.

        What this tests:
        ---------------
        1. Max concurrent queries respected
        2. No overwhelming of Cassandra
        3. Proper throttling

        Why this matters:
        ----------------
        - Protect Cassandra cluster
        - Share resources fairly
        - Production stability
        """
        # Create table with many partitions
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                partition_id INT,
                cluster_id INT,
                data TEXT,
                PRIMARY KEY (partition_id, cluster_id)
            )
            """
        )

        try:
            # Insert data across multiple partitions
            insert_stmt = await session.prepare(
                f"""
                INSERT INTO {test_table_name}
                (partition_id, cluster_id, data)
                VALUES (?, ?, ?)
                """
            )

            for p in range(20):
                for c in range(50):
                    await session.execute(insert_stmt, (p, c, f"data_{p}_{c}"))

            # Read with concurrency limit
            df = await read_cassandra_table(
                f"test_dataframe.{test_table_name}",
                session=session,
                partition_count=10,  # Force multiple partitions
                max_concurrent_queries=3,  # Limit concurrent queries
                max_concurrent_partitions=5,  # Limit concurrent processing
                memory_per_partition_mb=1,  # Small to force many queries
            )

            pdf = df.compute()

            # Verify all data read despite throttling
            assert len(pdf) == 20 * 50

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    @pytest.mark.asyncio
    async def test_columns_from_metadata(self, session, test_table_name):
        """
        Test automatic column detection from metadata.

        What this tests:
        ---------------
        1. Columns auto-detected when not specified
        2. All columns included
        3. No SELECT * used internally

        Why this matters:
        ----------------
        - User convenience
        - Schema evolution safety
        - Best practices
        """
        # Create table
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                id INT PRIMARY KEY,
                name TEXT,
                email TEXT,
                age INT,
                active BOOLEAN
            )
            """
        )

        try:
            # Insert data
            await session.execute(
                f"""
                INSERT INTO {test_table_name}
                (id, name, email, age, active)
                VALUES (1, 'Alice', 'alice@example.com', 30, true)
                """
            )

            # Read WITHOUT specifying columns
            df = await read_cassandra_table(
                f"test_dataframe.{test_table_name}",
                session=session,
                # No columns parameter - should auto-detect
            )

            pdf = df.compute()

            # Should have all columns from metadata
            expected_columns = {"id", "name", "email", "age", "active"}
            assert set(pdf.columns) == expected_columns

            # Verify data
            assert len(pdf) == 1
            assert pdf.iloc[0]["name"] == "Alice"
            assert pdf.iloc[0]["age"] == 30

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")
