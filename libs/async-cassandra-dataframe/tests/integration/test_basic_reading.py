"""
Basic integration tests for DataFrame reading.

Tests core functionality of reading Cassandra tables as Dask DataFrames.
"""

import async_cassandra_dataframe as cdf
import dask.dataframe as dd
import pandas as pd
import pytest


class TestBasicReading:
    """Test basic DataFrame reading functionality."""

    @pytest.mark.asyncio
    async def test_read_simple_table(self, session, basic_test_table):
        """
        Test reading a simple table as DataFrame.

        What this tests:
        ---------------
        1. Basic table reading works
        2. All columns are read correctly
        3. Data types are preserved
        4. Row count is correct

        Why this matters:
        ----------------
        - Fundamental functionality must work
        - Type conversion must be correct
        - No data loss during read
        """
        # Read table as Dask DataFrame
        df = await cdf.read_cassandra_table(basic_test_table, session=session)

        # Verify it's a Dask DataFrame
        assert isinstance(df, dd.DataFrame)

        # Compute to pandas for verification
        pdf = df.compute()

        # Verify structure
        assert len(pdf) == 1000  # We inserted 1000 rows
        assert set(pdf.columns) == {"id", "name", "value", "created_at", "is_active"}

        # Verify data types
        assert pdf["id"].dtype == "int32"
        assert pdf["name"].dtype == "object"
        assert pdf["value"].dtype == "float64"
        assert pd.api.types.is_datetime64_any_dtype(pdf["created_at"])
        assert pdf["is_active"].dtype == "bool"

        # Verify some data
        assert pdf["id"].min() == 0
        assert pdf["id"].max() == 999
        assert pdf["name"].iloc[0] == "name_0"
        assert pdf["value"].iloc[0] == 0.0

    @pytest.mark.asyncio
    async def test_read_with_column_selection(self, session, basic_test_table):
        """
        Test reading specific columns only.

        What this tests:
        ---------------
        1. Column selection works
        2. Only requested columns are read
        3. Performance optimization

        Why this matters:
        ----------------
        - Reduces memory usage
        - Improves performance
        - Common use case
        """
        # Read only specific columns
        df = await cdf.read_cassandra_table(
            basic_test_table, session=session, columns=["id", "name"]
        )

        pdf = df.compute()

        # Verify only requested columns
        assert set(pdf.columns) == {"id", "name"}
        assert len(pdf) == 1000

    @pytest.mark.asyncio
    async def test_read_with_partition_control(self, session, basic_test_table):
        """
        Test reading with explicit partition count.

        What this tests:
        ---------------
        1. Partition count override works
        2. Data is split correctly
        3. All data is read

        Why this matters:
        ----------------
        - Users need control over parallelism
        - Different cluster sizes need different settings
        - Performance tuning
        """
        # Read with specific partition count
        df = await cdf.read_cassandra_table(basic_test_table, session=session, partition_count=5)

        # Check partition count
        assert df.npartitions == 5

        # Verify all data is read
        pdf = df.compute()
        assert len(pdf) == 1000

    @pytest.mark.asyncio
    async def test_read_with_memory_limit(self, session, basic_test_table):
        """
        Test reading with memory limit per partition.

        What this tests:
        ---------------
        1. Memory limits are respected
        2. Adaptive partitioning works
        3. No OOM errors

        Why this matters:
        ----------------
        - Memory safety is critical
        - Must work on limited resources
        - Adaptive approach validation
        """
        # Read with small memory limit - should create more partitions
        df = await cdf.read_cassandra_table(
            basic_test_table, session=session, memory_per_partition_mb=10  # Small limit
        )

        # Should have more partitions due to memory limit
        assert df.npartitions > 1

        # But all data should be read
        pdf = df.compute()
        assert len(pdf) == 1000

    @pytest.mark.asyncio
    async def test_read_empty_table(self, session, test_table_name):
        """
        Test reading an empty table.

        What this tests:
        ---------------
        1. Empty tables handled gracefully
        2. Schema is still correct
        3. No errors on empty data

        Why this matters:
        ----------------
        - Edge case handling
        - Robustness
        - Common in development/testing
        """
        # Create empty table
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                id INT PRIMARY KEY,
                data TEXT
            )
            """
        )

        try:
            df = await cdf.read_cassandra_table(
                f"test_dataframe.{test_table_name}", session=session
            )

            pdf = df.compute()

            # Should be empty but have correct schema
            assert len(pdf) == 0
            assert set(pdf.columns) == {"id", "data"}
            assert pdf["id"].dtype == "int32"
            assert pdf["data"].dtype == "object"

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    @pytest.mark.asyncio
    async def test_read_with_simple_filter(self, session, basic_test_table):
        """
        Test reading with filter expression.

        What this tests:
        ---------------
        1. Filter expressions work
        2. Data is filtered correctly
        3. Performance benefit

        Why this matters:
        ----------------
        - Common use case
        - Reduces data transfer
        - Improves performance
        """
        # Read with filter
        df = await cdf.read_cassandra_table(
            basic_test_table, session=session, filter_expr="id < 100"
        )

        pdf = df.compute()

        # Verify filter applied
        assert len(pdf) == 100
        assert pdf["id"].max() == 99

    @pytest.mark.asyncio
    async def test_error_on_missing_table(self, session):
        """
        Test error handling for non-existent table.

        What this tests:
        ---------------
        1. Clear error on missing table
        2. No confusing stack traces
        3. Helpful error message

        Why this matters:
        ----------------
        - User experience
        - Debugging ease
        - Common mistake
        """
        with pytest.raises(ValueError) as exc_info:
            await cdf.read_cassandra_table("test_dataframe.does_not_exist", session=session)

        assert "not found" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_error_on_missing_columns(self, session, basic_test_table):
        """
        Test error handling for non-existent columns.

        What this tests:
        ---------------
        1. Clear error on missing columns
        2. Lists invalid columns
        3. Helpful error message

        Why this matters:
        ----------------
        - Common user error
        - Clear feedback needed
        - Debugging support
        """
        with pytest.raises(ValueError) as exc_info:
            await cdf.read_cassandra_table(
                basic_test_table, session=session, columns=["id", "does_not_exist"]
            )

        assert "does_not_exist" in str(exc_info.value)
