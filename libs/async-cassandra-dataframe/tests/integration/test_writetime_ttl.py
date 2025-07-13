"""
Test writetime and TTL functionality.

CRITICAL: Tests metadata columns work correctly.
"""

import async_cassandra_dataframe as cdf
import pandas as pd
import pytest


class TestWritetimeTTL:
    """Test writetime and TTL support."""

    @pytest.mark.asyncio
    async def test_writetime_columns(self, session, test_table_name):
        """
        Test reading writetime columns.

        What this tests:
        ---------------
        1. Writetime queries work
        2. Timestamp conversion correct
        3. Timezone handling
        4. Multiple writetime columns

        Why this matters:
        ----------------
        - Common audit use case
        - Debugging data issues
        - Compliance requirements
        """
        # Create table
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                id INT PRIMARY KEY,
                name TEXT,
                value INT,
                data TEXT
            )
            """
        )

        try:
            # Insert data
            await session.execute(
                f"""
                INSERT INTO {test_table_name} (id, name, value, data)
                VALUES (1, 'test', 100, 'sample')
                """
            )

            # Read with writetime
            df = await cdf.read_cassandra_table(
                f"test_dataframe.{test_table_name}",
                session=session,
                writetime_columns=["name", "value", "data"],
            )

            pdf = df.compute()

            # Should have writetime columns
            assert "name_writetime" in pdf.columns
            assert "value_writetime" in pdf.columns
            assert "data_writetime" in pdf.columns

            # Should be timestamps
            assert pd.api.types.is_datetime64_any_dtype(pdf["name_writetime"])
            assert pd.api.types.is_datetime64_any_dtype(pdf["value_writetime"])
            assert pd.api.types.is_datetime64_any_dtype(pdf["data_writetime"])

            # Should have timezone
            row = pdf.iloc[0]
            assert row["name_writetime"].tz is not None
            assert row["name_writetime"].tz.zone == "UTC"

            # All writetimes should be the same (inserted together)
            assert row["name_writetime"] == row["value_writetime"]
            assert row["value_writetime"] == row["data_writetime"]

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    @pytest.mark.asyncio
    async def test_ttl_columns(self, session, test_table_name):
        """
        Test reading TTL columns.

        What this tests:
        ---------------
        1. TTL queries work
        2. TTL values correct
        3. NULL TTL handling
        4. Multiple TTL columns

        Why this matters:
        ----------------
        - Data expiration tracking
        - Cache management
        - Cleanup scheduling
        """
        # Create table
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                id INT PRIMARY KEY,
                cache_data TEXT,
                temp_value INT
            )
            """
        )

        try:
            # Insert with TTL
            await session.execute(
                f"""
                INSERT INTO {test_table_name} (id, cache_data, temp_value)
                VALUES (1, 'cached', 42)
                USING TTL 3600
                """
            )

            # Insert without TTL
            await session.execute(
                f"""
                INSERT INTO {test_table_name} (id, cache_data, temp_value)
                VALUES (2, 'permanent', 100)
                """
            )

            # Read with TTL
            df = await cdf.read_cassandra_table(
                f"test_dataframe.{test_table_name}",
                session=session,
                ttl_columns=["cache_data", "temp_value"],
            )

            pdf = df.compute()
            pdf = pdf.sort_values("id").reset_index(drop=True)

            # Should have TTL columns
            assert "cache_data_ttl" in pdf.columns
            assert "temp_value_ttl" in pdf.columns

            # Row 1 should have TTL
            row1 = pdf.iloc[0]
            assert row1["cache_data_ttl"] is not None
            assert row1["cache_data_ttl"] > 0
            assert row1["cache_data_ttl"] <= 3600

            # Row 2 should have no TTL
            row2 = pdf.iloc[1]
            assert pd.isna(row2["cache_data_ttl"]) or row2["cache_data_ttl"] is None
            assert pd.isna(row2["temp_value_ttl"]) or row2["temp_value_ttl"] is None

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    @pytest.mark.asyncio
    async def test_writetime_ttl_combined(self, session, test_table_name):
        """
        Test reading both writetime and TTL together.

        What this tests:
        ---------------
        1. Combined metadata queries work
        2. Column name conflicts avoided
        3. Correct values for each

        Why this matters:
        ----------------
        - Complete metadata view
        - Audit and expiration together
        - Complex use cases
        """
        # Create table
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                id INT PRIMARY KEY,
                data TEXT,
                counter INT
            )
            """
        )

        try:
            # Insert with TTL
            await session.execute(
                f"""
                INSERT INTO {test_table_name} (id, data, counter)
                VALUES (1, 'test', 100)
                USING TTL 7200
                """
            )

            # Read with both
            df = await cdf.read_cassandra_table(
                f"test_dataframe.{test_table_name}",
                session=session,
                writetime_columns=["data", "counter"],
                ttl_columns=["data", "counter"],
            )

            pdf = df.compute()

            # Should have both types of columns
            assert "data_writetime" in pdf.columns
            assert "data_ttl" in pdf.columns
            assert "counter_writetime" in pdf.columns
            assert "counter_ttl" in pdf.columns

            # Verify values
            row = pdf.iloc[0]
            assert row["data_writetime"] is not None
            assert row["data_ttl"] is not None
            assert row["data_ttl"] <= 7200

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    @pytest.mark.asyncio
    async def test_writetime_wildcard(self, session, test_table_name):
        """
        Test writetime with wildcard selection.

        What this tests:
        ---------------
        1. Wildcard "*" expands correctly
        2. Only non-PK columns included
        3. All eligible columns get writetime

        Why this matters:
        ----------------
        - Convenience feature
        - Full audit trail
        - Bulk metadata queries
        """
        # Create table
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                id INT PRIMARY KEY,
                col1 TEXT,
                col2 INT,
                col3 BOOLEAN,
                col4 FLOAT
            )
            """
        )

        try:
            # Insert data
            await session.execute(
                f"""
                INSERT INTO {test_table_name}
                (id, col1, col2, col3, col4)
                VALUES (1, 'a', 1, true, 3.14)
                """
            )

            # Read with wildcard
            df = await cdf.read_cassandra_table(
                f"test_dataframe.{test_table_name}", session=session, writetime_columns=["*"]
            )

            pdf = df.compute()

            # Should have writetime for all non-PK columns
            assert "id_writetime" not in pdf.columns  # PK excluded
            assert "col1_writetime" in pdf.columns
            assert "col2_writetime" in pdf.columns
            assert "col3_writetime" in pdf.columns
            assert "col4_writetime" in pdf.columns

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    @pytest.mark.asyncio
    async def test_no_writetime_for_pk(self, session, test_table_name):
        """
        Test that primary key columns don't get writetime.

        What this tests:
        ---------------
        1. PK columns excluded from writetime
        2. Error handling if requested
        3. Metadata validation

        Why this matters:
        ----------------
        - Cassandra limitation
        - Prevent invalid queries
        - Clear error messages
        """
        # Create table with composite key
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
            # Insert data
            await session.execute(
                f"""
                INSERT INTO {test_table_name}
                (partition_id, cluster_id, data)
                VALUES (1, 1, 'test')
                """
            )

            # Try to read writetime for all
            df = await cdf.read_cassandra_table(
                f"test_dataframe.{test_table_name}",
                session=session,
                writetime_columns=["partition_id", "cluster_id", "data"],
            )

            pdf = df.compute()

            # PK columns should not have writetime
            assert "partition_id_writetime" not in pdf.columns
            assert "cluster_id_writetime" not in pdf.columns
            # Regular column should have writetime
            assert "data_writetime" in pdf.columns

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")
