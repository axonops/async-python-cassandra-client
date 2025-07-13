"""
Comprehensive tests for all Cassandra data types.

CRITICAL: Tests every Cassandra type for correct DataFrame conversion.
"""

from datetime import date, datetime
from decimal import Decimal
from ipaddress import IPv4Address
from uuid import uuid4

import async_cassandra_dataframe as cdf
import pandas as pd
import pytest
from cassandra.util import uuid_from_time


class TestAllCassandraTypes:
    """Test DataFrame reading with all Cassandra types."""

    @pytest.mark.asyncio
    async def test_all_basic_types(self, session, all_types_table):
        """
        Test all basic Cassandra types.

        What this tests:
        ---------------
        1. Every Cassandra type converts correctly
        2. NULL values handled properly
        3. Type precision preserved
        4. No data corruption

        Why this matters:
        ----------------
        - Must support all Cassandra types
        - Type safety critical for data integrity
        - Common source of bugs
        - Production systems use all types
        """
        # Insert test data with all types
        test_uuid = uuid4()
        test_timeuuid = uuid_from_time(datetime.now())

        await session.execute(
            f"""
            INSERT INTO {all_types_table.split('.')[1]} (
                id, ascii_col, text_col, varchar_col,
                tinyint_col, smallint_col, int_col, bigint_col, varint_col,
                float_col, double_col, decimal_col,
                date_col, time_col, timestamp_col, duration_col,
                blob_col, boolean_col, inet_col, uuid_col, timeuuid_col,
                list_col, set_col, map_col, tuple_col
            ) VALUES (
                1, 'ascii_test', 'text_test', 'varchar_test',
                127, 32767, 2147483647, 9223372036854775807, 123456789012345678901234567890,
                3.14, 3.14159265359, 123.456789012345678901234567890,
                '2024-01-15', '10:30:45.123456789', '2024-01-15T10:30:45.123Z', 1mo2d3h4m5s6ms7us8ns,
                0x48656c6c6f, true, '192.168.1.1', %s, %s,
                ['item1', 'item2'], {1, 2, 3}, {'key1': 10, 'key2': 20}, ('test', 42, true)
            )
            """,
            (test_uuid, test_timeuuid),
        )

        # Insert row with NULLs
        await session.execute(f"INSERT INTO {all_types_table.split('.')[1]} (id) VALUES (2)")

        # Insert row with empty collections
        await session.execute(
            f"""
            INSERT INTO {all_types_table.split('.')[1]} (
                id, list_col, set_col, map_col
            ) VALUES (
                3, [], {{}}, {{}}
            )
            """
        )

        # Read as DataFrame
        df = await cdf.read_cassandra_table(all_types_table, session=session)

        pdf = df.compute()

        # Sort by ID for consistent testing
        pdf = pdf.sort_values("id").reset_index(drop=True)

        # Test row 1 - all values populated
        row1 = pdf.iloc[0]

        # String types
        assert row1["ascii_col"] == "ascii_test"
        assert row1["text_col"] == "text_test"
        assert row1["varchar_col"] == "varchar_test"

        # Numeric types
        assert row1["tinyint_col"] == 127
        assert row1["smallint_col"] == 32767
        assert row1["int_col"] == 2147483647
        assert row1["bigint_col"] == 9223372036854775807
        assert row1["varint_col"] == 123456789012345678901234567890  # Python int
        assert abs(row1["float_col"] - 3.14) < 0.001
        assert abs(row1["double_col"] - 3.14159265359) < 0.0000001

        # Decimal - MUST preserve precision
        assert isinstance(row1["decimal_col"], Decimal)
        assert str(row1["decimal_col"]) == "123.456789012345678901234567890"

        # Temporal types
        assert isinstance(row1["date_col"], pd.Timestamp)
        assert row1["date_col"].date() == date(2024, 1, 15)

        assert isinstance(row1["time_col"], pd.Timedelta)
        # Time should be 10:30:45.123456789
        expected_time = pd.Timedelta(hours=10, minutes=30, seconds=45, nanoseconds=123456789)
        assert row1["time_col"] == expected_time

        assert isinstance(row1["timestamp_col"], pd.Timestamp)
        assert row1["timestamp_col"].year == 2024
        assert row1["timestamp_col"].month == 1
        assert row1["timestamp_col"].day == 15

        # Duration - special type
        assert row1["duration_col"] is not None  # Complex type, kept as object

        # Binary
        assert row1["blob_col"] == b"Hello"

        # Other types
        assert row1["boolean_col"] is True
        assert row1["inet_col"] == IPv4Address("192.168.1.1")
        assert row1["uuid_col"] == test_uuid
        assert row1["timeuuid_col"] == test_timeuuid

        # Collections
        assert row1["list_col"] == ["item1", "item2"]
        assert set(row1["set_col"]) == {1, 2, 3}  # Sets become lists
        assert row1["map_col"] == {"key1": 10, "key2": 20}
        assert row1["tuple_col"] == ["test", 42, True]  # Tuples become lists

        # Test row 2 - all NULLs
        row2 = pdf.iloc[1]
        assert row2["id"] == 2
        for col in pdf.columns:
            if col != "id":
                assert pd.isna(row2[col]) or row2[col] is None

        # Test row 3 - empty collections
        row3 = pdf.iloc[2]
        assert row3["id"] == 3
        # Empty collections should be NULL (Cassandra behavior)
        assert row3["list_col"] is None
        assert row3["set_col"] is None
        assert row3["map_col"] is None

    @pytest.mark.asyncio
    async def test_counter_type(self, session, test_table_name):
        """
        Test counter type handling.

        Counters are special in Cassandra and have restrictions.
        """
        # Create counter table
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                id INT PRIMARY KEY,
                count_value COUNTER
            )
            """
        )

        try:
            # Update counter
            await session.execute(
                f"UPDATE {test_table_name} SET count_value = count_value + 10 WHERE id = 1"
            )
            await session.execute(
                f"UPDATE {test_table_name} SET count_value = count_value + 5 WHERE id = 1"
            )

            # Read as DataFrame
            df = await cdf.read_cassandra_table(
                f"test_dataframe.{test_table_name}", session=session
            )

            pdf = df.compute()

            # Verify counter value
            assert len(pdf) == 1
            assert pdf.iloc[0]["id"] == 1
            assert pdf.iloc[0]["count_value"] == 15

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    @pytest.mark.asyncio
    async def test_static_columns(self, session, test_table_name):
        """
        Test static column handling.

        Static columns are shared across all rows in a partition.
        """
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                partition_id INT,
                cluster_id INT,
                static_data TEXT STATIC,
                regular_data TEXT,
                PRIMARY KEY (partition_id, cluster_id)
            )
            """
        )

        try:
            # Insert data with static column
            await session.execute(
                f"""
                INSERT INTO {test_table_name}
                (partition_id, cluster_id, static_data, regular_data)
                VALUES (1, 1, 'shared_static', 'regular_1')
                """
            )
            await session.execute(
                f"""
                INSERT INTO {test_table_name}
                (partition_id, cluster_id, regular_data)
                VALUES (1, 2, 'regular_2')
                """
            )

            # Read as DataFrame
            df = await cdf.read_cassandra_table(
                f"test_dataframe.{test_table_name}", session=session
            )

            pdf = df.compute()
            pdf = pdf.sort_values(["partition_id", "cluster_id"]).reset_index(drop=True)

            # Both rows should have same static value
            assert len(pdf) == 2
            assert pdf.iloc[0]["static_data"] == "shared_static"
            assert pdf.iloc[1]["static_data"] == "shared_static"
            assert pdf.iloc[0]["regular_data"] == "regular_1"
            assert pdf.iloc[1]["regular_data"] == "regular_2"

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    @pytest.mark.asyncio
    async def test_frozen_types(self, session, test_table_name):
        """
        Test frozen collection types.

        Frozen types can be used in primary keys.
        """
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                id INT,
                frozen_list FROZEN<LIST<TEXT>>,
                frozen_set FROZEN<SET<INT>>,
                frozen_map FROZEN<MAP<TEXT, INT>>,
                PRIMARY KEY (id, frozen_list)
            )
            """
        )

        try:
            # Insert data with frozen collections
            await session.execute(
                f"""
                INSERT INTO {test_table_name}
                (id, frozen_list, frozen_set, frozen_map)
                VALUES (1, ['a', 'b'], {{1, 2}}, {{'x': 10}})
                """
            )

            # Read as DataFrame
            df = await cdf.read_cassandra_table(
                f"test_dataframe.{test_table_name}", session=session
            )

            pdf = df.compute()

            # Verify frozen collections
            assert len(pdf) == 1
            row = pdf.iloc[0]
            assert row["frozen_list"] == ["a", "b"]
            assert set(row["frozen_set"]) == {1, 2}
            assert row["frozen_map"] == {"x": 10}

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    @pytest.mark.asyncio
    async def test_nested_collections(self, session, test_table_name):
        """
        Test nested collection types.

        Cassandra supports collections within collections.
        """
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                id INT PRIMARY KEY,
                list_of_lists LIST<FROZEN<LIST<TEXT>>>,
                map_of_sets MAP<TEXT, FROZEN<SET<INT>>>,
                complex_type MAP<TEXT, FROZEN<LIST<FROZEN<SET<INT>>>>>
            )
            """
        )

        try:
            # Insert nested data
            await session.execute(
                f"""
                INSERT INTO {test_table_name}
                (id, list_of_lists, map_of_sets, complex_type)
                VALUES (
                    1,
                    [['a', 'b'], ['c', 'd']],
                    {{'set1': {{1, 2}}, 'set2': {{3, 4}}}},
                    {{'key1': [{{1, 2}}, {{3, 4}}]}}
                )
                """
            )

            # Read as DataFrame
            df = await cdf.read_cassandra_table(
                f"test_dataframe.{test_table_name}", session=session
            )

            pdf = df.compute()

            # Verify nested structures preserved
            assert len(pdf) == 1
            row = pdf.iloc[0]

            assert row["list_of_lists"] == [["a", "b"], ["c", "d"]]
            assert row["map_of_sets"]["set1"] == [1, 2]  # Sets → lists
            assert row["map_of_sets"]["set2"] == [3, 4]

            # Complex nested type
            assert len(row["complex_type"]["key1"]) == 2
            assert set(row["complex_type"]["key1"][0]) == {1, 2}

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")
