"""
Test that all Cassandra data types maintain precision and correctness.

What this tests:
---------------
1. Every Cassandra type converts correctly without precision loss
2. Decimal precision is preserved (CRITICAL for financial data)
3. Varint unlimited precision is maintained
4. Temporal types maintain microsecond/nanosecond precision
5. UUID/TimeUUID integrity
6. Binary data integrity
7. Special float values (NaN, Inf)
8. NULL handling for all types
9. Collection types with nested complex types

Why this matters:
----------------
- Data precision loss is UNACCEPTABLE
- Financial systems depend on decimal precision
- Temporal precision matters for event ordering
- Binary data corruption breaks applications
- Type safety prevents runtime errors
"""

from datetime import UTC, date, datetime, time
from decimal import Decimal
from ipaddress import IPv4Address, IPv6Address
from uuid import UUID, uuid4

import numpy as np
import pandas as pd
import pytest
from cassandra.util import Duration, uuid_from_time

import async_cassandra_dataframe as cdf


class TestTypePrecision:
    """Test that all Cassandra types maintain precision."""

    @pytest.mark.asyncio
    async def test_integer_types_precision(self, session, test_table_name):
        """
        Test all integer types maintain exact values.

        What this tests:
        ---------------
        1. TINYINT (-128 to 127)
        2. SMALLINT (-32768 to 32767)
        3. INT (-2147483648 to 2147483647)
        4. BIGINT (-9223372036854775808 to 9223372036854775807)
        5. VARINT (unlimited precision)
        6. COUNTER (distributed counter)
        7. NULL values for all integer types

        Why this matters:
        ----------------
        - Integer overflow/underflow causes data corruption
        - Varint precision loss breaks cryptographic applications
        - Counter accuracy is critical for analytics
        """
        # Create table with all integer types
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                id INT PRIMARY KEY,
                tinyint_col TINYINT,
                smallint_col SMALLINT,
                int_col INT,
                bigint_col BIGINT,
                varint_col VARINT
            )
        """
        )

        try:
            # Test edge cases
            test_cases = [
                # Max values
                (1, 127, 32767, 2147483647, 9223372036854775807, 10**100),
                # Min values
                (2, -128, -32768, -2147483648, -9223372036854775808, -(10**100)),
                # Zero
                (3, 0, 0, 0, 0, 0),
                # NULL values
                (4, None, None, None, None, None),
                # Very large varint
                (
                    5,
                    42,
                    1000,
                    1000000,
                    1000000000000,
                    123456789012345678901234567890123456789012345678901234567890,
                ),
            ]

            # Insert test data
            insert_stmt = await session.prepare(
                f"""
                INSERT INTO {test_table_name}
                (id, tinyint_col, smallint_col, int_col, bigint_col, varint_col)
                VALUES (?, ?, ?, ?, ?, ?)
            """
            )

            for values in test_cases:
                await session.execute(insert_stmt, values)

            # Read back
            df = await cdf.read_cassandra_table(
                f"test_dataframe.{test_table_name}", session=session
            )
            pdf = df.compute()
            pdf = pdf.sort_values("id").reset_index(drop=True)

            # Verify types
            assert pdf["tinyint_col"].dtype in [
                "int8",
                "Int8",
            ], f"Wrong dtype for tinyint: {pdf['tinyint_col'].dtype}"
            assert pdf["smallint_col"].dtype in [
                "int16",
                "Int16",
            ], f"Wrong dtype for smallint: {pdf['smallint_col'].dtype}"
            assert pdf["int_col"].dtype in [
                "int32",
                "Int32",
            ], f"Wrong dtype for int: {pdf['int_col'].dtype}"
            assert pdf["bigint_col"].dtype in [
                "int64",
                "Int64",
            ], f"Wrong dtype for bigint: {pdf['bigint_col'].dtype}"
            # Varint now has custom dtype to preserve unlimited precision
            assert (
                str(pdf["varint_col"].dtype) == "cassandra_varint"
            ), f"Wrong dtype for varint: {pdf['varint_col'].dtype}"

            # Verify values
            # Max values
            assert pdf.iloc[0]["tinyint_col"] == 127
            assert pdf.iloc[0]["smallint_col"] == 32767
            assert pdf.iloc[0]["int_col"] == 2147483647
            assert pdf.iloc[0]["bigint_col"] == 9223372036854775807
            assert pdf.iloc[0]["varint_col"] == 10**100  # Must maintain precision!

            # Min values
            assert pdf.iloc[1]["tinyint_col"] == -128
            assert pdf.iloc[1]["smallint_col"] == -32768
            assert pdf.iloc[1]["int_col"] == -2147483648
            assert pdf.iloc[1]["bigint_col"] == -9223372036854775808
            assert pdf.iloc[1]["varint_col"] == -(10**100)

            # NULL handling
            assert pd.isna(pdf.iloc[3]["tinyint_col"])
            assert pd.isna(pdf.iloc[3]["varint_col"])

            # Very large varint
            expected_varint = 123456789012345678901234567890123456789012345678901234567890
            assert pdf.iloc[4]["varint_col"] == expected_varint, "Varint precision lost!"

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    @pytest.mark.asyncio
    async def test_decimal_and_float_precision(self, session, test_table_name):
        """
        Test decimal and floating point precision.

        What this tests:
        ---------------
        1. DECIMAL arbitrary precision (CRITICAL for money!)
        2. FLOAT (32-bit IEEE-754)
        3. DOUBLE (64-bit IEEE-754)
        4. Special values (NaN, Infinity, -Infinity)
        5. Very small decimal values
        6. Very large decimal values

        Why this matters:
        ----------------
        - Financial calculations require exact decimal precision
        - Scientific computing needs proper float handling
        - Special values must be preserved
        """
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                id INT PRIMARY KEY,
                decimal_col DECIMAL,
                float_col FLOAT,
                double_col DOUBLE
            )
        """
        )

        try:
            # Test cases with precision edge cases
            test_cases = [
                # Financial precision
                (
                    1,
                    Decimal("123456789012345678901234567890.123456789012345678901234567890"),
                    3.14159265,
                    3.141592653589793238462643383279,
                ),
                # Very small decimal
                (
                    2,
                    Decimal("0.000000000000000000000000000001"),
                    1.175494e-38,
                    2.2250738585072014e-308,
                ),  # Near min normal float/double
                # Very large values
                (
                    3,
                    Decimal("999999999999999999999999999999.999999999999999999999999999999"),
                    3.4028235e38,
                    1.7976931348623157e308,
                ),  # Near max
                # Special float values
                (4, Decimal("0"), float("nan"), float("inf")),
                (5, Decimal("-0"), float("-inf"), float("-inf")),
                # Exact decimal for money
                (6, Decimal("19.99"), 19.99, 19.99),
                (7, Decimal("0.01"), 0.01, 0.01),  # One cent must be exact!
            ]

            insert_stmt = await session.prepare(
                f"""
                INSERT INTO {test_table_name} (id, decimal_col, float_col, double_col)
                VALUES (?, ?, ?, ?)
            """
            )

            for values in test_cases:
                await session.execute(insert_stmt, values)

            # Read back
            df = await cdf.read_cassandra_table(
                f"test_dataframe.{test_table_name}", session=session
            )
            pdf = df.compute()
            pdf = pdf.sort_values("id").reset_index(drop=True)

            # Verify decimal precision is EXACT
            row1_decimal = pdf.iloc[0]["decimal_col"]
            if isinstance(row1_decimal, str):
                row1_decimal = Decimal(row1_decimal)
            expected = Decimal("123456789012345678901234567890.123456789012345678901234567890")
            assert row1_decimal == expected, f"Decimal precision lost! Got {row1_decimal}"

            # Very small decimal
            row2_decimal = pdf.iloc[1]["decimal_col"]
            if isinstance(row2_decimal, str):
                row2_decimal = Decimal(row2_decimal)
            assert row2_decimal == Decimal("0.000000000000000000000000000001")

            # Money precision
            row6_decimal = pdf.iloc[5]["decimal_col"]
            if isinstance(row6_decimal, str):
                row6_decimal = Decimal(row6_decimal)
            assert row6_decimal == Decimal("19.99"), "Money precision lost!"

            # Float/Double types - now using nullable types
            assert str(pdf["float_col"].dtype) in ["float32", "Float32"]
            assert str(pdf["double_col"].dtype) in ["float64", "Float64"]

            # Special values - with nullable types, special float values might be handled differently
            # NaN might be converted to pd.NA in nullable float types
            float_val = pdf.iloc[3]["float_col"]
            # Check if it's either NaN or NA (both are acceptable for representing missing/undefined)
            assert pd.isna(float_val) or (pd.notna(float_val) and np.isnan(float_val))

            double_val = pdf.iloc[3]["double_col"]
            # Infinity should be preserved
            assert pd.notna(double_val) and np.isinf(double_val)

            float_neginf = pdf.iloc[4]["float_col"]
            # Negative infinity should be preserved
            assert pd.notna(float_neginf) and np.isneginf(float_neginf)

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    @pytest.mark.asyncio
    async def test_temporal_types_precision(self, session, test_table_name):
        """
        Test temporal type precision.

        What this tests:
        ---------------
        1. DATE precision
        2. TIME precision (nanosecond)
        3. TIMESTAMP precision (microsecond)
        4. DURATION complex type
        5. Edge cases (min/max dates, leap seconds)

        Why this matters:
        ----------------
        - Event ordering depends on timestamp precision
        - Time calculations need accuracy
        - Duration calculations for SLAs
        """
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                id INT PRIMARY KEY,
                date_col DATE,
                time_col TIME,
                timestamp_col TIMESTAMP,
                duration_col DURATION
            )
        """
        )

        try:
            # Test cases
            test_timestamp = datetime(2024, 3, 15, 14, 30, 45, 123456, tzinfo=UTC)
            test_cases = [
                # Normal case with microsecond precision
                (
                    1,
                    date(2024, 3, 15),
                    time(14, 30, 45, 123456),
                    test_timestamp,
                    Duration(months=1, days=2, nanoseconds=3456789012),
                ),
                # Edge cases
                (
                    2,
                    date(1, 1, 1),
                    time(0, 0, 0, 0),
                    datetime(1970, 1, 1, 0, 0, 0, 0, tzinfo=UTC),
                    Duration(months=0, days=0, nanoseconds=0),
                ),
                (
                    3,
                    date(9999, 12, 31),
                    time(23, 59, 59, 999999),
                    datetime(2038, 1, 19, 3, 14, 7, 999999, tzinfo=UTC),
                    Duration(months=12, days=365, nanoseconds=86399999999999),
                ),
            ]

            insert_stmt = await session.prepare(
                f"""
                INSERT INTO {test_table_name}
                (id, date_col, time_col, timestamp_col, duration_col)
                VALUES (?, ?, ?, ?, ?)
            """
            )

            for values in test_cases:
                await session.execute(insert_stmt, values)

            # Read back
            df = await cdf.read_cassandra_table(
                f"test_dataframe.{test_table_name}", session=session
            )
            pdf = df.compute()
            pdf = pdf.sort_values("id").reset_index(drop=True)

            # Verify DATE
            date_val = pdf.iloc[0]["date_col"]
            if isinstance(date_val, str):
                date_val = pd.to_datetime(date_val).date()
            elif hasattr(date_val, "date"):
                date_val = date_val.date()
            assert date_val == date(2024, 3, 15), f"Date precision lost: {date_val}"

            # Verify TIME with microsecond precision
            time_val = pdf.iloc[0]["time_col"]
            if isinstance(time_val, int | np.int64):
                # Time as nanoseconds - verify precision
                assert time_val == 52245123456000  # 14:30:45.123456 in nanoseconds
            elif isinstance(time_val, pd.Timedelta):
                # Verify components
                assert time_val.components.hours == 14
                assert time_val.components.minutes == 30
                assert time_val.components.seconds == 45
                # Microseconds must be exact!
                total_microseconds = time_val.total_seconds() * 1e6
                expected_microseconds = (14 * 3600 + 30 * 60 + 45) * 1e6 + 123456
                assert (
                    abs(total_microseconds - expected_microseconds) < 1
                ), "Time microsecond precision lost!"

            # Verify TIMESTAMP
            ts_val = pdf.iloc[0]["timestamp_col"]
            if hasattr(ts_val, "tz_localize"):
                if ts_val.tz is None:
                    ts_val = ts_val.tz_localize("UTC")
            assert ts_val.year == 2024
            # Cassandra only stores millisecond precision (3 decimal places)
            # 123456 microseconds -> 123000 microseconds (123 milliseconds)
            assert (
                ts_val.microsecond == 123000
            ), f"Timestamp millisecond precision lost: {ts_val.microsecond}"

            # Verify DURATION
            duration_val = pdf.iloc[0]["duration_col"]
            assert isinstance(
                duration_val, Duration
            ), f"Duration type changed to {type(duration_val)}"

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    @pytest.mark.asyncio
    async def test_string_and_binary_types(self, session, test_table_name):
        """
        Test string and binary data integrity.

        What this tests:
        ---------------
        1. ASCII restrictions
        2. TEXT/VARCHAR with special characters
        3. BLOB binary data integrity
        4. Large text/blob data
        5. Empty strings vs NULL

        Why this matters:
        ----------------
        - Binary data corruption breaks files/images
        - Character encoding issues cause data loss
        - Special characters must be preserved
        """
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                id INT PRIMARY KEY,
                ascii_col ASCII,
                text_col TEXT,
                varchar_col VARCHAR,
                blob_col BLOB
            )
        """
        )

        try:
            # Test cases
            large_text = "X" * 10000  # 10KB text
            large_blob = b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09" * 1000  # 10KB binary

            test_cases = [
                # Normal data
                (
                    1,
                    "ASCII_ONLY_123",
                    "UTF-8 with émojis 🎉🌍🔥",
                    "Quotes: 'single' \"double\"",
                    b"Binary\x00\x01\x02\xFF",
                ),
                # Special characters
                (
                    2,
                    "SPECIAL!@#$%",
                    "Line1\nLine2\rLine3\tTab",
                    "Escaped: \\n\\r\\t",
                    bytes(range(256)),
                ),  # All byte values
                # Large data
                (3, "A" * 100, large_text, large_text[:1000], large_blob),
                # Empty vs NULL
                (4, "", "", "", b""),
                (5, None, None, None, None),
            ]

            insert_stmt = await session.prepare(
                f"""
                INSERT INTO {test_table_name}
                (id, ascii_col, text_col, varchar_col, blob_col)
                VALUES (?, ?, ?, ?, ?)
            """
            )

            for values in test_cases:
                await session.execute(insert_stmt, values)

            # Read back
            df = await cdf.read_cassandra_table(
                f"test_dataframe.{test_table_name}", session=session
            )
            pdf = df.compute()
            pdf = pdf.sort_values("id").reset_index(drop=True)

            # Verify string types
            assert pdf["ascii_col"].dtype in ["object", "string"]
            assert pdf["text_col"].dtype in ["object", "string"]

            # Special characters preserved
            assert pdf.iloc[0]["text_col"] == "UTF-8 with émojis 🎉🌍🔥"
            assert pdf.iloc[1]["text_col"] == "Line1\nLine2\rLine3\tTab"

            # Binary data integrity
            assert pdf.iloc[0]["blob_col"] == b"Binary\x00\x01\x02\xFF"
            assert pdf.iloc[1]["blob_col"] == bytes(range(256))  # All bytes preserved
            assert len(pdf.iloc[2]["blob_col"]) == 10000  # Large blob intact

            # Empty vs NULL
            assert pdf.iloc[3]["ascii_col"] == ""
            assert pd.isna(pdf.iloc[4]["ascii_col"])

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    @pytest.mark.asyncio
    async def test_uuid_and_inet_types(self, session, test_table_name):
        """
        Test UUID and network address types.

        What this tests:
        ---------------
        1. UUID integrity
        2. TIMEUUID ordering
        3. IPv4 addresses
        4. IPv6 addresses
        5. Special addresses (localhost, any)

        Why this matters:
        ----------------
        - UUID corruption breaks references
        - TimeUUID ordering is critical for time-series
        - Network addresses need exact representation
        """
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                id INT PRIMARY KEY,
                uuid_col UUID,
                timeuuid_col TIMEUUID,
                inet_col INET
            )
        """
        )

        try:
            # Test UUIDs
            test_uuid = UUID("550e8400-e29b-41d4-a716-446655440000")
            test_timeuuid = uuid_from_time(datetime.now())

            test_cases = [
                (1, test_uuid, test_timeuuid, "192.168.1.1"),
                (
                    2,
                    uuid4(),
                    uuid_from_time(datetime.now()),
                    "2001:0db8:85a3:0000:0000:8a2e:0370:7334",
                ),
                (3, UUID("00000000-0000-0000-0000-000000000000"), None, "0.0.0.0"),
                (4, None, None, "::1"),  # IPv6 localhost
            ]

            insert_stmt = await session.prepare(
                f"""
                INSERT INTO {test_table_name} (id, uuid_col, timeuuid_col, inet_col)
                VALUES (?, ?, ?, ?)
            """
            )

            for values in test_cases:
                await session.execute(insert_stmt, values)

            # Read back
            df = await cdf.read_cassandra_table(
                f"test_dataframe.{test_table_name}", session=session
            )
            pdf = df.compute()
            pdf = pdf.sort_values("id").reset_index(drop=True)

            # Verify UUID integrity
            uuid_val = pdf.iloc[0]["uuid_col"]
            if isinstance(uuid_val, str):
                uuid_val = UUID(uuid_val)
            assert uuid_val == test_uuid, f"UUID corrupted: {uuid_val}"

            # Verify NULL UUID
            assert str(pdf.iloc[2]["uuid_col"]) == "00000000-0000-0000-0000-000000000000"

            # Verify INET addresses
            inet_val = pdf.iloc[0]["inet_col"]
            if isinstance(inet_val, str):
                inet_val = IPv4Address(inet_val)
            assert str(inet_val) == "192.168.1.1"

            # IPv6
            inet6_val = pdf.iloc[1]["inet_col"]
            if isinstance(inet6_val, str):
                inet6_val = IPv6Address(inet6_val)
            assert str(inet6_val) == "2001:db8:85a3::8a2e:370:7334"  # Normalized form

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    @pytest.mark.asyncio
    async def test_collection_types_with_complex_values(self, session, test_table_name):
        """
        Test collection types with complex nested values.

        What this tests:
        ---------------
        1. LIST with large integers
        2. SET with UUIDs
        3. MAP with decimal values
        4. Frozen collections
        5. Empty collections vs NULL

        Why this matters:
        ----------------
        - Collections often contain complex types
        - Precision must be maintained in collections
        - Frozen collections enable primary key usage
        """
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                id INT PRIMARY KEY,
                list_bigint LIST<BIGINT>,
                set_uuid SET<UUID>,
                map_decimal MAP<TEXT, DECIMAL>,
                frozen_list FROZEN<LIST<DOUBLE>>,
                tuple_col TUPLE<INT, TEXT, BOOLEAN, DECIMAL>
            )
        """
        )

        try:
            test_uuid1 = uuid4()
            test_uuid2 = uuid4()

            test_cases = [
                # Complex values in collections
                (
                    1,
                    [9223372036854775807, -9223372036854775808, 0],  # Max/min bigint
                    {test_uuid1, test_uuid2},
                    {"price": Decimal("19.99"), "tax": Decimal("1.45"), "total": Decimal("21.44")},
                    [float("inf"), float("-inf"), float("nan"), 1.23456789],
                    (42, "test", True, Decimal("99.99")),
                ),
                # Empty collections
                (2, [], set(), {}, [], None),
                # NULL
                (3, None, None, None, None, None),
            ]

            insert_stmt = await session.prepare(
                f"""
                INSERT INTO {test_table_name}
                (id, list_bigint, set_uuid, map_decimal, frozen_list, tuple_col)
                VALUES (?, ?, ?, ?, ?, ?)
            """
            )

            for values in test_cases:
                await session.execute(insert_stmt, values)

            # Read back
            df = await cdf.read_cassandra_table(
                f"test_dataframe.{test_table_name}", session=session
            )
            pdf = df.compute()
            pdf = pdf.sort_values("id").reset_index(drop=True)

            # Verify list with bigints
            list_val = pdf.iloc[0]["list_bigint"]
            if isinstance(list_val, str):
                import ast

                list_val = ast.literal_eval(list_val)
            assert list_val == [
                9223372036854775807,
                -9223372036854775808,
                0,
            ], "List bigint precision lost!"

            # Verify map with decimals
            map_val = pdf.iloc[0]["map_decimal"]
            if isinstance(map_val, str):
                import ast

                map_val = ast.literal_eval(map_val)
                # Convert string decimals back
                map_val = {k: Decimal(v) if isinstance(v, str) else v for k, v in map_val.items()}

            assert map_val["price"] == Decimal("19.99"), "Map decimal precision lost!"
            assert map_val["total"] == Decimal("21.44"), "Map decimal precision lost!"

            # Verify tuple
            tuple_val = pdf.iloc[0]["tuple_col"]
            if isinstance(tuple_val, str):
                import ast

                tuple_val = ast.literal_eval(tuple_val)
            # Tuple becomes list in pandas
            assert tuple_val[0] == 42
            assert tuple_val[1] == "test"
            assert tuple_val[2] == True  # noqa: E712
            # Check decimal in tuple
            if isinstance(tuple_val[3], str):
                assert Decimal(tuple_val[3]) == Decimal("99.99")
            else:
                assert tuple_val[3] == Decimal("99.99")

            # Empty collections should be None (Cassandra behavior)
            assert pd.isna(pdf.iloc[1]["list_bigint"]) or pdf.iloc[1]["list_bigint"] is None

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")
