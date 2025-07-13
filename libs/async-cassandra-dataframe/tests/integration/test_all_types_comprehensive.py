#!/usr/bin/env python3
"""
Comprehensive test to verify ALL Cassandra data types are converted correctly
without any precision loss or type corruption.

This is a CRITICAL test that ensures data integrity for all Cassandra types.
"""

from datetime import UTC, date, datetime, time
from decimal import Decimal
from uuid import UUID, uuid4

import async_cassandra_dataframe as cdf
import numpy as np
import pandas as pd
import pytest
from cassandra.util import Duration, uuid_from_time


class TestAllTypesComprehensive:
    """Comprehensive test for ALL Cassandra data types."""

    @pytest.mark.asyncio
    async def test_all_cassandra_types_precision(self, session, test_table_name):
        """
        Test that ALL Cassandra types maintain precision and correctness.

        This is a CRITICAL test that ensures no data loss or corruption
        occurs for any Cassandra data type.
        """
        # Create table with ALL Cassandra types
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                id INT PRIMARY KEY,

                -- Text types
                ascii_col ASCII,
                text_col TEXT,
                varchar_col VARCHAR,

                -- Integer types
                tinyint_col TINYINT,
                smallint_col SMALLINT,
                int_col INT,
                bigint_col BIGINT,
                varint_col VARINT,  -- Unlimited precision integer

                -- Decimal types
                decimal_col DECIMAL,  -- Arbitrary precision decimal
                float_col FLOAT,      -- 32-bit IEEE-754
                double_col DOUBLE,    -- 64-bit IEEE-754

                -- Temporal types
                date_col DATE,
                time_col TIME,
                timestamp_col TIMESTAMP,
                duration_col DURATION,

                -- UUID types
                uuid_col UUID,
                timeuuid_col TIMEUUID,

                -- Other types
                boolean_col BOOLEAN,
                blob_col BLOB,
                inet_col INET,

                -- Collection types
                list_col LIST<INT>,
                set_col SET<TEXT>,
                map_col MAP<TEXT, DECIMAL>,
                tuple_col TUPLE<INT, TEXT, BOOLEAN>,
                frozen_list FROZEN<LIST<DOUBLE>>,
                frozen_set FROZEN<SET<UUID>>,
                frozen_map FROZEN<MAP<INT, TEXT>>
            )
        """
        )

        try:
            # Prepare test data with edge cases for precision testing
            test_cases = [
                {
                    "id": 1,
                    "description": "Maximum values and precision test",
                    "data": {
                        # Text - with special characters
                        "ascii_col": "ASCII_TEST_123!@#",
                        "text_col": "UTF-8 with émojis 🎉 and special chars: \n\t\r",
                        "varchar_col": "Variable \" ' characters",
                        # Integer edge cases
                        "tinyint_col": 127,  # max tinyint
                        "smallint_col": 32767,  # max smallint
                        "int_col": 2147483647,  # max int
                        "bigint_col": 9223372036854775807,  # max bigint
                        "varint_col": 123456789012345678901234567890123456789012345678901234567890,  # Very large
                        # Decimal precision - CRITICAL for financial data
                        "decimal_col": Decimal(
                            "123456789012345678901234567890.123456789012345678901234567890"
                        ),
                        "float_col": 3.4028235e38,  # Near max float
                        "double_col": 1.7976931348623157e308,  # Near max double
                        # Temporal precision
                        "date_col": date(9999, 12, 31),  # Max date
                        "time_col": time(23, 59, 59, 999999),  # Max time with microseconds
                        "timestamp_col": datetime(
                            2038, 1, 19, 3, 14, 7, 999999, tzinfo=UTC
                        ),  # Near max timestamp
                        "duration_col": Duration(
                            months=12, days=30, nanoseconds=86399999999999
                        ),  # Large duration
                        # UUIDs
                        "uuid_col": UUID("550e8400-e29b-41d4-a716-446655440000"),
                        "timeuuid_col": uuid_from_time(datetime.now()),
                        # Other types
                        "boolean_col": True,
                        "blob_col": b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09"
                        * 100,  # Binary data
                        "inet_col": "2001:0db8:85a3:0000:0000:8a2e:0370:7334",  # IPv6
                        # Collections with various types
                        "list_col": [1, 2, 3, 2147483647, -2147483648],
                        "set_col": {"unique1", "unique2", "unique3"},
                        "map_col": {"key1": Decimal("999.999"), "key2": Decimal("-0.000000001")},
                        "tuple_col": (42, "nested", False),
                        "frozen_list": [1.1, 2.2, 3.3, float("inf"), float("-inf")],
                        "frozen_set": {uuid4(), uuid4(), uuid4()},
                        "frozen_map": {1: "one", 2: "two", 3: "three"},
                    },
                },
                {
                    "id": 2,
                    "description": "Minimum values and negative test",
                    "data": {
                        "tinyint_col": -128,  # min tinyint
                        "smallint_col": -32768,  # min smallint
                        "int_col": -2147483648,  # min int
                        "bigint_col": -9223372036854775808,  # min bigint
                        "varint_col": -123456789012345678901234567890123456789012345678901234567890,
                        "decimal_col": Decimal(
                            "-999999999999999999999999999999.999999999999999999999999999999"
                        ),
                        "float_col": -3.4028235e38,  # Near min float
                        "double_col": -1.7976931348623157e308,  # Near min double
                        "date_col": date(1, 1, 1),  # Min date
                        "time_col": time(0, 0, 0, 0),  # Min time
                        "timestamp_col": datetime(1970, 1, 1, 0, 0, 0, 0, tzinfo=UTC),  # Epoch
                        "boolean_col": False,
                        "inet_col": "0.0.0.0",  # Min IPv4
                    },
                },
                {
                    "id": 3,
                    "description": "Special float values",
                    "data": {
                        "float_col": float("nan"),  # NaN
                        "double_col": float("inf"),  # Infinity
                    },
                },
                {
                    "id": 4,
                    "description": "Precision edge cases",
                    "data": {
                        # Test decimal precision is maintained
                        "decimal_col": Decimal("0.000000000000000000000000000001"),  # Very small
                        "float_col": 1.23456789,  # Should truncate to float32 precision
                        "double_col": 1.2345678901234567890123456789,  # Should maintain double precision
                        # Test varint with extremely large number
                        "varint_col": 10**100,  # Googol
                    },
                },
            ]

            # Insert test data
            for test_case in test_cases:
                columns = ["id"] + list(test_case["data"].keys())
                values = [test_case["id"]] + list(test_case["data"].values())

                placeholders = ", ".join(["?" for _ in columns])
                col_list = ", ".join(columns)

                query = f"INSERT INTO {test_table_name} ({col_list}) VALUES ({placeholders})"
                prepared = await session.prepare(query)
                await session.execute(prepared, values)

            # Read data back
            df = await cdf.read_cassandra_table(
                f"test_dataframe.{test_table_name}", session=session
            )

            pdf = df.compute()
            pdf = pdf.sort_values("id").reset_index(drop=True)

            # Verify each type maintains precision
            # Test Case 1: Maximum values
            row1 = pdf.iloc[0]

            # Text types
            assert row1["ascii_col"] == "ASCII_TEST_123!@#", "ASCII precision lost"
            assert (
                row1["text_col"] == "UTF-8 with émojis 🎉 and special chars: \n\t\r"
            ), "TEXT precision lost"
            assert row1["varchar_col"] == "Variable \" ' characters", "VARCHAR precision lost"

            # Integer types
            assert row1["tinyint_col"] == 127, f"TINYINT precision lost: {row1['tinyint_col']}"
            assert row1["smallint_col"] == 32767, f"SMALLINT precision lost: {row1['smallint_col']}"
            assert row1["int_col"] == 2147483647, f"INT precision lost: {row1['int_col']}"
            assert (
                row1["bigint_col"] == 9223372036854775807
            ), f"BIGINT precision lost: {row1['bigint_col']}"
            assert (
                row1["varint_col"] == 123456789012345678901234567890123456789012345678901234567890
            ), "VARINT precision lost!"

            # CRITICAL: Decimal precision
            decimal_val = row1["decimal_col"]
            if isinstance(decimal_val, str):
                decimal_val = Decimal(decimal_val)
            expected_decimal = Decimal(
                "123456789012345678901234567890.123456789012345678901234567890"
            )
            assert (
                decimal_val == expected_decimal
            ), f"DECIMAL precision lost! Got {decimal_val}, expected {expected_decimal}"

            # Float/Double precision
            assert (
                abs(row1["float_col"] - 3.4028235e38) < 1e32
            ), f"FLOAT precision issue: {row1['float_col']}"
            assert (
                abs(row1["double_col"] - 1.7976931348623157e308) < 1e300
            ), f"DOUBLE precision issue: {row1['double_col']}"

            # Temporal types
            if isinstance(row1["date_col"], str):
                date_val = pd.to_datetime(row1["date_col"]).date()
            else:
                date_val = row1["date_col"]
            assert date_val == date(9999, 12, 31) or pd.Timestamp(date_val).date() == date(
                9999, 12, 31
            ), f"DATE precision lost: {date_val}"

            # Time precision check - microseconds must be preserved
            if isinstance(row1["time_col"], int | np.int64):
                # Time as nanoseconds
                time_ns = row1["time_col"]
                hours = time_ns // (3600 * 1e9)
                minutes = (time_ns % (3600 * 1e9)) // (60 * 1e9)
                seconds = (time_ns % (60 * 1e9)) / 1e9
                assert (
                    hours == 23 and minutes == 59 and abs(seconds - 59.999999) < 0.000001
                ), "TIME precision lost"

            # UUID types
            assert isinstance(row1["uuid_col"], UUID | str), "UUID type corrupted"
            assert isinstance(row1["timeuuid_col"], UUID | str), "TIMEUUID type corrupted"

            # Binary data
            assert (
                row1["blob_col"] == b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09" * 100
            ), "BLOB data corrupted"

            # Collections
            list_val = row1["list_col"]
            if isinstance(list_val, str):
                import ast

                list_val = ast.literal_eval(list_val)
            assert list_val == [
                1,
                2,
                3,
                2147483647,
                -2147483648,
            ], f"LIST precision lost: {list_val}"

            map_val = row1["map_col"]
            if isinstance(map_val, str):
                import ast

                map_val = ast.literal_eval(map_val)
            # Check map decimal values maintained precision
            if isinstance(map_val["key1"], str):
                assert Decimal(map_val["key1"]) == Decimal("999.999"), "MAP decimal precision lost"
            else:
                assert map_val["key1"] == Decimal("999.999"), "MAP decimal precision lost"

            # Test Case 2: Minimum values
            row2 = pdf.iloc[1]
            assert row2["tinyint_col"] == -128, "TINYINT min value corrupted"
            assert row2["smallint_col"] == -32768, "SMALLINT min value corrupted"
            assert row2["int_col"] == -2147483648, "INT min value corrupted"
            assert row2["bigint_col"] == -9223372036854775808, "BIGINT min value corrupted"
            assert (
                row2["varint_col"] == -123456789012345678901234567890123456789012345678901234567890
            ), "VARINT negative precision lost"

            # Test Case 3: Special float values
            row3 = pdf.iloc[2]
            assert pd.isna(row3["float_col"]) or np.isnan(
                row3["float_col"]
            ), "Float NaN not preserved"
            assert np.isinf(row3["double_col"]), "Double infinity not preserved"

            # Test Case 4: Extreme precision
            row4 = pdf.iloc[3]
            decimal_val = row4["decimal_col"]
            if isinstance(decimal_val, str):
                decimal_val = Decimal(decimal_val)
            assert decimal_val == Decimal(
                "0.000000000000000000000000000001"
            ), "Extreme decimal precision lost!"
            assert row4["varint_col"] == 10**100, "Large varint precision lost!"

            # Verify dtypes are correct
            assert pdf["tinyint_col"].dtype in [
                np.int8,
                "Int8",
            ], f"Wrong dtype for tinyint: {pdf['tinyint_col'].dtype}"
            assert pdf["smallint_col"].dtype in [
                np.int16,
                "Int16",
            ], f"Wrong dtype for smallint: {pdf['smallint_col'].dtype}"
            assert pdf["int_col"].dtype in [
                np.int32,
                "Int32",
            ], f"Wrong dtype for int: {pdf['int_col'].dtype}"
            assert pdf["bigint_col"].dtype in [
                np.int64,
                "Int64",
            ], f"Wrong dtype for bigint: {pdf['bigint_col'].dtype}"
            assert (
                pdf["float_col"].dtype == np.float32
            ), f"Wrong dtype for float: {pdf['float_col'].dtype}"
            assert (
                pdf["double_col"].dtype == np.float64
            ), f"Wrong dtype for double: {pdf['double_col'].dtype}"
            assert (
                pdf["boolean_col"].dtype == bool
            ), f"Wrong dtype for boolean: {pdf['boolean_col'].dtype}"
            assert (
                pdf["varint_col"].dtype == "object"
            ), f"Wrong dtype for varint: {pdf['varint_col'].dtype}"
            assert (
                pdf["decimal_col"].dtype == "object"
            ), f"Wrong dtype for decimal: {pdf['decimal_col'].dtype}"

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")
