"""
Unit tests for Cassandra to pandas type conversion.

What this tests:
---------------
1. Numeric type conversions (int, float, decimal)
2. Date/time type conversions
3. UUID and network type conversions
4. Collection type handling
5. Precision preservation for decimal/varint

Why this matters:
----------------
- Prevent data loss during type conversion
- Ensure correct pandas dtypes
- Handle null values properly
- Preserve precision for financial data
"""

from datetime import date, datetime, time
from decimal import Decimal
from ipaddress import IPv4Address, IPv6Address
from uuid import UUID

import pandas as pd
from cassandra.util import Date, Time

from async_cassandra_dataframe.type_converter import DataFrameTypeConverter


class TestNumericConversions:
    """Test numeric type conversions."""

    def test_convert_tinyint(self):
        """Test tinyint conversion to Int8."""
        df = pd.DataFrame({"value": [1, 127, -128, None, 0]})
        metadata = {"columns": [{"name": "value", "type": "tinyint"}]}

        result = DataFrameTypeConverter.convert_dataframe_types(df, metadata, None)

        assert result["value"].dtype == "Int8"
        assert result["value"].iloc[0] == 1
        assert result["value"].iloc[1] == 127
        assert result["value"].iloc[2] == -128
        assert pd.isna(result["value"].iloc[3])

    def test_convert_smallint(self):
        """Test smallint conversion to Int16."""
        df = pd.DataFrame({"value": [100, 32767, -32768, None]})
        metadata = {"columns": [{"name": "value", "type": "smallint"}]}

        result = DataFrameTypeConverter.convert_dataframe_types(df, metadata, None)

        assert result["value"].dtype == "Int16"
        assert result["value"].iloc[0] == 100
        assert result["value"].iloc[1] == 32767

    def test_convert_int(self):
        """Test int conversion to Int32."""
        df = pd.DataFrame({"value": [1000, 2147483647, -2147483648, None]})
        metadata = {"columns": [{"name": "value", "type": "int"}]}

        result = DataFrameTypeConverter.convert_dataframe_types(df, metadata, None)

        assert result["value"].dtype == "Int32"
        assert result["value"].iloc[0] == 1000

    def test_convert_bigint(self):
        """Test bigint conversion to Int64."""
        df = pd.DataFrame({"value": [1000000, 9223372036854775807, None]})
        metadata = {"columns": [{"name": "value", "type": "bigint"}]}

        result = DataFrameTypeConverter.convert_dataframe_types(df, metadata, None)

        assert result["value"].dtype == "Int64"
        assert result["value"].iloc[0] == 1000000

    def test_convert_counter(self):
        """Test counter type conversion to Int64."""
        df = pd.DataFrame({"count": [100, 200, 300]})
        metadata = {"columns": [{"name": "count", "type": "counter"}]}

        result = DataFrameTypeConverter.convert_dataframe_types(df, metadata, None)

        assert result["count"].dtype == "Int64"

    def test_convert_float(self):
        """Test float conversion to float32."""
        df = pd.DataFrame({"value": [1.5, 3.14159, -0.001, None]})
        metadata = {"columns": [{"name": "value", "type": "float"}]}

        result = DataFrameTypeConverter.convert_dataframe_types(df, metadata, None)

        assert result["value"].dtype == "float32"
        assert abs(result["value"].iloc[0] - 1.5) < 0.0001
        assert pd.isna(result["value"].iloc[3])

    def test_convert_double(self):
        """Test double conversion to float64."""
        df = pd.DataFrame({"value": [1.5e100, 3.141592653589793, None]})
        metadata = {"columns": [{"name": "value", "type": "double"}]}

        result = DataFrameTypeConverter.convert_dataframe_types(df, metadata, None)

        assert result["value"].dtype == "float64"
        assert result["value"].iloc[0] == 1.5e100

    def test_convert_decimal(self):
        """Test decimal conversion preserving precision."""
        df = pd.DataFrame(
            {"amount": [Decimal("123.45"), Decimal("999999999999.999999"), Decimal("-0.01"), None]}
        )
        metadata = {"columns": [{"name": "amount", "type": "decimal"}]}

        result = DataFrameTypeConverter.convert_dataframe_types(df, metadata, None)

        # Should keep as object dtype to preserve Decimal
        assert result["amount"].dtype == "object"
        assert isinstance(result["amount"].iloc[0], Decimal)
        assert result["amount"].iloc[0] == Decimal("123.45")
        assert result["amount"].iloc[1] == Decimal("999999999999.999999")

    def test_convert_varint(self):
        """Test varint conversion preserving unlimited precision."""
        df = pd.DataFrame(
            {
                "value": [
                    123,
                    12345678901234567890123456789012345678901234567890,  # Very large int
                    -999999999999999999999999999999999999999999999999,
                    None,
                ]
            }
        )
        metadata = {"columns": [{"name": "value", "type": "varint"}]}

        result = DataFrameTypeConverter.convert_dataframe_types(df, metadata, None)

        # Should keep as object dtype for unlimited precision
        assert result["value"].dtype == "object"
        assert result["value"].iloc[0] == 123
        assert result["value"].iloc[1] == 12345678901234567890123456789012345678901234567890


class TestDateTimeConversions:
    """Test date/time type conversions."""

    def test_convert_date(self):
        """Test date conversion."""
        df = pd.DataFrame(
            {"event_date": [Date(18628), date(2021, 1, 1), None]}  # Cassandra Date object
        )
        metadata = {"columns": [{"name": "event_date", "type": "date"}]}

        result = DataFrameTypeConverter.convert_dataframe_types(df, metadata, None)

        # Should convert to pandas datetime64
        assert pd.api.types.is_datetime64_dtype(result["event_date"])
        assert pd.isna(result["event_date"].iloc[2])

    def test_convert_time(self):
        """Test time conversion to Timedelta."""
        df = pd.DataFrame(
            {
                "event_time": [
                    Time(37845000000000),  # Cassandra Time in nanoseconds (10:30:45)
                    time(10, 30, 45),
                    None,
                ]
            }
        )
        metadata = {"columns": [{"name": "event_time", "type": "time"}]}

        result = DataFrameTypeConverter.convert_dataframe_types(df, metadata, None)

        # Time values should be converted to Timedelta
        assert isinstance(result["event_time"].iloc[0], pd.Timedelta)
        assert result["event_time"].iloc[0] == pd.Timedelta(hours=10, minutes=30, seconds=45)
        assert result["event_time"].iloc[1] == pd.Timedelta(hours=10, minutes=30, seconds=45)
        assert pd.isna(result["event_time"].iloc[2])

    def test_convert_timestamp(self):
        """Test timestamp conversion with timezone."""
        df = pd.DataFrame({"created_at": [datetime(2021, 1, 1, 12, 0, 0), datetime.now(), None]})
        metadata = {"columns": [{"name": "created_at", "type": "timestamp"}]}

        result = DataFrameTypeConverter.convert_dataframe_types(df, metadata, None)

        # Should convert to datetime64 with UTC timezone
        assert isinstance(result["created_at"].dtype, pd.DatetimeTZDtype)
        assert result["created_at"].iloc[0] == pd.Timestamp("2021-01-01 12:00:00", tz="UTC")
        assert str(result["created_at"].dt.tz) == "UTC"


class TestUUIDAndNetworkTypes:
    """Test UUID and network type conversions."""

    def test_convert_uuid(self):
        """Test UUID conversion."""
        uuid1 = UUID("550e8400-e29b-41d4-a716-446655440000")
        uuid2 = UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")

        df = pd.DataFrame({"id": [uuid1, uuid2, None]})
        metadata = {"columns": [{"name": "id", "type": "uuid"}]}

        result = DataFrameTypeConverter.convert_dataframe_types(df, metadata, None)

        # Should keep as object dtype with UUID objects
        assert result["id"].dtype == "object"
        assert isinstance(result["id"].iloc[0], UUID)
        assert result["id"].iloc[0] == uuid1

    def test_convert_timeuuid(self):
        """Test timeuuid conversion."""
        uuid1 = UUID("550e8400-e29b-11eb-a716-446655440000")  # Time-based UUID

        df = pd.DataFrame({"event_id": [uuid1, None]})
        metadata = {"columns": [{"name": "event_id", "type": "timeuuid"}]}

        result = DataFrameTypeConverter.convert_dataframe_types(df, metadata, None)

        assert result["event_id"].dtype == "object"
        assert isinstance(result["event_id"].iloc[0], UUID)

    def test_convert_inet(self):
        """Test inet (IP address) conversion."""
        df = pd.DataFrame(
            {
                "ip_address": [
                    IPv4Address("192.168.1.1"),
                    IPv6Address("2001:db8::1"),
                    "10.0.0.1",  # String representation
                    None,
                ]
            }
        )
        metadata = {"columns": [{"name": "ip_address", "type": "inet"}]}

        result = DataFrameTypeConverter.convert_dataframe_types(df, metadata, None)

        # Should handle various IP formats
        assert result["ip_address"].dtype == "object"


class TestStringAndBinaryTypes:
    """Test string and binary type conversions."""

    def test_convert_text_types(self):
        """Test text, varchar, ascii conversions."""
        df = pd.DataFrame(
            {
                "name": ["Alice", "Bob", None],
                "email": ["alice@example.com", "bob@example.com", ""],
                "code": ["ABC123", "XYZ789", None],
            }
        )
        metadata = {
            "columns": [
                {"name": "name", "type": "text"},
                {"name": "email", "type": "varchar"},
                {"name": "code", "type": "ascii"},
            ]
        }

        result = DataFrameTypeConverter.convert_dataframe_types(df, metadata, None)

        # All should be string type
        assert result["name"].dtype == "string"
        assert result["email"].dtype == "string"
        assert result["code"].dtype == "string"
        assert pd.isna(result["name"].iloc[2])

    def test_convert_blob(self):
        """Test blob (binary) conversion."""
        df = pd.DataFrame({"data": [b"binary data", bytes([0x00, 0x01, 0x02, 0xFF]), None]})
        metadata = {"columns": [{"name": "data", "type": "blob"}]}

        result = DataFrameTypeConverter.convert_dataframe_types(df, metadata, None)

        # Should preserve bytes
        assert result["data"].dtype == "object"
        assert isinstance(result["data"].iloc[0], bytes)
        assert result["data"].iloc[0] == b"binary data"


class TestCollectionTypes:
    """Test collection type conversions."""

    def test_skip_writetime_ttl_columns(self):
        """Test that writetime and TTL columns are skipped."""
        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["A", "B", "C"],
                "name_writetime": [1234567890, 1234567891, 1234567892],
                "name_ttl": [3600, 7200, 10800],
            }
        )
        metadata = {"columns": [{"name": "id", "type": "int"}, {"name": "name", "type": "text"}]}

        result = DataFrameTypeConverter.convert_dataframe_types(df, metadata, None)

        # Regular columns converted
        assert result["id"].dtype == "Int32"
        assert result["name"].dtype == "string"

        # Writetime/TTL columns unchanged
        assert result["name_writetime"].dtype == df["name_writetime"].dtype
        assert result["name_ttl"].dtype == df["name_ttl"].dtype

    def test_empty_dataframe(self):
        """Test conversion of empty DataFrame."""
        df = pd.DataFrame()
        metadata = {"columns": [{"name": "id", "type": "int"}]}

        result = DataFrameTypeConverter.convert_dataframe_types(df, metadata, None)

        assert result.empty
        assert result.equals(df)

    def test_unknown_column(self):
        """Test handling of columns not in metadata."""
        df = pd.DataFrame({"id": [1, 2, 3], "unknown_col": ["A", "B", "C"]})
        metadata = {"columns": [{"name": "id", "type": "int"}]}

        result = DataFrameTypeConverter.convert_dataframe_types(df, metadata, None)

        # Known column converted
        assert result["id"].dtype == "Int32"

        # Unknown column unchanged
        assert result["unknown_col"].dtype == df["unknown_col"].dtype


class TestHelperMethods:
    """Test internal helper methods."""

    def test_convert_varint_helper(self):
        """Test _convert_varint helper method."""
        # Normal int
        assert DataFrameTypeConverter._convert_varint(123) == 123

        # Large int
        large_int = 12345678901234567890
        assert DataFrameTypeConverter._convert_varint(large_int) == large_int

        # String representation
        assert DataFrameTypeConverter._convert_varint("999") == 999

        # None
        assert DataFrameTypeConverter._convert_varint(None) is None

    def test_convert_decimal_helper(self):
        """Test _convert_decimal helper method."""
        # Decimal object
        dec = Decimal("123.45")
        assert DataFrameTypeConverter._convert_decimal(dec) == dec

        # String representation
        assert DataFrameTypeConverter._convert_decimal("999.99") == Decimal("999.99")

        # None
        assert DataFrameTypeConverter._convert_decimal(None) is None

    def test_ensure_bytes_helper(self):
        """Test _ensure_bytes helper method."""
        # Already bytes
        assert DataFrameTypeConverter._ensure_bytes(b"test") == b"test"

        # String to bytes
        assert DataFrameTypeConverter._ensure_bytes("test") == b"test"

        # None
        assert DataFrameTypeConverter._ensure_bytes(None) is None

    def test_convert_date_helper(self):
        """Test _convert_date helper method."""
        # Date object
        d = date(2021, 1, 1)
        result = DataFrameTypeConverter._convert_date(d)
        assert isinstance(result, pd.Timestamp)

        # Cassandra Date object
        cassandra_date = Date(18628)  # Days since epoch
        result = DataFrameTypeConverter._convert_date(cassandra_date)
        assert isinstance(result, pd.Timestamp)

        # None
        assert pd.isna(DataFrameTypeConverter._convert_date(None))

    def test_convert_time_helper(self):
        """Test _convert_time helper method."""
        # Time object converts to Timedelta
        t = time(10, 30, 45)
        result = DataFrameTypeConverter._convert_time(t)
        assert isinstance(result, pd.Timedelta)
        assert result == pd.Timedelta(hours=10, minutes=30, seconds=45)

        # Cassandra Time object (nanoseconds since midnight)
        cassandra_time = Time(37845000000000)  # 10:30:45
        result = DataFrameTypeConverter._convert_time(cassandra_time)
        assert isinstance(result, pd.Timedelta)
        assert result == pd.Timedelta(nanoseconds=37845000000000)

        # None
        assert pd.isna(DataFrameTypeConverter._convert_time(None))

    def test_convert_to_int_helper(self):
        """Test _convert_to_int helper method."""
        series = pd.Series([1, 2, None, 4])

        # Convert to Int32
        result = DataFrameTypeConverter._convert_to_int(series, "Int32")
        assert result.dtype == "Int32"
        assert pd.isna(result.iloc[2])

        # Convert with string numbers
        series_str = pd.Series(["1", "2", None, "4"])
        result = DataFrameTypeConverter._convert_to_int(series_str, "Int64")
        assert result.dtype == "Int64"
        assert result.iloc[0] == 1
