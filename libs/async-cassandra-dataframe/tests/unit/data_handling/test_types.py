"""
Unit tests for Cassandra type mapping.

Tests type conversions, NULL handling, and edge cases.
"""

from datetime import UTC, date, datetime, time
from decimal import Decimal

import pandas as pd
import pytest
from cassandra.util import Date, Time

from async_cassandra_dataframe.types import CassandraTypeMapper


class TestCassandraTypeMapper:
    """Test type mapping functionality."""

    @pytest.fixture
    def mapper(self):
        """Create type mapper instance."""
        return CassandraTypeMapper()

    def test_basic_type_mapping(self, mapper):
        """Test basic type mappings."""
        # String types - Using nullable string dtype
        assert mapper.get_pandas_dtype("text") == "string"
        assert mapper.get_pandas_dtype("varchar") == "string"
        assert mapper.get_pandas_dtype("ascii") == "string"

        # Numeric types - Using nullable dtypes
        assert mapper.get_pandas_dtype("int") == "Int32"
        assert mapper.get_pandas_dtype("bigint") == "Int64"
        assert mapper.get_pandas_dtype("smallint") == "Int16"
        assert mapper.get_pandas_dtype("tinyint") == "Int8"
        assert mapper.get_pandas_dtype("float") == "Float32"
        assert mapper.get_pandas_dtype("double") == "Float64"
        assert (
            str(mapper.get_pandas_dtype("decimal")) == "cassandra_decimal"
        )  # Custom dtype for precision
        assert (
            str(mapper.get_pandas_dtype("varint")) == "cassandra_varint"
        )  # Custom dtype for unlimited precision
        assert mapper.get_pandas_dtype("counter") == "Int64"

        # Temporal types
        assert mapper.get_pandas_dtype("timestamp") == "datetime64[ns, UTC]"
        assert (
            str(mapper.get_pandas_dtype("date")) == "cassandra_date"
        )  # Custom dtype for full date range
        assert mapper.get_pandas_dtype("time") == "timedelta64[ns]"
        assert str(mapper.get_pandas_dtype("duration")) == "cassandra_duration"  # Custom dtype

        # Other types
        assert mapper.get_pandas_dtype("boolean") == "boolean"  # Nullable boolean
        assert str(mapper.get_pandas_dtype("uuid")) == "cassandra_uuid"
        assert (
            str(mapper.get_pandas_dtype("timeuuid")) == "cassandra_timeuuid"
        )  # Separate from UUID
        assert str(mapper.get_pandas_dtype("inet")) == "cassandra_inet"
        assert mapper.get_pandas_dtype("blob") == "object"

    def test_collection_type_mapping(self, mapper):
        """Test collection type mappings."""
        assert mapper.get_pandas_dtype("list<text>") == "object"
        assert mapper.get_pandas_dtype("set<int>") == "object"
        assert mapper.get_pandas_dtype("map<text,int>") == "object"
        assert mapper.get_pandas_dtype("frozen<list<text>>") == "object"

    def test_null_value_conversion(self, mapper):
        """Test NULL value handling."""
        # NULL values should remain None
        assert mapper.convert_value(None, "text") is None
        assert mapper.convert_value(None, "int") is None
        assert mapper.convert_value(None, "list<text>") is None

    def test_empty_collection_to_null(self, mapper):
        """
        Test empty collection conversion to NULL.

        CRITICAL: Cassandra stores empty collections as NULL.
        """
        # Empty collections should become None
        assert mapper.convert_value([], "list<text>") is None
        assert mapper.convert_value(set(), "set<int>") is None
        assert mapper.convert_value({}, "map<text,int>") is None
        assert mapper.convert_value((), "tuple<text,int>") is None

        # Non-empty collections should be preserved
        assert mapper.convert_value(["a", "b"], "list<text>") == ["a", "b"]
        assert mapper.convert_value({1, 2}, "set<int>") == [1, 2]  # Sets → lists
        assert mapper.convert_value({"a": 1}, "map<text,int>") == {"a": 1}

    def test_decimal_precision_preservation(self, mapper):
        """
        Test decimal precision is preserved.

        CRITICAL: Must not lose precision by converting to float.
        """
        decimal_value = Decimal("123.456789012345678901234567890")
        result = mapper.convert_value(decimal_value, "decimal")

        # Should still be a Decimal, not float
        assert isinstance(result, Decimal)
        assert result == decimal_value

    def test_date_conversions(self, mapper):
        """Test date type conversions."""
        # Cassandra Date → Python date object (with CassandraDateDtype)
        cass_date = Date(date(2024, 1, 15))
        result = mapper.convert_value(cass_date, "date")
        assert isinstance(result, date)
        assert result == date(2024, 1, 15)

        # Python date → stays as Python date
        py_date = date(2024, 1, 15)
        result = mapper.convert_value(py_date, "date")
        assert isinstance(result, date)
        assert result == py_date

    def test_time_conversions(self, mapper):
        """Test time type conversions."""
        # Cassandra Time → pandas Timedelta
        # Time stores nanoseconds since midnight
        cass_time = Time(10 * 3600 * 1_000_000_000 + 30 * 60 * 1_000_000_000)  # 10:30
        result = mapper.convert_value(cass_time, "time")
        assert isinstance(result, pd.Timedelta)
        assert result == pd.Timedelta(hours=10, minutes=30)

        # Python time → pandas Timedelta
        py_time = time(10, 30, 45, 123456)
        result = mapper.convert_value(py_time, "time")
        assert isinstance(result, pd.Timedelta)
        assert result == pd.Timedelta(hours=10, minutes=30, seconds=45, microseconds=123456)

    def test_timestamp_timezone_handling(self, mapper):
        """Test timestamp timezone handling."""
        # Naive datetime should get UTC
        naive_dt = datetime(2024, 1, 15, 10, 30, 45)
        result = mapper.convert_value(naive_dt, "timestamp")
        assert isinstance(result, pd.Timestamp)
        assert result.tz is not None
        assert str(result.tz) == "UTC"

        # Aware datetime should preserve timezone
        aware_dt = datetime(2024, 1, 15, 10, 30, 45, tzinfo=UTC)
        result = mapper.convert_value(aware_dt, "timestamp")
        assert isinstance(result, pd.Timestamp)
        assert result.tz is not None

    def test_writetime_conversion(self, mapper):
        """Test writetime value conversion."""
        # Writetime is microseconds since epoch
        writetime = 1705324245123456  # 2024-01-15 10:30:45.123456 UTC
        result = mapper.convert_writetime_value(writetime)

        assert isinstance(result, pd.Timestamp)
        assert result.tz is not None
        assert str(result.tz) == "UTC"
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15
        assert result.microsecond == 123456

        # NULL writetime
        assert mapper.convert_writetime_value(None) is None

    def test_ttl_conversion(self, mapper):
        """Test TTL value conversion."""
        # TTL is seconds remaining
        ttl = 3600  # 1 hour
        result = mapper.convert_ttl_value(ttl)
        assert result == 3600

        # NULL TTL (no expiry)
        assert mapper.convert_ttl_value(None) is None

    def test_create_empty_dataframe(self, mapper):
        """Test empty DataFrame creation with schema."""
        schema = {
            "id": "int32",
            "name": "object",
            "value": "float64",
            "created": "datetime64[ns]",
            "active": "bool",
        }

        df = mapper.create_empty_dataframe(schema)

        # Should be empty but have correct dtypes
        assert len(df) == 0
        assert df["id"].dtype == "int32"
        assert df["name"].dtype == "object"
        assert df["value"].dtype == "float64"
        assert pd.api.types.is_datetime64_any_dtype(df["created"])
        assert df["active"].dtype == "bool"

    def test_handle_null_values_in_dataframe(self, mapper):
        """Test NULL handling in DataFrames."""
        # Create test DataFrame
        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "list_col": [["a", "b"], [], ["c"]],
                "set_col": [{1, 2}, set(), {3}],
                "text_col": ["hello", "", None],
            }
        )

        # Mock table metadata
        table_metadata = {
            "columns": [
                {"name": "id", "type": "int"},
                {"name": "list_col", "type": "list<text>"},
                {"name": "set_col", "type": "set<int>"},
                {"name": "text_col", "type": "text"},
            ]
        }

        # Apply NULL handling
        result = mapper.handle_null_values(df.copy(), table_metadata)

        # Empty collections should become None
        assert result["list_col"].iloc[1] is None
        assert result["set_col"].iloc[1] is None

        # Empty string should NOT become None
        assert result["text_col"].iloc[1] == ""

        # Existing None should remain None
        assert result["text_col"].iloc[2] is None
