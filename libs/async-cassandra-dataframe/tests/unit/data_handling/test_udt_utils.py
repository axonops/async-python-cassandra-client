"""
Unit tests for User Defined Type (UDT) utilities.

What this tests:
---------------
1. UDT serialization/deserialization
2. DataFrame preparation for Dask
3. UDT column detection
4. Handling of nested UDTs and collections

Why this matters:
----------------
- Dask converts dicts to strings during transport
- UDTs need special handling to preserve structure
- Correct detection prevents data corruption
"""

import json

import pandas as pd

from async_cassandra_dataframe.udt_utils import (
    deserialize_udt_from_dask,
    detect_udt_columns,
    prepare_dataframe_for_dask,
    restore_udts_in_dataframe,
    serialize_udt_for_dask,
)


class TestUDTSerialization:
    """Test UDT serialization/deserialization."""

    def test_serialize_simple_udt(self):
        """Test serializing a simple UDT dict."""
        udt = {"field1": "value1", "field2": 123}

        result = serialize_udt_for_dask(udt)

        assert result.startswith("__UDT__")
        assert json.loads(result[7:]) == udt

    def test_serialize_nested_udt(self):
        """Test serializing nested UDT structures."""
        udt = {"name": "John", "address": {"street": "123 Main St", "city": "Springfield"}}

        result = serialize_udt_for_dask(udt)

        assert result.startswith("__UDT__")
        assert json.loads(result[7:]) == udt

    def test_serialize_list_of_udts(self):
        """Test serializing a list of UDT dicts."""
        udts = [{"id": 1, "name": "Item1"}, {"id": 2, "name": "Item2"}]

        result = serialize_udt_for_dask(udts)

        assert result.startswith("__UDT_LIST__")
        assert json.loads(result[12:]) == udts

    def test_serialize_non_udt_value(self):
        """Test serializing non-UDT values."""
        # String should pass through
        assert serialize_udt_for_dask("hello") == "hello"

        # Number should pass through
        assert serialize_udt_for_dask(123) == 123

        # None should pass through
        assert serialize_udt_for_dask(None) is None

    def test_deserialize_simple_udt(self):
        """Test deserializing a simple UDT."""
        udt = {"field1": "value1", "field2": 123}
        serialized = f"__UDT__{json.dumps(udt)}"

        result = deserialize_udt_from_dask(serialized)

        assert result == udt

    def test_deserialize_list_of_udts(self):
        """Test deserializing a list of UDTs."""
        udts = [{"id": 1, "name": "Item1"}, {"id": 2, "name": "Item2"}]
        serialized = f"__UDT_LIST__{json.dumps(udts)}"

        result = deserialize_udt_from_dask(serialized)

        assert result == udts

    def test_deserialize_legacy_dict_string(self):
        """Test deserializing legacy dict-like strings."""
        # Dask sometimes converts dicts to string representation
        dict_str = "{'field1': 'value1', 'field2': 123}"

        result = deserialize_udt_from_dask(dict_str)

        assert result == {"field1": "value1", "field2": 123}

    def test_deserialize_non_udt_value(self):
        """Test deserializing non-UDT values."""
        # Regular string
        assert deserialize_udt_from_dask("hello") == "hello"

        # Number
        assert deserialize_udt_from_dask(123) == 123

        # None
        assert deserialize_udt_from_dask(None) is None

        # Invalid dict string
        assert deserialize_udt_from_dask("{invalid}") == "{invalid}"

    def test_round_trip_serialization(self):
        """Test round-trip serialization/deserialization."""
        test_cases = [
            {"simple": "udt"},
            {"nested": {"inner": "value"}},
            [{"id": 1}, {"id": 2}],
            {"mixed": [1, 2, {"inner": "dict"}]},
        ]

        for original in test_cases:
            serialized = serialize_udt_for_dask(original)
            deserialized = deserialize_udt_from_dask(serialized)
            assert deserialized == original


class TestDataFrameOperations:
    """Test DataFrame UDT operations."""

    def test_prepare_dataframe_for_dask(self):
        """Test preparing DataFrame with UDT columns for Dask."""
        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["A", "B", "C"],
                "metadata": [
                    {"type": "regular", "priority": 1},
                    {"type": "special", "priority": 2},
                    {"type": "regular", "priority": 3},
                ],
                "tags": [[{"tag": "red"}, {"tag": "blue"}], [{"tag": "green"}], []],
            }
        )

        udt_columns = ["metadata", "tags"]
        result = prepare_dataframe_for_dask(df, udt_columns)

        # Original DataFrame should be unchanged
        assert isinstance(df["metadata"].iloc[0], dict)

        # Result should have serialized columns
        assert result["metadata"].iloc[0].startswith("__UDT__")
        assert result["tags"].iloc[0].startswith("__UDT_LIST__")
        assert result["tags"].iloc[2] == "__UDT_LIST__[]"  # Empty list

        # Non-UDT columns should be unchanged
        assert result["id"].equals(df["id"])
        assert result["name"].equals(df["name"])

    def test_restore_udts_in_dataframe(self):
        """Test restoring UDTs in DataFrame after Dask."""
        # Create DataFrame with serialized UDTs
        df = pd.DataFrame(
            {
                "id": [1, 2],
                "metadata": [
                    '__UDT__{"type": "regular", "priority": 1}',
                    '__UDT__{"type": "special", "priority": 2}',
                ],
                "tags": [
                    '__UDT_LIST__[{"tag": "red"}, {"tag": "blue"}]',
                    '__UDT_LIST__[{"tag": "green"}]',
                ],
            }
        )

        udt_columns = ["metadata", "tags"]
        result = restore_udts_in_dataframe(df.copy(), udt_columns)

        # Check restored values
        assert result["metadata"].iloc[0] == {"type": "regular", "priority": 1}
        assert result["metadata"].iloc[1] == {"type": "special", "priority": 2}
        assert result["tags"].iloc[0] == [{"tag": "red"}, {"tag": "blue"}]
        assert result["tags"].iloc[1] == [{"tag": "green"}]

    def test_prepare_restore_round_trip(self):
        """Test complete round trip of prepare and restore."""
        original = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "user_data": [
                    {"name": "Alice", "age": 30},
                    {"name": "Bob", "age": 25},
                    {"name": "Charlie", "age": 35},
                ],
                "settings": [
                    {"theme": "dark", "notifications": True},
                    {"theme": "light", "notifications": False},
                    {"theme": "auto", "notifications": True},
                ],
            }
        )

        udt_columns = ["user_data", "settings"]

        # Prepare for Dask
        prepared = prepare_dataframe_for_dask(original, udt_columns)

        # Simulate Dask processing (nothing changes in this test)

        # Restore UDTs
        restored = restore_udts_in_dataframe(prepared, udt_columns)

        # Should match original
        pd.testing.assert_frame_equal(original, restored)

    def test_handle_missing_columns(self):
        """Test handling when UDT columns don't exist in DataFrame."""
        df = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})

        # Try to process non-existent columns
        udt_columns = ["metadata", "settings"]

        # Should not raise error
        prepared = prepare_dataframe_for_dask(df, udt_columns)
        restored = restore_udts_in_dataframe(prepared, udt_columns)

        # Should be unchanged
        pd.testing.assert_frame_equal(df, restored)


class TestUDTDetection:
    """Test UDT column detection from metadata."""

    def test_detect_frozen_udt(self):
        """Test detecting frozen UDT columns."""
        metadata = {
            "columns": [
                {"name": "id", "type": "int"},
                {"name": "address", "type": "frozen<address_type>"},
                {"name": "name", "type": "text"},
            ]
        }

        result = detect_udt_columns(metadata)

        assert result == ["address"]

    def test_detect_non_frozen_udt(self):
        """Test detecting non-frozen UDT columns."""
        metadata = {
            "columns": [
                {"name": "id", "type": "int"},
                {"name": "profile", "type": "user_profile"},  # Custom type
                {"name": "settings", "type": "app_settings"},  # Custom type
            ]
        }

        result = detect_udt_columns(metadata)

        assert sorted(result) == ["profile", "settings"]

    def test_detect_collections_with_udts(self):
        """Test detecting collections containing UDTs."""
        metadata = {
            "columns": [
                {"name": "id", "type": "int"},
                {"name": "addresses", "type": "list<frozen<address_type>>"},
                {"name": "metadata", "type": "map<text, frozen<meta_type>>"},
                {"name": "tags", "type": "set<text>"},  # Not a UDT
            ]
        }

        result = detect_udt_columns(metadata)

        assert sorted(result) == ["addresses", "metadata"]

    def test_ignore_primitive_types(self):
        """Test that primitive types are not detected as UDTs."""
        metadata = {
            "columns": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "text"},
                {"name": "age", "type": "bigint"},
                {"name": "created", "type": "timestamp"},
                {"name": "active", "type": "boolean"},
                {"name": "balance", "type": "decimal"},
                {"name": "data", "type": "blob"},
                {"name": "ip", "type": "inet"},
                {"name": "uid", "type": "uuid"},
                {"name": "version", "type": "varint"},
            ]
        }

        result = detect_udt_columns(metadata)

        assert result == []

    def test_frozen_collections_detected(self):
        """Test that frozen collections are detected (current behavior)."""
        metadata = {
            "columns": [
                {"name": "id", "type": "int"},
                {"name": "tags", "type": "frozen<list<text>>"},
                {"name": "scores", "type": "frozen<set<int>>"},
                {"name": "config", "type": "frozen<map<text, text>>"},
                {"name": "point", "type": "frozen<tuple<double, double>>"},
            ]
        }

        result = detect_udt_columns(metadata)

        # Current implementation detects any type with "frozen<"
        # This might include collections that don't actually contain UDTs
        assert sorted(result) == ["config", "point", "scores", "tags"]

    def test_complex_nested_types(self):
        """Test complex nested type detection."""
        metadata = {
            "columns": [
                {"name": "id", "type": "int"},
                {"name": "nested", "type": "map<text, list<frozen<custom_type>>>"},
                {"name": "simple_map", "type": "map<text, int>"},  # No UDT
                {"name": "udt_set", "type": "set<frozen<another_type>>"},
            ]
        }

        result = detect_udt_columns(metadata)

        assert sorted(result) == ["nested", "udt_set"]

    def test_empty_metadata(self):
        """Test handling empty metadata."""
        assert detect_udt_columns({}) == []
        assert detect_udt_columns({"columns": []}) == []
