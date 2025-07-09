"""Unit tests for Cassandra to Iceberg schema mapping.

What this tests:
---------------
1. CQL type to Iceberg type conversions
2. Collection type handling (list, set, map)
3. Field ID assignment
4. Primary key handling (required vs nullable)

Why this matters:
----------------
- Schema mapping is critical for data integrity
- Type mismatches can cause data loss
- Field IDs enable schema evolution
- Nullability affects query semantics
"""

import unittest
from unittest.mock import Mock

from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    StringType,
    TimestamptzType,
)

from bulk_operations.iceberg.schema_mapper import CassandraToIcebergSchemaMapper


class TestCassandraToIcebergSchemaMapper(unittest.TestCase):
    """Test schema mapping from Cassandra to Iceberg."""

    def setUp(self):
        """Set up test fixtures."""
        self.mapper = CassandraToIcebergSchemaMapper()

    def test_simple_type_mappings(self):
        """
        Test mapping of simple CQL types to Iceberg types.

        What this tests:
        ---------------
        1. String types (text, ascii, varchar)
        2. Numeric types (int, bigint, float, double)
        3. Boolean type
        4. Binary type (blob)

        Why this matters:
        ----------------
        - Ensures basic data types are preserved
        - Critical for data integrity
        - Foundation for complex types
        """
        test_cases = [
            # String types
            ("text", StringType),
            ("ascii", StringType),
            ("varchar", StringType),
            # Integer types
            ("tinyint", IntegerType),
            ("smallint", IntegerType),
            ("int", IntegerType),
            ("bigint", LongType),
            ("counter", LongType),
            # Floating point
            ("float", FloatType),
            ("double", DoubleType),
            # Other types
            ("boolean", BooleanType),
            ("blob", BinaryType),
            ("date", DateType),
            ("timestamp", TimestamptzType),
            ("uuid", StringType),
            ("timeuuid", StringType),
            ("inet", StringType),
        ]

        for cql_type, expected_type in test_cases:
            with self.subTest(cql_type=cql_type):
                result = self.mapper._map_cql_type(cql_type)
                self.assertIsInstance(result, expected_type)

    def test_decimal_type_mapping(self):
        """
        Test decimal and varint type mappings.

        What this tests:
        ---------------
        1. Decimal type with default precision
        2. Varint as decimal with 0 scale

        Why this matters:
        ----------------
        - Financial data requires exact decimal representation
        - Varint needs appropriate precision
        """
        # Decimal
        decimal_type = self.mapper._map_cql_type("decimal")
        self.assertIsInstance(decimal_type, DecimalType)
        self.assertEqual(decimal_type.precision, 38)
        self.assertEqual(decimal_type.scale, 10)

        # Varint (arbitrary precision integer)
        varint_type = self.mapper._map_cql_type("varint")
        self.assertIsInstance(varint_type, DecimalType)
        self.assertEqual(varint_type.precision, 38)
        self.assertEqual(varint_type.scale, 0)

    def test_collection_type_mappings(self):
        """
        Test mapping of collection types.

        What this tests:
        ---------------
        1. List type with element type
        2. Set type (becomes list in Iceberg)
        3. Map type with key and value types

        Why this matters:
        ----------------
        - Collections are common in Cassandra
        - Iceberg has no native set type
        - Nested types need proper handling
        """
        # List<text>
        list_type = self.mapper._map_cql_type("list<text>")
        self.assertIsInstance(list_type, ListType)
        self.assertIsInstance(list_type.element_type, StringType)
        self.assertFalse(list_type.element_required)

        # Set<int> (becomes List in Iceberg)
        set_type = self.mapper._map_cql_type("set<int>")
        self.assertIsInstance(set_type, ListType)
        self.assertIsInstance(set_type.element_type, IntegerType)

        # Map<text, double>
        map_type = self.mapper._map_cql_type("map<text, double>")
        self.assertIsInstance(map_type, MapType)
        self.assertIsInstance(map_type.key_type, StringType)
        self.assertIsInstance(map_type.value_type, DoubleType)
        self.assertFalse(map_type.value_required)

    def test_nested_collection_types(self):
        """
        Test mapping of nested collection types.

        What this tests:
        ---------------
        1. List<list<int>>
        2. Map<text, list<double>>

        Why this matters:
        ----------------
        - Cassandra supports nested collections
        - Complex data structures need proper mapping
        """
        # List<list<int>>
        nested_list = self.mapper._map_cql_type("list<list<int>>")
        self.assertIsInstance(nested_list, ListType)
        self.assertIsInstance(nested_list.element_type, ListType)
        self.assertIsInstance(nested_list.element_type.element_type, IntegerType)

        # Map<text, list<double>>
        nested_map = self.mapper._map_cql_type("map<text, list<double>>")
        self.assertIsInstance(nested_map, MapType)
        self.assertIsInstance(nested_map.key_type, StringType)
        self.assertIsInstance(nested_map.value_type, ListType)
        self.assertIsInstance(nested_map.value_type.element_type, DoubleType)

    def test_frozen_type_handling(self):
        """
        Test handling of frozen collections.

        What this tests:
        ---------------
        1. Frozen<list<text>>
        2. Frozen types are unwrapped

        Why this matters:
        ----------------
        - Frozen is a Cassandra concept not in Iceberg
        - Inner type should be preserved
        """
        frozen_list = self.mapper._map_cql_type("frozen<list<text>>")
        self.assertIsInstance(frozen_list, ListType)
        self.assertIsInstance(frozen_list.element_type, StringType)

    def test_field_id_assignment(self):
        """
        Test unique field ID assignment.

        What this tests:
        ---------------
        1. Sequential field IDs
        2. Unique IDs for nested fields
        3. ID counter reset

        Why this matters:
        ----------------
        - Field IDs enable schema evolution
        - Must be unique within schema
        - IDs are permanent for a field
        """
        # Reset counter
        self.mapper.reset_field_ids()

        # Create mock column metadata
        col1 = Mock()
        col1.cql_type = "text"
        col1.is_primary_key = True

        col2 = Mock()
        col2.cql_type = "int"
        col2.is_primary_key = False

        col3 = Mock()
        col3.cql_type = "list<text>"
        col3.is_primary_key = False

        # Map columns
        field1 = self.mapper._map_column("id", col1)
        field2 = self.mapper._map_column("value", col2)
        field3 = self.mapper._map_column("tags", col3)

        # Check field IDs
        self.assertEqual(field1.field_id, 1)
        self.assertEqual(field2.field_id, 2)
        self.assertEqual(field3.field_id, 4)  # ID 3 was used for list element

        # List type should have element ID too
        self.assertEqual(field3.field_type.element_id, 3)

    def test_primary_key_required_fields(self):
        """
        Test that primary key columns are marked as required.

        What this tests:
        ---------------
        1. Primary key columns are required (not null)
        2. Non-primary columns are nullable

        Why this matters:
        ----------------
        - Primary keys cannot be null in Cassandra
        - Affects Iceberg query semantics
        - Important for data validation
        """
        # Primary key column
        pk_col = Mock()
        pk_col.cql_type = "text"
        pk_col.is_primary_key = True

        pk_field = self.mapper._map_column("id", pk_col)
        self.assertTrue(pk_field.required)

        # Regular column
        reg_col = Mock()
        reg_col.cql_type = "text"
        reg_col.is_primary_key = False

        reg_field = self.mapper._map_column("name", reg_col)
        self.assertFalse(reg_field.required)

    def test_table_schema_mapping(self):
        """
        Test mapping of complete table schema.

        What this tests:
        ---------------
        1. Multiple columns mapped correctly
        2. Schema contains all fields
        3. Field order preserved

        Why this matters:
        ----------------
        - Complete schema mapping is the main use case
        - All columns must be included
        - Order affects data files
        """
        # Mock table metadata
        table_meta = Mock()

        # Mock columns
        id_col = Mock()
        id_col.cql_type = "uuid"
        id_col.is_primary_key = True

        name_col = Mock()
        name_col.cql_type = "text"
        name_col.is_primary_key = False

        tags_col = Mock()
        tags_col.cql_type = "set<text>"
        tags_col.is_primary_key = False

        table_meta.columns = {
            "id": id_col,
            "name": name_col,
            "tags": tags_col,
        }

        # Map schema
        schema = self.mapper.map_table_schema(table_meta)

        # Verify schema
        self.assertEqual(len(schema.fields), 3)

        # Check field names and types
        field_names = [f.name for f in schema.fields]
        self.assertEqual(field_names, ["id", "name", "tags"])

        # Check types
        self.assertIsInstance(schema.fields[0].field_type, StringType)
        self.assertIsInstance(schema.fields[1].field_type, StringType)
        self.assertIsInstance(schema.fields[2].field_type, ListType)

    def test_unknown_type_fallback(self):
        """
        Test that unknown types fall back to string.

        What this tests:
        ---------------
        1. Unknown CQL types become strings
        2. No exceptions thrown

        Why this matters:
        ----------------
        - Future Cassandra versions may add types
        - Graceful degradation is better than failure
        """
        unknown_type = self.mapper._map_cql_type("future_type")
        self.assertIsInstance(unknown_type, StringType)

    def test_time_type_mapping(self):
        """
        Test time type mapping.

        What this tests:
        ---------------
        1. Time type maps to LongType
        2. Represents nanoseconds since midnight

        Why this matters:
        ----------------
        - Time representation differs between systems
        - Precision must be preserved
        """
        time_type = self.mapper._map_cql_type("time")
        self.assertIsInstance(time_type, LongType)


if __name__ == "__main__":
    unittest.main()
