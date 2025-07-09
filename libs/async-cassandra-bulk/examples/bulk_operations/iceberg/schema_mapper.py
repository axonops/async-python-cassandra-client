"""Maps Cassandra table schemas to Iceberg schemas."""

from cassandra.metadata import ColumnMetadata, TableMetadata
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IcebergType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    TimestamptzType,
)


class CassandraToIcebergSchemaMapper:
    """Maps Cassandra table schemas to Apache Iceberg schemas.

    What this does:
    --------------
    1. Converts CQL types to Iceberg types
    2. Preserves column nullability
    3. Handles complex types (lists, sets, maps)
    4. Assigns unique field IDs for schema evolution

    Why this matters:
    ----------------
    - Enables seamless data migration from Cassandra to Iceberg
    - Preserves type information for analytics
    - Supports schema evolution in Iceberg
    - Maintains data integrity during export
    """

    def __init__(self):
        """Initialize the schema mapper."""
        self._field_id_counter = 1

    def map_table_schema(self, table_metadata: TableMetadata) -> Schema:
        """Map a Cassandra table schema to an Iceberg schema.

        Args:
            table_metadata: Cassandra table metadata

        Returns:
            Iceberg Schema object
        """
        fields = []

        # Map each column
        for column_name, column_meta in table_metadata.columns.items():
            field = self._map_column(column_name, column_meta)
            fields.append(field)

        return Schema(*fields)

    def _map_column(self, name: str, column_meta: ColumnMetadata) -> NestedField:
        """Map a single Cassandra column to an Iceberg field.

        Args:
            name: Column name
            column_meta: Cassandra column metadata

        Returns:
            Iceberg NestedField
        """
        # Get the Iceberg type
        iceberg_type = self._map_cql_type(column_meta.cql_type)

        # Create field with unique ID
        field_id = self._get_next_field_id()

        # In Cassandra, primary key columns are required (not null)
        # All other columns are nullable
        is_required = column_meta.is_primary_key

        return NestedField(
            field_id=field_id,
            name=name,
            field_type=iceberg_type,
            required=is_required,
        )

    def _map_cql_type(self, cql_type: str) -> IcebergType:
        """Map a CQL type string to an Iceberg type.

        Args:
            cql_type: CQL type string (e.g., "text", "int", "list<text>")

        Returns:
            Iceberg Type
        """
        # Handle parameterized types
        base_type = cql_type.split("<")[0].lower()

        # Simple type mappings
        type_mapping = {
            # String types
            "ascii": StringType(),
            "text": StringType(),
            "varchar": StringType(),
            # Numeric types
            "tinyint": IntegerType(),  # 8-bit in Cassandra, 32-bit in Iceberg
            "smallint": IntegerType(),  # 16-bit in Cassandra, 32-bit in Iceberg
            "int": IntegerType(),
            "bigint": LongType(),
            "counter": LongType(),
            "varint": DecimalType(38, 0),  # Arbitrary precision integer
            "decimal": DecimalType(38, 10),  # Default precision/scale
            "float": FloatType(),
            "double": DoubleType(),
            # Boolean
            "boolean": BooleanType(),
            # Date/Time types
            "date": DateType(),
            "timestamp": TimestamptzType(),  # Cassandra timestamps have timezone
            "time": LongType(),  # Time as nanoseconds since midnight
            # Binary
            "blob": BinaryType(),
            # UUID types
            "uuid": StringType(),  # Store as string for compatibility
            "timeuuid": StringType(),
            # Network
            "inet": StringType(),  # IP address as string
        }

        # Handle simple types
        if base_type in type_mapping:
            return type_mapping[base_type]

        # Handle collection types
        if base_type == "list":
            element_type = self._extract_collection_type(cql_type)
            return ListType(
                element_id=self._get_next_field_id(),
                element_type=self._map_cql_type(element_type),
                element_required=False,  # Cassandra allows null elements
            )
        elif base_type == "set":
            # Sets become lists in Iceberg (no native set type)
            element_type = self._extract_collection_type(cql_type)
            return ListType(
                element_id=self._get_next_field_id(),
                element_type=self._map_cql_type(element_type),
                element_required=False,
            )
        elif base_type == "map":
            key_type, value_type = self._extract_map_types(cql_type)
            return MapType(
                key_id=self._get_next_field_id(),
                key_type=self._map_cql_type(key_type),
                value_id=self._get_next_field_id(),
                value_type=self._map_cql_type(value_type),
                value_required=False,  # Cassandra allows null values
            )
        elif base_type == "tuple":
            # Tuples become structs in Iceberg
            # For now, we'll use a string representation
            # TODO: Implement proper tuple parsing
            return StringType()
        elif base_type == "frozen":
            # Frozen collections - strip "frozen" and process inner type
            inner_type = cql_type[7:-1]  # Remove "frozen<" and ">"
            return self._map_cql_type(inner_type)
        else:
            # Default to string for unknown types
            return StringType()

    def _extract_collection_type(self, cql_type: str) -> str:
        """Extract element type from list<type> or set<type>."""
        start = cql_type.index("<") + 1
        end = cql_type.rindex(">")
        return cql_type[start:end].strip()

    def _extract_map_types(self, cql_type: str) -> tuple[str, str]:
        """Extract key and value types from map<key_type, value_type>."""
        start = cql_type.index("<") + 1
        end = cql_type.rindex(">")
        types = cql_type[start:end].split(",", 1)
        return types[0].strip(), types[1].strip()

    def _get_next_field_id(self) -> int:
        """Get the next available field ID."""
        field_id = self._field_id_counter
        self._field_id_counter += 1
        return field_id

    def reset_field_ids(self) -> None:
        """Reset field ID counter (useful for testing)."""
        self._field_id_counter = 1
