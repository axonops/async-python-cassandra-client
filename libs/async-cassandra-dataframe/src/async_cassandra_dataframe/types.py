"""
Cassandra to Pandas type mapping with comprehensive support for all types.

Critical component that handles all type conversions including edge cases
discovered during async-cassandra-bulk development.
"""

from datetime import date, datetime, time
from typing import Any

import numpy as np
import pandas as pd
from cassandra.util import Date, Time


class CassandraTypeMapper:
    """
    Maps Cassandra types to pandas dtypes with special handling for:
    - Precision preservation (decimals, timestamps)
    - NULL semantics (empty collections → NULL)
    - Special types (duration, counter)
    - Writetime/TTL values
    """

    # Basic type mapping
    BASIC_TYPE_MAP = {
        # String types
        "ascii": "object",
        "text": "object",
        "varchar": "object",
        # Numeric types - preserve precision!
        "tinyint": "int8",
        "smallint": "int16",
        "int": "int32",
        "bigint": "int64",
        "varint": "object",  # Python int, unlimited precision
        "float": "float32",
        "double": "float64",
        "decimal": "object",  # Keep as Decimal for precision
        "counter": "int64",
        # Temporal types
        "date": "datetime64[ns]",
        "time": "timedelta64[ns]",
        "timestamp": "datetime64[ns, UTC]",
        "duration": "object",  # Special Cassandra type
        # Binary
        "blob": "object",  # bytes
        # Other types
        "boolean": "bool",
        "inet": "object",  # IP address
        "uuid": "object",  # UUID object
        "timeuuid": "object",  # TimeUUID object
        # Collection types - always object
        "list": "object",
        "set": "object",
        "map": "object",
        "tuple": "object",
        "frozen": "object",
        # Vector type (Cassandra 5.0+)
        "vector": "object",  # List of floats
    }

    # Types that need special NULL handling
    COLLECTION_TYPES = {"list", "set", "map", "tuple", "frozen", "vector"}

    # Types that cannot have writetime
    NO_WRITETIME_TYPES = {"counter"}

    def __init__(self):
        """Initialize type mapper."""
        self._dtype_cache: dict[str, np.dtype] = {}

    def get_pandas_dtype(self, cassandra_type: str) -> str | np.dtype:
        """
        Get pandas dtype for Cassandra type.

        Args:
            cassandra_type: CQL type name

        Returns:
            Pandas dtype string or numpy dtype
        """
        # Normalize type name
        base_type = self._extract_base_type(cassandra_type)

        # Check cache
        if base_type in self._dtype_cache:
            return self._dtype_cache[base_type]

        # Get dtype
        dtype = self.BASIC_TYPE_MAP.get(base_type, "object")

        # Cache and return
        self._dtype_cache[base_type] = dtype
        return dtype

    def _extract_base_type(self, type_str: str) -> str:
        """Extract base type from complex type string."""
        # Handle frozen types
        if type_str.startswith("frozen<"):
            return "frozen"

        # Handle parameterized types
        if "<" in type_str:
            return type_str.split("<")[0]

        return type_str

    def convert_value(self, value: Any, cassandra_type: str) -> Any:
        """
        Convert Cassandra value to appropriate pandas value.

        CRITICAL: Handle NULL semantics correctly!
        - Empty collections → None (Cassandra stores as NULL)
        - Explicit None → None
        - Preserve precision for decimals and timestamps
        """
        # NULL handling
        if value is None:
            return None

        base_type = self._extract_base_type(cassandra_type)

        # Collection NULL handling - CRITICAL!
        if base_type in self.COLLECTION_TYPES:
            # Empty collections are stored as NULL in Cassandra
            if self._is_empty_collection(value):
                return None
            # Convert sets to lists for pandas compatibility
            if isinstance(value, set):
                return list(value)
            return value

        # Special type handling
        if base_type == "decimal":
            # Keep as Decimal - DO NOT convert to float!
            return value

        elif base_type == "date":
            # Cassandra Date to pandas datetime
            if isinstance(value, Date):
                # Date.date() returns datetime.date
                return pd.Timestamp(value.date())
            elif isinstance(value, date):
                return pd.Timestamp(value)
            return value

        elif base_type == "time":
            # Cassandra Time to pandas timedelta
            if isinstance(value, Time):
                # Convert nanoseconds to timedelta
                return pd.Timedelta(nanoseconds=value.nanosecond_time)
            elif isinstance(value, time):
                # Convert time to timedelta from midnight
                return pd.Timedelta(
                    hours=value.hour,
                    minutes=value.minute,
                    seconds=value.second,
                    microseconds=value.microsecond,
                )
            return value

        elif base_type == "timestamp":
            # Ensure datetime has timezone info
            if isinstance(value, datetime) and value.tzinfo is None:
                # Cassandra timestamps are UTC
                return pd.Timestamp(value, tz="UTC")
            return pd.Timestamp(value)

        elif base_type == "duration":
            # Keep as Duration object - special handling needed
            return value

        # Handle UDTs (User Defined Types)
        # UDTs come as named tuple-like objects
        if hasattr(value, "_fields") and hasattr(value, "_asdict"):
            # Convert UDT to dictionary
            return value._asdict()

        # Check if it's a string representation of a dict/UDT
        if isinstance(value, str):
            # Check if it looks like a dict representation
            if value.startswith("{") and value.endswith("}"):
                try:
                    # Try to safely evaluate the dict string
                    import ast

                    return ast.literal_eval(value)
                except (ValueError, SyntaxError):
                    # If parsing fails, return as-is
                    pass

            # Check for old-style UDT string representation
            if cassandra_type and value.startswith(cassandra_type + "("):
                # This is a string representation, try to parse it
                import warnings

                warnings.warn(
                    f"UDT {cassandra_type} returned as string: {value}. "
                    "This may indicate a driver version issue.",
                    RuntimeWarning,
                    stacklevel=2,
                )
                return value

        # Default - return as is
        return value

    def _is_empty_collection(self, value: Any) -> bool:
        """Check if value is an empty collection."""
        if value is None:
            return False

        # Check various collection types
        if isinstance(value, list | set | tuple | dict):
            return len(value) == 0

        # Check for other collection-like objects
        try:
            return len(value) == 0
        except (TypeError, AttributeError):
            return False

    def convert_writetime_value(self, value: int | None) -> pd.Timestamp | None:
        """
        Convert writetime value to pandas Timestamp.

        Writetime is microseconds since epoch.
        Returns None for NULL values (correct Cassandra behavior).
        """
        if value is None:
            return None

        # Convert microseconds to timestamp
        # CRITICAL: Preserve microsecond precision!
        seconds = value // 1_000_000
        microseconds = value % 1_000_000

        # Create timestamp with full precision
        ts = pd.Timestamp(seconds, unit="s", tz="UTC")
        # Add microseconds separately to avoid precision loss
        ts = ts + pd.Timedelta(microseconds=microseconds)

        return ts

    def convert_ttl_value(self, value: int | None) -> int | None:
        """
        Convert TTL value.

        TTL is seconds remaining until expiry.
        Returns None for NULL values or non-expiring data.
        """
        # TTL is already in the correct format (seconds as int)
        return value

    def get_dataframe_schema(self, table_metadata: dict[str, Any]) -> dict[str, str | np.dtype]:
        """
        Get pandas DataFrame schema from Cassandra table metadata.

        Args:
            table_metadata: Table metadata including column definitions

        Returns:
            Dict mapping column names to pandas dtypes
        """
        schema = {}

        for column in table_metadata.get("columns", []):
            col_name = column["name"]
            col_type = column["type"]

            # Get base dtype
            dtype = self.get_pandas_dtype(col_type)
            schema[col_name] = dtype

            # Add writetime/TTL columns if needed
            if not self._is_primary_key(column) and col_type not in self.NO_WRITETIME_TYPES:
                # Writetime columns are always datetime64[ns]
                schema[f"{col_name}_writetime"] = "datetime64[ns]"
                # TTL columns are always int64
                schema[f"{col_name}_ttl"] = "int64"

        return schema

    def _is_primary_key(self, column_def: dict[str, Any]) -> bool:
        """Check if column is part of primary key."""
        return (
            column_def.get("is_primary_key", False)
            or column_def.get("is_partition_key", False)
            or column_def.get("is_clustering_key", False)
        )

    def create_empty_dataframe(self, schema: dict[str, str | np.dtype]) -> pd.DataFrame:
        """
        Create empty DataFrame with correct schema.

        Used for Dask metadata.
        """
        # Create empty series for each column with correct dtype
        data = {}
        for col_name, dtype in schema.items():
            if dtype == "object":
                # Object columns need empty list
                data[col_name] = pd.Series([], dtype=dtype)
            else:
                # Other dtypes can use standard constructor
                data[col_name] = pd.Series(dtype=dtype)

        return pd.DataFrame(data)

    def handle_null_values(self, df: pd.DataFrame, table_metadata: dict[str, Any]) -> pd.DataFrame:
        """
        Apply Cassandra NULL semantics to DataFrame.

        CRITICAL: Must match Cassandra's exact behavior!
        """
        for column in table_metadata.get("columns", []):
            col_name = column["name"]
            col_type = column["type"]

            if col_name not in df.columns:
                continue

            base_type = self._extract_base_type(col_type)

            # Collection types: empty → NULL
            if base_type in self.COLLECTION_TYPES:
                # Replace empty collections with None
                mask = df[col_name].apply(self._is_empty_collection)
                df.loc[mask, col_name] = None

        return df
