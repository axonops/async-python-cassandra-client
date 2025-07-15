"""
Cassandra to Pandas type mapping with comprehensive support for all types.

Critical component that handles all type conversions including edge cases
discovered during async-cassandra-bulk development.
"""

# mypy: ignore-errors

from datetime import date, datetime, time
from typing import Any

import numpy as np
import pandas as pd
from cassandra.util import Date, Time

from .cassandra_dtypes import (
    CassandraDateDtype,
    CassandraDecimalDtype,
    CassandraDurationDtype,
    CassandraInetDtype,
    CassandraTimeUUIDDtype,
    CassandraUUIDDtype,
    CassandraVarintDtype,
)
from .cassandra_udt_dtype import CassandraUDTDtype


class CassandraTypeMapper:
    """
    Maps Cassandra types to pandas dtypes with special handling for:
    - Precision preservation (decimals, timestamps)
    - NULL semantics (empty collections → NULL)
    - Special types (duration, counter)
    - Writetime/TTL values
    """

    # Basic type mapping - Using Pandas nullable dtypes
    BASIC_TYPE_MAP = {
        # String types - Use nullable string dtype
        "ascii": "string",  # Nullable string
        "text": "string",  # Nullable string
        "varchar": "string",  # Nullable string
        # Numeric types - Use nullable integer types
        "tinyint": "Int8",  # Nullable int8
        "smallint": "Int16",  # Nullable int16
        "int": "Int32",  # Nullable int32
        "bigint": "Int64",  # Nullable int64
        "varint": CassandraVarintDtype(),  # Unlimited precision integer
        "float": "Float32",  # Nullable float32
        "double": "Float64",  # Nullable float64
        "decimal": CassandraDecimalDtype(),  # Full precision decimal
        "counter": "Int64",  # Nullable int64
        # Temporal types
        "date": CassandraDateDtype(),  # Custom dtype for full Cassandra date range
        "time": "timedelta64[ns]",  # Handles NaT
        "timestamp": "datetime64[ns, UTC]",  # Handles NaT
        "duration": CassandraDurationDtype(),  # Cassandra Duration type
        # Binary
        "blob": "object",  # bytes
        # Other types
        "boolean": "boolean",  # Nullable boolean
        "inet": CassandraInetDtype(),  # IP address with proper type
        "uuid": CassandraUUIDDtype(),  # UUID with proper type
        "timeuuid": CassandraTimeUUIDDtype(),  # TimeUUID with timestamp extraction
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

    def get_pandas_dtype(
        self, cassandra_type: str, table_metadata: dict[str, Any] = None
    ) -> str | np.dtype:
        """
        Get pandas dtype for Cassandra type.

        Args:
            cassandra_type: CQL type name
            table_metadata: Optional table metadata containing UDT information

        Returns:
            Pandas dtype string or numpy dtype
        """
        # Normalize type name
        base_type = self._extract_base_type(cassandra_type)

        # Check cache
        if base_type in self._dtype_cache:
            return self._dtype_cache[base_type]

        # Get dtype
        dtype = self.BASIC_TYPE_MAP.get(base_type, None)

        if dtype is None:
            # Check if it's a UDT
            if table_metadata and self._is_udt_type(cassandra_type, table_metadata):
                # Extract keyspace if available
                keyspace = table_metadata.get("keyspace", "")
                dtype = CassandraUDTDtype(keyspace=keyspace, udt_name=base_type)
            else:
                dtype = "object"

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
            # Cassandra Date to Python date object (kept as object dtype)
            # This avoids issues with dates outside pandas datetime64[ns] range
            if isinstance(value, Date):
                # Date.date() returns datetime.date
                return value.date()
            elif isinstance(value, date):
                return value
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
            # Keep as Duration object
            return value

        elif base_type == "inet":
            # Convert string to IP address object if needed
            if isinstance(value, str):
                from ipaddress import ip_address

                return ip_address(value)
            return value

        # Handle UDTs (User Defined Types)
        # Keep UDTs as namedtuples to preserve type information
        if hasattr(value, "_fields") and hasattr(value, "_asdict"):
            # Return the UDT as-is to preserve type information
            return value

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

    def _is_udt_type(self, col_type_str: str, table_metadata: dict[str, Any]) -> bool:
        """
        Check if a column type is a UDT.

        Args:
            col_type_str: String representation of column type
            table_metadata: Table metadata containing UDT information

        Returns:
            True if the type is a UDT
        """
        # Remove frozen wrapper if present
        type_str = col_type_str
        if type_str.startswith("frozen<") and type_str.endswith(">"):
            type_str = type_str[7:-1]

        # Check if it's a collection of UDTs - collections themselves aren't UDTs
        if any(type_str.startswith(prefix) for prefix in ["list<", "set<", "map<", "tuple<"]):
            return False

        # Check against user types defined in the keyspace
        user_types = table_metadata.get("user_types", {})
        if type_str in user_types:
            return True

        # It's a UDT if it's not a known Cassandra type
        return type_str not in {
            "ascii",
            "bigint",
            "blob",
            "boolean",
            "counter",
            "date",
            "decimal",
            "double",
            "duration",
            "float",
            "inet",
            "int",
            "smallint",
            "text",
            "time",
            "timestamp",
            "timeuuid",
            "tinyint",
            "uuid",
            "varchar",
            "varint",
            "list",
            "set",
            "map",
            "tuple",
            "frozen",
            "vector",
        }

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

            # Get base dtype (pass table_metadata for UDT detection)
            dtype = self.get_pandas_dtype(col_type, table_metadata)
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
        # Import extension arrays
        from .cassandra_dtypes import (
            CassandraDateArray,
            CassandraDecimalArray,
            CassandraDurationArray,
            CassandraInetArray,
            CassandraTimeUUIDArray,
            CassandraUUIDArray,
            CassandraVarintArray,
        )
        from .cassandra_udt_dtype import CassandraUDTArray

        # Create empty series for each column with correct dtype
        data = {}
        for col_name, dtype in schema.items():
            if dtype == "object":
                # Object columns need empty list
                data[col_name] = pd.Series([], dtype=dtype)
            elif dtype in [
                "Int8",
                "Int16",
                "Int32",
                "Int64",
                "Float32",
                "Float64",
                "boolean",
                "string",
            ]:
                # Nullable dtypes - create with correct nullable type
                data[col_name] = pd.Series(dtype=dtype)
            elif dtype == "datetime64[ns]":
                # Date columns - use datetime64[ns]
                data[col_name] = pd.Series(dtype="datetime64[ns]")
            elif dtype == "timedelta64[ns]":
                # Time columns - use timedelta64[ns]
                data[col_name] = pd.Series(dtype="timedelta64[ns]")
            elif dtype == "datetime64[ns, UTC]":
                # Timestamp columns - use datetime64[ns, UTC]
                data[col_name] = pd.Series(dtype="datetime64[ns, UTC]")
            elif isinstance(dtype, CassandraDateDtype):
                data[col_name] = pd.Series(CassandraDateArray([], dtype), dtype=dtype)
            elif isinstance(dtype, CassandraDecimalDtype):
                data[col_name] = pd.Series(CassandraDecimalArray([], dtype), dtype=dtype)
            elif isinstance(dtype, CassandraVarintDtype):
                data[col_name] = pd.Series(CassandraVarintArray([], dtype), dtype=dtype)
            elif isinstance(dtype, CassandraInetDtype):
                data[col_name] = pd.Series(CassandraInetArray([], dtype), dtype=dtype)
            elif isinstance(dtype, CassandraUUIDDtype):
                data[col_name] = pd.Series(CassandraUUIDArray([], dtype), dtype=dtype)
            elif isinstance(dtype, CassandraTimeUUIDDtype):
                data[col_name] = pd.Series(CassandraTimeUUIDArray([], dtype), dtype=dtype)
            elif isinstance(dtype, CassandraDurationDtype):
                data[col_name] = pd.Series(CassandraDurationArray([], dtype), dtype=dtype)
            elif isinstance(dtype, CassandraUDTDtype):
                data[col_name] = pd.Series(CassandraUDTArray([], dtype), dtype=dtype)
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
