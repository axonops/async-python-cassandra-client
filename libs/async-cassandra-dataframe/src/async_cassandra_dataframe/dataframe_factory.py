"""
DataFrame metadata and factory functions.

Creates Pandas/Dask DataFrame metadata with proper types and schemas.
"""

from typing import Any

import pandas as pd

from .cassandra_writetime_dtype import CassandraWritetimeDtype
from .types import CassandraTypeMapper


class DataFrameFactory:
    """Creates DataFrame metadata and schemas for Cassandra tables."""

    def __init__(self, table_metadata: dict[str, Any], type_mapper: CassandraTypeMapper):
        """
        Initialize DataFrame factory.

        Args:
            table_metadata: Cassandra table metadata
            type_mapper: Type mapping utility
        """
        self._table_metadata = table_metadata
        self._type_mapper = type_mapper

    def create_dataframe_meta(
        self,
        columns: list[str],
        writetime_columns: list[str] | None,
        ttl_columns: list[str] | None,
    ) -> pd.DataFrame:
        """
        Create DataFrame metadata for Dask with proper examples for object columns.

        Args:
            columns: Regular columns to include
            writetime_columns: Columns to get writetime for
            ttl_columns: Columns to get TTL for

        Returns:
            Empty DataFrame with correct schema
        """
        # Create data with example values for object columns
        data = {}

        for col in columns:
            col_info = next((c for c in self._table_metadata["columns"] if c["name"] == col), None)
            if col_info:
                col_type = str(col_info["type"])
                dtype = self._type_mapper.get_pandas_dtype(col_type)

                if dtype == "object":
                    # Provide example values for object columns to prevent Dask serialization issues
                    data[col] = self._create_example_series(col_type)
                else:
                    # Non-object types
                    data[col] = pd.Series(dtype=dtype)

        # Add writetime columns
        if writetime_columns:
            for col in writetime_columns:
                data[f"{col}_writetime"] = pd.Series(dtype=CassandraWritetimeDtype())

        # Add TTL columns
        if ttl_columns:
            for col in ttl_columns:
                data[f"{col}_ttl"] = pd.Series(dtype="Int64")  # Nullable int64

        # Create DataFrame and ensure it's empty but with correct types
        df = pd.DataFrame(data)
        return df.iloc[0:0]  # Empty but with preserved types

    def _create_example_series(self, col_type: str) -> pd.Series:
        """Create example Series for object column types."""
        if col_type == "list" or col_type.startswith("list<"):
            return pd.Series([[]], dtype="object")
        elif col_type == "set" or col_type.startswith("set<"):
            return pd.Series([set()], dtype="object")
        elif col_type == "map" or col_type.startswith("map<"):
            return pd.Series([{}], dtype="object")
        elif col_type.startswith("frozen<"):
            # Frozen collections or UDTs
            if "list" in col_type:
                return pd.Series([[]], dtype="object")
            elif "set" in col_type:
                return pd.Series([set()], dtype="object")
            elif "map" in col_type:
                return pd.Series([{}], dtype="object")
            else:
                # Frozen UDT
                return pd.Series([{}], dtype="object")
        elif "<" not in col_type and col_type not in [
            "text",
            "varchar",
            "ascii",
            "blob",
            "uuid",
            "timeuuid",
            "inet",
        ]:
            # Likely a UDT (non-parameterized custom type)
            return pd.Series([{}], dtype="object")
        else:
            # Other object types
            return pd.Series([], dtype="object")
