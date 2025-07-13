"""
Utilities for handling User Defined Types (UDTs) in DataFrames.

Dask has a known limitation where dict objects are converted to strings
during serialization. This module provides utilities to work around this.
"""

import ast
import json
from typing import Any

import pandas as pd


def serialize_udt_for_dask(value: Any) -> str:
    """
    Serialize UDT dict to a special JSON format for Dask transport.

    Args:
        value: Dict, list of dicts, or other value

    Returns:
        JSON string with special marker for UDTs
    """
    if isinstance(value, dict):
        # Mark as UDT with special prefix
        return f"__UDT__{json.dumps(value)}"
    elif isinstance(value, list):
        # Handle list of UDTs
        serialized = []
        for item in value:
            if isinstance(item, dict):
                serialized.append(
                    json.loads(serialize_udt_for_dask(item)[7:])
                )  # Remove __UDT__ prefix
            else:
                serialized.append(item)
        return f"__UDT_LIST__{json.dumps(serialized)}"
    else:
        return value


def deserialize_udt_from_dask(value: Any) -> Any:
    """
    Deserialize UDT from Dask string representation.

    Args:
        value: String representation or original value

    Returns:
        Original dict/list or value
    """
    if isinstance(value, str):
        if value.startswith("__UDT__"):
            # Deserialize single UDT
            return json.loads(value[7:])
        elif value.startswith("__UDT_LIST__"):
            # Deserialize list of UDTs
            return json.loads(value[12:])
        elif value.startswith("{") and value.endswith("}"):
            # Try to parse dict-like string (fallback for existing data)
            try:
                return ast.literal_eval(value)
            except (ValueError, SyntaxError):
                pass
    return value


def prepare_dataframe_for_dask(df: pd.DataFrame, udt_columns: list[str]) -> pd.DataFrame:
    """
    Prepare DataFrame for Dask by serializing UDT columns.

    Args:
        df: DataFrame with UDT columns
        udt_columns: List of column names containing UDTs

    Returns:
        DataFrame with serialized UDT columns
    """
    df_copy = df.copy()
    for col in udt_columns:
        if col in df_copy.columns:
            df_copy[col] = df_copy[col].apply(serialize_udt_for_dask)
    return df_copy


def restore_udts_in_dataframe(df: pd.DataFrame, udt_columns: list[str]) -> pd.DataFrame:
    """
    Restore UDTs in DataFrame after Dask computation.

    Args:
        df: DataFrame with serialized UDT columns
        udt_columns: List of column names containing UDTs

    Returns:
        DataFrame with restored UDT dicts
    """
    for col in udt_columns:
        if col in df.columns:
            df[col] = df[col].apply(deserialize_udt_from_dask)
    return df


def detect_udt_columns(table_metadata: dict[str, Any]) -> list[str]:
    """
    Detect which columns contain UDTs based on table metadata.

    Args:
        table_metadata: Cassandra table metadata

    Returns:
        List of column names that contain UDTs
    """
    udt_columns = []

    for column in table_metadata.get("columns", []):
        col_name = column["name"]
        col_type = str(column["type"])

        # Check if column type is a UDT
        if col_type.startswith("frozen<") and not any(
            col_type.startswith(f"frozen<{t}") for t in ["list", "set", "map", "tuple"]
        ):
            # It's a frozen UDT
            udt_columns.append(col_name)
        elif "<" not in col_type and col_type not in [
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
        ]:
            # It's likely a non-frozen UDT
            udt_columns.append(col_name)
        elif "frozen<" in col_type:
            # Collection containing frozen UDTs
            udt_columns.append(col_name)

    return udt_columns
