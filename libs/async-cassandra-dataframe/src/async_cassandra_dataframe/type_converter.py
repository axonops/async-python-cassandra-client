"""
Comprehensive type conversion utilities for Cassandra to pandas DataFrames.

This module ensures NO precision loss and correct type mapping for ALL Cassandra types.
"""

from datetime import date, datetime, time
from decimal import Decimal
from ipaddress import IPv4Address, IPv6Address
from typing import Any
from uuid import UUID

import numpy as np
import pandas as pd
from cassandra.util import Date, Time


class DataFrameTypeConverter:
    """Convert Cassandra types to proper pandas dtypes without precision loss."""

    @staticmethod
    def convert_dataframe_types(
        df: pd.DataFrame, table_metadata: dict, type_mapper
    ) -> pd.DataFrame:
        """
        Apply comprehensive type conversions to a DataFrame.

        Args:
            df: DataFrame to convert
            table_metadata: Cassandra table metadata
            type_mapper: CassandraTypeMapper instance

        Returns:
            DataFrame with correct types
        """
        if df.empty:
            return df

        import logging

        logger = logging.getLogger(__name__)
        logger.debug(f"Converting DataFrame types for {len(df)} rows")

        for col in df.columns:
            # Skip writetime/TTL columns
            if col.endswith("_writetime") or col.endswith("_ttl"):
                continue

            # Get column metadata
            col_info = next((c for c in table_metadata["columns"] if c["name"] == col), None)

            if not col_info:
                continue

            col_type = str(col_info["type"])

            # Apply conversions based on Cassandra type
            if col_type == "tinyint":
                df[col] = DataFrameTypeConverter._convert_to_int(df[col], "Int8")
            elif col_type == "smallint":
                df[col] = DataFrameTypeConverter._convert_to_int(df[col], "Int16")
            elif col_type == "int":
                df[col] = DataFrameTypeConverter._convert_to_int(df[col], "Int32")
            elif col_type in ["bigint", "counter"]:
                df[col] = DataFrameTypeConverter._convert_to_int(df[col], "Int64")
            elif col_type == "varint":
                # Varint needs special handling - keep as object for unlimited precision
                logger.debug(f"Converting varint column {col}")
                logger.debug(
                    f"  Before: dtype={df[col].dtype}, sample={df[col].iloc[0] if len(df) > 0 else 'empty'}"
                )
                df[col] = df[col].apply(DataFrameTypeConverter._convert_varint)
                # Ensure dtype is object, not string
                df[col] = df[col].astype("object")
                logger.debug(
                    f"  After: dtype={df[col].dtype}, sample={df[col].iloc[0] if len(df) > 0 else 'empty'}"
                )
            elif col_type == "float":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("float32")
            elif col_type == "double":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
            elif col_type == "decimal":
                # CRITICAL: Preserve decimal precision
                df[col] = df[col].apply(DataFrameTypeConverter._convert_decimal)
                # Ensure dtype is object to preserve Decimal type
                df[col] = df[col].astype("object")
            elif col_type == "boolean":
                df[col] = df[col].astype("bool")
            elif col_type in ["text", "varchar", "ascii"]:
                # String types - ensure they're strings
                df[col] = df[col].astype("string")
            elif col_type == "blob":
                # Binary data - keep as bytes
                df[col] = df[col].apply(DataFrameTypeConverter._ensure_bytes)
                # Ensure dtype is object to preserve bytes type
                df[col] = df[col].astype("object")
            elif col_type == "date":
                df[col] = df[col].apply(DataFrameTypeConverter._convert_date)
            elif col_type == "time":
                df[col] = df[col].apply(DataFrameTypeConverter._convert_time)
            elif col_type == "timestamp":
                df[col] = df[col].apply(DataFrameTypeConverter._convert_timestamp)
            elif col_type == "duration":
                # Keep Duration objects as-is
                pass
            elif col_type in ["uuid", "timeuuid"]:
                df[col] = df[col].apply(DataFrameTypeConverter._convert_uuid)
            elif col_type == "inet":
                df[col] = df[col].apply(DataFrameTypeConverter._convert_inet)
            elif (
                col_type.startswith("list")
                or col_type.startswith("set")
                or col_type.startswith("map")
            ):
                # Collections - apply type mapper conversion
                df[col] = df[col].apply(lambda x, ct=col_type: type_mapper.convert_value(x, ct))
            elif col_type.startswith("tuple") or col_type.startswith("frozen"):
                # Tuples and frozen types
                df[col] = df[col].apply(lambda x, ct=col_type: type_mapper.convert_value(x, ct))
            else:
                # Unknown type or UDT - use type mapper
                df[col] = df[col].apply(lambda x, ct=col_type: type_mapper.convert_value(x, ct))

        return df

    @staticmethod
    def _convert_to_int(series: pd.Series, dtype: str) -> pd.Series:
        """Convert to nullable integer type to handle NaN values."""
        try:
            # First convert to numeric, then to nullable integer
            return pd.to_numeric(series, errors="coerce").astype(dtype)  # type: ignore[call-overload, no-any-return]
        except Exception:
            # If conversion fails, keep as numeric float
            return pd.to_numeric(series, errors="coerce")

    @staticmethod
    def _convert_varint(value: Any) -> Any:
        """Convert varint values - preserve unlimited precision."""
        if pd.isna(value):
            return None
        if isinstance(value, str):
            # Convert string back to Python int for unlimited precision
            return int(value)
        return value

    @staticmethod
    def _convert_decimal(value: Any) -> Any:
        """Convert decimal values - CRITICAL to preserve precision."""
        if pd.isna(value):
            return None
        if isinstance(value, str):
            return Decimal(value)
        return value

    @staticmethod
    def _ensure_bytes(value: Any) -> Any:
        """Ensure blob data is bytes."""
        if pd.isna(value):
            return None
        if isinstance(value, str):
            # Check if it's a hex string representation
            if value.startswith("0x"):
                try:
                    return bytes.fromhex(value[2:])
                except ValueError:
                    pass
            # Otherwise encode as UTF-8
            try:
                return value.encode("utf-8")
            except UnicodeEncodeError:
                # If it fails, try latin-1
                return value.encode("latin-1")
        return value

    @staticmethod
    def _convert_date(value: Any) -> Any:
        """Convert date values to pandas Timestamp."""
        if pd.isna(value):
            return pd.NaT
        if isinstance(value, Date):
            return pd.Timestamp(value.date())
        if isinstance(value, date):
            return pd.Timestamp(value)
        if isinstance(value, str):
            return pd.to_datetime(value)
        return value

    @staticmethod
    def _convert_time(value: Any) -> Any:
        """Convert time values to pandas Timedelta."""
        if pd.isna(value):
            return pd.NaT
        if isinstance(value, Time):
            return pd.Timedelta(value.nanosecond_time, unit="ns")
        if isinstance(value, time):
            return pd.Timedelta(
                hours=value.hour,
                minutes=value.minute,
                seconds=value.second,
                microseconds=value.microsecond,
            )
        if isinstance(value, int | np.int64):
            # Time as nanoseconds
            return pd.Timedelta(int(value), unit="ns")
        return value

    @staticmethod
    def _convert_timestamp(value: Any) -> Any:
        """Convert timestamp values to pandas Timestamp with timezone."""
        if pd.isna(value):
            return pd.NaT
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return pd.Timestamp(value, tz="UTC")
            return pd.Timestamp(value)
        if isinstance(value, str):
            return pd.to_datetime(value, utc=True)
        return value

    @staticmethod
    def _convert_uuid(value: Any) -> Any:
        """Convert UUID values."""
        if pd.isna(value):
            return None
        if isinstance(value, str):
            return UUID(value)
        return value

    @staticmethod
    def _convert_inet(value: Any) -> Any:
        """Convert inet values to IP address objects."""
        if pd.isna(value):
            return None
        if isinstance(value, str):
            if ":" in value:
                return IPv6Address(value)
            return IPv4Address(value)
        return value
