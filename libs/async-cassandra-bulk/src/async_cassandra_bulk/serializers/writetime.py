"""
Writetime serializer for Cassandra writetime values.

Handles conversion of writetime microseconds to human-readable formats
for different export targets.
"""

from datetime import datetime, timezone
from typing import Any

from .base import SerializationContext, TypeSerializer


class WritetimeSerializer(TypeSerializer):
    """
    Serializer for Cassandra writetime values.

    Writetimes are stored as microseconds since Unix epoch and need
    to be converted to appropriate formats for export.
    """

    def serialize(self, value: Any, context: SerializationContext) -> Any:
        """
        Serialize writetime value based on target format.

        Args:
            value: Writetime in microseconds since epoch
            context: Serialization context with format info

        Returns:
            Formatted writetime for target format
        """
        if value is None:
            # Handle null writetime
            if context.format == "csv":
                return context.options.get("null_value", "")
            return None

        # Handle list values (can happen with collection columns)
        if isinstance(value, list):
            # For collections, Cassandra may return a list of writetimes
            # Use the first one (they should all be the same for a single write)
            if value:
                value = value[0]
            else:
                return None

        # Convert microseconds to datetime
        # Cassandra writetime is microseconds since epoch
        timestamp = datetime.fromtimestamp(value / 1_000_000, tz=timezone.utc)

        if context.format == "csv":
            # For CSV, use configurable format or ISO
            fmt = context.options.get("writetime_format")
            if fmt is None:
                fmt = "%Y-%m-%d %H:%M:%S.%f"
            return timestamp.strftime(fmt)
        elif context.format == "json":
            # For JSON, use ISO format with timezone
            return timestamp.isoformat()
        else:
            # For other formats, return as-is
            return value

    def can_handle(self, value: Any) -> bool:
        """
        Check if value is a writetime column.

        Writetime columns are identified by their column name suffix
        or by being large integer values (microseconds since epoch).

        Args:
            value: Value to check

        Returns:
            False - writetime is handled by column name pattern
        """
        # Writetime serialization is triggered by column name pattern
        # not by value type, so this serializer won't auto-detect
        return False


class WritetimeColumnSerializer:
    """
    Special serializer that detects writetime columns by name pattern.

    This is used during export to identify and serialize writetime columns
    based on their _writetime suffix.
    """

    def __init__(self) -> None:
        """Initialize with writetime serializer."""
        self._writetime_serializer = WritetimeSerializer()

    def is_writetime_column(self, column_name: str) -> bool:
        """
        Check if column name indicates a writetime column.

        Args:
            column_name: Column name to check

        Returns:
            True if column is a writetime column
        """
        return column_name.endswith("_writetime")

    def serialize_if_writetime(
        self, column_name: str, value: Any, context: SerializationContext
    ) -> tuple[bool, Any]:
        """
        Serialize value if column is a writetime column.

        Args:
            column_name: Column name
            value: Value to potentially serialize
            context: Serialization context

        Returns:
            Tuple of (is_writetime, serialized_value)
        """
        if self.is_writetime_column(column_name):
            return True, self._writetime_serializer.serialize(value, context)
        return False, value
