"""
Serializers for basic Cassandra data types.

Handles serialization of fundamental types like integers, strings,
timestamps, UUIDs, etc. to different output formats.
"""

import ipaddress
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any
from uuid import UUID

from cassandra.util import Date, Time

from .base import SerializationContext, TypeSerializer


class NullSerializer(TypeSerializer):
    """Serializer for NULL/None values."""

    def serialize(self, value: Any, context: SerializationContext) -> Any:
        """Serialize NULL values based on format."""
        if value is not None:
            raise ValueError(f"NullSerializer can only handle None, got {type(value)}")

        if context.format == "csv":
            # Use configured null value or empty string
            return context.get_option("null_value", "")
        elif context.format in ("json", "parquet"):
            return None
        else:
            return None

    def can_handle(self, value: Any) -> bool:
        """Check if value is None."""
        return value is None


class BooleanSerializer(TypeSerializer):
    """Serializer for boolean values."""

    def serialize(self, value: Any, context: SerializationContext) -> Any:
        """Serialize boolean values."""
        if context.format == "csv":
            return "true" if value else "false"
        else:
            # JSON, Parquet, etc. support native booleans
            return bool(value)

    def can_handle(self, value: Any) -> bool:
        """Check if value is boolean."""
        return isinstance(value, bool)


class IntegerSerializer(TypeSerializer):
    """Serializer for integer types (TINYINT, SMALLINT, INT, BIGINT, VARINT)."""

    def serialize(self, value: Any, context: SerializationContext) -> Any:
        """Serialize integer values."""
        if context.format == "csv":
            return str(value)
        else:
            # JSON and Parquet support native integers
            return int(value)

    def can_handle(self, value: Any) -> bool:
        """Check if value is integer."""
        return isinstance(value, int) and not isinstance(value, bool)


class FloatSerializer(TypeSerializer):
    """Serializer for floating point types (FLOAT, DOUBLE)."""

    def serialize(self, value: Any, context: SerializationContext) -> Any:
        """Serialize float values."""
        if context.format == "csv":
            # Handle special float values
            if value != value:  # NaN
                return "NaN"
            elif value == float("inf"):
                return "Infinity"
            elif value == float("-inf"):
                return "-Infinity"
            else:
                return str(value)
        else:
            # JSON doesn't support NaN/Infinity natively
            if context.format == "json" and (value != value or abs(value) == float("inf")):
                # Convert to string representation
                if value != value:
                    return "NaN"
                elif value == float("inf"):
                    return "Infinity"
                elif value == float("-inf"):
                    return "-Infinity"
            return float(value)

    def can_handle(self, value: Any) -> bool:
        """Check if value is float."""
        return isinstance(value, float)


class DecimalSerializer(TypeSerializer):
    """Serializer for DECIMAL type."""

    def serialize(self, value: Any, context: SerializationContext) -> Any:
        """Serialize decimal values."""
        if context.format == "csv":
            return str(value)
        elif context.format == "json":
            # JSON doesn't have a decimal type, use string to preserve precision
            if context.get_option("decimal_as_float", False):
                return float(value)
            else:
                return str(value)
        else:
            # Parquet can handle decimals natively
            return value

    def can_handle(self, value: Any) -> bool:
        """Check if value is Decimal."""
        return isinstance(value, Decimal)


class StringSerializer(TypeSerializer):
    """Serializer for string types (TEXT, VARCHAR, ASCII)."""

    def serialize(self, value: Any, context: SerializationContext) -> Any:
        """Serialize string values."""
        # Strings are generally preserved as-is across formats
        return str(value)

    def can_handle(self, value: Any) -> bool:
        """Check if value is string."""
        return isinstance(value, str)


class BinarySerializer(TypeSerializer):
    """Serializer for BLOB type."""

    def serialize(self, value: Any, context: SerializationContext) -> Any:
        """Serialize binary data."""
        if context.format == "csv":
            # Convert to hex string for CSV
            return value.hex()
        elif context.format == "json":
            # Base64 encode for JSON
            import base64

            return base64.b64encode(value).decode("ascii")
        else:
            # Parquet can handle binary natively
            return value

    def can_handle(self, value: Any) -> bool:
        """Check if value is bytes."""
        return isinstance(value, (bytes, bytearray))


class UUIDSerializer(TypeSerializer):
    """Serializer for UUID and TIMEUUID types."""

    def serialize(self, value: Any, context: SerializationContext) -> Any:
        """Serialize UUID values."""
        if context.format in ("csv", "json"):
            return str(value)
        else:
            # Some formats might support UUID natively
            return value

    def can_handle(self, value: Any) -> bool:
        """Check if value is UUID."""
        return isinstance(value, UUID)


class TimestampSerializer(TypeSerializer):
    """Serializer for TIMESTAMP type."""

    def serialize(self, value: Any, context: SerializationContext) -> Any:
        """Serialize timestamp values."""
        if context.format == "csv":
            # Use ISO 8601 format
            return value.isoformat()
        elif context.format == "json":
            # JSON: ISO 8601 string or Unix timestamp
            if context.get_option("timestamp_format", "iso") == "unix":
                return int(value.timestamp() * 1000)  # Milliseconds
            else:
                return value.isoformat()
        else:
            # Parquet can handle timestamps natively
            return value

    def can_handle(self, value: Any) -> bool:
        """Check if value is datetime."""
        return isinstance(value, datetime)


class DateSerializer(TypeSerializer):
    """Serializer for DATE type."""

    def serialize(self, value: Any, context: SerializationContext) -> Any:
        """Serialize date values."""
        # Handle both cassandra.util.Date and datetime.date
        if isinstance(value, Date):
            # Extract the date
            date_value = (
                value.date()
                if hasattr(value, "date")
                else date.fromordinal(value.days_from_epoch + 719163)
            )
        else:
            date_value = value

        if context.format in ("csv", "json"):
            # Use ISO format YYYY-MM-DD
            return date_value.isoformat()
        else:
            return date_value

    def can_handle(self, value: Any) -> bool:
        """Check if value is date."""
        return isinstance(value, (date, Date)) and not isinstance(value, datetime)


class TimeSerializer(TypeSerializer):
    """Serializer for TIME type."""

    def serialize(self, value: Any, context: SerializationContext) -> Any:
        """Serialize time values."""
        # Handle both cassandra.util.Time and datetime.time
        if isinstance(value, Time):
            # Convert nanoseconds to time
            total_nanos = value.nanosecond_time
            hours = total_nanos // (3600 * 1_000_000_000)
            remaining = total_nanos % (3600 * 1_000_000_000)
            minutes = remaining // (60 * 1_000_000_000)
            remaining = remaining % (60 * 1_000_000_000)
            seconds = remaining // 1_000_000_000
            microseconds = (remaining % 1_000_000_000) // 1000
            time_value = time(
                hour=int(hours),
                minute=int(minutes),
                second=int(seconds),
                microsecond=int(microseconds),
            )
        else:
            time_value = value

        if context.format in ("csv", "json"):
            # Use ISO format HH:MM:SS.ffffff
            return time_value.isoformat()
        else:
            return time_value

    def can_handle(self, value: Any) -> bool:
        """Check if value is time."""
        return isinstance(value, (time, Time))


class InetSerializer(TypeSerializer):
    """Serializer for INET type (IP addresses)."""

    def serialize(self, value: Any, context: SerializationContext) -> Any:
        """Serialize IP address values."""
        # Cassandra returns IP addresses as strings
        if context.format in ("csv", "json"):
            return str(value)
        else:
            # Try to parse for validation
            try:
                ip = ipaddress.ip_address(value)
                return str(ip)
            except Exception:
                return str(value)

    def can_handle(self, value: Any) -> bool:
        """Check if value is IP address string."""
        if not isinstance(value, str):
            return False
        try:
            ipaddress.ip_address(value)
            return True
        except Exception:
            return False


class DurationSerializer(TypeSerializer):
    """Serializer for Duration type (Cassandra 3.10+)."""

    def serialize(self, value: Any, context: SerializationContext) -> Any:
        """Serialize duration values."""
        # Duration has months, days, and nanoseconds components
        if hasattr(value, "months") and hasattr(value, "days") and hasattr(value, "nanoseconds"):
            if context.format == "csv":
                # ISO 8601 duration format (approximate)
                return f"P{value.months}M{value.days}DT{value.nanoseconds/1_000_000_000}S"
            elif context.format == "json":
                # Return as object with components
                return {
                    "months": value.months,
                    "days": value.days,
                    "nanoseconds": value.nanoseconds,
                }
            else:
                return value
        return value

    def can_handle(self, value: Any) -> bool:
        """Check if value is Duration."""
        return hasattr(value, "months") and hasattr(value, "days") and hasattr(value, "nanoseconds")


class CounterSerializer(TypeSerializer):
    """Serializer for COUNTER type."""

    def serialize(self, value: Any, context: SerializationContext) -> Any:
        """Serialize counter values."""
        # Counters are 64-bit signed integers
        if context.format == "csv":
            return str(value)
        else:
            return int(value)

    def can_handle(self, value: Any) -> bool:
        """Check if value is counter (integer)."""
        # Counters appear as regular integers when read
        return isinstance(value, int) and not isinstance(value, bool)


class VectorSerializer(TypeSerializer):
    """Serializer for VECTOR type (Cassandra 5.0+)."""

    def serialize(self, value: Any, context: SerializationContext) -> Any:
        """Serialize vector values."""
        # Vectors are fixed-length arrays of floats
        if hasattr(value, "__iter__") and not isinstance(value, (str, bytes)):
            if context.format == "csv":
                # CSV: comma-separated values in brackets
                float_strs = [str(float(v)) for v in value]
                return f"[{','.join(float_strs)}]"
            elif context.format == "json":
                # JSON: native array
                return [float(v) for v in value]
            else:
                return value
        return value

    def can_handle(self, value: Any) -> bool:
        """Check if value is vector (list/array of numbers)."""
        if not hasattr(value, "__iter__") or isinstance(value, (str, bytes, dict)):
            return False

        # Exclude tuples - they have their own serializer
        if isinstance(value, tuple):
            return False

        # Check if it looks like a vector (all numeric values)
        try:
            # Vectors should contain only numbers and not be empty
            items = list(value)
            if not items:  # Empty list is not a vector
                return False
            return all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in items)
        except Exception:
            return False
