"""
Serializers for Cassandra collection types.

Handles serialization of LIST, SET, MAP, TUPLE, and frozen collections
to different output formats.
"""

import json
from typing import Any

from .base import SerializationContext, TypeSerializer

# Import Cassandra types if available
try:
    from cassandra.util import OrderedMapSerializedKey, SortedSet
except ImportError:
    OrderedMapSerializedKey = None
    SortedSet = None


class ListSerializer(TypeSerializer):
    """Serializer for LIST collection type."""

    def serialize(self, value: Any, context: SerializationContext) -> Any:
        """Serialize list values."""
        if not isinstance(value, list):
            raise ValueError(f"ListSerializer expects list, got {type(value)}")

        # Import here to avoid circular import
        from .registry import get_global_registry

        registry = get_global_registry()

        # For nested collections in CSV, we need to avoid double-encoding
        # Create a temporary context for recursion
        if context.format == "csv":
            # For nested elements, use a temporary JSON context
            # to avoid double JSON encoding
            nested_context = SerializationContext(
                format="json", options=context.options, column_metadata=context.column_metadata
            )
        else:
            nested_context = context

        # Serialize each element
        serialized_items = []
        for item in value:
            serialized_items.append(registry.serialize(item, nested_context))

        if context.format == "csv":
            # CSV: JSON array string
            return json.dumps(serialized_items, default=str)
        elif context.format == "json":
            # JSON: native array
            return serialized_items
        else:
            return value

    def can_handle(self, value: Any) -> bool:
        """Check if value is list."""
        return isinstance(value, list)


class SetSerializer(TypeSerializer):
    """Serializer for SET collection type."""

    def serialize(self, value: Any, context: SerializationContext) -> Any:
        """Serialize set values."""
        # Handle Cassandra SortedSet
        if SortedSet and isinstance(value, SortedSet):
            # SortedSet is already sorted, just convert to list
            value_list = list(value)
        elif isinstance(value, (set, frozenset)):
            # Regular sets need sorting
            value_list = sorted(list(value), key=str)
        else:
            raise ValueError(f"SetSerializer expects set, got {type(value)}")

        # Import here to avoid circular import
        from .registry import get_global_registry

        registry = get_global_registry()

        # For nested collections in CSV, we need to avoid double-encoding
        if context.format == "csv":
            nested_context = SerializationContext(
                format="json", options=context.options, column_metadata=context.column_metadata
            )
        else:
            nested_context = context

        # Serialize each element
        serialized_items = []
        for item in value_list:
            serialized_items.append(registry.serialize(item, nested_context))

        if context.format == "csv":
            # CSV: JSON array string
            return json.dumps(serialized_items, default=str)
        elif context.format == "json":
            # JSON: array
            return serialized_items
        else:
            return value

    def can_handle(self, value: Any) -> bool:
        """Check if value is set."""
        if isinstance(value, (set, frozenset)):
            return True
        # Handle Cassandra SortedSet
        if SortedSet and isinstance(value, SortedSet):
            return True
        return False


class MapSerializer(TypeSerializer):
    """Serializer for MAP collection type."""

    def serialize(self, value: Any, context: SerializationContext) -> Any:
        """Serialize map values."""
        # Handle OrderedMapSerializedKey
        if OrderedMapSerializedKey and isinstance(value, OrderedMapSerializedKey):
            # Convert to regular dict
            value = dict(value)

        if not isinstance(value, dict):
            raise ValueError(f"MapSerializer expects dict, got {type(value)}")

        # Import here to avoid circular import
        from .registry import get_global_registry

        registry = get_global_registry()

        # For nested collections in CSV, we need to avoid double-encoding
        if context.format == "csv":
            nested_context = SerializationContext(
                format="json", options=context.options, column_metadata=context.column_metadata
            )
        else:
            nested_context = context

        # Serialize keys and values
        serialized_map = {}
        for k, v in value.items():
            # Keys might need serialization too
            serialized_key = registry.serialize(k, nested_context) if not isinstance(k, str) else k
            serialized_value = registry.serialize(v, nested_context)
            serialized_map[str(serialized_key)] = serialized_value

        if context.format == "csv":
            # CSV: JSON object string
            return json.dumps(serialized_map, default=str)
        elif context.format == "json":
            # JSON: native object
            return serialized_map
        else:
            return value

    def can_handle(self, value: Any) -> bool:
        """Check if value is dict."""
        if isinstance(value, dict):
            return True
        # Handle Cassandra OrderedMapSerializedKey
        if OrderedMapSerializedKey and isinstance(value, OrderedMapSerializedKey):
            return True
        return False


class TupleSerializer(TypeSerializer):
    """Serializer for TUPLE type."""

    def serialize(self, value: Any, context: SerializationContext) -> Any:
        """Serialize tuple values."""
        if not isinstance(value, tuple):
            raise ValueError(f"TupleSerializer expects tuple, got {type(value)}")

        # Import here to avoid circular import
        from .registry import get_global_registry

        registry = get_global_registry()

        # For nested collections in CSV, we need to avoid double-encoding
        if context.format == "csv":
            nested_context = SerializationContext(
                format="json", options=context.options, column_metadata=context.column_metadata
            )
        else:
            nested_context = context

        # Serialize each element
        serialized_items = []
        for item in value:
            serialized_items.append(registry.serialize(item, nested_context))

        if context.format == "csv":
            # CSV: JSON array string
            return json.dumps(serialized_items, default=str)
        elif context.format == "json":
            # JSON: convert to array (JSON doesn't have tuples)
            return serialized_items
        else:
            return value

    def can_handle(self, value: Any) -> bool:
        """Check if value is tuple (but not a UDT)."""
        if not isinstance(value, tuple):
            return False

        # Exclude UDTs (which are named tuples from cassandra.cqltypes)
        module = getattr(type(value), "__module__", "")
        if module == "cassandra.cqltypes":
            return False

        # Exclude other named tuples that might be UDTs
        if hasattr(value, "_fields") and hasattr(value, "_asdict"):
            return False

        return True


class FrozenCollectionSerializer(TypeSerializer):
    """
    Serializer for frozen collections.

    Frozen collections are immutable and serialized the same way
    as their non-frozen counterparts.
    """

    def __init__(self, inner_serializer: TypeSerializer):
        """
        Initialize with the serializer for the inner collection type.

        Args:
            inner_serializer: Serializer for the collection inside frozen()
        """
        self.inner_serializer = inner_serializer

    def serialize(self, value: Any, context: SerializationContext) -> Any:
        """Serialize frozen collection using inner serializer."""
        return self.inner_serializer.serialize(value, context)

    def can_handle(self, value: Any) -> bool:
        """Check if inner serializer can handle the value."""
        return self.inner_serializer.can_handle(value)

    def __repr__(self) -> str:
        """String representation."""
        return f"FrozenCollectionSerializer({self.inner_serializer})"


class UDTSerializer(TypeSerializer):
    """
    Serializer for User-Defined Types (UDT).

    UDTs are represented as named tuples or objects with attributes.
    """

    def serialize(self, value: Any, context: SerializationContext) -> Any:
        """Serialize UDT values."""
        # UDTs can be accessed as objects with attributes
        if hasattr(value, "_asdict"):
            # Named tuple - convert to dict
            udt_dict = value._asdict()
        elif hasattr(value, "__dict__"):
            # Object with attributes
            udt_dict = {k: v for k, v in value.__dict__.items() if not k.startswith("_")}
        else:
            # Try to extract fields dynamically
            udt_dict = {}
            for attr in dir(value):
                if not attr.startswith("_"):
                    try:
                        udt_dict[attr] = getattr(value, attr)
                    except Exception:
                        pass

        # Import here to avoid circular import
        from .registry import get_global_registry

        registry = get_global_registry()

        # For nested collections in CSV, we need to avoid double-encoding
        if context.format == "csv":
            nested_context = SerializationContext(
                format="json", options=context.options, column_metadata=context.column_metadata
            )
        else:
            nested_context = context

        # Serialize each field value
        serialized_dict = {}
        for k, v in udt_dict.items():
            serialized_dict[k] = registry.serialize(v, nested_context)

        if context.format == "csv":
            # CSV: JSON object string
            return json.dumps(serialized_dict, default=str)
        elif context.format == "json":
            # JSON: native object
            return serialized_dict
        else:
            return value

    def can_handle(self, value: Any) -> bool:
        """
        Check if value is a UDT.

        UDTs are typically custom objects or named tuples.
        This is a heuristic check.
        """
        # Check if it's from cassandra.cqltypes module (this is how UDTs are returned)
        module = getattr(type(value), "__module__", "")
        if module == "cassandra.cqltypes":
            return True

        # Check if it has a cassandra UDT marker
        if hasattr(value, "__cassandra_udt__"):
            return True

        # Check if it's from cassandra.usertype module
        if "cassandra" in module and "usertype" in module:
            return True

        # Check if it's a named tuple (but we already checked the module above)
        # This is a fallback for other named tuples that might be UDTs
        if hasattr(value, "_fields") and hasattr(value, "_asdict"):
            # But exclude regular tuples (which don't have these attributes)
            return True

        return False
