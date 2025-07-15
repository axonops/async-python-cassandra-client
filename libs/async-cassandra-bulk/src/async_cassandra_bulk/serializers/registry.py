"""
Serializer registry for managing type serializers.

Provides a central registry for looking up appropriate serializers
based on value types and handles serialization dispatch.
"""

from typing import Any, Dict, List, Optional, Type

from .base import SerializationContext, TypeSerializer
from .basic_types import (
    BinarySerializer,
    BooleanSerializer,
    CounterSerializer,
    DateSerializer,
    DecimalSerializer,
    DurationSerializer,
    FloatSerializer,
    InetSerializer,
    IntegerSerializer,
    NullSerializer,
    StringSerializer,
    TimeSerializer,
    TimestampSerializer,
    UUIDSerializer,
    VectorSerializer,
)
from .collection_types import (
    ListSerializer,
    MapSerializer,
    SetSerializer,
    TupleSerializer,
    UDTSerializer,
)


class SerializerRegistry:
    """
    Registry for type serializers.

    Manages serializer lookup and provides a central point for
    serialization of all Cassandra types.
    """

    def __init__(self) -> None:
        """Initialize the registry with empty serializer list."""
        self._serializers: List[TypeSerializer] = []
        self._type_cache: Dict[Type, TypeSerializer] = {}

    def register(self, serializer: TypeSerializer) -> None:
        """
        Register a type serializer.

        Args:
            serializer: The serializer to register
        """
        self._serializers.append(serializer)
        # Clear cache when registry changes
        self._type_cache.clear()

    def find_serializer(self, value: Any) -> Optional[TypeSerializer]:
        """
        Find appropriate serializer for a value.

        Args:
            value: The value to find a serializer for

        Returns:
            Appropriate serializer or None if not found
        """
        # Check cache first
        value_type = type(value)
        if value_type in self._type_cache:
            return self._type_cache[value_type]

        # Find serializer that can handle this value
        for serializer in self._serializers:
            if serializer.can_handle(value):
                # Cache for faster lookup
                self._type_cache[value_type] = serializer
                return serializer

        return None

    def serialize(self, value: Any, context: SerializationContext) -> Any:
        """
        Serialize a value using appropriate serializer.

        Args:
            value: The value to serialize
            context: Serialization context

        Returns:
            Serialized value

        Raises:
            ValueError: If no appropriate serializer found
        """
        serializer = self.find_serializer(value)
        if serializer is None:
            # Fallback to string representation
            if context.format == "csv":
                return str(value)
            else:
                # For JSON/Parquet, try to return value as-is
                # and let the format handler deal with it
                return value

        # Let the serializer handle its own value
        return serializer.serialize(value, context)


def get_default_registry() -> SerializerRegistry:
    """
    Get a registry with all default serializers registered.

    Returns:
        Registry with all built-in serializers
    """
    registry = SerializerRegistry()

    # Register serializers in order of specificity
    # Null first (most specific)
    registry.register(NullSerializer())

    # Basic types
    registry.register(BooleanSerializer())
    registry.register(IntegerSerializer())
    registry.register(FloatSerializer())
    registry.register(DecimalSerializer())
    registry.register(StringSerializer())
    registry.register(BinarySerializer())
    registry.register(UUIDSerializer())

    # Temporal types
    registry.register(TimestampSerializer())
    registry.register(DateSerializer())
    registry.register(TimeSerializer())
    registry.register(DurationSerializer())

    # Network types
    registry.register(InetSerializer())

    # Special numeric types
    registry.register(CounterSerializer())

    # Complex types (before collections to avoid false matches)
    registry.register(UDTSerializer())

    # Vector must come before List to properly detect numeric arrays
    registry.register(VectorSerializer())

    # Collection types
    registry.register(ListSerializer())
    registry.register(SetSerializer())
    registry.register(MapSerializer())
    registry.register(TupleSerializer())

    return registry


# Global default registry
_default_registry = None


def get_global_registry() -> SerializerRegistry:
    """
    Get the global default registry (singleton).

    Returns:
        The global registry instance
    """
    global _default_registry
    if _default_registry is None:
        _default_registry = get_default_registry()
    return _default_registry


def reset_global_registry() -> None:
    """Reset the global registry (mainly for testing)."""
    global _default_registry
    _default_registry = None
