"""
Base serializer interface and context.

Defines the contract for type serializers and provides
context for serialization operations.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class SerializationContext:
    """
    Context for serialization operations.

    Provides format-specific options and metadata for serializers.
    """

    format: str  # Target format (csv, json, parquet, etc.)
    options: Dict[str, Any]  # Format-specific options
    column_metadata: Optional[Dict[str, Any]] = None  # Column type information

    def get_option(self, key: str, default: Any = None) -> Any:
        """Get a serialization option with default."""
        return self.options.get(key, default)


class TypeSerializer(ABC):
    """
    Abstract base class for type serializers.

    Each Cassandra type should have a serializer that knows how to
    convert values to different output formats.
    """

    @abstractmethod
    def serialize(self, value: Any, context: SerializationContext) -> Any:
        """
        Serialize a value for the target format.

        Args:
            value: The value to serialize (can be None)
            context: Serialization context with format and options

        Returns:
            Serialized value appropriate for the target format
        """
        pass

    @abstractmethod
    def can_handle(self, value: Any) -> bool:
        """
        Check if this serializer can handle the given value.

        Args:
            value: The value to check

        Returns:
            True if this serializer can handle the value type
        """
        pass

    def __repr__(self) -> str:
        """String representation of the serializer."""
        return f"{self.__class__.__name__}()"
