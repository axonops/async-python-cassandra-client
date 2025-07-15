"""
JSON exporter implementation.

Exports Cassandra data to JSON format with support for both array mode
(single JSON array) and objects mode (newline-delimited JSON).
"""

import asyncio
import json
from typing import Any, Dict, List, Optional

from async_cassandra_bulk.exporters.base import BaseExporter
from async_cassandra_bulk.serializers import SerializationContext, get_global_registry
from async_cassandra_bulk.serializers.writetime import WritetimeColumnSerializer


class CassandraJSONEncoder(json.JSONEncoder):
    """
    Custom JSON encoder for Cassandra types.

    Uses the serialization registry to handle all Cassandra types.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize with serialization options."""
        self.serialization_options = kwargs.pop("serialization_options", {})
        self._writetime_serializer = WritetimeColumnSerializer()
        super().__init__(*args, **kwargs)

    def encode(self, o: Any) -> str:
        """Override encode to pre-process objects before JSON encoding."""
        # Pre-process the object tree to handle Cassandra types
        processed = self._pre_process(o)
        # Then use the standard encoder
        return super().encode(processed)

    def _pre_process(self, obj: Any, key: Optional[str] = None) -> Any:
        """Pre-process objects to handle Cassandra types before JSON sees them."""
        # Create serialization context
        context = SerializationContext(
            format="json",
            options=self.serialization_options,
        )

        # Check if this is a writetime column by key
        if key and isinstance(obj, (int, type(None))):
            is_writetime, result = self._writetime_serializer.serialize_if_writetime(
                key, obj, context
            )
            if is_writetime:
                return result

        # Use the global registry
        registry = get_global_registry()

        # Handle dict - recurse into values, passing keys
        if isinstance(obj, dict):
            return {k: self._pre_process(v, k) for k, v in obj.items()}
        # Handle list - recurse into items
        elif isinstance(obj, list):
            return [self._pre_process(item) for item in obj]
        # For everything else, let the registry handle it
        else:
            # The registry will convert UDTs to dicts, etc.
            return registry.serialize(obj, context)

    def default(self, obj: Any) -> Any:
        """
        Convert Cassandra types to JSON-serializable formats.

        Args:
            obj: Object to convert

        Returns:
            JSON-serializable representation
        """
        # Create serialization context
        context = SerializationContext(
            format="json",
            options=self.serialization_options,
        )

        # Use the global registry to serialize
        registry = get_global_registry()
        result = registry.serialize(obj, context)

        # If registry couldn't handle it, let default encoder try
        if result is obj:
            return super().default(obj)

        return result


class JSONExporter(BaseExporter):
    """
    JSON format exporter.

    Supports two modes:
    - array: Single JSON array containing all rows (default)
    - objects: Newline-delimited JSON objects (JSONL format)

    Handles all Cassandra types with appropriate conversions.
    """

    def __init__(self, output_path: str, options: Optional[Dict[str, Any]] = None) -> None:
        """
        Initialize JSON exporter with formatting options.

        Args:
            output_path: Path where to write the JSON file
            options: JSON-specific options:
                - mode: 'array' or 'objects' (default: 'array')
                - pretty: Enable pretty printing (default: False)
                - streaming: Enable streaming mode (default: False)
        """
        super().__init__(output_path, options)

        # Extract JSON options with defaults
        self.mode = self.options.get("mode", "array")
        self.pretty = self.options.get("pretty", False)
        self.streaming = self.options.get("streaming", False)

        # Internal state
        self._columns: List[str] = []
        self._first_row = True
        self._encoder = CassandraJSONEncoder(
            indent=2 if self.pretty else None,
            ensure_ascii=False,
            serialization_options=self.options,
        )
        self._write_lock = asyncio.Lock()  # For thread-safe writes in array mode

    async def write_header(self, columns: List[str]) -> None:
        """
        Write JSON header based on mode.

        Args:
            columns: List of column names

        Note:
            - Array mode: Opens JSON array with '['
            - Objects mode: No header needed
        """
        # Ensure file is open
        await self._ensure_file_open()

        self._columns = columns
        self._first_row = True

        if self.mode == "array" and self._file:
            await self._file.write("[")

    async def write_row(self, row: Dict[str, Any]) -> None:
        """
        Write a single row to JSON.

        Args:
            row: Dictionary mapping column names to values

        Note:
            Handles proper formatting for both array and objects modes
        """
        if not self._file:
            return

        # Convert row to JSON
        json_str = self._encoder.encode(row)

        if self.mode == "array":
            # Array mode - use lock to ensure thread-safe writes
            async with self._write_lock:
                # Add comma before non-first rows
                if self._first_row:
                    await self._file.write(json_str)
                    self._first_row = False
                else:
                    await self._file.write("," + json_str)
        else:
            # Objects mode - each row on its own line
            await self._file.write(json_str + "\n")

    async def write_footer(self) -> None:
        """
        Write JSON footer based on mode.

        Note:
            - Array mode: Closes array with ']'
            - Objects mode: No footer needed
        """
        if self.mode == "array" and self._file:
            await self._file.write("]\n")
