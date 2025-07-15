"""
CSV exporter implementation.

Exports Cassandra data to CSV format with proper type conversions and
configurable formatting options.
"""

import csv
import io
from typing import Any, Dict, List, Optional

from async_cassandra_bulk.exporters.base import BaseExporter
from async_cassandra_bulk.serializers import SerializationContext, get_global_registry
from async_cassandra_bulk.serializers.writetime import WritetimeColumnSerializer


class CSVExporter(BaseExporter):
    """
    CSV format exporter.

    Handles conversion of Cassandra types to CSV-compatible string representations
    with support for custom delimiters, quotes, and null handling.
    """

    def __init__(self, output_path: str, options: Optional[Dict[str, Any]] = None) -> None:
        """
        Initialize CSV exporter with formatting options.

        Args:
            output_path: Path where to write the CSV file
            options: CSV-specific options:
                - delimiter: Field delimiter (default: ',')
                - quote_char: Quote character (default: '"')
                - include_header: Write header row (default: True)
                - null_value: String for NULL values (default: '')

        """
        super().__init__(output_path, options)

        # Extract CSV options with defaults
        self.delimiter = self.options.get("delimiter", ",")
        self.quote_char = self.options.get("quote_char", '"')
        self.escape_char = self.options.get("escape_char", "\\")
        self.include_header = self.options.get("include_header", True)
        self.null_value = self.options.get("null_value", "")

        # CSV writer will be initialized when we know the columns
        self._writer: Optional[csv.DictWriter[str]] = None
        self._buffer: Optional[io.StringIO] = None

        # Writetime column handler
        self._writetime_serializer = WritetimeColumnSerializer()

    def _convert_value(self, value: Any, column_name: Optional[str] = None) -> str:
        """
        Convert Cassandra types to CSV-compatible strings.

        Args:
            value: Value to convert
            column_name: Optional column name for writetime detection

        Returns:
            String representation suitable for CSV

        Note:
            Uses the serialization registry to handle all Cassandra types
        """
        # Create serialization context
        context = SerializationContext(
            format="csv",
            options={
                "null_value": self.null_value,
                "escape_char": self.escape_char,
                "quote_char": self.quote_char,
                "writetime_format": self.options.get("writetime_format"),
            },
        )

        # Check if this is a writetime column
        if column_name:
            is_writetime, result = self._writetime_serializer.serialize_if_writetime(
                column_name, value, context
            )
            if is_writetime:
                return str(result) if not isinstance(result, str) else result

        # Use the global registry to serialize
        registry = get_global_registry()
        result = registry.serialize(value, context)

        # Ensure result is string
        return str(result) if not isinstance(result, str) else result

    async def write_header(self, columns: List[str]) -> None:
        """
        Write CSV header with column names.

        Args:
            columns: List of column names

        Note:
            Only writes if include_header is True
        """
        # Ensure file is open
        await self._ensure_file_open()

        # Initialize CSV writer with columns
        self._buffer = io.StringIO()
        self._writer = csv.DictWriter(
            self._buffer,
            fieldnames=columns,
            delimiter=self.delimiter,
            quotechar=self.quote_char,
            quoting=csv.QUOTE_MINIMAL,
        )

        # Write header if enabled
        if self.include_header and self._writer and self._buffer and self._file:
            self._writer.writeheader()
            # Get the content and write to file
            self._buffer.seek(0)
            content = self._buffer.read()
            self._buffer.truncate(0)
            self._buffer.seek(0)
            await self._file.write(content)

    async def write_row(self, row: Dict[str, Any]) -> None:
        """
        Write a single row to CSV.

        Args:
            row: Dictionary mapping column names to values

        Note:
            Converts all values to appropriate string representations
        """
        if not self._writer:
            raise RuntimeError("write_header must be called before write_row")

        # Convert all values, passing column names for writetime detection
        converted_row = {key: self._convert_value(value, key) for key, value in row.items()}

        # Write to buffer
        self._writer.writerow(converted_row)

        # Get content from buffer and write to file
        if self._buffer and self._file:
            self._buffer.seek(0)
            content = self._buffer.read()
            self._buffer.truncate(0)
            self._buffer.seek(0)
            await self._file.write(content)

    async def write_footer(self) -> None:
        """
        Write CSV footer.

        Note:
            CSV files don't have footers, so this does nothing
        """
        pass  # CSV has no footer
