"""CSV export implementation."""

import asyncio
import csv
import io
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from bulk_operations.exporters.base import Exporter, ExportFormat, ExportProgress


class CSVExporter(Exporter):
    """Export Cassandra data to CSV format with streaming support."""

    def __init__(
        self,
        operator,
        delimiter: str = ",",
        quoting: int = csv.QUOTE_MINIMAL,
        null_string: str = "",
        compression: str | None = None,
        buffer_size: int = 8192,
    ):
        """Initialize CSV exporter.

        Args:
            operator: Token-aware bulk operator instance
            delimiter: Field delimiter (default: comma)
            quoting: CSV quoting style (default: QUOTE_MINIMAL)
            null_string: String to represent NULL values (default: empty string)
            compression: Compression type (gzip, bz2, lz4)
            buffer_size: Buffer size for file operations
        """
        super().__init__(operator, compression, buffer_size)
        self.delimiter = delimiter
        self.quoting = quoting
        self.null_string = null_string

    async def export(  # noqa: C901
        self,
        keyspace: str,
        table: str,
        output_path: Path,
        columns: list[str] | None = None,
        split_count: int | None = None,
        parallelism: int | None = None,
        progress: ExportProgress | None = None,
        progress_callback: Any | None = None,
        consistency_level: Any | None = None,
    ) -> ExportProgress:
        """Export table data to CSV format.

        What this does:
        --------------
        1. Discovers table schema if columns not specified
        2. Creates/resumes progress tracking
        3. Streams data by token ranges
        4. Writes CSV with proper escaping
        5. Supports compression and resume

        Why this matters:
        ----------------
        - Memory efficient for large tables
        - Maintains data fidelity
        - Resume capability for long exports
        - Compatible with standard tools
        """
        # Get table metadata if columns not specified
        if columns is None:
            metadata = self.operator.session._session.cluster.metadata
            keyspace_metadata = metadata.keyspaces.get(keyspace)
            if not keyspace_metadata:
                raise ValueError(f"Keyspace '{keyspace}' not found")
            table_metadata = keyspace_metadata.tables.get(table)
            if not table_metadata:
                raise ValueError(f"Table '{keyspace}.{table}' not found")
            columns = list(table_metadata.columns.keys())

        # Initialize or resume progress
        if progress is None:
            progress = ExportProgress(
                export_id=str(uuid.uuid4()),
                keyspace=keyspace,
                table=table,
                format=ExportFormat.CSV,
                output_path=str(output_path),
                started_at=datetime.now(UTC),
            )

        # Get actual output path with compression extension
        actual_output_path = self._get_output_path_with_compression(output_path)

        # Open output file (append mode if resuming)
        mode = "a" if progress.completed_ranges else "w"
        file_handle = await self._open_output_file(actual_output_path, mode)

        try:
            # Write header for new exports
            if mode == "w":
                await self.write_header(file_handle, columns)

            # Store columns for row filtering
            self._export_columns = columns

            # Track bytes written
            file_handle.tell() if hasattr(file_handle, "tell") else 0

            # Export by token ranges
            async for row in self.operator.export_by_token_ranges(
                keyspace=keyspace,
                table=table,
                split_count=split_count,
                parallelism=parallelism,
                consistency_level=consistency_level,
            ):
                # Check if we need to track a new range
                # (This is simplified - in real implementation we'd track actual ranges)
                bytes_written = await self.write_row(file_handle, row)
                progress.rows_exported += 1
                progress.bytes_written += bytes_written

                # Periodic progress callback
                if progress_callback and progress.rows_exported % 1000 == 0:
                    if asyncio.iscoroutinefunction(progress_callback):
                        await progress_callback(progress)
                    else:
                        progress_callback(progress)

            # Mark completion
            progress.completed_at = datetime.now(UTC)

            # Final callback
            if progress_callback:
                if asyncio.iscoroutinefunction(progress_callback):
                    await progress_callback(progress)
                else:
                    progress_callback(progress)

        finally:
            if hasattr(file_handle, "close"):
                file_handle.close()

        # Save final progress
        progress.save()
        return progress

    async def write_header(self, file_handle: Any, columns: list[str]) -> None:
        """Write CSV header row."""
        writer = csv.writer(file_handle, delimiter=self.delimiter, quoting=self.quoting)
        writer.writerow(columns)

    async def write_row(self, file_handle: Any, row: Any) -> int:
        """Write a single row to CSV."""
        # Convert row to list of values in column order
        # Row objects from Cassandra driver have _fields attribute
        values = []
        if hasattr(row, "_fields"):
            # If we have specific columns, only export those
            if hasattr(self, "_export_columns") and self._export_columns:
                for col in self._export_columns:
                    if hasattr(row, col):
                        value = getattr(row, col)
                        values.append(self._serialize_csv_value(value))
                    else:
                        values.append(self._serialize_csv_value(None))
            else:
                # Export all fields
                for field in row._fields:
                    value = getattr(row, field)
                    values.append(self._serialize_csv_value(value))
        else:
            # Fallback for other row types
            for i in range(len(row)):
                values.append(self._serialize_csv_value(row[i]))

        # Write to string buffer first to calculate bytes
        buffer = io.StringIO()
        writer = csv.writer(buffer, delimiter=self.delimiter, quoting=self.quoting)
        writer.writerow(values)
        row_data = buffer.getvalue()

        # Write to actual file
        async with self._write_lock:
            file_handle.write(row_data)
            if hasattr(file_handle, "flush"):
                file_handle.flush()

        return len(row_data.encode("utf-8"))

    async def write_footer(self, file_handle: Any) -> None:
        """CSV files don't have footers."""
        pass

    def _serialize_csv_value(self, value: Any) -> str:
        """Serialize value for CSV output."""
        if value is None:
            return self.null_string
        elif isinstance(value, bool):
            return "true" if value else "false"
        elif isinstance(value, list | set):
            # Format collections as [item1, item2, ...]
            items = [self._serialize_csv_value(v) for v in value]
            return f"[{', '.join(items)}]"
        elif isinstance(value, dict):
            # Format maps as {key1: value1, key2: value2}
            items = [
                f"{self._serialize_csv_value(k)}: {self._serialize_csv_value(v)}"
                for k, v in value.items()
            ]
            return f"{{{', '.join(items)}}}"
        elif isinstance(value, bytes):
            # Hex encode bytes
            return value.hex()
        elif isinstance(value, datetime):
            return value.isoformat()
        elif isinstance(value, uuid.UUID):
            return str(value)
        else:
            return str(value)
