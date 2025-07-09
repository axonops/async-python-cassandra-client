"""JSON export implementation."""

import asyncio
import json
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from bulk_operations.exporters.base import Exporter, ExportFormat, ExportProgress


class JSONExporter(Exporter):
    """Export Cassandra data to JSON format (line-delimited by default)."""

    def __init__(
        self,
        operator,
        format_mode: str = "jsonl",  # jsonl (line-delimited) or array
        indent: int | None = None,
        compression: str | None = None,
        buffer_size: int = 8192,
    ):
        """Initialize JSON exporter.

        Args:
            operator: Token-aware bulk operator instance
            format_mode: Output format - 'jsonl' (line-delimited) or 'array'
            indent: JSON indentation (None for compact)
            compression: Compression type (gzip, bz2, lz4)
            buffer_size: Buffer size for file operations
        """
        super().__init__(operator, compression, buffer_size)
        self.format_mode = format_mode
        self.indent = indent
        self._first_row = True

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
        """Export table data to JSON format.

        What this does:
        --------------
        1. Exports as line-delimited JSON (default) or JSON array
        2. Handles all Cassandra data types with proper serialization
        3. Supports compression for smaller files
        4. Maintains streaming for memory efficiency

        Why this matters:
        ----------------
        - JSONL works well with streaming tools
        - JSON arrays are compatible with many APIs
        - Preserves type information better than CSV
        - Standard format for data pipelines
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
                format=ExportFormat.JSON,
                output_path=str(output_path),
                started_at=datetime.now(UTC),
                metadata={"format_mode": self.format_mode},
            )

        # Get actual output path with compression extension
        actual_output_path = self._get_output_path_with_compression(output_path)

        # Open output file
        mode = "a" if progress.completed_ranges else "w"
        file_handle = await self._open_output_file(actual_output_path, mode)

        try:
            # Write header for array mode
            if mode == "w" and self.format_mode == "array":
                await self.write_header(file_handle, columns)

            # Store columns for row filtering
            self._export_columns = columns

            # Export by token ranges
            async for row in self.operator.export_by_token_ranges(
                keyspace=keyspace,
                table=table,
                split_count=split_count,
                parallelism=parallelism,
                consistency_level=consistency_level,
            ):
                bytes_written = await self.write_row(file_handle, row)
                progress.rows_exported += 1
                progress.bytes_written += bytes_written

                # Progress callback
                if progress_callback and progress.rows_exported % 1000 == 0:
                    if asyncio.iscoroutinefunction(progress_callback):
                        await progress_callback(progress)
                    else:
                        progress_callback(progress)

            # Write footer for array mode
            if self.format_mode == "array":
                await self.write_footer(file_handle)

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

        # Save progress
        progress.save()
        return progress

    async def write_header(self, file_handle: Any, columns: list[str]) -> None:
        """Write JSON array opening bracket."""
        if self.format_mode == "array":
            file_handle.write("[\n")
            self._first_row = True

    async def write_row(self, file_handle: Any, row: Any) -> int:  # noqa: C901
        """Write a single row as JSON."""
        # Convert row to dictionary
        row_dict = {}
        if hasattr(row, "_fields"):
            # If we have specific columns, only export those
            if hasattr(self, "_export_columns") and self._export_columns:
                for col in self._export_columns:
                    if hasattr(row, col):
                        value = getattr(row, col)
                        row_dict[col] = self._serialize_value(value)
                    else:
                        row_dict[col] = None
            else:
                # Export all fields
                for field in row._fields:
                    value = getattr(row, field)
                    row_dict[field] = self._serialize_value(value)
        else:
            # Handle other row types
            for i, value in enumerate(row):
                row_dict[f"column_{i}"] = self._serialize_value(value)

        # Format as JSON
        if self.format_mode == "jsonl":
            # Line-delimited JSON
            json_str = json.dumps(row_dict, separators=(",", ":"))
            json_str += "\n"
        else:
            # Array mode
            if not self._first_row:
                json_str = ",\n"
            else:
                json_str = ""
                self._first_row = False

            if self.indent:
                json_str += json.dumps(row_dict, indent=self.indent)
            else:
                json_str += json.dumps(row_dict, separators=(",", ":"))

        # Write to file
        async with self._write_lock:
            file_handle.write(json_str)
            if hasattr(file_handle, "flush"):
                file_handle.flush()

        return len(json_str.encode("utf-8"))

    async def write_footer(self, file_handle: Any) -> None:
        """Write JSON array closing bracket."""
        if self.format_mode == "array":
            file_handle.write("\n]")

    def _serialize_value(self, value: Any) -> Any:
        """Override to handle UUID and other types."""
        if isinstance(value, uuid.UUID):
            return str(value)
        elif isinstance(value, set | frozenset):
            # JSON doesn't have sets, convert to list
            return [self._serialize_value(v) for v in sorted(value)]
        elif hasattr(value, "__class__") and "SortedSet" in value.__class__.__name__:
            # Handle SortedSet specifically
            return [self._serialize_value(v) for v in value]
        elif isinstance(value, Decimal):
            # Convert Decimal to float for JSON
            return float(value)
        else:
            # Use parent class serialization
            return super()._serialize_value(value)
