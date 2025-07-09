"""Parquet export implementation using PyArrow."""

import asyncio
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    raise ImportError(
        "PyArrow is required for Parquet export. Install with: pip install pyarrow"
    ) from None

from cassandra.util import OrderedMap, OrderedMapSerializedKey

from bulk_operations.exporters.base import Exporter, ExportFormat, ExportProgress


class ParquetExporter(Exporter):
    """Export Cassandra data to Parquet format - the foundation for Iceberg."""

    def __init__(
        self,
        operator,
        compression: str = "snappy",
        row_group_size: int = 50000,
        use_dictionary: bool = True,
        buffer_size: int = 8192,
    ):
        """Initialize Parquet exporter.

        Args:
            operator: Token-aware bulk operator instance
            compression: Compression codec (snappy, gzip, brotli, lz4, zstd)
            row_group_size: Number of rows per row group
            use_dictionary: Enable dictionary encoding for strings
            buffer_size: Buffer size for file operations
        """
        super().__init__(operator, compression, buffer_size)
        self.row_group_size = row_group_size
        self.use_dictionary = use_dictionary
        self._batch_rows = []
        self._schema = None
        self._writer = None

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
        """Export table data to Parquet format.

        What this does:
        --------------
        1. Converts Cassandra schema to Arrow schema
        2. Batches rows into row groups for efficiency
        3. Applies columnar compression
        4. Creates Parquet files ready for Iceberg

        Why this matters:
        ----------------
        - Parquet is the storage format for Iceberg
        - Columnar format enables analytics
        - Excellent compression ratios
        - Schema evolution support
        """
        # Get table metadata
        metadata = self.operator.session._session.cluster.metadata
        keyspace_metadata = metadata.keyspaces.get(keyspace)
        if not keyspace_metadata:
            raise ValueError(f"Keyspace '{keyspace}' not found")
        table_metadata = keyspace_metadata.tables.get(table)
        if not table_metadata:
            raise ValueError(f"Table '{keyspace}.{table}' not found")

        # Get columns
        if columns is None:
            columns = list(table_metadata.columns.keys())

        # Build Arrow schema from Cassandra schema
        self._schema = self._build_arrow_schema(table_metadata, columns)

        # Initialize progress
        if progress is None:
            progress = ExportProgress(
                export_id=str(uuid.uuid4()),
                keyspace=keyspace,
                table=table,
                format=ExportFormat.PARQUET,
                output_path=str(output_path),
                started_at=datetime.now(UTC),
                metadata={
                    "compression": self.compression,
                    "row_group_size": self.row_group_size,
                },
            )

        # Note: Parquet doesn't use compression extension in filename
        # Compression is internal to the format

        try:
            # Open Parquet writer
            self._writer = pq.ParquetWriter(
                output_path,
                self._schema,
                compression=self.compression,
                use_dictionary=self.use_dictionary,
            )

            # Export by token ranges
            async for row in self.operator.export_by_token_ranges(
                keyspace=keyspace,
                table=table,
                split_count=split_count,
                parallelism=parallelism,
                consistency_level=consistency_level,
            ):
                # Add row to batch
                row_data = self._convert_row_to_dict(row, columns)
                self._batch_rows.append(row_data)

                # Write batch when full
                if len(self._batch_rows) >= self.row_group_size:
                    await self._write_batch()
                    progress.bytes_written = output_path.stat().st_size

                progress.rows_exported += 1

                # Progress callback
                if progress_callback and progress.rows_exported % 1000 == 0:
                    if asyncio.iscoroutinefunction(progress_callback):
                        await progress_callback(progress)
                    else:
                        progress_callback(progress)

            # Write final batch
            if self._batch_rows:
                await self._write_batch()

            # Close writer
            self._writer.close()

            # Final stats
            progress.bytes_written = output_path.stat().st_size
            progress.completed_at = datetime.now(UTC)

            # Final callback
            if progress_callback:
                if asyncio.iscoroutinefunction(progress_callback):
                    await progress_callback(progress)
                else:
                    progress_callback(progress)

        except Exception:
            # Ensure writer is closed on error
            if self._writer:
                self._writer.close()
            raise

        # Save progress
        progress.save()
        return progress

    def _build_arrow_schema(self, table_metadata, columns):
        """Build PyArrow schema from Cassandra table metadata."""
        fields = []

        for col_name in columns:
            col_meta = table_metadata.columns.get(col_name)
            if not col_meta:
                continue

            # Map Cassandra types to Arrow types
            arrow_type = self._cassandra_to_arrow_type(col_meta.cql_type)
            fields.append(pa.field(col_name, arrow_type, nullable=True))

        return pa.schema(fields)

    def _cassandra_to_arrow_type(self, cql_type: str) -> pa.DataType:
        """Map Cassandra types to PyArrow types."""
        # Handle parameterized types
        base_type = cql_type.split("<")[0].lower()

        type_mapping = {
            "ascii": pa.string(),
            "bigint": pa.int64(),
            "blob": pa.binary(),
            "boolean": pa.bool_(),
            "counter": pa.int64(),
            "date": pa.date32(),
            "decimal": pa.decimal128(38, 10),  # Max precision
            "double": pa.float64(),
            "float": pa.float32(),
            "inet": pa.string(),
            "int": pa.int32(),
            "smallint": pa.int16(),
            "text": pa.string(),
            "time": pa.int64(),  # Nanoseconds since midnight
            "timestamp": pa.timestamp("us"),  # Microsecond precision
            "timeuuid": pa.string(),
            "tinyint": pa.int8(),
            "uuid": pa.string(),
            "varchar": pa.string(),
            "varint": pa.string(),  # Store as string for arbitrary precision
        }

        # Handle collections
        if base_type == "list" or base_type == "set":
            element_type = self._extract_collection_type(cql_type)
            return pa.list_(self._cassandra_to_arrow_type(element_type))
        elif base_type == "map":
            key_type, value_type = self._extract_map_types(cql_type)
            return pa.map_(
                self._cassandra_to_arrow_type(key_type),
                self._cassandra_to_arrow_type(value_type),
            )

        return type_mapping.get(base_type, pa.string())  # Default to string

    def _extract_collection_type(self, cql_type: str) -> str:
        """Extract element type from list<type> or set<type>."""
        start = cql_type.index("<") + 1
        end = cql_type.rindex(">")
        return cql_type[start:end].strip()

    def _extract_map_types(self, cql_type: str) -> tuple[str, str]:
        """Extract key and value types from map<key_type, value_type>."""
        start = cql_type.index("<") + 1
        end = cql_type.rindex(">")
        types = cql_type[start:end].split(",", 1)
        return types[0].strip(), types[1].strip()

    def _convert_row_to_dict(self, row: Any, columns: list[str]) -> dict[str, Any]:
        """Convert Cassandra row to dictionary with proper type conversion."""
        row_dict = {}

        if hasattr(row, "_fields"):
            for field in row._fields:
                value = getattr(row, field)
                row_dict[field] = self._convert_value_for_arrow(value)
        else:
            for i, col in enumerate(columns):
                if i < len(row):
                    row_dict[col] = self._convert_value_for_arrow(row[i])

        return row_dict

    def _convert_value_for_arrow(self, value: Any) -> Any:
        """Convert Cassandra value to Arrow-compatible format."""
        if value is None:
            return None
        elif isinstance(value, uuid.UUID):
            return str(value)
        elif isinstance(value, Decimal):
            # Keep as Decimal for Arrow's decimal128 type
            return value
        elif isinstance(value, set):
            # Convert sets to lists
            return list(value)
        elif isinstance(value, OrderedMap | OrderedMapSerializedKey):
            # Convert Cassandra map types to dict
            return dict(value)
        elif isinstance(value, bytes):
            # Keep as bytes for binary columns
            return value
        elif isinstance(value, datetime):
            # Ensure timezone aware
            if value.tzinfo is None:
                return value.replace(tzinfo=UTC)
            return value
        else:
            return value

    async def _write_batch(self):
        """Write accumulated batch to Parquet file."""
        if not self._batch_rows:
            return

        # Convert to Arrow Table
        table = pa.Table.from_pylist(self._batch_rows, schema=self._schema)

        # Write to file
        async with self._write_lock:
            self._writer.write_table(table)

        # Clear batch
        self._batch_rows = []

    async def write_header(self, file_handle: Any, columns: list[str]) -> None:
        """Parquet handles headers internally."""
        pass

    async def write_row(self, file_handle: Any, row: Any) -> int:
        """Parquet uses batch writing, not row-by-row."""
        # This is handled in export() method
        return 0

    async def write_footer(self, file_handle: Any) -> None:
        """Parquet handles footers internally."""
        pass
