"""Base classes for export format implementations."""

import asyncio
import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

from cassandra.util import OrderedMap, OrderedMapSerializedKey

from bulk_operations.bulk_operator import TokenAwareBulkOperator


class ExportFormat(Enum):
    """Supported export formats."""

    CSV = "csv"
    JSON = "json"
    PARQUET = "parquet"
    ICEBERG = "iceberg"


@dataclass
class ExportProgress:
    """Tracks export progress for resume capability."""

    export_id: str
    keyspace: str
    table: str
    format: ExportFormat
    output_path: str
    started_at: datetime
    completed_at: datetime | None = None
    total_ranges: int = 0
    completed_ranges: list[tuple[int, int]] = field(default_factory=list)
    rows_exported: int = 0
    bytes_written: int = 0
    errors: list[dict[str, Any]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> str:
        """Serialize progress to JSON."""
        data = {
            "export_id": self.export_id,
            "keyspace": self.keyspace,
            "table": self.table,
            "format": self.format.value,
            "output_path": self.output_path,
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "total_ranges": self.total_ranges,
            "completed_ranges": self.completed_ranges,
            "rows_exported": self.rows_exported,
            "bytes_written": self.bytes_written,
            "errors": self.errors,
            "metadata": self.metadata,
        }
        return json.dumps(data, indent=2)

    @classmethod
    def from_json(cls, json_str: str) -> "ExportProgress":
        """Deserialize progress from JSON."""
        data = json.loads(json_str)
        return cls(
            export_id=data["export_id"],
            keyspace=data["keyspace"],
            table=data["table"],
            format=ExportFormat(data["format"]),
            output_path=data["output_path"],
            started_at=datetime.fromisoformat(data["started_at"]),
            completed_at=(
                datetime.fromisoformat(data["completed_at"]) if data["completed_at"] else None
            ),
            total_ranges=data["total_ranges"],
            completed_ranges=[(r[0], r[1]) for r in data["completed_ranges"]],
            rows_exported=data["rows_exported"],
            bytes_written=data["bytes_written"],
            errors=data["errors"],
            metadata=data["metadata"],
        )

    def save(self, progress_file: Path | None = None) -> Path:
        """Save progress to file."""
        if progress_file is None:
            progress_file = Path(f"{self.output_path}.progress")
        progress_file.write_text(self.to_json())
        return progress_file

    @classmethod
    def load(cls, progress_file: Path) -> "ExportProgress":
        """Load progress from file."""
        return cls.from_json(progress_file.read_text())

    def is_range_completed(self, start: int, end: int) -> bool:
        """Check if a token range has been completed."""
        return (start, end) in self.completed_ranges

    def mark_range_completed(self, start: int, end: int, rows: int) -> None:
        """Mark a token range as completed."""
        if not self.is_range_completed(start, end):
            self.completed_ranges.append((start, end))
            self.rows_exported += rows

    @property
    def is_complete(self) -> bool:
        """Check if export is complete."""
        return len(self.completed_ranges) == self.total_ranges

    @property
    def progress_percentage(self) -> float:
        """Calculate progress percentage."""
        if self.total_ranges == 0:
            return 0.0
        return (len(self.completed_ranges) / self.total_ranges) * 100


class Exporter(ABC):
    """Base class for export format implementations."""

    def __init__(
        self,
        operator: TokenAwareBulkOperator,
        compression: str | None = None,
        buffer_size: int = 8192,
    ):
        """Initialize exporter.

        Args:
            operator: Token-aware bulk operator instance
            compression: Compression type (gzip, bz2, lz4, etc.)
            buffer_size: Buffer size for file operations
        """
        self.operator = operator
        self.compression = compression
        self.buffer_size = buffer_size
        self._write_lock = asyncio.Lock()

    @abstractmethod
    async def export(
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
        """Export table data to the specified format.

        Args:
            keyspace: Keyspace name
            table: Table name
            output_path: Output file path
            columns: Columns to export (None for all)
            split_count: Number of token range splits
            parallelism: Max concurrent operations
            progress: Resume from previous progress
            progress_callback: Callback for progress updates

        Returns:
            ExportProgress with final statistics
        """
        pass

    @abstractmethod
    async def write_header(self, file_handle: Any, columns: list[str]) -> None:
        """Write file header if applicable."""
        pass

    @abstractmethod
    async def write_row(self, file_handle: Any, row: Any) -> int:
        """Write a single row and return bytes written."""
        pass

    @abstractmethod
    async def write_footer(self, file_handle: Any) -> None:
        """Write file footer if applicable."""
        pass

    def _serialize_value(self, value: Any) -> Any:
        """Serialize Cassandra types to exportable format."""
        if value is None:
            return None
        elif isinstance(value, list | set):
            return [self._serialize_value(v) for v in value]
        elif isinstance(value, dict | OrderedMap | OrderedMapSerializedKey):
            # Handle Cassandra map types
            return {str(k): self._serialize_value(v) for k, v in value.items()}
        elif isinstance(value, bytes):
            # Convert bytes to base64 for JSON compatibility
            import base64

            return base64.b64encode(value).decode("ascii")
        elif isinstance(value, datetime):
            return value.isoformat()
        else:
            return value

    async def _open_output_file(self, output_path: Path, mode: str = "w") -> Any:
        """Open output file with optional compression."""
        if self.compression == "gzip":
            import gzip

            return gzip.open(output_path, mode + "t", encoding="utf-8")
        elif self.compression == "bz2":
            import bz2

            return bz2.open(output_path, mode + "t", encoding="utf-8")
        elif self.compression == "lz4":
            try:
                import lz4.frame

                return lz4.frame.open(output_path, mode + "t", encoding="utf-8")
            except ImportError:
                raise ImportError("lz4 compression requires 'pip install lz4'") from None
        else:
            return open(output_path, mode, encoding="utf-8", buffering=self.buffer_size)

    def _get_output_path_with_compression(self, output_path: Path) -> Path:
        """Add compression extension to output path if needed."""
        if self.compression:
            return output_path.with_suffix(output_path.suffix + f".{self.compression}")
        return output_path
