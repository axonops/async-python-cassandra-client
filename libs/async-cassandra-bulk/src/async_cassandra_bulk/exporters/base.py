"""
Base exporter abstract class.

Defines the interface and common functionality for all data exporters.
Subclasses implement format-specific logic for CSV, JSON, Parquet, etc.
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, AsyncIterator, Dict, List, Optional

import aiofiles


class BaseExporter(ABC):
    """
    Abstract base class for data exporters.

    Provides common functionality for exporting data from Cassandra to various
    file formats. Subclasses must implement format-specific methods.
    """

    def __init__(self, output_path: str, options: Optional[Dict[str, Any]] = None) -> None:
        """
        Initialize exporter with output configuration.

        Args:
            output_path: Path where to write the exported data
            options: Format-specific options

        Raises:
            ValueError: If output_path is empty or None
        """
        if not output_path:
            raise ValueError("output_path cannot be empty")

        self.output_path = output_path
        self.options = options or {}
        self._file: Any = None
        self._file_opened = False

    async def _ensure_file_open(self) -> None:
        """Ensure output file is open."""
        if not self._file_opened:
            # Ensure parent directory exists
            output_dir = Path(self.output_path).parent
            output_dir.mkdir(parents=True, exist_ok=True)

            # Open file
            self._file = await aiofiles.open(self.output_path, mode="w", encoding="utf-8")
            self._file_opened = True

    async def _close_file(self) -> None:
        """Close output file if open."""
        if self._file and self._file_opened:
            await self._file.close()
            self._file = None
            self._file_opened = False

    @abstractmethod
    async def write_header(self, columns: List[str]) -> None:
        """
        Write file header with column information.

        Args:
            columns: List of column names

        Note:
            Implementation depends on output format
        """
        pass

    @abstractmethod
    async def write_row(self, row: Dict[str, Any]) -> None:
        """
        Write a single row of data.

        Args:
            row: Dictionary mapping column names to values

        Note:
            Implementation handles format-specific encoding
        """
        pass

    @abstractmethod
    async def write_footer(self) -> None:
        """
        Write file footer and finalize output.

        Note:
            Some formats require closing tags or summary data
        """
        pass

    async def finalize(self) -> None:
        """
        Finalize export and close file.

        This should be called after all writing is complete.
        """
        await self._close_file()

    async def export_rows(self, rows: AsyncIterator[Dict[str, Any]], columns: List[str]) -> int:
        """
        Export rows to file using format-specific methods.

        This is the main entry point that orchestrates the export process:
        1. Creates parent directories if needed
        2. Opens output file
        3. Writes header
        4. Writes all rows
        5. Writes footer
        6. Closes file

        Args:
            rows: Async iterator of row dictionaries
            columns: List of column names

        Returns:
            Number of rows exported

        Raises:
            Exception: Any errors during export are propagated
        """
        # Ensure parent directory exists
        output_dir = Path(self.output_path).parent
        output_dir.mkdir(parents=True, exist_ok=True)

        row_count = 0

        async with aiofiles.open(self.output_path, mode="w", encoding="utf-8") as self._file:
            self._file_opened = True  # Mark as opened for write methods

            # Write header
            await self.write_header(columns)

            # Write rows
            async for row in rows:
                await self.write_row(row)
                row_count += 1

            # Write footer
            await self.write_footer()

        self._file_opened = False  # Reset after closing

        return row_count
