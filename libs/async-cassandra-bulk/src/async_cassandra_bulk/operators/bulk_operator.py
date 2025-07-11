"""
Core BulkOperator class for bulk operations on Cassandra tables.

This provides the main entry point for all bulk operations including:
- Count operations
- Export to various formats (CSV, JSON, Parquet)
- Import from various formats (future)
"""

from typing import Any, Callable, Dict, Literal, Optional

from async_cassandra import AsyncCassandraSession

from ..exporters import BaseExporter, CSVExporter, JSONExporter
from ..parallel_export import ParallelExporter
from ..utils.stats import BulkOperationStats


class BulkOperator:
    """
    Main operator for bulk operations on Cassandra tables.

    This class provides high-level methods for bulk operations while
    handling parallelism, progress tracking, and error recovery.
    """

    def __init__(self, session: AsyncCassandraSession) -> None:
        """
        Initialize BulkOperator with an async-cassandra session.

        Args:
            session: An AsyncCassandraSession instance from async-cassandra

        Raises:
            ValueError: If session doesn't have required methods
        """
        # Validate session has required methods
        if not hasattr(session, "execute") or not hasattr(session, "prepare"):
            raise ValueError(
                "Session must have 'execute' and 'prepare' methods. "
                "Please use an AsyncCassandraSession from async-cassandra."
            )

        self.session = session

    async def count(self, table: str, where: Optional[str] = None) -> int:
        """
        Count rows in a Cassandra table.

        Args:
            table: Full table name in format 'keyspace.table'
            where: Optional WHERE clause (without 'WHERE' keyword)

        Returns:
            Total row count

        Raises:
            ValueError: If table name format is invalid
            Exception: Any Cassandra query errors
        """
        # Validate table name format
        if "." not in table:
            raise ValueError(f"Table name must be in format 'keyspace.table', got: {table}")

        # Build count query
        query = f"SELECT COUNT(*) AS count FROM {table}"
        if where:
            query += f" WHERE {where}"

        # Execute query
        result = await self.session.execute(query)
        row = result.one()

        if row is None:
            return 0

        return int(row.count)

    async def export(
        self,
        table: str,
        output_path: str,
        format: Literal["csv", "json", "parquet"] = "csv",
        columns: Optional[list[str]] = None,
        where: Optional[str] = None,
        concurrency: int = 4,
        batch_size: int = 1000,
        progress_callback: Optional[Callable[[BulkOperationStats], None]] = None,
        checkpoint_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
        checkpoint_interval: int = 100,
        resume_from: Optional[Dict[str, Any]] = None,
        options: Optional[Dict[str, Any]] = None,
        csv_options: Optional[Dict[str, Any]] = None,
        json_options: Optional[Dict[str, Any]] = None,
        parquet_options: Optional[Dict[str, Any]] = None,
    ) -> BulkOperationStats:
        """
        Export data from a Cassandra table to a file.

        Args:
            table: Full table name in format 'keyspace.table'
            output_path: Path where to write the exported data
            format: Output format (csv, json, or parquet)
            columns: List of columns to export (default: all)
            where: Optional WHERE clause (not supported yet)
            concurrency: Number of parallel workers
            batch_size: Rows per batch
            progress_callback: Called with progress updates
            checkpoint_callback: Called to save checkpoints
            checkpoint_interval: How often to checkpoint
            resume_from: Previous checkpoint to resume from
            options: General export options:
                - include_writetime: Include writetime for columns (default: False)
                - writetime_columns: List of columns to get writetime for
                  (default: None, use ["*"] for all non-key columns)
            csv_options: CSV-specific options
            json_options: JSON-specific options
            parquet_options: Parquet-specific options

        Returns:
            Export statistics including row count, duration, etc.

        Raises:
            ValueError: If format is not supported
        """
        supported_formats = ["csv", "json", "parquet"]
        if format not in supported_formats:
            raise ValueError(
                f"Unsupported format '{format}'. "
                f"Supported formats: {', '.join(supported_formats)}"
            )

        # Parse table name - could be keyspace.table or just table
        parts = table.split(".")
        if len(parts) == 2:
            keyspace, table_name = parts
        else:
            # Get current keyspace from session
            keyspace = self.session._session.keyspace
            if not keyspace:
                raise ValueError(
                    "No keyspace specified. Use 'keyspace.table' format or set keyspace first"
                )
            # table_name is parsed from parts[0] but not used separately

        # Create appropriate exporter based on format
        exporter: BaseExporter
        if format == "csv":
            exporter = CSVExporter(
                output_path=output_path,
                options=csv_options or {},
            )
        elif format == "json":
            exporter = JSONExporter(
                output_path=output_path,
                options=json_options or {},
            )
        else:
            # This should not happen due to validation above
            raise ValueError(f"Format '{format}' not yet implemented")

        # Extract writetime options
        export_options = options or {}
        writetime_columns = export_options.get("writetime_columns")
        if export_options.get("include_writetime") and not writetime_columns:
            # Default to all columns if include_writetime is True
            writetime_columns = ["*"]

        # Create parallel exporter
        parallel_exporter = ParallelExporter(
            session=self.session,
            table=table,  # Use full table name (keyspace.table)
            exporter=exporter,
            concurrency=concurrency,
            batch_size=batch_size,
            progress_callback=progress_callback,
            checkpoint_callback=checkpoint_callback,
            checkpoint_interval=checkpoint_interval,
            resume_from=resume_from,
            columns=columns,
            writetime_columns=writetime_columns,
        )

        # Perform export
        stats = await parallel_exporter.export()

        return stats
