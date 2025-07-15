"""
Core BulkOperator class for bulk operations on Cassandra tables.

This provides the main entry point for all bulk operations including:
- Count operations
- Export to various formats (CSV, JSON, Parquet)
- Import from various formats (future)
"""

from datetime import datetime, timezone
from typing import Any, Callable, Dict, Literal, Optional, Union

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

    def _parse_writetime_filters(self, options: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse writetime filter options into microseconds.

        Args:
            options: Dict containing writetime_after and/or writetime_before

        Returns:
            Dict with parsed writetime_after_micros and/or writetime_before_micros

        Raises:
            ValueError: If timestamps are invalid or before < after
        """
        parsed = {}

        # Parse writetime_after
        if "writetime_after" in options:
            after_value = options["writetime_after"]
            parsed["writetime_after_micros"] = self._parse_timestamp_to_micros(after_value)

        # Parse writetime_before
        if "writetime_before" in options:
            before_value = options["writetime_before"]
            parsed["writetime_before_micros"] = self._parse_timestamp_to_micros(before_value)

        # Validate logical consistency
        if "writetime_after_micros" in parsed and "writetime_before_micros" in parsed:
            if parsed["writetime_before_micros"] <= parsed["writetime_after_micros"]:
                raise ValueError("writetime_before must be later than writetime_after")

        return parsed

    def _parse_timestamp_to_micros(self, timestamp: Union[str, int, float, datetime]) -> int:
        """
        Convert various timestamp formats to microseconds since epoch.

        Args:
            timestamp: ISO string, unix timestamp (seconds/millis), or datetime

        Returns:
            Microseconds since epoch

        Raises:
            ValueError: If timestamp format is invalid
        """
        if isinstance(timestamp, datetime):
            # Datetime object
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
            return int(timestamp.timestamp() * 1_000_000)

        elif isinstance(timestamp, str):
            # ISO format string
            try:
                dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return int(dt.timestamp() * 1_000_000)
            except ValueError as e:
                raise ValueError(f"Invalid timestamp format: {timestamp}") from e

        elif isinstance(timestamp, (int, float)):
            # Unix timestamp
            if timestamp < 0:
                raise ValueError("Timestamp cannot be negative")

            # Detect if it's seconds, milliseconds, or microseconds
            # If timestamp is less than year 3000 in seconds, assume seconds
            if timestamp < 32503680000:  # Jan 1, 3000 in seconds
                return int(timestamp * 1_000_000)
            # If timestamp is less than year 3000 in milliseconds, assume milliseconds
            elif timestamp < 32503680000000:  # Jan 1, 3000 in milliseconds
                return int(timestamp * 1_000)
            else:
                # Assume microseconds (already in the correct unit)
                return int(timestamp)

        else:
            raise TypeError(f"Unsupported timestamp type: {type(timestamp)}")

    def _validate_writetime_options(self, options: Dict[str, Any]) -> None:
        """
        Validate writetime-related options.

        Args:
            options: Export options to validate

        Raises:
            ValueError: If options are invalid
        """
        # If using writetime filters, must have writetime columns
        has_filters = "writetime_after" in options or "writetime_before" in options
        has_columns = bool(options.get("writetime_columns"))

        if has_filters and not has_columns:
            raise ValueError("writetime_columns must be specified when using writetime filters")

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
                - writetime_after: Export rows where ANY column was written after this time
                - writetime_before: Export rows where ANY column was written before this time
                - writetime_filter_mode: "any" (default) or "all" - whether ANY or ALL
                  writetime columns must match the filter criteria
                - include_ttl: Include TTL (time to live) for columns (default: False)
                - ttl_columns: List of columns to get TTL for
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
            # Update the options dict so validation sees it
            export_options["writetime_columns"] = writetime_columns

        # Extract TTL options
        ttl_columns = export_options.get("ttl_columns")
        if export_options.get("include_ttl") and not ttl_columns:
            # Default to all columns if include_ttl is True
            ttl_columns = ["*"]

        # Validate writetime options
        self._validate_writetime_options(export_options)

        # Parse writetime filters
        parsed_filters = self._parse_writetime_filters(export_options)
        writetime_after_micros = parsed_filters.get("writetime_after_micros")
        writetime_before_micros = parsed_filters.get("writetime_before_micros")
        writetime_filter_mode = export_options.get("writetime_filter_mode", "any")

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
            ttl_columns=ttl_columns,
            writetime_after_micros=writetime_after_micros,
            writetime_before_micros=writetime_before_micros,
            writetime_filter_mode=writetime_filter_mode,
        )

        # Perform export
        stats = await parallel_exporter.export()

        return stats
