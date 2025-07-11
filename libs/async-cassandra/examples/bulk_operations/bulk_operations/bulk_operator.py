"""
Token-aware bulk operator for parallel Cassandra operations.
"""

import asyncio
import time
from collections.abc import AsyncIterator, Callable
from pathlib import Path
from typing import Any

from async_cassandra import AsyncCassandraSession
from cassandra import ConsistencyLevel

from .parallel_export import export_by_token_ranges_parallel
from .stats import BulkOperationStats
from .token_utils import TokenRange, TokenRangeSplitter, discover_token_ranges


class BulkOperationError(Exception):
    """Error during bulk operation."""

    def __init__(
        self, message: str, partial_result: Any = None, errors: list[Exception] | None = None
    ):
        super().__init__(message)
        self.partial_result = partial_result
        self.errors = errors or []


class TokenAwareBulkOperator:
    """Performs bulk operations using token ranges for parallelism.

    This class uses prepared statements for all token range queries to:
    - Improve performance through query plan caching
    - Provide protection against injection attacks
    - Ensure type safety and validation
    - Follow Cassandra best practices

    Token range boundaries are passed as parameters to prepared statements,
    not embedded in the query string.
    """

    def __init__(self, session: AsyncCassandraSession):
        self.session = session
        self.splitter = TokenRangeSplitter()
        self._prepared_statements: dict[str, dict[str, Any]] = {}

    async def _get_prepared_statements(
        self, keyspace: str, table: str, partition_keys: list[str]
    ) -> dict[str, Any]:
        """Get or prepare statements for token range queries."""
        pk_list = ", ".join(partition_keys)
        key = f"{keyspace}.{table}"

        if key not in self._prepared_statements:
            # Prepare all the statements we need for this table
            self._prepared_statements[key] = {
                "count_range": await self.session.prepare(
                    f"""
                    SELECT COUNT(*) FROM {keyspace}.{table}
                    WHERE token({pk_list}) > ?
                    AND token({pk_list}) <= ?
                """
                ),
                "count_wraparound_gt": await self.session.prepare(
                    f"""
                    SELECT COUNT(*) FROM {keyspace}.{table}
                    WHERE token({pk_list}) > ?
                """
                ),
                "count_wraparound_lte": await self.session.prepare(
                    f"""
                    SELECT COUNT(*) FROM {keyspace}.{table}
                    WHERE token({pk_list}) <= ?
                """
                ),
                "select_range": await self.session.prepare(
                    f"""
                    SELECT * FROM {keyspace}.{table}
                    WHERE token({pk_list}) > ?
                    AND token({pk_list}) <= ?
                """
                ),
                "select_wraparound_gt": await self.session.prepare(
                    f"""
                    SELECT * FROM {keyspace}.{table}
                    WHERE token({pk_list}) > ?
                """
                ),
                "select_wraparound_lte": await self.session.prepare(
                    f"""
                    SELECT * FROM {keyspace}.{table}
                    WHERE token({pk_list}) <= ?
                """
                ),
            }

        return self._prepared_statements[key]

    async def count_by_token_ranges(
        self,
        keyspace: str,
        table: str,
        split_count: int | None = None,
        parallelism: int | None = None,
        progress_callback: Callable[[BulkOperationStats], None] | None = None,
        consistency_level: ConsistencyLevel | None = None,
    ) -> int:
        """Count all rows in a table using parallel token range queries.

        Args:
            keyspace: The keyspace name.
            table: The table name.
            split_count: Number of token range splits (default: 4 * number of nodes).
            parallelism: Max concurrent operations (default: 2 * number of nodes).
            progress_callback: Optional callback for progress updates.
            consistency_level: Consistency level for queries (default: None, uses driver default).

        Returns:
            Total row count.
        """
        count, _ = await self.count_by_token_ranges_with_stats(
            keyspace=keyspace,
            table=table,
            split_count=split_count,
            parallelism=parallelism,
            progress_callback=progress_callback,
            consistency_level=consistency_level,
        )
        return count

    async def count_by_token_ranges_with_stats(
        self,
        keyspace: str,
        table: str,
        split_count: int | None = None,
        parallelism: int | None = None,
        progress_callback: Callable[[BulkOperationStats], None] | None = None,
        consistency_level: ConsistencyLevel | None = None,
    ) -> tuple[int, BulkOperationStats]:
        """Count all rows and return statistics."""
        # Get table metadata
        table_meta = await self._get_table_metadata(keyspace, table)
        partition_keys = [col.name for col in table_meta.partition_key]

        # Discover and split token ranges
        ranges = await discover_token_ranges(self.session, keyspace)

        if split_count is None:
            # Default: 4 splits per node
            split_count = len(self.session._session.cluster.contact_points) * 4

        splits = self.splitter.split_proportionally(ranges, split_count)

        # Initialize stats
        stats = BulkOperationStats(total_ranges=len(splits))

        # Determine parallelism
        if parallelism is None:
            parallelism = min(len(splits), len(self.session._session.cluster.contact_points) * 2)

        # Get prepared statements for this table
        prepared_stmts = await self._get_prepared_statements(keyspace, table, partition_keys)

        # Create count tasks
        semaphore = asyncio.Semaphore(parallelism)
        tasks = []

        for split in splits:
            task = self._count_range(
                keyspace,
                table,
                partition_keys,
                split,
                semaphore,
                stats,
                progress_callback,
                prepared_stmts,
                consistency_level,
            )
            tasks.append(task)

        # Execute all tasks
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results
        total_count = 0
        for result in results:
            if isinstance(result, Exception):
                stats.errors.append(result)
            else:
                total_count += int(result)

        stats.end_time = time.time()

        if stats.errors:
            raise BulkOperationError(
                f"Failed to count all ranges: {len(stats.errors)} errors",
                partial_result=total_count,
                errors=stats.errors,
            )

        return total_count, stats

    async def _count_range(
        self,
        keyspace: str,
        table: str,
        partition_keys: list[str],
        token_range: TokenRange,
        semaphore: asyncio.Semaphore,
        stats: BulkOperationStats,
        progress_callback: Callable[[BulkOperationStats], None] | None,
        prepared_stmts: dict[str, Any],
        consistency_level: ConsistencyLevel | None,
    ) -> int:
        """Count rows in a single token range."""
        async with semaphore:
            # Check if this is a wraparound range
            if token_range.end < token_range.start:
                # Wraparound range needs to be split into two queries
                # First part: from start to MAX_TOKEN
                stmt = prepared_stmts["count_wraparound_gt"]
                if consistency_level is not None:
                    stmt.consistency_level = consistency_level
                result1 = await self.session.execute(stmt, (token_range.start,))
                row1 = result1.one()
                count1 = row1.count if row1 else 0

                # Second part: from MIN_TOKEN to end
                stmt = prepared_stmts["count_wraparound_lte"]
                if consistency_level is not None:
                    stmt.consistency_level = consistency_level
                result2 = await self.session.execute(stmt, (token_range.end,))
                row2 = result2.one()
                count2 = row2.count if row2 else 0

                count = count1 + count2
            else:
                # Normal range - use prepared statement
                stmt = prepared_stmts["count_range"]
                if consistency_level is not None:
                    stmt.consistency_level = consistency_level
                result = await self.session.execute(stmt, (token_range.start, token_range.end))
                row = result.one()
                count = row.count if row else 0

            # Update stats
            stats.rows_processed += count
            stats.ranges_completed += 1

            # Call progress callback if provided
            if progress_callback:
                progress_callback(stats)

            return int(count)

    async def export_by_token_ranges(
        self,
        keyspace: str,
        table: str,
        split_count: int | None = None,
        parallelism: int | None = None,
        progress_callback: Callable[[BulkOperationStats], None] | None = None,
        consistency_level: ConsistencyLevel | None = None,
    ) -> AsyncIterator[Any]:
        """Export all rows from a table by streaming token ranges in parallel.

        This method uses parallel queries to stream data from multiple token ranges
        concurrently, providing high performance for large table exports.

        Args:
            keyspace: The keyspace name.
            table: The table name.
            split_count: Number of token range splits (default: 4 * number of nodes).
            parallelism: Max concurrent queries (default: 2 * number of nodes).
            progress_callback: Optional callback for progress updates.
            consistency_level: Consistency level for queries (default: None, uses driver default).

        Yields:
            Row data from the table, streamed as results arrive from parallel queries.
        """
        # Get table metadata
        table_meta = await self._get_table_metadata(keyspace, table)
        partition_keys = [col.name for col in table_meta.partition_key]

        # Discover and split token ranges
        ranges = await discover_token_ranges(self.session, keyspace)

        if split_count is None:
            split_count = len(self.session._session.cluster.contact_points) * 4

        splits = self.splitter.split_proportionally(ranges, split_count)

        # Determine parallelism
        if parallelism is None:
            parallelism = min(len(splits), len(self.session._session.cluster.contact_points) * 2)

        # Initialize stats
        stats = BulkOperationStats(total_ranges=len(splits))

        # Get prepared statements for this table
        prepared_stmts = await self._get_prepared_statements(keyspace, table, partition_keys)

        # Use parallel export
        async for row in export_by_token_ranges_parallel(
            operator=self,
            keyspace=keyspace,
            table=table,
            splits=splits,
            prepared_stmts=prepared_stmts,
            parallelism=parallelism,
            consistency_level=consistency_level,
            stats=stats,
            progress_callback=progress_callback,
        ):
            yield row

        stats.end_time = time.time()

    async def import_from_iceberg(
        self,
        iceberg_warehouse_path: str,
        iceberg_table: str,
        target_keyspace: str,
        target_table: str,
        parallelism: int | None = None,
        batch_size: int = 1000,
        progress_callback: Callable[[BulkOperationStats], None] | None = None,
    ) -> BulkOperationStats:
        """Import data from Iceberg to Cassandra."""
        # This will be implemented when we add Iceberg integration
        raise NotImplementedError("Iceberg import will be implemented in next phase")

    async def _get_table_metadata(self, keyspace: str, table: str) -> Any:
        """Get table metadata from cluster."""
        metadata = self.session._session.cluster.metadata

        if keyspace not in metadata.keyspaces:
            raise ValueError(f"Keyspace '{keyspace}' not found")

        keyspace_meta = metadata.keyspaces[keyspace]

        if table not in keyspace_meta.tables:
            raise ValueError(f"Table '{table}' not found in keyspace '{keyspace}'")

        return keyspace_meta.tables[table]

    async def export_to_csv(
        self,
        keyspace: str,
        table: str,
        output_path: str | Path,
        columns: list[str] | None = None,
        delimiter: str = ",",
        null_string: str = "",
        compression: str | None = None,
        split_count: int | None = None,
        parallelism: int | None = None,
        progress_callback: Callable[[Any], Any] | None = None,
        consistency_level: ConsistencyLevel | None = None,
    ) -> Any:
        """Export table to CSV format.

        Args:
            keyspace: Keyspace name
            table: Table name
            output_path: Output file path
            columns: Columns to export (None for all)
            delimiter: CSV delimiter
            null_string: String to represent NULL values
            compression: Compression type (gzip, bz2, lz4)
            split_count: Number of token range splits
            parallelism: Max concurrent operations
            progress_callback: Progress callback function
            consistency_level: Consistency level for queries

        Returns:
            ExportProgress object
        """
        from .exporters import CSVExporter

        exporter = CSVExporter(
            self,
            delimiter=delimiter,
            null_string=null_string,
            compression=compression,
        )

        return await exporter.export(
            keyspace=keyspace,
            table=table,
            output_path=Path(output_path),
            columns=columns,
            split_count=split_count,
            parallelism=parallelism,
            progress_callback=progress_callback,
            consistency_level=consistency_level,
        )

    async def export_to_json(
        self,
        keyspace: str,
        table: str,
        output_path: str | Path,
        columns: list[str] | None = None,
        format_mode: str = "jsonl",
        indent: int | None = None,
        compression: str | None = None,
        split_count: int | None = None,
        parallelism: int | None = None,
        progress_callback: Callable[[Any], Any] | None = None,
        consistency_level: ConsistencyLevel | None = None,
    ) -> Any:
        """Export table to JSON format.

        Args:
            keyspace: Keyspace name
            table: Table name
            output_path: Output file path
            columns: Columns to export (None for all)
            format_mode: 'jsonl' (line-delimited) or 'array'
            indent: JSON indentation
            compression: Compression type (gzip, bz2, lz4)
            split_count: Number of token range splits
            parallelism: Max concurrent operations
            progress_callback: Progress callback function
            consistency_level: Consistency level for queries

        Returns:
            ExportProgress object
        """
        from .exporters import JSONExporter

        exporter = JSONExporter(
            self,
            format_mode=format_mode,
            indent=indent,
            compression=compression,
        )

        return await exporter.export(
            keyspace=keyspace,
            table=table,
            output_path=Path(output_path),
            columns=columns,
            split_count=split_count,
            parallelism=parallelism,
            progress_callback=progress_callback,
            consistency_level=consistency_level,
        )

    async def export_to_parquet(
        self,
        keyspace: str,
        table: str,
        output_path: str | Path,
        columns: list[str] | None = None,
        compression: str = "snappy",
        row_group_size: int = 50000,
        split_count: int | None = None,
        parallelism: int | None = None,
        progress_callback: Callable[[Any], Any] | None = None,
        consistency_level: ConsistencyLevel | None = None,
    ) -> Any:
        """Export table to Parquet format.

        Args:
            keyspace: Keyspace name
            table: Table name
            output_path: Output file path
            columns: Columns to export (None for all)
            compression: Parquet compression (snappy, gzip, brotli, lz4, zstd)
            row_group_size: Rows per row group
            split_count: Number of token range splits
            parallelism: Max concurrent operations
            progress_callback: Progress callback function

        Returns:
            ExportProgress object
        """
        from .exporters import ParquetExporter

        exporter = ParquetExporter(
            self,
            compression=compression,
            row_group_size=row_group_size,
        )

        return await exporter.export(
            keyspace=keyspace,
            table=table,
            output_path=Path(output_path),
            columns=columns,
            split_count=split_count,
            parallelism=parallelism,
            progress_callback=progress_callback,
            consistency_level=consistency_level,
        )

    async def export_to_iceberg(
        self,
        keyspace: str,
        table: str,
        namespace: str | None = None,
        table_name: str | None = None,
        catalog: Any | None = None,
        catalog_config: dict[str, Any] | None = None,
        warehouse_path: str | Path | None = None,
        partition_spec: Any | None = None,
        table_properties: dict[str, str] | None = None,
        compression: str = "snappy",
        row_group_size: int = 100000,
        columns: list[str] | None = None,
        split_count: int | None = None,
        parallelism: int | None = None,
        progress_callback: Any | None = None,
    ) -> Any:
        """Export table data to Apache Iceberg format.

        This enables modern data lakehouse features like ACID transactions,
        time travel, and schema evolution.

        Args:
            keyspace: Cassandra keyspace to export from
            table: Cassandra table to export
            namespace: Iceberg namespace (default: keyspace name)
            table_name: Iceberg table name (default: Cassandra table name)
            catalog: Pre-configured Iceberg catalog (optional)
            catalog_config: Custom catalog configuration (optional)
            warehouse_path: Path to Iceberg warehouse (for filesystem catalog)
            partition_spec: Iceberg partition specification
            table_properties: Additional Iceberg table properties
            compression: Parquet compression (default: snappy)
            row_group_size: Rows per Parquet file (default: 100000)
            columns: Columns to export (default: all)
            split_count: Number of token range splits
            parallelism: Max concurrent operations
            progress_callback: Progress callback function

        Returns:
            ExportProgress with Iceberg metadata
        """
        from .iceberg import IcebergExporter

        exporter = IcebergExporter(
            self,
            catalog=catalog,
            catalog_config=catalog_config,
            warehouse_path=warehouse_path,
            compression=compression,
            row_group_size=row_group_size,
        )
        return await exporter.export(
            keyspace=keyspace,
            table=table,
            namespace=namespace,
            table_name=table_name,
            partition_spec=partition_spec,
            table_properties=table_properties,
            columns=columns,
            split_count=split_count,
            parallelism=parallelism,
            progress_callback=progress_callback,
        )
