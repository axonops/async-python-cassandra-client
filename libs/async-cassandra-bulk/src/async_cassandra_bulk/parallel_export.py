"""
Parallel export functionality for bulk operations.

Manages concurrent export of token ranges with progress tracking,
error handling, and checkpointing support.
"""

import asyncio
import logging
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from async_cassandra_bulk.exporters.base import BaseExporter
from async_cassandra_bulk.utils.stats import BulkOperationStats
from async_cassandra_bulk.utils.token_utils import (
    MAX_TOKEN,
    MIN_TOKEN,
    TokenRange,
    TokenRangeSplitter,
    discover_token_ranges,
    generate_token_range_query,
)

logger = logging.getLogger(__name__)


class ParallelExporter:
    """
    Manages parallel export of Cassandra data.

    Coordinates multiple workers to export token ranges concurrently
    with progress tracking and error handling.
    """

    def __init__(
        self,
        session: Any,
        table: str,
        exporter: BaseExporter,
        concurrency: int = 4,
        batch_size: int = 1000,
        checkpoint_interval: int = 10,
        checkpoint_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
        progress_callback: Optional[Callable[[BulkOperationStats], None]] = None,
        resume_from: Optional[Dict[str, Any]] = None,
        columns: Optional[List[str]] = None,
        writetime_columns: Optional[List[str]] = None,
    ) -> None:
        """
        Initialize parallel exporter.

        Args:
            session: AsyncCassandraSession instance
            table: Full table name (keyspace.table)
            exporter: Exporter instance for output format
            concurrency: Number of concurrent workers
            batch_size: Rows per query page
            checkpoint_interval: Save checkpoint after N ranges
            checkpoint_callback: Function to save checkpoint state
            progress_callback: Function to report progress
            resume_from: Previous checkpoint to resume from
            columns: Optional list of columns to export (default: all)
            writetime_columns: Optional list of columns to get writetime for
        """
        self.session = session
        self.table = table
        self.exporter = exporter
        self.concurrency = concurrency
        self.batch_size = batch_size
        self.checkpoint_interval = checkpoint_interval
        self.checkpoint_callback = checkpoint_callback
        self.progress_callback = progress_callback
        self.resume_from = resume_from
        self.columns = columns
        self.writetime_columns = writetime_columns

        # Parse table name
        if "." not in table:
            raise ValueError(f"Table must be in format 'keyspace.table', got: {table}")
        self.keyspace, self.table_name = table.split(".", 1)

        # Internal state
        self._stats = BulkOperationStats()
        self._completed_ranges: Set[Tuple[int, int]] = set()
        self._range_splitter = TokenRangeSplitter()
        self._semaphore = asyncio.Semaphore(concurrency)
        self._resolved_columns: Optional[List[str]] = None
        self._header_written = False

        # Load from checkpoint if provided
        if resume_from:
            self._load_checkpoint(resume_from)

    def _load_checkpoint(self, checkpoint: Dict[str, Any]) -> None:
        """Load state from checkpoint."""
        # Check version compatibility
        version = checkpoint.get("version", "0.0")
        if version != "1.0":
            logger.warning(
                f"Checkpoint version {version} may not be compatible with current version 1.0"
            )

        self._completed_ranges = set(tuple(r) for r in checkpoint.get("completed_ranges", []))
        self._stats.rows_processed = checkpoint.get("total_rows", 0)
        self._stats.start_time = checkpoint.get("start_time", self._stats.start_time)

        # Validate configuration if available
        if "export_config" in checkpoint:
            config = checkpoint["export_config"]

            # Warn if configuration has changed
            if config.get("table") != self.table:
                logger.warning(f"Table changed from {config['table']} to {self.table}")

            if config.get("columns") != self.columns:
                logger.warning(f"Column list changed from {config['columns']} to {self.columns}")

            if config.get("writetime_columns") != self.writetime_columns:
                logger.warning(
                    f"Writetime columns changed from {config['writetime_columns']} to {self.writetime_columns}"
                )

        logger.info(
            f"Resuming from checkpoint: {len(self._completed_ranges)} ranges completed, "
            f"{self._stats.rows_processed} rows processed"
        )

    async def _discover_and_split_ranges(self) -> List[TokenRange]:
        """Discover token ranges and split for parallelism."""
        # Discover ranges from cluster
        ranges = await discover_token_ranges(self.session, self.keyspace)
        logger.info(f"Discovered {len(ranges)} token ranges")

        # Split ranges based on concurrency
        target_splits = max(self.concurrency * 2, len(ranges))
        split_ranges = self._range_splitter.split_proportionally(ranges, target_splits)
        logger.info(f"Split into {len(split_ranges)} ranges for processing")

        # Filter out completed ranges if resuming
        if self._completed_ranges:
            original_count = len(split_ranges)
            split_ranges = [
                r for r in split_ranges if (r.start, r.end) not in self._completed_ranges
            ]
            logger.info(
                f"Resuming with {len(split_ranges)} remaining ranges (filtered {original_count - len(split_ranges)} completed)"
            )

        return split_ranges

    async def _get_columns(self) -> List[str]:
        """Get column names for the table."""
        # If specific columns were requested, return those
        if self.columns:
            return self.columns

        # Otherwise get all columns from table metadata
        # Access cluster metadata through sync session
        cluster = self.session._session.cluster
        metadata = cluster.metadata

        keyspace_meta = metadata.keyspaces.get(self.keyspace)
        if not keyspace_meta:
            raise ValueError(f"Keyspace '{self.keyspace}' not found")

        table_meta = keyspace_meta.tables.get(self.table_name)
        if not table_meta:
            raise ValueError(f"Table '{self.table}' not found")

        return list(table_meta.columns.keys())

    async def _export_range(self, token_range: TokenRange, stats: BulkOperationStats) -> int:
        """
        Export a single token range.

        Args:
            token_range: Token range to export
            stats: Statistics tracker

        Returns:
            Number of rows exported
        """
        row_count = 0

        try:
            # Get partition keys for token function
            cluster = self.session._session.cluster
            metadata = cluster.metadata
            table_meta = metadata.keyspaces[self.keyspace].tables[self.table_name]
            partition_keys = [col.name for col in table_meta.partition_key]
            clustering_keys = [col.name for col in table_meta.clustering_key]

            # Get counter columns
            counter_columns = []
            for col_name, col_meta in table_meta.columns.items():
                if col_meta.cql_type == "counter":
                    counter_columns.append(col_name)

            # Check if this is a wraparound range
            if token_range.end < token_range.start:
                # Split wraparound range into two queries
                # First part: from start to MAX_TOKEN
                query1 = generate_token_range_query(
                    self.keyspace,
                    self.table_name,
                    partition_keys,
                    TokenRange(
                        start=token_range.start, end=MAX_TOKEN, replicas=token_range.replicas
                    ),
                    self._resolved_columns or self.columns,
                    self.writetime_columns,
                    clustering_keys,
                    counter_columns,
                )
                result1 = await self.session.execute(query1)

                # Process first part
                async for row in result1:
                    row_dict = {}
                    for field in row._fields:
                        row_dict[field] = getattr(row, field)
                    await self.exporter.write_row(row_dict)
                    row_count += 1
                    stats.rows_processed += 1

                # Second part: from MIN_TOKEN to end
                query2 = generate_token_range_query(
                    self.keyspace,
                    self.table_name,
                    partition_keys,
                    TokenRange(start=MIN_TOKEN, end=token_range.end, replicas=token_range.replicas),
                    self._resolved_columns or self.columns,
                    self.writetime_columns,
                    clustering_keys,
                    counter_columns,
                )
                result2 = await self.session.execute(query2)

                # Process second part
                async for row in result2:
                    row_dict = {}
                    for field in row._fields:
                        row_dict[field] = getattr(row, field)
                    await self.exporter.write_row(row_dict)
                    row_count += 1
                    stats.rows_processed += 1
            else:
                # Non-wraparound range - process normally
                query = generate_token_range_query(
                    self.keyspace,
                    self.table_name,
                    partition_keys,
                    token_range,
                    self._resolved_columns or self.columns,
                    self.writetime_columns,
                    clustering_keys,
                    counter_columns,
                )
                result = await self.session.execute(query)

                # Process all rows
                async for row in result:
                    row_dict = {}
                    for field in row._fields:
                        row_dict[field] = getattr(row, field)
                    await self.exporter.write_row(row_dict)
                    row_count += 1
                    stats.rows_processed += 1

            # Update stats
            stats.ranges_completed += 1
            logger.debug(f"Completed range {token_range.start}-{token_range.end}: {row_count} rows")

        except Exception as e:
            logger.error(f"Error exporting range {token_range.start}-{token_range.end}: {e}")
            stats.errors.append(e)
            # Return -1 to indicate failure
            return -1

        return row_count

    async def _worker(
        self, queue: asyncio.Queue, stats: BulkOperationStats, checkpoint_counter: List[int]
    ) -> None:
        """
        Worker coroutine to process ranges from queue.

        Args:
            queue: Queue of token ranges to process
            stats: Shared statistics object
            checkpoint_counter: Shared counter for checkpointing
        """
        while True:
            try:
                token_range = await queue.get()
                if token_range is None:  # Sentinel
                    break

                async with self._semaphore:
                    # Export the range - if it fails, don't mark as completed
                    row_count = await self._export_range(token_range, stats)

                    # Only mark as completed if export succeeded (no exception)
                    if row_count >= 0:  # _export_range returns row count on success
                        self._completed_ranges.add((token_range.start, token_range.end))

                        # Progress callback
                        if self.progress_callback:
                            self.progress_callback(stats)

                        # Checkpoint if needed
                        checkpoint_counter[0] += 1
                        if (
                            self.checkpoint_callback
                            and checkpoint_counter[0] % self.checkpoint_interval == 0
                        ):
                            await self._save_checkpoint(stats)

            except Exception as e:
                logger.error(f"Worker error: {e}")
                stats.errors.append(e)
            finally:
                queue.task_done()

    async def _save_checkpoint(self, stats: BulkOperationStats) -> None:
        """Save checkpoint state."""
        checkpoint = {
            "version": "1.0",
            "completed_ranges": list(self._completed_ranges),
            "total_rows": stats.rows_processed,
            "start_time": stats.start_time,
            "timestamp": datetime.now().isoformat(),
            "export_config": {
                "table": self.table,
                "columns": self.columns,
                "writetime_columns": self.writetime_columns,
                "batch_size": self.batch_size,
                "concurrency": self.concurrency,
            },
        }

        if asyncio.iscoroutinefunction(self.checkpoint_callback):
            await self.checkpoint_callback(checkpoint)
        elif self.checkpoint_callback:
            self.checkpoint_callback(checkpoint)

        logger.info(
            f"Saved checkpoint: {stats.ranges_completed} ranges, {stats.rows_processed} rows"
        )

    async def _process_ranges(self, ranges: List[TokenRange]) -> BulkOperationStats:
        """
        Process all ranges using worker pool.

        Args:
            ranges: List of token ranges to process

        Returns:
            Final statistics
        """
        # Setup stats
        self._stats.total_ranges = len(ranges) + len(self._completed_ranges)
        self._stats.ranges_completed = len(self._completed_ranges)

        # Create work queue
        queue: asyncio.Queue = asyncio.Queue()
        for token_range in ranges:
            await queue.put(token_range)

        # Create workers
        checkpoint_counter = [0]  # Shared counter in list
        workers = []
        for _ in range(min(self.concurrency, len(ranges))):
            worker = asyncio.create_task(self._worker(queue, self._stats, checkpoint_counter))
            workers.append(worker)

        # Add sentinels for workers to stop
        for _ in workers:
            await queue.put(None)

        # Wait for all work to complete
        await queue.join()
        await asyncio.gather(*workers)

        return self._stats

    async def export(self) -> BulkOperationStats:
        """
        Execute parallel export.

        Returns:
            Export statistics

        Raises:
            Exception: Any unhandled errors during export
        """
        logger.info(f"Starting parallel export of {self.table}")

        try:
            # Get columns
            columns = await self._get_columns()
            self._resolved_columns = columns

            # Write header including writetime columns
            header_columns = columns.copy()
            if self.writetime_columns:
                # Get key columns and counter columns to exclude
                cluster = self.session._session.cluster
                metadata = cluster.metadata
                table_meta = metadata.keyspaces[self.keyspace].tables[self.table_name]
                partition_keys = {col.name for col in table_meta.partition_key}
                clustering_keys = {col.name for col in table_meta.clustering_key}
                key_columns = partition_keys | clustering_keys

                # Get counter columns (they don't support writetime)
                counter_columns = set()
                for col_name, col_meta in table_meta.columns.items():
                    if col_meta.cql_type == "counter":
                        counter_columns.add(col_name)

                # Add writetime columns to header
                if self.writetime_columns == ["*"]:
                    # Add writetime for all non-key, non-counter columns
                    for col in columns:
                        if col not in key_columns and col not in counter_columns:
                            header_columns.append(f"{col}_writetime")
                else:
                    # Add writetime for specific columns (excluding keys and counters)
                    for col in self.writetime_columns:
                        if col not in key_columns and col not in counter_columns:
                            header_columns.append(f"{col}_writetime")

            # Write header only if not resuming
            if not self._header_written:
                await self.exporter.write_header(header_columns)
                self._header_written = True

            # Discover and split ranges
            ranges = await self._discover_and_split_ranges()

            # Check if there's any work to do
            if not ranges:
                logger.info("All ranges already completed - export is up to date")
                # Return stats from checkpoint
                self._stats.end_time = datetime.now().timestamp()
                return self._stats

            # Process all ranges
            stats = await self._process_ranges(ranges)

            # Write footer
            await self.exporter.write_footer()

            # Finalize exporter (closes file)
            await self.exporter.finalize()

            # Final checkpoint if needed
            if self.checkpoint_callback and stats.ranges_completed > 0:
                await self._save_checkpoint(stats)

            # Mark completion
            stats.end_time = datetime.now().timestamp()

            # Check if there were critical errors
            if stats.errors:
                # If we have errors and NO data was exported, it's a complete failure
                if stats.rows_processed == 0:
                    logger.error(f"Export completely failed with {len(stats.errors)} errors")
                    # Re-raise the first error
                    raise stats.errors[0]
                # Log errors but don't fail if we got some data
                elif not stats.is_complete:
                    logger.warning(
                        f"Export completed with {len(stats.errors)} errors. "
                        f"Exported {stats.rows_processed} rows from {stats.ranges_completed}/{stats.total_ranges} ranges"
                    )

            logger.info(
                f"Export completed: {stats.rows_processed} rows in "
                f"{stats.duration_seconds:.1f} seconds "
                f"({stats.rows_per_second:.1f} rows/sec)"
            )

            return stats

        except Exception as e:
            logger.error(f"Export failed: {e}")
            self._stats.errors.append(e)
            self._stats.end_time = datetime.now().timestamp()
            raise
