"""
Token-aware bulk operator for parallel Cassandra operations.
"""

import asyncio
import time
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field
from typing import Any

from async_cassandra import AsyncCassandraSession

from .token_utils import (
    TokenRange,
    TokenRangeSplitter,
    discover_token_ranges,
    generate_token_range_query,
)


@dataclass
class BulkOperationStats:
    """Statistics for bulk operations."""

    rows_processed: int = 0
    ranges_completed: int = 0
    total_ranges: int = 0
    start_time: float = field(default_factory=time.time)
    end_time: float | None = None
    errors: list[Exception] = field(default_factory=list)

    @property
    def duration_seconds(self) -> float:
        """Calculate operation duration."""
        if self.end_time:
            return self.end_time - self.start_time
        return time.time() - self.start_time

    @property
    def rows_per_second(self) -> float:
        """Calculate processing rate."""
        duration = self.duration_seconds
        if duration > 0:
            return self.rows_processed / duration
        return 0

    @property
    def progress_percentage(self) -> float:
        """Calculate progress as percentage."""
        if self.total_ranges > 0:
            return (self.ranges_completed / self.total_ranges) * 100
        return 0

    @property
    def success(self) -> bool:
        """Check if operation completed successfully."""
        return len(self.errors) == 0 and self.ranges_completed == self.total_ranges


class BulkOperationError(Exception):
    """Error during bulk operation."""

    def __init__(
        self, message: str, partial_result: Any = None, errors: list[Exception] | None = None
    ):
        super().__init__(message)
        self.partial_result = partial_result
        self.errors = errors or []


class TokenAwareBulkOperator:
    """Performs bulk operations using token ranges for parallelism."""

    def __init__(self, session: AsyncCassandraSession):
        self.session = session
        self.splitter = TokenRangeSplitter()

    async def count_by_token_ranges(
        self,
        keyspace: str,
        table: str,
        split_count: int | None = None,
        parallelism: int | None = None,
        progress_callback: Callable[[BulkOperationStats], None] | None = None,
    ) -> int:
        """Count all rows in a table using parallel token range queries."""
        count, _ = await self.count_by_token_ranges_with_stats(
            keyspace=keyspace,
            table=table,
            split_count=split_count,
            parallelism=parallelism,
            progress_callback=progress_callback,
        )
        return count

    async def count_by_token_ranges_with_stats(
        self,
        keyspace: str,
        table: str,
        split_count: int | None = None,
        parallelism: int | None = None,
        progress_callback: Callable[[BulkOperationStats], None] | None = None,
    ) -> tuple[int, BulkOperationStats]:
        """Count all rows and return statistics."""
        # Get table metadata
        table_meta = await self._get_table_metadata(keyspace, table)
        partition_keys = [col.name for col in table_meta.partition_key]

        # Discover and split token ranges
        ranges = await discover_token_ranges(self.session, keyspace)

        if split_count is None:
            # Default: 4 splits per node
            split_count = len(self.session.cluster.contact_points) * 4  # type: ignore[attr-defined]

        splits = self.splitter.split_proportionally(ranges, split_count)

        # Initialize stats
        stats = BulkOperationStats(total_ranges=len(splits))

        # Determine parallelism
        if parallelism is None:
            parallelism = min(len(splits), len(self.session.cluster.contact_points) * 2)  # type: ignore[attr-defined]

        # Create count tasks
        semaphore = asyncio.Semaphore(parallelism)
        tasks = []

        for split in splits:
            task = self._count_range(
                keyspace, table, partition_keys, split, semaphore, stats, progress_callback
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
                total_count += result

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
    ) -> int:
        """Count rows in a single token range."""
        async with semaphore:
            query = generate_token_range_query(
                keyspace=keyspace,
                table=table,
                partition_keys=partition_keys,
                token_range=token_range,
            )

            # Add COUNT(*) to query
            count_query = query.replace("SELECT *", "SELECT COUNT(*)")

            result = await self.session.execute(count_query)
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
    ) -> AsyncIterator[Any]:
        """Export all rows from a table by streaming token ranges."""
        # Get table metadata
        table_meta = await self._get_table_metadata(keyspace, table)
        partition_keys = [col.name for col in table_meta.partition_key]

        # Discover and split token ranges
        ranges = await discover_token_ranges(self.session, keyspace)

        if split_count is None:
            split_count = len(self.session.cluster.contact_points) * 4  # type: ignore[attr-defined]

        splits = self.splitter.split_proportionally(ranges, split_count)

        # Initialize stats
        stats = BulkOperationStats(total_ranges=len(splits))

        # Stream results from each range
        for split in splits:
            query = generate_token_range_query(
                keyspace=keyspace, table=table, partition_keys=partition_keys, token_range=split
            )

            # Stream results from this range
            async with await self.session.execute_stream(query) as result:
                async for row in result:
                    stats.rows_processed += 1
                    yield row

            stats.ranges_completed += 1

            if progress_callback:
                progress_callback(stats)

        stats.end_time = time.time()

    async def export_to_iceberg(
        self,
        source_keyspace: str,
        source_table: str,
        iceberg_warehouse_path: str,
        iceberg_table: str,
        partition_by: list[str] | None = None,
        split_count: int | None = None,
        batch_size: int = 10000,
        progress_callback: Callable[[BulkOperationStats], None] | None = None,
    ) -> BulkOperationStats:
        """Export Cassandra table to Iceberg format."""
        # This will be implemented when we add Iceberg integration
        raise NotImplementedError("Iceberg export will be implemented in next phase")

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
        metadata = self.session.cluster.metadata  # type: ignore[attr-defined]

        if keyspace not in metadata.keyspaces:
            raise ValueError(f"Keyspace '{keyspace}' not found")

        keyspace_meta = metadata.keyspaces[keyspace]

        if table not in keyspace_meta.tables:
            raise ValueError(f"Table '{table}' not found in keyspace '{keyspace}'")

        return keyspace_meta.tables[table]
