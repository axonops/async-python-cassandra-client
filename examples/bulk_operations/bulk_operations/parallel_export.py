"""
Parallel export implementation for production-grade bulk operations.

This module provides a truly parallel export capability that streams data
from multiple token ranges concurrently, similar to DSBulk.
"""

import asyncio
from collections.abc import AsyncIterator, Callable
from typing import Any

from cassandra import ConsistencyLevel

from .stats import BulkOperationStats
from .token_utils import TokenRange


class ParallelExportIterator:
    """
    Parallel export iterator that manages concurrent token range queries.

    This implementation uses asyncio queues to coordinate between multiple
    worker tasks that query different token ranges in parallel.
    """

    def __init__(
        self,
        operator: Any,
        keyspace: str,
        table: str,
        splits: list[TokenRange],
        prepared_stmts: dict[str, Any],
        parallelism: int,
        consistency_level: ConsistencyLevel | None,
        stats: BulkOperationStats,
        progress_callback: Callable[[BulkOperationStats], None] | None,
    ):
        self.operator = operator
        self.keyspace = keyspace
        self.table = table
        self.splits = splits
        self.prepared_stmts = prepared_stmts
        self.parallelism = parallelism
        self.consistency_level = consistency_level
        self.stats = stats
        self.progress_callback = progress_callback

        # Queue for results from parallel workers
        self.result_queue: asyncio.Queue[tuple[Any, bool]] = asyncio.Queue(maxsize=parallelism * 10)
        self.workers_done = False
        self.worker_tasks: list[asyncio.Task] = []

    async def __aiter__(self) -> AsyncIterator[Any]:
        """Start parallel workers and yield results as they come in."""
        # Start worker tasks
        await self._start_workers()

        # Yield results from the queue
        while True:
            try:
                # Wait for results with a timeout to check if workers are done
                row, is_end_marker = await asyncio.wait_for(self.result_queue.get(), timeout=0.1)

                if is_end_marker:
                    # This was an end marker from a worker
                    continue

                yield row

            except TimeoutError:
                # Check if all workers are done
                if self.workers_done and self.result_queue.empty():
                    break
                continue
            except Exception:
                # Cancel all workers on error
                await self._cancel_workers()
                raise

    async def _start_workers(self) -> None:
        """Start parallel worker tasks to process token ranges."""
        # Create a semaphore to limit concurrent queries
        semaphore = asyncio.Semaphore(self.parallelism)

        # Create worker tasks for each split
        for split in self.splits:
            task = asyncio.create_task(self._process_split(split, semaphore))
            self.worker_tasks.append(task)

        # Create a task to monitor when all workers are done
        asyncio.create_task(self._monitor_workers())

    async def _monitor_workers(self) -> None:
        """Monitor worker tasks and signal when all are complete."""
        try:
            # Wait for all workers to complete
            await asyncio.gather(*self.worker_tasks, return_exceptions=True)
        finally:
            self.workers_done = True
            # Put a final marker to unblock the iterator if needed
            await self.result_queue.put((None, True))

    async def _cancel_workers(self) -> None:
        """Cancel all worker tasks."""
        for task in self.worker_tasks:
            if not task.done():
                task.cancel()

        # Wait for cancellation to complete
        await asyncio.gather(*self.worker_tasks, return_exceptions=True)

    async def _process_split(self, split: TokenRange, semaphore: asyncio.Semaphore) -> None:
        """Process a single token range split."""
        async with semaphore:
            try:
                if split.end < split.start:
                    # Wraparound range - process in two parts
                    await self._query_and_queue(
                        self.prepared_stmts["select_wraparound_gt"], (split.start,)
                    )
                    await self._query_and_queue(
                        self.prepared_stmts["select_wraparound_lte"], (split.end,)
                    )
                else:
                    # Normal range
                    await self._query_and_queue(
                        self.prepared_stmts["select_range"], (split.start, split.end)
                    )

                # Update stats
                self.stats.ranges_completed += 1
                if self.progress_callback:
                    self.progress_callback(self.stats)

            except Exception as e:
                # Add error to stats but don't fail the whole export
                self.stats.errors.append(e)
                # Put an end marker to signal this worker is done
                await self.result_queue.put((None, True))
                raise

            # Signal this worker is done
            await self.result_queue.put((None, True))

    async def _query_and_queue(self, stmt: Any, params: tuple) -> None:
        """Execute a query and queue all results."""
        # Set consistency level if provided
        if self.consistency_level is not None:
            stmt.consistency_level = self.consistency_level

        # Execute streaming query
        async with await self.operator.session.execute_stream(stmt, params) as result:
            async for row in result:
                self.stats.rows_processed += 1
                # Queue the row for the main iterator
                await self.result_queue.put((row, False))


async def export_by_token_ranges_parallel(
    operator: Any,
    keyspace: str,
    table: str,
    splits: list[TokenRange],
    prepared_stmts: dict[str, Any],
    parallelism: int,
    consistency_level: ConsistencyLevel | None,
    stats: BulkOperationStats,
    progress_callback: Callable[[BulkOperationStats], None] | None,
) -> AsyncIterator[Any]:
    """
    Export rows from token ranges in parallel.

    This function creates a parallel export iterator that manages multiple
    concurrent queries to different token ranges, similar to how DSBulk works.

    Args:
        operator: The bulk operator instance
        keyspace: Keyspace name
        table: Table name
        splits: List of token ranges to query
        prepared_stmts: Prepared statements for queries
        parallelism: Maximum concurrent queries
        consistency_level: Consistency level for queries
        stats: Statistics object to update
        progress_callback: Optional progress callback

    Yields:
        Rows from the table, streamed as they arrive from parallel queries
    """
    iterator = ParallelExportIterator(
        operator=operator,
        keyspace=keyspace,
        table=table,
        splits=splits,
        prepared_stmts=prepared_stmts,
        parallelism=parallelism,
        consistency_level=consistency_level,
        stats=stats,
        progress_callback=progress_callback,
    )

    async for row in iterator:
        yield row
