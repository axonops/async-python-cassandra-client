"""
Parallel partition reading for async-cassandra-dataframe.

Provides concurrent execution of partition queries with proper
resource management and error handling.
"""

import asyncio
import time
from collections.abc import Callable
from typing import Any

import pandas as pd


class ParallelExecutionError(Exception):
    """
    Exception raised when parallel execution encounters errors.

    Attributes:
        errors: List of original exceptions
        successful_count: Number of successful partitions
        failed_count: Number of failed partitions
        partial_results: List of DataFrames from successful partitions (if any)
    """

    def __init__(self, message: str):
        super().__init__(message)
        self.errors = []
        self.successful_count = 0
        self.failed_count = 0
        self.partial_results = None


class ParallelPartitionReader:
    """
    Executes partition queries in parallel with concurrency control.

    Key features:
    - Configurable concurrency limits
    - Progress tracking
    - Error isolation
    - Resource management
    """

    def __init__(
        self,
        session,
        max_concurrent: int = 10,
        progress_callback: Callable | None = None,
        allow_partial_results: bool = False,
    ):
        """
        Initialize parallel reader.

        Args:
            session: AsyncCassandraSession
            max_concurrent: Maximum concurrent queries
            progress_callback: Optional callback for progress updates
            allow_partial_results: If True, return partial results on error
        """
        self.session = session
        self.max_concurrent = max_concurrent
        self.progress_callback = progress_callback
        self.allow_partial_results = allow_partial_results
        self._semaphore = asyncio.Semaphore(max_concurrent)

    async def read_partitions(self, partitions: list[dict[str, Any]]) -> list[pd.DataFrame]:
        """
        Read multiple partitions in parallel.

        Args:
            partitions: List of partition definitions

        Returns:
            List of DataFrames (one per partition)

        Raises:
            Exception: If any partition fails (unless partial results enabled)
        """
        total = len(partitions)
        completed = 0

        # Create wrapper to track partition info through execution
        async def read_partition_with_info(partition, index):
            """Wrapper that includes partition info in result."""
            try:
                df = await self._read_single_partition(partition, index, total)
                return {"index": index, "partition": partition, "df": df, "error": None}
            except Exception as e:
                return {"index": index, "partition": partition, "df": None, "error": e}

        # Create tasks
        tasks = [
            asyncio.create_task(read_partition_with_info(partition, i))
            for i, partition in enumerate(partitions)
        ]

        # Execute and collect results as they complete
        results = []
        errors = []

        for coro in asyncio.as_completed(tasks):
            result_info = await coro
            completed += 1

            if result_info["error"]:
                errors.append(
                    (result_info["index"], result_info["partition"], result_info["error"])
                )

                if self.progress_callback:
                    await self.progress_callback(
                        completed,
                        total,
                        f"Failed partition {result_info['index']}: {str(result_info['error'])}",
                    )
            else:
                results.append(result_info["df"])

                if self.progress_callback:
                    await self.progress_callback(
                        completed, total, f"Completed {completed}/{total} partitions"
                    )

        # Handle errors with better aggregation
        if errors:
            # If partial results are allowed and we have some successes, return them
            if self.allow_partial_results and results:
                # Log the errors but return partial results
                import warnings

                error_summary = (
                    f"Completed {len(results)}/{total} partitions with {len(errors)} failures"
                )
                warnings.warn(error_summary, UserWarning, stacklevel=2)
                return results

            # Otherwise, aggregate and raise detailed error
            # Group errors by type
            from collections import defaultdict

            error_types = defaultdict(list)
            for partition_idx, partition, error in errors:
                error_type = type(error).__name__
                partition_id = partition.get("partition_id", partition_idx)
                error_types[error_type].append((partition_id, str(error)))

            # Build detailed error message
            error_parts = [f"Failed to read {len(errors)} partitions:"]

            for error_type, instances in error_types.items():
                error_parts.append(f"\n  {error_type} ({len(instances)} occurrences):")
                # Show up to 3 examples per error type
                for partition_id, error_msg in instances[:3]:
                    error_parts.append(f"    - Partition {partition_id}: {error_msg}")
                if len(instances) > 3:
                    error_parts.append(f"    ... and {len(instances) - 3} more")

            # Include summary
            error_parts.append(
                f"\nTotal partitions: {total}, Successful: {len(results)}, Failed: {len(errors)}"
            )

            # Create a custom exception with all error details
            full_error_msg = "\n".join(error_parts)
            exception = ParallelExecutionError(full_error_msg)
            exception.errors = [e for _, _, e in errors]  # Original exceptions
            exception.successful_count = len(results)
            exception.failed_count = len(errors)
            exception.partial_results = results if results else None
            raise exception

        return results

    async def _read_single_partition(
        self, partition: dict[str, Any], index: int, total: int
    ) -> pd.DataFrame:
        """
        Read a single partition with concurrency control.

        Args:
            partition: Partition definition
            index: Partition index (for progress)
            total: Total partitions (for progress)

        Returns:
            DataFrame with partition data
        """
        async with self._semaphore:
            # Import here to avoid circular dependency
            from .partition import StreamingPartitionStrategy

            # Extract session from partition or use default
            session = partition.get("session", self.session)

            # Create strategy for this partition
            strategy = StreamingPartitionStrategy(
                session=session, memory_per_partition_mb=partition.get("memory_limit_mb", 128)
            )

            # Stream the partition
            start_time = time.time()
            df = await strategy.stream_partition(partition)
            duration = time.time() - start_time

            # Add metadata if requested
            if partition.get("add_partition_metadata", False):
                df["_partition_id"] = partition.get("partition_id", index)
                df["_read_duration_ms"] = int(duration * 1000)

            return df


async def execute_parallel_token_queries(
    session,
    table: str,
    token_ranges: list[Any],  # List[TokenRange]
    columns: list[str],
    max_concurrent: int = 10,
    **kwargs,
) -> pd.DataFrame:
    """
    Execute token range queries in parallel.

    Args:
        session: AsyncCassandraSession
        table: Full table name (keyspace.table)
        token_ranges: List of TokenRange objects
        columns: Columns to select
        max_concurrent: Max concurrent queries
        **kwargs: Additional arguments for queries

    Returns:
        Combined DataFrame from all ranges
    """
    from .token_ranges import generate_token_range_query, handle_wraparound_ranges

    # Parse table name
    if "." in table:
        keyspace, table_name = table.split(".", 1)
    else:
        raise ValueError("Table must be fully qualified: keyspace.table")

    # Handle wraparound ranges
    ranges = handle_wraparound_ranges(token_ranges)

    # Get partition keys from metadata
    partition_keys = kwargs.get("partition_keys", ["id"])  # Fallback

    # Create partition definitions
    partitions = []
    for i, token_range in enumerate(ranges):
        # Generate query for this range
        query = generate_token_range_query(
            keyspace=keyspace,
            table=table_name,
            partition_keys=partition_keys,
            token_range=token_range,
            columns=columns,
            writetime_columns=kwargs.get("writetime_columns"),
            ttl_columns=kwargs.get("ttl_columns"),
        )

        partition = {
            "partition_id": i,
            "query": query,
            "token_range": token_range,
            "columns": columns,
            "table": table,
            **kwargs,  # Pass through other options
        }
        partitions.append(partition)

    # Create parallel reader
    reader = ParallelPartitionReader(
        session=session,
        max_concurrent=max_concurrent,
        progress_callback=kwargs.get("progress_callback"),
    )

    # Execute in parallel
    dfs = await reader.read_partitions(partitions)

    # Combine results
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    else:
        # Return empty DataFrame with correct schema
        return pd.DataFrame(columns=columns)
