"""
Incremental DataFrame builder using streaming callbacks.

This module provides a more memory-efficient way to build DataFrames
by processing rows as they arrive rather than collecting all rows first.
"""

import asyncio
from collections.abc import Callable
from typing import Any

import pandas as pd


class IncrementalDataFrameBuilder:
    """
    Builds a DataFrame incrementally as rows are streamed.

    This is more memory efficient than collecting all rows first
    because we can:
    1. Convert types as we go
    2. Use pandas' internal optimizations
    3. Detect memory limits earlier
    4. Process/filter data during streaming
    """

    def __init__(
        self,
        columns: list[str],
        chunk_size: int = 1000,
        type_mapper: Any | None = None,
        table_metadata: dict | None = None,
    ):
        """
        Initialize incremental builder.

        Args:
            columns: Column names
            chunk_size: Rows per chunk before consolidation
            type_mapper: Optional type mapper for conversions
            table_metadata: Optional table metadata for type inference
        """
        self.columns = columns
        self.chunk_size = chunk_size
        self.type_mapper = type_mapper
        self.table_metadata = table_metadata

        # Store data in chunks
        self.chunks: list[pd.DataFrame] = []
        self.current_chunk_data: list[dict] = []
        self.total_rows = 0

    def add_row(self, row: Any) -> None:
        """
        Add a single row to the builder.

        This method is designed to be called from streaming callbacks.
        """
        # Convert row to dict
        row_dict = self._row_to_dict(row)

        # Apply type conversions if mapper provided
        if self.type_mapper:
            row_dict = self._apply_type_conversions(row_dict)

        self.current_chunk_data.append(row_dict)
        self.total_rows += 1

        # Consolidate chunk if it's full
        if len(self.current_chunk_data) >= self.chunk_size:
            self._consolidate_chunk()

    def _row_to_dict(self, row: Any) -> dict:
        """Convert a row object to dictionary."""
        if hasattr(row, "_asdict"):
            return row._asdict()
        elif hasattr(row, "__dict__"):
            return row.__dict__
        elif isinstance(row, dict):
            return row
        else:
            # Fallback - try to extract by column names
            result = {}
            for col in self.columns:
                if hasattr(row, col):
                    result[col] = getattr(row, col)
            return result

    def _apply_type_conversions(self, row_dict: dict) -> dict:
        """Apply type conversions to row data."""
        # This is a placeholder - integrate with existing type mapper
        return row_dict

    def _consolidate_chunk(self) -> None:
        """Convert current chunk data to DataFrame and store."""
        if self.current_chunk_data:
            # Create DataFrame with explicit dtypes to avoid string conversion
            chunk_df = pd.DataFrame(self.current_chunk_data)

            # Apply type conversions if we have metadata
            if self.table_metadata and self.type_mapper:
                from .type_converter import DataFrameTypeConverter

                chunk_df = DataFrameTypeConverter.convert_dataframe_types(
                    chunk_df, self.table_metadata, self.type_mapper
                )

            self.chunks.append(chunk_df)
            self.current_chunk_data = []

    def get_dataframe(self) -> pd.DataFrame:
        """
        Get the final DataFrame.

        This consolidates any remaining data and concatenates all chunks.
        """
        # Consolidate any remaining data
        self._consolidate_chunk()

        if not self.chunks:
            return pd.DataFrame(columns=self.columns)

        # Concatenate all chunks efficiently
        return pd.concat(self.chunks, ignore_index=True)

    def get_memory_usage(self) -> int:
        """Get approximate memory usage in bytes."""
        memory = 0

        # Memory from consolidated chunks
        for chunk in self.chunks:
            memory += chunk.memory_usage(deep=True).sum()

        # Estimate memory from current chunk
        memory += len(self.current_chunk_data) * len(self.columns) * 50

        return memory


class StreamingDataFrameBuilder:
    """
    Enhanced streaming with incremental DataFrame building.

    This integrates with async-cassandra's streaming to build
    DataFrames more efficiently.
    """

    def __init__(self, session):
        """Initialize with session."""
        self.session = session

    async def stream_to_dataframe(
        self,
        query: str,
        values: tuple,
        columns: list[str],
        fetch_size: int = 5000,
        memory_limit_mb: int = 128,
        progress_callback: Callable | None = None,
    ) -> pd.DataFrame:
        """
        Stream query results directly into a DataFrame.

        This is more memory efficient than collecting all rows first.
        """
        from async_cassandra.streaming import StreamConfig

        # Create incremental builder
        builder = IncrementalDataFrameBuilder(columns=columns, chunk_size=fetch_size)

        # Configure streaming with progress callback
        rows_processed = 0
        memory_limit_bytes = memory_limit_mb * 1024 * 1024

        async def internal_progress(current: int, total: int):
            nonlocal rows_processed
            rows_processed = current

            # Check memory usage
            if builder.get_memory_usage() > memory_limit_bytes:
                # We could implement early termination here
                pass

            # Call user progress callback
            if progress_callback:
                await progress_callback(current, total, "Streaming rows")

        # Configure streaming
        stream_config = StreamConfig(
            fetch_size=fetch_size, page_callback=internal_progress if progress_callback else None
        )

        # Prepare and execute query
        prepared = await self.session.prepare(query)
        stream_result = await self.session.execute_stream(
            prepared, values, stream_config=stream_config
        )

        # Stream rows directly into builder
        async with stream_result as stream:
            async for row in stream:
                builder.add_row(row)

                # Check memory periodically
                if builder.total_rows % 1000 == 0:
                    if builder.get_memory_usage() > memory_limit_bytes:
                        break

        return builder.get_dataframe()


async def parallel_stream_to_dataframe(
    session, queries: list[tuple[str, tuple]], columns: list[str], max_concurrent: int = 5, **kwargs
) -> pd.DataFrame:
    """
    Execute multiple streaming queries in parallel and combine results.

    This leverages asyncio for true parallel streaming.
    """
    builder = StreamingDataFrameBuilder(session)

    # Create tasks for parallel execution
    tasks = []
    semaphore = asyncio.Semaphore(max_concurrent)

    async def stream_with_limit(query: str, values: tuple):
        async with semaphore:
            return await builder.stream_to_dataframe(query, values, columns, **kwargs)

    for query, values in queries:
        task = asyncio.create_task(stream_with_limit(query, values))
        tasks.append(task)

    # Execute all streams in parallel
    dfs = await asyncio.gather(*tasks)

    # Combine results
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    else:
        return pd.DataFrame(columns=columns)
