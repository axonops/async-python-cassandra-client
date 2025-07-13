"""
Proper streaming implementation for Cassandra data.

This module provides streaming functionality that:
1. ALWAYS uses async streaming (no fallbacks)
2. Properly handles token-based pagination
3. Manages memory efficiently
4. Has low cyclomatic complexity
"""

from typing import Any

import pandas as pd
from async_cassandra.streaming import StreamConfig


class CassandraStreamer:
    """Handles streaming of Cassandra data with proper pagination."""

    def __init__(self, session):
        """Initialize streamer with session."""
        self.session = session

    async def stream_query(
        self,
        query: str,
        values: tuple,
        columns: list[str],
        fetch_size: int = 5000,
        memory_limit_mb: int = 128,
        consistency_level=None,
        table_metadata: dict | None = None,
        type_mapper: Any | None = None,
    ) -> pd.DataFrame:
        """
        Stream data from a simple query (no token pagination needed).

        Args:
            query: CQL query to execute
            values: Query parameters
            columns: Column names for DataFrame
            fetch_size: Rows per fetch
            memory_limit_mb: Memory limit in MB

        Returns:
            DataFrame with query results
        """
        # Set up progress logging
        rows_processed = 0

        async def log_progress(page_num: int, rows_in_page: int):
            nonlocal rows_processed
            rows_processed += rows_in_page
            if rows_processed > 0 and rows_processed % 10000 == 0:
                import logging

                logging.info(f"Streamed {rows_processed} rows from {query[:50]}...")

        stream_config = StreamConfig(fetch_size=fetch_size, page_callback=log_progress)
        prepared = await self.session.prepare(query)

        # Set consistency level on the prepared statement
        if consistency_level:
            prepared.consistency_level = consistency_level

        # Use incremental builder instead of collecting rows
        from .incremental_builder import IncrementalDataFrameBuilder

        builder = IncrementalDataFrameBuilder(
            columns=columns,
            chunk_size=fetch_size,
            type_mapper=type_mapper,
            table_metadata=table_metadata,
        )
        memory_limit_bytes = memory_limit_mb * 1024 * 1024

        # Execute streaming query
        stream_result = await self.session.execute_stream(
            prepared, values, stream_config=stream_config
        )

        # Stream data directly into builder
        # IMPORTANT: We do NOT break on memory limit - that would lose data!
        # Memory limit is for planning partition sizes, not truncating results
        memory_exceeded = False

        async with stream_result as stream:
            async for row in stream:
                builder.add_row(row)

                # Check memory periodically - but only to warn
                if builder.total_rows % 1000 == 0:
                    if builder.get_memory_usage() > memory_limit_bytes and not memory_exceeded:
                        import logging

                        logging.warning(
                            f"Memory limit of {memory_limit_mb}MB exceeded after {builder.total_rows} rows. "
                            f"Consider using more partitions or increasing memory_per_partition_mb."
                        )
                        memory_exceeded = True
                        # DO NOT BREAK - that would lose data!

        return builder.get_dataframe()

    async def stream_token_range(
        self,
        table: str,
        columns: list[str],
        partition_keys: list[str],
        start_token: int,
        end_token: int,
        fetch_size: int = 5000,
        memory_limit_mb: int = 128,
        where_clause: str = "",
        where_values: tuple = (),
        consistency_level=None,
        table_metadata: dict | None = None,
        type_mapper: Any | None = None,
    ) -> pd.DataFrame:
        """
        Stream data from a token range with proper pagination.

        This properly handles token pagination to fetch ALL data,
        not just the first page.

        Args:
            table: Table name
            columns: Columns to select
            partition_keys: Partition key columns
            start_token: Start of token range
            end_token: End of token range
            fetch_size: Rows per fetch
            memory_limit_mb: Memory limit in MB
            where_clause: Additional WHERE conditions
            where_values: Values for WHERE clause

        Returns:
            DataFrame with all data in token range
        """
        # Build token expression
        if len(partition_keys) == 1:
            token_expr = f"TOKEN({partition_keys[0]})"
        else:
            token_expr = f"TOKEN({', '.join(partition_keys)})"

        # Build base query
        select_list = ", ".join(columns)
        base_query = f"SELECT {select_list} FROM {table}"

        # Add WHERE clause
        where_parts = []
        values_list = list(where_values)

        if where_clause:
            where_parts.append(where_clause)

        # Token range condition
        where_parts.append(f"{token_expr} >= ? AND {token_expr} <= ?")
        values_list.extend([start_token, end_token])

        if where_parts:
            base_query += " WHERE " + " AND ".join(where_parts)

        # Add LIMIT for pagination
        query = base_query + f" LIMIT {fetch_size}"

        # Use incremental builder
        from .incremental_builder import IncrementalDataFrameBuilder

        builder = IncrementalDataFrameBuilder(
            columns=columns,
            chunk_size=fetch_size,
            type_mapper=type_mapper,
            table_metadata=table_metadata,
        )
        memory_limit_bytes = memory_limit_mb * 1024 * 1024
        current_start_token = start_token
        total_rows_for_range = 0

        while current_start_token <= end_token:
            # Update token range in values
            current_values = values_list.copy()
            current_values[-2] = current_start_token  # Update start token

            # Stream this batch
            rows = await self._stream_batch(
                query, tuple(current_values), columns, fetch_size, consistency_level
            )

            if not rows:
                break  # No more data

            # Add rows to builder incrementally
            for row in rows:
                builder.add_row(row)

            total_rows_for_range += len(rows)

            # Check memory limit - but only warn, don't break!
            if builder.get_memory_usage() > memory_limit_bytes:
                import logging

                logging.warning(
                    f"Memory limit of {memory_limit_mb}MB exceeded after {total_rows_for_range} rows in token range. "
                    f"Consider using more partitions."
                )
                # DO NOT BREAK - we must read the complete token range!

            # If we got fewer rows than limit, we're done
            if len(rows) < fetch_size:
                break

            # Calculate next start token
            # Get the token of the last row
            last_row = rows[-1]
            last_token = await self._get_row_token(table, partition_keys, last_row)

            if last_token is None or last_token >= end_token:
                break

            # Continue from next token
            current_start_token = last_token + 1

        return builder.get_dataframe()

    async def _stream_batch(
        self, query: str, values: tuple, columns: list[str], fetch_size: int, consistency_level=None
    ) -> list:
        """Stream a single batch of data."""
        stream_config = StreamConfig(fetch_size=fetch_size)
        prepared = await self.session.prepare(query)

        # Set consistency level on the prepared statement
        if consistency_level:
            prepared.consistency_level = consistency_level

        rows = []
        stream_result = await self.session.execute_stream(
            prepared, values, stream_config=stream_config
        )

        async with stream_result as stream:
            async for row in stream:
                rows.append(row)

        return rows

    async def _get_row_token(self, table: str, partition_keys: list[str], row: Any) -> int | None:
        """Get the token value for a row."""
        if not hasattr(row, "_asdict"):
            return None

        row_dict = row._asdict()

        # Build token query
        if len(partition_keys) == 1:
            token_expr = f"TOKEN({partition_keys[0]})"
        else:
            token_expr = f"TOKEN({', '.join(partition_keys)})"

        # Build WHERE clause for this row
        where_parts = []
        values = []
        for pk in partition_keys:
            if pk not in row_dict:
                return None
            where_parts.append(f"{pk} = ?")
            values.append(row_dict[pk])

        query = f"SELECT {token_expr} AS token_value FROM {table} WHERE {' AND '.join(where_parts)}"

        # Execute query
        prepared = await self.session.prepare(query)
        result = await self.session.execute(prepared, tuple(values))
        token_row = result.one()

        return token_row.token_value if token_row else None

    def _rows_to_dataframe(self, rows: list, columns: list[str]) -> pd.DataFrame:
        """Convert rows to DataFrame with UDT handling."""
        if not rows:
            return pd.DataFrame(columns=columns)

        # Convert rows to dicts, handling UDTs
        data = []
        for row in rows:
            row_dict = {}
            if hasattr(row, "_asdict"):
                temp_dict = row._asdict()
                for key, value in temp_dict.items():
                    row_dict[key] = self._convert_value(value)
            else:
                # Handle Row objects
                for col in columns:
                    if hasattr(row, col):
                        value = getattr(row, col)
                        row_dict[col] = self._convert_value(value)

            data.append(row_dict)

        return pd.DataFrame(data)

    def _convert_value(self, value: Any) -> Any:
        """Convert UDTs to dicts recursively."""
        if hasattr(value, "_fields") and hasattr(value, "_asdict"):
            # It's a UDT - convert to dict
            result = {}
            for field in value._fields:
                field_value = getattr(value, field)
                result[field] = self._convert_value(field_value)
            return result
        elif isinstance(value, list | tuple):
            # Handle collections containing UDTs
            return [self._convert_value(item) for item in value]
        elif isinstance(value, dict):
            # Handle maps containing UDTs
            return {k: self._convert_value(v) for k, v in value.items()}
        else:
            return value
