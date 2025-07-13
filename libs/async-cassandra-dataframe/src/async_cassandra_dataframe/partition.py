"""
Partition management using streaming/adaptive approach.

No upfront size estimation needed - partitions are created by streaming
data until memory limits are reached.
"""

from collections.abc import AsyncIterator
from typing import Any

import pandas as pd


class StreamingPartitionStrategy:
    """
    Streaming partition strategy that reads data in memory-bounded chunks.

    Key insight: We don't need to know total size upfront. We just need
    to ensure each partition fits in memory.
    """

    # Token range bounds for Murmur3
    MIN_TOKEN = -9223372036854775808  # -2^63
    MAX_TOKEN = 9223372036854775807  # 2^63 - 1

    def __init__(
        self,
        session,
        memory_per_partition_mb: int = 128,
        batch_size: int = 5000,
        sample_size: int = 5000,
    ):
        """
        Initialize streaming partition strategy.

        Args:
            session: AsyncSession instance
            memory_per_partition_mb: Target memory size per partition
            batch_size: Rows to fetch per query
            sample_size: Rows to sample for calibration
        """
        self.session = session
        self.memory_per_partition_mb = memory_per_partition_mb
        self.batch_size = batch_size
        self.sample_size = sample_size

    async def create_partitions(
        self,
        table: str,
        columns: list[str],
        partition_count: int | None = None,
        use_token_ranges: bool = True,
        pushdown_predicates: list | None = None,
    ) -> list[dict[str, Any]]:
        """
        Create partition definitions for streaming.

        If partition_count is specified, create fixed partitions.
        Otherwise, use adaptive streaming approach.

        Args:
            table: Full table name (keyspace.table)
            columns: Columns to read
            partition_count: Fixed partition count (overrides adaptive)
            use_token_ranges: Whether to use token ranges (disabled when partition key predicates exist)
            pushdown_predicates: Predicates to push down to Cassandra

        Returns:
            List of partition definitions
        """
        # If we have predicates but not using token ranges, create single partition
        if not use_token_ranges:
            return [
                {
                    "partition_id": 0,
                    "table": table,
                    "columns": columns,
                    "start_token": None,
                    "end_token": None,
                    "strategy": "predicate",
                    "memory_limit_mb": self.memory_per_partition_mb,
                    "use_token_ranges": False,
                }
            ]

        # Parse keyspace from table name
        if "." in table:
            keyspace, _ = table.split(".", 1)
        else:
            raise ValueError("Table must be fully qualified: keyspace.table")

        # Discover actual token ranges from cluster
        from .token_ranges import discover_token_ranges, split_proportionally

        token_ranges = await discover_token_ranges(self.session, keyspace)

        # Debug: print token ranges
        # print(f"Discovered {len(token_ranges)} token ranges from cluster")

        if partition_count:
            # User specified exact partition count - split proportionally
            split_ranges = split_proportionally(token_ranges, partition_count)
        else:
            # Adaptive approach - estimate based on data size
            avg_row_size = await self._calibrate_row_size(table, columns, pushdown_predicates)

            # Estimate number of splits needed
            memory_limit_bytes = self.memory_per_partition_mb * 1024 * 1024
            rows_per_partition = int(memory_limit_bytes / avg_row_size)

            # Estimate total rows (very rough - assumes even distribution)
            # In production, would query COUNT(*) or use statistics
            estimated_total_rows = rows_per_partition * len(token_ranges) * 10
            target_partitions = max(len(token_ranges), estimated_total_rows // rows_per_partition)

            split_ranges = split_proportionally(token_ranges, target_partitions)

        # Create partition definitions from token ranges
        partitions = []
        for i, token_range in enumerate(split_ranges):
            partitions.append(
                {
                    "partition_id": i,
                    "table": table,
                    "columns": columns,
                    "start_token": token_range.start,
                    "end_token": token_range.end,
                    "token_range": token_range,  # Include full range object
                    "replicas": token_range.replicas,
                    "strategy": "token_range",
                    "memory_limit_mb": self.memory_per_partition_mb,
                    "use_token_ranges": True,
                }
            )

        return partitions

    async def _calibrate_row_size(
        self, table: str, columns: list[str], pushdown_predicates: list | None = None
    ) -> float:
        """
        Sample data to estimate average row memory size.

        Args:
            table: Table to sample
            columns: Columns to include
            pushdown_predicates: Optional predicates to apply during sampling

        Returns:
            Average row size in bytes
        """
        # Read sample
        column_list = ", ".join(columns)
        query = f"SELECT {column_list} FROM {table}"

        # Add predicates if any
        if pushdown_predicates:
            where_clauses = []
            for pred in pushdown_predicates:
                col = pred["column"]
                op = pred["operator"]
                val = pred["value"]

                if op == "IN":
                    placeholders = ", ".join(["?" for _ in val])
                    where_clauses.append(f"{col} IN ({placeholders})")
                else:
                    where_clauses.append(f"{col} {op} ?")

            if where_clauses:
                query += " WHERE " + " AND ".join(where_clauses)

        query += f" LIMIT {self.sample_size}"

        try:
            # Prepare values for binding
            values = []
            if pushdown_predicates:
                for pred in pushdown_predicates:
                    if pred["operator"] == "IN":
                        values.extend(pred["value"])
                    else:
                        values.append(pred["value"])

            if values:
                prepared = await self.session.prepare(query)
                result = await self.session.execute(prepared, values)
            else:
                result = await self.session.execute(query)

            rows = list(result)

            if not rows:
                # No data, use conservative estimate
                return 1024  # 1KB per row default

            # Convert to DataFrame to measure memory
            df = pd.DataFrame([row._asdict() for row in rows])

            # Get deep memory usage
            memory_usage = df.memory_usage(deep=True).sum()
            avg_size = memory_usage / len(df)

            # Add 20% safety margin
            return avg_size * 1.2

        except Exception:
            # If sampling fails, use conservative default
            return 1024

    def _create_fixed_partitions(
        self, table: str, columns: list[str], partition_count: int
    ) -> list[dict[str, Any]]:
        """Create fixed number of partitions."""
        # This method is now deprecated - use create_partitions with partition_count
        # Kept for backward compatibility
        raise DeprecationWarning(
            "_create_fixed_partitions is deprecated. Use create_partitions with partition_count parameter."
        )

    async def _create_adaptive_partitions(
        self, table: str, columns: list[str], avg_row_size: float
    ) -> list[dict[str, Any]]:
        """
        Create adaptive partitions based on memory constraints.

        This method is now integrated into create_partitions.
        """
        # This method is now deprecated - logic moved to create_partitions
        raise DeprecationWarning(
            "_create_adaptive_partitions is deprecated. Logic is now in create_partitions."
        )

    def _split_token_ring(self, num_splits: int) -> list[tuple[int, int]]:
        """Split token ring into equal ranges.

        DEPRECATED: This method uses arbitrary token splitting which doesn't
        respect actual cluster topology. Use token range discovery instead.
        """
        raise DeprecationWarning(
            "_split_token_ring is deprecated. Use discover_token_ranges for actual cluster topology."
        )

    async def stream_partition(self, partition_def: dict[str, Any]) -> pd.DataFrame:
        """
        Stream a single partition with memory bounds.

        Args:
            partition_def: Partition definition

        Returns:
            DataFrame containing partition data
        """
        table = partition_def["table"]
        columns = partition_def["columns"]
        memory_limit_mb = partition_def["memory_limit_mb"]
        use_token_ranges = partition_def.get("use_token_ranges", True)
        pushdown_predicates = partition_def.get("pushdown_predicates", [])
        allow_filtering = partition_def.get("allow_filtering", False)
        page_size = partition_def.get("page_size")
        adaptive_page_size = partition_def.get("adaptive_page_size", False)

        # Build query with writetime/TTL columns
        query_builder = partition_def.get("query_builder")
        writetime_columns = partition_def.get("writetime_columns", [])
        ttl_columns = partition_def.get("ttl_columns", [])

        if query_builder:
            # Use the query builder to properly handle writetime/TTL columns
            query, values = query_builder.build_partition_query(
                columns=columns,
                writetime_columns=writetime_columns,
                ttl_columns=ttl_columns,
                predicates=pushdown_predicates if not use_token_ranges else None,
                allow_filtering=allow_filtering,
                token_range=(
                    (partition_def.get("start_token"), partition_def.get("end_token"))
                    if use_token_ranges
                    else None
                ),
            )
        else:
            # Fallback to manual query building
            select_parts = list(columns)

            # Add writetime columns
            if writetime_columns:
                for col in writetime_columns:
                    if col in columns:
                        select_parts.append(f"WRITETIME({col}) AS {col}_writetime")

            # Add TTL columns
            if ttl_columns:
                for col in ttl_columns:
                    if col in columns:
                        select_parts.append(f"TTL({col}) AS {col}_ttl")

            column_list = ", ".join(select_parts)
            query = f"SELECT {column_list} FROM {table}"
            values = []

            # Build WHERE clause
            where_clauses = []

            if use_token_ranges:
                # Use token-based partitioning
                start_token = partition_def["start_token"]
                end_token = partition_def["end_token"]
                pk_columns = partition_def.get("primary_key_columns", ["id"])
                token_expr = f"TOKEN({', '.join(pk_columns)})"
                where_clauses.append(f"{token_expr} >= ? AND {token_expr} <= ?")
                values.extend([start_token, end_token])

            # Add pushdown predicates
            # CRITICAL: When using token ranges, skip partition key predicates
            # as they conflict with TOKEN() function
            pk_columns = partition_def.get("primary_key_columns", ["id"])
            for pred in pushdown_predicates:
                col = pred["column"]
                op = pred["operator"]
                val = pred["value"]

                # Skip partition key predicates when using token ranges
                if use_token_ranges and col in pk_columns:
                    continue

                if op == "IN":
                    placeholders = ", ".join(["?" for _ in val])
                    where_clauses.append(f"{col} IN ({placeholders})")
                    values.extend(val)
                else:
                    where_clauses.append(f"{col} {op} ?")
                    values.append(val)

            if where_clauses:
                query += " WHERE " + " AND ".join(where_clauses)

            # Add ALLOW FILTERING if needed
            if allow_filtering and pushdown_predicates:
                query += " ALLOW FILTERING"

        # Determine page size
        if page_size:
            # Use explicit page size
            fetch_size = page_size
        elif adaptive_page_size:
            # Calculate adaptive page size based on memory limit and expected row size
            avg_row_size = partition_def.get("avg_row_size", 1024)  # Default 1KB
            memory_limit_bytes = memory_limit_mb * 1024 * 1024
            # Leave 20% headroom
            target_memory = memory_limit_bytes * 0.8
            fetch_size = max(100, min(5000, int(target_memory / avg_row_size)))
        else:
            # Use default batch size
            fetch_size = self.batch_size

        # ALWAYS use async-cassandra streaming - it's a required dependency

        if use_token_ranges:
            # For token-based queries, we need to handle pagination properly
            # start_token and end_token are defined above in the query building section
            start_token = partition_def.get("start_token")
            end_token = partition_def.get("end_token")
            if start_token is None or end_token is None:
                raise ValueError(
                    "Token range queries require start_token and end_token in partition definition"
                )

            # Use the simpler streaming approach
            from .streaming import CassandraStreamer

            streamer = CassandraStreamer(self.session)

            # Get partition key columns
            pk_columns = partition_def.get("primary_key_columns", ["id"])

            # Extract WHERE clause if any (excluding token conditions)
            where_clause = ""
            where_values = ()
            if pushdown_predicates:
                # Build WHERE clause from non-partition key predicates
                where_parts = []
                pred_values = []
                for pred in pushdown_predicates:
                    col = pred["column"]
                    if col not in pk_columns:  # Only non-partition key predicates
                        if pred["operator"] == "IN":
                            placeholders = ", ".join(["?" for _ in pred["value"]])
                            where_parts.append(f"{col} IN ({placeholders})")
                            pred_values.extend(pred["value"])
                        else:
                            where_parts.append(f"{col} {pred['operator']} ?")
                            pred_values.append(pred["value"])
                if where_parts:
                    where_clause = " AND ".join(where_parts)
                    where_values = tuple(pred_values)

            return await streamer.stream_token_range(
                table=partition_def["table"],
                columns=columns,
                partition_keys=pk_columns,
                start_token=start_token,
                end_token=end_token,
                fetch_size=fetch_size,
                memory_limit_mb=memory_limit_mb,
                where_clause=where_clause,
                where_values=where_values,
                consistency_level=partition_def.get("consistency_level"),
                table_metadata=partition_def.get("_table_metadata"),
                type_mapper=partition_def.get("type_mapper"),
            )
        else:
            # Non-token range query - use regular streaming
            from .streaming import CassandraStreamer

            streamer = CassandraStreamer(self.session)

            # For non-token queries, we need to build the query with predicates
            # The query should have been built by query_builder above
            if not query or not isinstance(values, tuple | list):
                # Fallback query building if query_builder wasn't used
                raise ValueError("Query builder must be provided for non-token range queries")

            return await streamer.stream_query(
                query=query,
                values=values,
                columns=columns,
                fetch_size=fetch_size,
                memory_limit_mb=memory_limit_mb,
                consistency_level=partition_def.get("consistency_level"),
                table_metadata=partition_def.get("_table_metadata"),
                type_mapper=partition_def.get("type_mapper"),
            )

    async def _stream_token_range_partition(
        self,
        query: str,
        values: tuple,
        columns: list[str],
        start_token: int,
        end_token: int,
        fetch_size: int,
        memory_limit_mb: int,
        partition_def: dict[str, Any],
    ) -> pd.DataFrame:
        """
        Stream data from a token range with proper pagination.

        This method properly handles token-based pagination to ensure
        we fetch ALL data in the token range, not just the first page.
        """
        from async_cassandra.streaming import StreamConfig

        rows = []
        memory_used = 0
        memory_limit_bytes = memory_limit_mb * 1024 * 1024

        # Get partition keys from metadata
        partition_keys = partition_def.get("primary_key_columns", ["id"])
        if not partition_keys:
            raise ValueError("Cannot paginate without partition keys")

        # Build token function for the partition keys
        if len(partition_keys) == 1:
            token_func = f"TOKEN({partition_keys[0]})"
        else:
            token_func = f"TOKEN({', '.join(partition_keys)})"

        # First query to get initial data
        prepared = await self.session.prepare(query)
        stream_config = StreamConfig(fetch_size=fetch_size)

        # Set consistency level on the prepared statement
        consistency_level = partition_def.get("consistency_level")
        if consistency_level:
            prepared.consistency_level = consistency_level

        # Stream the initial batch
        stream_result = await self.session.execute_stream(
            prepared, values, stream_config=stream_config
        )

        last_token = None
        async with stream_result as stream:
            async for row in stream:
                rows.append(row)

                # Track the last token we've seen
                if hasattr(row, "_asdict"):
                    row_dict = row._asdict()
                    # Calculate token for this row
                    pk_values = [row_dict[pk] for pk in partition_keys]
                    # We need to track this for pagination
                    last_token = pk_values

                # Check memory usage periodically
                if len(rows) % 1000 == 0:
                    memory_used = len(rows) * len(columns) * 50
                    if memory_used > memory_limit_bytes:
                        break

        # Continue paginating if we haven't reached the end of the token range
        while last_token is not None and memory_used < memory_limit_bytes:
            # Build pagination query
            # We need to continue from where we left off
            # Reconstruct the query with updated token range
            # Instead of text replacement, rebuild the query properly
            base_query_parts = query.split(" WHERE ")
            if len(base_query_parts) != 2:
                break  # Can't parse query safely

            select_part = base_query_parts[0]
            where_part = base_query_parts[1]

            # Build new WHERE clause with updated token range
            new_where_parts = []
            for part in where_part.split(" AND "):
                if token_func in part and ">=" in part:
                    # Skip the old start token condition
                    continue
                elif token_func in part and "<=" in part:
                    # Keep the end token condition
                    new_where_parts.append(part)
                else:
                    # Keep other conditions
                    new_where_parts.append(part)

            # Add new start token condition
            new_where_parts.insert(0, f"{token_func} > ?")

            pagination_query = select_part + " WHERE " + " AND ".join(new_where_parts)

            # Calculate the token value for the last row
            # For now, we'll use the prepared statement approach
            token_query = f"SELECT {token_func} AS token_value FROM {partition_def['table']} WHERE "
            where_parts = []
            pk_values = []
            for i, pk in enumerate(partition_keys):
                where_parts.append(f"{pk} = ?")
                pk_values.append(last_token[i])
            token_query += " AND ".join(where_parts)

            token_result = await self.session.execute(
                await self.session.prepare(token_query), tuple(pk_values)
            )
            token_row = token_result.one()
            if not token_row:
                break

            last_token_value = token_row.token_value

            # Continue from this token
            new_values = list(values)
            # Find the token range parameters in values
            # They should be at the end for token range queries
            if len(new_values) >= 2:
                new_values[-2] = last_token_value  # Update start token

            # Execute next page
            next_result = await self.session.execute_stream(
                await self.session.prepare(pagination_query),
                tuple(new_values),
                stream_config=stream_config,
            )

            batch_rows = []
            async with next_result as stream:
                async for row in stream:
                    batch_rows.append(row)

                    if hasattr(row, "_asdict"):
                        row_dict = row._asdict()
                        last_token = [row_dict[pk] for pk in partition_keys]

                    # Check memory
                    if len(batch_rows) % 1000 == 0:
                        memory_used = (len(rows) + len(batch_rows)) * len(columns) * 50
                        if memory_used > memory_limit_bytes:
                            break

            if not batch_rows:
                break  # No more data

            rows.extend(batch_rows)
            memory_used = len(rows) * len(columns) * 50

        # Convert to DataFrame
        if rows:
            # Convert rows to DataFrame preserving types
            # Special handling for UDTs which come as namedtuples
            def convert_value(value):
                """Recursively convert UDTs to dicts."""
                if hasattr(value, "_fields") and hasattr(value, "_asdict"):
                    # It's a UDT - convert to dict
                    result = {}
                    for field in value._fields:
                        field_value = getattr(value, field)
                        # Recursively convert nested UDTs
                        result[field] = convert_value(field_value)
                    return result
                elif isinstance(value, list | tuple):
                    # Handle collections containing UDTs
                    return [convert_value(item) for item in value]
                elif isinstance(value, dict):
                    # Handle maps containing UDTs
                    return {k: convert_value(v) for k, v in value.items()}
                else:
                    return value

            df_data = []
            for row in rows:
                row_dict = {}
                # Get column names from the row
                if hasattr(row, "_fields"):
                    for field in row._fields:
                        value = getattr(row, field)
                        row_dict[field] = convert_value(value)
                else:
                    # Fallback to regular _asdict but still convert values
                    temp_dict = row._asdict()
                    for key, value in temp_dict.items():
                        row_dict[key] = convert_value(value)
                df_data.append(row_dict)

            df = pd.DataFrame(df_data)

            # Debug: Check UDT values in DataFrame
            # for col in df.columns:
            #     if df[col].dtype == 'object' and len(df) > 0:
            #         first_val = df.iloc[0][col]
            #         if isinstance(first_val, dict):
            #             print(f"DEBUG partition.py: Column {col} has dict value: type={type(first_val)}, value={first_val}")
            #         elif isinstance(first_val, str):
            #             print(f"DEBUG partition.py: Column {col} is STRING: {first_val}")

            # Ensure columns are in the expected order
            if columns and set(df.columns) == set(columns):
                df = df[columns]

            # Apply type conversions using type mapper if available
            if "type_mapper" in partition_def and "_table_metadata" in partition_def:
                type_mapper = partition_def["type_mapper"]
                table_metadata = partition_def["_table_metadata"]

                # Apply type conversions
                for col in df.columns:
                    if not (col.endswith("_writetime") or col.endswith("_ttl")):
                        col_info = next(
                            (c for c in table_metadata["columns"] if c["name"] == col), None
                        )
                        if col_info:
                            col_type = str(col_info["type"])
                            # print(f"DEBUG: Column {col} has type {col_type}, current value type: {type(df.iloc[0][col]) if len(df) > 0 else 'empty'}")
                            # Apply conversion for complex types
                            if (
                                col_type.startswith("frozen")
                                or "<" in col_type
                                or col_type in ["udt", "tuple"]
                            ):
                                # print(f"DEBUG: Applying type mapper to column {col}, type {col_type}")
                                df[col] = df[col].apply(
                                    lambda x, ct=col_type: (
                                        type_mapper.convert_value(x, ct) if type_mapper else x
                                    )
                                )

            return df
        else:
            # Empty partition - return empty DataFrame with correct schema
            # Need to include writetime/TTL columns if requested
            all_columns = list(columns)

            # Add writetime columns
            writetime_columns = partition_def.get("writetime_columns", [])
            if writetime_columns:
                for col in writetime_columns:
                    if f"{col}_writetime" not in all_columns:
                        all_columns.append(f"{col}_writetime")

            # Add TTL columns
            ttl_columns = partition_def.get("ttl_columns", [])
            if ttl_columns:
                for col in ttl_columns:
                    if f"{col}_ttl" not in all_columns:
                        all_columns.append(f"{col}_ttl")

            return pd.DataFrame(columns=all_columns)

    def _get_primary_key_columns(self, table: str) -> list[str]:
        """Get primary key columns for table."""
        # This is now handled by passing primary_key_columns in partition_def
        # Fallback to 'id' if not provided
        return ["id"]

    def _extract_token_value(self, row: Any, pk_columns: list[str]) -> int:
        """Extract token value from row."""
        # Calculate token using Cassandra's token function
        # For now, return MAX_TOKEN to end iteration
        # In production, we'd extract values and compute actual token
        return self.MAX_TOKEN


class AdaptivePartitionIterator:
    """
    Iterator that creates partitions on demand based on memory usage.

    This allows truly adaptive partitioning without knowing sizes upfront.
    """

    def __init__(
        self,
        session,
        table: str,
        columns: list[str],
        memory_limit_mb: int = 128,
    ):
        """Initialize adaptive iterator."""
        self.session = session
        self.table = table
        self.columns = columns
        self.memory_limit_mb = memory_limit_mb
        self.current_token = StreamingPartitionStrategy.MIN_TOKEN
        self.exhausted = False

    async def __aiter__(self) -> AsyncIterator[pd.DataFrame]:
        """Async iteration over partitions."""
        while not self.exhausted:
            df, next_token = await self._read_next_partition()

            if df is not None and not df.empty:
                yield df

            if next_token >= StreamingPartitionStrategy.MAX_TOKEN:
                self.exhausted = True
            else:
                self.current_token = next_token

    async def _read_next_partition(self) -> tuple[pd.DataFrame | None, int]:
        """Read next partition up to memory limit."""
        # Implementation similar to stream_partition
        # Returns (DataFrame, next_token)
        pass
