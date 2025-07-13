"""
Enhanced DataFrame reader with writetime filtering and concurrency control.

Provides production-ready features including:
- Writetime-based filtering (older/younger than)
- Snapshot consistency with "now" parameter
- Concurrency control to protect Cassandra cluster
"""

import asyncio
import threading
from datetime import UTC, datetime
from typing import Any

import dask
import dask.dataframe as dd
import pandas as pd
from dask.distributed import Client

from .config import config
from .metadata import TableMetadataExtractor
from .parallel import ParallelPartitionReader
from .partition import StreamingPartitionStrategy
from .predicate_pushdown import PredicatePushdownAnalyzer
from .query_builder import QueryBuilder
from .serializers import TTLSerializer, WritetimeSerializer
from .thread_pool import ManagedThreadPool
from .type_converter import DataFrameTypeConverter
from .types import CassandraTypeMapper

# Configure Dask to not use PyArrow strings by default
# This preserves object dtypes for things like VARINT
dask.config.set({"dataframe.convert-string": False})


class CassandraDataFrameReader:
    """
    Enhanced reader with writetime filtering and concurrency control.

    Key features:
    - Writetime-based filtering for temporal queries
    - Snapshot consistency with configurable "now" time
    - Concurrency limiting to protect Cassandra
    - Memory-bounded streaming approach
    """

    def __init__(
        self,
        session,
        table: str,
        keyspace: str | None = None,
        max_concurrent_queries: int | None = None,
        consistency_level: str | None = None,
    ):
        """
        Initialize enhanced DataFrame reader.

        Args:
            session: AsyncSession from async-cassandra
            table: Table name
            keyspace: Keyspace name (optional if fully qualified table)
            max_concurrent_queries: Max concurrent queries to Cassandra (default: no limit)
            consistency_level: Cassandra consistency level (default: LOCAL_ONE)
        """
        self.session = session
        self.max_concurrent_queries = max_concurrent_queries

        # Set consistency level
        from cassandra import ConsistencyLevel

        if consistency_level is None:
            self.consistency_level = ConsistencyLevel.LOCAL_ONE
        else:
            # Parse string consistency level
            try:
                self.consistency_level = getattr(ConsistencyLevel, consistency_level.upper())
            except AttributeError as e:
                raise ValueError(f"Invalid consistency level: {consistency_level}") from e

        # Parse table name
        if "." in table:
            self.keyspace, self.table = table.split(".", 1)
        else:
            self.keyspace = keyspace or session._session.keyspace
            self.table = table

        if not self.keyspace:
            raise ValueError("Keyspace must be specified either in table name or separately")

        # Initialize components
        self.metadata_extractor = TableMetadataExtractor(session)
        self.type_mapper = CassandraTypeMapper()
        self.writetime_serializer = WritetimeSerializer()
        self.ttl_serializer = TTLSerializer()

        # Cached metadata
        self._table_metadata = None
        self._query_builder = None

        # Concurrency control
        self._semaphore = None
        if max_concurrent_queries:
            self._semaphore = asyncio.Semaphore(max_concurrent_queries)

    async def _ensure_metadata(self):
        """Ensure table metadata is loaded."""
        if self._table_metadata is None:
            self._table_metadata = await self.metadata_extractor.get_table_metadata(
                self.keyspace, self.table
            )
            self._query_builder = QueryBuilder(self._table_metadata)

    async def read(
        self,
        columns: list[str] | None = None,
        writetime_columns: list[str] | None = None,
        ttl_columns: list[str] | None = None,
        # Writetime filtering
        writetime_filter: dict[str, Any] | None = None,
        snapshot_time: datetime | str | None = None,
        # Predicate pushdown
        predicates: list[dict[str, Any]] | None = None,
        allow_filtering: bool = False,
        # Partitioning
        partition_count: int | None = None,
        memory_per_partition_mb: int = 128,
        # Concurrency
        max_concurrent_partitions: int | None = None,
        # Streaming
        page_size: int | None = None,
        adaptive_page_size: bool = False,
        # Parallel execution
        use_parallel_execution: bool = True,
        progress_callback: Any | None = None,
        # Dask
        client: Client | None = None,
    ) -> dd.DataFrame:
        """
        Read Cassandra table as Dask DataFrame with enhanced filtering.

        Args:
            columns: Columns to read (None = all)
            writetime_columns: Columns to get writetime for
            ttl_columns: Columns to get TTL for

            writetime_filter: Filter data by writetime. Examples:
                {"column": "data", "operator": ">", "timestamp": datetime(2024,1,1)}
                {"column": "data", "operator": "<=", "timestamp": "2024-01-01T00:00:00Z"}
                {"column": "*", "operator": ">", "timestamp": datetime.now()} # All columns

            snapshot_time: Fixed "now" time for consistency. Can be:
                - datetime object
                - ISO string "2024-01-01T00:00:00Z"
                - "now" to use current time

            predicates: List of column predicates for filtering. Each predicate is a dict with:
                - column: Column name
                - operator: One of =, <, >, <=, >=, IN, !=
                - value: Value to compare
                Example: [{"column": "user_id", "operator": "=", "value": 123}]

            allow_filtering: Allow ALLOW FILTERING clause (use with caution)

            partition_count: Fixed partition count (overrides adaptive)
            memory_per_partition_mb: Target memory per partition
            max_concurrent_partitions: Max partitions to read concurrently

            page_size: Number of rows to fetch per page from Cassandra (default: driver default)
            adaptive_page_size: Automatically adjust page size based on row size

            use_parallel_execution: Execute partition queries in parallel (default: True)
            progress_callback: Async callback for progress updates: async def callback(completed, total, message)

            client: Dask distributed client (optional)

        Returns:
            Dask DataFrame

        Examples:
            # Get data written after specific time
            df = await reader.read(
                writetime_filter={
                    "column": "status",
                    "operator": ">",
                    "timestamp": datetime(2024, 1, 1)
                }
            )

            # Snapshot consistency - all queries use same "now"
            df = await reader.read(
                snapshot_time="now",
                writetime_filter={
                    "column": "*",
                    "operator": "<",
                    "timestamp": "now"
                }
            )
        """
        # Ensure metadata loaded
        await self._ensure_metadata()

        # Validate page_size if provided
        if page_size is not None:
            if not isinstance(page_size, int):
                raise TypeError("page_size must be an integer")
            if page_size <= 0:
                raise ValueError("page_size must be greater than 0")
            if page_size >= 1000000:
                raise ValueError("page_size is too large (max 999999)")
            # Warn about very small page sizes
            if page_size < 100:
                import warnings

                warnings.warn(
                    f"page_size={page_size} is very small and may impact performance. "
                    "Consider using a larger value (100-5000) unless you have specific memory constraints.",
                    UserWarning,
                    stacklevel=2,
                )

        # Validate predicates first
        if predicates:
            # Check all columns exist
            valid_columns = {col["name"] for col in self._table_metadata["columns"]}
            for pred in predicates:
                if pred["column"] not in valid_columns:
                    raise ValueError(
                        f"Column '{pred['column']}' not found in table {self.keyspace}.{self.table}"
                    )

        # Analyze predicates for pushdown
        pushdown_predicates = []
        client_predicates = []
        use_token_ranges = True

        if predicates:
            analyzer = PredicatePushdownAnalyzer(self._table_metadata)
            pushdown_predicates, client_predicates, use_token_ranges = analyzer.analyze_predicates(
                predicates, use_token_ranges=True
            )

        # Handle snapshot time
        if snapshot_time:
            if snapshot_time == "now":
                snapshot_time = datetime.now(UTC)
            elif isinstance(snapshot_time, str):
                snapshot_time = pd.Timestamp(snapshot_time).to_pydatetime()

        # Process writetime filter
        if writetime_filter:
            # Validate and normalize filter
            writetime_filter = self._normalize_writetime_filter(writetime_filter, snapshot_time)

            # Expand wildcard if needed
            if writetime_filter["column"] == "*":
                # Get all writetime-capable columns
                capable_columns = self.metadata_extractor.get_writetime_capable_columns(
                    self._table_metadata
                )
                writetime_filter["columns"] = capable_columns
            else:
                writetime_filter["columns"] = [writetime_filter["column"]]

            # Ensure we're querying writetime for filtered columns
            if writetime_columns is None:
                writetime_columns = []
            writetime_columns = list(set(writetime_columns + writetime_filter["columns"]))

        # Prepare columns
        if columns is None:
            columns = [col["name"] for col in self._table_metadata["columns"]]
        else:
            # Validate columns exist
            self._query_builder.validate_columns(columns)

        # Expand writetime/TTL wildcards
        if writetime_columns:
            writetime_columns = self.metadata_extractor.expand_column_wildcards(
                writetime_columns, self._table_metadata, writetime_capable_only=True
            )

        if ttl_columns:
            ttl_columns = self.metadata_extractor.expand_column_wildcards(
                ttl_columns, self._table_metadata, ttl_capable_only=True
            )

        # Create partition strategy with concurrency control
        partition_strategy = StreamingPartitionStrategy(
            session=self.session,
            memory_per_partition_mb=memory_per_partition_mb,
        )

        # Create partitions
        partitions = await partition_strategy.create_partitions(
            table=f"{self.keyspace}.{self.table}",
            columns=columns,
            partition_count=partition_count,
            use_token_ranges=use_token_ranges,
            pushdown_predicates=pushdown_predicates,
        )

        # Prepare partition definitions with all required info
        for partition_def in partitions:
            # Add query-specific info to partition definition
            partition_def["writetime_columns"] = writetime_columns
            partition_def["ttl_columns"] = ttl_columns
            partition_def["query_builder"] = self._query_builder
            partition_def["type_mapper"] = self.type_mapper
            # For token queries, only use partition key columns
            partition_def["primary_key_columns"] = self._table_metadata["partition_key"]
            partition_def["_table_metadata"] = self._table_metadata
            partition_def["writetime_filter"] = writetime_filter
            partition_def["snapshot_time"] = snapshot_time
            partition_def["_semaphore"] = self._semaphore
            # Convert Predicate objects to dicts for partition reading
            partition_def["pushdown_predicates"] = [
                {"column": p.column, "operator": p.operator, "value": p.value}
                for p in pushdown_predicates
            ]
            partition_def["client_predicates"] = [
                {"column": p.column, "operator": p.operator, "value": p.value}
                for p in client_predicates
            ]
            partition_def["allow_filtering"] = allow_filtering
            partition_def["page_size"] = page_size
            partition_def["adaptive_page_size"] = adaptive_page_size
            partition_def["consistency_level"] = self.consistency_level

        # Get DataFrame schema
        meta = self._create_dataframe_meta(columns, writetime_columns, ttl_columns)

        if use_parallel_execution and len(partitions) > 1:
            # Use true parallel execution for multiple partitions
            parallel_reader = ParallelPartitionReader(
                session=self.session,
                max_concurrent=max_concurrent_partitions or 10,
                progress_callback=progress_callback,
            )

            # Execute partitions in parallel and get results
            dfs = await parallel_reader.read_partitions(partitions)

            # Combine results into single DataFrame
            if dfs:
                combined_df = pd.concat(dfs, ignore_index=True)

                # Apply comprehensive type conversions to ensure data integrity
                combined_df = DataFrameTypeConverter.convert_dataframe_types(
                    combined_df, self._table_metadata, self.type_mapper
                )

                # Handle any remaining UDT serialization issues
                for col in combined_df.columns:
                    if col.endswith("_writetime") or col.endswith("_ttl"):
                        continue  # Skip metadata columns

                    # Get column metadata
                    col_info = next(
                        (c for c in self._table_metadata["columns"] if c["name"] == col), None
                    )
                    if col_info:
                        col_type = str(col_info["type"])

                        # Check for UDTs - they won't be in the simple types list
                        # Also check for frozen types which can contain UDTs
                        is_simple_type = col_type in [
                            "text",
                            "varchar",
                            "ascii",
                            "blob",
                            "boolean",
                            "tinyint",
                            "smallint",
                            "int",
                            "bigint",
                            "varint",
                            "decimal",
                            "float",
                            "double",
                            "counter",
                            "timestamp",
                            "date",
                            "time",
                            "timeuuid",
                            "uuid",
                            "inet",
                            "duration",
                        ]

                        # Check if it's a simple collection (not containing UDTs)
                        is_simple_collection = False
                        if (
                            col_type.startswith("list<")
                            or col_type.startswith("set<")
                            or col_type.startswith("map<")
                        ):
                            # Extract inner type
                            if "frozen" not in col_type:
                                # Check if inner type is simple
                                inner_type = col_type[
                                    col_type.index("<") + 1 : col_type.rindex(">")
                                ]
                                if "," in inner_type:  # Map type
                                    key_type, val_type = inner_type.split(",", 1)
                                    is_simple_collection = key_type.strip() in [
                                        "text",
                                        "int",
                                        "bigint",
                                        "uuid",
                                    ] and val_type.strip() in ["text", "int", "bigint", "uuid"]
                                else:
                                    is_simple_collection = inner_type in [
                                        "text",
                                        "int",
                                        "bigint",
                                        "uuid",
                                        "double",
                                        "float",
                                    ]

                        # Check if it's a frozen type or UDT
                        # UDTs can be represented as just the type name (e.g., "address") without frozen<>
                        is_frozen_or_udt = col_type.startswith("frozen<") or (
                            not is_simple_type
                            and not is_simple_collection
                            and not col_type.startswith("tuple<")
                        )

                        # Also check for collections of UDTs
                        is_collection_of_udts = False
                        if (
                            col_type.startswith("list<frozen<")
                            or col_type.startswith("set<frozen<")
                            or col_type.startswith("map<")
                            and "frozen<" in col_type
                        ):
                            is_collection_of_udts = True

                        if is_frozen_or_udt or is_collection_of_udts:
                            # This is likely a UDT or frozen type that got stringified during concat
                            # Try to convert string representations back to dicts
                            def fix_udt_string(value, col_type=col_type):
                                # Recursive function to handle nested UDTs
                                def convert_udt_value(val):
                                    if hasattr(val, "_fields") and hasattr(val, "_asdict"):
                                        # It's a UDT - convert to dict recursively
                                        result = {}
                                        for field in val._fields:
                                            field_value = getattr(val, field)
                                            result[field] = convert_udt_value(field_value)
                                        return result
                                    elif isinstance(val, list | tuple):
                                        # Handle collections
                                        return [convert_udt_value(item) for item in val]
                                    elif isinstance(val, dict):
                                        # Handle maps
                                        return {k: convert_udt_value(v) for k, v in val.items()}
                                    elif isinstance(val, set):
                                        # Handle sets
                                        return {convert_udt_value(item) for item in val}
                                    else:
                                        # Return value as-is (including UUIDs, dates, etc)
                                        return val

                                # Handle None/NaN values
                                if value is None:
                                    return value
                                # Special handling for collections
                                if isinstance(value, list | set | tuple):
                                    # Convert each UDT in the collection
                                    result = []
                                    for item in value:
                                        if hasattr(item, "_fields") and hasattr(item, "_asdict"):
                                            result.append(convert_udt_value(item))
                                        else:
                                            result.append(item)
                                    # Return as list (even for sets)
                                    return result
                                elif isinstance(value, dict) or (
                                    hasattr(value, "__class__")
                                    and "OrderedMap" in value.__class__.__name__
                                ):
                                    # Handle maps
                                    result = {}
                                    items = value.items() if hasattr(value, "items") else value
                                    for k, v in items:
                                        if hasattr(v, "_fields") and hasattr(v, "_asdict"):
                                            result[k] = convert_udt_value(v)
                                        else:
                                            result[k] = v
                                    return result
                                elif (
                                    hasattr(value, "__class__")
                                    and "SortedSet" in value.__class__.__name__
                                ):
                                    # Handle Cassandra SortedSet
                                    result = []
                                    for item in value:
                                        if hasattr(item, "_fields") and hasattr(item, "_asdict"):
                                            result.append(convert_udt_value(item))
                                        else:
                                            result.append(item)
                                    return result
                                elif pd.isna(value):
                                    return value

                                # Check if it's a Cassandra UDT object
                                if hasattr(value, "_fields") and hasattr(value, "_asdict"):
                                    # Convert UDT to dict recursively
                                    result = convert_udt_value(value)
                                    return result
                                elif isinstance(value, str):
                                    # Parse UDT string representation (for pandas concat issues)

                                    # Check if it's a collection string representation
                                    if value.startswith("[") and value.endswith("]"):
                                        # It's a list representation
                                        try:
                                            # Parse list of UDTs
                                            import re

                                            # Extract UDT type from strings like "phone(type='mobile', ...)"
                                            udt_pattern = r"(\w+)\((.*?)\)"
                                            matches = re.findall(udt_pattern, value)
                                            result = []
                                            for _udt_type, fields_str in matches:
                                                # Parse fields
                                                field_dict = {}
                                                field_pattern = r"(\w+)='([^']*)'|(\w+)=([^,\)]+)"
                                                field_matches = re.findall(
                                                    field_pattern, fields_str
                                                )
                                                for match in field_matches:
                                                    if match[0]:  # String value
                                                        field_dict[match[0]] = match[1]
                                                    else:  # Non-string value
                                                        key = match[2]
                                                        val = match[3].strip()
                                                        # Try to convert to appropriate type
                                                        try:
                                                            if val == "None":
                                                                field_dict[key] = None
                                                            else:
                                                                field_dict[key] = int(val)
                                                        except ValueError:
                                                            try:
                                                                field_dict[key] = float(val)
                                                            except ValueError:
                                                                field_dict[key] = val
                                                result.append(field_dict)
                                            return result
                                        except Exception:
                                            # Fallback to string
                                            pass

                                    # Extract the actual type name from frozen<typename> if needed
                                    type_name = col_type
                                    if col_type.startswith("frozen<") and col_type.endswith(">"):
                                        type_name = col_type[7:-1]  # Remove "frozen<" and ">"

                                    # Check if string looks like a UDT representation or dict
                                    # For dict strings, always try to parse
                                    if value.startswith("{") or value.startswith(type_name + "("):
                                        # If it's already a dict string representation, try to parse it
                                        if value.startswith("{") and value.endswith("}"):
                                            try:
                                                import ast

                                                result = ast.literal_eval(value)
                                                return result
                                            except Exception:
                                                pass

                                        # Otherwise try to parse UDT representation
                                        try:
                                            # Try to parse as Python literal
                                            import ast
                                            import re

                                            # First handle UUID representations
                                            cleaned = re.sub(r"UUID\('([^']+)'\)", r"'\1'", value)
                                            # Handle frozen<...> syntax
                                            cleaned = re.sub(r"frozen<[^>]+>\(", "(", cleaned)
                                            # Try to evaluate
                                            result = ast.literal_eval(cleaned)
                                            # Convert UUID strings back to UUID objects
                                            if isinstance(result, dict):
                                                for k, v in result.items():
                                                    if isinstance(v, str) and k.endswith("_id"):
                                                        try:
                                                            from uuid import UUID

                                                            result[k] = UUID(v)
                                                        except (ValueError, TypeError):
                                                            pass
                                            return result
                                        except Exception:
                                            # Fallback to original parsing for simple UDTs
                                            try:
                                                # Extract the content between parentheses
                                                start_idx = value.find("(")
                                                if start_idx >= 0:
                                                    content = value[start_idx + 1 : -1]
                                                    # Parse key=value pairs
                                                    result = {}
                                                    for pair in content.split(", "):
                                                        if "=" in pair:
                                                            key, val = pair.split("=", 1)
                                                            # Remove quotes from string values
                                                            if val.startswith("'") and val.endswith(
                                                                "'"
                                                            ):
                                                                val = val[1:-1]
                                                            elif val == "None":
                                                                val = None
                                                            else:
                                                                # Try to convert to int/float if possible
                                                                try:
                                                                    val = int(val)
                                                                except ValueError:
                                                                    try:
                                                                        val = float(val)
                                                                    except ValueError:
                                                                        pass
                                                            result[key] = val
                                                    return result
                                            except Exception:
                                                pass
                                return value

                            combined_df[col] = combined_df[col].apply(fix_udt_string)
            else:
                combined_df = meta.copy()

            # Create Dask DataFrame from the already-computed result
            # This is a single partition Dask DataFrame
            df = dd.from_pandas(combined_df, npartitions=1)
        else:
            # Use original Dask delayed execution for single partition or when parallel disabled
            delayed_partitions = []

            for partition_def in partitions:
                # Create delayed task - wrap async function for Dask
                delayed = dask.delayed(self._read_partition_sync)(
                    partition_def,
                    self.session,
                )
                delayed_partitions.append(delayed)

            # Create Dask DataFrame
            df = dd.from_delayed(delayed_partitions, meta=meta)

        # Apply writetime filtering in Dask if needed
        if writetime_filter:
            df = self._apply_writetime_filter(df, writetime_filter)

        # Apply client-side predicates
        if client_predicates:
            df = self._apply_client_predicates(df, client_predicates)

        return df

    def _normalize_writetime_filter(
        self, filter_spec: dict[str, Any], snapshot_time: datetime | None
    ) -> dict[str, Any]:
        """Normalize and validate writetime filter specification."""
        # Required fields
        if "column" not in filter_spec:
            raise ValueError("writetime_filter must have 'column' field")
        if "operator" not in filter_spec:
            raise ValueError("writetime_filter must have 'operator' field")
        if "timestamp" not in filter_spec:
            raise ValueError("writetime_filter must have 'timestamp' field")

        # Validate operator
        valid_operators = [">", ">=", "<", "<=", "==", "!="]
        if filter_spec["operator"] not in valid_operators:
            raise ValueError(f"Invalid operator. Must be one of: {valid_operators}")

        # Process timestamp
        timestamp = filter_spec["timestamp"]
        if timestamp == "now":
            if snapshot_time:
                timestamp = snapshot_time
            else:
                timestamp = datetime.now(UTC)
        elif isinstance(timestamp, str):
            timestamp = pd.Timestamp(timestamp).to_pydatetime()

        # Ensure timezone aware
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=UTC)

        return {
            "column": filter_spec["column"],
            "operator": filter_spec["operator"],
            "timestamp": timestamp,
            "timestamp_micros": int(timestamp.timestamp() * 1_000_000),
        }

    def _apply_writetime_filter(
        self, df: dd.DataFrame, writetime_filter: dict[str, Any]
    ) -> dd.DataFrame:
        """Apply writetime filtering to DataFrame."""
        operator = writetime_filter["operator"]
        timestamp = writetime_filter["timestamp"]

        # Build filter expression for each column
        filter_mask = None
        for col in writetime_filter["columns"]:
            col_writetime = f"{col}_writetime"
            if col_writetime not in df.columns:
                continue

            # Create column filter
            if operator == ">":
                col_mask = df[col_writetime] > timestamp
            elif operator == ">=":
                col_mask = df[col_writetime] >= timestamp
            elif operator == "<":
                col_mask = df[col_writetime] < timestamp
            elif operator == "<=":
                col_mask = df[col_writetime] <= timestamp
            elif operator == "==":
                col_mask = df[col_writetime] == timestamp
            elif operator == "!=":
                col_mask = df[col_writetime] != timestamp

            # Combine with OR logic (any column matching is included)
            if filter_mask is None:
                filter_mask = col_mask
            else:
                filter_mask = filter_mask | col_mask

        # Apply filter
        if filter_mask is not None:
            df = df[filter_mask]

        return df

    def _apply_client_predicates(self, df: dd.DataFrame, predicates: list[Any]) -> dd.DataFrame:
        """Apply client-side predicates to DataFrame."""
        from decimal import Decimal

        for pred in predicates:
            col = pred.column
            op = pred.operator
            val = pred.value

            # For numeric comparisons with Decimal columns, ensure compatible types
            # We check the dtype of the column in the metadata
            col_info = next((c for c in self._table_metadata["columns"] if c["name"] == col), None)
            if col_info and str(col_info["type"]) == "decimal" and isinstance(val, int | float):
                # Convert numeric value to Decimal for comparison
                val = Decimal(str(val))

            if op == "=":
                df = df[df[col] == val]
            elif op == "!=":
                df = df[df[col] != val]
            elif op == ">":
                df = df[df[col] > val]
            elif op == ">=":
                df = df[df[col] >= val]
            elif op == "<":
                df = df[df[col] < val]
            elif op == "<=":
                df = df[df[col] <= val]
            elif op == "IN":
                df = df[df[col].isin(val)]
            else:
                raise ValueError(f"Unsupported operator for client-side filtering: {op}")

        return df

    def _create_dataframe_meta(
        self,
        columns: list[str],
        writetime_columns: list[str] | None,
        ttl_columns: list[str] | None,
    ) -> pd.DataFrame:
        """Create DataFrame metadata for Dask with proper examples for object columns."""
        # Create data with example values for object columns
        data = {}

        for col in columns:
            col_info = next((c for c in self._table_metadata["columns"] if c["name"] == col), None)
            if col_info:
                col_type = str(col_info["type"])
                dtype = self.type_mapper.get_pandas_dtype(col_type)

                if dtype == "object":
                    # Provide example values for object columns to prevent Dask serialization issues
                    if col_type == "list" or col_type.startswith("list<"):
                        data[col] = pd.Series([[]], dtype="object")
                    elif col_type == "set" or col_type.startswith("set<"):
                        data[col] = pd.Series([set()], dtype="object")
                    elif col_type == "map" or col_type.startswith("map<"):
                        data[col] = pd.Series([{}], dtype="object")
                    elif col_type.startswith("frozen<"):
                        # Frozen collections or UDTs
                        if "list" in col_type:
                            data[col] = pd.Series([[]], dtype="object")
                        elif "set" in col_type:
                            data[col] = pd.Series([set()], dtype="object")
                        elif "map" in col_type:
                            data[col] = pd.Series([{}], dtype="object")
                        else:
                            # Frozen UDT
                            data[col] = pd.Series([{}], dtype="object")
                    elif "<" not in col_type and col_type not in [
                        "text",
                        "varchar",
                        "ascii",
                        "blob",
                        "uuid",
                        "timeuuid",
                        "inet",
                    ]:
                        # Likely a UDT (non-parameterized custom type)
                        data[col] = pd.Series([{}], dtype="object")
                    else:
                        # Other object types
                        data[col] = pd.Series([], dtype="object")
                else:
                    # Non-object types
                    data[col] = pd.Series(dtype=dtype)

        # Add writetime columns
        if writetime_columns:
            for col in writetime_columns:
                data[f"{col}_writetime"] = pd.Series(dtype="datetime64[ns, UTC]")

        # Add TTL columns
        if ttl_columns:
            for col in ttl_columns:
                data[f"{col}_ttl"] = pd.Series(dtype="int64")

        # Create DataFrame and ensure it's empty but with correct types
        df = pd.DataFrame(data)
        return df.iloc[0:0]  # Empty but with preserved types

    # Shared resources for async execution
    _loop_runner = None
    _loop_runner_lock = threading.Lock()
    _loop_runner_config_hash = None  # Track config changes

    @classmethod
    def _get_loop_runner(cls):
        """Get or create the shared event loop runner."""
        # Check if config has changed
        current_config_hash = (
            config.get_thread_pool_size(),
            config.get_thread_name_prefix(),
            config.THREAD_IDLE_TIMEOUT_SECONDS,
            config.THREAD_CLEANUP_INTERVAL_SECONDS,
        )

        if cls._loop_runner is None or cls._loop_runner_config_hash != current_config_hash:
            with cls._loop_runner_lock:
                # Double-check inside lock
                if cls._loop_runner is None or cls._loop_runner_config_hash != current_config_hash:
                    # Shutdown old runner if config changed
                    if (
                        cls._loop_runner is not None
                        and cls._loop_runner_config_hash != current_config_hash
                    ):
                        cls._loop_runner.shutdown()
                        cls._loop_runner = None
                    import asyncio

                    class LoopRunner:
                        def __init__(self):
                            self.loop = asyncio.new_event_loop()
                            self.thread = None
                            self._ready = threading.Event()
                            # Create a managed thread pool with idle cleanup
                            self.executor = ManagedThreadPool(
                                max_workers=config.get_thread_pool_size(),
                                thread_name_prefix=config.get_thread_name_prefix(),
                                idle_timeout_seconds=config.THREAD_IDLE_TIMEOUT_SECONDS,
                                cleanup_interval_seconds=config.THREAD_CLEANUP_INTERVAL_SECONDS,
                            )
                            # Start the cleanup scheduler
                            self.executor.start_cleanup_scheduler()

                            # Create a wrapper that uses our managed submit
                            class ManagedExecutorWrapper:
                                def __init__(self, managed_pool):
                                    self.managed_pool = managed_pool

                                def submit(self, fn, *args, **kwargs):
                                    return self.managed_pool.submit(fn, *args, **kwargs)

                                def shutdown(self, wait=True):
                                    return self.managed_pool.shutdown(wait)

                            # Set our wrapper as the default executor
                            self.loop.set_default_executor(ManagedExecutorWrapper(self.executor))

                        def start(self):
                            def run():
                                asyncio.set_event_loop(self.loop)
                                self._ready.set()
                                self.loop.run_forever()

                            self.thread = threading.Thread(
                                target=run, name="cdf_event_loop", daemon=True
                            )
                            self.thread.start()
                            self._ready.wait()

                        def run_coroutine(self, coro):
                            """Run a coroutine and return the result."""
                            future = asyncio.run_coroutine_threadsafe(coro, self.loop)
                            return future.result()

                        def shutdown(self):
                            """Clean shutdown of the loop and executor."""
                            if self.loop and not self.loop.is_closed():
                                # Schedule cleanup
                                async def _shutdown():
                                    # Cancel all tasks
                                    tasks = [
                                        t for t in asyncio.all_tasks(self.loop) if not t.done()
                                    ]
                                    for task in tasks:
                                        task.cancel()
                                    # Don't wait for gather to avoid recursion
                                    # Shutdown async generators
                                    try:
                                        await self.loop.shutdown_asyncgens()
                                    except Exception:
                                        pass

                                future = asyncio.run_coroutine_threadsafe(_shutdown(), self.loop)
                                try:
                                    future.result(timeout=2.0)
                                except Exception:
                                    pass

                                # Stop the loop
                                self.loop.call_soon_threadsafe(self.loop.stop)

                                # Wait for thread
                                if self.thread and self.thread.is_alive():
                                    self.thread.join(timeout=2.0)

                                # Now shutdown the managed executor (which handles cleanup)
                                self.executor.shutdown(wait=True)

                                # Close the loop
                                try:
                                    self.loop.close()
                                except Exception:
                                    pass

                    cls._loop_runner = LoopRunner()
                    cls._loop_runner.start()
                    cls._loop_runner_config_hash = current_config_hash

        return cls._loop_runner

    @classmethod
    def cleanup_executor(cls):
        """Shutdown the shared event loop runner."""
        if cls._loop_runner is not None:
            with cls._loop_runner_lock:
                if cls._loop_runner is not None:
                    cls._loop_runner.shutdown()
                    cls._loop_runner = None
                    cls._loop_runner_config_hash = None

    @staticmethod
    def _read_partition_sync(
        partition_def: dict[str, Any],
        session,
    ) -> pd.DataFrame:
        """
        Synchronous wrapper for Dask delayed execution.

        Runs the async partition reader using a shared event loop.
        """
        # Get the shared loop runner
        runner = CassandraDataFrameReader._get_loop_runner()

        # Run the coroutine
        return runner.run_coroutine(
            CassandraDataFrameReader._read_partition(partition_def, session)
        )

    @staticmethod
    async def _read_partition(
        partition_def: dict[str, Any],
        session,
    ) -> pd.DataFrame:
        """
        Read a single partition with concurrency control.

        This is executed on Dask workers.
        """
        # Extract components from partition definition
        query_builder = partition_def["query_builder"]
        type_mapper = partition_def["type_mapper"]
        writetime_columns = partition_def.get("writetime_columns")
        ttl_columns = partition_def.get("ttl_columns")
        semaphore = partition_def.get("_semaphore")

        # Apply concurrency control if configured
        if semaphore:
            async with semaphore:
                return await CassandraDataFrameReader._read_partition_impl(
                    partition_def,
                    session,
                    query_builder,
                    type_mapper,
                    writetime_columns,
                    ttl_columns,
                )
        else:
            return await CassandraDataFrameReader._read_partition_impl(
                partition_def, session, query_builder, type_mapper, writetime_columns, ttl_columns
            )

    @staticmethod
    async def _read_partition_impl(
        partition_def: dict[str, Any],
        session,
        query_builder,
        type_mapper,
        writetime_columns,
        ttl_columns,
    ) -> pd.DataFrame:
        """Implementation of partition reading."""
        # Use streaming partition strategy to read data
        strategy = StreamingPartitionStrategy(
            session=session,
            memory_per_partition_mb=partition_def["memory_limit_mb"],
        )

        # Stream the partition
        df = await strategy.stream_partition(partition_def)

        # Apply type conversions based on table metadata
        if df.empty:
            # For empty DataFrames, ensure columns have correct dtypes
            schema = {}
            columns = partition_def["columns"]
            for col in columns:
                col_info = next(
                    (c for c in partition_def["_table_metadata"]["columns"] if c["name"] == col),
                    None,
                )
                if col_info:
                    col_type = str(col_info["type"])
                    pandas_dtype = type_mapper.get_pandas_dtype(col_type)
                    schema[col] = pandas_dtype

            # Create empty DataFrame with correct schema
            df = type_mapper.create_empty_dataframe(schema)
        else:
            # print(f"DEBUG reader: Before type conversion, df has {len(df)} rows")
            # for col in df.columns:
            #     if df[col].dtype == 'object' and len(df) > 0:
            #         print(f"DEBUG reader: Column {col} first value type: {type(df.iloc[0][col])}, value: {df.iloc[0][col]}")
            # Apply conversions to non-empty DataFrames
            for col in df.columns:
                if col.endswith("_writetime") and writetime_columns:
                    # Convert writetime values
                    df[col] = df[col].apply(WritetimeSerializer.to_timestamp)
                elif col.endswith("_ttl") and ttl_columns:
                    # TTL values are already in correct format
                    pass
                else:
                    # Apply type conversion based on column metadata
                    col_info = next(
                        (
                            c
                            for c in partition_def["_table_metadata"]["columns"]
                            if c["name"] == col
                        ),
                        None,
                    )
                    if col_info:
                        # Get the pandas dtype for this column
                        col_type = str(col_info["type"])
                        pandas_dtype = type_mapper.get_pandas_dtype(col_type)

                        # Convert the column to the expected dtype
                        if pandas_dtype == "bool":
                            df[col] = df[col].astype(bool)
                        elif pandas_dtype == "int32":
                            df[col] = df[col].astype("int32")
                        elif pandas_dtype == "int64":
                            df[col] = df[col].astype("int64")
                        elif pandas_dtype == "float32":
                            df[col] = df[col].astype("float32")
                        elif pandas_dtype == "float64":
                            df[col] = df[col].astype("float64")
                        elif pandas_dtype == "string[pyarrow]":
                            df[col] = df[col].astype("string")
                        # For complex types (UDTs, collections), always apply custom conversion
                        elif (
                            pandas_dtype == "object"
                            or col_type.startswith("frozen")
                            or "<" in col_type
                        ):
                            df[col] = df[col].apply(
                                lambda x, ct=col_type: type_mapper.convert_value(x, ct)
                            )
                        # Check for UDTs by checking if it's not a known simple type
                        elif col_type not in [
                            "text",
                            "varchar",
                            "ascii",
                            "blob",
                            "boolean",
                            "tinyint",
                            "smallint",
                            "int",
                            "bigint",
                            "varint",
                            "decimal",
                            "float",
                            "double",
                            "counter",
                            "timestamp",
                            "date",
                            "time",
                            "timeuuid",
                            "uuid",
                            "inet",
                            "duration",
                        ]:
                            # This is likely a UDT
                            df[col] = df[col].apply(
                                lambda x, ct=col_type: type_mapper.convert_value(x, ct)
                            )

            # Apply NULL semantics
            df = type_mapper.handle_null_values(df, partition_def["_table_metadata"])

        return df


async def read_cassandra_table(
    table: str,
    session=None,
    keyspace: str | None = None,
    columns: list[str] | None = None,
    # Writetime support
    writetime_columns: list[str] | None = None,
    writetime_filter: dict[str, Any] | None = None,
    snapshot_time: datetime | str | None = None,
    # TTL support
    ttl_columns: list[str] | None = None,
    # Predicate pushdown
    predicates: list[dict[str, Any]] | None = None,
    allow_filtering: bool = False,
    # Partitioning
    partition_count: int | None = None,
    memory_per_partition_mb: int = 128,
    # Concurrency control
    max_concurrent_queries: int | None = None,
    max_concurrent_partitions: int | None = None,
    # Consistency
    consistency_level: str | None = None,
    # Streaming
    page_size: int | None = None,
    adaptive_page_size: bool = False,
    # Parallel execution
    use_parallel_execution: bool = True,
    progress_callback: Any | None = None,
    # Dask
    client: Client | None = None,
) -> dd.DataFrame:
    """
    Read Cassandra table as Dask DataFrame with enhanced filtering and concurrency control.

    Args:
        table: Table name (can be keyspace.table)
        session: AsyncSession (required)
        keyspace: Keyspace if not in table name
        columns: Columns to read

        writetime_columns: Get writetime for these columns
        writetime_filter: Filter by writetime (see examples)
        snapshot_time: Fixed "now" time for consistency

        ttl_columns: Get TTL for these columns

        predicates: List of column predicates for filtering
        allow_filtering: Allow ALLOW FILTERING clause (use with caution)

        partition_count: Override adaptive partitioning
        memory_per_partition_mb: Memory limit per partition

        max_concurrent_queries: Max queries to Cassandra cluster
        max_concurrent_partitions: Max partitions to process at once

        consistency_level: Cassandra consistency level (default: LOCAL_ONE)
                          Options: ONE, TWO, THREE, QUORUM, ALL, LOCAL_QUORUM,
                          EACH_QUORUM, SERIAL, LOCAL_SERIAL, LOCAL_ONE, ANY

        page_size: Number of rows to fetch per page from Cassandra
        adaptive_page_size: Automatically adjust page size based on row size

        use_parallel_execution: Execute partition queries in parallel (default: True)
        progress_callback: Async callback for progress updates

        client: Dask distributed client

    Returns:
        Dask DataFrame

    Examples:
        # Get recent data
        df = await read_cassandra_table(
            "events",
            session=session,
            writetime_filter={
                "column": "data",
                "operator": ">",
                "timestamp": datetime.now() - timedelta(hours=1)
            }
        )

        # Snapshot at specific time
        df = await read_cassandra_table(
            "events",
            session=session,
            snapshot_time="2024-01-01T00:00:00Z",
            writetime_filter={
                "column": "*",
                "operator": "<",
                "timestamp": "2024-01-01T00:00:00Z"
            }
        )

        # Control concurrency
        df = await read_cassandra_table(
            "large_table",
            session=session,
            max_concurrent_queries=10,  # Limit Cassandra load
            max_concurrent_partitions=5  # Limit parallel processing
        )
    """
    if session is None:
        raise ValueError("session is required")

    reader = CassandraDataFrameReader(
        session=session,
        table=table,
        keyspace=keyspace,
        max_concurrent_queries=max_concurrent_queries,
        consistency_level=consistency_level,
    )

    return await reader.read(
        columns=columns,
        writetime_columns=writetime_columns,
        ttl_columns=ttl_columns,
        writetime_filter=writetime_filter,
        snapshot_time=snapshot_time,
        predicates=predicates,
        allow_filtering=allow_filtering,
        partition_count=partition_count,
        memory_per_partition_mb=memory_per_partition_mb,
        max_concurrent_partitions=max_concurrent_partitions,
        page_size=page_size,
        adaptive_page_size=adaptive_page_size,
        use_parallel_execution=use_parallel_execution,
        progress_callback=progress_callback,
        client=client,
    )


async def stream_cassandra_table(
    table: str,
    session=None,
    keyspace: str | None = None,
    columns: list[str] | None = None,
    batch_size: int = 1000,
    consistency_level: str | None = None,
    **kwargs,
):
    """
    Stream Cassandra table as async iterator of DataFrames.

    This is a memory-efficient way to process large tables by yielding
    DataFrames in batches rather than loading everything into memory.

    Args:
        table: Table name
        session: AsyncSession (required)
        keyspace: Keyspace name
        columns: Columns to read
        batch_size: Rows per batch (default: 1000)
        consistency_level: Cassandra consistency level (default: LOCAL_ONE)
        **kwargs: Additional arguments passed to read_cassandra_table

    Yields:
        pandas.DataFrame: Batches of data

    Example:
        async for batch_df in stream_cassandra_table("users", session=session):
            # Process each batch
            print(f"Processing {len(batch_df)} rows")
            await process_batch(batch_df)
    """
    if session is None:
        raise ValueError("session is required")

    # Use the standard reader with single partition to enable streaming
    reader = CassandraDataFrameReader(
        session=session,
        table=table,
        keyspace=keyspace,
        consistency_level=consistency_level,
    )

    # Ensure metadata is loaded
    await reader._ensure_metadata()

    # Parse table for streaming
    from .streaming import CassandraStreamer

    streamer = CassandraStreamer(session)

    # Build query
    if columns is None:
        columns = [col["name"] for col in reader._table_metadata["columns"]]

    select_list = ", ".join(columns)
    query = f"SELECT {select_list} FROM {reader.keyspace}.{reader.table}"

    # Add any predicates
    predicates = kwargs.get("predicates", [])
    values = []
    if predicates:
        where_parts = []
        for pred in predicates:
            where_parts.append(f"{pred['column']} {pred['operator']} ?")
            values.append(pred["value"])
        query += " WHERE " + " AND ".join(where_parts)

    # Stream in batches
    from async_cassandra.streaming import StreamConfig

    stream_config = StreamConfig(fetch_size=batch_size)
    prepared = await session.prepare(query)

    # Create execution profile if consistency level specified
    execution_profile = None
    if consistency_level:
        from .consistency import create_execution_profile, parse_consistency_level

        cl = parse_consistency_level(consistency_level)
        execution_profile = create_execution_profile(cl)

    # Execute streaming query
    stream_result = await session.execute_stream(
        prepared, tuple(values), stream_config=stream_config, execution_profile=execution_profile
    )

    # Yield batches
    batch_rows = []
    async with stream_result as stream:
        async for row in stream:
            batch_rows.append(row)

            if len(batch_rows) >= batch_size:
                # Convert batch to DataFrame
                df = streamer._rows_to_dataframe(batch_rows, columns)
                yield df
                batch_rows = []

        # Yield any remaining rows
        if batch_rows:
            df = streamer._rows_to_dataframe(batch_rows, columns)
            yield df
