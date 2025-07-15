"""
Enhanced DataFrame reader with writetime filtering and concurrency control.

Provides production-ready features including:
- Writetime-based filtering (older/younger than)
- Snapshot consistency with "now" parameter
- Concurrency control to protect Cassandra cluster
"""

import asyncio
import logging
from datetime import UTC, datetime
from typing import Any

import dask
import dask.dataframe as dd
import pandas as pd
from dask.distributed import Client

from .dataframe_factory import DataFrameFactory
from .event_loop_manager import EventLoopManager
from .filter_processor import FilterProcessor
from .metadata import TableMetadataExtractor
from .partition import StreamingPartitionStrategy
from .partition_reader import PartitionReader
from .partition_strategy import PartitioningStrategy, TokenRangeGrouper
from .predicate_pushdown import PredicatePushdownAnalyzer
from .query_builder import QueryBuilder
from .serializers import TTLSerializer, WritetimeSerializer
from .token_ranges import discover_token_ranges
from .types import CassandraTypeMapper

# Configure Dask to not use PyArrow strings by default
# This preserves object dtypes for things like VARINT
dask.config.set({"dataframe.convert-string": False})

logger = logging.getLogger(__name__)


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
        self.memory_per_partition_mb = 128  # Default

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
        self._token_range_grouper = TokenRangeGrouper()

        # Cached metadata
        self._table_metadata: dict[str, Any] | None = None
        self._query_builder: QueryBuilder | None = None
        self._filter_processor: FilterProcessor | None = None
        self._dataframe_factory: DataFrameFactory | None = None

        # Concurrency control
        self._semaphore = None
        if max_concurrent_queries:
            self._semaphore = asyncio.Semaphore(max_concurrent_queries)

        # Create shared executor for Dask
        self.executor = EventLoopManager.get_loop_runner().executor

    async def _ensure_metadata(self):
        """Ensure table metadata is loaded."""
        if self._table_metadata is None:
            self._table_metadata = await self.metadata_extractor.get_table_metadata(
                self.keyspace, self.table
            )
            self._query_builder = QueryBuilder(self._table_metadata)
            self._filter_processor = FilterProcessor(self._table_metadata)
            self._dataframe_factory = DataFrameFactory(self._table_metadata, self.type_mapper)

    @property
    def table_metadata(self) -> dict[str, Any]:
        """Get table metadata, raising error if not loaded."""
        if self._table_metadata is None:
            raise RuntimeError("Metadata not loaded. Call _ensure_metadata() first.")
        return self._table_metadata

    @property
    def query_builder(self) -> QueryBuilder:
        """Get query builder, raising error if not loaded."""
        if self._query_builder is None:
            raise RuntimeError("Query builder not loaded. Call _ensure_metadata() first.")
        return self._query_builder

    @property
    def filter_processor(self) -> FilterProcessor:
        """Get filter processor, raising error if not loaded."""
        if self._filter_processor is None:
            raise RuntimeError("Filter processor not loaded. Call _ensure_metadata() first.")
        return self._filter_processor

    @property
    def dataframe_factory(self) -> DataFrameFactory:
        """Get dataframe factory, raising error if not loaded."""
        if self._dataframe_factory is None:
            raise RuntimeError("DataFrame factory not loaded. Call _ensure_metadata() first.")
        return self._dataframe_factory

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
        max_concurrent_partitions: int | None = None,
        # Streaming
        page_size: int | None = None,
        adaptive_page_size: bool = False,
        # Partitioning strategy
        partition_strategy: str = "auto",
        target_partition_size_mb: int = 1024,
        split_factor: int | None = None,
        # Validation
        require_partition_key_predicate: bool = False,
        # Progress
        progress_callback: Any | None = None,
        # Dask
        client: Client | None = None,
    ) -> dd.DataFrame:
        """
        Read Cassandra table as Dask DataFrame with enhanced filtering.

        Args:
            See original docstring for full parameter documentation.

        Returns:
            Dask DataFrame
        """
        # Ensure metadata loaded
        await self._ensure_metadata()

        # Help mypy understand these are not None after _ensure_metadata
        assert self.table_metadata is not None
        assert self.query_builder is not None
        assert self.filter_processor is not None
        assert self._dataframe_factory is not None

        # Store memory limit for partition creation
        self.memory_per_partition_mb = memory_per_partition_mb

        # Validate and prepare parameters
        columns = await self._prepare_columns(columns)
        writetime_columns = await self._prepare_writetime_columns(writetime_columns)
        ttl_columns = await self._prepare_ttl_columns(ttl_columns)

        # Process filters and predicates
        writetime_filter = await self._process_writetime_filter(
            writetime_filter, snapshot_time, writetime_columns
        )
        pushdown_predicates, client_predicates, use_token_ranges = await self._process_predicates(
            predicates, require_partition_key_predicate
        )

        # Validate page size
        self._validate_page_size(page_size)

        # Create partitions
        partitions = await self._create_partitions(
            columns,
            partition_count,
            use_token_ranges,
            pushdown_predicates,
            partition_strategy,
            target_partition_size_mb,
            split_factor,
        )

        # Normalize snapshot time
        normalized_snapshot_time: datetime | None = None
        if snapshot_time:
            if snapshot_time == "now":
                normalized_snapshot_time = datetime.now(UTC)
            elif isinstance(snapshot_time, str):
                normalized_snapshot_time = pd.Timestamp(snapshot_time).to_pydatetime()
            else:
                normalized_snapshot_time = snapshot_time

        # Prepare partition definitions
        self._prepare_partition_definitions(
            partitions,
            columns,
            writetime_columns,
            ttl_columns,
            writetime_filter,
            normalized_snapshot_time,
            pushdown_predicates,
            client_predicates,
            allow_filtering,
            page_size,
            adaptive_page_size,
        )

        # Get DataFrame schema
        meta = self.dataframe_factory.create_dataframe_meta(columns, writetime_columns, ttl_columns)

        # Create Dask DataFrame using delayed execution
        df = self._create_dask_dataframe(partitions, meta)

        # Apply post-processing filters
        if writetime_filter:
            df = self.filter_processor.apply_writetime_filter(df, writetime_filter)

        if client_predicates:
            df = self.filter_processor.apply_client_predicates(df, client_predicates)

        return df

    async def _prepare_columns(self, columns: list[str] | None) -> list[str]:
        """Prepare and validate columns."""
        if columns is None:
            columns = [col["name"] for col in self.table_metadata["columns"]]
        else:
            # Validate columns exist
            self.query_builder.validate_columns(columns)
        return columns

    async def _prepare_writetime_columns(
        self, writetime_columns: list[str] | None
    ) -> list[str] | None:
        """Prepare writetime columns."""
        if writetime_columns:
            # Expand wildcards and filter to writetime-capable columns
            valid_columns = self.metadata_extractor.expand_column_wildcards(
                writetime_columns, self.table_metadata, writetime_capable_only=True
            )

            # Check if any requested columns don't support writetime
            if "*" not in writetime_columns:
                # Get all writetime-capable columns
                capable_columns = set(
                    self.metadata_extractor.get_writetime_capable_columns(self.table_metadata)
                )

                # Check each requested column
                for col in writetime_columns:
                    if col not in capable_columns:
                        # Find the column info to provide better error message
                        col_info = next(
                            (c for c in self.table_metadata["columns"] if c["name"] == col), None
                        )
                        if col_info:
                            col_type = str(col_info["type"])
                            if col_info["is_primary_key"]:
                                raise ValueError(
                                    f"Column '{col}' is a primary key column and doesn't support writetime"
                                )
                            elif col_type == "counter":
                                raise ValueError(
                                    f"Column '{col}' is a counter column and doesn't support writetime"
                                )
                            elif self.metadata_extractor._is_udt_type(col_type):
                                raise ValueError(
                                    f"Column '{col}' is a UDT type and doesn't support writetime"
                                )
                            else:
                                raise ValueError(f"Column '{col}' doesn't support writetime")
                        else:
                            raise ValueError(f"Column '{col}' not found in table")

            return valid_columns
        return writetime_columns

    async def _prepare_ttl_columns(self, ttl_columns: list[str] | None) -> list[str] | None:
        """Prepare TTL columns."""
        if ttl_columns:
            # Expand wildcards and filter to TTL-capable columns
            valid_columns = self.metadata_extractor.expand_column_wildcards(
                ttl_columns, self.table_metadata, ttl_capable_only=True
            )

            # Check if any requested columns don't support TTL
            if "*" not in ttl_columns:
                # Get all TTL-capable columns
                capable_columns = set(
                    self.metadata_extractor.get_ttl_capable_columns(self.table_metadata)
                )

                # Check each requested column
                for col in ttl_columns:
                    if col not in capable_columns:
                        # Find the column info to provide better error message
                        col_info = next(
                            (c for c in self.table_metadata["columns"] if c["name"] == col), None
                        )
                        if col_info:
                            col_type = str(col_info["type"])
                            if col_info["is_primary_key"]:
                                raise ValueError(
                                    f"Column '{col}' is a primary key column and doesn't support TTL"
                                )
                            elif col_type == "counter":
                                raise ValueError(
                                    f"Column '{col}' is a counter column and doesn't support TTL"
                                )
                            else:
                                raise ValueError(f"Column '{col}' doesn't support TTL")
                        else:
                            raise ValueError(f"Column '{col}' not found in table")

            return valid_columns
        return ttl_columns

    async def _process_writetime_filter(
        self,
        writetime_filter: dict[str, Any] | None,
        snapshot_time: datetime | str | None,
        writetime_columns: list[str] | None,
    ) -> dict[str, Any] | None:
        """Process writetime filter and snapshot time."""
        if not writetime_filter:
            return None

        # Handle snapshot time
        normalized_snapshot_time: datetime | None = None
        if snapshot_time:
            if snapshot_time == "now":
                normalized_snapshot_time = datetime.now(UTC)
            elif isinstance(snapshot_time, str):
                normalized_snapshot_time = pd.Timestamp(snapshot_time).to_pydatetime()
            else:
                normalized_snapshot_time = snapshot_time

        # Normalize filter
        writetime_filter = self.filter_processor.normalize_writetime_filter(
            writetime_filter, normalized_snapshot_time
        )

        # Expand wildcard if needed
        if writetime_filter["column"] == "*":
            # Get all writetime-capable columns
            capable_columns = self.metadata_extractor.get_writetime_capable_columns(
                self.table_metadata
            )
            writetime_filter["columns"] = capable_columns
        else:
            writetime_filter["columns"] = [writetime_filter["column"]]

        return writetime_filter

    async def _process_predicates(
        self, predicates: list[dict[str, Any]] | None, require_partition_key_predicate: bool
    ) -> tuple[list, list, bool]:
        """Process predicates for pushdown."""
        if not predicates:
            return [], [], True

        # Validate columns exist
        valid_columns = {col["name"] for col in self.table_metadata["columns"]}
        for pred in predicates:
            if pred["column"] not in valid_columns:
                raise ValueError(
                    f"Column '{pred['column']}' not found in table {self.keyspace}.{self.table}"
                )

        # Validate partition key predicates if required
        self.filter_processor.validate_partition_key_predicates(
            predicates, require_partition_key_predicate
        )

        # Analyze predicates
        analyzer = PredicatePushdownAnalyzer(self.table_metadata)
        pushdown_predicates, client_predicates, use_token_ranges = analyzer.analyze_predicates(
            predicates, use_token_ranges=True
        )

        return pushdown_predicates, client_predicates, use_token_ranges

    def _validate_page_size(self, page_size: int | None) -> None:
        """Validate page size parameter."""
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

    async def _create_partitions(
        self,
        columns: list[str],
        partition_count: int | None,
        use_token_ranges: bool,
        pushdown_predicates: list,
        partition_strategy: str,
        target_partition_size_mb: int,
        split_factor: int | None,
    ) -> list[dict[str, Any]]:
        """Create partition definitions."""
        # Create partition strategy
        streaming_strategy = StreamingPartitionStrategy(
            session=self.session,
            memory_per_partition_mb=self.memory_per_partition_mb,
        )

        # Create initial partitions
        partitions = await streaming_strategy.create_partitions(
            table=f"{self.keyspace}.{self.table}",
            columns=columns,
            partition_count=partition_count,
            use_token_ranges=use_token_ranges,
            pushdown_predicates=pushdown_predicates,
        )

        # Apply intelligent partitioning strategies if requested
        if partition_strategy != "legacy" and use_token_ranges:
            try:
                partitions = await self._create_grouped_partitions(
                    partitions,
                    partition_strategy,
                    partition_count,
                    target_partition_size_mb,
                    columns,
                    None,  # writetime_columns
                    None,  # ttl_columns
                    split_factor,
                )
            except Exception as e:
                logger.warning(f"Could not apply partitioning strategy: {e}")

        return partitions

    async def _create_grouped_partitions(
        self,
        original_partitions: list[dict[str, Any]],
        partition_strategy: str,
        partition_count: int | None,
        target_partition_size_mb: int,
        columns: list[str],
        writetime_columns: list[str] | None,
        ttl_columns: list[str] | None,
        split_factor: int | None,
    ) -> list[dict[str, Any]]:
        """Create grouped partitions based on partitioning strategy."""
        # Get natural token ranges
        natural_ranges = await discover_token_ranges(self.session, self.keyspace)

        if not natural_ranges or len(natural_ranges) <= 1:
            # Not enough ranges to group
            return original_partitions

        # Apply intelligent grouping
        strategy_enum = PartitioningStrategy(partition_strategy)
        partition_groups = self._token_range_grouper.group_token_ranges(
            natural_ranges,
            strategy=strategy_enum,
            target_partition_count=partition_count,
            target_partition_size_mb=target_partition_size_mb,
            split_factor=split_factor,
        )

        # Log partitioning info
        summary = self._token_range_grouper.get_partition_summary(partition_groups)
        logger.info(
            f"Partitioning strategy '{partition_strategy}': "
            f"{summary['partition_count']} Dask partitions from "
            f"{summary['total_token_ranges']} token ranges"
        )

        # Create new partition definitions based on groups
        grouped_partitions = []
        table = f"{self.keyspace}.{self.table}"

        for group in partition_groups:
            # Each group contains multiple token ranges
            partition_def = {
                "partition_id": group.partition_id,
                "table": table,
                "columns": columns,
                "token_ranges": group.token_ranges,  # Multiple ranges
                "replicas": group.primary_replica,
                "strategy": "grouped_token_ranges",
                "memory_limit_mb": self.memory_per_partition_mb,
                "use_token_ranges": True,
                "group_info": {
                    "range_count": group.range_count,
                    "total_fraction": group.total_fraction,
                    "estimated_size_mb": group.estimated_size_mb,
                },
            }
            grouped_partitions.append(partition_def)

        return grouped_partitions

    def _prepare_partition_definitions(
        self,
        partitions: list[dict[str, Any]],
        columns: list[str],
        writetime_columns: list[str] | None,
        ttl_columns: list[str] | None,
        writetime_filter: dict[str, Any] | None,
        snapshot_time: datetime | None,
        pushdown_predicates: list,
        client_predicates: list,
        allow_filtering: bool,
        page_size: int | None,
        adaptive_page_size: bool,
    ) -> None:
        """Prepare partition definitions with all required info."""
        for partition_def in partitions:
            # Add query-specific info to partition definition
            partition_def["writetime_columns"] = writetime_columns
            partition_def["ttl_columns"] = ttl_columns
            partition_def["query_builder"] = self.query_builder
            partition_def["type_mapper"] = self.type_mapper
            # For token queries, only use partition key columns
            partition_def["primary_key_columns"] = self.table_metadata["partition_key"]
            partition_def["_table_metadata"] = self.table_metadata
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

    def _create_dask_dataframe(
        self, partitions: list[dict[str, Any]], meta: pd.DataFrame
    ) -> dd.DataFrame:
        """Create Dask DataFrame using delayed execution."""
        delayed_partitions = []

        for partition_def in partitions:
            # Create delayed task
            delayed = dask.delayed(PartitionReader.read_partition_sync)(
                partition_def,
                self.session,
            )
            delayed_partitions.append(delayed)

        # Debug
        # print(f"DEBUG reader._create_dask_dataframe_delayed: Creating {len(partitions)} partitions")
        # if partitions:
        #     print(f"DEBUG reader: First partition writetime_columns={partitions[0].get('writetime_columns')}")

        # Create multi-partition Dask DataFrame
        df = dd.from_delayed(delayed_partitions, meta=meta)

        logger.info(
            f"Created Dask DataFrame with {df.npartitions} partitions using delayed execution"
        )

        return df  # type: ignore[no-any-return]

    @classmethod
    def cleanup_executor(cls):
        """Shutdown the shared event loop runner."""
        EventLoopManager.cleanup()


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
    # Partitioning strategy
    partition_strategy: str = "auto",
    partitioning_strategy: str | None = None,  # Alias for backward compatibility
    target_partition_size_mb: int = 1024,
    split_factor: int | None = None,
    # Validation
    require_partition_key_predicate: bool = False,
    # Progress
    progress_callback: Any | None = None,
    # Dask
    client: Client | None = None,
) -> dd.DataFrame:
    """
    Read Cassandra table as Dask DataFrame with enhanced filtering and concurrency control.

    See CassandraDataFrameReader.read() for full documentation.
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
        partition_strategy=partitioning_strategy or partition_strategy,  # Use alias if provided
        target_partition_size_mb=target_partition_size_mb,
        split_factor=split_factor,
        require_partition_key_predicate=require_partition_key_predicate,
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

    See original implementation for full documentation.
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

    # Help mypy understand these are not None after _ensure_metadata
    assert reader._table_metadata is not None

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
