"""
Experimental code to test token range to Dask partition mapping concepts.

This file explores different strategies for mapping Cassandra's natural
token ranges to Dask partitions while respecting data locality.
"""

import asyncio
from dataclasses import dataclass
from typing import Any

import dask
import dask.dataframe as dd
import pandas as pd


@dataclass
class TokenRange:
    """Represents a Cassandra token range with its replicas."""

    start_token: int
    end_token: int
    replicas: list[str]
    estimated_size_mb: float = 0.0


@dataclass
class DaskPartitionPlan:
    """Plan for a single Dask partition containing multiple token ranges."""

    partition_id: int
    token_ranges: list[TokenRange]
    estimated_total_size_mb: float
    primary_replica: str  # Preferred replica for routing


def simulate_cassandra_token_ranges(num_nodes: int = 3, vnodes: int = 256) -> list[TokenRange]:
    """
    Simulate token ranges for a Cassandra cluster.

    In reality, these would come from system.local and system.peers.
    """
    total_ranges = num_nodes * vnodes
    token_space = 2**63
    ranges = []

    for i in range(total_ranges):
        start = int(-token_space + (2 * token_space * i / total_ranges))
        end = int(-token_space + (2 * token_space * (i + 1) / total_ranges))

        # Simulate replica assignment (simplified)
        primary_node = i % num_nodes
        replicas = [f"node{(primary_node + j) % num_nodes}" for j in range(min(3, num_nodes))]

        # Simulate varying data sizes
        size_mb = 50 + (i % 100)  # 50-150MB per range

        ranges.append(TokenRange(start, end, replicas, size_mb))

    return ranges


def group_token_ranges_for_dask(
    token_ranges: list[TokenRange],
    target_partitions: int,
    target_partition_size_mb: float = 1024,  # 1GB default
) -> list[DaskPartitionPlan]:
    """
    Group Cassandra token ranges into Dask partitions intelligently.

    Goals:
    1. Never split a natural token range
    2. Try to group ranges from the same replica together
    3. Balance partition sizes
    4. Respect the user's target partition count (if possible)
    """
    # First, group by primary replica for better data locality
    ranges_by_replica: dict[str, list[TokenRange]] = {}
    for tr in token_ranges:
        primary = tr.replicas[0]
        if primary not in ranges_by_replica:
            ranges_by_replica[primary] = []
        ranges_by_replica[primary].append(tr)

    # Calculate ideal ranges per partition
    total_ranges = len(token_ranges)
    ranges_per_partition = max(1, total_ranges // target_partitions)

    dask_partitions = []
    partition_id = 0

    # Process each replica's ranges
    for replica, ranges in ranges_by_replica.items():
        current_partition_ranges = []
        current_size = 0.0

        for token_range in ranges:
            current_partition_ranges.append(token_range)
            current_size += token_range.estimated_size_mb

            # Create partition if we've hit our targets
            should_create_partition = (
                len(current_partition_ranges) >= ranges_per_partition
                or current_size >= target_partition_size_mb
                or len(dask_partitions) < target_partitions - (total_ranges - partition_id)
            )

            if should_create_partition and current_partition_ranges:
                dask_partitions.append(
                    DaskPartitionPlan(
                        partition_id=partition_id,
                        token_ranges=current_partition_ranges.copy(),
                        estimated_total_size_mb=current_size,
                        primary_replica=replica,
                    )
                )
                partition_id += 1
                current_partition_ranges = []
                current_size = 0.0

        # Don't forget remaining ranges
        if current_partition_ranges:
            dask_partitions.append(
                DaskPartitionPlan(
                    partition_id=partition_id,
                    token_ranges=current_partition_ranges,
                    estimated_total_size_mb=current_size,
                    primary_replica=replica,
                )
            )
            partition_id += 1

    return dask_partitions


async def read_token_range_async(
    session: Any, table: str, token_range: TokenRange  # Would be AsyncSession in real code
) -> pd.DataFrame:
    """Simulate reading a single token range from Cassandra."""
    # In real implementation, this would:
    # 1. Build query: SELECT * FROM table WHERE token(pk) >= start AND token(pk) <= end
    # 2. Stream results using async-cassandra
    # 3. Build DataFrame incrementally

    # Simulate some data
    num_rows = int(token_range.estimated_size_mb * 1000)  # ~1000 rows per MB
    return pd.DataFrame(
        {
            "id": range(num_rows),
            "value": [f"data_{i}" for i in range(num_rows)],
            "token_range": f"{token_range.start_token}_{token_range.end_token}",
        }
    )


def read_dask_partition(
    session: Any, table: str, partition_plan: DaskPartitionPlan
) -> pd.DataFrame:
    """
    Read all token ranges for a single Dask partition.

    This function will be called by dask.delayed for each partition.
    """
    # Create event loop for async operations (since Dask uses threads)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        # Read all token ranges in parallel within this partition
        async def read_all_ranges():
            tasks = [
                read_token_range_async(session, table, tr) for tr in partition_plan.token_ranges
            ]
            dfs = await asyncio.gather(*tasks)
            return pd.concat(dfs, ignore_index=True)

        # Execute and return combined DataFrame
        return loop.run_until_complete(read_all_ranges())
    finally:
        loop.close()


def create_dask_dataframe_from_cassandra(
    session: Any, table: str, partition_count: int = None, partition_size_mb: float = 1024
) -> dd.DataFrame:
    """
    Main entry point: Create a Dask DataFrame from Cassandra table.

    This respects Cassandra's natural token ranges while providing
    the desired Dask partition count.
    """
    # 1. Discover natural token ranges
    natural_ranges = simulate_cassandra_token_ranges()
    print(f"Discovered {len(natural_ranges)} natural token ranges")

    # 2. Determine partition count
    if partition_count is None:
        # Auto-calculate based on total data size
        total_size_mb = sum(tr.estimated_size_mb for tr in natural_ranges)
        partition_count = max(1, int(total_size_mb / partition_size_mb))

    # Ensure we don't have more partitions than token ranges
    partition_count = min(partition_count, len(natural_ranges))

    # 3. Group token ranges into Dask partitions
    partition_plans = group_token_ranges_for_dask(
        natural_ranges, partition_count, partition_size_mb
    )
    print(f"Created {len(partition_plans)} Dask partition plans")

    # 4. Create delayed tasks
    delayed_partitions = []
    for plan in partition_plans:
        delayed = dask.delayed(read_dask_partition)(session, table, plan)
        delayed_partitions.append(delayed)

    # 5. Create Dask DataFrame (lazy)
    meta = pd.DataFrame(
        {
            "id": pd.Series([], dtype="int64"),
            "value": pd.Series([], dtype="object"),
            "token_range": pd.Series([], dtype="object"),
        }
    )

    df = dd.from_delayed(delayed_partitions, meta=meta)

    return df


def test_concept():
    """Test the token range grouping concept."""
    # Simulate a session (would be real AsyncSession)
    session = "mock_session"

    # Test different partition counts
    for requested_partitions in [10, 100, 1000]:
        print(f"\n--- Testing with {requested_partitions} requested partitions ---")

        df = create_dask_dataframe_from_cassandra(
            session, "test_table", partition_count=requested_partitions
        )

        print(f"Actual Dask partitions: {df.npartitions}")

        # This would actually load data in real usage
        # row_counts = df.map_partitions(len).compute()
        # print(f"Rows per partition: {row_counts.tolist()}")


if __name__ == "__main__":
    test_concept()
