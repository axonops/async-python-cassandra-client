"""
Partitioning strategies for mapping Cassandra token ranges to Dask partitions.

This module provides intelligent strategies for grouping Cassandra's natural
token ranges into Dask DataFrame partitions while respecting data locality
and cluster topology.
"""

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any

from .token_ranges import TokenRange

logger = logging.getLogger(__name__)


class PartitioningStrategy(str, Enum):
    """Available partitioning strategies."""

    AUTO = "auto"  # Intelligent defaults based on topology
    NATURAL = "natural"  # One partition per token range
    COMPACT = "compact"  # Balance parallelism and overhead
    FIXED = "fixed"  # User-specified partition count
    SPLIT = "split"  # Split each token range into N sub-partitions


@dataclass
class PartitionGroup:
    """A group of token ranges that will form a single Dask partition."""

    partition_id: int
    token_ranges: list[TokenRange]
    estimated_size_mb: float
    primary_replica: str | None = None

    @property
    def range_count(self) -> int:
        """Number of token ranges in this group."""
        return len(self.token_ranges)

    @property
    def total_fraction(self) -> float:
        """Total fraction of the ring covered by this group."""
        return sum(tr.fraction for tr in self.token_ranges)

    def add_range(self, token_range: TokenRange, size_mb: float = 0) -> None:
        """Add a token range to this group."""
        self.token_ranges.append(token_range)
        self.estimated_size_mb += size_mb


class TokenRangeGrouper:
    """Groups Cassandra token ranges into Dask partitions."""

    def __init__(self, default_partition_size_mb: int = 1024, max_partitions_per_node: int = 50):
        """
        Initialize the grouper.

        Args:
            default_partition_size_mb: Target size for each partition in MB
            max_partitions_per_node: Maximum partitions per Cassandra node
        """
        self.default_partition_size_mb = default_partition_size_mb
        self.max_partitions_per_node = max_partitions_per_node

    def group_token_ranges(
        self,
        token_ranges: list[TokenRange],
        strategy: PartitioningStrategy = PartitioningStrategy.AUTO,
        target_partition_count: int | None = None,
        target_partition_size_mb: int | None = None,
        split_factor: int | None = None,
    ) -> list[PartitionGroup]:
        """
        Group token ranges into partitions based on strategy.

        Args:
            token_ranges: Natural token ranges from Cassandra
            strategy: Partitioning strategy to use
            target_partition_count: Desired number of partitions (for FIXED strategy)
            target_partition_size_mb: Target size per partition
            split_factor: Number of sub-partitions per token range (for SPLIT strategy)

        Returns:
            List of partition groups
        """
        if not token_ranges:
            return []

        target_size = target_partition_size_mb or self.default_partition_size_mb

        if strategy == PartitioningStrategy.NATURAL:
            return self._natural_grouping(token_ranges)
        elif strategy == PartitioningStrategy.COMPACT:
            return self._compact_grouping(token_ranges, target_size)
        elif strategy == PartitioningStrategy.FIXED:
            if target_partition_count is None:
                raise ValueError("FIXED strategy requires target_partition_count")
            return self._fixed_grouping(token_ranges, target_partition_count)
        elif strategy == PartitioningStrategy.SPLIT:
            if split_factor is None:
                raise ValueError("SPLIT strategy requires split_factor")
            return self._split_grouping(token_ranges, split_factor)
        else:  # AUTO
            return self._auto_grouping(token_ranges, target_size)

    def _natural_grouping(self, token_ranges: list[TokenRange]) -> list[PartitionGroup]:
        """One partition per token range - maximum parallelism."""
        groups = []
        # Estimate size based on fraction of ring
        total_fraction = sum(tr.fraction for tr in token_ranges)
        avg_size_mb = self.default_partition_size_mb / max(10, len(token_ranges))

        for i, tr in enumerate(token_ranges):
            # Estimate size based on fraction of ring
            estimated_size = avg_size_mb * (tr.fraction / total_fraction) * len(token_ranges)
            group = PartitionGroup(
                partition_id=i,
                token_ranges=[tr],
                estimated_size_mb=estimated_size,
                primary_replica=tr.replicas[0] if tr.replicas else None,
            )
            groups.append(group)
        return groups

    def _compact_grouping(
        self, token_ranges: list[TokenRange], target_size_mb: int
    ) -> list[PartitionGroup]:
        """Group ranges to achieve target partition size."""
        # First group by primary replica for better locality
        ranges_by_replica = self._group_by_replica(token_ranges)

        # Estimate size per range based on fraction
        total_fraction = sum(tr.fraction for tr in token_ranges)
        estimated_total_size = target_size_mb * len(token_ranges) / 10  # Rough estimate

        groups = []
        partition_id = 0

        for replica, ranges in ranges_by_replica.items():
            current_group = PartitionGroup(
                partition_id=partition_id,
                token_ranges=[],
                estimated_size_mb=0,
                primary_replica=replica,
            )

            for token_range in ranges:
                # Estimate size for this range
                range_size = estimated_total_size * (token_range.fraction / total_fraction)

                # Check if adding this range would exceed target size
                if (
                    current_group.estimated_size_mb > 0
                    and current_group.estimated_size_mb + range_size > target_size_mb
                ):
                    # Start a new group
                    groups.append(current_group)
                    partition_id += 1
                    current_group = PartitionGroup(
                        partition_id=partition_id,
                        token_ranges=[],
                        estimated_size_mb=0,
                        primary_replica=replica,
                    )

                current_group.add_range(token_range, range_size)

            # Don't forget the last group
            if current_group.token_ranges:
                groups.append(current_group)
                partition_id += 1

        return groups

    def _fixed_grouping(
        self, token_ranges: list[TokenRange], target_count: int
    ) -> list[PartitionGroup]:
        """Group into exactly the specified number of partitions."""
        # Can't have more partitions than token ranges
        actual_count = min(target_count, len(token_ranges))

        if actual_count == len(token_ranges):
            return self._natural_grouping(token_ranges)

        # Group by replica first for better locality
        ranges_by_replica = self._group_by_replica(token_ranges)

        # Calculate ranges per partition
        ranges_per_partition = len(token_ranges) / actual_count

        groups = []
        partition_id = 0
        current_group = PartitionGroup(
            partition_id=partition_id, token_ranges=[], estimated_size_mb=0
        )
        ranges_added = 0

        for replica, ranges in ranges_by_replica.items():
            for token_range in ranges:
                # Estimate size for even distribution
                range_size = self.default_partition_size_mb / actual_count
                current_group.add_range(token_range, range_size)
                current_group.primary_replica = current_group.primary_replica or replica
                ranges_added += 1

                # Check if we should start a new partition
                if (
                    ranges_added >= ranges_per_partition * (partition_id + 1)
                    and partition_id < actual_count - 1
                ):
                    groups.append(current_group)
                    partition_id += 1
                    current_group = PartitionGroup(
                        partition_id=partition_id, token_ranges=[], estimated_size_mb=0
                    )

        # Add the last group
        if current_group.token_ranges:
            groups.append(current_group)

        return groups

    def _auto_grouping(
        self, token_ranges: list[TokenRange], target_size_mb: int
    ) -> list[PartitionGroup]:
        """
        Intelligent grouping based on cluster characteristics.

        Heuristics:
        - High vnode count (>= 256): Group aggressively
        - Medium vnode count (16-255): Moderate grouping
        - Low vnode count (<= 16): Close to natural
        """
        # Estimate cluster characteristics
        unique_nodes = len({tr.replicas[0] for tr in token_ranges if tr.replicas})
        vnodes_per_node = len(token_ranges) / max(1, unique_nodes)

        logger.info(
            f"Auto partitioning: {len(token_ranges)} ranges, "
            f"{unique_nodes} nodes, {vnodes_per_node:.1f} vnodes/node"
        )

        if vnodes_per_node >= 256:
            # High vnode count - group aggressively
            # Target 10-50 partitions per node
            target_partitions = max(
                unique_nodes * 10, min(unique_nodes * 50, len(token_ranges) // 20)
            )
            return self._fixed_grouping(token_ranges, target_partitions)

        elif vnodes_per_node >= 16:
            # Medium vnode count - moderate grouping
            # Use compact strategy with adjusted size
            adjusted_size = target_size_mb * 2  # Larger partitions
            return self._compact_grouping(token_ranges, adjusted_size)

        else:
            # Low vnode count - close to natural
            if len(token_ranges) <= 16:
                # Very few ranges - use natural grouping
                return self._natural_grouping(token_ranges)
            else:
                # Apply minimal grouping
                target_partitions = max(len(token_ranges) // 2, unique_nodes * 4)
                return self._fixed_grouping(token_ranges, target_partitions)

    def _split_grouping(
        self, token_ranges: list[TokenRange], split_factor: int
    ) -> list[PartitionGroup]:
        """
        Split each token range into N sub-partitions.

        Args:
            token_ranges: Original token ranges from Cassandra
            split_factor: Number of sub-partitions per token range

        Returns:
            List of partition groups, one per sub-range
        """
        groups = []
        partition_id = 0

        for token_range in token_ranges:
            # Split the token range into sub-ranges
            sub_ranges = token_range.split(split_factor)

            # Create a partition group for each sub-range
            for sub_range in sub_ranges:
                # Estimate size based on fraction
                estimated_size = self.default_partition_size_mb * sub_range.fraction

                group = PartitionGroup(
                    partition_id=partition_id,
                    token_ranges=[sub_range],
                    estimated_size_mb=estimated_size,
                    primary_replica=sub_range.replicas[0] if sub_range.replicas else None,
                )
                groups.append(group)
                partition_id += 1

        logger.info(
            f"Split partitioning: {len(token_ranges)} ranges split by {split_factor} "
            f"= {len(groups)} partitions"
        )

        return groups

    def _group_by_replica(self, token_ranges: list[TokenRange]) -> dict[str, list[TokenRange]]:
        """Group token ranges by their primary replica."""
        ranges_by_replica: dict[str, list[TokenRange]] = {}

        for tr in token_ranges:
            primary = tr.replicas[0] if tr.replicas else "unknown"
            if primary not in ranges_by_replica:
                ranges_by_replica[primary] = []
            ranges_by_replica[primary].append(tr)

        return ranges_by_replica

    def get_partition_summary(self, groups: list[PartitionGroup]) -> dict[str, Any]:
        """Get summary statistics about the partitioning."""
        if not groups:
            return {"partition_count": 0}

        sizes = [g.estimated_size_mb for g in groups]
        range_counts = [g.range_count for g in groups]

        return {
            "partition_count": len(groups),
            "total_token_ranges": sum(range_counts),
            "avg_ranges_per_partition": sum(range_counts) / len(groups),
            "min_ranges_per_partition": min(range_counts),
            "max_ranges_per_partition": max(range_counts),
            "total_size_mb": sum(sizes),
            "avg_partition_size_mb": sum(sizes) / len(groups),
            "min_partition_size_mb": min(sizes),
            "max_partition_size_mb": max(sizes),
        }
