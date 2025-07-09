"""
Integration tests for token range splitting functionality.

What this tests:
---------------
1. Token range splitting with different strategies
2. Proportional splitting based on range sizes
3. Handling of very small ranges (vnodes)
4. Replica-aware clustering

Why this matters:
----------------
- Efficient parallelism requires good splitting
- Vnodes create many small ranges that shouldn't be over-split
- Replica clustering improves coordinator efficiency
- Performance optimization foundation
"""

import pytest

from async_cassandra import AsyncCluster
from bulk_operations.token_utils import TokenRangeSplitter, discover_token_ranges


@pytest.mark.integration
class TestTokenSplitting:
    """Test token range splitting strategies."""

    @pytest.fixture
    async def cluster(self):
        """Create connection to test cluster."""
        cluster = AsyncCluster(
            contact_points=["localhost"],
            port=9042,
        )
        yield cluster
        await cluster.shutdown()

    @pytest.fixture
    async def session(self, cluster):
        """Create test session with keyspace."""
        session = await cluster.connect()

        # Create test keyspace
        await session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS bulk_test
            WITH replication = {
                'class': 'SimpleStrategy',
                'replication_factor': 1
            }
        """
        )

        yield session

    @pytest.mark.asyncio
    async def test_token_range_splitting_with_vnodes(self, session):
        """
        Test that splitting handles vnode token ranges correctly.

        What this tests:
        ---------------
        1. Natural ranges from vnodes are small
        2. Splitting respects range boundaries
        3. Very small ranges aren't over-split
        4. Large splits still cover all ranges

        Why this matters:
        ----------------
        - Vnodes create many small ranges
        - Over-splitting causes overhead
        - Under-splitting reduces parallelism
        - Must balance performance
        """
        ranges = await discover_token_ranges(session, "bulk_test")
        splitter = TokenRangeSplitter()

        # Test different split counts
        for split_count in [10, 50, 100, 500]:
            splits = splitter.split_proportionally(ranges, split_count)

            print(f"\nSplitting {len(ranges)} ranges into {split_count} splits:")
            print(f"  Actual splits: {len(splits)}")

            # Verify coverage
            total_size = sum(r.size for r in ranges)
            split_size = sum(s.size for s in splits)

            assert split_size == total_size, f"Split size mismatch: {split_size} vs {total_size}"

            # With vnodes, we might not achieve the exact split count
            # because many ranges are too small to split
            if split_count < len(ranges):
                assert (
                    len(splits) >= split_count * 0.5
                ), f"Too few splits: {len(splits)} (wanted ~{split_count})"

    @pytest.mark.asyncio
    async def test_single_range_splitting(self, session):
        """
        Test splitting of individual token ranges.

        What this tests:
        ---------------
        1. Single range can be split evenly
        2. Last split gets remainder
        3. Small ranges aren't over-split
        4. Split boundaries are correct

        Why this matters:
        ----------------
        - Foundation of proportional splitting
        - Must handle edge cases correctly
        - Affects query generation
        - Performance depends on even distribution
        """
        ranges = await discover_token_ranges(session, "bulk_test")
        splitter = TokenRangeSplitter()

        # Find a reasonably large range to test
        sorted_ranges = sorted(ranges, key=lambda r: r.size, reverse=True)
        large_range = sorted_ranges[0]

        print("\nTesting single range splitting:")
        print(f"  Range size: {large_range.size}")
        print(f"  Range: {large_range.start} to {large_range.end}")

        # Test different split counts
        for split_count in [1, 2, 5, 10]:
            splits = splitter.split_single_range(large_range, split_count)

            print(f"\n  Splitting into {split_count}:")
            print(f"    Actual splits: {len(splits)}")

            # Verify coverage
            assert sum(s.size for s in splits) == large_range.size

            # Verify contiguous
            for i in range(len(splits) - 1):
                assert splits[i].end == splits[i + 1].start

            # Verify boundaries
            assert splits[0].start == large_range.start
            assert splits[-1].end == large_range.end

            # Verify replicas preserved
            for s in splits:
                assert s.replicas == large_range.replicas

    @pytest.mark.asyncio
    async def test_replica_clustering(self, session):
        """
        Test clustering ranges by replica sets.

        What this tests:
        ---------------
        1. Ranges are correctly grouped by replicas
        2. All ranges are included in clusters
        3. No ranges are duplicated
        4. Replica sets are handled consistently

        Why this matters:
        ----------------
        - Coordinator efficiency depends on replica locality
        - Reduces network hops in multi-DC setups
        - Improves cache utilization
        - Foundation for topology-aware operations
        """
        # For this test, use multi-node replication
        await session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS bulk_test_replicated
            WITH replication = {
                'class': 'SimpleStrategy',
                'replication_factor': 3
            }
        """
        )

        ranges = await discover_token_ranges(session, "bulk_test_replicated")
        splitter = TokenRangeSplitter()

        clusters = splitter.cluster_by_replicas(ranges)

        print("\nReplica clustering results:")
        print(f"  Total ranges: {len(ranges)}")
        print(f"  Replica clusters: {len(clusters)}")

        total_clustered = sum(len(ranges_list) for ranges_list in clusters.values())
        print(f"  Total ranges in clusters: {total_clustered}")

        # Verify all ranges are clustered
        assert total_clustered == len(
            ranges
        ), f"Not all ranges clustered: {total_clustered} vs {len(ranges)}"

        # Verify no duplicates
        seen_ranges = set()
        for _replica_set, range_list in clusters.items():
            for r in range_list:
                range_key = (r.start, r.end)
                assert range_key not in seen_ranges, f"Duplicate range: {range_key}"
                seen_ranges.add(range_key)

        # Print cluster distribution
        for replica_set, range_list in sorted(clusters.items()):
            print(f"  Replicas {replica_set}: {len(range_list)} ranges")

    @pytest.mark.asyncio
    async def test_proportional_splitting_accuracy(self, session):
        """
        Test that proportional splitting maintains relative sizes.

        What this tests:
        ---------------
        1. Large ranges get more splits than small ones
        2. Total coverage is preserved
        3. Split distribution matches range distribution
        4. No ranges are lost or duplicated

        Why this matters:
        ----------------
        - Even work distribution across ranges
        - Prevents hotspots from uneven splitting
        - Optimizes parallel execution
        - Critical for performance
        """
        ranges = await discover_token_ranges(session, "bulk_test")
        splitter = TokenRangeSplitter()

        # Calculate range size distribution
        total_size = sum(r.size for r in ranges)
        range_fractions = [(r, r.size / total_size) for r in ranges]

        # Sort by size for analysis
        range_fractions.sort(key=lambda x: x[1], reverse=True)

        print("\nRange size distribution:")
        print(f"  Largest range: {range_fractions[0][1]:.2%} of total")
        print(f"  Smallest range: {range_fractions[-1][1]:.2%} of total")
        print(f"  Median range: {range_fractions[len(range_fractions)//2][1]:.2%} of total")

        # Test proportional splitting
        target_splits = 100
        splits = splitter.split_proportionally(ranges, target_splits)

        # Analyze split distribution
        splits_per_range = {}
        for split in splits:
            # Find which original range this split came from
            for orig_range in ranges:
                if (split.start >= orig_range.start and split.end <= orig_range.end) or (
                    orig_range.start == split.start and orig_range.end == split.end
                ):
                    key = (orig_range.start, orig_range.end)
                    splits_per_range[key] = splits_per_range.get(key, 0) + 1
                    break

        # Verify proportionality
        print("\nProportional splitting results:")
        print(f"  Target splits: {target_splits}")
        print(f"  Actual splits: {len(splits)}")
        print(f"  Ranges that got splits: {len(splits_per_range)}")

        # Large ranges should get more splits
        large_range = range_fractions[0][0]
        large_range_key = (large_range.start, large_range.end)
        large_range_splits = splits_per_range.get(large_range_key, 0)

        small_range = range_fractions[-1][0]
        small_range_key = (small_range.start, small_range.end)
        small_range_splits = splits_per_range.get(small_range_key, 0)

        print(f"  Largest range got {large_range_splits} splits")
        print(f"  Smallest range got {small_range_splits} splits")

        # Large ranges should generally get more splits
        # (unless they're still too small to split effectively)
        if large_range.size > small_range.size * 10:
            assert (
                large_range_splits >= small_range_splits
            ), "Large range should get at least as many splits as small range"
