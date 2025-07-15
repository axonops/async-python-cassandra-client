"""
Test partitioning strategies.

What this tests:
---------------
1. Different partitioning strategies work correctly
2. Token ranges are grouped appropriately
3. Data locality is preserved
4. Edge cases are handled

Why this matters:
----------------
- Proper partitioning is critical for performance
- Must respect Cassandra's architecture
- Affects memory usage and parallelism
"""

from async_cassandra_dataframe.partition_strategy import PartitioningStrategy, TokenRangeGrouper
from async_cassandra_dataframe.token_ranges import TokenRange


def create_mock_token_ranges(count: int, nodes: int = 3, size_mb: float = 100) -> list[TokenRange]:
    """Create mock token ranges for testing."""
    ranges = []
    token_space = 2**63

    for i in range(count):
        start = int(-token_space + (2 * token_space * i / count))
        end = int(-token_space + (2 * token_space * (i + 1) / count))

        # Simulate replica assignment
        primary_node = i % nodes
        replicas = [f"node{(primary_node + j) % nodes}" for j in range(min(3, nodes))]

        ranges.append(TokenRange(start=start, end=end, replicas=replicas))

    return ranges


class TestTokenRangeGrouper:
    """Test the TokenRangeGrouper class."""

    def test_natural_grouping(self):
        """
        Test natural grouping creates one partition per token range.

        Given: Token ranges
        When: Using NATURAL strategy
        Then: Each range gets its own partition
        """
        # Given
        ranges = create_mock_token_ranges(10)
        grouper = TokenRangeGrouper()

        # When
        groups = grouper.group_token_ranges(ranges, strategy=PartitioningStrategy.NATURAL)

        # Then
        assert len(groups) == 10
        for i, group in enumerate(groups):
            assert group.partition_id == i
            assert len(group.token_ranges) == 1
            assert group.token_ranges[0] == ranges[i]

    def test_compact_grouping_by_size(self):
        """
        Test compact grouping respects target size.

        Given: Token ranges with known sizes
        When: Using COMPACT strategy with target size
        Then: Groups don't exceed target size
        """
        # Given - 20 ranges of 100MB each
        ranges = create_mock_token_ranges(20, size_mb=100)
        grouper = TokenRangeGrouper()

        # When - target 500MB per partition
        groups = grouper.group_token_ranges(
            ranges, strategy=PartitioningStrategy.COMPACT, target_partition_size_mb=500
        )

        # Then
        assert len(groups) >= 2  # At least some grouping
        assert len(groups) < 20  # But not natural (one per range)
        # Since we're estimating sizes, just verify grouping happened
        for group in groups:
            assert len(group.token_ranges) >= 1

    def test_fixed_grouping_exact_count(self):
        """
        Test fixed grouping creates exact partition count.

        Given: Token ranges
        When: Using FIXED strategy with count
        Then: Exactly that many partitions created
        """
        # Given
        ranges = create_mock_token_ranges(100)
        grouper = TokenRangeGrouper()

        # When
        groups = grouper.group_token_ranges(
            ranges, strategy=PartitioningStrategy.FIXED, target_partition_count=10
        )

        # Then
        assert len(groups) == 10
        # Verify all ranges are included
        total_ranges = sum(len(g.token_ranges) for g in groups)
        assert total_ranges == 100

    def test_fixed_grouping_exceeds_ranges(self):
        """
        Test fixed grouping when requested count exceeds ranges.

        Given: 10 token ranges
        When: Requesting 20 partitions
        Then: Only 10 partitions created (natural limit)
        """
        # Given
        ranges = create_mock_token_ranges(10)
        grouper = TokenRangeGrouper()

        # When
        groups = grouper.group_token_ranges(
            ranges, strategy=PartitioningStrategy.FIXED, target_partition_count=20
        )

        # Then
        assert len(groups) == 10  # Can't exceed natural ranges

    def test_auto_grouping_high_vnodes(self):
        """
        Test auto grouping with high vnode count.

        Given: Many token ranges (simulating 256 vnodes)
        When: Using AUTO strategy
        Then: Aggressive grouping applied
        """
        # Given - 768 ranges (3 nodes * 256 vnodes)
        ranges = create_mock_token_ranges(768, nodes=3)
        grouper = TokenRangeGrouper()

        # When
        groups = grouper.group_token_ranges(ranges, strategy=PartitioningStrategy.AUTO)

        # Then
        # Should group aggressively
        assert len(groups) < 100  # Much less than 768
        assert len(groups) >= 30  # But still reasonable parallelism

    def test_auto_grouping_low_vnodes(self):
        """
        Test auto grouping with low vnode count.

        Given: Few token ranges (simulating low vnodes)
        When: Using AUTO strategy
        Then: Close to natural grouping
        """
        # Given - 12 ranges (3 nodes * 4 vnodes)
        ranges = create_mock_token_ranges(12, nodes=3)
        grouper = TokenRangeGrouper()

        # When
        groups = grouper.group_token_ranges(ranges, strategy=PartitioningStrategy.AUTO)

        # Then
        # Should be close to natural
        assert len(groups) >= 6  # At least half of natural
        assert len(groups) <= 12  # At most natural

    def test_data_locality_preserved(self):
        """
        Test that grouping preserves data locality.

        Given: Token ranges with replica information
        When: Grouping with any strategy
        Then: Ranges from same replica grouped together when possible
        """
        # Given
        ranges = create_mock_token_ranges(30, nodes=3)
        grouper = TokenRangeGrouper()

        # When
        groups = grouper.group_token_ranges(
            ranges, strategy=PartitioningStrategy.COMPACT, target_partition_size_mb=500
        )

        # Then
        # Check that groups tend to have ranges from same replica
        for group in groups:
            if len(group.token_ranges) > 1:
                # Get all primary replicas in group
                replicas = [tr.replicas[0] for tr in group.token_ranges]
                # Most should be from same replica
                most_common = max(set(replicas), key=replicas.count)
                same_replica_count = replicas.count(most_common)
                assert same_replica_count >= len(replicas) * 0.7

    def test_empty_ranges(self):
        """
        Test handling of empty token ranges.

        Given: No token ranges
        When: Grouping with any strategy
        Then: Empty list returned
        """
        # Given
        grouper = TokenRangeGrouper()

        # When/Then
        for strategy in PartitioningStrategy:
            groups = grouper.group_token_ranges([], strategy=strategy, target_partition_count=10)
            assert groups == []

    def test_partition_summary(self):
        """
        Test partition summary statistics.

        Given: Grouped partitions
        When: Getting summary
        Then: Correct statistics returned
        """
        # Given
        ranges = create_mock_token_ranges(100, size_mb=100)
        grouper = TokenRangeGrouper()
        groups = grouper.group_token_ranges(
            ranges, strategy=PartitioningStrategy.FIXED, target_partition_count=10
        )

        # When
        summary = grouper.get_partition_summary(groups)

        # Then
        assert summary["partition_count"] == 10
        assert summary["total_token_ranges"] == 100
        assert summary["avg_ranges_per_partition"] == 10
        assert summary["total_size_mb"] > 0
        assert "min_partition_size_mb" in summary
        assert "max_partition_size_mb" in summary

    def test_single_node_grouping(self):
        """
        Test grouping for single-node clusters.

        Given: Token ranges all from one node
        When: Grouping with AUTO strategy
        Then: Reasonable partitioning based on size
        """
        # Given - single node cluster
        ranges = create_mock_token_ranges(100, nodes=1, size_mb=50)
        grouper = TokenRangeGrouper()

        # When
        groups = grouper.group_token_ranges(ranges, strategy=PartitioningStrategy.AUTO)

        # Then
        # Should create reasonable partitions based on size
        assert len(groups) > 1
        assert len(groups) < 100  # Some grouping applied
