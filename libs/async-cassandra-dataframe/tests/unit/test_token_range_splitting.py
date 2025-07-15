"""
Unit tests for token range splitting functionality.

What this tests:
---------------
1. Token range can be split into N equal sub-ranges
2. Sub-ranges cover the entire original range without gaps
3. Sub-ranges don't overlap
4. Edge cases like split_factor=1, large split factors
5. Token arithmetic wrapping around the ring

Why this matters:
----------------
- Users need fine-grained control over partitioning
- Automatic calculations may not suit all data distributions
- Large token ranges may need to be split for better parallelism
- Ensures correctness of token range arithmetic
"""

import pytest

from async_cassandra_dataframe.token_ranges import MAX_TOKEN, MIN_TOKEN, TokenRange


class TestTokenRangeSplitting:
    """Test splitting individual token ranges into sub-ranges."""

    def test_split_token_range_basic(self):
        """
        Test basic splitting of a token range.

        Given: A token range covering part of the ring
        When: Split into 2 parts
        Then: Should create 2 equal sub-ranges
        """
        # Token range from -1000 to 1000
        original = TokenRange(
            start=-1000,
            end=1000,
            replicas=["node1"],
        )

        # Split into 2 parts
        sub_ranges = original.split(2)

        assert len(sub_ranges) == 2

        # First sub-range: -1000 to 0
        assert sub_ranges[0].start == -1000
        assert sub_ranges[0].end == 0
        assert sub_ranges[0].replicas == ["node1"]

        # Second sub-range: 0 to 1000
        assert sub_ranges[1].start == 0
        assert sub_ranges[1].end == 1000
        assert sub_ranges[1].replicas == ["node1"]

    def test_split_token_range_multiple(self):
        """
        Test splitting into multiple parts.

        Given: A token range
        When: Split into 4 parts
        Then: Should create 4 equal sub-ranges
        """
        original = TokenRange(
            start=0,
            end=4000,
            replicas=["node1", "node2"],
        )

        sub_ranges = original.split(4)

        assert len(sub_ranges) == 4

        # Check boundaries
        expected_boundaries = [(0, 1000), (1000, 2000), (2000, 3000), (3000, 4000)]

        for i, (start, end) in enumerate(expected_boundaries):
            assert sub_ranges[i].start == start
            assert sub_ranges[i].end == end
            assert sub_ranges[i].replicas == ["node1", "node2"]
            # Each sub-range should have 1/4 of the original fraction
            assert sub_ranges[i].fraction == pytest.approx(original.fraction / 4)

    def test_split_token_range_wrap_around(self):
        """
        Test splitting a range that wraps around the ring.

        Given: A token range from positive to negative (wraps around)
        When: Split into parts
        Then: Should handle wrap-around correctly
        """
        # Range that wraps around: from near end to near beginning
        original = TokenRange(
            start=MAX_TOKEN - 1000,
            end=MIN_TOKEN + 1000,
            replicas=["node1"],
        )

        sub_ranges = original.split(2)

        assert len(sub_ranges) == 2

        # First sub-range should go from start to MAX_TOKEN
        assert sub_ranges[0].start == MAX_TOKEN - 1000
        assert sub_ranges[0].end == MAX_TOKEN

        # Second sub-range should go from MIN_TOKEN to end
        assert sub_ranges[1].start == MIN_TOKEN
        assert sub_ranges[1].end == MIN_TOKEN + 1000

    def test_split_factor_one(self):
        """
        Test split_factor=1 returns original range.

        Given: A token range
        When: Split factor is 1
        Then: Should return list with original range
        """
        original = TokenRange(
            start=100,
            end=200,
            replicas=["node1"],
        )

        sub_ranges = original.split(1)

        assert len(sub_ranges) == 1
        assert sub_ranges[0].start == original.start
        assert sub_ranges[0].end == original.end
        assert sub_ranges[0].fraction == original.fraction
        assert sub_ranges[0].replicas == original.replicas

    def test_split_factor_validation(self):
        """
        Test invalid split factors are rejected.

        Given: A token range
        When: Invalid split factor provided
        Then: Should raise appropriate error
        """
        original = TokenRange(
            start=0,
            end=1000,
            replicas=["node1"],
        )

        # Zero or negative split factors
        with pytest.raises(ValueError, match="split_factor must be positive"):
            original.split(0)

        with pytest.raises(ValueError, match="split_factor must be positive"):
            original.split(-1)

    def test_split_small_range(self):
        """
        Test splitting a very small token range.

        Given: A token range with only a few tokens
        When: Split into more parts than tokens
        Then: Should handle gracefully
        """
        # Range with only 5 tokens
        original = TokenRange(
            start=10,
            end=15,
            replicas=["node1"],
        )

        # Try to split into 10 parts (more than available tokens)
        sub_ranges = original.split(10)

        # Should create as many ranges as possible
        # Some sub-ranges might be empty or very small
        assert len(sub_ranges) == 10

        # Verify no gaps or overlaps
        for i in range(len(sub_ranges) - 1):
            assert sub_ranges[i].end == sub_ranges[i + 1].start

    def test_split_preserves_total_fraction(self):
        """
        Test that split ranges preserve total fraction.

        Given: A token range with a specific fraction
        When: Split into N parts
        Then: Sum of sub-range fractions should equal original
        """
        original = TokenRange(
            start=1000,
            end=5000,
            replicas=["node1", "node2", "node3"],
        )

        for split_factor in [2, 3, 5, 10]:
            sub_ranges = original.split(split_factor)

            # Sum of fractions should equal original
            total_fraction = sum(sr.fraction for sr in sub_ranges)
            assert total_fraction == pytest.approx(original.fraction, rel=1e-10)

            # Each sub-range should have equal fraction
            expected_fraction = original.fraction / split_factor
            for sr in sub_ranges:
                assert sr.fraction == pytest.approx(expected_fraction, rel=1e-10)


class TestPartitionStrategyWithSplitting:
    """Test the new SPLIT partitioning strategy."""

    def test_split_strategy_basic(self):
        """
        Test SPLIT strategy with basic configuration.

        Given: Token ranges and split_factor=2
        When: Using SPLIT partitioning strategy
        Then: Each token range creates 2 partitions
        """
        from async_cassandra_dataframe.partition_strategy import (
            PartitioningStrategy,
            TokenRangeGrouper,
        )

        # Create some token ranges
        token_ranges = [
            TokenRange(start=-1000, end=0, replicas=["node1"]),
            TokenRange(start=0, end=1000, replicas=["node1"]),
            TokenRange(start=1000, end=2000, replicas=["node2"]),
            TokenRange(start=2000, end=3000, replicas=["node2"]),
        ]

        grouper = TokenRangeGrouper()
        groups = grouper.group_token_ranges(
            token_ranges,
            strategy=PartitioningStrategy.SPLIT,
            split_factor=2,
        )

        # Should have 4 ranges * 2 splits = 8 partitions
        assert len(groups) == 8

        # Each group should have exactly one sub-range
        for group in groups:
            assert len(group.token_ranges) == 1

        # Verify first original range was split correctly
        assert groups[0].token_ranges[0].start == -1000
        assert groups[0].token_ranges[0].end == -500
        assert groups[1].token_ranges[0].start == -500
        assert groups[1].token_ranges[0].end == 0

    def test_split_strategy_uneven_distribution(self):
        """
        Test SPLIT strategy with uneven token distribution.

        Given: Token ranges of different sizes
        When: Using SPLIT strategy
        Then: Each range is split equally regardless of size
        """
        from async_cassandra_dataframe.partition_strategy import (
            PartitioningStrategy,
            TokenRangeGrouper,
        )

        # Create token ranges with very different sizes
        token_ranges = [
            TokenRange(start=0, end=100, replicas=["node1"]),  # Small
            TokenRange(start=100, end=10000, replicas=["node1"]),  # Large
        ]

        grouper = TokenRangeGrouper()
        groups = grouper.group_token_ranges(
            token_ranges,
            strategy=PartitioningStrategy.SPLIT,
            split_factor=3,
        )

        # Should have 2 ranges * 3 splits = 6 partitions
        assert len(groups) == 6

        # First range splits (small range)
        # Range 0-100, size=100, split by 3: 0-33, 33-66, 66-100
        assert groups[0].token_ranges[0].start == 0
        assert groups[0].token_ranges[0].end == 33
        assert groups[1].token_ranges[0].start == 33
        assert groups[1].token_ranges[0].end == 66
        assert groups[2].token_ranges[0].start == 66
        assert groups[2].token_ranges[0].end == 100

        # Second range splits (large range)
        # Range 100-10000, size=9900, split by 3: 100-3400, 3400-6700, 6700-10000
        assert groups[3].token_ranges[0].start == 100
        assert groups[3].token_ranges[0].end == 3400
        assert groups[4].token_ranges[0].start == 3400
        assert groups[4].token_ranges[0].end == 6700
        assert groups[5].token_ranges[0].start == 6700
        assert groups[5].token_ranges[0].end == 10000

    def test_split_strategy_with_target_partition_count(self):
        """
        Test that split_factor is required for SPLIT strategy.

        Given: SPLIT strategy without split_factor
        When: Trying to group token ranges
        Then: Should raise error
        """
        from async_cassandra_dataframe.partition_strategy import (
            PartitioningStrategy,
            TokenRangeGrouper,
        )

        token_ranges = [
            TokenRange(start=0, end=1000, replicas=["node1"]),
        ]

        grouper = TokenRangeGrouper()

        # Should raise error without split_factor
        with pytest.raises(ValueError, match="SPLIT strategy requires split_factor"):
            grouper.group_token_ranges(
                token_ranges,
                strategy=PartitioningStrategy.SPLIT,
            )

    def test_split_strategy_preserves_locality(self):
        """
        Test that SPLIT strategy preserves replica information.

        Given: Token ranges with different replicas
        When: Split into sub-ranges
        Then: Sub-ranges should maintain same replica information
        """
        from async_cassandra_dataframe.partition_strategy import (
            PartitioningStrategy,
            TokenRangeGrouper,
        )

        token_ranges = [
            TokenRange(start=0, end=1000, replicas=["node1", "node2"]),
            TokenRange(start=1000, end=2000, replicas=["node2", "node3"]),
        ]

        grouper = TokenRangeGrouper()
        groups = grouper.group_token_ranges(
            token_ranges,
            strategy=PartitioningStrategy.SPLIT,
            split_factor=2,
        )

        # First range's sub-partitions should have node1, node2
        assert groups[0].primary_replica == "node1"
        assert groups[0].token_ranges[0].replicas == ["node1", "node2"]
        assert groups[1].primary_replica == "node1"
        assert groups[1].token_ranges[0].replicas == ["node1", "node2"]

        # Second range's sub-partitions should have node2, node3
        assert groups[2].primary_replica == "node2"
        assert groups[2].token_ranges[0].replicas == ["node2", "node3"]
        assert groups[3].primary_replica == "node2"
        assert groups[3].token_ranges[0].replicas == ["node2", "node3"]
