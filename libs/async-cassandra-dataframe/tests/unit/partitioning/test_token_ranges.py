"""
Unit tests for token range utilities.

What this tests:
---------------
1. Token range size calculations
2. Wraparound range handling
3. Range splitting logic
4. Token boundary validation
5. Query generation

Why this matters:
----------------
- Correct token ranges ensure complete data coverage
- Proper splitting enables efficient parallel processing
- Wraparound handling prevents data loss
"""

from async_cassandra_dataframe.token_ranges import (
    MAX_TOKEN,
    MIN_TOKEN,
    TOTAL_TOKEN_RANGE,
    TokenRange,
    TokenRangeSplitter,
    generate_token_range_query,
    handle_wraparound_ranges,
    split_proportionally,
)


class TestTokenRange:
    """Test TokenRange class functionality."""

    def test_token_range_creation(self):
        """Test creating a token range."""
        tr = TokenRange(start=0, end=1000, replicas=["node1", "node2"])

        assert tr.start == 0
        assert tr.end == 1000
        assert tr.replicas == ["node1", "node2"]

    def test_token_range_size_normal(self):
        """Test size calculation for normal ranges."""
        tr = TokenRange(start=0, end=1000, replicas=[])
        assert tr.size == 1000

        tr2 = TokenRange(start=-1000, end=1000, replicas=[])
        assert tr2.size == 2000

        tr3 = TokenRange(start=MIN_TOKEN, end=0, replicas=[])
        assert tr3.size == -MIN_TOKEN

    def test_token_range_size_wraparound(self):
        """Test size calculation for wraparound ranges."""
        # Range that wraps from near MAX_TOKEN to near MIN_TOKEN
        tr = TokenRange(start=MAX_TOKEN - 1000, end=MIN_TOKEN + 1000, replicas=[])

        # Size should be small (just the wrapped portion)
        expected_size = 1000 + 1000 + 1  # 1000 tokens on each side plus the boundary
        assert tr.size == expected_size

    def test_token_range_fraction(self):
        """Test fraction calculation."""
        # Half the ring
        half_ring_size = TOTAL_TOKEN_RANGE // 2
        tr = TokenRange(start=MIN_TOKEN, end=MIN_TOKEN + half_ring_size, replicas=[])

        # Should be approximately 0.5
        assert 0.45 < tr.fraction < 0.55  # Allow for rounding

        # Full ring
        tr_full = TokenRange(start=MIN_TOKEN, end=MAX_TOKEN, replicas=[])
        assert tr_full.fraction > 0.99  # Close to 1.0

    def test_is_wraparound(self):
        """Test wraparound detection."""
        # Normal range
        tr = TokenRange(start=0, end=1000, replicas=[])
        assert not tr.is_wraparound

        # Wraparound range
        tr_wrap = TokenRange(start=1000, end=0, replicas=[])
        assert tr_wrap.is_wraparound

    def test_contains_token(self):
        """Test token containment check."""
        # Normal range
        tr = TokenRange(start=0, end=1000, replicas=[])
        assert tr.contains_token(500)
        assert tr.contains_token(0)
        assert tr.contains_token(1000)
        assert not tr.contains_token(-1)
        assert not tr.contains_token(1001)

        # Wraparound range
        tr_wrap = TokenRange(start=MAX_TOKEN - 1000, end=MIN_TOKEN + 1000, replicas=[])
        assert tr_wrap.contains_token(MAX_TOKEN - 500)  # In start portion
        assert tr_wrap.contains_token(MIN_TOKEN + 500)  # In end portion
        assert not tr_wrap.contains_token(0)  # In middle, not included

    def test_boundary_tokens(self):
        """Test that MIN_TOKEN and MAX_TOKEN are correct."""
        assert MIN_TOKEN == -(2**63)
        assert MAX_TOKEN == 2**63 - 1
        assert TOTAL_TOKEN_RANGE == 2**64 - 1


class TestTokenRangeSplitting:
    """Test token range splitting functionality."""

    def test_split_single_range_basic(self):
        """Test basic token range splitting."""
        splitter = TokenRangeSplitter()
        tr = TokenRange(start=0, end=1000, replicas=["node1"])

        # Split into 2 ranges
        splits = splitter.split_single_range(tr, split_count=2)

        assert len(splits) == 2
        # First split
        assert splits[0].start == 0
        assert splits[0].end == 500
        assert splits[0].replicas == ["node1"]

        # Second split
        assert splits[1].start == 500
        assert splits[1].end == 1000
        assert splits[1].replicas == ["node1"]

    def test_split_single_range_multiple(self):
        """Test splitting into multiple ranges."""
        splitter = TokenRangeSplitter()
        tr = TokenRange(start=-1000, end=1000, replicas=["node1", "node2"])

        # Split into 4 ranges
        splits = splitter.split_single_range(tr, split_count=4)

        assert len(splits) == 4

        # Verify ranges are contiguous
        for i in range(len(splits) - 1):
            assert splits[i].end == splits[i + 1].start

        # Verify first and last match original
        assert splits[0].start == -1000
        assert splits[-1].end == 1000

        # All should have same replicas
        for split in splits:
            assert split.replicas == ["node1", "node2"]

    def test_split_single_range_no_split(self):
        """Test splitting into 1 range (no split)."""
        splitter = TokenRangeSplitter()
        tr = TokenRange(start=100, end=200, replicas=["node1"])

        splits = splitter.split_single_range(tr, split_count=1)

        assert len(splits) == 1
        assert splits[0].start == 100
        assert splits[0].end == 200

    def test_split_small_range(self):
        """Test splitting a very small range."""
        splitter = TokenRangeSplitter()
        tr = TokenRange(start=0, end=3, replicas=["node1"])

        # Try to split into more pieces than tokens
        splits = splitter.split_single_range(tr, split_count=10)

        # Should return original if too small to split
        assert len(splits) == 1
        assert splits[0].start == 0
        assert splits[0].end == 3

    def test_split_wraparound_range(self):
        """Test splitting a wraparound range."""
        splitter = TokenRangeSplitter()
        # Range that wraps around
        tr = TokenRange(start=MAX_TOKEN - 1000, end=MIN_TOKEN + 1000, replicas=["node1"])

        splits = splitter.split_single_range(tr, split_count=2)

        # Should handle wraparound by splitting into non-wraparound parts first
        assert len(splits) >= 2  # May split into more due to wraparound handling


class TestProportionalSplitting:
    """Test proportional splitting functionality."""

    def test_split_proportionally_basic(self):
        """Test basic proportional splitting."""
        # Create ranges of different sizes
        ranges = [
            TokenRange(start=0, end=1000, replicas=["node1"]),  # Size 1000
            TokenRange(start=1000, end=3000, replicas=["node2"]),  # Size 2000
        ]

        # Split into 6 total splits
        splits = split_proportionally(ranges, target_splits=6)

        # Should have approximately 6 splits total
        assert 5 <= len(splits) <= 7  # Allow some variance

        # Larger range should get more splits
        range1_splits = [s for s in splits if s.start >= 0 and s.end <= 1000]
        range2_splits = [s for s in splits if s.start >= 1000 and s.end <= 3000]

        # Range 2 is twice as large, should get approximately twice as many splits
        assert len(range2_splits) >= len(range1_splits)

    def test_split_proportionally_empty(self):
        """Test splitting empty range list."""
        result = split_proportionally([], target_splits=10)
        assert result == []

    def test_split_proportionally_single(self):
        """Test splitting single range."""
        ranges = [TokenRange(start=0, end=1000, replicas=["node1"])]

        splits = split_proportionally(ranges, target_splits=4)

        assert len(splits) == 4
        assert all(s.replicas == ["node1"] for s in splits)


class TestWraparoundHandling:
    """Test wraparound range handling."""

    def test_handle_wraparound_ranges(self):
        """Test handling of wraparound ranges."""
        # Mix of normal and wraparound ranges
        ranges = [
            TokenRange(start=0, end=1000, replicas=["node1"]),  # Normal
            TokenRange(
                start=MAX_TOKEN - 1000, end=MIN_TOKEN + 1000, replicas=["node2"]
            ),  # Wraparound
        ]

        result = handle_wraparound_ranges(ranges)

        # Should have 3 ranges: 1 normal + 2 from split wraparound
        assert len(result) == 3

        # First should be unchanged
        assert result[0] == ranges[0]

        # Wraparound should be split into two
        wraparound_parts = result[1:]
        assert len(wraparound_parts) == 2

        # Check the split parts
        assert wraparound_parts[0].start == MAX_TOKEN - 1000
        assert wraparound_parts[0].end == MAX_TOKEN
        assert wraparound_parts[0].replicas == ["node2"]

        assert wraparound_parts[1].start == MIN_TOKEN
        assert wraparound_parts[1].end == MIN_TOKEN + 1000
        assert wraparound_parts[1].replicas == ["node2"]

    def test_handle_no_wraparound(self):
        """Test handling when no wraparound ranges."""
        ranges = [
            TokenRange(start=0, end=1000, replicas=["node1"]),
            TokenRange(start=1000, end=2000, replicas=["node2"]),
        ]

        result = handle_wraparound_ranges(ranges)

        # Should be unchanged
        assert result == ranges


class TestQueryGeneration:
    """Test CQL query generation for token ranges."""

    def test_generate_token_range_query_basic(self):
        """Test basic query generation."""
        tr = TokenRange(start=0, end=1000, replicas=[])

        query = generate_token_range_query(
            keyspace="test_ks",
            table="test_table",
            partition_keys=["id"],
            token_range=tr,
        )

        assert "SELECT * FROM test_ks.test_table" in query
        assert "WHERE token(id) > 0 AND token(id) <= 1000" in query

    def test_generate_token_range_query_min_token(self):
        """Test query generation for minimum token boundary."""
        tr = TokenRange(start=MIN_TOKEN, end=0, replicas=[])

        query = generate_token_range_query(
            keyspace="test_ks",
            table="test_table",
            partition_keys=["id"],
            token_range=tr,
        )

        # Should use >= for MIN_TOKEN to include it
        assert f"token(id) >= {MIN_TOKEN}" in query
        assert "token(id) <= 0" in query

    def test_generate_token_range_query_with_columns(self):
        """Test query with specific columns."""
        tr = TokenRange(start=0, end=1000, replicas=[])

        query = generate_token_range_query(
            keyspace="test_ks",
            table="test_table",
            partition_keys=["id"],
            token_range=tr,
            columns=["id", "name", "value"],
        )

        assert "SELECT id, name, value FROM" in query

    def test_generate_token_range_query_with_writetime(self):
        """Test query with writetime columns."""
        tr = TokenRange(start=0, end=1000, replicas=[])

        query = generate_token_range_query(
            keyspace="test_ks",
            table="test_table",
            partition_keys=["id"],
            token_range=tr,
            columns=["id", "name"],
            writetime_columns=["name"],
        )

        assert "id, name, WRITETIME(name) AS name_writetime" in query

    def test_generate_token_range_query_composite_partition_key(self):
        """Test query with composite partition key."""
        tr = TokenRange(start=0, end=1000, replicas=[])

        query = generate_token_range_query(
            keyspace="test_ks",
            table="test_table",
            partition_keys=["user_id", "date"],
            token_range=tr,
        )

        assert "token(user_id, date)" in query
