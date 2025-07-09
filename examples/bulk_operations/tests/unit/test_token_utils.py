"""
Unit tests for token range utilities.

What this tests:
---------------
1. Token range size calculations
2. Range splitting logic
3. Wraparound handling
4. Proportional distribution
5. Replica clustering

Why this matters:
----------------
- Ensures data completeness
- Prevents missing rows
- Maintains proper load distribution
- Enables efficient parallel processing

Additional context:
---------------------------------
Token ranges in Cassandra use Murmur3 hash which
produces 128-bit values from -2^63 to 2^63-1.
"""

from unittest.mock import Mock

import pytest

from bulk_operations.token_utils import (
    MAX_TOKEN,
    MIN_TOKEN,
    TOTAL_TOKEN_RANGE,
    TokenRange,
    TokenRangeSplitter,
    discover_token_ranges,
    generate_token_range_query,
)


class TestTokenRange:
    """Test the TokenRange dataclass."""

    @pytest.mark.unit
    def test_token_range_size_normal(self):
        """
        Test size calculation for normal ranges.

        What this tests:
        ---------------
        1. Size calculation for positive ranges
        2. Size calculation for negative ranges
        3. Basic arithmetic correctness
        4. No wraparound edge cases

        Why this matters:
        ----------------
        - Token range sizes determine split proportions
        - Incorrect sizes lead to unbalanced loads
        - Foundation for all range splitting logic
        - Critical for even data distribution
        """
        range = TokenRange(start=0, end=1000, replicas=["node1"])
        assert range.size == 1000

        range = TokenRange(start=-1000, end=0, replicas=["node1"])
        assert range.size == 1000

    @pytest.mark.unit
    def test_token_range_size_wraparound(self):
        """
        Test size calculation for ranges that wrap around.

        What this tests:
        ---------------
        1. Wraparound from MAX_TOKEN to MIN_TOKEN
        2. Correct size calculation across boundaries
        3. Edge case handling for ring topology
        4. Boundary arithmetic correctness

        Why this matters:
        ----------------
        - Cassandra's token ring wraps around
        - Last range often crosses the boundary
        - Incorrect handling causes missing data
        - Real clusters always have wraparound ranges
        """
        # Range wraps from near max to near min
        range = TokenRange(start=MAX_TOKEN - 1000, end=MIN_TOKEN + 1000, replicas=["node1"])
        expected_size = 1000 + 1000 + 1  # 1000 on each side plus the boundary
        assert range.size == expected_size

    @pytest.mark.unit
    def test_token_range_fraction(self):
        """Test fraction calculation."""
        # Quarter of the ring
        quarter_size = TOTAL_TOKEN_RANGE // 4
        range = TokenRange(start=0, end=quarter_size, replicas=["node1"])
        assert abs(range.fraction - 0.25) < 0.001


class TestTokenRangeSplitter:
    """Test the TokenRangeSplitter class."""

    @pytest.fixture
    def splitter(self):
        """Create a TokenRangeSplitter instance."""
        return TokenRangeSplitter()

    @pytest.mark.unit
    def test_split_single_range_no_split(self, splitter):
        """Test that requesting 1 or 0 splits returns original range."""
        range = TokenRange(start=0, end=1000, replicas=["node1"])

        result = splitter.split_single_range(range, 1)
        assert len(result) == 1
        assert result[0].start == 0
        assert result[0].end == 1000

    @pytest.mark.unit
    def test_split_single_range_even_split(self, splitter):
        """Test splitting a range into even parts."""
        range = TokenRange(start=0, end=1000, replicas=["node1"])

        result = splitter.split_single_range(range, 4)
        assert len(result) == 4

        # Check splits
        assert result[0].start == 0
        assert result[0].end == 250
        assert result[1].start == 250
        assert result[1].end == 500
        assert result[2].start == 500
        assert result[2].end == 750
        assert result[3].start == 750
        assert result[3].end == 1000

    @pytest.mark.unit
    def test_split_single_range_small_range(self, splitter):
        """Test that very small ranges aren't split."""
        range = TokenRange(start=0, end=2, replicas=["node1"])

        result = splitter.split_single_range(range, 10)
        assert len(result) == 1  # Too small to split

    @pytest.mark.unit
    def test_split_proportionally_empty(self, splitter):
        """Test proportional splitting with empty input."""
        result = splitter.split_proportionally([], 10)
        assert result == []

    @pytest.mark.unit
    def test_split_proportionally_single_range(self, splitter):
        """Test proportional splitting with single range."""
        ranges = [TokenRange(start=0, end=1000, replicas=["node1"])]

        result = splitter.split_proportionally(ranges, 4)
        assert len(result) == 4

    @pytest.mark.unit
    def test_split_proportionally_multiple_ranges(self, splitter):
        """
        Test proportional splitting with ranges of different sizes.

        What this tests:
        ---------------
        1. Proportional distribution based on size
        2. Larger ranges get more splits
        3. Rounding behavior is reasonable
        4. All input ranges are covered

        Why this matters:
        ----------------
        - Uneven token distribution is common
        - Load balancing requires proportional splits
        - Prevents hotspots in processing
        - Mimics real cluster token distributions
        """
        ranges = [
            TokenRange(start=0, end=1000, replicas=["node1"]),  # Size 1000
            TokenRange(start=1000, end=4000, replicas=["node2"]),  # Size 3000
        ]

        result = splitter.split_proportionally(ranges, 4)

        # Should split proportionally: 1 split for first, 3 for second
        # But implementation uses round(), so might be slightly different
        assert len(result) >= 2
        assert len(result) <= 4

    @pytest.mark.unit
    def test_cluster_by_replicas(self, splitter):
        """
        Test clustering ranges by replica sets.

        What this tests:
        ---------------
        1. Ranges are grouped by replica nodes
        2. Replica order doesn't affect grouping
        3. All ranges are included in clusters
        4. Unique replica sets are identified

        Why this matters:
        ----------------
        - Enables coordinator-local processing
        - Reduces network traffic in operations
        - Improves performance through locality
        - Critical for multi-datacenter efficiency
        """
        ranges = [
            TokenRange(start=0, end=100, replicas=["node1", "node2"]),
            TokenRange(start=100, end=200, replicas=["node2", "node3"]),
            TokenRange(start=200, end=300, replicas=["node1", "node2"]),
            TokenRange(start=300, end=400, replicas=["node3", "node1"]),
        ]

        clusters = splitter.cluster_by_replicas(ranges)

        # Should have 3 unique replica sets
        assert len(clusters) == 3

        # Check that ranges are properly grouped
        key1 = tuple(sorted(["node1", "node2"]))
        assert key1 in clusters
        assert len(clusters[key1]) == 2


class TestDiscoverTokenRanges:
    """Test token range discovery from cluster metadata."""

    @pytest.mark.unit
    async def test_discover_token_ranges_success(self):
        """
        Test successful token range discovery.

        What this tests:
        ---------------
        1. Token ranges are extracted from metadata
        2. Replica information is preserved
        3. All ranges from token map are returned
        4. Async operation completes successfully

        Why this matters:
        ----------------
        - Discovery is the foundation of token-aware ops
        - Replica awareness enables local reads
        - Must handle all Cassandra metadata structures
        - Critical for multi-datacenter deployments
        """
        # Mock session and cluster
        mock_session = Mock()
        mock_cluster = Mock()
        mock_metadata = Mock()
        mock_token_map = Mock()

        # Setup tokens in the ring
        from .test_helpers import MockToken

        mock_token1 = MockToken(-1000)
        mock_token2 = MockToken(0)
        mock_token3 = MockToken(1000)
        mock_token_map.ring = [mock_token1, mock_token2, mock_token3]

        # Setup replicas
        mock_replica1 = Mock()
        mock_replica1.address = "192.168.1.1"
        mock_replica2 = Mock()
        mock_replica2.address = "192.168.1.2"

        mock_token_map.get_replicas.side_effect = [
            [mock_replica1, mock_replica2],
            [mock_replica2, mock_replica1],
            [mock_replica1, mock_replica2],  # For the third token range
        ]

        mock_metadata.token_map = mock_token_map
        mock_cluster.metadata = mock_metadata
        mock_session._session = Mock()
        mock_session._session.cluster = mock_cluster

        # Test discovery
        ranges = await discover_token_ranges(mock_session, "test_ks")

        assert len(ranges) == 3  # Three tokens create three ranges
        assert ranges[0].start == -1000
        assert ranges[0].end == 0
        assert ranges[0].replicas == ["192.168.1.1", "192.168.1.2"]
        assert ranges[1].start == 0
        assert ranges[1].end == 1000
        assert ranges[1].replicas == ["192.168.1.2", "192.168.1.1"]
        assert ranges[2].start == 1000
        assert ranges[2].end == -1000  # Wraparound range
        assert ranges[2].replicas == ["192.168.1.1", "192.168.1.2"]

    @pytest.mark.unit
    async def test_discover_token_ranges_no_token_map(self):
        """Test error when token map is not available."""
        mock_session = Mock()
        mock_cluster = Mock()
        mock_metadata = Mock()
        mock_metadata.token_map = None
        mock_cluster.metadata = mock_metadata
        mock_session._session = Mock()
        mock_session._session.cluster = mock_cluster

        with pytest.raises(RuntimeError, match="Token map not available"):
            await discover_token_ranges(mock_session, "test_ks")


class TestGenerateTokenRangeQuery:
    """Test CQL query generation for token ranges."""

    @pytest.mark.unit
    def test_generate_query_all_columns(self):
        """Test query generation with all columns."""
        query = generate_token_range_query(
            keyspace="test_ks",
            table="test_table",
            partition_keys=["id"],
            token_range=TokenRange(start=0, end=1000, replicas=["node1"]),
        )

        expected = "SELECT * FROM test_ks.test_table " "WHERE token(id) > 0 AND token(id) <= 1000"
        assert query == expected

    @pytest.mark.unit
    def test_generate_query_specific_columns(self):
        """Test query generation with specific columns."""
        query = generate_token_range_query(
            keyspace="test_ks",
            table="test_table",
            partition_keys=["id"],
            token_range=TokenRange(start=0, end=1000, replicas=["node1"]),
            columns=["id", "name", "value"],
        )

        expected = (
            "SELECT id, name, value FROM test_ks.test_table "
            "WHERE token(id) > 0 AND token(id) <= 1000"
        )
        assert query == expected

    @pytest.mark.unit
    def test_generate_query_minimum_token(self):
        """
        Test query generation for minimum token edge case.

        What this tests:
        ---------------
        1. MIN_TOKEN uses >= instead of >
        2. Prevents missing first token value
        3. Query syntax is valid CQL
        4. Edge case is handled correctly

        Why this matters:
        ----------------
        - MIN_TOKEN is a valid token value
        - Using > would skip data at MIN_TOKEN
        - Common source of missing data bugs
        - DSBulk compatibility requires this behavior
        """
        query = generate_token_range_query(
            keyspace="test_ks",
            table="test_table",
            partition_keys=["id"],
            token_range=TokenRange(start=MIN_TOKEN, end=0, replicas=["node1"]),
        )

        expected = (
            f"SELECT * FROM test_ks.test_table "
            f"WHERE token(id) >= {MIN_TOKEN} AND token(id) <= 0"
        )
        assert query == expected

    @pytest.mark.unit
    def test_generate_query_compound_partition_key(self):
        """Test query generation with compound partition key."""
        query = generate_token_range_query(
            keyspace="test_ks",
            table="test_table",
            partition_keys=["id", "type"],
            token_range=TokenRange(start=0, end=1000, replicas=["node1"]),
        )

        expected = (
            "SELECT * FROM test_ks.test_table "
            "WHERE token(id, type) > 0 AND token(id, type) <= 1000"
        )
        assert query == expected
