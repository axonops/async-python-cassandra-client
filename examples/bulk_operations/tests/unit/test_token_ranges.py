"""
Unit tests for token range operations.

What this tests:
---------------
1. Token range calculation and splitting
2. Proportional distribution of ranges
3. Handling of ring wraparound
4. Replica awareness

Why this matters:
----------------
- Correct token ranges ensure complete data coverage
- Proportional splitting ensures balanced workload
- Proper handling prevents missing or duplicate data
- Replica awareness enables data locality

Additional context:
---------------------------------
Token ranges in Cassandra use Murmur3 hash with range:
-9223372036854775808 to 9223372036854775807
"""

from unittest.mock import MagicMock, Mock

import pytest

from bulk_operations.token_utils import (
    TokenRange,
    TokenRangeSplitter,
    discover_token_ranges,
    generate_token_range_query,
)


class TestTokenRange:
    """Test TokenRange data class."""

    @pytest.mark.unit
    def test_token_range_creation(self):
        """Test creating a token range."""
        range = TokenRange(start=-9223372036854775808, end=0, replicas=["node1", "node2", "node3"])

        assert range.start == -9223372036854775808
        assert range.end == 0
        assert range.size == 9223372036854775808
        assert range.replicas == ["node1", "node2", "node3"]
        assert 0.49 < range.fraction < 0.51  # About 50% of ring

    @pytest.mark.unit
    def test_token_range_wraparound(self):
        """Test token range that wraps around the ring."""
        # Range from positive to negative (wraps around)
        range = TokenRange(start=9223372036854775800, end=-9223372036854775800, replicas=["node1"])

        # Size calculation should handle wraparound
        expected_size = 16  # Small range wrapping around
        assert range.size == expected_size
        assert range.fraction < 0.001  # Very small fraction of ring

    @pytest.mark.unit
    def test_token_range_full_ring(self):
        """Test token range covering entire ring."""
        range = TokenRange(
            start=-9223372036854775808,
            end=9223372036854775807,
            replicas=["node1", "node2", "node3"],
        )

        assert range.size == 18446744073709551615  # 2^64 - 1
        assert range.fraction == 1.0  # 100% of ring


class TestTokenRangeSplitter:
    """Test token range splitting logic."""

    @pytest.mark.unit
    def test_split_single_range_evenly(self):
        """Test splitting a single range into equal parts."""
        splitter = TokenRangeSplitter()
        original = TokenRange(start=0, end=1000, replicas=["node1", "node2"])

        splits = splitter.split_single_range(original, 4)

        assert len(splits) == 4
        # Check splits are contiguous and cover entire range
        assert splits[0].start == 0
        assert splits[0].end == 250
        assert splits[1].start == 250
        assert splits[1].end == 500
        assert splits[2].start == 500
        assert splits[2].end == 750
        assert splits[3].start == 750
        assert splits[3].end == 1000

        # All splits should have same replicas
        for split in splits:
            assert split.replicas == ["node1", "node2"]

    @pytest.mark.unit
    def test_split_proportionally(self):
        """Test proportional splitting based on range sizes."""
        splitter = TokenRangeSplitter()

        # Create ranges of different sizes
        ranges = [
            TokenRange(start=0, end=1000, replicas=["node1"]),  # 10% of total
            TokenRange(start=1000, end=9000, replicas=["node2"]),  # 80% of total
            TokenRange(start=9000, end=10000, replicas=["node3"]),  # 10% of total
        ]

        # Request 10 splits total
        splits = splitter.split_proportionally(ranges, 10)

        # Should get approximately 1, 8, 1 splits for each range
        node1_splits = [s for s in splits if s.replicas == ["node1"]]
        node2_splits = [s for s in splits if s.replicas == ["node2"]]
        node3_splits = [s for s in splits if s.replicas == ["node3"]]

        assert len(node1_splits) == 1
        assert len(node2_splits) == 8
        assert len(node3_splits) == 1
        assert len(splits) == 10

    @pytest.mark.unit
    def test_split_with_minimum_size(self):
        """Test that small ranges don't get over-split."""
        splitter = TokenRangeSplitter()

        # Very small range
        small_range = TokenRange(start=0, end=10, replicas=["node1"])

        # Request many splits
        splits = splitter.split_single_range(small_range, 100)

        # Should not create more splits than makes sense
        # (implementation should have minimum split size)
        assert len(splits) <= 10  # Assuming minimum split size of 1

    @pytest.mark.unit
    def test_cluster_by_replicas(self):
        """Test clustering ranges by their replica sets."""
        splitter = TokenRangeSplitter()

        ranges = [
            TokenRange(start=0, end=100, replicas=["node1", "node2"]),
            TokenRange(start=100, end=200, replicas=["node2", "node3"]),
            TokenRange(start=200, end=300, replicas=["node1", "node2"]),
            TokenRange(start=300, end=400, replicas=["node2", "node3"]),
        ]

        clustered = splitter.cluster_by_replicas(ranges)

        # Should have 2 clusters based on replica sets
        assert len(clustered) == 2

        # Find clusters
        cluster1 = None
        cluster2 = None
        for replicas, cluster_ranges in clustered.items():
            if set(replicas) == {"node1", "node2"}:
                cluster1 = cluster_ranges
            elif set(replicas) == {"node2", "node3"}:
                cluster2 = cluster_ranges

        assert cluster1 is not None
        assert cluster2 is not None
        assert len(cluster1) == 2
        assert len(cluster2) == 2


class TestTokenRangeDiscovery:
    """Test discovering token ranges from cluster metadata."""

    @pytest.mark.unit
    async def test_discover_token_ranges(self):
        """
        Test discovering token ranges from cluster metadata.

        What this tests:
        ---------------
        1. Extraction from Cassandra metadata
        2. All token ranges are discovered
        3. Replica information is captured
        4. Async operation works correctly

        Why this matters:
        ----------------
        - Must discover all ranges for completeness
        - Replica info enables local processing
        - Integration point with driver metadata
        - Foundation of token-aware operations
        """
        # Mock cluster metadata
        mock_session = Mock()
        mock_cluster = Mock()
        mock_metadata = Mock()
        mock_token_map = Mock()

        # Set up mock relationships
        mock_session._session = Mock()
        mock_session._session.cluster = mock_cluster
        mock_cluster.metadata = mock_metadata
        mock_metadata.token_map = mock_token_map

        # Mock tokens in the ring
        from .test_helpers import MockToken

        mock_token1 = MockToken(-9223372036854775808)
        mock_token2 = MockToken(0)
        mock_token3 = MockToken(9223372036854775807)
        mock_token_map.ring = [mock_token1, mock_token2, mock_token3]

        # Mock replicas
        mock_token_map.get_replicas = MagicMock(
            side_effect=[
                [Mock(address="127.0.0.1"), Mock(address="127.0.0.2")],
                [Mock(address="127.0.0.2"), Mock(address="127.0.0.3")],
                [Mock(address="127.0.0.3"), Mock(address="127.0.0.1")],  # For wraparound
            ]
        )

        # Discover ranges
        ranges = await discover_token_ranges(mock_session, "test_keyspace")

        assert len(ranges) == 3  # Three tokens create three ranges
        assert ranges[0].start == -9223372036854775808
        assert ranges[0].end == 0
        assert ranges[0].replicas == ["127.0.0.1", "127.0.0.2"]
        assert ranges[1].start == 0
        assert ranges[1].end == 9223372036854775807
        assert ranges[1].replicas == ["127.0.0.2", "127.0.0.3"]
        assert ranges[2].start == 9223372036854775807
        assert ranges[2].end == -9223372036854775808  # Wraparound
        assert ranges[2].replicas == ["127.0.0.3", "127.0.0.1"]


class TestTokenRangeQueryGeneration:
    """Test generating CQL queries with token ranges."""

    @pytest.mark.unit
    def test_generate_basic_token_range_query(self):
        """
        Test generating a basic token range query.

        What this tests:
        ---------------
        1. Valid CQL syntax generation
        2. Token function usage is correct
        3. Range boundaries use proper operators
        4. Fully qualified table names

        Why this matters:
        ----------------
        - Query syntax must be valid CQL
        - Token function enables range scans
        - Boundary operators prevent gaps/overlaps
        - Production queries depend on this
        """
        range = TokenRange(start=0, end=1000, replicas=["node1"])

        query = generate_token_range_query(
            keyspace="test_ks", table="test_table", partition_keys=["id"], token_range=range
        )

        expected = "SELECT * FROM test_ks.test_table " "WHERE token(id) > 0 AND token(id) <= 1000"
        assert query == expected

    @pytest.mark.unit
    def test_generate_query_with_multiple_partition_keys(self):
        """Test query generation with composite partition key."""
        range = TokenRange(start=-1000, end=1000, replicas=["node1"])

        query = generate_token_range_query(
            keyspace="test_ks",
            table="test_table",
            partition_keys=["country", "city"],
            token_range=range,
        )

        expected = (
            "SELECT * FROM test_ks.test_table "
            "WHERE token(country, city) > -1000 AND token(country, city) <= 1000"
        )
        assert query == expected

    @pytest.mark.unit
    def test_generate_query_with_column_selection(self):
        """Test query generation with specific columns."""
        range = TokenRange(start=0, end=1000, replicas=["node1"])

        query = generate_token_range_query(
            keyspace="test_ks",
            table="test_table",
            partition_keys=["id"],
            token_range=range,
            columns=["id", "name", "created_at"],
        )

        expected = (
            "SELECT id, name, created_at FROM test_ks.test_table "
            "WHERE token(id) > 0 AND token(id) <= 1000"
        )
        assert query == expected

    @pytest.mark.unit
    def test_generate_query_with_min_token(self):
        """Test query generation starting from minimum token."""
        range = TokenRange(start=-9223372036854775808, end=0, replicas=["node1"])  # Min token

        query = generate_token_range_query(
            keyspace="test_ks", table="test_table", partition_keys=["id"], token_range=range
        )

        # First range should use >= instead of >
        expected = (
            "SELECT * FROM test_ks.test_table "
            "WHERE token(id) >= -9223372036854775808 AND token(id) <= 0"
        )
        assert query == expected
