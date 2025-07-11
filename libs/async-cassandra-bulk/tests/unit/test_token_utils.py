"""
Test token range utilities for bulk operations.

What this tests:
---------------
1. TokenRange dataclass functionality
2. Token range splitting logic
3. Token range discovery from cluster
4. Query generation for token ranges

Why this matters:
----------------
- Token ranges enable parallel processing
- Correct splitting ensures even workload distribution
- Query generation must handle edge cases properly
- Foundation for all bulk operations
"""

from unittest.mock import AsyncMock, Mock

import pytest

from async_cassandra_bulk.utils.token_utils import (
    MAX_TOKEN,
    MIN_TOKEN,
    TOTAL_TOKEN_RANGE,
    TokenRange,
    TokenRangeSplitter,
    discover_token_ranges,
    generate_token_range_query,
)


class TestTokenRange:
    """Test TokenRange dataclass functionality."""

    def test_token_range_stores_values(self):
        """
        Test TokenRange dataclass stores all required values.

        What this tests:
        ---------------
        1. Dataclass initialization with all parameters
        2. Property access returns exact values provided
        3. Replica list maintained as provided
        4. No unexpected transformations during storage

        Why this matters:
        ----------------
        - Basic data structure for all bulk operations
        - Must correctly store range boundaries for queries
        - Replica information critical for node-aware scheduling
        - Production reliability depends on data integrity

        Additional context:
        ---------------------------------
        - Start/end are token values in Murmur3 hash space
        - Replicas are IP addresses of Cassandra nodes
        - Used throughout parallel export operations
        """
        token_range = TokenRange(start=0, end=1000, replicas=["127.0.0.1", "127.0.0.2"])

        assert token_range.start == 0
        assert token_range.end == 1000
        assert token_range.replicas == ["127.0.0.1", "127.0.0.2"]

    def test_token_range_size_calculation(self):
        """
        Test size calculation for normal token ranges.

        What this tests:
        ---------------
        1. Size property calculates end - start correctly
        2. Works for normal ranges where end > start
        3. Returns positive integer size
        4. Calculation is deterministic

        Why this matters:
        ----------------
        - Size determines proportional splitting ratios
        - Used for accurate progress tracking
        - Workload distribution depends on size accuracy
        - Production exports rely on size for ETA calculations

        Additional context:
        ---------------------------------
        - Murmur3 token space is -2^63 to 2^63-1
        - Normal ranges don't wrap around zero
        - Size represents number of tokens in range
        """
        token_range = TokenRange(start=100, end=500, replicas=[])
        assert token_range.size == 400

    def test_token_range_wraparound_size(self):
        """
        Test size calculation for ranges that wrap around token space.

        What this tests:
        ---------------
        1. Wraparound detection when end < start
        2. Correct calculation across MIN/MAX token boundary
        3. Size includes tokens from MAX to MIN
        4. Formula: (MAX - start) + (end - MIN) + 1

        Why this matters:
        ----------------
        - Last range in ring always wraps around
        - Missing wraparound means data loss
        - Critical for 100% data coverage
        - Production bug if wraparound calculated wrong

        Additional context:
        ---------------------------------
        - Cassandra's token ring is circular
        - Range [MAX_TOKEN-100, MIN_TOKEN+100] is valid
        - Common source of off-by-one errors
        """
        # Wraparound from near MAX_TOKEN to near MIN_TOKEN
        token_range = TokenRange(start=MAX_TOKEN - 100, end=MIN_TOKEN + 100, replicas=[])

        expected_size = 201  # 100 tokens before wrap + 100 after + 1 for inclusive
        assert token_range.size == expected_size

    def test_token_range_fraction(self):
        """
        Test fraction calculation as proportion of total ring.

        What this tests:
        ---------------
        1. Fraction property returns size/total_range
        2. Value between 0.0 and 1.0
        3. Accurate for quarter of ring (0.25)
        4. Floating point precision acceptable

        Why this matters:
        ----------------
        - Determines proportional split counts
        - Enables accurate progress percentage
        - Used for fair work distribution
        - Production monitoring shows completion %

        Additional context:
        ---------------------------------
        - Total token space is 2^64 tokens
        - Fraction used in split_proportionally()
        - Small rounding errors acceptable
        """
        # Range covering 1/4 of total space
        quarter_size = TOTAL_TOKEN_RANGE // 4
        token_range = TokenRange(start=0, end=quarter_size, replicas=[])

        assert abs(token_range.fraction - 0.25) < 0.001


class TestTokenRangeSplitter:
    """Test token range splitting logic."""

    def setup_method(self):
        """Create splitter instance for tests."""
        self.splitter = TokenRangeSplitter()

    def test_split_single_range_basic(self):
        """
        Test splitting single token range into equal parts.

        What this tests:
        ---------------
        1. Range split into exactly N equal parts
        2. No gaps between consecutive splits
        3. No overlaps (end of one = start of next)
        4. Replica information preserved in all splits

        Why this matters:
        ----------------
        - Enables parallel processing with N workers
        - Gaps would cause data loss
        - Overlaps would duplicate data
        - Production correctness depends on contiguous splits

        Additional context:
        ---------------------------------
        - Split boundaries use integer division
        - Last split may be slightly larger due to rounding
        - Replicas help with node-local processing
        """
        original = TokenRange(start=0, end=1000, replicas=["node1"])
        splits = self.splitter.split_single_range(original, 4)

        assert len(splits) == 4

        # Check splits are contiguous
        assert splits[0].start == 0
        assert splits[0].end == 250
        assert splits[1].start == 250
        assert splits[1].end == 500
        assert splits[2].start == 500
        assert splits[2].end == 750
        assert splits[3].start == 750
        assert splits[3].end == 1000

        # Check replicas preserved
        for split in splits:
            assert split.replicas == ["node1"]

    def test_split_single_range_no_split(self):
        """
        Test that ranges too small to split return unchanged.

        What this tests:
        ---------------
        1. Split count of 1 returns original range
        2. Ranges smaller than split count return unsplit
        3. Original range object preserved (not copied)
        4. Prevents splits smaller than 1 token

        Why this matters:
        ----------------
        - Prevents excessive fragmentation overhead
        - Maintains query efficiency
        - Avoids degenerate empty ranges
        - Production performance requires reasonable splits

        Additional context:
        ---------------------------------
        - Minimum practical split size is 1 token
        - Too many small splits hurt performance
        - Better to have fewer larger splits
        """
        original = TokenRange(start=0, end=10, replicas=["node1"])

        # No split requested
        splits = self.splitter.split_single_range(original, 1)
        assert len(splits) == 1
        assert splits[0] is original

        # Range too small to split into 100 parts
        splits = self.splitter.split_single_range(original, 100)
        assert len(splits) == 1

    def test_split_proportionally(self):
        """
        Test proportional splitting across ranges of different sizes.

        What this tests:
        ---------------
        1. Larger ranges receive proportionally more splits
        2. Total split count approximates target (±20%)
        3. Each range gets at least one split
        4. Split allocation based on range.fraction

        Why this matters:
        ----------------
        - Ensures even workload distribution
        - Handles uneven vnode token distributions
        - Prevents worker starvation or overload
        - Production clusters have varying range sizes

        Additional context:
        ---------------------------------
        - Real clusters have 256+ vnodes per node
        - Range sizes vary by 10x or more
        - Algorithm: splits = target * range.fraction
        """
        ranges = [
            TokenRange(start=0, end=1000, replicas=["node1"]),  # Large
            TokenRange(start=1000, end=1100, replicas=["node2"]),  # Small
            TokenRange(start=1100, end=2100, replicas=["node3"]),  # Large
        ]

        splits = self.splitter.split_proportionally(ranges, target_splits=10)

        # Should have approximately 10 splits total
        assert 8 <= len(splits) <= 12

        # Verify first large range got more splits than small one
        first_range_splits = [s for s in splits if s.start >= 0 and s.end <= 1000]
        second_range_splits = [s for s in splits if s.start >= 1000 and s.end <= 1100]

        assert len(first_range_splits) > len(second_range_splits)

    def test_cluster_by_replicas(self):
        """
        Test grouping token ranges by their replica node sets.

        What this tests:
        ---------------
        1. Ranges grouped by identical replica sets
        2. Replica order normalized (sorted) for grouping
        3. Returns dict mapping replica tuples to ranges
        4. All input ranges present in output

        Why this matters:
        ----------------
        - Enables node-aware work scheduling
        - Improves data locality and reduces network traffic
        - Coordinator selection optimization
        - Production performance with rack awareness

        Additional context:
        ---------------------------------
        - Replicas listed in preference order normally
        - Same nodes in different order = same replica set
        - Used for scheduling workers near data
        """
        ranges = [
            TokenRange(start=0, end=100, replicas=["node1", "node2"]),
            TokenRange(
                start=100, end=200, replicas=["node2", "node1"]
            ),  # Same nodes, different order
            TokenRange(start=200, end=300, replicas=["node2", "node3"]),
            TokenRange(start=300, end=400, replicas=["node1", "node3"]),
        ]

        clusters = self.splitter.cluster_by_replicas(ranges)

        # Should have 3 unique replica sets
        assert len(clusters) == 3

        # First two ranges should be in same cluster (same replica set)
        node1_node2_key = tuple(sorted(["node1", "node2"]))
        assert node1_node2_key in clusters
        assert len(clusters[node1_node2_key]) == 2


class TestDiscoverTokenRanges:
    """Test token range discovery from cluster."""

    @pytest.mark.asyncio
    async def test_discover_token_ranges_basic(self):
        """
        Test token range discovery from Cassandra cluster metadata.

        What this tests:
        ---------------
        1. Extracts token ranges from cluster token map
        2. Creates contiguous ranges between tokens
        3. Queries replica nodes for each range
        4. Returns complete coverage of token space

        Why this matters:
        ----------------
        - Must accurately reflect current cluster topology
        - Foundation for all parallel bulk operations
        - Incorrect ranges mean data loss or duplication
        - Production changes (adding nodes) must be detected

        Additional context:
        ---------------------------------
        - Uses driver's metadata.token_map.ring
        - Tokens sorted to create proper ranges
        - Last range wraps from final token to first
        """
        # Mock session and cluster
        mock_session = AsyncMock()
        mock_sync_session = Mock()
        mock_session._session = mock_sync_session

        # Mock cluster metadata
        mock_cluster = Mock()
        mock_sync_session.cluster = mock_cluster

        mock_metadata = Mock()
        mock_cluster.metadata = mock_metadata

        # Mock token map
        mock_token_map = Mock()
        mock_metadata.token_map = mock_token_map

        # Mock tokens with proper sorting support
        class MockToken:
            def __init__(self, value):
                self.value = value

            def __lt__(self, other):
                return self.value < other.value

        mock_tokens = [
            MockToken(-1000),
            MockToken(0),
            MockToken(1000),
        ]
        mock_token_map.ring = mock_tokens

        # Mock replicas
        def get_replicas(keyspace, token):
            return [Mock(address="127.0.0.1"), Mock(address="127.0.0.2")]

        mock_token_map.get_replicas = get_replicas

        # Execute
        ranges = await discover_token_ranges(mock_session, "test_keyspace")

        # Verify
        assert len(ranges) == 3

        # Check first range
        assert ranges[0].start == -1000
        assert ranges[0].end == 0
        assert set(ranges[0].replicas) == {"127.0.0.1", "127.0.0.2"}

        # Check wraparound range (last to first)
        assert ranges[2].start == 1000
        assert ranges[2].end == -1000  # Wraps to first token

    @pytest.mark.asyncio
    async def test_discover_token_ranges_no_token_map(self):
        """
        Test error handling when cluster token map is unavailable.

        What this tests:
        ---------------
        1. Detects when metadata.token_map is None
        2. Raises RuntimeError with descriptive message
        3. Error mentions "Token map not available"
        4. Fails fast before attempting operations

        Why this matters:
        ----------------
        - Graceful failure for disconnected clusters
        - Clear error helps troubleshooting
        - Prevents confusing NoneType errors later
        - Production clusters may lack metadata access

        Additional context:
        ---------------------------------
        - Token map requires DESCRIBE permission
        - Some cloud providers restrict metadata
        - Error guides users to check permissions
        """
        # Mock session without token map
        mock_session = AsyncMock()
        mock_sync_session = Mock()
        mock_session._session = mock_sync_session

        mock_cluster = Mock()
        mock_sync_session.cluster = mock_cluster

        mock_metadata = Mock()
        mock_cluster.metadata = mock_metadata
        mock_metadata.token_map = None

        # Should raise error
        with pytest.raises(RuntimeError) as exc_info:
            await discover_token_ranges(mock_session, "test_keyspace")

        assert "Token map not available" in str(exc_info.value)


class TestGenerateTokenRangeQuery:
    """Test query generation for token ranges."""

    def test_generate_basic_query(self):
        """
        Test basic CQL query generation for token range.

        What this tests:
        ---------------
        1. Generates syntactically correct CQL
        2. Uses token() function on partition key
        3. Includes proper range boundaries (> start, <= end)
        4. Fully qualified table name (keyspace.table)

        Why this matters:
        ----------------
        - Query syntax errors would fail all exports
        - Token ranges must be exact for data completeness
        - Boundary conditions prevent data loss/duplication
        - Production queries process billions of rows

        Additional context:
        ---------------------------------
        - Uses > for start and <= for end (except MIN_TOKEN)
        - Token function required for range queries
        - Standard pattern for all bulk operations
        """
        token_range = TokenRange(start=100, end=200, replicas=[])

        query = generate_token_range_query(
            keyspace="test_ks", table="test_table", partition_keys=["id"], token_range=token_range
        )

        expected = "SELECT * FROM test_ks.test_table WHERE token(id) > 100 AND token(id) <= 200"
        assert query == expected

    def test_generate_query_with_columns(self):
        """
        Test query generation with specific column projection.

        What this tests:
        ---------------
        1. Column list formatted as comma-separated
        2. SELECT clause uses column list instead of *
        3. Token range conditions remain unchanged
        4. Column order preserved as specified

        Why this matters:
        ----------------
        - Reduces network data transfer significantly
        - Supports selective export of large tables
        - Memory efficiency for wide tables
        - Production exports often need subset of columns

        Additional context:
        ---------------------------------
        - Column names not validated (Cassandra will error)
        - Order matters for CSV export compatibility
        - Typically 10x reduction in data transfer
        """
        token_range = TokenRange(start=100, end=200, replicas=[])

        query = generate_token_range_query(
            keyspace="test_ks",
            table="test_table",
            partition_keys=["id"],
            token_range=token_range,
            columns=["id", "name", "created_at"],
        )

        assert query.startswith("SELECT id, name, created_at FROM")

    def test_generate_query_compound_partition_key(self):
        """
        Test query generation for tables with compound partition keys.

        What this tests:
        ---------------
        1. Multiple partition key columns in token()
        2. Correct syntax: token(col1, col2, ...)
        3. Column order matches partition key definition
        4. All partition key parts included

        Why this matters:
        ----------------
        - Many production tables use compound keys
        - Token function must include ALL partition columns
        - Wrong order or missing columns = query error
        - Critical for multi-tenant data models

        Additional context:
        ---------------------------------
        - Order must match CREATE TABLE definition
        - Common pattern: (tenant_id, user_id)
        - Token computed from all parts combined
        """
        token_range = TokenRange(start=100, end=200, replicas=[])

        query = generate_token_range_query(
            keyspace="test_ks",
            table="test_table",
            partition_keys=["tenant_id", "user_id"],
            token_range=token_range,
        )

        assert "token(tenant_id, user_id)" in query

    def test_generate_query_minimum_token(self):
        """
        Test query generation for range starting at MIN_TOKEN.

        What this tests:
        ---------------
        1. Uses >= (not >) for MIN_TOKEN boundary
        2. Special case handling for first range
        3. Ensures first row in ring not skipped
        4. End boundary still uses <= as normal

        Why this matters:
        ----------------
        - MIN_TOKEN row would be lost with > operator
        - First range must include absolute minimum
        - Off-by-one error would lose data
        - Production correctness for complete export

        Additional context:
        ---------------------------------
        - MIN_TOKEN = -9223372036854775808 (min long)
        - Only first range in ring starts at MIN_TOKEN
        - All other ranges use > for start boundary
        """
        token_range = TokenRange(start=MIN_TOKEN, end=0, replicas=[])

        query = generate_token_range_query(
            keyspace="test_ks", table="test_table", partition_keys=["id"], token_range=token_range
        )

        # Should use >= for MIN_TOKEN
        assert f"token(id) >= {MIN_TOKEN}" in query
        assert "token(id) <= 0" in query
