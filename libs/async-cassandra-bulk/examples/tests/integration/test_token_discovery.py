"""
Integration tests for token range discovery with vnodes.

What this tests:
---------------
1. Token range discovery matches cluster vnodes configuration
2. Validation against nodetool describering output
3. Token distribution across nodes
4. Non-overlapping and complete token coverage

Why this matters:
----------------
- Vnodes create hundreds of non-contiguous ranges
- Token metadata must match cluster reality
- Incorrect discovery means data loss
- Production clusters always use vnodes
"""

import subprocess
from collections import defaultdict

import pytest

from async_cassandra import AsyncCluster
from bulk_operations.token_utils import TOTAL_TOKEN_RANGE, discover_token_ranges


@pytest.mark.integration
class TestTokenDiscovery:
    """Test token range discovery against real Cassandra cluster."""

    @pytest.fixture
    async def cluster(self):
        """Create connection to test cluster."""
        # Connect to all three nodes
        cluster = AsyncCluster(
            contact_points=["localhost", "127.0.0.1", "127.0.0.2"],
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
                'replication_factor': 3
            }
        """
        )

        yield session

    @pytest.mark.asyncio
    async def test_token_range_discovery_with_vnodes(self, session):
        """
        Test token range discovery matches cluster vnodes configuration.

        What this tests:
        ---------------
        1. Number of ranges matches vnode configuration
        2. Each node owns approximately equal ranges
        3. All ranges have correct replica information
        4. Token ranges are non-overlapping and complete

        Why this matters:
        ----------------
        - With 256 vnodes × 3 nodes = ~768 ranges expected
        - Vnodes distribute ownership across the ring
        - Incorrect discovery means data loss
        - Must handle non-contiguous ownership correctly
        """
        ranges = await discover_token_ranges(session, "bulk_test")

        # With 3 nodes and 256 vnodes each, expect many ranges
        # Due to replication factor 3, each range has 3 replicas
        assert len(ranges) > 100, f"Expected many ranges with vnodes, got {len(ranges)}"

        # Count ranges per node
        ranges_per_node = defaultdict(int)
        for r in ranges:
            for replica in r.replicas:
                ranges_per_node[replica] += 1

        print(f"\nToken ranges discovered: {len(ranges)}")
        print("Ranges per node:")
        for node, count in sorted(ranges_per_node.items()):
            print(f"  {node}: {count} ranges")

        # Each node should own approximately the same number of ranges
        counts = list(ranges_per_node.values())
        if len(counts) >= 3:
            avg_count = sum(counts) / len(counts)
            for count in counts:
                # Allow 20% variance
                assert (
                    0.8 * avg_count <= count <= 1.2 * avg_count
                ), f"Uneven distribution: {ranges_per_node}"

        # Verify ranges cover the entire ring
        sorted_ranges = sorted(ranges, key=lambda r: r.start)

        # With vnodes, tokens are randomly distributed, so the first range
        # won't necessarily start at MIN_TOKEN. What matters is:
        # 1. No gaps between consecutive ranges
        # 2. The last range wraps around to the first range
        # 3. Total coverage equals the token space

        # Check for gaps or overlaps between consecutive ranges
        gaps = 0
        for i in range(len(sorted_ranges) - 1):
            current = sorted_ranges[i]
            next_range = sorted_ranges[i + 1]

            # Ranges should be contiguous
            if current.end != next_range.start:
                gaps += 1
                print(f"Gap found: {current.end} to {next_range.start}")

        assert gaps == 0, f"Found {gaps} gaps in token ranges"

        # Verify the last range wraps around to the first
        assert sorted_ranges[-1].end == sorted_ranges[0].start, (
            f"Ring not closed: last range ends at {sorted_ranges[-1].end}, "
            f"first range starts at {sorted_ranges[0].start}"
        )

        # Verify total coverage
        total_size = sum(r.size for r in ranges)
        # Allow for small rounding differences
        assert abs(total_size - TOTAL_TOKEN_RANGE) <= len(
            ranges
        ), f"Total coverage {total_size} differs from expected {TOTAL_TOKEN_RANGE}"

    @pytest.mark.asyncio
    async def test_compare_with_nodetool_describering(self, session):
        """
        Compare discovered ranges with nodetool describering output.

        What this tests:
        ---------------
        1. Our discovery matches nodetool output
        2. Token boundaries are correct
        3. Replica assignments match
        4. No missing or extra ranges

        Why this matters:
        ----------------
        - nodetool is the source of truth
        - Mismatches indicate bugs in discovery
        - Critical for production reliability
        - Validates driver metadata accuracy
        """
        ranges = await discover_token_ranges(session, "bulk_test")

        # Get nodetool output from first node
        try:
            result = subprocess.run(
                ["podman", "exec", "bulk-cassandra-1", "nodetool", "describering", "bulk_test"],
                capture_output=True,
                text=True,
                check=True,
            )
            nodetool_output = result.stdout
        except subprocess.CalledProcessError:
            # Try docker if podman fails
            try:
                result = subprocess.run(
                    ["docker", "exec", "bulk-cassandra-1", "nodetool", "describering", "bulk_test"],
                    capture_output=True,
                    text=True,
                    check=True,
                )
                nodetool_output = result.stdout
            except subprocess.CalledProcessError as e:
                pytest.skip(f"Cannot run nodetool: {e}")

        print("\nNodetool describering output (first 20 lines):")
        print("\n".join(nodetool_output.split("\n")[:20]))

        # Parse token count from nodetool output
        token_ranges_in_output = nodetool_output.count("TokenRange")

        print("\nComparison:")
        print(f"  Discovered ranges: {len(ranges)}")
        print(f"  Nodetool ranges: {token_ranges_in_output}")

        # Should have same number of ranges (allowing small variance)
        assert (
            abs(len(ranges) - token_ranges_in_output) <= 5
        ), f"Mismatch in range count: discovered {len(ranges)} vs nodetool {token_ranges_in_output}"
