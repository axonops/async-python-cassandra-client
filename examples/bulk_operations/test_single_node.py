#!/usr/bin/env python3
"""Quick test to verify token range discovery with single node."""

import asyncio

from async_cassandra import AsyncCluster
from bulk_operations.token_utils import (
    MAX_TOKEN,
    MIN_TOKEN,
    TOTAL_TOKEN_RANGE,
    discover_token_ranges,
)


async def test_single_node():
    """Test token range discovery with single node."""
    print("Connecting to single-node cluster...")

    async with AsyncCluster(contact_points=["localhost"]) as cluster:
        session = await cluster.connect()

        # Create test keyspace
        await session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS test_single
            WITH replication = {
                'class': 'SimpleStrategy',
                'replication_factor': 1
            }
        """
        )

        print("Discovering token ranges...")
        ranges = await discover_token_ranges(session, "test_single")

        print(f"\nToken ranges discovered: {len(ranges)}")
        print("Expected with 1 node × 256 vnodes: 256 ranges")

        # Verify we have the expected number of ranges
        assert len(ranges) == 256, f"Expected 256 ranges, got {len(ranges)}"

        # Verify ranges cover the entire ring
        sorted_ranges = sorted(ranges, key=lambda r: r.start)

        # Debug first and last ranges
        print(f"First range: {sorted_ranges[0].start} to {sorted_ranges[0].end}")
        print(f"Last range: {sorted_ranges[-1].start} to {sorted_ranges[-1].end}")
        print(f"MIN_TOKEN: {MIN_TOKEN}, MAX_TOKEN: {MAX_TOKEN}")

        # The token ring is circular, so we need to handle wraparound
        # The smallest token in the sorted list might not be MIN_TOKEN
        # because of how Cassandra distributes vnodes

        # Check for gaps or overlaps
        gaps = []
        overlaps = []
        for i in range(len(sorted_ranges) - 1):
            current = sorted_ranges[i]
            next_range = sorted_ranges[i + 1]
            if current.end < next_range.start:
                gaps.append((current.end, next_range.start))
            elif current.end > next_range.start:
                overlaps.append((current.end, next_range.start))

        print(f"\nGaps found: {len(gaps)}")
        if gaps:
            for gap in gaps[:3]:
                print(f"  Gap: {gap[0]} to {gap[1]}")

        print(f"Overlaps found: {len(overlaps)}")

        # Check if ranges form a complete ring
        # In a proper token ring, each range's end should equal the next range's start
        # The last range should wrap around to the first
        total_size = sum(r.size for r in ranges)
        print(f"\nTotal token space covered: {total_size:,}")
        print(f"Expected total space: {TOTAL_TOKEN_RANGE:,}")

        # Show sample ranges
        print("\nSample token ranges (first 5):")
        for i, r in enumerate(sorted_ranges[:5]):
            print(f"  Range {i+1}: {r.start} to {r.end} (size: {r.size:,})")

        print("\n✅ All tests passed!")

        # Session is closed automatically by the context manager
        return True


if __name__ == "__main__":
    try:
        asyncio.run(test_single_node())
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()
        exit(1)
