#!/usr/bin/env python3
"""Debug token range coverage issue."""

import asyncio

from bulk_operations.bulk_operator import TokenAwareBulkOperator
from bulk_operations.token_utils import MIN_TOKEN, discover_token_ranges, generate_token_range_query

from async_cassandra import AsyncCluster


async def debug_coverage():
    """Debug why we're missing rows."""
    print("Debugging token range coverage...")

    async with AsyncCluster(contact_points=["localhost"]) as cluster:
        session = await cluster.connect()

        # First, let's see what tokens our test data actually has
        print("\nChecking token distribution of test data...")

        # Get a sample of tokens
        result = await session.execute(
            """
            SELECT id, token(id) as token_value
            FROM bulk_test.test_data
            LIMIT 20
        """
        )

        print("Sample tokens:")
        for row in result:
            print(f"  ID {row.id}: token = {row.token_value}")

        # Get min and max tokens in our data
        result = await session.execute(
            """
            SELECT MIN(token(id)) as min_token, MAX(token(id)) as max_token
            FROM bulk_test.test_data
        """
        )
        row = result.one()
        print(f"\nActual token range in data: {row.min_token} to {row.max_token}")
        print(f"MIN_TOKEN constant: {MIN_TOKEN}")

        # Now let's see our token ranges
        ranges = await discover_token_ranges(session, "bulk_test")
        sorted_ranges = sorted(ranges, key=lambda r: r.start)

        print("\nFirst 5 token ranges:")
        for i, r in enumerate(sorted_ranges[:5]):
            print(f"  Range {i}: {r.start} to {r.end}")

        # Check if any of our data falls outside the discovered ranges
        print("\nChecking for data outside discovered ranges...")

        # Find the range that should contain MIN_TOKEN
        min_token_range = None
        for r in sorted_ranges:
            if r.start <= row.min_token <= r.end:
                min_token_range = r
                break

        if min_token_range:
            print(
                f"Range containing minimum data token: {min_token_range.start} to {min_token_range.end}"
            )
        else:
            print("WARNING: No range found containing minimum data token!")

        # Let's also check if we have the wraparound issue
        print(f"\nLast range: {sorted_ranges[-1].start} to {sorted_ranges[-1].end}")
        print(f"First range: {sorted_ranges[0].start} to {sorted_ranges[0].end}")

        # The issue might be with how we handle the wraparound
        # In Cassandra's token ring, the last range wraps to the first
        # Let's verify this
        if sorted_ranges[-1].end != sorted_ranges[0].start:
            print(
                f"WARNING: Ring not properly closed! Last end: {sorted_ranges[-1].end}, First start: {sorted_ranges[0].start}"
            )

        # Test the actual queries
        print("\nTesting actual token range queries...")
        operator = TokenAwareBulkOperator(session)

        # Get table metadata
        table_meta = await operator._get_table_metadata("bulk_test", "test_data")
        partition_keys = [col.name for col in table_meta.partition_key]

        # Test first range query
        first_query = generate_token_range_query(
            "bulk_test", "test_data", partition_keys, sorted_ranges[0]
        )
        print(f"\nFirst range query: {first_query}")
        count_query = first_query.replace("SELECT *", "SELECT COUNT(*)")
        result = await session.execute(count_query)
        print(f"Rows in first range: {result.one()[0]}")

        # Test last range query
        last_query = generate_token_range_query(
            "bulk_test", "test_data", partition_keys, sorted_ranges[-1]
        )
        print(f"\nLast range query: {last_query}")
        count_query = last_query.replace("SELECT *", "SELECT COUNT(*)")
        result = await session.execute(count_query)
        print(f"Rows in last range: {result.one()[0]}")


if __name__ == "__main__":
    try:
        asyncio.run(debug_coverage())
    except Exception as e:
        print(f"Error: {e}")
        import traceback

        traceback.print_exc()
