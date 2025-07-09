#!/usr/bin/env python3
"""Simple test to debug count issue."""

import asyncio

from async_cassandra import AsyncCluster
from bulk_operations.bulk_operator import TokenAwareBulkOperator


async def test_count():
    """Test count with error details."""
    async with AsyncCluster(contact_points=["localhost"]) as cluster:
        session = await cluster.connect()

        operator = TokenAwareBulkOperator(session)

        try:
            count = await operator.count_by_token_ranges(
                keyspace="bulk_test", table="test_data", split_count=4, parallelism=2
            )
            print(f"Count successful: {count}")
        except Exception as e:
            print(f"Error: {e}")
            if hasattr(e, "errors"):
                print(f"Detailed errors: {e.errors}")
                for err in e.errors:
                    print(f"  - {err}")


if __name__ == "__main__":
    asyncio.run(test_count())
