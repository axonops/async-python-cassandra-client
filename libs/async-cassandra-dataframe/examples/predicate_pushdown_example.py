"""
Example of predicate pushdown with Cassandra and Dask DataFrames.

Shows how different types of predicates are handled.
"""

import asyncio

from async_cassandra import AsyncCluster


async def example_predicate_pushdown():
    """Demonstrate predicate pushdown scenarios."""
    print("\n=== Predicate Pushdown Examples ===")

    async with AsyncCluster(contact_points=["localhost"]) as cluster:
        session = await cluster.connect()

        # Setup example table
        await session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS test_pushdown
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
            """
        )
        await session.set_keyspace("test_pushdown")

        # Create a table with various key types
        await session.execute("DROP TABLE IF EXISTS user_events")
        await session.execute(
            """
            CREATE TABLE user_events (
                user_id INT,
                event_date DATE,
                event_time TIMESTAMP,
                event_type TEXT,
                details TEXT,
                PRIMARY KEY ((user_id, event_date), event_time)
            ) WITH CLUSTERING ORDER BY (event_time DESC)
            """
        )

        # Create secondary index
        await session.execute("CREATE INDEX IF NOT EXISTS ON user_events (event_type)")

        print("\nTable structure:")
        print("- Partition keys: user_id, event_date")
        print("- Clustering key: event_time")
        print("- Indexed column: event_type")

        # Insert sample data
        # In a real application, you would use this prepared statement:
        # insert_stmt = await session.prepare(
        #     """
        #     INSERT INTO user_events (user_id, event_date, event_time, event_type, details)
        #     VALUES (?, ?, ?, ?, ?)
        #     """
        # )
        # await session.execute(insert_stmt, (123, date(2024, 1, 15), time(10, 30), 'LOGIN', {'ip': '192.168.1.1'}))

        # ... insert data ...

        # Example 1: Partition key predicate (most efficient)
        print("\n1. Partition Key Predicate - Pushed to Cassandra:")
        print("   Filter: user_id = 123 AND event_date = '2024-01-15'")
        print("   CQL: SELECT * FROM user_events WHERE user_id = 123 AND event_date = '2024-01-15'")
        print("   ✅ No token ranges needed, direct partition access")

        # With future API:
        # df = await cdf.read_cassandra_table(
        #     "user_events",
        #     session=session,
        #     predicates=[
        #         {"column": "user_id", "operator": "=", "value": 123},
        #         {"column": "event_date", "operator": "=", "value": "2024-01-15"}
        #     ]
        # )

        # Example 2: Clustering key with partition key
        print("\n2. Clustering Key Predicate - Pushed to Cassandra:")
        print(
            "   Filter: user_id = 123 AND event_date = '2024-01-15' AND event_time > '2024-01-15 12:00:00'"
        )
        print(
            "   CQL: WHERE user_id = 123 AND event_date = '2024-01-15' AND event_time > '2024-01-15 12:00:00'"
        )
        print("   ✅ Clustering predicate allowed because partition key is complete")

        # Example 3: Regular column without partition key
        print("\n3. Regular Column Predicate - Client-side filtering:")
        print("   Filter: event_type = 'login'")
        print(
            "   CQL: SELECT * FROM user_events WHERE TOKEN(user_id, event_date) >= ? AND TOKEN(...) <= ?"
        )
        print("   ⚠️  event_type filter applied in Dask after fetching data")
        print("   Why: Without partition key, would need ALLOW FILTERING (slow)")

        # Example 4: Secondary index predicate
        print("\n4. Indexed Column Predicate - Pushed to Cassandra:")
        print("   Filter: event_type = 'login' (with index)")
        print("   CQL: SELECT * FROM user_events WHERE event_type = 'login'")
        print("   ✅ Can use index for efficient filtering")

        # Example 5: Mixed predicates
        print("\n5. Mixed Predicates:")
        print("   Filter: user_id = 123 AND event_type = 'login' AND details LIKE '%error%'")
        print("   Pushed: user_id = 123, event_type = 'login'")
        print("   Client-side: details LIKE '%error%'")
        print("   ✅ Optimal push down of supported predicates")

        # Example 6: Token range with client filtering
        print("\n6. Parallel Scan with Filtering:")
        print("   Filter: event_time > '2024-01-01' (across all partitions)")
        print("   CQL: Multiple queries with TOKEN ranges")
        print("   ⚠️  event_time filter in client (can't push without partition key)")

        print("\n=== Performance Implications ===")
        print("1. Partition key predicates: Fastest - O(1) partition lookup")
        print("2. Clustering predicates: Fast - Uses partition + sorted order")
        print("3. Indexed predicates: Medium - Index lookup + random reads")
        print("4. Client-side filtering: Slowest - Reads all data then filters")
        print("5. ALLOW FILTERING: Dangerous - Full table scan")

        await session.close()


async def example_integration_with_dask():
    """Show how predicate pushdown would work with Dask operations."""
    print("\n=== Dask Integration Example ===")

    # Future API design:
    print(
        """
    # Read with predicate pushdown
    df = await cdf.read_cassandra_table(
        "user_events",
        session=session,
        # These predicates will be analyzed for pushdown
        predicates=[
            {"column": "user_id", "operator": "=", "value": 123},
            {"column": "event_type", "operator": "=", "value": "login"}
        ]
    )

    # Dask operations that could trigger pushdown
    filtered_df = df[df['event_time'] > '2024-01-01']
    # The reader could intercept this and push down if possible

    # Complex query with partial pushdown
    result = df[
        (df['user_id'] == 123) &  # Can push down
        (df['details'].str.contains('error'))  # Must filter client-side
    ]

    # The analyzer would:
    # 1. Push user_id = 123 to Cassandra
    # 2. Apply string contains in Dask
    """
    )


if __name__ == "__main__":
    asyncio.run(example_predicate_pushdown())
