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
        async with cluster.connect() as session:
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

            # Insert sample data using prepared statements
            from datetime import date, datetime, timedelta

            insert_stmt = await session.prepare(
                """
                INSERT INTO user_events (user_id, event_date, event_time, event_type, details)
                VALUES (?, ?, ?, ?, ?)
                """
            )

            # Insert data for multiple users and dates
            print("\nInserting sample data...")
            base_date = date(2024, 1, 15)

            event_types = ["LOGIN", "LOGOUT", "ERROR", "UPDATE", "DELETE"]

            for user_id in [123, 456, 789]:
                for day_offset in range(3):  # 3 days of data
                    event_date = base_date + timedelta(days=day_offset)

                    for hour in range(0, 24, 4):  # Events every 4 hours
                        event_time = datetime.combine(event_date, datetime.min.time()) + timedelta(
                            hours=hour
                        )
                        event_type = event_types[hour % len(event_types)]
                        details = f'{{"ip": "192.168.1.{user_id % 255}", "action": "{event_type.lower()}"}}'

                        await session.execute(
                            insert_stmt, (user_id, event_date, event_time, event_type, details)
                        )

            print("✅ Sample data inserted")

            # Example 1: Partition key predicate (most efficient)
            print("\n1. Partition Key Predicate - Pushed to Cassandra:")
            print("   Filter: user_id = 123 AND event_date = '2024-01-15'")
            print(
                "   CQL: SELECT * FROM user_events WHERE user_id = 123 AND event_date = '2024-01-15'"
            )
            print("   ✅ No token ranges needed, direct partition access")

            # Demonstrate with actual query
            query_stmt = await session.prepare(
                "SELECT * FROM user_events WHERE user_id = ? AND event_date = ?"
            )
            result = await session.execute(query_stmt, (123, base_date))
            rows = list(result)
            print(f"   Result: {len(rows)} rows found")
            if rows:
                print(f"   Sample: user_id={rows[0].user_id}, event_type={rows[0].event_type}")

            # Example 2: Clustering key with partition key
            print("\n2. Clustering Key Predicate - Pushed to Cassandra:")
            print(
                "   Filter: user_id = 123 AND event_date = '2024-01-15' AND event_time > '2024-01-15 12:00:00'"
            )

            # Demonstrate with actual query
            cluster_query = await session.prepare(
                """
                SELECT * FROM user_events
                WHERE user_id = ? AND event_date = ? AND event_time > ?
                """
            )
            threshold_time = datetime(2024, 1, 15, 12, 0, 0)
            result = await session.execute(cluster_query, (123, base_date, threshold_time))
            rows = list(result)
            print(f"   Result: {len(rows)} rows after {threshold_time.time()}")
            print("   ✅ Clustering predicate allowed because partition key is complete")

            # Example 3: Regular column without partition key (would need ALLOW FILTERING)
            print("\n3. Regular Column Predicate - Client-side filtering:")
            print("   Filter: event_type = 'LOGIN'")
            print("   Without partition key, would need ALLOW FILTERING")

            # Show what happens with indexed column instead
            print("\n4. Indexed Column Predicate - Pushed to Cassandra:")
            print("   Filter: event_type = 'LOGIN' (with index)")

            # Demonstrate indexed query
            index_query = await session.prepare("SELECT * FROM user_events WHERE event_type = ?")
            result = await session.execute(index_query, ("LOGIN",))
            rows = list(result)
            print(f"   Result: {len(rows)} LOGIN events found across all partitions")
            print("   ✅ Can use index for efficient filtering")

            # Example 5: Mixed predicates
            print("\n5. Mixed Predicates:")
            print("   Filter: user_id = 123 AND event_type = 'LOGIN'")

            # Note: This query requires ALLOW FILTERING because event_type is not a key
            # In practice, you'd filter event_type client-side or use the index

            # Better approach - use partition key and filter client-side
            result = await session.execute(query_stmt, (123, base_date))
            login_rows = [row for row in result if row.event_type == "LOGIN"]
            print(f"   Result: {len(login_rows)} LOGIN events for user 123 on {base_date}")
            print("   ✅ Partition key pushed, event_type filtered client-side")

            # Example 6: Token range queries (for parallel processing)
            print("\n6. Token Range Queries (for parallel scan):")

            # Get token ranges
            token_query = (
                "SELECT token(user_id, event_date), user_id, event_date FROM user_events LIMIT 10"
            )
            result = await session.execute(token_query)
            tokens = [(row[0], row[1], row[2]) for row in result]

            if tokens:
                print(
                    f"   Sample tokens: {tokens[0][0]} for partition ({tokens[0][1]}, {tokens[0][2]})"
                )
                print("   These would be used to split work across Dask workers")

            print("\n=== Performance Implications ===")
            print("1. Partition key predicates: Fastest - O(1) partition lookup")
            print("2. Clustering predicates: Fast - Uses partition + sorted order")
            print("3. Indexed predicates: Medium - Index lookup + random reads")
            print("4. Client-side filtering: Slowest - Reads all data then filters")
            print("5. ALLOW FILTERING: Dangerous - Full table scan")

            # Demonstrate count queries for performance comparison
            print("\n=== Query Performance Comparison ===")

            # Fast: Direct partition access
            count_query = await session.prepare(
                "SELECT COUNT(*) FROM user_events WHERE user_id = ? AND event_date = ?"
            )
            result = await session.execute(count_query, (123, base_date))
            count = list(result)[0][0]
            print(f"Partition key query: {count} rows (fast)")

            # Medium: Index lookup
            count_index = await session.prepare(
                "SELECT COUNT(*) FROM user_events WHERE event_type = ?"
            )
            result = await session.execute(count_index, ("LOGIN",))
            count = list(result)[0][0]
            print(f"Indexed column query: {count} rows (medium speed)")

            # Show total for comparison
            total_result = await session.execute("SELECT COUNT(*) FROM user_events")
            total = list(total_result)[0][0]
            print(f"Total rows in table: {total}")


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
