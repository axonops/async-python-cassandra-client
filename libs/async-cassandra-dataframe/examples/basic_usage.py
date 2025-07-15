"""
Basic usage example for async-cassandra-dataframe.

Shows how to read Cassandra tables as Dask DataFrames for distributed processing.
"""

import asyncio

import async_cassandra_dataframe as cdf
from async_cassandra import AsyncCluster


async def main():
    """Example of reading Cassandra data as Dask DataFrame."""
    # Connect to Cassandra
    async with AsyncCluster(contact_points=["localhost"]) as cluster:
        async with cluster.connect() as session:
            # Create test keyspace and table
            await session.execute(
                """
                CREATE KEYSPACE IF NOT EXISTS test_df
                WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
                """
            )
            await session.set_keyspace("test_df")

            await session.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id INT PRIMARY KEY,
                    name TEXT,
                    email TEXT,
                    age INT,
                    created_at TIMESTAMP
                )
                """
            )

            # Insert test data
            insert_stmt = await session.prepare(
                "INSERT INTO users (id, name, email, age, created_at) VALUES (?, ?, ?, ?, ?)"
            )

            from datetime import UTC, datetime

            now = datetime.now(UTC)

            for i in range(1000):
                await session.execute(
                    insert_stmt, (i, f"User {i}", f"user{i}@example.com", 20 + (i % 50), now)
                )

            # Read table as Dask DataFrame
            df = await cdf.read_cassandra_table(
                "users", session=session, memory_per_partition_mb=50  # Small partitions for demo
            )

            print(f"DataFrame has {df.npartitions} partitions")

            # Perform distributed operations
            # Count users by age group
            age_groups = df.assign(
                age_group=df.age.apply(lambda x: f"{(x // 10) * 10}s", meta=("age_group", "object"))
            )

            # Compute results
            result = await age_groups.groupby("age_group").size().compute()
            print("\nUsers by age group:")
            print(result.sort_index())

            # Select specific columns and filter
            young_users = await df[df.age < 30][["name", "email"]].compute()
            print(f"\nFound {len(young_users)} users under 30")
            print(young_users.head())

            # Read with writetime
            df_with_writetime = await cdf.read_cassandra_table(
                "users",
                session=session,
                columns=["id", "name", "created_at"],
                writetime_columns=["name", "created_at"],
            )

            # Check writetime
            wt_result = await df_with_writetime.head(5).compute()
            print("\nSample data with writetime:")
            print(wt_result)


if __name__ == "__main__":
    asyncio.run(main())
