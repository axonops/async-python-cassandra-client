"""
Advanced usage examples for async-cassandra-dataframe.

Shows writetime filtering, snapshot consistency, and concurrency control.
"""

import asyncio
from datetime import UTC, datetime

import async_cassandra_dataframe as cdf
from async_cassandra import AsyncCluster


async def example_writetime_filtering():
    """Example: Filter data by writetime."""
    print("\n=== Writetime Filtering Example ===")

    async with AsyncCluster(contact_points=["localhost"]) as cluster:
        async with cluster.connect() as session:
            # Setup
            await session.execute(
                """
                CREATE KEYSPACE IF NOT EXISTS test_df
                WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
                """
            )
            await session.set_keyspace("test_df")

            await session.execute("DROP TABLE IF EXISTS events")
            await session.execute(
                """
                CREATE TABLE events (
                    id INT PRIMARY KEY,
                    type TEXT,
                    data TEXT,
                    processed BOOLEAN
                )
                """
            )

            # Insert some old data
            for i in range(5):
                await session.execute(
                    f"INSERT INTO events (id, type, data, processed) "
                    f"VALUES ({i}, 'old', 'old_data_{i}', false)"
                )

            # Mark cutoff time
            cutoff_time = datetime.now(UTC)
            print(f"Cutoff time: {cutoff_time}")

            # Wait a bit
            await asyncio.sleep(0.1)

            # Insert new data
            for i in range(5, 10):
                await session.execute(
                    f"INSERT INTO events (id, type, data, processed) "
                    f"VALUES ({i}, 'new', 'new_data_{i}', false)"
                )

            # Get only new data (written after cutoff)
            df = await cdf.read_cassandra_table(
                "events",
                session=session,
                writetime_filter={"column": "data", "operator": ">", "timestamp": cutoff_time},
            )

            result = await df.compute()
            print(f"\nNew events (after {cutoff_time.isoformat()}):")
            print(result[["id", "type", "data", "data_writetime"]])

            # Get old data (written before cutoff)
            df_old = await cdf.read_cassandra_table(
                "events",
                session=session,
                writetime_filter={"column": "data", "operator": "<=", "timestamp": cutoff_time},
            )

            result_old = await df_old.compute()
            print(f"\nOld events (before {cutoff_time.isoformat()}):")
            print(result_old[["id", "type", "data", "data_writetime"]])


async def example_snapshot_consistency():
    """Example: Consistent snapshot with fixed 'now' time."""
    print("\n=== Snapshot Consistency Example ===")

    async with AsyncCluster(contact_points=["localhost"]) as cluster:
        async with cluster.connect() as session:
            await session.set_keyspace("test_df")

            await session.execute("DROP TABLE IF EXISTS inventory")
            await session.execute(
                """
                CREATE TABLE inventory (
                    sku TEXT PRIMARY KEY,
                    quantity INT,
                    location TEXT,
                    last_updated TIMESTAMP
                )
                """
            )

            # Initial inventory
            items = [
                ("SKU001", 100, "warehouse_a"),
                ("SKU002", 50, "warehouse_b"),
                ("SKU003", 75, "warehouse_a"),
            ]

            for sku, qty, loc in items:
                await session.execute(
                    f"INSERT INTO inventory (sku, quantity, location, last_updated) "
                    f"VALUES ('{sku}', {qty}, '{loc}', toTimestamp(now()))"
                )

            # Take a snapshot at current time
            # All queries will use this exact time for consistency
            df = await cdf.read_cassandra_table(
                "inventory",
                session=session,
                snapshot_time="now",  # Fix "now" at this moment
                writetime_filter={
                    "column": "*",  # Any column
                    "operator": "<=",
                    "timestamp": "now",  # Uses the same snapshot time
                },
            )

            snapshot_data = await df.compute()
            snapshot_time = snapshot_data.iloc[0]["quantity_writetime"]

            print(f"\nSnapshot taken at: {snapshot_time}")
            print("Initial inventory:")
            print(snapshot_data[["sku", "quantity", "location"]])

            # Simulate changes happening after snapshot
            await session.execute("UPDATE inventory SET quantity = 150 WHERE sku = 'SKU001'")
            await session.execute(
                "INSERT INTO inventory (sku, quantity, location, last_updated) "
                "VALUES ('SKU004', 200, 'warehouse_c', toTimestamp(now()))"
            )

            # Read with same snapshot time - changes are not visible
            df_consistent = await cdf.read_cassandra_table(
                "inventory",
                session=session,
                snapshot_time=snapshot_time,  # Use exact same time
                writetime_filter={"column": "*", "operator": "<=", "timestamp": snapshot_time},
            )

            consistent_data = await df_consistent.compute()
            print("\nData at snapshot time (changes not visible):")
            print(consistent_data[["sku", "quantity", "location"]])
            print(
                f"SKU001 quantity still shows: {consistent_data[consistent_data['sku'] == 'SKU001']['quantity'].iloc[0]}"
            )
            print(f"SKU004 not in snapshot: {'SKU004' not in consistent_data['sku'].values}")


async def example_concurrency_control():
    """Example: Control concurrent load on Cassandra."""
    print("\n=== Concurrency Control Example ===")

    async with AsyncCluster(contact_points=["localhost"]) as cluster:
        async with cluster.connect() as session:
            await session.set_keyspace("test_df")

            await session.execute("DROP TABLE IF EXISTS large_table")
            await session.execute(
                """
                CREATE TABLE large_table (
                    partition_id INT,
                    item_id INT,
                    data TEXT,
                    PRIMARY KEY (partition_id, item_id)
                )
                """
            )

            # Create data across many partitions
            print("Creating test data...")
            insert_stmt = await session.prepare(
                "INSERT INTO large_table (partition_id, item_id, data) VALUES (?, ?, ?)"
            )

            for p in range(20):
                for i in range(100):
                    await session.execute(insert_stmt, (p, i, f"data_p{p}_i{i}"))

            print("Reading with concurrency limits...")

            # Read with controlled concurrency
            df = await cdf.read_cassandra_table(
                "large_table",
                session=session,
                partition_count=10,  # Split into 10 partitions
                max_concurrent_queries=3,  # Only 3 queries to Cassandra at once
                max_concurrent_partitions=5,  # Process max 5 partitions in parallel
                memory_per_partition_mb=50,  # Small partitions
            )

            # Track timing
            start = datetime.now()
            result = await df.compute()
            duration = (datetime.now() - start).total_seconds()

            print(f"\nProcessed {len(result)} rows in {duration:.2f} seconds")
            print(f"Partitions: {df.npartitions}")
            print("With max 3 concurrent queries to protect Cassandra")
            print(f"Sample data: {result.head(3)}")


async def example_automatic_columns():
    """Example: Automatic column detection from metadata."""
    print("\n=== Automatic Column Detection Example ===")

    async with AsyncCluster(contact_points=["localhost"]) as cluster:
        async with cluster.connect() as session:
            await session.set_keyspace("test_df")

            await session.execute("DROP TABLE IF EXISTS products")
            await session.execute(
                """
                CREATE TABLE products (
                    id UUID PRIMARY KEY,
                    name TEXT,
                    category TEXT,
                    price DECIMAL,
                    in_stock BOOLEAN,
                    tags SET<TEXT>,
                    attributes MAP<TEXT, TEXT>
                )
                """
            )

            # Insert a product
            await session.execute(
                """
                INSERT INTO products (id, name, category, price, in_stock, tags, attributes)
                VALUES (
                    uuid(),
                    'Laptop Pro',
                    'Electronics',
                    1299.99,
                    true,
                    {'portable', 'powerful', 'business'},
                    {'brand': 'TechCorp', 'warranty': '2 years'}
                )
                """
            )

            # Read WITHOUT specifying columns - they're detected automatically
            df = await cdf.read_cassandra_table(
                "products",
                session=session,
                # No columns parameter!
            )

            result = await df.compute()

            print("\nColumns automatically detected from Cassandra metadata:")
            print(f"Columns: {list(result.columns)}")
            print("\nData types:")
            for col in result.columns:
                print(f"  {col}: {result[col].dtype}")

            print("\nSample data:")
            print(result)


async def example_incremental_load():
    """Example: Incremental data loading using writetime."""
    print("\n=== Incremental Load Example ===")

    async with AsyncCluster(contact_points=["localhost"]) as cluster:
        async with cluster.connect() as session:
            await session.set_keyspace("test_df")

            await session.execute("DROP TABLE IF EXISTS transactions")
            await session.execute(
                """
                CREATE TABLE transactions (
                    id UUID PRIMARY KEY,
                    account TEXT,
                    amount DECIMAL,
                    type TEXT
                )
                """
            )

            # Simulate initial load
            print("Initial data load...")
            for i in range(5):
                await session.execute(
                    f"""
                    INSERT INTO transactions (id, account, amount, type)
                    VALUES (uuid(), 'ACC00{i}', {100 + i * 10}, 'credit')
                    """
                )

            # Track last load time
            last_load_time = datetime.now(UTC)
            print(f"Last load time: {last_load_time}")

            # Wait and add new transactions
            await asyncio.sleep(0.1)

            print("\nNew transactions arrive...")
            for i in range(5, 8):
                await session.execute(
                    f"""
                    INSERT INTO transactions (id, account, amount, type)
                    VALUES (uuid(), 'ACC00{i}', {100 + i * 10}, 'debit')
                    """
                )

            # Incremental load - only get new data
            print(f"\nIncremental load - data after {last_load_time}...")
            df_incremental = await cdf.read_cassandra_table(
                "transactions",
                session=session,
                writetime_filter={
                    "column": "*",  # Check any column
                    "operator": ">",
                    "timestamp": last_load_time,
                },
            )

            new_data = await df_incremental.compute()
            print(f"Found {len(new_data)} new transactions:")
            print(new_data[["account", "amount", "type"]])


async def main():
    """Run all examples."""
    await example_automatic_columns()
    await example_writetime_filtering()
    await example_snapshot_consistency()
    await example_concurrency_control()
    await example_incremental_load()


if __name__ == "__main__":
    asyncio.run(main())
