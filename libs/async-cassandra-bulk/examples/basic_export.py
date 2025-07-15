#!/usr/bin/env python3
"""
Basic export example demonstrating CSV and JSON exports.

This example shows how to:
1. Connect to Cassandra cluster
2. Count rows in a table
3. Export data to CSV format
4. Export data to JSON format
5. Track progress during export
"""

import asyncio
from pathlib import Path

from async_cassandra import AsyncCluster

from async_cassandra_bulk import BulkOperator


async def setup_sample_data(session):
    """Create sample table and data for demonstration."""
    # Create keyspace
    await session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS examples
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """
    )

    await session.set_keyspace("examples")

    # Create table
    await session.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id uuid PRIMARY KEY,
            username text,
            email text,
            age int,
            active boolean,
            created_at timestamp
        )
    """
    )

    # Insert sample data
    from datetime import datetime, timezone
    from uuid import uuid4

    insert_stmt = await session.prepare(
        """
        INSERT INTO users (id, username, email, age, active, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """
    )

    print("Inserting sample data...")
    for i in range(100):
        await session.execute(
            insert_stmt,
            (
                uuid4(),
                f"user{i}",
                f"user{i}@example.com",
                20 + (i % 40),
                i % 3 != 0,  # 2/3 are active
                datetime.now(timezone.utc),
            ),
        )

    print("Sample data created!")


async def basic_export_example():
    """Demonstrate basic export functionality."""
    # Connect to Cassandra
    async with AsyncCluster(["localhost"]) as cluster:
        async with cluster.connect() as session:
            # Setup sample data
            await setup_sample_data(session)

            # Create operator
            operator = BulkOperator(session=session)

            # Count rows
            print("\n--- Counting Rows ---")
            total_count = await operator.count("examples.users")
            print(f"Total users: {total_count}")

            active_count = await operator.count(
                "examples.users", where="active = true ALLOW FILTERING"
            )
            print(f"Active users: {active_count}")

            # Create output directory
            output_dir = Path("export_output")
            output_dir.mkdir(exist_ok=True)

            # Export to CSV
            print("\n--- Exporting to CSV ---")
            csv_path = output_dir / "users.csv"

            def progress_callback(stats):
                print(
                    f"CSV Export Progress: {stats.progress_percentage:.1f}% "
                    f"({stats.rows_processed}/{total_count} rows)"
                )

            csv_stats = await operator.export(
                table="examples.users",
                output_path=str(csv_path),
                format="csv",
                progress_callback=progress_callback,
            )

            print("\nCSV Export Complete:")
            print(f"  - Rows exported: {csv_stats.rows_processed}")
            print(f"  - Duration: {csv_stats.duration_seconds:.2f} seconds")
            print(f"  - Rate: {csv_stats.rows_per_second:.0f} rows/second")
            print(f"  - Output file: {csv_path}")

            # Show sample of CSV
            print("\nFirst 3 lines of CSV:")
            with open(csv_path, "r") as f:
                for i, line in enumerate(f):
                    if i < 3:
                        print(f"  {line.strip()}")

            # Export to JSON
            print("\n--- Exporting to JSON ---")
            json_path = output_dir / "users.json"

            json_stats = await operator.export(
                table="examples.users",
                output_path=str(json_path),
                format="json",
                json_options={"pretty": True},
            )

            print("\nJSON Export Complete:")
            print(f"  - Rows exported: {json_stats.rows_processed}")
            print(f"  - Output file: {json_path}")

            # Export to JSONL (streaming)
            print("\n--- Exporting to JSONL (streaming) ---")
            jsonl_path = output_dir / "users.jsonl"

            jsonl_stats = await operator.export(
                table="examples.users",
                output_path=str(jsonl_path),
                format="json",
                json_options={"mode": "objects"},
            )

            print("\nJSONL Export Complete:")
            print(f"  - Rows exported: {jsonl_stats.rows_processed}")
            print(f"  - Output file: {jsonl_path}")

            # Export specific columns only
            print("\n--- Exporting Specific Columns ---")
            partial_path = output_dir / "users_basic.csv"

            partial_stats = await operator.export(
                table="examples.users",
                output_path=str(partial_path),
                format="csv",
                columns=["username", "email", "active"],
            )

            print("\nPartial Export Complete:")
            print("  - Columns: username, email, active")
            print(f"  - Rows exported: {partial_stats.rows_processed}")
            print(f"  - Output file: {partial_path}")


if __name__ == "__main__":
    print("=== Async Cassandra Bulk Export Example ===\n")
    print("This example demonstrates basic export functionality.")
    print("Make sure Cassandra is running on localhost:9042\n")

    try:
        asyncio.run(basic_export_example())
        print("\n✅ Example completed successfully!")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("\nMake sure Cassandra is running and accessible.")
