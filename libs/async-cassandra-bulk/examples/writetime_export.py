#!/usr/bin/env python3
"""
Writetime export example.

This example demonstrates how to export data with writetime information,
which shows when each cell was last written to Cassandra.
"""

import asyncio
from datetime import datetime
from pathlib import Path

from async_cassandra import AsyncCluster

from async_cassandra_bulk import BulkOperator


async def setup_example_data(session):
    """Create example data with known writetime values."""
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
        CREATE TABLE IF NOT EXISTS user_activity (
            user_id UUID PRIMARY KEY,
            username TEXT,
            email TEXT,
            last_login TIMESTAMP,
            login_count INT,
            preferences MAP<TEXT, TEXT>,
            tags SET<TEXT>
        )
    """
    )

    # Insert data with explicit timestamp (writetime)
    from uuid import uuid4

    # User 1 - All data written at the same time
    user1_id = uuid4()
    user1_writetime = 1700000000000000  # Microseconds since epoch
    await session.execute(
        f"""
        INSERT INTO user_activity
        (user_id, username, email, last_login, login_count, preferences, tags)
        VALUES (
            {user1_id},
            'alice',
            'alice@example.com',
            '2024-01-15 10:00:00+0000',
            42,
            {{'theme': 'dark', 'language': 'en'}},
            {{'premium', 'verified'}}
        ) USING TIMESTAMP {user1_writetime}
        """
    )

    # User 2 - Different columns updated at different times
    user2_id = uuid4()
    base_writetime = 1700000000000000

    # Initial insert
    await session.execute(
        f"""
        INSERT INTO user_activity
        (user_id, username, email, last_login, login_count)
        VALUES (
            {user2_id},
            'bob',
            'bob@example.com',
            '2024-01-01 09:00:00+0000',
            10
        ) USING TIMESTAMP {base_writetime}
        """
    )

    # Update email later
    await session.execute(
        f"""
        UPDATE user_activity
        USING TIMESTAMP {base_writetime + 86400000000}  -- 1 day later
        SET email = 'bob.smith@example.com'
        WHERE user_id = {user2_id}
        """
    )

    # Update last_login even later
    await session.execute(
        f"""
        UPDATE user_activity
        USING TIMESTAMP {base_writetime + 172800000000}  -- 2 days later
        SET last_login = '2024-01-16 14:30:00+0000',
            login_count = 11
        WHERE user_id = {user2_id}
        """
    )

    print("✓ Example data created")


async def basic_writetime_export():
    """Basic writetime export example."""
    output_dir = Path("export_output")
    output_dir.mkdir(exist_ok=True)

    async with AsyncCluster(["localhost"]) as cluster:
        async with cluster.connect() as session:
            # Setup data
            await setup_example_data(session)

            operator = BulkOperator(session=session)

            print("\n--- Basic Writetime Export ---")

            # Export without writetime (default)
            print("\n1. Export WITHOUT writetime (default behavior):")
            output_file = output_dir / "users_no_writetime.csv"

            await operator.export(
                table="examples.user_activity",
                output_path=str(output_file),
                format="csv",
            )

            print(f"   Exported to: {output_file}")
            with open(output_file, "r") as f:
                print("   Headers:", f.readline().strip())

            # Export with writetime for specific columns
            print("\n2. Export WITH writetime for specific columns:")
            output_file = output_dir / "users_with_writetime.csv"

            await operator.export(
                table="examples.user_activity",
                output_path=str(output_file),
                format="csv",
                options={
                    "writetime_columns": ["username", "email", "last_login"],
                },
            )

            print(f"   Exported to: {output_file}")
            with open(output_file, "r") as f:
                headers = f.readline().strip()
                print("   Headers:", headers)
                print("\n   Sample data:")
                for i, line in enumerate(f):
                    if i < 2:
                        print(f"   {line.strip()}")

            # Export with writetime for all columns
            print("\n3. Export WITH writetime for ALL eligible columns:")
            output_file = output_dir / "users_all_writetime.json"

            await operator.export(
                table="examples.user_activity",
                output_path=str(output_file),
                format="json",
                options={
                    "writetime_columns": ["*"],  # All non-key columns
                },
                json_options={
                    "mode": "array",
                },
            )

            print(f"   Exported to: {output_file}")

            # Show writetime values
            import json

            with open(output_file, "r") as f:
                data = json.load(f)

            print("\n   Writetime analysis:")
            for i, row in enumerate(data):
                print(f"\n   User {i+1} ({row['username']}):")

                # Show writetime for each column
                for key, value in row.items():
                    if key.endswith("_writetime") and value:
                        col_name = key.replace("_writetime", "")
                        print(f"     - {col_name}: {value}")

                        # Parse and show as human-readable
                        try:
                            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
                            print(f"       (Written at: {dt.strftime('%Y-%m-%d %H:%M:%S UTC')})")
                        except Exception:
                            pass


async def writetime_format_examples():
    """Show different writetime format options."""
    import json

    output_dir = Path("export_output")
    output_dir.mkdir(exist_ok=True)

    async with AsyncCluster(["localhost"]) as cluster:
        async with cluster.connect() as session:
            await session.set_keyspace("examples")

            operator = BulkOperator(session=session)

            print("\n--- Writetime Format Examples ---")

            # CSV with custom timestamp format
            print("\n1. CSV with custom timestamp format:")
            output_file = output_dir / "users_custom_format.csv"

            await operator.export(
                table="user_activity",
                output_path=str(output_file),
                format="csv",
                options={
                    "writetime_columns": ["email", "last_login"],
                },
                csv_options={
                    "writetime_format": "%Y-%m-%d %H:%M:%S",  # Without microseconds
                },
            )

            print(f"   Exported to: {output_file}")
            with open(output_file, "r") as f:
                print("   Format: YYYY-MM-DD HH:MM:SS")
                f.readline()  # Skip header
                print(f"   Sample: {f.readline().strip()}")

            # JSON with ISO format (default)
            print("\n2. JSON with ISO format (default):")
            output_file = output_dir / "users_iso_format.json"

            await operator.export(
                table="user_activity",
                output_path=str(output_file),
                format="json",
                options={
                    "writetime_columns": ["email"],
                },
                json_options={
                    "mode": "objects",  # JSONL format
                },
            )

            print(f"   Exported to: {output_file}")
            with open(output_file, "r") as f:
                first_line = json.loads(f.readline())
                print(f"   ISO format: {first_line.get('email_writetime')}")


async def main():
    """Run writetime export examples."""
    print("=== Cassandra Writetime Export Examples ===\n")

    try:
        # Basic examples
        await basic_writetime_export()

        # Format examples
        await writetime_format_examples()

        print("\n✅ All examples completed successfully!")
        print("\nNote: Writetime shows when each cell was last written to Cassandra.")
        print("This is useful for:")
        print("  - Data migration (preserving original write times)")
        print("  - Audit trails (seeing when data changed)")
        print("  - Debugging (understanding data history)")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
