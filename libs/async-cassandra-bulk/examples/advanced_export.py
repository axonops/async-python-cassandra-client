#!/usr/bin/env python3
"""
Advanced export example with checkpointing and custom exporters.

This example demonstrates:
1. Large dataset export with progress tracking
2. Checkpointing for resumable exports
3. Custom exporter implementation
4. Performance tuning options
5. Error handling and recovery
"""

import asyncio
import json
import time
from pathlib import Path
from typing import Dict, List

from async_cassandra import AsyncCluster

from async_cassandra_bulk import BaseExporter, BulkOperationStats, BulkOperator, ParallelExporter


class TSVExporter(BaseExporter):
    """Custom Tab-Separated Values exporter."""

    def __init__(self, output_path: str, include_header: bool = True):
        super().__init__(output_path)
        self.include_header = include_header
        self.file = None
        self.writer = None

    async def initialize(self) -> None:
        """Open file for writing."""
        self.file = open(self.output_path, "w", encoding="utf-8")

    async def write_header(self, columns: List[str]) -> None:
        """Write TSV header."""
        if self.include_header:
            self.file.write("\t".join(columns) + "\n")

    async def write_row(self, row: Dict) -> None:
        """Write row as tab-separated values."""
        # Convert values to strings, handling None
        values = [str(row.get(col, "")) if row.get(col) is not None else "" for col in row.keys()]
        self.file.write("\t".join(values) + "\n")

    async def finalize(self) -> None:
        """Close file."""
        if self.file:
            self.file.close()


async def setup_large_dataset(session, num_rows: int = 10000):
    """Create a larger dataset for testing."""
    await session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS examples
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """
    )

    await session.set_keyspace("examples")

    # Create table with more columns
    await session.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
            id uuid PRIMARY KEY,
            user_id uuid,
            event_type text,
            timestamp timestamp,
            properties map<text, text>,
            tags set<text>,
            metrics list<double>,
            status text
        )
    """
    )

    # Check if already populated
    count = await session.execute("SELECT COUNT(*) FROM events")
    existing = count.one()[0]

    if existing >= num_rows:
        print(f"Table already has {existing} rows")
        return

    # Insert data in batches
    from datetime import datetime, timedelta, timezone
    from uuid import uuid4

    insert_stmt = await session.prepare(
        """
        INSERT INTO events (
            id, user_id, event_type, timestamp,
            properties, tags, metrics, status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
    )

    print(f"Inserting {num_rows} events...")
    batch_size = 100

    for i in range(0, num_rows, batch_size):
        batch = []
        for j in range(min(batch_size, num_rows - i)):
            event_time = datetime.now(timezone.utc) - timedelta(hours=j)
            event_type = ["login", "purchase", "view", "logout"][j % 4]

            batch.append(
                (
                    uuid4(),
                    uuid4(),
                    event_type,
                    event_time,
                    {"ip": f"192.168.1.{j % 255}", "browser": "Chrome"},
                    {f"tag{j % 5}", f"category{j % 3}"},
                    [j * 0.1, j * 0.2, j * 0.3],
                    "completed" if j % 10 != 0 else "pending",
                )
            )

        # Execute batch
        for params in batch:
            await session.execute(insert_stmt, params)

        if (i + batch_size) % 1000 == 0:
            print(f"  Inserted {i + batch_size} rows...")

    print(f"Created {num_rows} events!")


async def checkpointed_export_example():
    """Demonstrate checkpointed export with resume capability."""
    output_dir = Path("export_output")
    output_dir.mkdir(exist_ok=True)

    checkpoint_file = output_dir / "export_checkpoint.json"
    output_file = output_dir / "events_large.csv"

    async with AsyncCluster(["localhost"]) as cluster:
        async with cluster.connect() as session:
            # Setup data
            await setup_large_dataset(session, num_rows=10000)

            operator = BulkOperator(session=session)

            # Check if we have a checkpoint
            resume_checkpoint = None
            if checkpoint_file.exists():
                print(f"\n🔄 Found checkpoint file: {checkpoint_file}")
                with open(checkpoint_file, "r") as f:
                    resume_checkpoint = json.load(f)
                print(f"  Resuming from: {resume_checkpoint['total_rows']} rows processed")

            # Define checkpoint callback
            async def save_checkpoint(state: dict):
                """Save checkpoint to file."""
                with open(checkpoint_file, "w") as f:
                    json.dump(state, f, indent=2)
                print(
                    f"  💾 Checkpoint saved: {state['total_rows']} rows, "
                    f"{len(state['completed_ranges'])} ranges completed"
                )

            # Progress tracking
            start_time = time.time()
            last_update = start_time

            def progress_callback(stats: BulkOperationStats):
                nonlocal last_update
                current_time = time.time()

                # Update every 2 seconds
                if current_time - last_update >= 2:
                    elapsed = current_time - start_time
                    eta = (
                        (elapsed / stats.progress_percentage * 100) - elapsed
                        if stats.progress_percentage > 0
                        else 0
                    )

                    print(
                        f"\r📊 Progress: {stats.progress_percentage:6.2f}% | "
                        f"Rows: {stats.rows_processed:,} | "
                        f"Rate: {stats.rows_per_second:,.0f} rows/s | "
                        f"ETA: {eta:.0f}s",
                        end="",
                        flush=True,
                    )

                    last_update = current_time

            # Export with checkpointing
            print("\n--- Starting Checkpointed Export ---")

            try:
                stats = await operator.export(
                    table="examples.events",
                    output_path=str(output_file),
                    format="csv",
                    concurrency=8,
                    batch_size=1000,
                    progress_callback=progress_callback,
                    checkpoint_interval=10,  # Checkpoint every 10 seconds
                    checkpoint_callback=save_checkpoint,
                    resume_from=resume_checkpoint,
                    options={
                        "writetime_columns": [
                            "event_type",
                            "status",
                        ],  # Include writetime for these columns
                    },
                )

                print("\n\n✅ Export completed successfully!")
                print(f"  - Total rows: {stats.rows_processed:,}")
                print(f"  - Duration: {stats.duration_seconds:.2f} seconds")
                print(f"  - Average rate: {stats.rows_per_second:,.0f} rows/second")
                print(f"  - Output file: {output_file}")

                # Clean up checkpoint
                if checkpoint_file.exists():
                    checkpoint_file.unlink()
                    print("  - Checkpoint file removed")

            except KeyboardInterrupt:
                print(f"\n\n⚠️  Export interrupted! Checkpoint saved to: {checkpoint_file}")
                print("Run the script again to resume from checkpoint.")
                raise
            except Exception as e:
                print(f"\n\n❌ Export failed: {e}")
                print(f"Checkpoint saved to: {checkpoint_file}")
                raise


async def custom_exporter_example():
    """Demonstrate custom exporter implementation."""
    output_dir = Path("export_output")
    output_dir.mkdir(exist_ok=True)

    async with AsyncCluster(["localhost"]) as cluster:
        async with cluster.connect() as session:
            await session.set_keyspace("examples")

            print("\n--- Custom TSV Exporter Example ---")

            # Create custom exporter
            tsv_path = output_dir / "events.tsv"
            exporter = TSVExporter(str(tsv_path))

            # Use with ParallelExporter directly
            parallel = ParallelExporter(
                session=session, table="events", exporter=exporter, concurrency=4, batch_size=500
            )

            print(f"Exporting to TSV format: {tsv_path}")

            stats = await parallel.export()

            print("\n✅ TSV Export completed!")
            print(f"  - Rows exported: {stats.rows_processed:,}")
            print(f"  - Duration: {stats.duration_seconds:.2f} seconds")

            # Show sample
            print("\nFirst 3 lines of TSV:")
            with open(tsv_path, "r") as f:
                for i, line in enumerate(f):
                    if i < 3:
                        print(f"  {line.strip()}")


async def writetime_export_example():
    """Demonstrate writetime export functionality."""
    output_dir = Path("export_output")
    output_dir.mkdir(exist_ok=True)

    async with AsyncCluster(["localhost"]) as cluster:
        async with cluster.connect() as session:
            await session.set_keyspace("examples")

            operator = BulkOperator(session=session)

            print("\n--- Writetime Export Examples ---")

            # Example 1: Export with writetime for specific columns
            output_file = output_dir / "events_with_writetime.csv"
            print(f"\n1. Exporting with writetime for specific columns to: {output_file}")

            stats = await operator.export(
                table="events",
                output_path=str(output_file),
                format="csv",
                options={
                    "writetime_columns": ["event_type", "status", "timestamp"],
                },
            )

            print(f"   ✓ Exported {stats.rows_processed:,} rows")

            # Show sample of output
            print("\n   Sample output (first 3 lines):")
            with open(output_file, "r") as f:
                import csv

                reader = csv.DictReader(f)
                for i, row in enumerate(reader):
                    if i < 3:
                        print(f"     Row {i+1}:")
                        print(f"       - event_type: {row.get('event_type')}")
                        print(f"       - event_type_writetime: {row.get('event_type_writetime')}")
                        print(f"       - status: {row.get('status')}")
                        print(f"       - status_writetime: {row.get('status_writetime')}")

            # Example 2: Export with writetime for all non-key columns
            output_file_json = output_dir / "events_all_writetime.json"
            print(f"\n2. Exporting with writetime for all columns to: {output_file_json}")

            stats = await operator.export(
                table="events",
                output_path=str(output_file_json),
                format="json",
                options={
                    "writetime_columns": ["*"],  # All non-key columns
                },
                json_options={
                    "mode": "array",  # Array of objects
                },
            )

            print(f"   ✓ Exported {stats.rows_processed:,} rows")

            # Show sample JSON
            print("\n   Sample JSON output (first record):")
            with open(output_file_json, "r") as f:
                data = json.load(f)
                if data:
                    first_row = data[0]
                    print(f"     ID: {first_row.get('id')}")
                    for key, value in first_row.items():
                        if key.endswith("_writetime"):
                            print(f"     {key}: {value}")


async def performance_tuning_example():
    """Demonstrate performance tuning options."""
    output_dir = Path("export_output")
    output_dir.mkdir(exist_ok=True)

    async with AsyncCluster(["localhost"]) as cluster:
        async with cluster.connect() as session:
            await session.set_keyspace("examples")

            operator = BulkOperator(session=session)

            print("\n--- Performance Tuning Comparison ---")

            # Test different configurations
            configs = [
                {"name": "Default", "concurrency": 4, "batch_size": 1000},
                {"name": "High Concurrency", "concurrency": 16, "batch_size": 1000},
                {"name": "Large Batches", "concurrency": 4, "batch_size": 5000},
                {"name": "Optimized", "concurrency": 8, "batch_size": 2500},
            ]

            for config in configs:
                output_file = (
                    output_dir / f"perf_test_{config['name'].lower().replace(' ', '_')}.csv"
                )

                print(f"\nTesting {config['name']}:")
                print(f"  - Concurrency: {config['concurrency']}")
                print(f"  - Batch size: {config['batch_size']}")

                start = time.time()

                stats = await operator.export(
                    table="events",
                    output_path=str(output_file),
                    format="csv",
                    concurrency=config["concurrency"],
                    batch_size=config["batch_size"],
                )

                duration = time.time() - start

                print(f"  - Duration: {duration:.2f} seconds")
                print(f"  - Rate: {stats.rows_per_second:,.0f} rows/second")

                # Clean up test file
                output_file.unlink()


async def main():
    """Run all examples."""
    print("=== Advanced Async Cassandra Bulk Export Examples ===\n")

    try:
        # Run checkpointed export
        await checkpointed_export_example()

        # Run custom exporter
        await custom_exporter_example()

        # Run writetime export
        await writetime_export_example()

        # Run performance comparison
        await performance_tuning_example()

        print("\n✅ All examples completed successfully!")

    except KeyboardInterrupt:
        print("\n\n⚠️  Examples interrupted by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
