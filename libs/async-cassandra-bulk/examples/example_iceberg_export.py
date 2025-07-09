#!/usr/bin/env python3
"""Example: Export Cassandra data to Apache Iceberg tables.

This demonstrates the power of Apache Iceberg:
- ACID transactions on data lakes
- Schema evolution
- Time travel queries
- Hidden partitioning
- Integration with modern analytics tools
"""

import asyncio
import logging
from datetime import datetime, timedelta
from pathlib import Path

from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.transforms import DayTransform
from rich.console import Console
from rich.logging import RichHandler
from rich.panel import Panel
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn, TimeRemainingColumn
from rich.table import Table as RichTable

from async_cassandra import AsyncCluster
from bulk_operations.bulk_operator import TokenAwareBulkOperator
from bulk_operations.iceberg import IcebergExporter

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[RichHandler(console=Console(stderr=True))],
)
logger = logging.getLogger(__name__)


async def iceberg_export_demo():
    """Demonstrate Cassandra to Iceberg export with advanced features."""
    console = Console()

    # Header
    console.print(
        Panel.fit(
            "[bold cyan]Apache Iceberg Export Demo[/bold cyan]\n"
            "Exporting Cassandra data to modern data lakehouse format",
            border_style="cyan",
        )
    )

    # Connect to Cassandra
    console.print("\n[bold blue]1. Connecting to Cassandra...[/bold blue]")
    cluster = AsyncCluster(["localhost"])
    session = await cluster.connect()

    try:
        # Setup test data
        await setup_demo_data(session, console)

        # Create bulk operator
        operator = TokenAwareBulkOperator(session)

        # Configure Iceberg export
        warehouse_path = Path("iceberg_warehouse")
        console.print(
            f"\n[bold blue]2. Setting up Iceberg warehouse at:[/bold blue] {warehouse_path}"
        )

        # Create Iceberg exporter
        exporter = IcebergExporter(
            operator=operator,
            warehouse_path=warehouse_path,
            compression="snappy",
            row_group_size=10000,
        )

        # Example 1: Basic export
        console.print("\n[bold green]Example 1: Basic Iceberg Export[/bold green]")
        console.print("   • Creates Iceberg table from Cassandra schema")
        console.print("   • Writes data in Parquet format")
        console.print("   • Enables ACID transactions")

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Exporting to Iceberg...", total=100)

            def iceberg_progress(export_progress):
                progress.update(
                    task,
                    completed=export_progress.progress_percentage,
                    description=f"Iceberg: {export_progress.rows_exported:,} rows",
                )

            result = await exporter.export(
                keyspace="iceberg_demo",
                table="user_events",
                namespace="cassandra_export",
                table_name="user_events",
                progress_callback=iceberg_progress,
            )

        console.print(f"✓ Exported {result.rows_exported:,} rows to Iceberg")
        console.print("  Table: iceberg://cassandra_export.user_events")

        # Example 2: Partitioned export
        console.print("\n[bold green]Example 2: Partitioned Iceberg Table[/bold green]")
        console.print("   • Partitions by day for efficient queries")
        console.print("   • Hidden partitioning (no query changes needed)")
        console.print("   • Automatic partition pruning")

        # Create partition spec (partition by day)
        partition_spec = PartitionSpec(
            PartitionField(
                source_id=4,  # event_time field ID
                field_id=1000,
                transform=DayTransform(),
                name="event_day",
            )
        )

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Exporting with partitions...", total=100)

            def partition_progress(export_progress):
                progress.update(
                    task,
                    completed=export_progress.progress_percentage,
                    description=f"Partitioned: {export_progress.rows_exported:,} rows",
                )

            result = await exporter.export(
                keyspace="iceberg_demo",
                table="user_events",
                namespace="cassandra_export",
                table_name="user_events_partitioned",
                partition_spec=partition_spec,
                progress_callback=partition_progress,
            )

        console.print("✓ Created partitioned Iceberg table")
        console.print("  Partitioned by: event_day (daily partitions)")

        # Show Iceberg features
        console.print("\n[bold cyan]Iceberg Features Enabled:[/bold cyan]")
        features = RichTable(show_header=True, header_style="bold magenta")
        features.add_column("Feature", style="cyan")
        features.add_column("Description", style="green")
        features.add_column("Example Query")

        features.add_row(
            "Time Travel",
            "Query data at any point in time",
            "SELECT * FROM table AS OF '2025-01-01'",
        )
        features.add_row(
            "Schema Evolution",
            "Add/drop/rename columns safely",
            "ALTER TABLE table ADD COLUMN new_field STRING",
        )
        features.add_row(
            "Hidden Partitioning",
            "Partition pruning without query changes",
            "WHERE event_time > '2025-01-01' -- uses partitions",
        )
        features.add_row(
            "ACID Transactions",
            "Atomic commits and rollbacks",
            "Multiple concurrent writers supported",
        )
        features.add_row(
            "Incremental Processing",
            "Process only new data",
            "Read incrementally from snapshot N to M",
        )

        console.print(features)

        # Explain the power of Iceberg
        console.print(
            Panel(
                "[bold yellow]Why Apache Iceberg Matters:[/bold yellow]\n\n"
                "• [cyan]Netflix Scale:[/cyan] Created by Netflix to handle petabytes\n"
                "• [cyan]Open Format:[/cyan] Works with Spark, Trino, Flink, and more\n"
                "• [cyan]Cloud Native:[/cyan] Designed for S3, GCS, Azure storage\n"
                "• [cyan]Performance:[/cyan] Faster than traditional data lakes\n"
                "• [cyan]Reliability:[/cyan] ACID guarantees prevent data corruption\n\n"
                "[bold green]Your Cassandra data is now ready for:[/bold green]\n"
                "• Analytics with Spark or Trino\n"
                "• Machine learning pipelines\n"
                "• Data warehousing with Snowflake/BigQuery\n"
                "• Real-time processing with Flink",
                title="[bold red]The Modern Data Lakehouse[/bold red]",
                border_style="yellow",
            )
        )

        # Show next steps
        console.print("\n[bold blue]Next Steps:[/bold blue]")
        console.print(
            "1. Query with Spark: spark.read.format('iceberg').load('cassandra_export.user_events')"
        )
        console.print(
            "2. Time travel: SELECT * FROM user_events FOR SYSTEM_TIME AS OF '2025-01-01'"
        )
        console.print("3. Schema evolution: ALTER TABLE user_events ADD COLUMNS (score DOUBLE)")
        console.print(f"4. Explore warehouse: {warehouse_path}/")

    finally:
        await session.close()
        await cluster.shutdown()


async def setup_demo_data(session, console):
    """Create demo keyspace and data."""
    console.print("\n[bold blue]Setting up demo data...[/bold blue]")

    # Create keyspace
    await session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS iceberg_demo
        WITH replication = {
            'class': 'SimpleStrategy',
            'replication_factor': 1
        }
    """
    )

    # Create table with various data types
    await session.execute(
        """
        CREATE TABLE IF NOT EXISTS iceberg_demo.user_events (
            user_id UUID,
            event_id UUID,
            event_type TEXT,
            event_time TIMESTAMP,
            properties MAP<TEXT, TEXT>,
            metrics MAP<TEXT, DOUBLE>,
            tags SET<TEXT>,
            is_processed BOOLEAN,
            score DECIMAL,
            PRIMARY KEY (user_id, event_time, event_id)
        ) WITH CLUSTERING ORDER BY (event_time DESC, event_id ASC)
    """
    )

    # Check if data exists
    result = await session.execute("SELECT COUNT(*) FROM iceberg_demo.user_events")
    count = result.one().count

    if count < 10000:
        console.print("   Inserting sample events...")
        insert_stmt = await session.prepare(
            """
            INSERT INTO iceberg_demo.user_events
            (user_id, event_id, event_type, event_time, properties,
             metrics, tags, is_processed, score)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        )

        # Insert events over the last 30 days
        import uuid
        from decimal import Decimal

        base_time = datetime.now() - timedelta(days=30)
        event_types = ["login", "purchase", "view", "click", "share", "logout"]

        for i in range(10000):
            user_id = uuid.UUID(f"00000000-0000-0000-0000-{i % 100:012d}")
            event_time = base_time + timedelta(minutes=i * 5)

            await session.execute(
                insert_stmt,
                (
                    user_id,
                    uuid.uuid4(),
                    event_types[i % len(event_types)],
                    event_time,
                    {"device": "mobile", "version": "2.0"} if i % 3 == 0 else {},
                    {"duration": float(i % 300), "count": float(i % 10)},
                    {f"tag{i % 5}", f"category{i % 3}"},
                    i % 10 != 0,  # 90% processed
                    Decimal(str(0.1 * (i % 100))),
                ),
            )

        console.print("   ✓ Created 10,000 events across 100 users")


if __name__ == "__main__":
    asyncio.run(iceberg_export_demo())
