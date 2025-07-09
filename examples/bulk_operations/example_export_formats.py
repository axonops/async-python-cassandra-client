#!/usr/bin/env python3
"""
Example: Export Cassandra data to multiple formats.

This demonstrates exporting to:
- CSV (with compression)
- JSON (line-delimited and array)
- Parquet (foundation for Iceberg)

Shows why Parquet is critical for the Iceberg integration.
"""

import asyncio
import logging
from pathlib import Path

from rich.console import Console
from rich.logging import RichHandler
from rich.panel import Panel
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn, TimeRemainingColumn
from rich.table import Table

from async_cassandra import AsyncCluster
from bulk_operations.bulk_operator import TokenAwareBulkOperator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[RichHandler(console=Console(stderr=True))],
)
logger = logging.getLogger(__name__)


async def export_format_examples():
    """Demonstrate all export formats."""
    console = Console()

    # Header
    console.print(
        Panel.fit(
            "[bold cyan]Cassandra Bulk Export Examples[/bold cyan]\n"
            "Exporting to CSV, JSON, and Parquet formats",
            border_style="cyan",
        )
    )

    # Connect to Cassandra
    console.print("\n[bold blue]Connecting to Cassandra...[/bold blue]")
    cluster = AsyncCluster(["localhost"])
    session = await cluster.connect()

    try:
        # Setup test data
        await setup_test_data(session)

        # Create bulk operator
        operator = TokenAwareBulkOperator(session)

        # Create exports directory
        exports_dir = Path("exports")
        exports_dir.mkdir(exist_ok=True)

        # Export to different formats
        results = {}

        # 1. CSV Export
        console.print("\n[bold green]1. CSV Export (Universal Format)[/bold green]")
        console.print("   • Human readable")
        console.print("   • Compatible with Excel, databases, etc.")
        console.print("   • Good for data exchange")

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Exporting to CSV...", total=100)

            def csv_progress(export_progress):
                progress.update(
                    task,
                    completed=export_progress.progress_percentage,
                    description=f"CSV: {export_progress.rows_exported:,} rows",
                )

            results["csv"] = await operator.export_to_csv(
                keyspace="export_demo",
                table="events",
                output_path=exports_dir / "events.csv",
                compression="gzip",
                progress_callback=csv_progress,
            )

        # 2. JSON Export (Line-delimited)
        console.print("\n[bold green]2. JSON Export (Streaming Format)[/bold green]")
        console.print("   • Preserves data types")
        console.print("   • Works with streaming tools")
        console.print("   • Good for data pipelines")

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Exporting to JSONL...", total=100)

            def json_progress(export_progress):
                progress.update(
                    task,
                    completed=export_progress.progress_percentage,
                    description=f"JSON: {export_progress.rows_exported:,} rows",
                )

            results["json"] = await operator.export_to_json(
                keyspace="export_demo",
                table="events",
                output_path=exports_dir / "events.jsonl",
                format_mode="jsonl",
                compression="gzip",
                progress_callback=json_progress,
            )

        # 3. Parquet Export (Foundation for Iceberg)
        console.print("\n[bold yellow]3. Parquet Export (CRITICAL for Iceberg)[/bold yellow]")
        console.print("   • Columnar format for analytics")
        console.print("   • Excellent compression")
        console.print("   • Schema included in file")
        console.print("   • [bold red]This is what Iceberg uses![/bold red]")

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Exporting to Parquet...", total=100)

            def parquet_progress(export_progress):
                progress.update(
                    task,
                    completed=export_progress.progress_percentage,
                    description=f"Parquet: {export_progress.rows_exported:,} rows",
                )

            results["parquet"] = await operator.export_to_parquet(
                keyspace="export_demo",
                table="events",
                output_path=exports_dir / "events.parquet",
                compression="snappy",
                row_group_size=10000,
                progress_callback=parquet_progress,
            )

        # Show results comparison
        console.print("\n[bold cyan]Export Results Comparison:[/bold cyan]")
        comparison = Table(show_header=True, header_style="bold magenta")
        comparison.add_column("Format", style="cyan")
        comparison.add_column("File", style="green")
        comparison.add_column("Size", justify="right")
        comparison.add_column("Rows", justify="right")
        comparison.add_column("Time", justify="right")

        for format_name, result in results.items():
            file_path = Path(result.output_path)
            if format_name != "parquet" and result.metadata.get("compression"):
                file_path = file_path.with_suffix(
                    file_path.suffix + f".{result.metadata['compression']}"
                )

            size_mb = result.bytes_written / (1024 * 1024)
            duration = (result.completed_at - result.started_at).total_seconds()

            comparison.add_row(
                format_name.upper(),
                file_path.name,
                f"{size_mb:.1f} MB",
                f"{result.rows_exported:,}",
                f"{duration:.1f}s",
            )

        console.print(comparison)

        # Explain Parquet importance
        console.print(
            Panel(
                "[bold yellow]Why Parquet Matters for Iceberg:[/bold yellow]\n\n"
                "• Iceberg tables store data in Parquet files\n"
                "• Columnar format enables fast analytics queries\n"
                "• Built-in schema makes evolution easier\n"
                "• Compression reduces storage costs\n"
                "• Row groups enable efficient filtering\n\n"
                "[bold cyan]Next Phase:[/bold cyan] These Parquet files will become "
                "Iceberg table data files!",
                title="[bold red]The Path to Iceberg[/bold red]",
                border_style="yellow",
            )
        )

    finally:
        await session.close()
        await cluster.shutdown()


async def setup_test_data(session):
    """Create test keyspace and data."""
    # Create keyspace
    await session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS export_demo
        WITH replication = {
            'class': 'SimpleStrategy',
            'replication_factor': 1
        }
    """
    )

    # Create events table with various data types
    await session.execute(
        """
        CREATE TABLE IF NOT EXISTS export_demo.events (
            event_id UUID PRIMARY KEY,
            event_type TEXT,
            user_id INT,
            timestamp TIMESTAMP,
            properties MAP<TEXT, TEXT>,
            tags SET<TEXT>,
            metrics LIST<DOUBLE>,
            is_processed BOOLEAN,
            processing_time DECIMAL
        )
    """
    )

    # Check if data exists
    result = await session.execute("SELECT COUNT(*) FROM export_demo.events")
    count = result.one().count

    if count < 50000:
        logger.info("Inserting test events...")
        insert_stmt = await session.prepare(
            """
            INSERT INTO export_demo.events
            (event_id, event_type, user_id, timestamp, properties,
             tags, metrics, is_processed, processing_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        )

        # Insert test events
        import uuid
        from datetime import datetime, timedelta
        from decimal import Decimal

        base_time = datetime.now() - timedelta(days=30)
        event_types = ["login", "purchase", "view", "click", "logout"]

        for i in range(50000):
            event_time = base_time + timedelta(seconds=i * 60)

            await session.execute(
                insert_stmt,
                (
                    uuid.uuid4(),
                    event_types[i % len(event_types)],
                    i % 1000,  # user_id
                    event_time,
                    {"source": "web", "version": "2.0"} if i % 3 == 0 else {},
                    {f"tag{i % 5}", f"cat{i % 3}"} if i % 2 == 0 else None,
                    [float(i), float(i * 0.1), float(i * 0.01)] if i % 4 == 0 else None,
                    i % 10 != 0,  # 90% processed
                    Decimal(str(0.001 * (i % 1000))),
                ),
            )


if __name__ == "__main__":
    asyncio.run(export_format_examples())
