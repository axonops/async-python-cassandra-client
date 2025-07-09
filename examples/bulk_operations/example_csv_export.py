#!/usr/bin/env python3
"""
Example: Export Cassandra table to CSV format.

This demonstrates:
- Basic CSV export
- Compressed CSV export
- Custom delimiters and NULL handling
- Progress tracking
- Resume capability
"""

import asyncio
import logging
from pathlib import Path

from rich.console import Console
from rich.logging import RichHandler
from rich.progress import Progress, SpinnerColumn, TextColumn
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


async def export_examples():
    """Run various CSV export examples."""
    console = Console()

    # Connect to Cassandra
    console.print("\n[bold blue]Connecting to Cassandra...[/bold blue]")
    cluster = AsyncCluster(["localhost"])
    session = await cluster.connect()

    try:
        # Ensure test data exists
        await setup_test_data(session)

        # Create bulk operator
        operator = TokenAwareBulkOperator(session)

        # Example 1: Basic CSV export
        console.print("\n[bold green]Example 1: Basic CSV Export[/bold green]")
        output_path = Path("exports/products.csv")
        output_path.parent.mkdir(exist_ok=True)

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Exporting to CSV...", total=None)

            def progress_callback(export_progress):
                progress.update(
                    task,
                    description=f"Exported {export_progress.rows_exported:,} rows "
                    f"({export_progress.progress_percentage:.1f}%)",
                )

            result = await operator.export_to_csv(
                keyspace="bulk_demo",
                table="products",
                output_path=output_path,
                progress_callback=progress_callback,
            )

        console.print(f"✓ Exported {result.rows_exported:,} rows to {output_path}")
        console.print(f"  File size: {result.bytes_written:,} bytes")

        # Example 2: Compressed CSV with custom delimiter
        console.print("\n[bold green]Example 2: Compressed Tab-Delimited Export[/bold green]")
        output_path = Path("exports/products_tab.csv")

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Exporting compressed CSV...", total=None)

            def progress_callback(export_progress):
                progress.update(
                    task,
                    description=f"Exported {export_progress.rows_exported:,} rows",
                )

            result = await operator.export_to_csv(
                keyspace="bulk_demo",
                table="products",
                output_path=output_path,
                delimiter="\t",
                compression="gzip",
                progress_callback=progress_callback,
            )

        console.print(f"✓ Exported to {output_path}.gzip")
        console.print(f"  Compressed size: {result.bytes_written:,} bytes")

        # Example 3: Export with specific columns and NULL handling
        console.print("\n[bold green]Example 3: Selective Column Export[/bold green]")
        output_path = Path("exports/products_summary.csv")

        result = await operator.export_to_csv(
            keyspace="bulk_demo",
            table="products",
            output_path=output_path,
            columns=["id", "name", "price", "category"],
            null_string="NULL",
        )

        console.print(f"✓ Exported {result.rows_exported:,} rows (selected columns)")

        # Show export summary
        console.print("\n[bold cyan]Export Summary:[/bold cyan]")
        summary_table = Table(show_header=True, header_style="bold magenta")
        summary_table.add_column("Export", style="cyan")
        summary_table.add_column("Format", style="green")
        summary_table.add_column("Rows", justify="right")
        summary_table.add_column("Size", justify="right")
        summary_table.add_column("Compression")

        summary_table.add_row(
            "products.csv",
            "CSV",
            "10,000",
            "~500 KB",
            "None",
        )
        summary_table.add_row(
            "products_tab.csv.gzip",
            "TSV",
            "10,000",
            "~150 KB",
            "gzip",
        )
        summary_table.add_row(
            "products_summary.csv",
            "CSV",
            "10,000",
            "~300 KB",
            "None",
        )

        console.print(summary_table)

        # Example 4: Demonstrate resume capability
        console.print("\n[bold green]Example 4: Resume Capability[/bold green]")
        console.print("Progress files saved at:")
        for csv_file in Path("exports").glob("*.csv"):
            progress_file = csv_file.with_suffix(".csv.progress")
            if progress_file.exists():
                console.print(f"  • {progress_file}")

    finally:
        await session.close()
        await cluster.shutdown()


async def setup_test_data(session):
    """Create test keyspace and data if not exists."""
    # Create keyspace
    await session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS bulk_demo
        WITH replication = {
            'class': 'SimpleStrategy',
            'replication_factor': 1
        }
    """
    )

    # Create table
    await session.execute(
        """
        CREATE TABLE IF NOT EXISTS bulk_demo.products (
            id INT PRIMARY KEY,
            name TEXT,
            description TEXT,
            price DECIMAL,
            category TEXT,
            in_stock BOOLEAN,
            tags SET<TEXT>,
            attributes MAP<TEXT, TEXT>,
            created_at TIMESTAMP
        )
    """
    )

    # Check if data exists
    result = await session.execute("SELECT COUNT(*) FROM bulk_demo.products")
    count = result.one().count

    if count < 10000:
        logger.info("Inserting test data...")
        insert_stmt = await session.prepare(
            """
            INSERT INTO bulk_demo.products
            (id, name, description, price, category, in_stock, tags, attributes, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, toTimestamp(now()))
        """
        )

        # Insert in batches
        for i in range(10000):
            await session.execute(
                insert_stmt,
                (
                    i,
                    f"Product {i}",
                    f"Description for product {i}" if i % 3 != 0 else None,
                    float(10 + (i % 1000) * 0.1),
                    ["Electronics", "Books", "Clothing", "Food"][i % 4],
                    i % 5 != 0,  # 80% in stock
                    {"tag1", f"tag{i % 10}"} if i % 2 == 0 else None,
                    {"color": ["red", "blue", "green"][i % 3], "size": "M"} if i % 4 == 0 else {},
                ),
            )


if __name__ == "__main__":
    asyncio.run(export_examples())
