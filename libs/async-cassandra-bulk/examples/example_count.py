#!/usr/bin/env python3
"""
Example: Token-aware bulk count operation.

This example demonstrates how to count all rows in a table
using token-aware parallel processing for maximum performance.
"""

import asyncio
import logging
import time

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TimeElapsedColumn
from rich.table import Table

from async_cassandra import AsyncCluster
from bulk_operations.bulk_operator import TokenAwareBulkOperator

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Rich console for pretty output
console = Console()


async def count_table_example():
    """Demonstrate token-aware counting of a large table."""

    # Connect to cluster
    console.print("[cyan]Connecting to Cassandra cluster...[/cyan]")

    async with AsyncCluster(contact_points=["localhost", "127.0.0.1"], port=9042) as cluster:
        session = await cluster.connect()
        # Create test data if needed
        console.print("[yellow]Setting up test keyspace and table...[/yellow]")

        # Create keyspace
        await session.execute(
            """
        CREATE KEYSPACE IF NOT EXISTS bulk_demo
        WITH replication = {
            'class': 'SimpleStrategy',
            'replication_factor': 3
        }
        """
        )

        # Create table
        await session.execute(
            """
        CREATE TABLE IF NOT EXISTS bulk_demo.large_table (
            partition_key INT,
            clustering_key INT,
            data TEXT,
            value DOUBLE,
            PRIMARY KEY (partition_key, clustering_key)
        )
        """
        )

        # Check if we need to insert test data
        result = await session.execute("SELECT COUNT(*) FROM bulk_demo.large_table LIMIT 1")
        current_count = result.one().count

        if current_count < 10000:
            console.print(
                f"[yellow]Table has {current_count} rows. " f"Inserting test data...[/yellow]"
            )

            # Insert some test data using prepared statement
            insert_stmt = await session.prepare(
                """
            INSERT INTO bulk_demo.large_table
            (partition_key, clustering_key, data, value)
            VALUES (?, ?, ?, ?)
        """
            )

            with Progress(
                SpinnerColumn(),
                *Progress.get_default_columns(),
                TimeElapsedColumn(),
                console=console,
            ) as progress:
                task = progress.add_task("[green]Inserting test data...", total=10000)

                for pk in range(100):
                    for ck in range(100):
                        await session.execute(
                            insert_stmt, (pk, ck, f"data-{pk}-{ck}", pk * ck * 0.1)
                        )
                        progress.update(task, advance=1)

        # Now demonstrate bulk counting
        console.print("\n[bold cyan]Token-Aware Bulk Count Demo[/bold cyan]\n")

        operator = TokenAwareBulkOperator(session)

        # Progress tracking
        stats_list = []

        def progress_callback(stats):
            """Track progress during operation."""
            stats_list.append(
                {
                    "rows": stats.rows_processed,
                    "ranges": stats.ranges_completed,
                    "total_ranges": stats.total_ranges,
                    "progress": stats.progress_percentage,
                    "rate": stats.rows_per_second,
                }
            )

        # Perform count with different split counts
        table = Table(title="Bulk Count Performance Comparison")
        table.add_column("Split Count", style="cyan")
        table.add_column("Total Rows", style="green")
        table.add_column("Duration (s)", style="yellow")
        table.add_column("Rows/Second", style="magenta")
        table.add_column("Ranges Processed", style="blue")

        for split_count in [1, 4, 8, 16, 32]:
            console.print(f"\n[cyan]Counting with {split_count} splits...[/cyan]")

            start_time = time.time()

            try:
                with Progress(
                    SpinnerColumn(),
                    *Progress.get_default_columns(),
                    TimeElapsedColumn(),
                    console=console,
                ) as progress:
                    current_task = progress.add_task(
                        f"[green]Counting with {split_count} splits...", total=100
                    )

                    # Track progress
                    last_progress = 0

                    def update_progress(stats, task=current_task):
                        nonlocal last_progress
                        progress.update(task, completed=int(stats.progress_percentage))
                        last_progress = stats.progress_percentage
                        progress_callback(stats)

                    count, final_stats = await operator.count_by_token_ranges_with_stats(
                        keyspace="bulk_demo",
                        table="large_table",
                        split_count=split_count,
                        progress_callback=update_progress,
                    )

                duration = time.time() - start_time

                table.add_row(
                    str(split_count),
                    f"{count:,}",
                    f"{duration:.2f}",
                    f"{final_stats.rows_per_second:,.0f}",
                    str(final_stats.ranges_completed),
                )

            except Exception as e:
                console.print(f"[red]Error: {e}[/red]")
                continue

        # Display results
        console.print("\n")
        console.print(table)

        # Show token range distribution
        console.print("\n[bold]Token Range Analysis:[/bold]")

        from bulk_operations.token_utils import discover_token_ranges

        ranges = await discover_token_ranges(session, "bulk_demo")

        range_table = Table(title="Natural Token Ranges")
        range_table.add_column("Range #", style="cyan")
        range_table.add_column("Start Token", style="green")
        range_table.add_column("End Token", style="yellow")
        range_table.add_column("Size", style="magenta")
        range_table.add_column("Replicas", style="blue")

        for i, r in enumerate(ranges[:5]):  # Show first 5
            range_table.add_row(
                str(i + 1), str(r.start), str(r.end), f"{r.size:,}", ", ".join(r.replicas)
            )

        if len(ranges) > 5:
            range_table.add_row("...", "...", "...", "...", "...")

        console.print(range_table)
        console.print(f"\nTotal natural ranges: {len(ranges)}")


if __name__ == "__main__":
    try:
        asyncio.run(count_table_example())
    except KeyboardInterrupt:
        console.print("\n[yellow]Operation cancelled by user[/yellow]")
    except Exception as e:
        console.print(f"\n[red]Error: {e}[/red]")
        logger.exception("Unexpected error")
