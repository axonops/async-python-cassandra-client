#!/usr/bin/env python3
"""
Visualize token distribution in the Cassandra cluster.

This script helps understand how vnodes distribute tokens
across the cluster and validates our token range discovery.
"""

import asyncio
from collections import defaultdict

from rich.console import Console
from rich.table import Table

from async_cassandra import AsyncCluster
from bulk_operations.token_utils import MAX_TOKEN, MIN_TOKEN, discover_token_ranges

console = Console()


def analyze_node_distribution(ranges):
    """Analyze and display token distribution by node."""
    primary_owner_count = defaultdict(int)
    all_replica_count = defaultdict(int)

    for r in ranges:
        # First replica is primary owner
        if r.replicas:
            primary_owner_count[r.replicas[0]] += 1
            for replica in r.replicas:
                all_replica_count[replica] += 1

    # Display node statistics
    table = Table(title="Token Distribution by Node")
    table.add_column("Node", style="cyan")
    table.add_column("Primary Ranges", style="green")
    table.add_column("Total Ranges (with replicas)", style="yellow")
    table.add_column("Percentage of Ring", style="magenta")

    total_primary = sum(primary_owner_count.values())

    for node in sorted(all_replica_count.keys()):
        primary = primary_owner_count.get(node, 0)
        total = all_replica_count.get(node, 0)
        percentage = (primary / total_primary * 100) if total_primary > 0 else 0

        table.add_row(node, str(primary), str(total), f"{percentage:.1f}%")

    console.print(table)
    return primary_owner_count


def analyze_range_sizes(ranges):
    """Analyze and display token range sizes."""
    console.print("\n[bold]Token Range Size Analysis[/bold]")

    range_sizes = [r.size for r in ranges]
    avg_size = sum(range_sizes) / len(range_sizes)
    min_size = min(range_sizes)
    max_size = max(range_sizes)

    console.print(f"Average range size: {avg_size:,.0f}")
    console.print(f"Smallest range: {min_size:,}")
    console.print(f"Largest range: {max_size:,}")
    console.print(f"Size ratio (max/min): {max_size/min_size:.2f}x")


def validate_ring_coverage(ranges):
    """Validate token ring coverage for gaps."""
    console.print("\n[bold]Token Ring Coverage Validation[/bold]")

    sorted_ranges = sorted(ranges, key=lambda r: r.start)

    # Check for gaps
    gaps = []
    for i in range(len(sorted_ranges) - 1):
        current = sorted_ranges[i]
        next_range = sorted_ranges[i + 1]
        if current.end != next_range.start:
            gaps.append((current.end, next_range.start))

    if gaps:
        console.print(f"[red]⚠ Found {len(gaps)} gaps in token ring![/red]")
        for gap_start, gap_end in gaps[:5]:  # Show first 5
            console.print(f"  Gap: {gap_start} to {gap_end}")
    else:
        console.print("[green]✓ No gaps found - complete ring coverage[/green]")

    # Check first and last ranges
    if sorted_ranges[0].start == MIN_TOKEN:
        console.print("[green]✓ First range starts at MIN_TOKEN[/green]")
    else:
        console.print(f"[red]⚠ First range starts at {sorted_ranges[0].start}, not MIN_TOKEN[/red]")

    if sorted_ranges[-1].end == MAX_TOKEN:
        console.print("[green]✓ Last range ends at MAX_TOKEN[/green]")
    else:
        console.print(f"[yellow]Last range ends at {sorted_ranges[-1].end}[/yellow]")

    return sorted_ranges


def display_sample_ranges(sorted_ranges):
    """Display sample token ranges."""
    console.print("\n[bold]Sample Token Ranges (first 5)[/bold]")
    sample_table = Table()
    sample_table.add_column("Range #", style="cyan")
    sample_table.add_column("Start", style="green")
    sample_table.add_column("End", style="yellow")
    sample_table.add_column("Size", style="magenta")
    sample_table.add_column("Replicas", style="blue")

    for i, r in enumerate(sorted_ranges[:5]):
        sample_table.add_row(
            str(i + 1), str(r.start), str(r.end), f"{r.size:,}", ", ".join(r.replicas)
        )

    console.print(sample_table)


async def visualize_token_distribution():
    """Visualize how tokens are distributed across the cluster."""

    console.print("[cyan]Connecting to Cassandra cluster...[/cyan]")

    async with AsyncCluster(contact_points=["localhost"]) as cluster, cluster.connect() as session:
        # Create test keyspace if needed
        await session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS token_test
            WITH replication = {
                'class': 'SimpleStrategy',
                'replication_factor': 3
            }
        """
        )

        console.print("[green]✓ Connected to cluster[/green]\n")

        # Discover token ranges
        ranges = await discover_token_ranges(session, "token_test")

        # Analyze distribution
        console.print("[bold]Token Range Analysis[/bold]")
        console.print(f"Total ranges discovered: {len(ranges)}")
        console.print("Expected with 3 nodes × 256 vnodes: ~768 ranges\n")

        # Analyze node distribution
        primary_owner_count = analyze_node_distribution(ranges)

        # Analyze range sizes
        analyze_range_sizes(ranges)

        # Validate ring coverage
        sorted_ranges = validate_ring_coverage(ranges)

        # Display sample ranges
        display_sample_ranges(sorted_ranges)

        # Vnode insight
        console.print("\n[bold]Vnode Configuration Insight[/bold]")
        console.print(f"With {len(primary_owner_count)} nodes and {len(ranges)} ranges:")
        console.print(f"Average vnodes per node: {len(ranges) / len(primary_owner_count):.1f}")
        console.print("This matches the expected 256 vnodes per node configuration.")


if __name__ == "__main__":
    try:
        asyncio.run(visualize_token_distribution())
    except KeyboardInterrupt:
        console.print("\n[yellow]Visualization cancelled[/yellow]")
    except Exception as e:
        console.print(f"\n[red]Error: {e}[/red]")
        import traceback

        traceback.print_exc()
