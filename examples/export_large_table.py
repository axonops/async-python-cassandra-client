#!/usr/bin/env python3
"""
Example of exporting a large Cassandra table to CSV using streaming.

This example demonstrates:
- Memory-efficient export of large tables
- Progress tracking during export
- Async file I/O with aiofiles
- Proper error handling

How to run:
-----------
1. Using Make (automatically starts Cassandra if needed):
   make example-export-large-table

2. With external Cassandra cluster:
   CASSANDRA_CONTACT_POINTS=10.0.0.1,10.0.0.2 make example-export-large-table

3. Direct Python execution:
   python examples/export_large_table.py

4. With custom contact points:
   CASSANDRA_CONTACT_POINTS=cassandra.example.com python examples/export_large_table.py

Environment variables:
- CASSANDRA_CONTACT_POINTS: Comma-separated list of contact points (default: localhost)
- CASSANDRA_PORT: Port number (default: 9042)
- EXAMPLE_OUTPUT_DIR: Directory for output files (default: examples/exampleoutput)
"""

import asyncio
import csv
import logging
import os
from datetime import datetime
from pathlib import Path

from async_cassandra import AsyncCluster, StreamConfig

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Note: aiofiles is optional - you can use sync file I/O if preferred
try:
    import aiofiles

    ASYNC_FILE_IO = True
except ImportError:
    ASYNC_FILE_IO = False
    logger.warning("aiofiles not installed - using synchronous file I/O")


async def count_table_rows(session, keyspace: str, table_name: str) -> int:
    """Count total rows in a table (approximate for large tables)."""
    # Note: COUNT(*) can be slow on large tables
    # Consider using token ranges for very large tables

    # For COUNT queries, we can't use prepared statements with dynamic table names
    # In production, consider implementing a token range count for large tables
    result = await session.execute(f"SELECT COUNT(*) FROM {keyspace}.{table_name}")
    return result.one()[0]


async def export_table_async(session, keyspace: str, table_name: str, output_file: str):
    """Export table using async file I/O (requires aiofiles)."""
    logger.info("\n" + "=" * 80)
    logger.info("📤 CSV EXPORT WITH ASYNC FILE I/O")
    logger.info("=" * 80)
    logger.info(f"\n📊 Exporting: {keyspace}.{table_name}")
    logger.info(f"💾 Output file: {output_file}")

    # Get approximate row count for progress tracking
    total_rows = await count_table_rows(session, keyspace, table_name)
    logger.info(f"📋 Table size: ~{total_rows:,} rows")

    # Configure streaming with progress callback
    rows_exported = 0

    def progress_callback(page_num: int, rows_so_far: int):
        nonlocal rows_exported
        rows_exported = rows_so_far
        if total_rows > 0:
            progress = (rows_so_far / total_rows) * 100
            bar_length = 40
            filled = int(bar_length * progress / 100)
            bar = "█" * filled + "░" * (bar_length - filled)
            logger.info(
                f"📊 Progress: [{bar}] {progress:.1f}% "
                f"({rows_so_far:,}/{total_rows:,} rows) - Page {page_num}"
            )

    config = StreamConfig(fetch_size=5000, page_callback=progress_callback)

    # Start streaming
    start_time = datetime.now()

    # CRITICAL: Use context manager for streaming to prevent memory leaks
    # For SELECT * with dynamic table names, we can't use prepared statements
    async with await session.execute_stream(
        f"SELECT * FROM {keyspace}.{table_name}", stream_config=config
    ) as result:
        # Export to CSV
        async with aiofiles.open(output_file, "w", newline="") as f:
            writer = None
            row_count = 0

            async for row in result:
                if writer is None:
                    # Write header on first row
                    fieldnames = row._fields
                    header = ",".join(fieldnames) + "\n"
                    await f.write(header)
                    writer = True  # Mark that header has been written

                # Write row data
                row_data = []
                for field in row._fields:
                    value = getattr(row, field)
                    # Handle special types
                    if value is None:
                        row_data.append("")
                    elif isinstance(value, (list, set)):
                        row_data.append(str(value))
                    elif isinstance(value, dict):
                        row_data.append(str(value))
                    elif isinstance(value, datetime):
                        row_data.append(value.isoformat())
                    else:
                        row_data.append(str(value))

                line = ",".join(row_data) + "\n"
                await f.write(line)
                row_count += 1

    elapsed = (datetime.now() - start_time).total_seconds()
    file_size_mb = os.path.getsize(output_file) / (1024 * 1024)

    logger.info("\n" + "─" * 80)
    logger.info("✅ EXPORT COMPLETED SUCCESSFULLY!")
    logger.info("─" * 80)
    logger.info("\n📊 Export Statistics:")
    logger.info(f"   • Rows exported: {row_count:,}")
    logger.info(f"   • Time elapsed: {elapsed:.2f} seconds")
    logger.info(f"   • Export rate: {row_count/elapsed:,.0f} rows/sec")
    logger.info(f"   • File size: {file_size_mb:.2f} MB ({os.path.getsize(output_file):,} bytes)")
    logger.info(f"   • Output path: {output_file}")


def export_table_sync(session, keyspace: str, table_name: str, output_file: str):
    """Export table using synchronous file I/O."""
    logger.info("\n" + "=" * 80)
    logger.info("📤 CSV EXPORT WITH SYNC FILE I/O")
    logger.info("=" * 80)
    logger.info(f"\n📊 Exporting: {keyspace}.{table_name}")
    logger.info(f"💾 Output file: {output_file}")

    async def _export():
        # Get approximate row count
        total_rows = await count_table_rows(session, keyspace, table_name)
        logger.info(f"📋 Table size: ~{total_rows:,} rows")

        # Configure streaming
        def sync_progress(page_num: int, rows_so_far: int):
            if total_rows > 0:
                progress = (rows_so_far / total_rows) * 100
                bar_length = 40
                filled = int(bar_length * progress / 100)
                bar = "█" * filled + "░" * (bar_length - filled)
                logger.info(
                    f"📊 Progress: [{bar}] {progress:.1f}% "
                    f"({rows_so_far:,}/{total_rows:,} rows) - Page {page_num}"
                )

        config = StreamConfig(fetch_size=5000, page_callback=sync_progress)

        start_time = datetime.now()

        # Use context manager for proper streaming cleanup
        # For SELECT * with dynamic table names, we can't use prepared statements
        async with await session.execute_stream(
            f"SELECT * FROM {keyspace}.{table_name}", stream_config=config
        ) as result:
            # Export to CSV synchronously
            with open(output_file, "w", newline="") as f:
                writer = None
                row_count = 0

                async for row in result:
                    if writer is None:
                        # Create CSV writer with field names
                        fieldnames = row._fields
                        writer = csv.DictWriter(f, fieldnames=fieldnames)
                        writer.writeheader()

                    # Convert row to dict and write
                    row_dict = {}
                    for field in row._fields:
                        value = getattr(row, field)
                        # Handle special types
                        if isinstance(value, (datetime,)):
                            row_dict[field] = value.isoformat()
                        elif isinstance(value, (list, set, dict)):
                            row_dict[field] = str(value)
                        else:
                            row_dict[field] = value

                    writer.writerow(row_dict)
                    row_count += 1

        elapsed = (datetime.now() - start_time).total_seconds()
        file_size_mb = os.path.getsize(output_file) / (1024 * 1024)

        logger.info("\n" + "─" * 80)
        logger.info("✅ EXPORT COMPLETED SUCCESSFULLY!")
        logger.info("─" * 80)
        logger.info("\n📊 Export Statistics:")
        logger.info(f"   • Rows exported: {row_count:,}")
        logger.info(f"   • Time elapsed: {elapsed:.2f} seconds")
        logger.info(f"   • Export rate: {row_count/elapsed:,.0f} rows/sec")
        logger.info(
            f"   • File size: {file_size_mb:.2f} MB ({os.path.getsize(output_file):,} bytes)"
        )
        logger.info(f"   • Output path: {output_file}")

    # Run the async export function
    return _export()


async def setup_sample_data(session):
    """Create sample table with data for testing."""
    logger.info("\n🛠️  Setting up sample data...")

    # Create keyspace
    await session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS export_example
        WITH REPLICATION = {
            'class': 'SimpleStrategy',
            'replication_factor': 1
        }
    """
    )

    # Create table
    await session.execute(
        """
        CREATE TABLE IF NOT EXISTS export_example.products (
            category text,
            product_id int,
            name text,
            price decimal,
            in_stock boolean,
            tags list<text>,
            attributes map<text, text>,
            created_at timestamp,
            PRIMARY KEY (category, product_id)
        )
    """
    )

    # Insert sample data
    insert_stmt = await session.prepare(
        """
        INSERT INTO export_example.products (
            category, product_id, name, price, in_stock,
            tags, attributes, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
    )

    categories = ["electronics", "books", "clothing", "food", "toys"]

    # Insert 5000 products
    batch_size = 100
    total_products = 5000

    for i in range(0, total_products, batch_size):
        tasks = []
        for j in range(batch_size):
            if i + j >= total_products:
                break

            product_id = i + j
            category = categories[product_id % len(categories)]

            tasks.append(
                session.execute(
                    insert_stmt,
                    [
                        category,
                        product_id,
                        f"Product {product_id}",
                        19.99 + (product_id % 100),
                        product_id % 2 == 0,  # 50% in stock
                        [f"tag{product_id % 3}", f"tag{product_id % 5}"],
                        {"color": f"color{product_id % 10}", "size": f"size{product_id % 4}"},
                        datetime.now(),
                    ],
                )
            )

        await asyncio.gather(*tasks)

    logger.info(f"✅ Created {total_products:,} sample products in 'export_example.products' table")


async def main():
    """Run the export example."""
    # Get contact points from environment or use localhost
    contact_points = os.environ.get("CASSANDRA_CONTACT_POINTS", "localhost").split(",")
    port = int(os.environ.get("CASSANDRA_PORT", "9042"))

    logger.info(f"Connecting to Cassandra at {contact_points}:{port}")

    # Connect to Cassandra using context manager
    async with AsyncCluster(contact_points, port=port) as cluster:
        async with await cluster.connect() as session:
            # Setup sample data
            await setup_sample_data(session)

            # Create output directory
            output_dir = Path(os.environ.get("EXAMPLE_OUTPUT_DIR", "examples/exampleoutput"))
            output_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Output directory: {output_dir}")

            # Export using async I/O if available
            if ASYNC_FILE_IO:
                await export_table_async(
                    session, "export_example", "products", str(output_dir / "products_async.csv")
                )
            else:
                await export_table_sync(
                    session, "export_example", "products", str(output_dir / "products_sync.csv")
                )

            # Cleanup (optional)
            logger.info("\n🧹 Cleaning up...")
            await session.execute("DROP KEYSPACE export_example")
            logger.info("✅ Keyspace dropped")


if __name__ == "__main__":
    asyncio.run(main())
