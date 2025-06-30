#!/usr/bin/env python3
"""
Example of exporting a large Cassandra table to CSV using streaming.

This example demonstrates:
- Memory-efficient export of large tables
- Progress tracking during export
- Async file I/O with aiofiles
- Proper error handling
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
    # Using system schema to validate table exists and avoid SQL injection
    validation_query = await session.execute(
        "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?",
        [keyspace, table_name],
    )
    if not validation_query.one():
        raise ValueError(f"Table {keyspace}.{table_name} does not exist")

    # Safe to use table name after validation - but still use qualified name
    # In production, consider using prepared statements even for COUNT queries
    result = await session.execute(f"SELECT COUNT(*) FROM {keyspace}.{table_name}")
    return result.one()[0]


async def export_table_async(session, keyspace: str, table_name: str, output_file: str):
    """Export table using async file I/O (requires aiofiles)."""
    logger.info(f"Starting async export of {keyspace}.{table_name} to {output_file}")

    # Get approximate row count for progress tracking
    total_rows = await count_table_rows(session, keyspace, table_name)
    logger.info(f"Table has approximately {total_rows:,} rows")

    # Configure streaming with progress callback
    rows_exported = 0

    def progress_callback(page_num: int, rows_so_far: int):
        nonlocal rows_exported
        rows_exported = rows_so_far
        if total_rows > 0:
            progress = (rows_so_far / total_rows) * 100
            logger.info(
                f"Export progress: {rows_so_far:,}/{total_rows:,} rows "
                f"({progress:.1f}%) - Page {page_num}"
            )

    config = StreamConfig(fetch_size=5000, page_callback=progress_callback)

    # Start streaming
    start_time = datetime.now()

    # CRITICAL: Use context manager for streaming to prevent memory leaks
    # Validate table exists before streaming
    validation_query = await session.execute(
        "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?",
        [keyspace, table_name],
    )
    if not validation_query.one():
        raise ValueError(f"Table {keyspace}.{table_name} does not exist")

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
    logger.info("\nExport completed:")
    logger.info(f"- Rows exported: {row_count:,}")
    logger.info(f"- Time elapsed: {elapsed:.2f} seconds")
    logger.info(f"- Export rate: {row_count/elapsed:.0f} rows/sec")
    logger.info(f"- Output file: {output_file}")
    logger.info(f"- File size: {os.path.getsize(output_file):,} bytes")


def export_table_sync(session, keyspace: str, table_name: str, output_file: str):
    """Export table using synchronous file I/O."""
    logger.info(f"Starting sync export of {keyspace}.{table_name} to {output_file}")

    async def _export():
        # Get approximate row count
        total_rows = await count_table_rows(session, keyspace, table_name)
        logger.info(f"Table has approximately {total_rows:,} rows")

        # Configure streaming
        config = StreamConfig(
            fetch_size=5000,
            page_callback=lambda p, t: (
                logger.info(f"Exported {t:,}/{total_rows:,} rows ({100*t/total_rows:.1f}%)")
                if total_rows > 0
                else None
            ),
        )

        start_time = datetime.now()

        # Use context manager for proper streaming cleanup
        # Validate table exists before streaming
        validation_query = await session.execute(
            "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?",
            [keyspace, table_name],
        )
        if not validation_query.one():
            raise ValueError(f"Table {keyspace}.{table_name} does not exist")

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
        logger.info("\nExport completed:")
        logger.info(f"- Rows exported: {row_count:,}")
        logger.info(f"- Time elapsed: {elapsed:.2f} seconds")
        logger.info(f"- Export rate: {row_count/elapsed:.0f} rows/sec")
        logger.info(f"- Output file: {output_file}")
        logger.info(f"- File size: {os.path.getsize(output_file):,} bytes")

    # Run the async export function
    return _export()


async def setup_sample_data(session):
    """Create sample table with data for testing."""
    logger.info("Setting up sample data...")

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

    await session.set_keyspace("export_example")

    # Create table
    await session.execute(
        """
        CREATE TABLE IF NOT EXISTS products (
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
        INSERT INTO products (
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

    logger.info(f"Created {total_products} sample products")


async def main():
    """Run the export example."""
    # Connect to Cassandra
    cluster = AsyncCluster(["localhost"])

    try:
        session = await cluster.connect()

        # Setup sample data
        await setup_sample_data(session)

        # Create output directory
        output_dir = Path("exports")
        output_dir.mkdir(exist_ok=True)

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
        logger.info("\nCleaning up...")
        await session.execute("DROP KEYSPACE export_example")

    finally:
        await cluster.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
