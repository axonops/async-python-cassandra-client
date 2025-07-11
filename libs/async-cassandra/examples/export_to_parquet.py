#!/usr/bin/env python3
"""
Export large Cassandra tables to Parquet format efficiently.

This example demonstrates:
- Memory-efficient streaming of large result sets
- Exporting data to Parquet format without loading entire dataset in memory
- Progress tracking during export
- Schema inference from Cassandra data
- Handling different data types
- Batch writing for optimal performance

How to run:
-----------
1. Using Make (automatically starts Cassandra if needed):
   make example-export-parquet

2. With external Cassandra cluster:
   CASSANDRA_CONTACT_POINTS=10.0.0.1,10.0.0.2 make example-export-parquet

3. Direct Python execution:
   python examples/export_to_parquet.py

4. With custom contact points:
   CASSANDRA_CONTACT_POINTS=cassandra.example.com python examples/export_to_parquet.py

Environment variables:
- CASSANDRA_CONTACT_POINTS: Comma-separated list of contact points (default: localhost)
- CASSANDRA_PORT: Port number (default: 9042)
- EXAMPLE_OUTPUT_DIR: Directory for output files (default: examples/exampleoutput)
"""

import asyncio
import logging
import os
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional

import pyarrow as pa
import pyarrow.parquet as pq
from async_cassandra import AsyncCluster, StreamConfig

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ParquetExporter:
    """Export Cassandra tables to Parquet format with streaming."""

    def __init__(self, output_dir: str = "parquet_exports"):
        """
        Initialize the exporter.

        Args:
            output_dir: Directory to save Parquet files
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def infer_arrow_type(cassandra_type: Any) -> pa.DataType:
        """
        Infer PyArrow data type from Cassandra column type.

        Args:
            cassandra_type: Cassandra column type

        Returns:
            Corresponding PyArrow data type
        """
        # Map common Cassandra types to PyArrow types
        type_name = str(cassandra_type).lower()

        if "text" in type_name or "varchar" in type_name or "ascii" in type_name:
            return pa.string()
        elif "int" in type_name and "big" in type_name:
            return pa.int64()
        elif "int" in type_name:
            return pa.int32()
        elif "float" in type_name:
            return pa.float32()
        elif "double" in type_name:
            return pa.float64()
        elif "decimal" in type_name:
            return pa.decimal128(38, 10)  # Default precision/scale
        elif "boolean" in type_name:
            return pa.bool_()
        elif "timestamp" in type_name:
            return pa.timestamp("ms")
        elif "date" in type_name:
            return pa.date32()
        elif "time" in type_name:
            return pa.time64("ns")
        elif "uuid" in type_name:
            return pa.string()  # Store UUIDs as strings
        elif "blob" in type_name:
            return pa.binary()
        else:
            # Default to string for unknown types
            return pa.string()

    async def export_table(
        self,
        session,
        table_name: str,
        keyspace: str,
        fetch_size: int = 10000,
        row_group_size: int = 50000,
        where_clause: Optional[str] = None,
        compression: str = "snappy",
    ) -> Dict[str, Any]:
        """
        Export a Cassandra table to Parquet format.

        Args:
            session: AsyncCassandraSession instance
            table_name: Name of the table to export
            keyspace: Keyspace containing the table
            fetch_size: Number of rows to fetch per page
            row_group_size: Number of rows per Parquet row group
            where_clause: Optional WHERE clause for filtering
            compression: Parquet compression codec

        Returns:
            Export statistics
        """
        start_time = datetime.now()
        output_file = self.output_dir / f"{keyspace}.{table_name}.parquet"
        temp_file = self.output_dir / f"{keyspace}.{table_name}.parquet.tmp"

        logger.info(f"\n🎯 Starting export of {keyspace}.{table_name}")
        logger.info(f"📄 Output: {output_file}")
        logger.info(f"🗜️  Compression: {compression}")

        # Build query
        query = f"SELECT * FROM {keyspace}.{table_name}"
        if where_clause:
            query += f" WHERE {where_clause}"

        # Statistics
        total_rows = 0
        total_pages = 0
        total_bytes = 0

        # Progress callback
        def progress_callback(page_num: int, rows_in_page: int):
            nonlocal total_pages
            total_pages = page_num
            if page_num % 10 == 0:
                logger.info(
                    f"📦 Processing page {page_num} ({total_rows + rows_in_page:,} rows exported so far)"
                )

        # Configure streaming
        config = StreamConfig(
            fetch_size=fetch_size,
            page_callback=progress_callback,
        )

        schema = None
        writer = None
        batch_data: Dict[str, List[Any]] = {}

        try:
            # Stream data from Cassandra
            async with await session.execute_stream(query, stream_config=config) as result:
                # Process pages for memory efficiency
                async for page in result.pages():
                    if not page:
                        continue

                    # Infer schema from first page
                    if schema is None and page:
                        first_row = page[0]

                        # Get column names from first row
                        column_names = list(first_row._fields)

                        # Build PyArrow schema by inspecting actual values
                        fields = []
                        for name in column_names:
                            value = getattr(first_row, name)

                            # Infer type from actual value
                            if value is None:
                                # For None values, we'll need to look at other rows
                                # For now, default to string which can handle nulls
                                arrow_type = pa.string()
                            elif isinstance(value, bool):
                                arrow_type = pa.bool_()
                            elif isinstance(value, int):
                                arrow_type = pa.int64()
                            elif isinstance(value, float):
                                arrow_type = pa.float64()
                            elif isinstance(value, Decimal):
                                arrow_type = pa.float64()  # Convert Decimal to float64
                            elif isinstance(value, datetime):
                                arrow_type = pa.timestamp("ms")
                            elif isinstance(value, str):
                                arrow_type = pa.string()
                            elif isinstance(value, bytes):
                                arrow_type = pa.binary()
                            elif isinstance(value, (list, set, dict)):
                                arrow_type = pa.string()  # Convert collections to string
                            elif hasattr(value, "__class__") and value.__class__.__name__ in [
                                "OrderedMapSerializedKey",
                                "SortedSet",
                            ]:
                                arrow_type = pa.string()  # Cassandra special types
                            else:
                                arrow_type = pa.string()  # Default to string for unknown types

                            fields.append(pa.field(name, arrow_type))

                        schema = pa.schema(fields)

                        # Create Parquet writer
                        writer = pq.ParquetWriter(
                            temp_file,
                            schema,
                            compression=compression,
                            version="2.6",  # Latest format
                            use_dictionary=True,
                        )

                        # Initialize batch data structure
                        batch_data = {name: [] for name in column_names}

                    # Process rows in page
                    for row in page:
                        # Add row data to batch
                        for field in column_names:
                            value = getattr(row, field)

                            # Handle special types
                            if isinstance(value, datetime):
                                # Keep as datetime - PyArrow handles conversion
                                pass
                            elif isinstance(value, Decimal):
                                # Convert Decimal to float
                                value = float(value)
                            elif isinstance(value, (list, set, dict)):
                                # Convert collections to string
                                value = str(value)
                            elif value is not None and not isinstance(
                                value, (str, bytes, int, float, bool, datetime)
                            ):
                                # Convert other objects like UUID to string
                                value = str(value)

                            batch_data[field].append(value)

                        total_rows += 1

                        # Write batch when it reaches the desired size
                        if total_rows % row_group_size == 0:
                            batch = pa.record_batch(batch_data, schema=schema)
                            writer.write_batch(batch)

                            # Clear batch data
                            batch_data = {name: [] for name in column_names}

                            logger.info(
                                f"💾 Written {total_rows:,} rows to Parquet (row group {total_rows // row_group_size})"
                            )

                # Write final partial batch
                if any(batch_data.values()):
                    batch = pa.record_batch(batch_data, schema=schema)
                    writer.write_batch(batch)

        finally:
            if writer:
                writer.close()

                # Get file size
                total_bytes = temp_file.stat().st_size

                # Rename temp file to final name
                temp_file.rename(output_file)

        # Calculate statistics
        duration = (datetime.now() - start_time).total_seconds()
        rows_per_second = total_rows / duration if duration > 0 else 0
        mb_per_second = (total_bytes / (1024 * 1024)) / duration if duration > 0 else 0

        stats = {
            "table": f"{keyspace}.{table_name}",
            "output_file": str(output_file),
            "total_rows": total_rows,
            "total_pages": total_pages,
            "total_bytes": total_bytes,
            "total_mb": round(total_bytes / (1024 * 1024), 2),
            "duration_seconds": round(duration, 2),
            "rows_per_second": round(rows_per_second),
            "mb_per_second": round(mb_per_second, 2),
            "compression": compression,
            "row_group_size": row_group_size,
        }

        logger.info("\n" + "─" * 80)
        logger.info("✅ PARQUET EXPORT COMPLETED!")
        logger.info("─" * 80)
        logger.info("\n📊 Export Statistics:")
        logger.info(f"   • Table: {stats['table']}")
        logger.info(f"   • Rows: {stats['total_rows']:,}")
        logger.info(f"   • Pages: {stats['total_pages']}")
        logger.info(f"   • Size: {stats['total_mb']} MB")
        logger.info(f"   • Duration: {stats['duration_seconds']}s")
        logger.info(
            f"   • Speed: {stats['rows_per_second']:,} rows/sec ({stats['mb_per_second']} MB/s)"
        )
        logger.info(f"   • Compression: {stats['compression']}")
        logger.info(f"   • Row Group Size: {stats['row_group_size']:,}")

        return stats


async def setup_test_data(session):
    """Create test data for export demonstration."""
    logger.info("\n🛠️  Setting up test data for Parquet export demonstration...")

    # Create keyspace
    await session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS analytics
        WITH REPLICATION = {
            'class': 'SimpleStrategy',
            'replication_factor': 1
        }
    """
    )

    # Create a table with various data types
    await session.execute(
        """
        CREATE TABLE IF NOT EXISTS analytics.user_events (
            user_id UUID,
            event_time TIMESTAMP,
            event_type TEXT,
            device_type TEXT,
            country_code TEXT,
            city TEXT,
            revenue DECIMAL,
            duration_seconds INT,
            is_premium BOOLEAN,
            metadata MAP<TEXT, TEXT>,
            tags SET<TEXT>,
            PRIMARY KEY (user_id, event_time)
        ) WITH CLUSTERING ORDER BY (event_time DESC)
    """
    )

    # Insert test data
    insert_stmt = await session.prepare(
        """
        INSERT INTO analytics.user_events (
            user_id, event_time, event_type, device_type,
            country_code, city, revenue, duration_seconds,
            is_premium, metadata, tags
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    )

    # Generate substantial test data
    logger.info("📝 Inserting test data with complex types (maps, sets, decimals)...")

    import random
    import uuid
    from decimal import Decimal

    event_types = ["view", "click", "purchase", "signup", "logout"]
    device_types = ["mobile", "desktop", "tablet", "tv"]
    countries = ["US", "UK", "DE", "FR", "JP", "BR", "IN", "AU"]
    cities = ["New York", "London", "Berlin", "Paris", "Tokyo", "São Paulo", "Mumbai", "Sydney"]

    base_time = datetime.now() - timedelta(days=30)
    tasks = []
    total_inserted = 0

    # Insert data for 100 users over 30 days
    for user_num in range(100):
        user_id = uuid.uuid4()
        is_premium = random.random() > 0.7

        # Each user has 100-500 events
        num_events = random.randint(100, 500)

        for event_num in range(num_events):
            event_time = base_time + timedelta(
                days=random.randint(0, 29),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59),
            )

            event_type = random.choice(event_types)
            revenue = (
                Decimal(str(round(random.uniform(0, 100), 2)))
                if event_type == "purchase"
                else Decimal("0")
            )

            metadata = {
                "session_id": str(uuid.uuid4()),
                "version": f"{random.randint(1, 5)}.{random.randint(0, 9)}.{random.randint(0, 9)}",
                "platform": random.choice(["iOS", "Android", "Web"]),
            }

            tags = set(
                random.sample(
                    ["mobile", "desktop", "premium", "trial", "organic", "paid", "social"],
                    k=random.randint(1, 4),
                )
            )

            tasks.append(
                session.execute(
                    insert_stmt,
                    [
                        user_id,
                        event_time,
                        event_type,
                        random.choice(device_types),
                        random.choice(countries),
                        random.choice(cities),
                        revenue,
                        random.randint(10, 3600),
                        is_premium,
                        metadata,
                        tags,
                    ],
                )
            )

            # Execute in batches
            if len(tasks) >= 100:
                await asyncio.gather(*tasks)
                tasks = []
                total_inserted += 100

                if total_inserted % 5000 == 0:
                    logger.info(f"   📊 Progress: {total_inserted:,} events inserted...")

    # Execute remaining tasks
    if tasks:
        await asyncio.gather(*tasks)
        total_inserted += len(tasks)

    logger.info(
        f"✅ Test data setup complete: {total_inserted:,} events inserted into analytics.user_events"
    )


async def demonstrate_exports(session):
    """Demonstrate various export scenarios."""
    output_dir = os.environ.get("EXAMPLE_OUTPUT_DIR", "examples/exampleoutput")
    logger.info(f"\n📁 Output directory: {output_dir}")

    # Example 1: Export entire table
    logger.info("\n" + "=" * 80)
    logger.info("EXAMPLE 1: Export Entire Table with Snappy Compression")
    logger.info("=" * 80)
    exporter1 = ParquetExporter(str(Path(output_dir) / "example1"))
    stats1 = await exporter1.export_table(
        session,
        table_name="user_events",
        keyspace="analytics",
        fetch_size=5000,
        row_group_size=25000,
    )

    # Example 2: Export with filtering
    logger.info("\n" + "=" * 80)
    logger.info("EXAMPLE 2: Export Filtered Data (Purchase Events Only)")
    logger.info("=" * 80)
    exporter2 = ParquetExporter(str(Path(output_dir) / "example2"))
    stats2 = await exporter2.export_table(
        session,
        table_name="user_events",
        keyspace="analytics",
        fetch_size=5000,
        row_group_size=25000,
        where_clause="event_type = 'purchase' ALLOW FILTERING",
        compression="gzip",
    )

    # Example 3: Export with different compression
    logger.info("\n" + "=" * 80)
    logger.info("EXAMPLE 3: Export with LZ4 Compression")
    logger.info("=" * 80)
    exporter3 = ParquetExporter(str(Path(output_dir) / "example3"))
    stats3 = await exporter3.export_table(
        session,
        table_name="user_events",
        keyspace="analytics",
        fetch_size=10000,
        row_group_size=50000,
        compression="lz4",
    )

    return [stats1, stats2, stats3]


async def verify_parquet_files():
    """Verify the exported Parquet files."""
    logger.info("\n" + "=" * 80)
    logger.info("🔍 VERIFYING EXPORTED PARQUET FILES")
    logger.info("=" * 80)

    export_dir = Path(os.environ.get("EXAMPLE_OUTPUT_DIR", "examples/exampleoutput"))

    # Look for Parquet files in subdirectories too
    for parquet_file in export_dir.rglob("*.parquet"):
        logger.info(f"\n📄 Verifying: {parquet_file.name}")
        logger.info("─" * 60)

        # Read Parquet file metadata
        parquet_file_obj = pq.ParquetFile(parquet_file)

        # Display metadata
        logger.info(f"   📋 Schema columns: {len(parquet_file_obj.schema)}")
        logger.info(f"   📊 Row groups: {parquet_file_obj.num_row_groups}")
        logger.info(f"   📈 Total rows: {parquet_file_obj.metadata.num_rows:,}")
        logger.info(
            f"   🗜️  Compression: {parquet_file_obj.metadata.row_group(0).column(0).compression}"
        )

        # Read first few rows
        table = pq.read_table(parquet_file, columns=None)
        df = table.to_pandas()

        logger.info(f"   📐 Dimensions: {df.shape[0]:,} rows × {df.shape[1]} columns")
        logger.info(f"   💾 Memory usage: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
        logger.info(
            f"   🏷️  Columns: {', '.join(list(df.columns)[:5])}{' ...' if len(df.columns) > 5 else ''}"
        )

        # Show data types
        logger.info("\n   📊 Sample data (first 3 rows):")
        for idx, row in df.head(3).iterrows():
            logger.info(
                f"      Row {idx}: event_type='{row['event_type']}', revenue={row['revenue']}, city='{row['city']}'"
            )


async def main():
    """Run the Parquet export examples."""
    # Get contact points from environment or use localhost
    contact_points = os.environ.get("CASSANDRA_CONTACT_POINTS", "localhost").split(",")
    port = int(os.environ.get("CASSANDRA_PORT", "9042"))

    logger.info(f"Connecting to Cassandra at {contact_points}:{port}")

    # Connect to Cassandra using context manager
    async with AsyncCluster(contact_points, port=port) as cluster:
        async with await cluster.connect() as session:
            # Setup test data
            await setup_test_data(session)

            # Run export demonstrations
            export_stats = await demonstrate_exports(session)

            # Verify exported files
            await verify_parquet_files()

            # Summary
            logger.info("\n" + "=" * 80)
            logger.info("📊 EXPORT SUMMARY")
            logger.info("=" * 80)
            logger.info("\n🎯 Three exports completed:")
            for i, stats in enumerate(export_stats, 1):
                logger.info(
                    f"\n   {i}. {stats['compression'].upper()} compression:"
                    f"\n      • {stats['total_rows']:,} rows exported"
                    f"\n      • {stats['total_mb']} MB file size"
                    f"\n      • {stats['duration_seconds']}s duration"
                    f"\n      • {stats['rows_per_second']:,} rows/sec throughput"
                )

            # Cleanup
            logger.info("\n🧹 Cleaning up...")
            await session.execute("DROP KEYSPACE analytics")
            logger.info("✅ Keyspace dropped")


if __name__ == "__main__":
    asyncio.run(main())
