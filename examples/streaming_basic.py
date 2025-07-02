#!/usr/bin/env python3
"""
Basic example of streaming large result sets with async-cassandra.

This example demonstrates:
- Basic streaming with execute_stream()
- Configuring fetch size
- Processing rows one at a time
- Handling empty results

How to run:
-----------
1. Using Make (automatically starts Cassandra if needed):
   make example-streaming

2. With external Cassandra cluster:
   CASSANDRA_CONTACT_POINTS=10.0.0.1,10.0.0.2 make example-streaming

3. Direct Python execution:
   python examples/streaming_basic.py

4. With custom contact points:
   CASSANDRA_CONTACT_POINTS=cassandra.example.com python examples/streaming_basic.py

Environment variables:
- CASSANDRA_CONTACT_POINTS: Comma-separated list of contact points (default: localhost)
- CASSANDRA_PORT: Port number (default: 9042)
"""

import asyncio
import logging
import os
from datetime import datetime

from async_cassandra import AsyncCluster, StreamConfig

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def setup_test_data(session):
    """Create test keyspace and table with sample data."""
    # Create keyspace
    await session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS streaming_example
        WITH REPLICATION = {
            'class': 'SimpleStrategy',
            'replication_factor': 1
        }
    """
    )

    # Create table
    await session.execute(
        """
        CREATE TABLE IF NOT EXISTS streaming_example.events (
            partition_id int,
            event_id int,
            event_time timestamp,
            event_type text,
            data text,
            PRIMARY KEY (partition_id, event_id)
        )
    """
    )

    # Insert test data
    logger.info("Inserting test data...")
    insert_stmt = await session.prepare(
        """
        INSERT INTO streaming_example.events (partition_id, event_id, event_time, event_type, data)
        VALUES (?, ?, ?, ?, ?)
    """
    )

    # Insert 100,000 events across 100 partitions for realistic paging demonstration
    batch_size = 500  # Larger batches for faster insertion
    total_events = 100000  # 100k events to demonstrate multiple pages

    for i in range(0, total_events, batch_size):
        tasks = []
        for j in range(batch_size):
            if i + j >= total_events:
                break

            event_id = i + j
            partition_id = event_id % 100  # 100 partitions for better distribution
            tasks.append(
                session.execute(
                    insert_stmt,
                    [
                        partition_id,
                        event_id,
                        datetime.now(),
                        f"type_{event_id % 5}",  # 5 event types
                        f"Event data for event {event_id} with some additional content to make rows larger",
                    ],
                )
            )

        await asyncio.gather(*tasks)

        if (i + batch_size) % 10000 == 0:
            logger.info(f"Inserted {i + batch_size} events...")

    logger.info(f"Inserted {total_events} test events")


async def basic_streaming_example(session):
    """Demonstrate basic streaming."""
    logger.info("\n" + "=" * 80)
    logger.info("BASIC STREAMING EXAMPLE")
    logger.info("=" * 80)

    # Configure streaming with smaller page size to demonstrate True Async Paging
    # IMPORTANT: The driver fetches pages on-demand, not all at once
    # - fetch_size controls rows per page (smaller = less memory, more round trips)
    # - Pages are fetched asynchronously as you consume data
    # - No pre-fetching of all pages - truly streaming!
    config = StreamConfig(
        fetch_size=5000,  # Fetch 5000 rows per page - will result in ~20 pages
        page_callback=lambda page, total: logger.info(
            f"Fetched page {page} - Total rows so far: {total:,}"
        ),
    )

    # Stream all events
    logger.info("Starting to stream all events...")
    start_time = datetime.now()

    # CRITICAL: Always use context manager to prevent memory leaks
    async with await session.execute_stream(
        "SELECT * FROM streaming_example.events", stream_config=config
    ) as result:
        # Process rows one at a time
        event_count = 0
        event_types = {}

        async for row in result:
            event_count += 1

            # Track event types
            event_type = row.event_type
            event_types[event_type] = event_types.get(event_type, 0) + 1

            # Log progress every 10000 events
            if event_count % 10000 == 0:
                elapsed = (datetime.now() - start_time).total_seconds()
                rate = event_count / elapsed
                logger.info(f"Processed {event_count:,} events ({rate:,.0f} events/sec)")

    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info("\n✅ Streaming completed!")
    logger.info("📊 Statistics:")
    logger.info(f"   • Total events: {event_count:,}")
    logger.info(f"   • Time elapsed: {elapsed:.2f} seconds")
    logger.info(f"   • Processing rate: {event_count/elapsed:,.0f} events/sec")
    logger.info(f"   • Event types distribution: {event_types}")


async def filtered_streaming_example(session):
    """Demonstrate streaming with WHERE clause."""
    logger.info("\n" + "=" * 80)
    logger.info("FILTERED STREAMING EXAMPLE")
    logger.info("=" * 80)

    # Prepare a filtered query
    # Note: event_type is not part of primary key, so we need ALLOW FILTERING
    stmt = await session.prepare(
        """
        SELECT * FROM streaming_example.events
        WHERE partition_id = ?
        ALLOW FILTERING
    """
    )

    # Stream events for specific partition
    partition_id = 5

    config = StreamConfig(fetch_size=500)

    # Use context manager for proper cleanup
    async with await session.execute_stream(
        stmt, parameters=[partition_id], stream_config=config
    ) as result:
        count = 0
        type_counts = {}
        async for row in result:
            count += 1
            event_type = row.event_type
            type_counts[event_type] = type_counts.get(event_type, 0) + 1

    logger.info("\n✅ Filtered streaming completed!")
    logger.info(f"📊 Results for partition {partition_id}:")
    logger.info(f"   • Total events: {count}")
    logger.info(f"   • Event type breakdown: {type_counts}")


async def page_based_streaming_example(session):
    """Demonstrate True Async Paging with page-by-page processing."""
    logger.info("\n" + "=" * 80)
    logger.info("PAGE-BASED STREAMING EXAMPLE (True Async Paging)")
    logger.info("=" * 80)
    logger.info("\n💡 Key Insight: Pages are fetched ON-DEMAND as you process them!")
    logger.info("   The driver fetches the next page WHILE you process the current one.\n")

    # Page Size Recommendations:
    # - Smaller pages (1000-5000): Better for memory, responsiveness, real-time processing
    # - Medium pages (5000-10000): Good balance for most use cases
    # - Larger pages (10000+): Better throughput for bulk operations, fewer round trips
    #
    # The driver fetches the next page WHILE you're processing the current one!
    config = StreamConfig(fetch_size=7500)  # Will result in ~13-14 pages

    # Use context manager for automatic resource cleanup
    async with await session.execute_stream(
        "SELECT * FROM streaming_example.events", stream_config=config
    ) as result:
        # Process data page by page using True Async Paging
        page_count = 0
        total_events = 0
        processing_times = []

        logger.info("Processing pages asynchronously...")
        async for page in result.pages():
            page_start = datetime.now()
            page_count += 1
            events_in_page = len(page)
            total_events += events_in_page

            logger.info(f"📄 Processing page {page_count} ({events_in_page:,} events)...")

            # Simulate batch processing (e.g., writing to another system)
            # In real scenarios, this could be bulk writes to S3, another DB, etc.
            await asyncio.sleep(0.05)  # Simulate processing time

            # Calculate some statistics on the page
            event_types = {}
            for row in page:
                event_type = row.event_type
                event_types[event_type] = event_types.get(event_type, 0) + 1

            page_time = (datetime.now() - page_start).total_seconds()
            processing_times.append(page_time)
            logger.info(f"   ✓ Page {page_count} done in {page_time:.3f}s | Types: {event_types}")

    avg_page_time = sum(processing_times) / len(processing_times) if processing_times else 0
    logger.info("\n✅ Page-based streaming completed!")
    logger.info("📊 Statistics:")
    logger.info(f"   • Total pages processed: {page_count}")
    logger.info(f"   • Total events: {total_events:,}")
    logger.info(f"   • Average page processing time: {avg_page_time:.3f}s")
    logger.info("\n🚀 Performance Note: Pages were fetched asynchronously!")
    logger.info("   While you processed each page, the driver was already fetching the next one.")


async def main():
    """Run all streaming examples."""
    # Get contact points from environment or use localhost
    contact_points = os.environ.get("CASSANDRA_CONTACT_POINTS", "localhost").split(",")
    port = int(os.environ.get("CASSANDRA_PORT", "9042"))

    logger.info(f"Connecting to Cassandra at {contact_points}:{port}")

    # Connect to Cassandra using context manager
    async with AsyncCluster(contact_points, port=port) as cluster:
        async with await cluster.connect() as session:
            # Setup test data
            await setup_test_data(session)

            # Run examples
            await basic_streaming_example(session)
            await filtered_streaming_example(session)
            await page_based_streaming_example(session)

            # Cleanup
            await session.execute("DROP KEYSPACE streaming_example")


if __name__ == "__main__":
    asyncio.run(main())
