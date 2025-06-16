#!/usr/bin/env python3
"""
Basic example of streaming large result sets with async-cassandra.

This example demonstrates:
- Basic streaming with execute_stream()
- Configuring fetch size
- Processing rows one at a time
- Handling empty results
"""

import asyncio
import logging
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

    await session.set_keyspace("streaming_example")

    # Create table
    await session.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
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
        INSERT INTO events (partition_id, event_id, event_time, event_type, data)
        VALUES (?, ?, ?, ?, ?)
    """
    )

    # Insert 10,000 events across 10 partitions
    batch_size = 100
    total_events = 10000

    for i in range(0, total_events, batch_size):
        tasks = []
        for j in range(batch_size):
            if i + j >= total_events:
                break

            event_id = i + j
            partition_id = event_id % 10  # 10 partitions
            tasks.append(
                session.execute(
                    insert_stmt,
                    [
                        partition_id,
                        event_id,
                        datetime.now(),
                        f"type_{event_id % 5}",  # 5 event types
                        f"Event data for event {event_id}",
                    ],
                )
            )

        await asyncio.gather(*tasks)

        if (i + batch_size) % 1000 == 0:
            logger.info(f"Inserted {i + batch_size} events...")

    logger.info(f"Inserted {total_events} test events")


async def basic_streaming_example(session):
    """Demonstrate basic streaming."""
    logger.info("\n=== Basic Streaming Example ===")

    # Configure streaming
    config = StreamConfig(
        fetch_size=1000,  # Fetch 1000 rows per page
        page_callback=lambda page, total: logger.info(
            f"Fetched page {page} - Total rows so far: {total}"
        ),
    )

    # Stream all events
    logger.info("Starting to stream all events...")
    start_time = datetime.now()

    # CRITICAL: Always use context manager to prevent memory leaks
    async with await session.execute_stream("SELECT * FROM events", stream_config=config) as result:
        # Process rows one at a time
        event_count = 0
        event_types = {}

        async for row in result:
            event_count += 1

            # Track event types
            event_type = row.event_type
            event_types[event_type] = event_types.get(event_type, 0) + 1

            # Log progress every 1000 events
            if event_count % 1000 == 0:
                elapsed = (datetime.now() - start_time).total_seconds()
                rate = event_count / elapsed
                logger.info(f"Processed {event_count} events ({rate:.0f} events/sec)")

    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info("\nStreaming completed:")
    logger.info(f"- Total events: {event_count}")
    logger.info(f"- Time elapsed: {elapsed:.2f} seconds")
    logger.info(f"- Rate: {event_count/elapsed:.0f} events/sec")
    logger.info(f"- Event types: {event_types}")


async def filtered_streaming_example(session):
    """Demonstrate streaming with WHERE clause."""
    logger.info("\n=== Filtered Streaming Example ===")

    # Prepare a filtered query
    stmt = await session.prepare(
        """
        SELECT * FROM events
        WHERE partition_id = ?
        AND event_type = ?
    """
    )

    # Stream events for specific partition and type
    partition_id = 5
    event_type = "type_2"

    config = StreamConfig(fetch_size=500)

    # Use context manager for proper cleanup
    async with await session.execute_stream(
        stmt, parameters=[partition_id, event_type], stream_config=config
    ) as result:
        count = 0
        async for row in result:
            count += 1

    logger.info(f"Found {count} events in partition {partition_id} of type '{event_type}'")


async def page_based_streaming_example(session):
    """Demonstrate page-based processing."""
    logger.info("\n=== Page-Based Streaming Example ===")

    config = StreamConfig(fetch_size=2000, max_pages=5)  # Limit to 5 pages for demo

    # Use context manager for automatic resource cleanup
    async with await session.execute_stream("SELECT * FROM events", stream_config=config) as result:
        # Process data page by page
        page_count = 0
        total_events = 0

        async for page in result.pages():
            page_count += 1
            events_in_page = len(page)
            total_events += events_in_page

            logger.info(f"Processing page {page_count} with {events_in_page} events")

            # Simulate batch processing
            await asyncio.sleep(0.1)  # Simulate processing time

    logger.info(f"Processed {page_count} pages with {total_events} total events")


async def main():
    """Run all streaming examples."""
    # Connect to Cassandra
    cluster = AsyncCluster(["localhost"])

    try:
        session = await cluster.connect()

        # Setup test data
        await setup_test_data(session)

        # Run examples
        await basic_streaming_example(session)
        await filtered_streaming_example(session)
        await page_based_streaming_example(session)

        # Cleanup
        await session.execute("DROP KEYSPACE streaming_example")

    finally:
        await cluster.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
