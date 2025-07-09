#!/usr/bin/env python3
"""
Visual demonstration that streaming doesn't block the event loop.

This example shows that while pages are being fetched from Cassandra,
the event loop remains free to handle other tasks like updating a
progress bar, handling other requests, or maintaining heartbeats.

How to run:
-----------
1. Using Make (automatically starts Cassandra if needed):
   make example-streaming-demo

2. With external Cassandra cluster:
   CASSANDRA_CONTACT_POINTS=10.0.0.1,10.0.0.2 make example-streaming-demo

3. Direct Python execution:
   python examples/streaming_non_blocking_demo.py

4. With custom contact points:
   CASSANDRA_CONTACT_POINTS=cassandra.example.com python examples/streaming_non_blocking_demo.py

Environment variables:
- CASSANDRA_CONTACT_POINTS: Comma-separated list of contact points (default: localhost)
- CASSANDRA_PORT: Port number (default: 9042)
"""

import asyncio
import os
import time
from datetime import datetime

from async_cassandra import AsyncCluster, StreamConfig


class EventLoopMonitor:
    """Monitor event loop responsiveness during streaming."""

    def __init__(self):
        self.heartbeats = []
        self.page_fetch_times = []
        self.monitoring = True

    async def heartbeat(self):
        """Send a heartbeat every 50ms to detect blocking."""
        print("💓 Starting heartbeat monitor...")
        while self.monitoring:
            self.heartbeats.append(time.perf_counter())
            await asyncio.sleep(0.05)  # 50ms

    def record_page_fetch(self, page_num: int):
        """Record when a page is fetched."""
        self.page_fetch_times.append((page_num, time.perf_counter()))

    def analyze(self):
        """Analyze heartbeat gaps to detect blocking."""
        if len(self.heartbeats) < 2:
            return

        gaps = []
        for i in range(1, len(self.heartbeats)):
            gap = self.heartbeats[i] - self.heartbeats[i - 1]
            gaps.append(gap * 1000)  # Convert to ms

        avg_gap = sum(gaps) / len(gaps)
        max_gap = max(gaps)
        blocked_count = sum(1 for gap in gaps if gap > 100)  # >100ms gaps

        print("\n📊 Event Loop Analysis:")
        print(f"  Average heartbeat gap: {avg_gap:.1f}ms (target: 50ms)")
        print(f"  Maximum gap: {max_gap:.1f}ms")
        print(f"  Gaps > 100ms: {blocked_count}")

        if max_gap < 100:
            print("  ✅ Event loop remained responsive!")
        elif max_gap < 200:
            print("  ⚠️  Minor blocking detected")
        else:
            print("  ❌ Significant blocking detected")


async def setup_demo_data(session):
    """Create demo data with enough rows for multiple pages."""
    print("🔧 Setting up demo data...")

    # Create keyspace and table
    await session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS streaming_demo
        WITH REPLICATION = {
            'class': 'SimpleStrategy',
            'replication_factor': 1
        }
    """
    )

    await session.execute(
        """
        CREATE TABLE IF NOT EXISTS streaming_demo.sensor_data (
            sensor_id TEXT,
            reading_time TIMESTAMP,
            temperature DOUBLE,
            humidity DOUBLE,
            location TEXT,
            PRIMARY KEY (sensor_id, reading_time)
        ) WITH CLUSTERING ORDER BY (reading_time DESC)
    """
    )

    # Insert data - enough for multiple pages
    insert_stmt = await session.prepare(
        """
        INSERT INTO streaming_demo.sensor_data (sensor_id, reading_time, temperature, humidity, location)
        VALUES (?, ?, ?, ?, ?)
    """
    )

    sensors = [f"sensor_{i:03d}" for i in range(20)]
    locations = ["Building A", "Building B", "Building C", "Outdoor"]

    tasks = []
    total_rows = 0
    base_time = datetime.now()

    for sensor_id in sensors:
        for hour in range(24):
            for minute in range(0, 60, 5):  # Every 5 minutes
                reading_time = base_time.replace(hour=hour, minute=minute, second=0)
                temperature = 20.0 + (hash(f"{sensor_id}{hour}{minute}") % 100) / 10.0
                humidity = 40.0 + (hash(f"{sensor_id}{hour}{minute}h") % 400) / 10.0
                location = locations[hash(sensor_id) % len(locations)]

                tasks.append(
                    session.execute(
                        insert_stmt, [sensor_id, reading_time, temperature, humidity, location]
                    )
                )
                total_rows += 1

                # Execute in batches
                if len(tasks) >= 100:
                    await asyncio.gather(*tasks)
                    tasks = []

    # Execute remaining
    if tasks:
        await asyncio.gather(*tasks)

    print(f"✅ Created {total_rows:,} sensor readings")


async def demonstrate_non_blocking_streaming(session):
    """Show that streaming doesn't block the event loop."""
    monitor = EventLoopMonitor()

    print("\n🚀 Starting non-blocking streaming demonstration...")
    print("Watch how the heartbeat continues while pages are fetched!\n")

    # Start heartbeat monitor
    heartbeat_task = asyncio.create_task(monitor.heartbeat())

    # Configure streaming with page callback
    def page_callback(page_num: int, rows_in_page: int):
        monitor.record_page_fetch(page_num)
        print(f"📄 Page {page_num} fetched ({rows_in_page} rows) - " f"Heartbeat still running! 💓")

    config = StreamConfig(
        fetch_size=500, page_callback=page_callback  # Small pages to see multiple fetches
    )

    # Stream data
    start_time = time.perf_counter()
    rows_processed = 0

    print("🔄 Starting to stream sensor data...\n")

    async with await session.execute_stream(
        "SELECT * FROM streaming_demo.sensor_data", stream_config=config
    ) as result:
        async for row in result:
            rows_processed += 1

            # Show progress periodically
            if rows_processed % 1000 == 0:
                elapsed = time.perf_counter() - start_time
                rate = rows_processed / elapsed
                print(f"⚡ Processed {rows_processed:,} rows " f"({rate:.0f} rows/sec)")

            # Simulate some async processing
            if rows_processed % 100 == 0:
                await asyncio.sleep(0.001)  # 1ms

    # Stop monitoring
    monitor.monitoring = False
    await heartbeat_task

    # Show results
    duration = time.perf_counter() - start_time
    print("\n✅ Streaming complete!")
    print(f"  Total rows: {rows_processed:,}")
    print(f"  Duration: {duration:.2f}s")
    print(f"  Rate: {rows_processed/duration:.0f} rows/sec")
    print(f"  Pages fetched: {len(monitor.page_fetch_times)}")

    # Analyze event loop blocking
    monitor.analyze()


async def demonstrate_concurrent_operations(session):
    """Show that other operations can run during streaming."""
    print("\n\n🎯 Demonstrating concurrent operations during streaming...")

    # Prepare queries
    count_stmt = await session.prepare(
        "SELECT COUNT(*) FROM streaming_demo.sensor_data WHERE sensor_id = ?"
    )

    concurrent_results = []

    async def run_concurrent_queries():
        """Run other queries while streaming is happening."""
        for i in range(10):
            await asyncio.sleep(0.5)  # Every 500ms

            start = time.perf_counter()
            result = await session.execute(count_stmt, [f"sensor_{i:03d}"])
            duration = time.perf_counter() - start
            count = result.one()[0]

            concurrent_results.append((i, duration, count))
            print(f"  🔍 Query {i+1} completed in {duration*1000:.1f}ms " f"(count: {count})")

    async def stream_data():
        """Stream data concurrently."""
        config = StreamConfig(fetch_size=1000)
        rows = 0

        async with await session.execute_stream(
            "SELECT * FROM streaming_demo.sensor_data", stream_config=config
        ) as result:
            async for row in result:
                rows += 1
                if rows % 2000 == 0:
                    print(f"  📊 Streaming progress: {rows:,} rows")

        return rows

    # Run both concurrently
    print("  Running streaming and queries concurrently...\n")

    streaming_task = asyncio.create_task(stream_data())
    queries_task = asyncio.create_task(run_concurrent_queries())

    rows_streamed, _ = await asyncio.gather(streaming_task, queries_task)

    # Analyze concurrent query performance
    if concurrent_results:
        avg_duration = sum(d for _, d, _ in concurrent_results) / len(concurrent_results)
        max_duration = max(d for _, d, _ in concurrent_results)

        print("\n  ✅ Concurrent operations summary:")
        print(f"     Rows streamed: {rows_streamed:,}")
        print(f"     Concurrent queries: {len(concurrent_results)}")
        print(f"     Average query time: {avg_duration*1000:.1f}ms")
        print(f"     Max query time: {max_duration*1000:.1f}ms")

        if max_duration < 0.1:  # 100ms
            print("     🎉 All queries remained fast during streaming!")


async def main():
    """Run the non-blocking streaming demonstration."""
    # Get contact points from environment or use localhost
    contact_points = os.environ.get("CASSANDRA_CONTACT_POINTS", "localhost").split(",")
    port = int(os.environ.get("CASSANDRA_PORT", "9042"))

    print(f"Connecting to Cassandra at {contact_points}:{port}\n")

    # Connect to Cassandra
    async with AsyncCluster(contact_points, port=port) as cluster:
        async with await cluster.connect() as session:
            # Setup demo data
            await setup_demo_data(session)

            # Run demonstrations
            await demonstrate_non_blocking_streaming(session)
            await demonstrate_concurrent_operations(session)

            # Cleanup
            print("\n🧹 Cleaning up...")
            await session.execute("DROP KEYSPACE streaming_demo")

    print("\n✨ Demonstration complete!")
    print("\nKey takeaways:")
    print("1. The event loop stays responsive during page fetches")
    print("2. Other queries can execute concurrently with streaming")
    print("3. async-cassandra bridges the driver's threads without blocking")


if __name__ == "__main__":
    print("🎪 async-cassandra Non-Blocking Streaming Demo")
    print("=" * 50)
    asyncio.run(main())
