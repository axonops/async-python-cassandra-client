"""
Integration tests demonstrating that streaming doesn't block the event loop.

This test proves that while the driver fetches pages in its thread pool,
the asyncio event loop remains free to handle other tasks.
"""

import asyncio
import time
from typing import List

import pytest

from async_cassandra import AsyncCluster, StreamConfig


class TestStreamingNonBlocking:
    """Test that streaming operations don't block the event loop."""

    @pytest.fixture(autouse=True)
    async def setup_test_data(self, cassandra_cluster):
        """Create test data for streaming tests."""
        async with AsyncCluster(["localhost"]) as cluster:
            async with await cluster.connect() as session:
                # Create keyspace and table
                await session.execute(
                    """
                    CREATE KEYSPACE IF NOT EXISTS test_streaming
                    WITH REPLICATION = {
                        'class': 'SimpleStrategy',
                        'replication_factor': 1
                    }
                """
                )
                await session.set_keyspace("test_streaming")

                await session.execute(
                    """
                    CREATE TABLE IF NOT EXISTS large_table (
                        partition_key INT,
                        clustering_key INT,
                        data TEXT,
                        PRIMARY KEY (partition_key, clustering_key)
                    )
                """
                )

                # Insert enough data to ensure multiple pages
                # With fetch_size=1000 and 10k rows, we'll have 10 pages
                insert_stmt = await session.prepare(
                    "INSERT INTO large_table (partition_key, clustering_key, data) VALUES (?, ?, ?)"
                )

                tasks = []
                for partition in range(10):
                    for cluster in range(1000):
                        # Create some data that takes time to process
                        data = f"Data for partition {partition}, cluster {cluster}" * 10
                        tasks.append(session.execute(insert_stmt, [partition, cluster, data]))

                        # Execute in batches
                        if len(tasks) >= 100:
                            await asyncio.gather(*tasks)
                            tasks = []

                # Execute remaining
                if tasks:
                    await asyncio.gather(*tasks)

                yield

                # Cleanup
                await session.execute("DROP KEYSPACE test_streaming")

    async def test_event_loop_not_blocked_during_paging(self, cassandra_cluster):
        """
        Test that the event loop remains responsive while pages are being fetched.

        This test runs a streaming query that fetches multiple pages while
        simultaneously running a "heartbeat" task that increments a counter
        every 10ms. If the event loop was blocked during page fetches,
        we would see gaps in the heartbeat counter.
        """
        heartbeat_count = 0
        heartbeat_times: List[float] = []
        streaming_events: List[tuple[float, str]] = []
        stop_heartbeat = False

        async def heartbeat_task():
            """Increment counter every 10ms to detect event loop blocking."""
            nonlocal heartbeat_count
            start_time = time.perf_counter()

            while not stop_heartbeat:
                heartbeat_count += 1
                current_time = time.perf_counter()
                heartbeat_times.append(current_time - start_time)
                await asyncio.sleep(0.01)  # 10ms

        async def streaming_task():
            """Stream data and record when pages are fetched."""
            nonlocal streaming_events

            async with AsyncCluster(["localhost"]) as cluster:
                async with await cluster.connect() as session:
                    await session.set_keyspace("test_streaming")

                    rows_seen = 0
                    pages_fetched = 0

                    def page_callback(page_num: int, rows_in_page: int):
                        nonlocal pages_fetched
                        pages_fetched = page_num
                        current_time = time.perf_counter() - start_time
                        streaming_events.append((current_time, f"Page {page_num} fetched"))

                    # Use small fetch_size to ensure multiple pages
                    config = StreamConfig(fetch_size=1000, page_callback=page_callback)

                    start_time = time.perf_counter()

                    async with await session.execute_stream(
                        "SELECT * FROM large_table", stream_config=config
                    ) as result:
                        async for row in result:
                            rows_seen += 1

                            # Simulate some processing time
                            await asyncio.sleep(0.001)  # 1ms per row

                            # Record progress at key points
                            if rows_seen % 1000 == 0:
                                current_time = time.perf_counter() - start_time
                                streaming_events.append(
                                    (current_time, f"Processed {rows_seen} rows")
                                )

                    return rows_seen, pages_fetched

        # Run both tasks concurrently
        heartbeat = asyncio.create_task(heartbeat_task())

        # Run streaming and measure time
        stream_start = time.perf_counter()
        rows_processed, pages = await streaming_task()
        stream_duration = time.perf_counter() - stream_start

        # Stop heartbeat
        stop_heartbeat = True
        await heartbeat

        # Analyze results
        print("\n=== Event Loop Blocking Test Results ===")
        print(f"Total rows processed: {rows_processed:,}")
        print(f"Total pages fetched: {pages}")
        print(f"Streaming duration: {stream_duration:.2f}s")
        print(f"Heartbeat count: {heartbeat_count}")
        print(f"Expected heartbeats: ~{int(stream_duration / 0.01)}")

        # Check heartbeat consistency
        if len(heartbeat_times) > 1:
            # Calculate gaps between heartbeats
            heartbeat_gaps = []
            for i in range(1, len(heartbeat_times)):
                gap = heartbeat_times[i] - heartbeat_times[i - 1]
                heartbeat_gaps.append(gap)

            avg_gap = sum(heartbeat_gaps) / len(heartbeat_gaps)
            max_gap = max(heartbeat_gaps)
            gaps_over_50ms = sum(1 for gap in heartbeat_gaps if gap > 0.05)

            print("\nHeartbeat Analysis:")
            print(f"Average gap: {avg_gap*1000:.1f}ms (target: 10ms)")
            print(f"Max gap: {max_gap*1000:.1f}ms")
            print(f"Gaps > 50ms: {gaps_over_50ms}")

            # Print streaming events timeline
            print("\nStreaming Events Timeline:")
            for event_time, event in streaming_events:
                print(f"  {event_time:.3f}s: {event}")

            # Assertions
            assert heartbeat_count > 0, "Heartbeat task didn't run"

            # The average gap should be close to 10ms
            # Allow some tolerance for scheduling
            assert avg_gap < 0.02, f"Average heartbeat gap too large: {avg_gap*1000:.1f}ms"

            # Max gap shows worst-case blocking
            # Even with page fetches, should not block for long
            assert max_gap < 0.1, f"Max heartbeat gap too large: {max_gap*1000:.1f}ms"

            # Should have very few large gaps
            assert gaps_over_50ms < 5, f"Too many large gaps: {gaps_over_50ms}"

        # Verify streaming completed successfully
        assert rows_processed == 10000, f"Expected 10000 rows, got {rows_processed}"
        assert pages >= 10, f"Expected at least 10 pages, got {pages}"

    async def test_concurrent_queries_during_streaming(self, cassandra_cluster):
        """
        Test that other queries can execute while streaming is in progress.

        This proves that the thread pool isn't completely blocked by streaming.
        """
        async with AsyncCluster(["localhost"]) as cluster:
            async with await cluster.connect() as session:
                await session.set_keyspace("test_streaming")

                # Prepare a simple query
                count_stmt = await session.prepare(
                    "SELECT COUNT(*) FROM large_table WHERE partition_key = ?"
                )

                query_times: List[float] = []
                queries_completed = 0

                async def run_concurrent_queries():
                    """Run queries every 100ms during streaming."""
                    nonlocal queries_completed

                    for i in range(20):  # 20 queries over 2 seconds
                        start = time.perf_counter()
                        await session.execute(count_stmt, [i % 10])
                        duration = time.perf_counter() - start
                        query_times.append(duration)
                        queries_completed += 1

                        # Log slow queries
                        if duration > 0.1:
                            print(f"Slow query {i}: {duration:.3f}s")

                        await asyncio.sleep(0.1)  # 100ms between queries

                async def stream_large_dataset():
                    """Stream the entire table."""
                    config = StreamConfig(fetch_size=1000)
                    rows = 0

                    async with await session.execute_stream(
                        "SELECT * FROM large_table", stream_config=config
                    ) as result:
                        async for row in result:
                            rows += 1
                            # Minimal processing
                            if rows % 2000 == 0:
                                await asyncio.sleep(0.001)

                    return rows

                # Run both concurrently
                streaming_task = asyncio.create_task(stream_large_dataset())
                queries_task = asyncio.create_task(run_concurrent_queries())

                # Wait for both to complete
                rows_streamed, _ = await asyncio.gather(streaming_task, queries_task)

                # Analyze results
                print("\n=== Concurrent Queries Test Results ===")
                print(f"Rows streamed: {rows_streamed:,}")
                print(f"Concurrent queries completed: {queries_completed}")

                if query_times:
                    avg_query_time = sum(query_times) / len(query_times)
                    max_query_time = max(query_times)

                    print(f"Average query time: {avg_query_time*1000:.1f}ms")
                    print(f"Max query time: {max_query_time*1000:.1f}ms")

                    # Assertions
                    assert queries_completed >= 15, "Not enough queries completed"
                    assert avg_query_time < 0.1, f"Queries too slow: {avg_query_time:.3f}s"

                    # Even the slowest query shouldn't be terribly slow
                    assert max_query_time < 0.5, f"Max query time too high: {max_query_time:.3f}s"

    async def test_multiple_streams_concurrent(self, cassandra_cluster):
        """
        Test that multiple streaming operations can run concurrently.

        This demonstrates that streaming doesn't monopolize the thread pool.
        """
        async with AsyncCluster(["localhost"]) as cluster:
            async with await cluster.connect() as session:
                await session.set_keyspace("test_streaming")

                async def stream_partition(partition: int) -> tuple[int, float]:
                    """Stream a specific partition."""
                    config = StreamConfig(fetch_size=500)
                    rows = 0
                    start = time.perf_counter()

                    stmt = await session.prepare(
                        "SELECT * FROM large_table WHERE partition_key = ?"
                    )

                    async with await session.execute_stream(
                        stmt, [partition], stream_config=config
                    ) as result:
                        async for row in result:
                            rows += 1

                    duration = time.perf_counter() - start
                    return rows, duration

                # Start multiple streams concurrently
                print("\n=== Multiple Concurrent Streams Test ===")
                start_time = time.perf_counter()

                # Stream 5 partitions concurrently
                tasks = [stream_partition(i) for i in range(5)]

                results = await asyncio.gather(*tasks)

                total_duration = time.perf_counter() - start_time

                # Analyze results
                total_rows = sum(rows for rows, _ in results)
                individual_durations = [duration for _, duration in results]

                print(f"Total rows streamed: {total_rows:,}")
                print(f"Total duration: {total_duration:.2f}s")
                print(f"Individual stream durations: {[f'{d:.2f}s' for d in individual_durations]}")

                # If streams were serialized, total duration would be sum of individual
                sum_durations = sum(individual_durations)
                concurrency_factor = sum_durations / total_duration

                print(f"Sum of individual durations: {sum_durations:.2f}s")
                print(f"Concurrency factor: {concurrency_factor:.1f}x")

                # Assertions
                assert total_rows == 5000, f"Expected 5000 rows total, got {total_rows}"

                # Should show significant concurrency (at least 2x)
                assert (
                    concurrency_factor > 2.0
                ), f"Insufficient concurrency: {concurrency_factor:.1f}x"

                # Total time should be much less than sum of individual times
                assert total_duration < sum_durations * 0.7, "Streams appear to be serialized"
