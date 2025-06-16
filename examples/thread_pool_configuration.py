#!/usr/bin/env python3
"""
Example demonstrating thread pool configuration for different use cases.
"""

import asyncio
import time
from contextlib import asynccontextmanager

from async_cassandra import AsyncCassandraSession, AsyncCluster


@asynccontextmanager
async def create_cluster_with_threads(thread_count: int, name: str):
    """Create a cluster with specified thread count."""
    print(f"\n{name}: Creating cluster with {thread_count} threads")

    cluster = AsyncCluster(contact_points=["localhost"], executor_threads=thread_count)

    # Verify configuration
    actual_threads = cluster._cluster.executor._max_workers
    print(f"{name}: Thread pool size: {actual_threads}")

    session = await cluster.connect()

    try:
        yield session
    finally:
        await session.close()
        await cluster.shutdown()


async def simulate_workload(
    session: AsyncCassandraSession, name: str, query_count: int, delay: float = 0
):
    """Simulate a workload with specified number of queries."""
    print(f"\n{name}: Running {query_count} queries...")
    start_time = time.time()

    # Create concurrent queries
    queries = []
    for i in range(query_count):
        query = session.execute("SELECT release_version FROM system.local")
        queries.append(query)

        # Optional delay between query submissions
        if delay > 0:
            await asyncio.sleep(delay)

    # Wait for all queries to complete
    await asyncio.gather(*queries)

    elapsed = time.time() - start_time
    print(f"{name}: Completed {query_count} queries in {elapsed:.2f}s")
    print(f"{name}: Average: {elapsed/query_count*1000:.1f}ms per query")

    return elapsed


async def example_web_application():
    """Example: Web application with moderate concurrency."""
    print("\n" + "=" * 60)
    print("EXAMPLE 1: Web Application (Moderate Concurrency)")
    print("=" * 60)

    # Web apps typically have sporadic requests
    # 16 threads is a good starting point
    async with create_cluster_with_threads(16, "WebApp") as session:
        # Simulate sporadic web requests
        for i in range(5):
            await simulate_workload(session, f"WebApp-Batch{i+1}", 10, delay=0.01)
            await asyncio.sleep(0.5)  # Gap between request batches


async def example_batch_processing():
    """Example: Batch processing with high concurrency."""
    print("\n" + "=" * 60)
    print("EXAMPLE 2: Batch Processing (High Concurrency)")
    print("=" * 60)

    # Batch processing needs more threads for concurrent queries
    async with create_cluster_with_threads(32, "BatchProcessor") as session:
        # Simulate batch processing - many concurrent queries
        await simulate_workload(session, "BatchProcessor", 100)


async def example_thread_pool_comparison():
    """Example: Compare different thread pool sizes."""
    print("\n" + "=" * 60)
    print("EXAMPLE 3: Thread Pool Size Comparison")
    print("=" * 60)

    thread_counts = [2, 8, 16, 32]
    query_count = 50

    results = {}

    for threads in thread_counts:
        async with create_cluster_with_threads(threads, f"Test-{threads}threads") as session:
            elapsed = await simulate_workload(session, f"Test-{threads}threads", query_count)
            results[threads] = elapsed

    # Show comparison
    print("\n" + "-" * 40)
    print("Thread Pool Size Comparison Results:")
    print("-" * 40)
    print(f"{'Threads':>10} | {'Time (s)':>10} | {'Queries/sec':>12}")
    print("-" * 40)

    for threads, elapsed in sorted(results.items()):
        qps = query_count / elapsed
        print(f"{threads:>10} | {elapsed:>10.2f} | {qps:>12.1f}")


async def example_thread_starvation():
    """Example: Demonstrate thread pool starvation."""
    print("\n" + "=" * 60)
    print("EXAMPLE 4: Thread Pool Starvation")
    print("=" * 60)

    # Create cluster with very few threads
    async with create_cluster_with_threads(2, "Starved") as session:
        print("\nStarved: Attempting 50 concurrent queries with only 2 threads...")
        print("Starved: This will demonstrate queuing behavior")

        await simulate_workload(session, "Starved", 50)

        print("\nStarved: Notice how queries queue up waiting for threads")
        print("Starved: Total time is much longer than with adequate threads")


async def example_monitoring():
    """Example: Monitor thread pool usage."""
    print("\n" + "=" * 60)
    print("EXAMPLE 5: Thread Pool Monitoring")
    print("=" * 60)

    cluster = AsyncCluster(contact_points=["localhost"], executor_threads=4)

    session = await cluster.connect()
    executor = cluster._cluster.executor

    print(f"\nMonitoring: Thread pool has {executor._max_workers} threads")

    # Submit tasks to see thread usage
    print("\nMonitoring: Submitting 10 slow queries...")

    def slow_query():
        """Simulate a slow operation."""
        time.sleep(0.5)
        return "done"

    # Submit tasks directly to executor to monitor
    futures = []
    for i in range(10):
        future = executor.submit(slow_query)
        futures.append(future)

        # Check queue size (implementation detail)
        if hasattr(executor, "_work_queue"):
            queue_size = executor._work_queue.qsize()
            print(f"Monitoring: Submitted task {i+1}, queue size: {queue_size}")

    print("\nMonitoring: Waiting for all tasks to complete...")
    for i, future in enumerate(futures):
        future.result()
        print(f"Monitoring: Task {i+1} completed")

    await session.close()
    await cluster.shutdown()


async def main():
    """Run all examples."""
    print("Thread Pool Configuration Examples")
    print("==================================")

    # Check if Cassandra is available
    try:
        test_cluster = AsyncCluster(["localhost"])
        test_session = await test_cluster.connect()
        await test_session.close()
        await test_cluster.shutdown()
    except Exception as e:
        print("\nError: Cannot connect to Cassandra on localhost:9042")
        print(f"Details: {e}")
        print("\nPlease ensure Cassandra is running before running these examples.")
        return

    # Run examples
    try:
        await example_web_application()
        await example_batch_processing()
        await example_thread_pool_comparison()
        await example_thread_starvation()
        await example_monitoring()

    except KeyboardInterrupt:
        print("\n\nExamples interrupted by user")
    except Exception as e:
        print(f"\n\nError running examples: {e}")

    print("\n" + "=" * 60)
    print("Examples completed!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
