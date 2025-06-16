"""
Integration tests for thread pool configuration.
"""

import asyncio
import time
from threading import current_thread

import pytest
import pytest_asyncio

from async_cassandra import AsyncCluster


@pytest.mark.integration
class TestThreadPoolConfigurationIntegration:
    """Integration tests for thread pool configuration."""

    @pytest_asyncio.fixture
    async def cluster_factory(self):
        """Factory for creating clusters with different configurations."""
        clusters = []

        async def _create_cluster(**kwargs):
            cluster = AsyncCluster(**kwargs)
            clusters.append(cluster)
            return cluster

        yield _create_cluster

        # Cleanup all created clusters
        for cluster in clusters:
            await cluster.shutdown()

    @pytest.mark.asyncio
    async def test_thread_pool_size_affects_concurrency(self, cluster_factory):
        """Test that thread pool size actually limits concurrency."""
        # Create cluster with small thread pool
        cluster = await cluster_factory(contact_points=["localhost"], executor_threads=2)

        session = await cluster.connect()
        await session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS test_thread_pool
            WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
            """
        )
        await session.set_keyspace("test_thread_pool")

        # Create a table
        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS thread_test (
                id INT PRIMARY KEY,
                data TEXT
            )
            """
        )

        # Track which threads execute queries
        thread_names = set()

        def track_thread():
            thread_names.add(current_thread().name)
            time.sleep(0.1)  # Simulate slow query
            return current_thread().name

        # Run multiple queries concurrently
        tasks = []
        for i in range(10):
            # We need to run the tracking in the executor
            task = asyncio.get_event_loop().run_in_executor(cluster._cluster.executor, track_thread)
            tasks.append(task)

        await asyncio.gather(*tasks)

        # With 2 threads, we should see at most 2 different thread names
        assert len(thread_names) <= 2

        await session.close()

    @pytest.mark.asyncio
    async def test_larger_thread_pool_allows_more_concurrency(self, cluster_factory):
        """Test that larger thread pool allows more concurrent operations."""
        # Create cluster with larger thread pool
        cluster = await cluster_factory(contact_points=["localhost"], executor_threads=10)

        session = await cluster.connect()
        await session.set_keyspace("test_thread_pool")

        # Measure time for concurrent queries
        start_time = time.time()

        # Run 20 queries that each take ~100ms
        queries = []
        for i in range(20):
            query = session.execute(
                "SELECT * FROM system.local WHERE key = 'local' ALLOW FILTERING"
            )
            queries.append(query)

        await asyncio.gather(*queries)

        total_time = time.time() - start_time

        # With 10 threads and 20 queries, should complete in ~200ms (2 batches)
        # Add buffer for overhead
        assert total_time < 1.0

        await session.close()

    @pytest.mark.asyncio
    async def test_thread_pool_saturation_behavior(self, cluster_factory):
        """Test behavior when thread pool is saturated."""
        # Create cluster with minimal threads
        cluster = await cluster_factory(contact_points=["localhost"], executor_threads=1)

        session = await cluster.connect()

        # Create many concurrent queries
        query_count = 50
        queries = []

        for i in range(query_count):
            query = session.execute("SELECT release_version FROM system.local")
            queries.append(query)

        results = await asyncio.gather(*queries)

        # All queries should complete successfully
        assert len(results) == query_count
        assert all(result is not None for result in results)

        # With 1 thread, queries are serialized
        # This helps verify thread pool is actually limiting concurrency

        await session.close()

    @pytest.mark.asyncio
    async def test_executor_monitoring(self, cluster_factory):
        """Test monitoring executor queue depth."""
        cluster = await cluster_factory(contact_points=["localhost"], executor_threads=2)

        session = await cluster.connect()
        executor = cluster._cluster.executor

        # Submit many tasks to saturate the pool
        futures = []

        def slow_task():
            time.sleep(0.5)
            return True

        # Submit tasks directly to executor
        for _ in range(10):
            future = executor.submit(slow_task)
            futures.append(future)

        # Check that we can monitor the executor state
        # Note: _work_queue is implementation detail of ThreadPoolExecutor
        if hasattr(executor, "_work_queue"):
            queue_size = executor._work_queue.qsize()
            # Should have queued tasks since we only have 2 threads
            assert queue_size > 0

        # Wait for all to complete
        for future in futures:
            future.result()

        await session.close()

    @pytest.mark.asyncio
    async def test_different_workload_patterns(self, cluster_factory):
        """Test different thread pool sizes for different workload patterns."""

        # Test 1: Low concurrency workload (web app)
        cluster_low = await cluster_factory(contact_points=["localhost"], executor_threads=4)
        session_low = await cluster_low.connect()

        # Simulate web app pattern - sporadic queries
        for _ in range(5):
            await session_low.execute("SELECT * FROM system.local")
            await asyncio.sleep(0.1)

        await session_low.close()

        # Test 2: High concurrency workload (batch processing)
        cluster_high = await cluster_factory(contact_points=["localhost"], executor_threads=16)
        session_high = await cluster_high.connect()

        # Simulate batch processing - many concurrent queries
        batch_queries = []
        for _ in range(50):
            query = session_high.execute("SELECT * FROM system.local")
            batch_queries.append(query)

        await asyncio.gather(*batch_queries)
        await session_high.close()

    @pytest.mark.asyncio
    async def test_thread_pool_with_prepared_statements(self, cluster_factory):
        """Test thread pool behavior with prepared statements."""
        cluster = await cluster_factory(contact_points=["localhost"], executor_threads=4)

        session = await cluster.connect()

        # Ensure keyspace and table exist
        await session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS test_thread_pool
            WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
            """
        )
        await session.set_keyspace("test_thread_pool")

        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS prepared_test (
                id INT PRIMARY KEY,
                value TEXT
            )
            """
        )

        # Prepare statement
        prepared = await session.prepare("INSERT INTO prepared_test (id, value) VALUES (?, ?)")

        # Execute many prepared statements concurrently
        tasks = []
        for i in range(20):
            task = session.execute(prepared, [i, f"value_{i}"])
            tasks.append(task)

        await asyncio.gather(*tasks)

        # Verify all inserted
        result = await session.execute("SELECT COUNT(*) FROM prepared_test")
        count = result.one()[0]
        assert count == 20

        await session.close()

    @pytest.mark.asyncio
    async def test_thread_pool_memory_overhead(self, cluster_factory):
        """Test memory overhead of different thread pool sizes."""
        import os

        import psutil

        process = psutil.Process(os.getpid())

        # Measure baseline
        baseline_memory = process.memory_info().rss / 1024 / 1024  # MB

        # Create cluster with many threads
        cluster = await cluster_factory(contact_points=["localhost"], executor_threads=50)

        # Force thread creation by submitting tasks
        futures = []
        for _ in range(50):
            future = cluster._cluster.executor.submit(time.sleep, 0.1)
            futures.append(future)

        # Wait a bit for threads to fully initialize
        await asyncio.sleep(0.5)

        # Measure memory with threads
        with_threads_memory = process.memory_info().rss / 1024 / 1024  # MB

        memory_increase = with_threads_memory - baseline_memory
        per_thread_memory = memory_increase / 50

        print(f"Memory increase for 50 threads: {memory_increase:.1f} MB")
        print(f"Per thread: {per_thread_memory:.2f} MB")

        # Verify it's in reasonable range (0.01-3 MB per thread)
        assert 0.01 <= per_thread_memory <= 3.0

        # Cleanup
        for future in futures:
            future.cancel()

        await cluster.shutdown()
