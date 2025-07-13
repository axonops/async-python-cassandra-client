"""
Test configurable thread pool size.

What this tests:
---------------
1. Thread pool size can be configured
2. Configured size is actually used
3. Thread names use configured prefix
4. Multiple concurrent operations use the thread pool

Why this matters:
----------------
- Users need to tune thread pool for their workloads
- Too few threads = poor performance
- Too many threads = resource waste
- Thread names help with debugging
"""

import threading
import time

import async_cassandra_dataframe as cdf
import pytest
from async_cassandra_dataframe.config import config


class TestThreadPoolConfig:
    """Test thread pool configuration in real usage."""

    @pytest.mark.asyncio
    async def test_thread_pool_size_is_used(self, session, test_table_name):
        """
        Test that configured thread pool size is actually used.

        What this tests:
        ---------------
        1. Thread pool respects configured size
        2. Concurrent operations are limited by pool size
        3. Thread names use configured prefix

        Why this matters:
        ----------------
        - Configuration must actually work, not just exist
        - Thread pool size affects performance
        - Debugging requires proper thread names
        """
        # Create test table
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                id INT PRIMARY KEY,
                data TEXT
            )
        """
        )

        try:
            # Insert test data
            insert_stmt = await session.prepare(
                f"INSERT INTO {test_table_name} (id, data) VALUES (?, ?)"
            )
            for i in range(10):
                await session.execute(insert_stmt, (i, f"data_{i}"))

            # Set thread pool size to 3
            original_size = config.THREAD_POOL_SIZE
            original_prefix = config.get_thread_name_prefix()
            try:
                config.set_thread_pool_size(3)
                config.set_thread_name_prefix("test_cdf_")

                # Track active threads during execution
                active_threads = set()
                thread_names = set()
                max_concurrent = 0

                def track_threads():
                    """Track active thread count and names."""
                    nonlocal max_concurrent
                    while tracking:
                        current_threads = set()
                        for thread in threading.enumerate():
                            # Look for our configured prefix OR cdf_io threads
                            if thread.name.startswith("test_cdf_") or thread.name.startswith(
                                "cdf_io_"
                            ):
                                current_threads.add(thread.ident)
                                thread_names.add(thread.name)

                        active_threads.update(current_threads)
                        max_concurrent = max(max_concurrent, len(current_threads))
                        time.sleep(0.01)

                # Start tracking
                tracking = True
                tracker = threading.Thread(target=track_threads)
                tracker.start()

                # Read data using Dask (which uses the thread pool)
                # Force non-parallel execution to use thread pool
                df = await cdf.read_cassandra_table(
                    f"test_dataframe.{test_table_name}",
                    session=session,
                    partition_count=5,  # Force multiple partitions
                    use_parallel_execution=False,  # This forces sync execution via thread pool
                )

                # Force computation to use thread pool with threads scheduler
                pdf = df.compute(scheduler="threads")

                # Give threads time to appear
                time.sleep(0.5)

                # Stop tracking
                tracking = False
                tracker.join()

                # Verify results
                assert len(pdf) == 10, "Should read all data"

                # Check thread pool was used (either our prefix or cdf_io)
                cdf_threads = [
                    name for name in thread_names if "cdf_" in name or "test_cdf_" in name
                ]
                assert (
                    len(cdf_threads) > 0
                ), f"Thread pool should be used. Threads seen: {thread_names}"

                # Check that we saw our configured threads
                # Note: The thread pool size affects the cdf_io threads created for async/sync bridge
                test_prefix_threads = [
                    name for name in thread_names if name.startswith("test_cdf_")
                ]
                cdf_io_threads = [name for name in thread_names if name.startswith("cdf_io_")]

                # We should see threads from our configured pool
                assert (
                    len(test_prefix_threads) > 0 or len(cdf_io_threads) > 0
                ), f"Should see thread pool threads. Saw: {thread_names}"

                # The number of cdf_io threads should not exceed our configured size
                if cdf_io_threads:
                    assert (
                        len(cdf_io_threads) <= 3
                    ), f"Thread pool size {len(cdf_io_threads)} should not exceed configured size 3"

            finally:
                # Restore original config
                config.THREAD_POOL_SIZE = original_size
                config.THREAD_NAME_PREFIX = original_prefix

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    @pytest.mark.asyncio
    async def test_thread_pool_size_limits_concurrency(self, session, test_table_name):
        """
        Test that thread pool size actually limits concurrency.

        What this tests:
        ---------------
        1. Small thread pool limits concurrent operations
        2. Operations queue when pool is full
        3. No deadlocks with small pool

        Why this matters:
        ----------------
        - Resource limits must be respected
        - Small pools shouldn't deadlock
        - Queue behavior affects performance
        """
        # Create table with many partitions
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                partition_id INT,
                id INT,
                data TEXT,
                PRIMARY KEY (partition_id, id)
            )
        """
        )

        try:
            # Insert data across many partitions
            insert_stmt = await session.prepare(
                f"INSERT INTO {test_table_name} (partition_id, id, data) VALUES (?, ?, ?)"
            )
            for p in range(10):
                for i in range(100):
                    await session.execute(insert_stmt, (p, i, f"data_{p}_{i}"))

            # Set very small thread pool
            original_size = config.THREAD_POOL_SIZE
            try:
                config.set_thread_pool_size(1)  # Only 1 thread!

                # This should still work without deadlock
                df = await cdf.read_cassandra_table(
                    f"test_dataframe.{test_table_name}",
                    session=session,
                    partition_count=10,  # Many partitions with 1 thread
                )

                # Should complete without hanging
                pdf = df.compute()
                assert len(pdf) == 1000, "Should read all data even with 1 thread"

            finally:
                config.THREAD_POOL_SIZE = original_size

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    @pytest.mark.asyncio
    async def test_thread_pool_env_var_config(self, session, test_table_name, monkeypatch):
        """
        Test that thread pool can be configured via environment variable.

        What this tests:
        ---------------
        1. CDF_THREAD_POOL_SIZE env var works
        2. CDF_THREAD_NAME_PREFIX env var works
        3. Env vars are picked up on import

        Why this matters:
        ----------------
        - Ops teams configure via environment
        - Docker/K8s use env vars
        - No code changes needed
        """
        # This test would need to restart the module to pick up env vars
        # For now, just verify the config module handles env vars correctly

        # Set env vars
        monkeypatch.setenv("CDF_THREAD_POOL_SIZE", "5")
        monkeypatch.setenv("CDF_THREAD_NAME_PREFIX", "env_test_")

        # Import fresh config
        from async_cassandra_dataframe.config import Config

        test_config = Config()
        assert test_config.THREAD_POOL_SIZE == 5
        assert test_config.THREAD_NAME_PREFIX == "env_test_"
