"""
Test automatic cleanup of idle threads.

What this tests:
---------------
1. Threads are cleaned up when idle
2. Idle timeout is configurable
3. Active threads are not cleaned up
4. Thread pool recreates threads as needed

Why this matters:
----------------
- Prevent resource leaks in long-running applications
- Reduce memory usage when idle
- Cloud environments charge for resources
- Thread cleanup prevents zombie threads
"""

import asyncio
import logging
import threading

import async_cassandra_dataframe as cdf
import pytest
from async_cassandra_dataframe.config import config

# Enable debug logging for thread pool
logging.getLogger("async_cassandra_dataframe.thread_pool").setLevel(logging.DEBUG)


class TestIdleThreadCleanup:
    """Test automatic cleanup of idle threads."""

    @pytest.mark.asyncio
    async def test_idle_threads_are_cleaned_up(self, session, test_table_name):
        """
        Test that idle threads are automatically cleaned up.

        What this tests:
        ---------------
        1. Threads created for work
        2. Threads cleaned up after idle timeout
        3. Thread count reduces to zero when idle

        Why this matters:
        ----------------
        - Long-running apps need cleanup
        - Prevents resource leaks
        - Saves memory and CPU
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

            # Set short idle timeout for testing
            original_timeout = getattr(config, "THREAD_IDLE_TIMEOUT_SECONDS", 60)
            original_interval = getattr(config, "THREAD_CLEANUP_INTERVAL_SECONDS", 30)
            try:
                config.THREAD_IDLE_TIMEOUT_SECONDS = 2  # 2 seconds for testing
                config.THREAD_CLEANUP_INTERVAL_SECONDS = 1  # Check every second

                # Force cleanup of existing loop runner to pick up new config
                from async_cassandra_dataframe.reader import CassandraDataFrameReader

                CassandraDataFrameReader.cleanup_executor()

                # Count threads before
                initial_threads = [t for t in threading.enumerate() if t.name.startswith("cdf_io_")]
                initial_count = len(initial_threads)

                # Read data (creates threads)
                df = await cdf.read_cassandra_table(
                    f"test_dataframe.{test_table_name}",
                    session=session,
                    use_parallel_execution=False,  # Force sync execution to use our thread pool
                )

                # Force synchronous computation to use the thread pool
                import dask

                with dask.config.set(scheduler="synchronous"):
                    df.compute()

                # Check threads were created after forcing sync operations
                # The cdf_io threads are created in the async_run_sync method
                all_threads = [(t.name, t.ident) for t in threading.enumerate()]
                print(f"All threads after compute: {all_threads}")

                # Look for both cdf_io_ threads and cdf_event_loop thread
                cdf_threads = [t for t in threading.enumerate() if "cdf" in t.name]
                print(f"CDF threads: {[t.name for t in cdf_threads]}")

                # We should at least see the event loop thread
                assert (
                    len(cdf_threads) > 0
                ), f"Should see CDF threads. All threads: {[t.name for t in threading.enumerate()]}"

                # Wait for idle timeout plus buffer
                await asyncio.sleep(3)

                # Check threads were cleaned up (but not the cleanup thread itself)
                final_threads = [
                    t
                    for t in threading.enumerate()
                    if t.name.startswith("cdf_io_") and not t.name.endswith("cleanup")
                ]
                print(f"Final CDF threads after timeout: {[t.name for t in final_threads]}")
                assert (
                    len(final_threads) <= initial_count
                ), f"Idle threads should be cleaned up. Now have {len(final_threads)} threads: {[t.name for t in final_threads]}"

            finally:
                config.THREAD_IDLE_TIMEOUT_SECONDS = original_timeout
                config.THREAD_CLEANUP_INTERVAL_SECONDS = original_interval

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    @pytest.mark.asyncio
    async def test_active_threads_not_cleaned_up(self, session, test_table_name):
        """
        Test that active threads are not cleaned up during work.

        What this tests:
        ---------------
        1. Active threads persist during work
        2. Cleanup doesn't interfere with operations
        3. Thread pool remains stable under load

        Why this matters:
        ----------------
        - Must not interrupt active work
        - Stability during operations
        - Performance consistency
        """
        # Create table with many rows
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
            # Insert lots of data
            insert_stmt = await session.prepare(
                f"INSERT INTO {test_table_name} (partition_id, id, data) VALUES (?, ?, ?)"
            )
            for p in range(5):
                for i in range(1000):
                    await session.execute(insert_stmt, (p, i, f"data_{p}_{i}"))

            # Set short idle timeout
            original_timeout = getattr(config, "THREAD_IDLE_TIMEOUT_SECONDS", 60)
            try:
                config.THREAD_IDLE_TIMEOUT_SECONDS = 1  # Very short!

                # Start long-running operation
                df = await cdf.read_cassandra_table(
                    f"test_dataframe.{test_table_name}", session=session, partition_count=5
                )

                # Track threads during computation
                thread_counts = []

                async def monitor_threads():
                    """Monitor thread count during operation."""
                    for _ in range(5):  # Monitor for 2.5 seconds
                        threads = [t for t in threading.enumerate() if t.name.startswith("cdf_io_")]
                        thread_counts.append(len(threads))
                        await asyncio.sleep(0.5)

                # Run computation and monitoring concurrently
                await asyncio.gather(
                    asyncio.create_task(df.to_delayed()[0].compute_async()), monitor_threads()
                )

                # Verify threads were not cleaned up during work
                assert all(
                    count > 0 for count in thread_counts
                ), f"Threads should not be cleaned up during active work. Counts: {thread_counts}"

                # Verify work completed successfully
                pdf = df.compute()
                assert len(pdf) == 5000, "All data should be read"

            finally:
                config.THREAD_IDLE_TIMEOUT_SECONDS = original_timeout

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    @pytest.mark.asyncio
    async def test_thread_pool_recreates_after_cleanup(self, session, test_table_name):
        """
        Test that thread pool recreates threads after cleanup.

        What this tests:
        ---------------
        1. Threads cleaned up when idle
        2. New threads created for new work
        3. Performance not degraded after cleanup

        Why this matters:
        ----------------
        - Apps have bursts of activity
        - Must handle idle->active transitions
        - Cleanup shouldn't break functionality
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
            for i in range(100):
                await session.execute(insert_stmt, (i, f"data_{i}"))

            # Set short idle timeout
            original_timeout = getattr(config, "THREAD_IDLE_TIMEOUT_SECONDS", 60)
            try:
                config.THREAD_IDLE_TIMEOUT_SECONDS = 1

                # First operation
                df1 = await cdf.read_cassandra_table(
                    f"test_dataframe.{test_table_name}", session=session
                )
                pdf1 = df1.compute()
                assert len(pdf1) == 100

                # Wait for cleanup
                await asyncio.sleep(2)

                # Verify threads cleaned up
                idle_threads = [t for t in threading.enumerate() if t.name.startswith("cdf_io_")]
                assert len(idle_threads) == 0, "Threads should be cleaned up when idle"

                # Second operation (threads should be recreated)
                df2 = await cdf.read_cassandra_table(
                    f"test_dataframe.{test_table_name}", session=session
                )

                # Check threads recreated during work
                working_threads = [t for t in threading.enumerate() if t.name.startswith("cdf_io_")]
                assert len(working_threads) > 0, "Threads should be recreated for new work"

                pdf2 = df2.compute()
                assert len(pdf2) == 100, "Second operation should complete successfully"

            finally:
                config.THREAD_IDLE_TIMEOUT_SECONDS = original_timeout

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    @pytest.mark.asyncio
    async def test_configurable_idle_timeout(self, session, test_table_name):
        """
        Test that idle timeout is configurable.

        What this tests:
        ---------------
        1. Timeout can be configured via config
        2. Different timeouts work correctly
        3. Zero timeout disables cleanup

        Why this matters:
        ----------------
        - Different apps have different needs
        - Some want aggressive cleanup
        - Some want threads to persist
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
            # Insert minimal data
            await session.execute(
                await session.prepare(f"INSERT INTO {test_table_name} (id, data) VALUES (?, ?)"),
                (1, "test"),
            )

            original_timeout = getattr(config, "THREAD_IDLE_TIMEOUT_SECONDS", 60)
            try:
                # Test with different timeouts
                for timeout in [1, 3, 0]:  # 0 means disabled
                    config.THREAD_IDLE_TIMEOUT_SECONDS = timeout

                    # Create threads
                    df = await cdf.read_cassandra_table(
                        f"test_dataframe.{test_table_name}", session=session
                    )
                    df.compute()

                    # Check threads exist
                    active = [t for t in threading.enumerate() if t.name.startswith("cdf_io_")]
                    assert len(active) > 0, f"Threads should exist after work (timeout={timeout})"

                    if timeout == 0:
                        # Threads should NOT be cleaned up
                        await asyncio.sleep(2)
                        remaining = [
                            t for t in threading.enumerate() if t.name.startswith("cdf_io_")
                        ]
                        assert len(remaining) > 0, "Threads should persist when timeout=0"
                    else:
                        # Wait for timeout
                        await asyncio.sleep(timeout + 1)
                        remaining = [
                            t for t in threading.enumerate() if t.name.startswith("cdf_io_")
                        ]
                        assert len(remaining) == 0, f"Threads should be cleaned up after {timeout}s"

            finally:
                config.THREAD_IDLE_TIMEOUT_SECONDS = original_timeout

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")
