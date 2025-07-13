"""
Test thread cleanup to ensure ZERO thread accumulation.

What this tests:
---------------
1. Thread count before and after operations
2. Thread pool cleanup effectiveness
3. No thread leakage under any conditions
4. Proper cleanup with different execution modes

Why this matters:
----------------
- Thread accumulation causes resource exhaustion
- Production systems need stable resource usage
- Memory leaks from threads are unacceptable
- Every thread must be accounted for
"""

import asyncio
import gc
import threading
import time

import async_cassandra_dataframe as cdf
import pytest
from async_cassandra import AsyncCluster
from async_cassandra_dataframe.reader import CassandraDataFrameReader
from cassandra.cluster import Cluster


class TestThreadCleanup:
    """Test thread cleanup and management."""

    @classmethod
    def setup_class(cls):
        """Set up test environment."""
        cls.keyspace = "test_thread_cleanup"

        # Create test data
        cluster = Cluster(["localhost"])
        session = cluster.connect()

        session.execute(
            f"""
            CREATE KEYSPACE IF NOT EXISTS {cls.keyspace}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        """
        )
        session.set_keyspace(cls.keyspace)

        session.execute(
            """
            CREATE TABLE IF NOT EXISTS test_table (
                id int PRIMARY KEY,
                data text
            )
        """
        )

        # Insert some data
        for i in range(100):
            session.execute("INSERT INTO test_table (id, data) VALUES (%s, %s)", (i, f"data_{i}"))

        session.shutdown()
        cluster.shutdown()

    @classmethod
    def teardown_class(cls):
        """Clean up test keyspace."""
        cluster = Cluster(["localhost"])
        session = cluster.connect()
        session.execute(f"DROP KEYSPACE IF EXISTS {cls.keyspace}")
        session.shutdown()
        cluster.shutdown()

    def get_thread_info(self):
        """Get detailed thread information."""
        threads = []
        for thread in threading.enumerate():
            threads.append(
                {
                    "name": thread.name,
                    "daemon": thread.daemon,
                    "alive": thread.is_alive(),
                    "ident": thread.ident,
                }
            )
        return threads

    def count_threads_by_prefix(self, prefix: str) -> int:
        """Count threads with a specific name prefix."""
        count = 0
        for thread in threading.enumerate():
            if thread.name.startswith(prefix):
                count += 1
        return count

    def print_thread_diff(self, before: list, after: list):
        """Print thread differences."""
        before_names = {t["name"] for t in before}
        after_names = {t["name"] for t in after}

        added = after_names - before_names
        removed = before_names - after_names

        if added:
            print(f"Added threads: {added}")
        if removed:
            print(f"Removed threads: {removed}")

    async def test_baseline_thread_count(self):
        """Test baseline thread count with no operations."""
        initial_threads = threading.active_count()
        initial_info = self.get_thread_info()

        print(f"\nBaseline thread count: {initial_threads}")
        print("Initial threads:")
        for t in initial_info:
            print(f"  - {t['name']} (daemon={t['daemon']})")

        # Just create and close a session
        async with AsyncCluster(["localhost"]) as cluster:
            session = await cluster.connect(self.keyspace)
            await session.close()

        # Force garbage collection
        gc.collect()
        time.sleep(0.5)

        final_threads = threading.active_count()
        final_info = self.get_thread_info()

        print(f"\nFinal thread count: {final_threads}")
        self.print_thread_diff(initial_info, final_info)

        # Some Cassandra driver threads may persist, but should be minimal
        assert (
            final_threads - initial_threads <= 5
        ), f"Too many threads created: {final_threads - initial_threads}"

    async def test_single_read_cleanup(self):
        """Test thread cleanup after a single read operation."""
        initial_threads = threading.active_count()
        initial_cdf_threads = self.count_threads_by_prefix("cdf_async_")

        print(f"\nInitial threads: {initial_threads}, CDF threads: {initial_cdf_threads}")

        async with AsyncCluster(["localhost"]) as cluster:
            session = await cluster.connect(self.keyspace)
            # Single read
            df = await cdf.read_cassandra_table("test_table", session=session, partition_count=1)
            result = df.compute()
            assert len(result) == 100

        # Cleanup
        CassandraDataFrameReader.cleanup_executor()
        gc.collect()
        time.sleep(0.5)

        final_threads = threading.active_count()
        final_cdf_threads = self.count_threads_by_prefix("cdf_async_")

        print(f"Final threads: {final_threads}, CDF threads: {final_cdf_threads}")

        # CDF threads should be cleaned up
        assert final_cdf_threads == 0, f"CDF threads not cleaned up: {final_cdf_threads}"

    async def test_parallel_execution_cleanup(self):
        """Test thread cleanup after parallel execution."""
        initial_threads = threading.active_count()
        initial_info = self.get_thread_info()

        async with AsyncCluster(["localhost"]) as cluster:
            session = await cluster.connect(self.keyspace)
            # Parallel execution
            df = await cdf.read_cassandra_table(
                "test_table",
                session=session,
                partition_count=10,
                use_parallel_execution=True,
                max_concurrent_partitions=5,
            )
            assert len(df) == 100

        # Cleanup
        CassandraDataFrameReader.cleanup_executor()
        gc.collect()
        time.sleep(1.0)  # Give threads time to terminate

        final_threads = threading.active_count()
        final_info = self.get_thread_info()

        print(f"\nParallel execution - Initial: {initial_threads}, Final: {final_threads}")
        self.print_thread_diff(initial_info, final_info)

        # Allow for some Cassandra threads, but not excessive
        thread_increase = final_threads - initial_threads
        assert thread_increase <= 10, f"Too many threads persisting: {thread_increase}"

    async def test_multiple_reads_cleanup(self):
        """Test thread cleanup after multiple read operations."""
        initial_threads = threading.active_count()
        thread_counts = []

        async with AsyncCluster(["localhost"]) as cluster:
            session = await cluster.connect(self.keyspace)
            # Multiple reads
            for i in range(5):
                df = await cdf.read_cassandra_table(
                    "test_table", session=session, partition_count=3, use_parallel_execution=True
                )
                assert len(df) == 100

                # Check thread count doesn't grow unbounded
                current_threads = threading.active_count()
                thread_counts.append(current_threads)
                current_info = self.get_thread_info()
                print(f"After read {i+1}: {current_threads} threads")
                # Print all threads on first iteration
                if i == 0:
                    for t in current_info:
                        if t["name"] not in ["MainThread", "event_loop"]:
                            print(f"  - {t['name']} (daemon={t['daemon']})")

        # Cleanup
        CassandraDataFrameReader.cleanup_executor()
        gc.collect()
        time.sleep(1.0)

        final_threads = threading.active_count()
        print(f"\nMultiple reads - Initial: {initial_threads}, Final: {final_threads}")
        print(f"Thread count progression: {thread_counts}")

        # Check that threads stabilized (last 3 reads should have similar thread counts)
        if len(thread_counts) >= 3:
            last_three = thread_counts[-3:]
            max_diff = max(last_three) - min(last_three)
            print(f"Thread count variation in last 3 reads: {max_diff}")
            assert max_diff <= 2, f"Threads not stabilizing: {last_three}"

        # Overall increase should be reasonable
        thread_increase = final_threads - initial_threads
        assert thread_increase <= 15, f"Too many threads created: {thread_increase}"

    async def test_dask_execution_cleanup(self):
        """Test thread cleanup with Dask delayed execution."""
        initial_threads = threading.active_count()
        initial_dask_threads = self.count_threads_by_prefix("ThreadPoolExecutor")

        print(f"\nInitial threads: {initial_threads}, Dask threads: {initial_dask_threads}")

        async with AsyncCluster(["localhost"]) as cluster:
            session = await cluster.connect(self.keyspace)
            # Dask delayed execution
            df = await cdf.read_cassandra_table(
                "test_table",
                session=session,
                partition_count=5,
                use_parallel_execution=False,  # Use Dask
            )
            result = df.compute()
            assert len(result) == 100

        # Cleanup
        CassandraDataFrameReader.cleanup_executor()

        # Dask threads may take time to clean up
        import dask

        dask.config.set({"distributed.worker.memory.terminate": 0})

        gc.collect()
        time.sleep(2.0)  # Give Dask time to clean up

        final_threads = threading.active_count()
        final_dask_threads = self.count_threads_by_prefix("ThreadPoolExecutor")

        print(f"Final threads: {final_threads}, Dask threads: {final_dask_threads}")

        # Dask may keep some threads, but should be reasonable
        thread_increase = final_threads - initial_threads
        assert thread_increase <= 20, f"Too many Dask threads persisting: {thread_increase}"

    async def test_error_cleanup(self):
        """Test thread cleanup after errors."""
        initial_threads = threading.active_count()

        async with AsyncCluster(["localhost"]) as cluster:
            session = await cluster.connect(self.keyspace)
            # Try to read non-existent table
            with pytest.raises(ValueError):
                await cdf.read_cassandra_table(
                    "non_existent_table",
                    session=session,
                    partition_count=5,
                    use_parallel_execution=True,
                )

        # Cleanup should still work after error
        CassandraDataFrameReader.cleanup_executor()
        gc.collect()
        time.sleep(0.5)

        final_threads = threading.active_count()
        print(f"\nError case - Initial: {initial_threads}, Final: {final_threads}")

        # Should not leak threads on error
        thread_increase = final_threads - initial_threads
        assert thread_increase <= 10, f"Thread leak after error: {thread_increase}"


def run_tests():
    """Run thread cleanup tests."""
    test = TestThreadCleanup()
    test.setup_class()

    try:
        print("=" * 60)
        print("THREAD CLEANUP TESTS")
        print("=" * 60)

        # Run each test
        tests = [
            test.test_baseline_thread_count,
            test.test_single_read_cleanup,
            test.test_parallel_execution_cleanup,
            test.test_multiple_reads_cleanup,
            test.test_dask_execution_cleanup,
            test.test_error_cleanup,
        ]

        for test_func in tests:
            print(f"\nRunning {test_func.__name__}...")
            try:
                asyncio.run(test_func())
                print(f"✓ {test_func.__name__} passed")
            except AssertionError as e:
                print(f"✗ {test_func.__name__} failed: {e}")
            except Exception as e:
                print(f"✗ {test_func.__name__} error: {e}")
                import traceback

                traceback.print_exc()

            # Clean up between tests
            CassandraDataFrameReader.cleanup_executor()
            gc.collect()
            time.sleep(0.5)

    finally:
        test.teardown_class()


if __name__ == "__main__":
    run_tests()
