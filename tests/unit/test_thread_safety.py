"""Core thread safety and event loop handling tests.

This module tests the critical thread pool configuration and event loop
integration that enables the async wrapper to work correctly.
"""

import asyncio
import threading
from unittest.mock import AsyncMock, Mock, patch

import pytest

from async_cassandra.utils import get_or_create_event_loop, safe_call_soon_threadsafe

# Test constants
MAX_WORKERS = 32
_thread_local = threading.local()


class TestEventLoopHandling:
    """Test event loop management in threaded environments."""

    @pytest.mark.core
    @pytest.mark.quick
    async def test_get_or_create_event_loop_main_thread(self):
        """Test getting event loop in main thread."""
        # In async context, should return the running loop
        expected_loop = asyncio.get_running_loop()
        result = get_or_create_event_loop()
        assert result == expected_loop

    @pytest.mark.core
    def test_get_or_create_event_loop_worker_thread(self):
        """Test creating event loop in worker thread."""
        result_loop = None

        def worker():
            nonlocal result_loop
            # Worker thread should create a new loop
            result_loop = get_or_create_event_loop()
            assert result_loop is not None
            assert isinstance(result_loop, asyncio.AbstractEventLoop)

        thread = threading.Thread(target=worker)
        thread.start()
        thread.join()

        assert result_loop is not None

    @pytest.mark.core
    @pytest.mark.critical
    def test_thread_local_event_loops(self):
        """Test that each thread gets its own event loop."""
        loops = []

        def worker():
            loop = get_or_create_event_loop()
            loops.append(loop)

        threads = []
        for _ in range(5):
            thread = threading.Thread(target=worker)
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # Each thread should have created a unique loop
        assert len(loops) == 5
        assert len(set(id(loop) for loop in loops)) == 5

    @pytest.mark.core
    async def test_safe_call_soon_threadsafe(self):
        """Test thread-safe callback scheduling."""
        result = []

        def callback(value):
            result.append(value)

        loop = asyncio.get_running_loop()

        # Schedule callback from same thread
        safe_call_soon_threadsafe(loop, callback, "test1")

        # Give callback time to execute
        await asyncio.sleep(0.1)

        assert result == ["test1"]

    @pytest.mark.core
    def test_safe_call_soon_threadsafe_from_thread(self):
        """Test scheduling callback from different thread."""
        result = []
        event = threading.Event()

        def callback(value):
            result.append(value)
            event.set()

        loop = asyncio.new_event_loop()

        def run_loop():
            asyncio.set_event_loop(loop)
            loop.run_forever()

        loop_thread = threading.Thread(target=run_loop)
        loop_thread.start()

        try:
            # Schedule from different thread
            def worker():
                safe_call_soon_threadsafe(loop, callback, "test2")

            worker_thread = threading.Thread(target=worker)
            worker_thread.start()
            worker_thread.join()

            # Wait for callback
            event.wait(timeout=1)
            assert result == ["test2"]

        finally:
            loop.call_soon_threadsafe(loop.stop)
            loop_thread.join()
            loop.close()

    @pytest.mark.core
    def test_safe_call_soon_threadsafe_closed_loop(self):
        """Test handling of closed event loop."""
        loop = asyncio.new_event_loop()
        loop.close()

        # Should handle gracefully
        safe_call_soon_threadsafe(loop, lambda: None)
        # No exception should be raised


class TestThreadPoolConfiguration:
    """Test thread pool configuration and limits."""

    @pytest.mark.core
    @pytest.mark.quick
    def test_max_workers_constant(self):
        """Test MAX_WORKERS is set correctly."""
        assert MAX_WORKERS == 32

    @pytest.mark.core
    def test_thread_pool_creation(self):
        """Test thread pool is created with correct parameters."""
        from async_cassandra.cluster import AsyncCluster

        cluster = AsyncCluster(executor_threads=16)
        assert cluster._cluster.executor._max_workers == 16

    @pytest.mark.core
    @pytest.mark.critical
    async def test_concurrent_operations_within_limit(self):
        """Test handling concurrent operations within thread pool limit."""
        from cassandra.cluster import ResponseFuture

        from async_cassandra.session import AsyncCassandraSession as AsyncSession

        mock_session = Mock()
        results = []

        def mock_execute_async(*args, **kwargs):
            mock_future = Mock(spec=ResponseFuture)
            mock_future.result.return_value = Mock(rows=[])
            mock_future.timeout = None
            mock_future.has_more_pages = False
            results.append(1)
            return mock_future

        mock_session.execute_async.side_effect = mock_execute_async

        async_session = AsyncSession(mock_session)

        # Run operations concurrently
        with patch("async_cassandra.session.AsyncResultHandler") as mock_handler_class:
            mock_handler = Mock()
            mock_handler.get_result = AsyncMock(return_value=Mock(rows=[]))
            mock_handler_class.return_value = mock_handler

            tasks = []
            for i in range(10):
                task = asyncio.create_task(async_session.execute(f"SELECT * FROM table{i}"))
                tasks.append(task)

            await asyncio.gather(*tasks)

        # All operations should complete
        assert len(results) == 10

    @pytest.mark.core
    def test_thread_local_storage(self):
        """Test thread-local storage for event loops."""
        # Each thread should have its own storage
        storage_values = []

        def worker(value):
            _thread_local.test_value = value
            storage_values.append((_thread_local.test_value, threading.current_thread().ident))

        threads = []
        for i in range(5):
            thread = threading.Thread(target=worker, args=(i,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # Each thread should have stored its own value
        assert len(storage_values) == 5
        values = [v[0] for v in storage_values]
        assert sorted(values) == [0, 1, 2, 3, 4]
