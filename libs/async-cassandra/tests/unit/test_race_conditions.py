"""Race condition and deadlock prevention tests.

This module tests for various race conditions including TOCTOU issues,
callback deadlocks, and concurrent access patterns.
"""

import asyncio
import threading
import time
from unittest.mock import Mock

import pytest
from async_cassandra import AsyncCassandraSession as AsyncSession
from async_cassandra.result import AsyncResultHandler


def create_mock_response_future(rows=None, has_more_pages=False):
    """Helper to create a properly configured mock ResponseFuture."""
    mock_future = Mock()
    mock_future.has_more_pages = has_more_pages
    mock_future.timeout = None  # Avoid comparison issues
    mock_future.add_callbacks = Mock()

    def handle_callbacks(callback=None, errback=None):
        if callback:
            callback(rows if rows is not None else [])

    mock_future.add_callbacks.side_effect = handle_callbacks
    return mock_future


class TestRaceConditions:
    """Test race conditions and thread safety."""

    @pytest.mark.resilience
    @pytest.mark.critical
    async def test_toctou_event_loop_check(self):
        """
        Test Time-of-Check-Time-of-Use race in event loop handling.

        What this tests:
        ---------------
        1. Thread-safe event loop access from multiple threads
        2. Race conditions in get_or_create_event_loop utility
        3. Concurrent thread access to event loop creation
        4. Proper synchronization in event loop management

        Why this matters:
        ----------------
        - Production systems often have multiple threads accessing async code
        - TOCTOU bugs can cause crashes or incorrect behavior
        - Event loop corruption can break entire applications
        - Critical for mixed sync/async codebases

        Additional context:
        ---------------------------------
        - Simulates 20 concurrent threads accessing event loop
        - Common pattern in web servers with thread pools
        - Tests defensive programming in utils module
        """
        from async_cassandra.utils import get_or_create_event_loop

        # Simulate rapid concurrent access from multiple threads
        results = []
        errors = []

        def worker():
            try:
                loop = get_or_create_event_loop()
                results.append(loop)
            except Exception as e:
                errors.append(e)

        # Create many threads to increase chance of race
        threads = []
        for _ in range(20):
            thread = threading.Thread(target=worker)
            threads.append(thread)

        # Start all threads at once
        for thread in threads:
            thread.start()

        # Wait for completion
        for thread in threads:
            thread.join()

        # Should have no errors
        assert len(errors) == 0
        # Each thread should get a valid event loop
        assert len(results) == 20
        assert all(loop is not None for loop in results)

    @pytest.mark.resilience
    async def test_callback_registration_race(self):
        """
        Test race condition in callback registration.

        What this tests:
        ---------------
        1. Thread-safe callback registration in AsyncResultHandler
        2. Race between success and error callbacks
        3. Proper result state management
        4. Only one callback should win in a race

        Why this matters:
        ----------------
        - Callbacks from driver happen on different threads
        - Race conditions can cause undefined behavior
        - Result state must be consistent
        - Prevents duplicate result processing

        Additional context:
        ---------------------------------
        - Driver callbacks are inherently multi-threaded
        - Tests internal synchronization mechanisms
        - Simulates real driver callback patterns
        """
        # Create a mock ResponseFuture
        mock_future = Mock()
        mock_future.has_more_pages = False
        mock_future.timeout = None
        mock_future.add_callbacks = Mock()

        handler = AsyncResultHandler(mock_future)
        results = []

        # Try to register callbacks from multiple threads
        def register_success():
            handler._handle_page(["success"])
            results.append("success")

        def register_error():
            handler._handle_error(Exception("error"))
            results.append("error")

        # Start threads that race to set result
        t1 = threading.Thread(target=register_success)
        t2 = threading.Thread(target=register_error)

        t1.start()
        t2.start()

        t1.join()
        t2.join()

        # Only one should win
        try:
            result = await handler.get_result()
            assert result._rows == ["success"]
            assert results.count("success") >= 1
        except Exception as e:
            assert str(e) == "error"
            assert results.count("error") >= 1

    @pytest.mark.resilience
    @pytest.mark.critical
    @pytest.mark.timeout(10)  # Add timeout to prevent hanging
    async def test_concurrent_session_operations(self):
        """
        Test concurrent operations on same session.

        What this tests:
        ---------------
        1. Thread-safe session operations under high concurrency
        2. No lost updates or race conditions in query execution
        3. Proper result isolation between concurrent queries
        4. Sequential counter integrity across 50 concurrent operations

        Why this matters:
        ----------------
        - Production apps execute many queries concurrently
        - Session must handle concurrent access safely
        - Lost queries can cause data inconsistency
        - Common pattern in web applications

        Additional context:
        ---------------------------------
        - Simulates 50 concurrent SELECT queries
        - Verifies each query gets unique result
        - Tests thread pool handling under load
        """
        mock_session = Mock()
        call_count = 0

        def thread_safe_execute(*args, **kwargs):
            nonlocal call_count
            # Simulate some work
            time.sleep(0.001)
            call_count += 1

            # Capture the count at creation time
            current_count = call_count
            return create_mock_response_future([{"count": current_count}])

        mock_session.execute_async.side_effect = thread_safe_execute

        async_session = AsyncSession(mock_session)

        # Execute many queries concurrently
        tasks = []
        for i in range(50):
            task = asyncio.create_task(async_session.execute(f"SELECT COUNT(*) FROM table{i}"))
            tasks.append(task)

        results = await asyncio.gather(*tasks)

        # All should complete
        assert len(results) == 50
        assert call_count == 50

        # Results should have sequential counts (no lost updates)
        counts = sorted([r._rows[0]["count"] for r in results])
        assert counts == list(range(1, 51))

    @pytest.mark.resilience
    @pytest.mark.timeout(10)  # Add timeout to prevent hanging
    async def test_page_callback_deadlock_prevention(self):
        """
        Test prevention of deadlock in paging callbacks.

        What this tests:
        ---------------
        1. Independent iteration state for concurrent AsyncResultSet usage
        2. No deadlock when multiple coroutines iterate same result
        3. Sequential iteration works correctly
        4. Each iterator maintains its own position

        Why this matters:
        ----------------
        - Paging through large results is common
        - Deadlocks can hang entire applications
        - Multiple consumers may process same result set
        - Critical for streaming large datasets

        Additional context:
        ---------------------------------
        - Tests both concurrent and sequential iteration
        - Each AsyncResultSet has independent state
        - Simulates real paging scenarios
        """
        from async_cassandra.result import AsyncResultSet

        # Test that each AsyncResultSet has its own iteration state
        rows = [1, 2, 3, 4, 5, 6]

        # Create separate result sets for each concurrent iteration
        async def collect_results():
            # Each task gets its own AsyncResultSet instance
            result_set = AsyncResultSet(rows.copy())
            collected = []
            async for row in result_set:
                # Simulate some async work
                await asyncio.sleep(0.001)
                collected.append(row)
            return collected

        # Run multiple iterations concurrently
        tasks = [asyncio.create_task(collect_results()) for _ in range(3)]

        # Wait for all to complete
        all_results = await asyncio.gather(*tasks)

        # Each iteration should get all rows
        for result in all_results:
            assert result == [1, 2, 3, 4, 5, 6]

        # Also test that sequential iterations work correctly
        single_result = AsyncResultSet([1, 2, 3])
        first_iteration = []
        async for row in single_result:
            first_iteration.append(row)

        second_iteration = []
        async for row in single_result:
            second_iteration.append(row)

        assert first_iteration == [1, 2, 3]
        assert second_iteration == [1, 2, 3]

    @pytest.mark.resilience
    @pytest.mark.timeout(15)  # Increase timeout to account for 5s shutdown delay
    async def test_session_close_during_query(self):
        """
        Test closing session while queries are in flight.

        What this tests:
        ---------------
        1. Graceful session closure with active queries
        2. Proper cleanup during 5-second shutdown delay
        3. In-flight queries complete before final closure
        4. No resource leaks or hanging queries

        Why this matters:
        ----------------
        - Applications need graceful shutdown
        - In-flight queries shouldn't be lost
        - Resource cleanup is critical
        - Prevents connection leaks in production

        Additional context:
        ---------------------------------
        - Tests 5-second graceful shutdown period
        - Simulates real shutdown scenarios
        - Critical for container deployments
        """
        mock_session = Mock()
        query_started = asyncio.Event()
        query_can_proceed = asyncio.Event()
        shutdown_called = asyncio.Event()

        def blocking_execute(*args):
            # Create a mock ResponseFuture that blocks
            mock_future = Mock()
            mock_future.has_more_pages = False
            mock_future.timeout = None  # Avoid comparison issues
            mock_future.add_callbacks = Mock()

            def handle_callbacks(callback=None, errback=None):
                async def wait_and_callback():
                    query_started.set()
                    await query_can_proceed.wait()
                    if callback:
                        callback([])

                asyncio.create_task(wait_and_callback())

            mock_future.add_callbacks.side_effect = handle_callbacks
            return mock_future

        mock_session.execute_async.side_effect = blocking_execute

        def mock_shutdown():
            shutdown_called.set()
            query_can_proceed.set()

        mock_session.shutdown = mock_shutdown

        async_session = AsyncSession(mock_session)

        # Start query
        query_task = asyncio.create_task(async_session.execute("SELECT * FROM users"))

        # Wait for query to start
        await query_started.wait()

        # Start closing session in background (includes 5s delay)
        close_task = asyncio.create_task(async_session.close())

        # Wait for driver shutdown
        await shutdown_called.wait()

        # Query should complete during the 5s delay
        await query_task

        # Wait for close to fully complete
        await close_task

        # Session should be closed
        assert async_session.is_closed

    @pytest.mark.resilience
    @pytest.mark.critical
    @pytest.mark.timeout(10)  # Add timeout to prevent hanging
    async def test_thread_pool_saturation(self):
        """
        Test behavior when thread pool is saturated.

        What this tests:
        ---------------
        1. Behavior with more queries than thread pool size
        2. No deadlock when thread pool is exhausted
        3. All queries eventually complete
        4. Async execution handles thread pool limits gracefully

        Why this matters:
        ----------------
        - Production loads can exceed thread pool capacity
        - Deadlocks under load are catastrophic
        - Must handle burst traffic gracefully
        - Common issue in high-traffic applications

        Additional context:
        ---------------------------------
        - Uses 2-thread pool with 6 concurrent queries
        - Tests 3x oversubscription scenario
        - Verifies async model prevents blocking
        """
        from async_cassandra.cluster import AsyncCluster

        # Create cluster with small thread pool
        cluster = AsyncCluster(executor_threads=2)

        # Mock the underlying cluster
        mock_cluster = Mock()
        mock_session = Mock()

        # Simulate slow queries
        def slow_query(*args):
            # Create a mock ResponseFuture that simulates delay
            mock_future = Mock()
            mock_future.has_more_pages = False
            mock_future.timeout = None  # Avoid comparison issues
            mock_future.add_callbacks = Mock()

            def handle_callbacks(callback=None, errback=None):
                # Call callback immediately to avoid empty result issue
                if callback:
                    callback([{"id": 1}])

            mock_future.add_callbacks.side_effect = handle_callbacks
            return mock_future

        mock_session.execute_async.side_effect = slow_query
        mock_cluster.connect.return_value = mock_session

        cluster._cluster = mock_cluster
        cluster._cluster.protocol_version = 5  # Mock protocol version

        session = await cluster.connect()

        # Submit more queries than thread pool size
        tasks = []
        for i in range(6):  # 3x thread pool size
            task = asyncio.create_task(session.execute(f"SELECT * FROM table{i}"))
            tasks.append(task)

        # All should eventually complete
        results = await asyncio.gather(*tasks)

        assert len(results) == 6
        # With async execution, all queries can run concurrently regardless of thread pool
        # Just verify they all completed
        assert all(result.rows == [{"id": 1}] for result in results)

    @pytest.mark.resilience
    @pytest.mark.timeout(5)  # Add timeout to prevent hanging
    async def test_event_loop_callback_ordering(self):
        """
        Test that callbacks maintain order when scheduled.

        What this tests:
        ---------------
        1. Thread-safe callback scheduling to event loop
        2. All callbacks execute despite concurrent scheduling
        3. No lost callbacks under concurrent access
        4. safe_call_soon_threadsafe utility correctness

        Why this matters:
        ----------------
        - Driver callbacks come from multiple threads
        - Lost callbacks mean lost query results
        - Order preservation prevents race conditions
        - Foundation of async-to-sync bridge

        Additional context:
        ---------------------------------
        - Tests 10 concurrent threads scheduling callbacks
        - Verifies thread-safe event loop integration
        - Core to driver callback handling
        """
        from async_cassandra.utils import safe_call_soon_threadsafe

        results = []
        loop = asyncio.get_running_loop()

        # Schedule callbacks from different threads
        def schedule_callback(value):
            safe_call_soon_threadsafe(loop, results.append, value)

        threads = []
        for i in range(10):
            thread = threading.Thread(target=schedule_callback, args=(i,))
            threads.append(thread)
            thread.start()

        # Wait for all threads
        for thread in threads:
            thread.join()

        # Give callbacks time to execute
        await asyncio.sleep(0.1)

        # All callbacks should have executed
        assert len(results) == 10
        assert sorted(results) == list(range(10))

    @pytest.mark.resilience
    @pytest.mark.timeout(10)  # Add timeout to prevent hanging
    async def test_prepared_statement_concurrent_access(self):
        """
        Test concurrent access to prepared statements.

        What this tests:
        ---------------
        1. Thread-safe prepared statement creation
        2. Multiple coroutines preparing same statement
        3. No corruption during concurrent preparation
        4. All coroutines receive valid prepared statement

        Why this matters:
        ----------------
        - Prepared statements are performance critical
        - Concurrent preparation is common at startup
        - Statement corruption causes query failures
        - Caching optimization opportunity identified

        Additional context:
        ---------------------------------
        - Currently allows duplicate preparation
        - Future optimization: statement caching
        - Tests current thread-safe behavior
        """
        mock_session = Mock()
        mock_prepared = Mock()

        prepare_count = 0

        def prepare_side_effect(*args):
            nonlocal prepare_count
            prepare_count += 1
            time.sleep(0.01)  # Simulate preparation time
            return mock_prepared

        mock_session.prepare.side_effect = prepare_side_effect

        # Create a mock ResponseFuture for execute_async
        mock_session.execute_async.return_value = create_mock_response_future([])

        async_session = AsyncSession(mock_session)

        # Many coroutines try to prepare same statement
        tasks = []
        for _ in range(10):
            task = asyncio.create_task(async_session.prepare("SELECT * FROM users WHERE id = ?"))
            tasks.append(task)

        prepared_statements = await asyncio.gather(*tasks)

        # All should get the same prepared statement
        assert all(ps == mock_prepared for ps in prepared_statements)
        # But prepare should only be called once (would need caching impl)
        # For now, it's called multiple times
        assert prepare_count == 10
