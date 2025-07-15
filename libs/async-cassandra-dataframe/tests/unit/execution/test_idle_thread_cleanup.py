"""
Test idle thread cleanup implementation.

What this tests:
---------------
1. Thread idle tracking
2. Cleanup scheduler logic
3. Thread pool lifecycle
4. Configuration handling

Why this matters:
----------------
- Resource management is critical
- Memory leaks hurt production
- Thread lifecycle must be correct
"""

import threading
import time
from unittest.mock import MagicMock, Mock, patch

from async_cassandra_dataframe.thread_pool import IdleThreadTracker, ManagedThreadPool


class TestIdleThreadTracker:
    """Test idle thread tracking logic."""

    def test_track_thread_activity(self):
        """
        Test tracking thread activity.

        What this tests:
        ---------------
        1. Threads marked active on use
        2. Last activity time updated
        3. Multiple threads tracked independently
        """
        tracker = IdleThreadTracker()

        # Track activity
        thread_id = threading.get_ident()
        tracker.mark_active(thread_id)

        # Check it's tracked
        assert thread_id in tracker._last_activity
        assert time.time() - tracker._last_activity[thread_id] < 0.1

        # Mark active again
        time.sleep(0.1)
        tracker.mark_active(thread_id)

        # Check time updated
        assert time.time() - tracker._last_activity[thread_id] < 0.05

    def test_get_idle_threads(self):
        """
        Test identifying idle threads.

        What this tests:
        ---------------
        1. Idle threads identified correctly
        2. Active threads not marked idle
        3. Timeout calculation works
        """
        tracker = IdleThreadTracker()

        # Add threads with different activity times
        thread1 = 1001
        thread2 = 1002
        thread3 = 1003

        # Thread 1: very old activity
        tracker._last_activity[thread1] = time.time() - 100

        # Thread 2: recent activity
        tracker._last_activity[thread2] = time.time() - 0.1

        # Thread 3: borderline
        tracker._last_activity[thread3] = time.time() - 5

        # Get idle threads with 3 second timeout
        idle = tracker.get_idle_threads(timeout_seconds=3)

        assert thread1 in idle
        assert thread2 not in idle
        assert thread3 in idle

    def test_cleanup_thread_tracking(self):
        """
        Test cleanup of thread tracking data.

        What this tests:
        ---------------
        1. Thread data removed on cleanup
        2. Only specified threads cleaned
        3. Active threads remain tracked
        """
        tracker = IdleThreadTracker()

        # Track multiple threads
        threads = [2001, 2002, 2003]
        for tid in threads:
            tracker.mark_active(tid)

        # Clean up some threads
        tracker.cleanup_threads([2001, 2003])

        # Check cleanup
        assert 2001 not in tracker._last_activity
        assert 2002 in tracker._last_activity
        assert 2003 not in tracker._last_activity


class TestManagedThreadPool:
    """Test managed thread pool with idle cleanup."""

    def test_thread_pool_creation(self):
        """
        Test creating managed thread pool.

        What this tests:
        ---------------
        1. Pool created with correct size
        2. Thread name prefix applied
        3. Idle timeout configured
        """
        pool = ManagedThreadPool(max_workers=4, thread_name_prefix="test_", idle_timeout_seconds=30)

        try:
            assert pool.max_workers == 4
            assert pool.thread_name_prefix == "test_"
            assert pool.idle_timeout_seconds == 30
            assert pool._executor is not None
        finally:
            pool.shutdown()

    def test_submit_marks_thread_active(self):
        """
        Test that submitting work marks thread as active.

        What this tests:
        ---------------
        1. Thread tracked when executing work
        2. Activity time updated correctly
        3. Work executes successfully
        """
        pool = ManagedThreadPool(max_workers=2, idle_timeout_seconds=10)

        try:
            # Track which thread runs the work
            thread_id = None

            def work():
                nonlocal thread_id
                thread_id = threading.get_ident()
                return "done"

            # Submit work
            future = pool.submit(work)
            result = future.result()

            # Check work completed
            assert result == "done"
            assert thread_id is not None

            # Check thread marked active
            assert thread_id in pool._idle_tracker._last_activity

        finally:
            pool.shutdown()

    @patch("async_cassandra_dataframe.thread_pool.ThreadPoolExecutor")
    def test_cleanup_idle_threads(self, mock_executor_class):
        """
        Test cleanup of idle threads.

        What this tests:
        ---------------
        1. Idle threads identified
        2. Executor shutdown called
        3. New executor created
        """
        # Mock executor
        mock_executor = MagicMock()
        mock_executor_class.return_value = mock_executor
        mock_executor._threads = set()

        pool = ManagedThreadPool(max_workers=2, idle_timeout_seconds=1)

        # Simulate idle threads
        pool._idle_tracker._last_activity[3001] = time.time() - 10
        pool._idle_tracker._last_activity[3002] = time.time() - 10

        # Mock thread objects
        thread1 = Mock()
        thread1.ident = 3001
        thread2 = Mock()
        thread2.ident = 3002
        mock_executor._threads = {thread1, thread2}

        # Run cleanup
        cleaned = pool._cleanup_idle_threads()

        # Check cleanup happened
        assert cleaned == 2
        assert mock_executor.shutdown.called
        assert mock_executor_class.call_count == 2  # Initial + recreate

    def test_cleanup_preserves_active_threads(self):
        """
        Test that cleanup doesn't affect active threads.

        What this tests:
        ---------------
        1. Active threads not cleaned up
        2. Work continues during cleanup
        3. Pool remains functional
        """
        pool = ManagedThreadPool(max_workers=2, idle_timeout_seconds=1)

        try:
            # Submit long-running work
            def long_work():
                time.sleep(2)
                return threading.get_ident()

            # Start work
            future = pool.submit(long_work)

            # Let thread start
            time.sleep(0.1)

            # Try cleanup (should not affect active thread)
            pool._cleanup_idle_threads()

            # Work should complete
            thread_id = future.result()
            assert thread_id is not None

        finally:
            pool.shutdown()

    def test_periodic_cleanup_scheduling(self):
        """
        Test periodic cleanup scheduling.

        What this tests:
        ---------------
        1. Cleanup scheduled periodically
        2. Cleanup runs at intervals
        3. Stops on shutdown
        """
        with patch.object(ManagedThreadPool, "_cleanup_idle_threads") as mock_cleanup:
            mock_cleanup.return_value = 0

            pool = ManagedThreadPool(
                max_workers=2, idle_timeout_seconds=0.5, cleanup_interval_seconds=0.1
            )

            try:
                # Start cleanup scheduler
                pool.start_cleanup_scheduler()

                # Wait for multiple cleanup cycles
                time.sleep(0.35)

                # Check cleanup was called multiple times
                assert mock_cleanup.call_count >= 3

            finally:
                pool.shutdown()

    def test_zero_timeout_disables_cleanup(self):
        """
        Test that zero timeout disables cleanup.

        What this tests:
        ---------------
        1. Zero timeout means no cleanup
        2. Threads persist indefinitely
        3. Scheduler not started
        """
        pool = ManagedThreadPool(max_workers=2, idle_timeout_seconds=0)

        try:
            # Submit work
            future = pool.submit(lambda: "test")
            future.result()

            # Try cleanup - should do nothing
            cleaned = pool._cleanup_idle_threads()
            assert cleaned == 0

            # Scheduler should not start
            pool.start_cleanup_scheduler()
            assert pool._cleanup_thread is None

        finally:
            pool.shutdown()

    def test_shutdown_stops_cleanup(self):
        """
        Test that shutdown stops cleanup scheduler.

        What this tests:
        ---------------
        1. Cleanup thread stops on shutdown
        2. Executor shuts down cleanly
        3. No operations after shutdown
        """
        pool = ManagedThreadPool(max_workers=2, idle_timeout_seconds=10)

        # Start scheduler
        pool.start_cleanup_scheduler()
        assert pool._cleanup_thread is not None
        assert pool._cleanup_thread.is_alive()

        # Shutdown
        pool.shutdown()

        # Check cleanup stopped
        assert pool._shutdown is True
        assert not pool._cleanup_thread.is_alive()
