"""
Managed thread pool with idle thread cleanup.

This module provides a thread pool that automatically cleans up
idle threads to prevent resource leaks in long-running applications.
"""

import logging
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

logger = logging.getLogger(__name__)


class IdleThreadTracker:
    """Track thread activity for idle cleanup."""

    def __init__(self):
        """Initialize idle thread tracker."""
        self._last_activity: dict[int, float] = {}
        self._lock = threading.Lock()

    def mark_active(self, thread_id: int) -> None:
        """
        Mark a thread as active.

        Args:
            thread_id: Thread identifier
        """
        with self._lock:
            self._last_activity[thread_id] = time.time()

    def get_idle_threads(self, timeout_seconds: float) -> set[int]:
        """
        Get threads that have been idle longer than timeout.

        Args:
            timeout_seconds: Idle timeout in seconds

        Returns:
            Set of idle thread IDs
        """
        current_time = time.time()
        idle_threads = set()

        with self._lock:
            for thread_id, last_activity in self._last_activity.items():
                if current_time - last_activity > timeout_seconds:
                    idle_threads.add(thread_id)

        return idle_threads

    def cleanup_threads(self, thread_ids: list[int]) -> None:
        """
        Remove tracking data for cleaned up threads.

        Args:
            thread_ids: Thread IDs to clean up
        """
        with self._lock:
            for thread_id in thread_ids:
                self._last_activity.pop(thread_id, None)


class ManagedThreadPool:
    """Thread pool with automatic idle thread cleanup."""

    def __init__(
        self,
        max_workers: int,
        thread_name_prefix: str = "cdf_io_",
        idle_timeout_seconds: float = 60,
        cleanup_interval_seconds: float = 30,
    ):
        """
        Initialize managed thread pool.

        Args:
            max_workers: Maximum number of threads
            thread_name_prefix: Prefix for thread names
            idle_timeout_seconds: Seconds before idle thread cleanup (0 to disable)
            cleanup_interval_seconds: Interval between cleanup checks
        """
        self.max_workers = max_workers
        self.thread_name_prefix = thread_name_prefix
        self.idle_timeout_seconds = idle_timeout_seconds
        self.cleanup_interval_seconds = cleanup_interval_seconds

        # Create thread pool
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix=thread_name_prefix
        )

        # Idle tracking
        self._idle_tracker = IdleThreadTracker()

        # Cleanup thread
        self._cleanup_thread: threading.Thread | None = None
        self._shutdown = False
        self._shutdown_lock = threading.Lock()

    def submit(self, fn: Callable[..., Any], *args, **kwargs) -> Any:
        """
        Submit work to thread pool and track activity.

        Args:
            fn: Function to execute
            *args: Positional arguments
            **kwargs: Keyword arguments

        Returns:
            Future object
        """

        def wrapped_fn(*args, **kwargs):
            # Mark thread as active
            thread_id = threading.get_ident()
            self._idle_tracker.mark_active(thread_id)

            try:
                # Execute actual work
                return fn(*args, **kwargs)
            finally:
                # Mark active again after work
                self._idle_tracker.mark_active(thread_id)

        return self._executor.submit(wrapped_fn, *args, **kwargs)

    def _cleanup_idle_threads(self) -> int:
        """
        Clean up idle threads.

        Returns:
            Number of threads cleaned up
        """
        if self.idle_timeout_seconds == 0:
            logger.debug("Idle cleanup disabled (timeout=0)")
            return 0

        # Get idle threads
        idle_threads = self._idle_tracker.get_idle_threads(self.idle_timeout_seconds)
        logger.debug(f"Found {len(idle_threads)} idle threads: {idle_threads}")

        if not idle_threads:
            return 0

        # Get executor threads
        executor_threads = getattr(self._executor, "_threads", set())
        logger.debug(f"Executor has {len(executor_threads)} threads")

        # Find threads to clean up
        threads_to_clean = []
        for thread in executor_threads:
            if hasattr(thread, "ident") and thread.ident in idle_threads:
                threads_to_clean.append(thread.ident)

        if not threads_to_clean:
            logger.debug("No executor threads match idle threads")
            return 0

        logger.info(f"Cleaning up {len(threads_to_clean)} idle threads")

        # Shutdown and recreate executor
        # This is the safest way to clean up threads
        with self._shutdown_lock:
            if not self._shutdown:
                # Shutdown current executor (wait for active threads)
                self._executor.shutdown(wait=True)

                # Create new executor
                self._executor = ThreadPoolExecutor(
                    max_workers=self.max_workers, thread_name_prefix=self.thread_name_prefix
                )

                # Clean up tracking data
                self._idle_tracker.cleanup_threads(threads_to_clean)

        return len(threads_to_clean)

    def _cleanup_loop(self) -> None:
        """Periodic cleanup loop."""
        logger.debug(
            f"Starting cleanup loop with interval={self.cleanup_interval_seconds}s, timeout={self.idle_timeout_seconds}s"
        )
        while not self._shutdown:
            try:
                # Wait for interval
                time.sleep(self.cleanup_interval_seconds)

                if not self._shutdown:
                    logger.debug("Running idle thread cleanup check")
                    cleaned = self._cleanup_idle_threads()
                    if cleaned > 0:
                        logger.info(f"Cleaned up {cleaned} idle threads")

            except Exception as e:
                logger.error(f"Error in cleanup loop: {e}", exc_info=True)

    def start_cleanup_scheduler(self) -> None:
        """Start periodic cleanup scheduler."""
        if self.idle_timeout_seconds == 0:
            logger.debug("Idle cleanup disabled (timeout=0)")
            return

        if self._cleanup_thread is None or not self._cleanup_thread.is_alive():
            self._cleanup_thread = threading.Thread(
                target=self._cleanup_loop, name=f"{self.thread_name_prefix}cleanup", daemon=True
            )
            self._cleanup_thread.start()
            logger.info(
                f"Started idle thread cleanup scheduler (timeout={self.idle_timeout_seconds}s)"
            )

    def shutdown(self, wait: bool = True) -> None:
        """
        Shutdown thread pool and cleanup scheduler.

        Args:
            wait: Wait for threads to complete
        """
        with self._shutdown_lock:
            self._shutdown = True

            # Stop cleanup thread
            if self._cleanup_thread and self._cleanup_thread.is_alive():
                # Cleanup thread will exit on next iteration
                self._cleanup_thread.join(timeout=self.cleanup_interval_seconds + 1)

            # Shutdown executor
            self._executor.shutdown(wait=wait)
