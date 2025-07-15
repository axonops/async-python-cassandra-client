"""
Event loop management for async-to-sync bridge.

Provides a shared event loop runner for executing async code
from synchronous contexts (e.g., Dask workers).
"""

import asyncio
import threading
from typing import Any, TypeVar

from .config import config
from .thread_pool import ManagedThreadPool

T = TypeVar("T")


class LoopRunner:
    """Manages a dedicated thread with an event loop for async execution."""

    def __init__(self):
        self.loop = asyncio.new_event_loop()
        self.thread = None
        self._ready = threading.Event()
        # Create a managed thread pool with idle cleanup
        self.executor = ManagedThreadPool(
            max_workers=config.get_thread_pool_size(),
            thread_name_prefix=config.get_thread_name_prefix(),
            idle_timeout_seconds=config.THREAD_IDLE_TIMEOUT_SECONDS,
            cleanup_interval_seconds=config.THREAD_CLEANUP_INTERVAL_SECONDS,
        )
        # Start the cleanup scheduler
        self.executor.start_cleanup_scheduler()

        # Set the internal ThreadPoolExecutor as the default executor
        self.loop.set_default_executor(self.executor._executor)

    def start(self):
        """Start the event loop in a dedicated thread."""

        def run():
            asyncio.set_event_loop(self.loop)
            self._ready.set()
            self.loop.run_forever()

        self.thread = threading.Thread(target=run, name="cdf_event_loop", daemon=True)
        self.thread.start()
        self._ready.wait()

    def run_coroutine(self, coro) -> Any:
        """Run a coroutine and return the result."""
        future = asyncio.run_coroutine_threadsafe(coro, self.loop)
        return future.result()

    def shutdown(self):
        """Clean shutdown of the loop and executor."""
        if self.loop and not self.loop.is_closed():
            # Schedule cleanup
            async def _shutdown():
                # Cancel all tasks
                tasks = [t for t in asyncio.all_tasks(self.loop) if not t.done()]
                for task in tasks:
                    task.cancel()
                # Shutdown async generators
                try:
                    await self.loop.shutdown_asyncgens()
                except Exception:
                    pass

            future = asyncio.run_coroutine_threadsafe(_shutdown(), self.loop)
            try:
                future.result(timeout=2.0)
            except Exception:
                pass

            # Stop the loop
            self.loop.call_soon_threadsafe(self.loop.stop)

            # Wait for thread
            if self.thread and self.thread.is_alive():
                self.thread.join(timeout=2.0)

            # Now shutdown the managed executor (which handles cleanup)
            self.executor.shutdown(wait=True)

            # Close the loop
            try:
                self.loop.close()
            except Exception:
                pass


class EventLoopManager:
    """Manages shared event loop for async-to-sync conversion."""

    _loop_runner = None
    _loop_runner_lock = threading.Lock()
    _loop_runner_config_hash = None  # Track config changes

    @classmethod
    def get_loop_runner(cls) -> LoopRunner:
        """Get or create the shared event loop runner."""
        # Check if config has changed
        current_config_hash = (
            config.get_thread_pool_size(),
            config.get_thread_name_prefix(),
            config.THREAD_IDLE_TIMEOUT_SECONDS,
            config.THREAD_CLEANUP_INTERVAL_SECONDS,
        )

        if cls._loop_runner is None or cls._loop_runner_config_hash != current_config_hash:
            with cls._loop_runner_lock:
                # Double-check inside lock
                if cls._loop_runner is None or cls._loop_runner_config_hash != current_config_hash:
                    # Shutdown old runner if config changed
                    if (
                        cls._loop_runner is not None
                        and cls._loop_runner_config_hash != current_config_hash
                    ):
                        cls._loop_runner.shutdown()
                        cls._loop_runner = None

                    cls._loop_runner = LoopRunner()
                    cls._loop_runner.start()
                    cls._loop_runner_config_hash = current_config_hash

        return cls._loop_runner

    @classmethod
    def cleanup(cls):
        """Shutdown the shared event loop runner."""
        if cls._loop_runner is not None:
            with cls._loop_runner_lock:
                if cls._loop_runner is not None:
                    cls._loop_runner.shutdown()
                    cls._loop_runner = None
                    cls._loop_runner_config_hash = None

    @classmethod
    def run_coroutine(cls, coro) -> Any:
        """Run a coroutine using the shared event loop."""
        runner = cls.get_loop_runner()
        return runner.run_coroutine(coro)
