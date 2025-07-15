"""
Configuration for async-cassandra-dataframe.

This module provides configuration options for controlling various
aspects of the library's behavior.
"""

import os


class Config:
    """Configuration settings for async-cassandra-dataframe."""

    def __init__(self):
        """Initialize config from environment variables."""
        # Thread pool configuration
        self.THREAD_POOL_SIZE: int = int(os.environ.get("CDF_THREAD_POOL_SIZE", "2"))
        """Number of threads in the thread pool for sync/async bridge. Default: 2"""

        self.THREAD_NAME_PREFIX: str = os.environ.get("CDF_THREAD_NAME_PREFIX", "cdf_io_")
        """Prefix for thread names in the thread pool. Default: 'cdf_io_'"""

        # Memory configuration
        self.DEFAULT_MEMORY_PER_PARTITION_MB: int = int(
            os.environ.get("CDF_MEMORY_PER_PARTITION_MB", "128")
        )
        """Default memory limit per partition in MB. Default: 128"""

        self.DEFAULT_FETCH_SIZE: int = int(os.environ.get("CDF_FETCH_SIZE", "5000"))
        """Default number of rows to fetch per query. Default: 5000"""

        # Concurrency configuration
        self.DEFAULT_MAX_CONCURRENT_QUERIES: int | None = None
        """Default max concurrent queries to Cassandra. None means no limit."""

        self.DEFAULT_MAX_CONCURRENT_PARTITIONS: int = int(
            os.environ.get("CDF_MAX_CONCURRENT_PARTITIONS", "10")
        )
        """Default max partitions to read concurrently. Default: 10"""

        # Dask configuration
        self.DASK_USE_PYARROW_STRINGS: bool = False
        """Whether to use PyArrow strings in Dask DataFrames. Default: False"""

        # Thread pool management
        self.THREAD_IDLE_TIMEOUT_SECONDS: float = float(
            os.environ.get("CDF_THREAD_IDLE_TIMEOUT_SECONDS", "60")
        )
        """Seconds before idle threads are cleaned up. 0 to disable. Default: 60"""

        self.THREAD_CLEANUP_INTERVAL_SECONDS: float = float(
            os.environ.get("CDF_THREAD_CLEANUP_INTERVAL_SECONDS", "30")
        )
        """Interval between thread cleanup checks in seconds. Default: 30"""

    def get_thread_pool_size(self) -> int:
        """Get configured thread pool size."""
        return max(1, self.THREAD_POOL_SIZE)

    def get_thread_name_prefix(self) -> str:
        """Get configured thread name prefix."""
        # Check if it was dynamically set
        if hasattr(self, "_thread_name_prefix"):
            return self._thread_name_prefix
        return self.THREAD_NAME_PREFIX

    def set_thread_name_prefix(self, prefix: str) -> None:
        """
        Set thread name prefix.

        Args:
            prefix: Thread name prefix

        Note:
            This only affects new thread pools created after this call.
            Existing thread pools are not affected.
        """
        self._thread_name_prefix = prefix

    def set_thread_pool_size(self, size: int) -> None:
        """
        Set thread pool size.

        Args:
            size: Number of threads (must be >= 1)

        Note:
            This only affects new thread pools created after this call.
            Existing thread pools are not affected.
        """
        if size < 1:
            raise ValueError("Thread pool size must be >= 1")
        self.THREAD_POOL_SIZE = size


# Create singleton instance
config = Config()
