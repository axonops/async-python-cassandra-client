"""
Test configuration module.

What this tests:
---------------
1. Configuration loading from environment
2. Thread pool size configuration
3. Configuration validation
4. Runtime configuration changes

Why this matters:
----------------
- Users need to tune thread pool for their workloads
- Configuration affects performance
- Wrong config can cause issues
"""

import pytest

from async_cassandra_dataframe.config import Config, config


class TestConfig:
    """Test configuration functionality."""

    def test_default_thread_pool_size(self):
        """Test default thread pool size."""
        # Default should be 2
        assert config.THREAD_POOL_SIZE == 2
        assert config.get_thread_pool_size() == 2

    def test_thread_pool_size_from_env(self, monkeypatch):
        """Test loading thread pool size from environment."""
        # Set environment variable
        monkeypatch.setenv("CDF_THREAD_POOL_SIZE", "8")

        # Create new config instance to pick up env var
        new_config = Config()
        assert new_config.THREAD_POOL_SIZE == 8
        assert new_config.get_thread_pool_size() == 8

    def test_set_thread_pool_size(self):
        """Test setting thread pool size at runtime."""
        original = config.THREAD_POOL_SIZE
        try:
            # Set new size
            config.set_thread_pool_size(4)
            assert config.get_thread_pool_size() == 4

            # Test minimum enforcement
            with pytest.raises(ValueError, match="Thread pool size must be >= 1"):
                config.set_thread_pool_size(0)

            with pytest.raises(ValueError, match="Thread pool size must be >= 1"):
                config.set_thread_pool_size(-1)
        finally:
            # Restore original
            config.THREAD_POOL_SIZE = original

    def test_thread_name_prefix(self):
        """Test thread name prefix configuration."""
        assert config.THREAD_NAME_PREFIX == "cdf_io_"
        assert config.get_thread_name_prefix() == "cdf_io_"

    def test_thread_name_prefix_from_env(self, monkeypatch):
        """Test loading thread name prefix from environment."""
        monkeypatch.setenv("CDF_THREAD_NAME_PREFIX", "custom_")

        new_config = Config()
        assert new_config.THREAD_NAME_PREFIX == "custom_"
        assert new_config.get_thread_name_prefix() == "custom_"

    def test_memory_configuration(self):
        """Test memory configuration defaults."""
        assert config.DEFAULT_MEMORY_PER_PARTITION_MB == 128
        assert config.DEFAULT_FETCH_SIZE == 5000

    def test_concurrency_configuration(self):
        """Test concurrency configuration defaults."""
        assert config.DEFAULT_MAX_CONCURRENT_QUERIES is None
        assert config.DEFAULT_MAX_CONCURRENT_PARTITIONS == 10

    def test_dask_configuration(self):
        """Test Dask configuration defaults."""
        assert config.DASK_USE_PYARROW_STRINGS is False
