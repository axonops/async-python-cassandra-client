"""
Unit tests for constants module.
"""

import pytest

from async_cassandra.constants import (
    DEFAULT_CONNECTION_TIMEOUT,
    DEFAULT_EXECUTOR_THREADS,
    DEFAULT_FETCH_SIZE,
    DEFAULT_REQUEST_TIMEOUT,
    MAX_CONCURRENT_QUERIES,
    MAX_EXECUTOR_THREADS,
    MAX_RETRY_ATTEMPTS,
    MIN_EXECUTOR_THREADS,
)


class TestConstants:
    """Test all constants are properly defined and have reasonable values."""

    def test_default_values(self):
        """
        Test default values are reasonable.

        What this tests:
        ---------------
        1. Fetch size is 1000
        2. Default threads is 4
        3. Connection timeout 30s
        4. Request timeout 120s

        Why this matters:
        ----------------
        Default values affect:
        - Performance out-of-box
        - Resource consumption
        - Timeout behavior

        Good defaults mean most
        apps work without tuning.
        """
        assert DEFAULT_FETCH_SIZE == 1000
        assert DEFAULT_EXECUTOR_THREADS == 4
        assert DEFAULT_CONNECTION_TIMEOUT == 30.0  # Increased for larger heap sizes
        assert DEFAULT_REQUEST_TIMEOUT == 120.0

    def test_limits(self):
        """
        Test limit values are reasonable.

        What this tests:
        ---------------
        1. Max queries is 100
        2. Max retries is 3
        3. Values not too high
        4. Values not too low

        Why this matters:
        ----------------
        Limits prevent:
        - Resource exhaustion
        - Infinite retries
        - System overload

        Reasonable limits protect
        production systems.
        """
        assert MAX_CONCURRENT_QUERIES == 100
        assert MAX_RETRY_ATTEMPTS == 3

    def test_thread_pool_settings(self):
        """
        Test thread pool settings are reasonable.

        What this tests:
        ---------------
        1. Min threads >= 1
        2. Max threads <= 128
        3. Min < Max relationship
        4. Default within bounds

        Why this matters:
        ----------------
        Thread pool sizing affects:
        - Concurrent operations
        - Memory usage
        - CPU utilization

        Proper bounds prevent thread
        explosion and starvation.
        """
        assert MIN_EXECUTOR_THREADS == 1
        assert MAX_EXECUTOR_THREADS == 128
        assert MIN_EXECUTOR_THREADS < MAX_EXECUTOR_THREADS
        assert MIN_EXECUTOR_THREADS <= DEFAULT_EXECUTOR_THREADS <= MAX_EXECUTOR_THREADS

    def test_timeout_relationships(self):
        """
        Test timeout values have reasonable relationships.

        What this tests:
        ---------------
        1. Connection < Request timeout
        2. Both timeouts positive
        3. Logical ordering
        4. No zero timeouts

        Why this matters:
        ----------------
        Timeout ordering ensures:
        - Connect fails before request
        - Clear failure modes
        - No hanging operations

        Prevents confusing timeout
        cascades in production.
        """
        # Connection timeout should be less than request timeout
        assert DEFAULT_CONNECTION_TIMEOUT < DEFAULT_REQUEST_TIMEOUT
        # Both should be positive
        assert DEFAULT_CONNECTION_TIMEOUT > 0
        assert DEFAULT_REQUEST_TIMEOUT > 0

    def test_fetch_size_reasonable(self):
        """
        Test fetch size is within reasonable bounds.

        What this tests:
        ---------------
        1. Fetch size positive
        2. Not too large (<=10k)
        3. Efficient batching
        4. Memory reasonable

        Why this matters:
        ----------------
        Fetch size affects:
        - Memory per query
        - Network efficiency
        - Latency vs throughput

        Balance prevents OOM while
        maintaining performance.
        """
        assert DEFAULT_FETCH_SIZE > 0
        assert DEFAULT_FETCH_SIZE <= 10000  # Not too large

    def test_concurrent_queries_reasonable(self):
        """
        Test concurrent queries limit is reasonable.

        What this tests:
        ---------------
        1. Positive limit
        2. Not too high (<=1000)
        3. Allows parallelism
        4. Prevents overload

        Why this matters:
        ----------------
        Query limits prevent:
        - Connection exhaustion
        - Memory explosion
        - Cassandra overload

        Protects both client and
        server from abuse.
        """
        assert MAX_CONCURRENT_QUERIES > 0
        assert MAX_CONCURRENT_QUERIES <= 1000  # Not too large

    def test_retry_attempts_reasonable(self):
        """
        Test retry attempts is reasonable.

        What this tests:
        ---------------
        1. At least 1 retry
        2. Max 10 retries
        3. Not infinite
        4. Allows recovery

        Why this matters:
        ----------------
        Retry limits balance:
        - Transient error recovery
        - Avoiding retry storms
        - Fail-fast behavior

        Too many retries hurt
        more than help.
        """
        assert MAX_RETRY_ATTEMPTS > 0
        assert MAX_RETRY_ATTEMPTS <= 10  # Not too many

    def test_constant_types(self):
        """
        Test constants have correct types.

        What this tests:
        ---------------
        1. Integers are int
        2. Timeouts are float
        3. No string types
        4. Type consistency

        Why this matters:
        ----------------
        Type safety ensures:
        - No runtime conversions
        - Clear API contracts
        - Predictable behavior

        Wrong types cause subtle
        bugs in production.
        """
        assert isinstance(DEFAULT_FETCH_SIZE, int)
        assert isinstance(DEFAULT_EXECUTOR_THREADS, int)
        assert isinstance(DEFAULT_CONNECTION_TIMEOUT, float)
        assert isinstance(DEFAULT_REQUEST_TIMEOUT, float)
        assert isinstance(MAX_CONCURRENT_QUERIES, int)
        assert isinstance(MAX_RETRY_ATTEMPTS, int)
        assert isinstance(MIN_EXECUTOR_THREADS, int)
        assert isinstance(MAX_EXECUTOR_THREADS, int)

    def test_constants_immutable(self):
        """
        Test that constants cannot be modified (basic check).

        What this tests:
        ---------------
        1. All constants uppercase
        2. Follow Python convention
        3. Clear naming pattern
        4. Module organization

        Why this matters:
        ----------------
        Naming conventions:
        - Signal immutability
        - Improve readability
        - Prevent accidents

        UPPERCASE warns developers
        not to modify values.
        """
        # This is more of a convention test - Python doesn't have true constants
        # But we can verify the module defines them properly
        import async_cassandra.constants as constants_module

        # Verify all constants are uppercase (Python convention)
        for attr_name in dir(constants_module):
            if not attr_name.startswith("_"):
                attr_value = getattr(constants_module, attr_name)
                if isinstance(attr_value, (int, float, str)):
                    assert attr_name.isupper(), f"Constant {attr_name} should be uppercase"

    @pytest.mark.parametrize(
        "constant_name,min_value,max_value",
        [
            ("DEFAULT_FETCH_SIZE", 1, 50000),
            ("DEFAULT_EXECUTOR_THREADS", 1, 32),
            ("DEFAULT_CONNECTION_TIMEOUT", 1.0, 60.0),
            ("DEFAULT_REQUEST_TIMEOUT", 10.0, 600.0),
            ("MAX_CONCURRENT_QUERIES", 10, 10000),
            ("MAX_RETRY_ATTEMPTS", 1, 20),
            ("MIN_EXECUTOR_THREADS", 1, 4),
            ("MAX_EXECUTOR_THREADS", 32, 256),
        ],
    )
    def test_constant_ranges(self, constant_name, min_value, max_value):
        """
        Test that constants are within expected ranges.

        What this tests:
        ---------------
        1. Each constant in range
        2. Not too small
        3. Not too large
        4. Sensible values

        Why this matters:
        ----------------
        Range validation prevents:
        - Extreme configurations
        - Performance problems
        - Resource issues

        Catches config errors
        before deployment.
        """
        import async_cassandra.constants as constants_module

        value = getattr(constants_module, constant_name)
        assert (
            min_value <= value <= max_value
        ), f"{constant_name} value {value} is outside expected range [{min_value}, {max_value}]"

    def test_no_missing_constants(self):
        """
        Test that all expected constants are defined.

        What this tests:
        ---------------
        1. All constants present
        2. No missing values
        3. No extra constants
        4. API completeness

        Why this matters:
        ----------------
        Complete constants ensure:
        - No hardcoded values
        - Consistent configuration
        - Clear tuning points

        Missing constants force
        magic numbers in code.
        """
        expected_constants = {
            "DEFAULT_FETCH_SIZE",
            "DEFAULT_EXECUTOR_THREADS",
            "DEFAULT_CONNECTION_TIMEOUT",
            "DEFAULT_REQUEST_TIMEOUT",
            "MAX_CONCURRENT_QUERIES",
            "MAX_RETRY_ATTEMPTS",
            "MIN_EXECUTOR_THREADS",
            "MAX_EXECUTOR_THREADS",
        }

        import async_cassandra.constants as constants_module

        module_constants = {
            name for name in dir(constants_module) if not name.startswith("_") and name.isupper()
        }

        missing = expected_constants - module_constants
        assert not missing, f"Missing constants: {missing}"

        # Also check no unexpected constants
        unexpected = module_constants - expected_constants
        assert not unexpected, f"Unexpected constants: {unexpected}"
