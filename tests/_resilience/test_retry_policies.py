"""Consolidated retry policy tests.

This module combines all retry policy tests including basic functionality,
comprehensive scenarios, idempotency handling, and batch operations.
"""

from unittest.mock import Mock

import pytest
from cassandra import ConsistencyLevel
from cassandra.policies import RetryPolicy, WriteType
from cassandra.query import BatchStatement, BatchType, SimpleStatement

from async_cassandra.retry_policy import AsyncRetryPolicy


class TestRetryPolicyCore:
    """Core retry policy functionality tests."""

    @pytest.mark.resilience
    @pytest.mark.quick
    @pytest.mark.critical
    def test_initialization(self):
        """Test retry policy initialization with custom values."""
        policy = AsyncRetryPolicy(max_retries=5)
        assert policy.max_retries == 5

    @pytest.mark.resilience
    @pytest.mark.core
    def test_read_timeout_data_received(self):
        """Test retry on read timeout when data was received."""
        policy = AsyncRetryPolicy()
        query = Mock(is_idempotent=True)

        # First attempt - should retry on same host
        decision = policy.on_read_timeout(query, ConsistencyLevel.ONE, 1, 1, True, retry_num=0)
        assert decision == (RetryPolicy.RETRY, ConsistencyLevel.ONE)

        # Second attempt - should still retry (max_retries defaults to 3)
        decision = policy.on_read_timeout(query, ConsistencyLevel.ONE, 1, 1, True, retry_num=1)
        assert decision == (RetryPolicy.RETRY, ConsistencyLevel.ONE)

    @pytest.mark.resilience
    @pytest.mark.core
    def test_read_timeout_no_data_received(self):
        """Test retry on read timeout when no data was received."""
        policy = AsyncRetryPolicy()
        query = Mock(is_idempotent=True)

        # Should retry when no data received and enough responses
        decision = policy.on_read_timeout(query, ConsistencyLevel.ONE, 1, 1, False, retry_num=0)
        assert decision == (RetryPolicy.RETRY, ConsistencyLevel.ONE)

    @pytest.mark.resilience
    @pytest.mark.critical
    def test_write_timeout_idempotent(self):
        """Test write timeout retry for idempotent queries."""
        policy = AsyncRetryPolicy()

        # Idempotent query should retry
        query = SimpleStatement("SELECT * FROM test")
        query.is_idempotent = True  # Must be exactly True, not just truthy
        decision = policy.on_write_timeout(
            query, ConsistencyLevel.ONE, WriteType.SIMPLE, 1, 1, retry_num=0
        )
        assert decision == (RetryPolicy.RETRY, ConsistencyLevel.ONE)

        # Non-idempotent should not retry
        query.is_idempotent = False
        decision = policy.on_write_timeout(
            query, ConsistencyLevel.ONE, WriteType.SIMPLE, 1, 1, retry_num=0
        )
        assert decision == (RetryPolicy.RETHROW, None)

    @pytest.mark.resilience
    def test_write_timeout_batch_types(self):
        """Test write timeout for different batch types."""
        policy = AsyncRetryPolicy()
        query = SimpleStatement("SELECT * FROM test")
        query.is_idempotent = True

        # BATCH should retry if idempotent
        decision = policy.on_write_timeout(
            query, ConsistencyLevel.ONE, WriteType.BATCH, 1, 1, retry_num=0
        )
        assert decision == (RetryPolicy.RETRY, ConsistencyLevel.ONE)

        # UNLOGGED_BATCH should retry if idempotent
        decision = policy.on_write_timeout(
            query, ConsistencyLevel.ONE, WriteType.UNLOGGED_BATCH, 1, 1, retry_num=0
        )
        assert decision == (RetryPolicy.RETRY, ConsistencyLevel.ONE)

        # COUNTER should never retry
        decision = policy.on_write_timeout(
            query, ConsistencyLevel.ONE, WriteType.COUNTER, 1, 1, retry_num=0
        )
        assert decision == (RetryPolicy.RETHROW, None)

    @pytest.mark.resilience
    @pytest.mark.core
    def test_unavailable_exception(self):
        """Test retry on unavailable exception."""
        policy = AsyncRetryPolicy()
        query = Mock()

        # First attempt - retry on next host
        decision = policy.on_unavailable(query, ConsistencyLevel.ONE, 1, 0, retry_num=0)
        assert decision == (RetryPolicy.RETRY_NEXT_HOST, ConsistencyLevel.ONE)

        # Subsequent attempts - retry on same host
        decision = policy.on_unavailable(query, ConsistencyLevel.ONE, 1, 0, retry_num=1)
        assert decision == (RetryPolicy.RETRY, ConsistencyLevel.ONE)

    @pytest.mark.resilience
    def test_request_error(self):
        """Test retry on request error."""
        policy = AsyncRetryPolicy()
        query = Mock()

        # Should always retry on next host for request errors
        decision = policy.on_request_error(
            query, ConsistencyLevel.ONE, Exception("Connection error"), retry_num=0
        )
        assert decision == (RetryPolicy.RETRY_NEXT_HOST, ConsistencyLevel.ONE)

    @pytest.mark.resilience
    def test_max_retry_count_enforcement(self):
        """Test that max retry count is enforced."""
        policy = AsyncRetryPolicy(max_retries=2)
        query = Mock(is_idempotent=True)

        # Should allow retries up to max
        decision = policy.on_read_timeout(query, ConsistencyLevel.ONE, 1, 1, True, retry_num=1)
        assert decision == (RetryPolicy.RETRY, ConsistencyLevel.ONE)

        # Should rethrow when max exceeded
        decision = policy.on_read_timeout(query, ConsistencyLevel.ONE, 1, 1, True, retry_num=2)
        assert decision == (RetryPolicy.RETHROW, None)


class TestRetryPolicyIdempotency:
    """Test idempotency handling in retry policies."""

    @pytest.mark.resilience
    @pytest.mark.critical
    def test_strict_idempotency_check(self):
        """Test that only is_idempotent=True triggers retry, not truthy values."""
        policy = AsyncRetryPolicy()

        # True should retry
        query = SimpleStatement("SELECT * FROM test")
        query.is_idempotent = True
        decision = policy.on_write_timeout(
            query, ConsistencyLevel.ONE, WriteType.SIMPLE, 1, 1, retry_num=0
        )
        assert decision == (RetryPolicy.RETRY, ConsistencyLevel.ONE)

        # Truthy values should NOT retry
        for truthy_value in [1, "yes", ["not empty"], {"key": "value"}]:
            query = SimpleStatement("SELECT * FROM test")
            query.is_idempotent = truthy_value
            decision = policy.on_write_timeout(
                query, ConsistencyLevel.ONE, WriteType.SIMPLE, 1, 1, retry_num=0
            )
            assert decision == (RetryPolicy.RETHROW, None)

        # False, None, and missing should not retry
        for falsy_value in [False, None, 0, "", [], {}]:
            query = SimpleStatement("SELECT * FROM test")
            query.is_idempotent = falsy_value
            decision = policy.on_write_timeout(
                query, ConsistencyLevel.ONE, WriteType.SIMPLE, 1, 1, retry_num=0
            )
            assert decision == (RetryPolicy.RETHROW, None)

    @pytest.mark.resilience
    def test_missing_idempotency_attribute(self):
        """Test handling when is_idempotent attribute is missing."""
        policy = AsyncRetryPolicy()
        query = Mock(spec=[])  # No attributes

        # Should not retry when attribute is missing
        decision = policy.on_write_timeout(
            query, ConsistencyLevel.ONE, WriteType.SIMPLE, 1, 1, retry_num=0
        )
        assert decision == (RetryPolicy.RETHROW, None)

    @pytest.mark.resilience
    def test_real_statement_objects(self):
        """Test with real Cassandra statement objects."""
        policy = AsyncRetryPolicy()

        # SimpleStatement - not idempotent by default
        simple_stmt = SimpleStatement("SELECT * FROM users")
        decision = policy.on_write_timeout(
            simple_stmt, ConsistencyLevel.ONE, WriteType.SIMPLE, 1, 1, retry_num=0
        )
        assert decision == (RetryPolicy.RETHROW, None)

        # SimpleStatement - explicitly idempotent
        simple_stmt.is_idempotent = True
        decision = policy.on_write_timeout(
            simple_stmt, ConsistencyLevel.ONE, WriteType.SIMPLE, 1, 1, retry_num=0
        )
        assert decision == (RetryPolicy.RETRY, ConsistencyLevel.ONE)

        # BatchStatement
        batch = BatchStatement()
        batch.is_idempotent = True
        decision = policy.on_write_timeout(
            batch, ConsistencyLevel.ONE, WriteType.BATCH, 1, 1, retry_num=0
        )
        assert decision == (RetryPolicy.RETRY, ConsistencyLevel.ONE)

    @pytest.mark.resilience
    def test_batch_statement_types(self):
        """Test different batch statement types."""
        policy = AsyncRetryPolicy()

        # LOGGED batch (default)
        batch = BatchStatement(batch_type=BatchType.LOGGED)
        batch.is_idempotent = True
        decision = policy.on_write_timeout(
            batch, ConsistencyLevel.ONE, WriteType.BATCH, 1, 1, retry_num=0
        )
        assert decision == (RetryPolicy.RETRY, ConsistencyLevel.ONE)

        # UNLOGGED batch
        batch = BatchStatement(batch_type=BatchType.UNLOGGED)
        batch.is_idempotent = True
        decision = policy.on_write_timeout(
            batch, ConsistencyLevel.ONE, WriteType.UNLOGGED_BATCH, 1, 1, retry_num=0
        )
        assert decision == (RetryPolicy.RETRY, ConsistencyLevel.ONE)

        # COUNTER batch - should never retry
        batch = BatchStatement(batch_type=BatchType.COUNTER)
        batch.is_idempotent = True  # Even if marked idempotent
        decision = policy.on_write_timeout(
            batch, ConsistencyLevel.ONE, WriteType.COUNTER, 1, 1, retry_num=0
        )
        assert decision == (RetryPolicy.RETHROW, None)


class TestRetryPolicyComprehensive:
    """Comprehensive retry policy scenarios."""

    @pytest.mark.resilience
    def test_read_timeout_response_combinations(self):
        """Test various response received/required combinations."""
        policy = AsyncRetryPolicy()
        query = Mock(is_idempotent=True)

        test_cases = [
            # (received, required, data_retrieved, should_retry)
            (1, 1, True, True),  # Enough responses, data received
            (1, 1, False, True),  # Enough responses, no data
            (0, 1, False, False),  # Not enough responses
            (2, 3, True, True),  # Some data received, should retry
            (3, 3, False, True),  # Enough responses, no data
        ]

        for received, required, data_retrieved, should_retry in test_cases:
            decision = policy.on_read_timeout(
                query, ConsistencyLevel.ONE, required, received, data_retrieved, retry_num=0
            )
            if should_retry:
                assert decision == (RetryPolicy.RETRY, ConsistencyLevel.ONE)
            else:
                assert decision == (RetryPolicy.RETHROW, None)

    @pytest.mark.resilience
    def test_different_write_types(self):
        """Test all different write types."""
        policy = AsyncRetryPolicy()
        query_idempotent = SimpleStatement("SELECT * FROM test")
        query_idempotent.is_idempotent = True
        query_not_idempotent = SimpleStatement("SELECT * FROM test")
        query_not_idempotent.is_idempotent = False

        # Write types that respect idempotency
        idempotent_types = [WriteType.SIMPLE, WriteType.BATCH, WriteType.UNLOGGED_BATCH]

        for write_type in idempotent_types:
            # Should retry if idempotent
            decision = policy.on_write_timeout(
                query_idempotent, ConsistencyLevel.ONE, write_type, 1, 1, retry_num=0
            )
            assert decision == (RetryPolicy.RETRY, ConsistencyLevel.ONE)

            # Should not retry if not idempotent
            decision = policy.on_write_timeout(
                query_not_idempotent, ConsistencyLevel.ONE, write_type, 1, 1, retry_num=0
            )
            assert decision == (RetryPolicy.RETHROW, None)

        # Write types that never retry
        never_retry_types = [WriteType.COUNTER, WriteType.CAS, WriteType.VIEW, WriteType.CDC]

        for write_type in never_retry_types:
            # Should not retry even if idempotent
            decision = policy.on_write_timeout(
                query_idempotent, ConsistencyLevel.ONE, write_type, 1, 1, retry_num=0
            )
            assert decision == (RetryPolicy.RETHROW, None)

    @pytest.mark.resilience
    def test_consistency_level_preservation(self):
        """Test that consistency level is preserved across retries."""
        policy = AsyncRetryPolicy()
        query = Mock(is_idempotent=True)

        consistency_levels = [
            ConsistencyLevel.ONE,
            ConsistencyLevel.QUORUM,
            ConsistencyLevel.ALL,
            ConsistencyLevel.LOCAL_QUORUM,
        ]

        for cl in consistency_levels:
            decision = policy.on_read_timeout(query, cl, 1, 1, True, retry_num=0)
            assert decision == (RetryPolicy.RETRY, cl)

            decision = policy.on_unavailable(query, cl, 1, 0, retry_num=0)
            assert decision == (RetryPolicy.RETRY_NEXT_HOST, cl)
