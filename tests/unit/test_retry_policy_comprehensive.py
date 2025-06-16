"""
Comprehensive unit tests for retry policy behavior.
Tests all query types and edge cases to ensure retry logic is correct.
"""

from unittest.mock import Mock

import pytest
from cassandra.policies import RetryPolicy, WriteType
from cassandra.query import BatchStatement, ConsistencyLevel, PreparedStatement, SimpleStatement

from async_cassandra.retry_policy import AsyncRetryPolicy


class TestAsyncRetryPolicyComprehensive:
    """Comprehensive tests for AsyncRetryPolicy behavior."""

    @pytest.fixture
    def policy(self):
        """Create a retry policy instance."""
        return AsyncRetryPolicy(max_retries=3)

    # ========== READ TIMEOUT TESTS ==========

    def test_read_timeout_select_query_retried(self, policy):
        """Test that SELECT queries are retried on read timeout."""
        # Mock a SELECT query
        query = SimpleStatement("SELECT * FROM users WHERE id = ?")

        # Test various scenarios where SELECT should be retried
        scenarios = [
            # (required_responses, received_responses, data_retrieved, expected_decision)
            (2, 1, True, RetryPolicy.RETRY),  # Data retrieved, should retry
            (3, 3, False, RetryPolicy.RETRY),  # Enough responses, should retry
            (3, 4, False, RetryPolicy.RETRY),  # More than enough responses, should retry
            (2, 1, False, RetryPolicy.RETHROW),  # No data, not enough responses, don't retry
            (3, 1, False, RetryPolicy.RETHROW),  # No data, not enough responses, don't retry
        ]

        for required, received, data_retrieved, expected in scenarios:
            decision, consistency = policy.on_read_timeout(
                query=query,
                consistency=ConsistencyLevel.QUORUM,
                required_responses=required,
                received_responses=received,
                data_retrieved=data_retrieved,
                retry_num=0,
            )
            assert decision == expected, (
                f"Expected {expected} for SELECT with required={required}, "
                f"received={received}, data_retrieved={data_retrieved}"
            )
            if expected == RetryPolicy.RETRY:
                assert consistency == ConsistencyLevel.QUORUM

    def test_read_timeout_prepared_select_retried(self, policy):
        """Test that prepared SELECT statements are retried."""
        # Mock a prepared SELECT statement
        prepared = Mock(spec=PreparedStatement)
        prepared.query_string = "SELECT * FROM users WHERE id = ?"
        prepared.is_idempotent = False  # Should still retry for reads

        decision, consistency = policy.on_read_timeout(
            query=prepared,
            consistency=ConsistencyLevel.QUORUM,
            required_responses=2,
            received_responses=1,
            data_retrieved=True,
            retry_num=0,
        )

        assert decision == RetryPolicy.RETRY
        assert consistency == ConsistencyLevel.QUORUM

    def test_read_timeout_respects_max_retries(self, policy):
        """Test that read timeout respects max_retries limit."""
        query = SimpleStatement("SELECT * FROM users")

        # Test at max retries limit
        decision, consistency = policy.on_read_timeout(
            query=query,
            consistency=ConsistencyLevel.QUORUM,
            required_responses=2,
            received_responses=1,
            data_retrieved=True,  # Would normally retry
            retry_num=3,  # At max retries
        )

        assert decision == RetryPolicy.RETHROW
        assert consistency is None

    def test_read_timeout_count_queries_retried(self, policy):
        """Test that COUNT queries are retried."""
        count_query = SimpleStatement("SELECT COUNT(*) FROM users")

        decision, consistency = policy.on_read_timeout(
            query=count_query,
            consistency=ConsistencyLevel.QUORUM,
            required_responses=2,
            received_responses=1,
            data_retrieved=True,
            retry_num=0,
        )

        assert decision == RetryPolicy.RETRY
        assert consistency == ConsistencyLevel.QUORUM

    # ========== WRITE TIMEOUT TESTS ==========

    def test_write_timeout_requires_idempotency(self, policy):
        """Test that writes require is_idempotent=True to be retried."""
        # Non-idempotent write (default)
        insert_query = SimpleStatement("INSERT INTO users (id, name) VALUES (?, ?)")
        assert insert_query.is_idempotent is False

        decision, consistency = policy.on_write_timeout(
            query=insert_query,
            consistency=ConsistencyLevel.QUORUM,
            write_type=WriteType.SIMPLE,
            required_responses=2,
            received_responses=1,
            retry_num=0,
        )

        assert decision == RetryPolicy.RETHROW
        assert consistency is None

        # Idempotent write
        insert_query.is_idempotent = True

        decision, consistency = policy.on_write_timeout(
            query=insert_query,
            consistency=ConsistencyLevel.QUORUM,
            write_type=WriteType.SIMPLE,
            required_responses=2,
            received_responses=1,
            retry_num=0,
        )

        assert decision == RetryPolicy.RETRY
        assert consistency == ConsistencyLevel.QUORUM

    def test_write_timeout_none_idempotency_not_retried(self, policy):
        """Test that queries with is_idempotent=None are not retried."""
        query = Mock()
        query.is_idempotent = None  # Explicitly None

        decision, consistency = policy.on_write_timeout(
            query=query,
            consistency=ConsistencyLevel.QUORUM,
            write_type=WriteType.SIMPLE,
            required_responses=2,
            received_responses=1,
            retry_num=0,
        )

        assert decision == RetryPolicy.RETHROW
        assert consistency is None

    def test_write_timeout_missing_idempotency_not_retried(self, policy):
        """Test that queries without is_idempotent attribute are not retried."""
        query = Mock()
        # No is_idempotent attribute at all
        delattr(query, "is_idempotent")

        decision, consistency = policy.on_write_timeout(
            query=query,
            consistency=ConsistencyLevel.QUORUM,
            write_type=WriteType.SIMPLE,
            required_responses=2,
            received_responses=1,
            retry_num=0,
        )

        assert decision == RetryPolicy.RETHROW
        assert consistency is None

    def test_write_timeout_only_simple_batch_retried(self, policy):
        """Test that only SIMPLE and BATCH write types are retried."""
        query = Mock()
        query.is_idempotent = True

        write_types_and_expected = [
            (WriteType.SIMPLE, RetryPolicy.RETRY),
            (WriteType.BATCH, RetryPolicy.RETRY),
            (WriteType.UNLOGGED_BATCH, RetryPolicy.RETRY),  # Now retried when idempotent
            (WriteType.COUNTER, RetryPolicy.RETHROW),
            (WriteType.BATCH_LOG, RetryPolicy.RETHROW),
            (WriteType.CAS, RetryPolicy.RETHROW),  # Compare-and-set
        ]

        for write_type, expected in write_types_and_expected:
            decision, consistency = policy.on_write_timeout(
                query=query,
                consistency=ConsistencyLevel.QUORUM,
                write_type=write_type,
                required_responses=2,
                received_responses=1,
                retry_num=0,
            )

            assert decision == expected, f"Expected {expected} for write_type={write_type}"
            if expected == RetryPolicy.RETRY:
                assert consistency == ConsistencyLevel.QUORUM
            else:
                assert consistency is None

    def test_batch_statement_idempotency(self, policy):
        """Test batch statement retry behavior."""
        # Batch without idempotency
        batch = Mock(spec=BatchStatement)
        # getattr should return None if not set

        decision, consistency = policy.on_write_timeout(
            query=batch,
            consistency=ConsistencyLevel.QUORUM,
            write_type=WriteType.BATCH,
            required_responses=2,
            received_responses=1,
            retry_num=0,
        )

        assert decision == RetryPolicy.RETHROW

        # Batch with idempotency
        batch.is_idempotent = True

        decision, consistency = policy.on_write_timeout(
            query=batch,
            consistency=ConsistencyLevel.QUORUM,
            write_type=WriteType.BATCH,
            required_responses=2,
            received_responses=1,
            retry_num=0,
        )

        assert decision == RetryPolicy.RETRY

    # ========== UNAVAILABLE TESTS ==========

    def test_unavailable_retry_behavior(self, policy):
        """Test retry behavior for unavailable exceptions."""
        query = SimpleStatement("SELECT * FROM users")

        # First retry should try next host
        decision, consistency = policy.on_unavailable(
            query=query,
            consistency=ConsistencyLevel.QUORUM,
            required_replicas=3,
            alive_replicas=1,
            retry_num=0,
        )

        assert decision == RetryPolicy.RETRY_NEXT_HOST
        assert consistency == ConsistencyLevel.QUORUM

        # Subsequent retries should retry on same host
        decision, consistency = policy.on_unavailable(
            query=query,
            consistency=ConsistencyLevel.QUORUM,
            required_replicas=3,
            alive_replicas=1,
            retry_num=1,
        )

        assert decision == RetryPolicy.RETRY
        assert consistency == ConsistencyLevel.QUORUM

    # ========== REQUEST ERROR TESTS ==========

    def test_request_error_retry_behavior(self, policy):
        """Test retry behavior for request errors."""
        query = SimpleStatement("SELECT * FROM users")
        error = Exception("Connection error")

        # Should try next host
        decision, consistency = policy.on_request_error(
            query=query,
            consistency=ConsistencyLevel.QUORUM,
            error=error,
            retry_num=0,
        )

        assert decision == RetryPolicy.RETRY_NEXT_HOST
        assert consistency == ConsistencyLevel.QUORUM

    # ========== EDGE CASES ==========

    def test_idempotent_with_truthy_values(self, policy):
        """Test that only is_idempotent=True (not truthy values) triggers retry."""
        truthy_values = [1, "true", "yes", [], {}, object()]

        for value in truthy_values:
            query = Mock()
            query.is_idempotent = value  # Truthy but not True

            decision, consistency = policy.on_write_timeout(
                query=query,
                consistency=ConsistencyLevel.QUORUM,
                write_type=WriteType.SIMPLE,
                required_responses=2,
                received_responses=1,
                retry_num=0,
            )

            assert (
                decision == RetryPolicy.RETHROW
            ), f"is_idempotent={value} should not trigger retry"

    def test_different_consistency_levels(self, policy):
        """Test retry behavior maintains consistency level."""
        query = SimpleStatement("SELECT * FROM users")
        consistency_levels = [
            ConsistencyLevel.ONE,
            ConsistencyLevel.QUORUM,
            ConsistencyLevel.ALL,
            ConsistencyLevel.LOCAL_QUORUM,
            ConsistencyLevel.LOCAL_ONE,
        ]

        for cl in consistency_levels:
            decision, returned_cl = policy.on_read_timeout(
                query=query,
                consistency=cl,
                required_responses=2,
                received_responses=1,
                data_retrieved=True,
                retry_num=0,
            )

            assert decision == RetryPolicy.RETRY
            assert returned_cl == cl, f"Consistency level {cl} should be maintained"
