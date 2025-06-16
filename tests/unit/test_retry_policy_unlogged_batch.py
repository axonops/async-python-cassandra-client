"""
Test retry policy for UNLOGGED_BATCH operations.

This test verifies that UNLOGGED_BATCH operations are only retried
when explicitly marked as idempotent, just like SIMPLE and BATCH operations.
"""

from unittest.mock import Mock

from cassandra.policies import ConsistencyLevel, RetryPolicy, WriteType
from cassandra.query import BatchStatement, BatchType

from async_cassandra.retry_policy import AsyncRetryPolicy


class TestUnloggedBatchRetry:
    """Test retry policy specifically for UNLOGGED_BATCH operations."""

    def test_unlogged_batch_not_retried_without_idempotent(self):
        """Test that UNLOGGED_BATCH is NOT retried without is_idempotent attribute."""
        policy = AsyncRetryPolicy(max_retries=2)

        # Create UNLOGGED batch without is_idempotent attribute
        batch = Mock(spec=BatchStatement)
        # Ensure is_idempotent is not set
        if hasattr(batch, "is_idempotent"):
            delattr(batch, "is_idempotent")

        # Test with UNLOGGED_BATCH write type
        decision, consistency = policy.on_write_timeout(
            query=batch,
            consistency=ConsistencyLevel.ONE,
            write_type=WriteType.UNLOGGED_BATCH,
            required_responses=1,
            received_responses=0,
            retry_num=0,
        )

        # Should NOT retry without explicit is_idempotent=True
        assert decision == RetryPolicy.RETHROW
        assert consistency is None

    def test_logged_batch_not_retried_without_idempotent(self):
        """Test that regular BATCH is not retried without is_idempotent."""
        policy = AsyncRetryPolicy(max_retries=2)

        # Create regular batch without is_idempotent
        batch = Mock(spec=BatchStatement)
        if hasattr(batch, "is_idempotent"):
            delattr(batch, "is_idempotent")

        # Test with regular BATCH write type
        decision, consistency = policy.on_write_timeout(
            query=batch,
            consistency=ConsistencyLevel.ONE,
            write_type=WriteType.BATCH,  # Regular logged batch
            required_responses=1,
            received_responses=0,
            retry_num=0,
        )

        assert decision == RetryPolicy.RETHROW
        assert consistency is None

    def test_unlogged_batch_with_explicit_idempotent_true(self):
        """Test UNLOGGED_BATCH with explicit is_idempotent=True."""
        policy = AsyncRetryPolicy(max_retries=2)

        # Create UNLOGGED batch with is_idempotent=True
        batch = Mock(spec=BatchStatement)
        batch.is_idempotent = True

        decision, consistency = policy.on_write_timeout(
            query=batch,
            consistency=ConsistencyLevel.ONE,
            write_type=WriteType.UNLOGGED_BATCH,
            required_responses=1,
            received_responses=0,
            retry_num=0,
        )

        assert decision == RetryPolicy.RETRY
        assert consistency == ConsistencyLevel.ONE

    def test_unlogged_batch_with_explicit_idempotent_false(self):
        """Test UNLOGGED_BATCH with explicit is_idempotent=False does NOT get retried."""
        policy = AsyncRetryPolicy(max_retries=2)

        # Create UNLOGGED batch with is_idempotent=False
        batch = Mock(spec=BatchStatement)
        batch.is_idempotent = False

        # With is_idempotent=False, UNLOGGED_BATCH should NOT be retried
        decision, consistency = policy.on_write_timeout(
            query=batch,
            consistency=ConsistencyLevel.ONE,
            write_type=WriteType.UNLOGGED_BATCH,
            required_responses=1,
            received_responses=0,
            retry_num=0,
        )

        assert decision == RetryPolicy.RETHROW
        assert consistency is None

    def test_unlogged_batch_respects_max_retries(self):
        """Test that UNLOGGED_BATCH respects max_retries limit."""
        policy = AsyncRetryPolicy(max_retries=2)

        batch = Mock(spec=BatchStatement)

        # Test at max retries
        decision, consistency = policy.on_write_timeout(
            query=batch,
            consistency=ConsistencyLevel.ONE,
            write_type=WriteType.UNLOGGED_BATCH,
            required_responses=1,
            received_responses=0,
            retry_num=2,  # At max retries
        )

        assert decision == RetryPolicy.RETHROW
        assert consistency is None

    def test_different_batch_types_behavior(self):
        """Test retry behavior for different batch types."""
        policy = AsyncRetryPolicy(max_retries=2)

        # Test data: (write_type, has_idempotent, idempotent_value, should_retry)
        test_cases = [
            (WriteType.SIMPLE, False, None, False),  # Simple without idempotent
            (WriteType.SIMPLE, True, True, True),  # Simple with idempotent=True
            (WriteType.BATCH, False, None, False),  # Logged batch without idempotent
            (WriteType.BATCH, True, True, True),  # Logged batch with idempotent=True
            (WriteType.UNLOGGED_BATCH, False, None, False),  # Unlogged batch without idempotent
            (WriteType.UNLOGGED_BATCH, True, True, True),  # Unlogged batch with idempotent=True
            (WriteType.UNLOGGED_BATCH, True, False, False),  # Unlogged batch with idempotent=False
            (WriteType.COUNTER, True, True, False),  # Counter (not in retry list)
        ]

        for write_type, has_attr, attr_value, should_retry in test_cases:
            query = Mock()
            if has_attr:
                query.is_idempotent = attr_value
            elif hasattr(query, "is_idempotent"):
                delattr(query, "is_idempotent")

            decision, _ = policy.on_write_timeout(
                query=query,
                consistency=ConsistencyLevel.ONE,
                write_type=write_type,
                required_responses=1,
                received_responses=0,
                retry_num=0,
            )

            if should_retry:
                assert (
                    decision == RetryPolicy.RETRY
                ), f"Failed for {write_type} with idempotent={attr_value}"
            else:
                assert (
                    decision == RetryPolicy.RETHROW
                ), f"Failed for {write_type} with idempotent={attr_value}"

    def test_real_batch_statement_types(self):
        """Test with real BatchStatement objects and different batch types."""
        policy = AsyncRetryPolicy(max_retries=2)

        # Create real batch statements
        logged_batch = BatchStatement(batch_type=BatchType.LOGGED)
        unlogged_batch = BatchStatement(batch_type=BatchType.UNLOGGED)
        counter_batch = BatchStatement(batch_type=BatchType.COUNTER)

        # Test logged batch without is_idempotent
        decision, _ = policy.on_write_timeout(
            query=logged_batch,
            consistency=ConsistencyLevel.ONE,
            write_type=WriteType.BATCH,
            required_responses=1,
            received_responses=0,
            retry_num=0,
        )
        assert decision == RetryPolicy.RETHROW

        # Test unlogged batch without is_idempotent (should NOT retry)
        decision, _ = policy.on_write_timeout(
            query=unlogged_batch,
            consistency=ConsistencyLevel.ONE,
            write_type=WriteType.UNLOGGED_BATCH,
            required_responses=1,
            received_responses=0,
            retry_num=0,
        )
        assert decision == RetryPolicy.RETHROW

        # Test counter batch (should not retry even if marked idempotent)
        counter_batch.is_idempotent = True
        decision, _ = policy.on_write_timeout(
            query=counter_batch,
            consistency=ConsistencyLevel.ONE,
            write_type=WriteType.COUNTER,
            required_responses=1,
            received_responses=0,
            retry_num=0,
        )
        assert decision == RetryPolicy.RETHROW
