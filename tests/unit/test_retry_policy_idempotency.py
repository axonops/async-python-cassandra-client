"""
Test retry policy idempotency checks.
"""

from unittest.mock import Mock

from cassandra.policies import WriteType
from cassandra.query import ConsistencyLevel, SimpleStatement

from async_cassandra.retry_policy import AsyncRetryPolicy


class TestRetryPolicyIdempotency:
    """Test that retry policy properly checks idempotency."""

    def test_write_timeout_idempotent_query(self):
        """Test that idempotent queries are retried on write timeout."""
        policy = AsyncRetryPolicy(max_retries=3)

        # Create a mock query marked as idempotent
        query = Mock()
        query.is_idempotent = True

        decision, consistency = policy.on_write_timeout(
            query=query,
            consistency=ConsistencyLevel.ONE,
            write_type=WriteType.SIMPLE,
            required_responses=1,
            received_responses=0,
            retry_num=0,
        )

        assert decision == policy.RETRY
        assert consistency == ConsistencyLevel.ONE

    def test_write_timeout_non_idempotent_query(self):
        """Test that non-idempotent queries are NOT retried on write timeout."""
        policy = AsyncRetryPolicy(max_retries=3)

        # Create a mock query marked as non-idempotent
        query = Mock()
        query.is_idempotent = False

        decision, consistency = policy.on_write_timeout(
            query=query,
            consistency=ConsistencyLevel.ONE,
            write_type=WriteType.SIMPLE,
            required_responses=1,
            received_responses=0,
            retry_num=0,
        )

        # Should NOT retry non-idempotent writes
        assert decision == policy.RETHROW
        assert consistency is None

    def test_write_timeout_no_idempotent_attribute(self):
        """Test behavior when query has no is_idempotent attribute - should NOT retry."""
        policy = AsyncRetryPolicy(max_retries=3)

        # Create a mock query without is_idempotent attribute
        query = Mock(spec=[])  # Empty spec means no attributes

        decision, consistency = policy.on_write_timeout(
            query=query,
            consistency=ConsistencyLevel.ONE,
            write_type=WriteType.SIMPLE,
            required_responses=1,
            received_responses=0,
            retry_num=0,
        )

        # Should NOT retry if no is_idempotent attribute (strict safety)
        assert decision == policy.RETHROW
        assert consistency is None

    def test_write_timeout_batch_idempotent(self):
        """Test that idempotent batch queries are retried."""
        policy = AsyncRetryPolicy(max_retries=3)

        query = Mock()
        query.is_idempotent = True

        decision, consistency = policy.on_write_timeout(
            query=query,
            consistency=ConsistencyLevel.ONE,
            write_type=WriteType.BATCH,
            required_responses=1,
            received_responses=0,
            retry_num=0,
        )

        assert decision == policy.RETRY
        assert consistency == ConsistencyLevel.ONE

    def test_write_timeout_batch_non_idempotent(self):
        """Test that non-idempotent batch queries are NOT retried."""
        policy = AsyncRetryPolicy(max_retries=3)

        query = Mock()
        query.is_idempotent = False

        decision, consistency = policy.on_write_timeout(
            query=query,
            consistency=ConsistencyLevel.ONE,
            write_type=WriteType.BATCH,
            required_responses=1,
            received_responses=0,
            retry_num=0,
        )

        assert decision == policy.RETHROW
        assert consistency is None

    def test_write_timeout_counter_update(self):
        """Test that counter updates are not retried."""
        policy = AsyncRetryPolicy(max_retries=3)

        # Counter updates have write_type "COUNTER"
        query = Mock()
        query.is_idempotent = True  # Even if marked idempotent

        decision, consistency = policy.on_write_timeout(
            query=query,
            consistency=ConsistencyLevel.ONE,
            write_type=WriteType.COUNTER,
            required_responses=1,
            received_responses=0,
            retry_num=0,
        )

        # Counter updates should not be retried
        assert decision == policy.RETHROW
        assert consistency is None

    def test_real_simple_statement_idempotent(self):
        """Test with real SimpleStatement marked as idempotent."""
        policy = AsyncRetryPolicy(max_retries=3)

        # Create real SimpleStatement
        query = SimpleStatement(
            "INSERT INTO users (id, name) VALUES (?, ?) IF NOT EXISTS", is_idempotent=True
        )

        decision, consistency = policy.on_write_timeout(
            query=query,
            consistency=ConsistencyLevel.ONE,
            write_type=WriteType.SIMPLE,
            required_responses=1,
            received_responses=0,
            retry_num=0,
        )

        assert decision == policy.RETRY
        assert consistency == ConsistencyLevel.ONE

    def test_real_simple_statement_non_idempotent(self):
        """Test with real SimpleStatement marked as non-idempotent."""
        policy = AsyncRetryPolicy(max_retries=3)

        # Create real SimpleStatement
        query = SimpleStatement(
            "UPDATE counters SET value = value + 1 WHERE id = ?", is_idempotent=False
        )

        decision, consistency = policy.on_write_timeout(
            query=query,
            consistency=ConsistencyLevel.ONE,
            write_type=WriteType.SIMPLE,
            required_responses=1,
            received_responses=0,
            retry_num=0,
        )

        assert decision == policy.RETHROW
        assert consistency is None

    def test_max_retries_still_applies(self):
        """Test that max retries limit is still enforced for idempotent queries."""
        policy = AsyncRetryPolicy(max_retries=2)

        query = Mock()
        query.is_idempotent = True

        # Third retry should fail even for idempotent queries
        decision, consistency = policy.on_write_timeout(
            query=query,
            consistency=ConsistencyLevel.ONE,
            write_type=WriteType.SIMPLE,
            required_responses=1,
            received_responses=0,
            retry_num=2,  # Already retried twice
        )

        assert decision == policy.RETHROW
        assert consistency is None

    def test_prepared_statement_without_idempotent(self):
        """Test PreparedStatement without is_idempotent should not retry."""
        from cassandra.query import PreparedStatement

        policy = AsyncRetryPolicy(max_retries=3)

        # Mock prepared statement without is_idempotent
        prepared = Mock(spec=PreparedStatement)
        # Don't set is_idempotent attribute

        decision, consistency = policy.on_write_timeout(
            query=prepared,
            consistency=ConsistencyLevel.ONE,
            write_type=WriteType.SIMPLE,
            required_responses=1,
            received_responses=0,
            retry_num=0,
        )

        assert decision == policy.RETHROW
        assert consistency is None

    def test_batch_statement_without_idempotent(self):
        """Test BatchStatement without is_idempotent should not retry."""
        from cassandra.query import BatchStatement

        policy = AsyncRetryPolicy(max_retries=3)

        # Mock batch statement without is_idempotent
        batch = Mock(spec=BatchStatement)
        # Don't set is_idempotent attribute

        decision, consistency = policy.on_write_timeout(
            query=batch,
            consistency=ConsistencyLevel.ONE,
            write_type=WriteType.BATCH,
            required_responses=1,
            received_responses=0,
            retry_num=0,
        )

        assert decision == policy.RETHROW
        assert consistency is None

    def test_default_simple_statement_not_idempotent(self):
        """Test that default SimpleStatement is not retried."""
        # Default SimpleStatement has is_idempotent=False
        query = SimpleStatement("INSERT INTO users (id, name) VALUES (?, ?)")
        # Check what the default actually is
        assert query.is_idempotent is False

        policy = AsyncRetryPolicy(max_retries=3)

        decision, consistency = policy.on_write_timeout(
            query=query,
            consistency=ConsistencyLevel.ONE,
            write_type=WriteType.SIMPLE,
            required_responses=1,
            received_responses=0,
            retry_num=0,
        )

        # Should NOT retry as is_idempotent is not explicitly True
        assert decision == policy.RETHROW
        assert consistency is None

    def test_only_explicit_true_is_retried(self):
        """Test that only is_idempotent=True is retried, not None or False."""
        policy = AsyncRetryPolicy(max_retries=3)

        test_cases = [
            (None, False),  # is_idempotent=None should not retry
            (False, False),  # is_idempotent=False should not retry
            (True, True),  # is_idempotent=True should retry
            (1, False),  # Truthy but not True should not retry
            ("true", False),  # String "true" should not retry
        ]

        for idempotent_value, should_retry in test_cases:
            query = Mock()
            query.is_idempotent = idempotent_value

            decision, consistency = policy.on_write_timeout(
                query=query,
                consistency=ConsistencyLevel.ONE,
                write_type=WriteType.SIMPLE,
                required_responses=1,
                received_responses=0,
                retry_num=0,
            )

            if should_retry:
                assert (
                    decision == policy.RETRY
                ), f"Expected RETRY for is_idempotent={idempotent_value}"
                assert consistency == ConsistencyLevel.ONE
            else:
                assert (
                    decision == policy.RETHROW
                ), f"Expected RETHROW for is_idempotent={idempotent_value}"
                assert consistency is None

    def test_different_write_types(self):
        """Test retry behavior for different write types."""
        policy = AsyncRetryPolicy(max_retries=3)

        # Even with is_idempotent=True, only SIMPLE, BATCH, and UNLOGGED_BATCH should retry
        write_types_should_retry = {
            WriteType.SIMPLE: True,
            WriteType.BATCH: True,
            WriteType.UNLOGGED_BATCH: True,  # Now retried when idempotent
            WriteType.COUNTER: False,  # Counter updates should never retry
            WriteType.CAS: False,  # Compare-and-swap should not retry
            "UNKNOWN": False,  # Unknown types should not retry (string to test unknown types)
        }

        for write_type, should_retry in write_types_should_retry.items():
            query = Mock()
            query.is_idempotent = True  # Mark as idempotent

            decision, consistency = policy.on_write_timeout(
                query=query,
                consistency=ConsistencyLevel.ONE,
                write_type=write_type,
                required_responses=1,
                received_responses=0,
                retry_num=0,
            )

            if should_retry:
                assert decision == policy.RETRY, f"Expected RETRY for write_type={write_type}"
                assert consistency == ConsistencyLevel.ONE
            else:
                assert decision == policy.RETHROW, f"Expected RETHROW for write_type={write_type}"
                assert consistency is None
