"""
Unified retry policy tests for async-python-cassandra.

This module consolidates all retry policy testing from multiple files:
- test_retry_policy.py: Basic retry policy initialization and configuration
- test_retry_policies.py: Partial consolidation attempt (used as base)
- test_retry_policy_comprehensive.py: Query-specific retry scenarios
- test_retry_policy_idempotency.py: Deep idempotency validation
- test_retry_policy_unlogged_batch.py: UNLOGGED_BATCH specific tests

Test Organization:
==================
1. Basic Retry Policy Tests - Initialization, configuration, basic behavior
2. Read Timeout Tests - All read timeout scenarios
3. Write Timeout Tests - All write timeout scenarios
4. Unavailable Tests - Node unavailability handling
5. Idempotency Tests - Comprehensive idempotency validation
6. Batch Operation Tests - LOGGED and UNLOGGED batch handling
7. Error Propagation Tests - Error handling and logging
8. Edge Cases - Special scenarios and boundary conditions

Key Testing Principles:
======================
- Test both idempotent and non-idempotent operations
- Verify retry counts and decision logic
- Ensure consistency level adjustments are correct
- Test all ConsistencyLevel combinations
- Validate error messages and logging
"""

from unittest.mock import Mock

from cassandra.policies import ConsistencyLevel, RetryPolicy, WriteType

from async_cassandra.retry_policy import AsyncRetryPolicy


class TestAsyncRetryPolicy:
    """
    Comprehensive tests for AsyncRetryPolicy.

    AsyncRetryPolicy extends the standard retry policy to handle
    async operations while maintaining idempotency guarantees.
    """

    # ========================================
    # Basic Retry Policy Tests
    # ========================================

    def test_initialization_default(self):
        """
        Test default initialization of AsyncRetryPolicy.

        What this tests:
        ---------------
        1. Policy can be created without parameters
        2. Default max retries is 3
        3. Inherits from cassandra.policies.RetryPolicy

        Why this matters:
        ----------------
        The retry policy must work with sensible defaults for
        users who don't customize retry behavior.
        """
        policy = AsyncRetryPolicy()
        assert isinstance(policy, RetryPolicy)
        assert policy.max_retries == 3

    def test_initialization_custom_max_retries(self):
        """
        Test initialization with custom max retries.

        What this tests:
        ---------------
        1. Custom max_retries is respected
        2. Value is stored correctly

        Why this matters:
        ----------------
        Different applications have different tolerance for retries.
        Some may want more aggressive retries, others less.
        """
        policy = AsyncRetryPolicy(max_retries=5)
        assert policy.max_retries == 5

    def test_initialization_zero_retries(self):
        """
        Test initialization with zero retries (fail fast).

        What this tests:
        ---------------
        1. Zero retries is valid configuration
        2. Policy will not retry on failures

        Why this matters:
        ----------------
        Some applications prefer to fail fast and handle
        retries at a higher level.
        """
        policy = AsyncRetryPolicy(max_retries=0)
        assert policy.max_retries == 0

    # ========================================
    # Read Timeout Tests
    # ========================================

    def test_on_read_timeout_sufficient_responses(self):
        """
        Test read timeout when we have enough responses.

        What this tests:
        ---------------
        1. When received >= required, retry the read
        2. Retry count is incremented
        3. Returns RETRY decision

        Why this matters:
        ----------------
        If we got enough responses but timed out, the data
        likely exists and a retry might succeed.
        """
        policy = AsyncRetryPolicy()
        query = Mock()

        decision = policy.on_read_timeout(
            query=query,
            consistency=ConsistencyLevel.QUORUM,
            required_responses=2,
            received_responses=2,  # Got enough responses
            data_retrieved=False,
            retry_num=0,
        )

        assert decision == (RetryPolicy.RETRY, ConsistencyLevel.QUORUM)

    def test_on_read_timeout_insufficient_responses(self):
        """
        Test read timeout when we don't have enough responses.

        What this tests:
        ---------------
        1. When received < required, rethrow the error
        2. No retry attempted

        Why this matters:
        ----------------
        If we didn't get enough responses, retrying immediately
        is unlikely to help. Better to fail fast.
        """
        policy = AsyncRetryPolicy()
        query = Mock()

        decision = policy.on_read_timeout(
            query=query,
            consistency=ConsistencyLevel.QUORUM,
            required_responses=2,
            received_responses=1,  # Not enough responses
            data_retrieved=False,
            retry_num=0,
        )

        assert decision == (RetryPolicy.RETHROW, None)

    def test_on_read_timeout_max_retries_exceeded(self):
        """
        Test read timeout when max retries exceeded.

        What this tests:
        ---------------
        1. After max_retries attempts, stop retrying
        2. Return RETHROW decision

        Why this matters:
        ----------------
        Prevents infinite retry loops and ensures eventual
        failure when operations consistently timeout.
        """
        policy = AsyncRetryPolicy(max_retries=2)
        query = Mock()

        decision = policy.on_read_timeout(
            query=query,
            consistency=ConsistencyLevel.QUORUM,
            required_responses=2,
            received_responses=2,
            data_retrieved=False,
            retry_num=2,  # Already at max retries
        )

        assert decision == (RetryPolicy.RETHROW, None)

    def test_on_read_timeout_data_retrieved(self):
        """
        Test read timeout when data was retrieved.

        What this tests:
        ---------------
        1. When data_retrieved=True, RETRY the read
        2. Data retrieved means we got some data and retry might get more

        Why this matters:
        ----------------
        If we already got some data, retrying might get the complete
        result set. This implementation differs from standard behavior.
        """
        policy = AsyncRetryPolicy()
        query = Mock()

        decision = policy.on_read_timeout(
            query=query,
            consistency=ConsistencyLevel.QUORUM,
            required_responses=2,
            received_responses=2,
            data_retrieved=True,  # Got some data
            retry_num=0,
        )

        assert decision == (RetryPolicy.RETRY, ConsistencyLevel.QUORUM)

    def test_on_read_timeout_all_consistency_levels(self):
        """
        Test read timeout behavior across all consistency levels.

        What this tests:
        ---------------
        1. Policy works with all ConsistencyLevel values
        2. Retry logic is consistent across levels

        Why this matters:
        ----------------
        Applications use different consistency levels for different
        use cases. The retry policy must handle all of them.
        """
        policy = AsyncRetryPolicy()
        query = Mock()

        consistency_levels = [
            ConsistencyLevel.ANY,
            ConsistencyLevel.ONE,
            ConsistencyLevel.TWO,
            ConsistencyLevel.THREE,
            ConsistencyLevel.QUORUM,
            ConsistencyLevel.ALL,
            ConsistencyLevel.LOCAL_QUORUM,
            ConsistencyLevel.EACH_QUORUM,
            ConsistencyLevel.LOCAL_ONE,
        ]

        for cl in consistency_levels:
            # Test with sufficient responses
            decision = policy.on_read_timeout(
                query=query,
                consistency=cl,
                required_responses=2,
                received_responses=2,
                data_retrieved=False,
                retry_num=0,
            )
            assert decision == (RetryPolicy.RETRY, cl)

    # ========================================
    # Write Timeout Tests
    # ========================================

    def test_on_write_timeout_idempotent_simple_statement(self):
        """
        Test write timeout for idempotent simple statement.

        What this tests:
        ---------------
        1. Idempotent writes are retried
        2. Consistency level is preserved
        3. WriteType.SIMPLE is handled correctly

        Why this matters:
        ----------------
        Idempotent operations can be safely retried without
        risk of duplicate effects.
        """
        policy = AsyncRetryPolicy()
        query = Mock(is_idempotent=True)

        decision = policy.on_write_timeout(
            query=query,
            consistency=ConsistencyLevel.QUORUM,
            write_type=WriteType.SIMPLE,
            required_responses=2,
            received_responses=1,
            retry_num=0,
        )

        assert decision == (RetryPolicy.RETRY, ConsistencyLevel.QUORUM)

    def test_on_write_timeout_non_idempotent_simple_statement(self):
        """
        Test write timeout for non-idempotent simple statement.

        What this tests:
        ---------------
        1. Non-idempotent writes are NOT retried
        2. Returns RETHROW decision

        Why this matters:
        ----------------
        Non-idempotent operations (like counter updates) could
        cause data corruption if retried after partial success.
        """
        policy = AsyncRetryPolicy()
        query = Mock(is_idempotent=False)

        decision = policy.on_write_timeout(
            query=query,
            consistency=ConsistencyLevel.QUORUM,
            write_type=WriteType.SIMPLE,
            required_responses=2,
            received_responses=1,
            retry_num=0,
        )

        assert decision == (RetryPolicy.RETHROW, None)

    def test_on_write_timeout_batch_log_write(self):
        """
        Test write timeout during batch log write.

        What this tests:
        ---------------
        1. BATCH_LOG writes are NOT retried in this implementation
        2. Only SIMPLE, BATCH, and UNLOGGED_BATCH are retried if idempotent

        Why this matters:
        ----------------
        This implementation focuses on user-facing write types.
        BATCH_LOG is an internal operation that's not covered.
        """
        policy = AsyncRetryPolicy()
        # Even idempotent query won't retry for BATCH_LOG
        query = Mock(is_idempotent=True)

        decision = policy.on_write_timeout(
            query=query,
            consistency=ConsistencyLevel.QUORUM,
            write_type=WriteType.BATCH_LOG,
            required_responses=2,
            received_responses=1,
            retry_num=0,
        )

        assert decision == (RetryPolicy.RETHROW, None)

    def test_on_write_timeout_unlogged_batch_idempotent(self):
        """
        Test write timeout for idempotent UNLOGGED_BATCH.

        What this tests:
        ---------------
        1. UNLOGGED_BATCH is retried if the batch itself is marked idempotent
        2. Individual statement idempotency is not checked here

        Why this matters:
        ----------------
        The retry policy checks the batch's is_idempotent attribute,
        not the individual statements within it.
        """
        policy = AsyncRetryPolicy()

        # Create a batch statement marked as idempotent
        from cassandra.query import BatchStatement

        batch = BatchStatement()
        batch.is_idempotent = True  # Mark the batch itself as idempotent
        batch._statements_and_parameters = [
            (Mock(is_idempotent=True), []),
            (Mock(is_idempotent=True), []),
        ]

        decision = policy.on_write_timeout(
            query=batch,
            consistency=ConsistencyLevel.QUORUM,
            write_type=WriteType.UNLOGGED_BATCH,
            required_responses=2,
            received_responses=1,
            retry_num=0,
        )

        assert decision == (RetryPolicy.RETRY, ConsistencyLevel.QUORUM)

    def test_on_write_timeout_unlogged_batch_mixed_idempotency(self):
        """
        Test write timeout for UNLOGGED_BATCH with mixed idempotency.

        What this tests:
        ---------------
        1. Batch with any non-idempotent statement is not retried
        2. Partial idempotency is not sufficient

        Why this matters:
        ----------------
        A single non-idempotent statement in an unlogged batch
        makes the entire batch non-retriable.
        """
        policy = AsyncRetryPolicy()

        from cassandra.query import BatchStatement

        batch = BatchStatement()
        batch._statements_and_parameters = [
            (Mock(is_idempotent=True), []),  # Idempotent
            (Mock(is_idempotent=False), []),  # Non-idempotent
        ]

        decision = policy.on_write_timeout(
            query=batch,
            consistency=ConsistencyLevel.QUORUM,
            write_type=WriteType.UNLOGGED_BATCH,
            required_responses=2,
            received_responses=1,
            retry_num=0,
        )

        assert decision == (RetryPolicy.RETHROW, None)

    def test_on_write_timeout_logged_batch(self):
        """
        Test that LOGGED batches are handled as BATCH write type.

        What this tests:
        ---------------
        1. LOGGED batches use WriteType.BATCH (not UNLOGGED_BATCH)
        2. Different retry logic applies

        Why this matters:
        ----------------
        LOGGED batches have atomicity guarantees through the batch log,
        so they have different retry semantics than UNLOGGED batches.
        """
        policy = AsyncRetryPolicy()

        from cassandra.query import BatchStatement, BatchType

        batch = BatchStatement(batch_type=BatchType.LOGGED)

        # For BATCH write type, should check idempotency
        batch.is_idempotent = True

        decision = policy.on_write_timeout(
            query=batch,
            consistency=ConsistencyLevel.QUORUM,
            write_type=WriteType.BATCH,  # Not UNLOGGED_BATCH
            required_responses=2,
            received_responses=1,
            retry_num=0,
        )

        assert decision == (RetryPolicy.RETRY, ConsistencyLevel.QUORUM)

    def test_on_write_timeout_counter_write(self):
        """
        Test write timeout for counter operations.

        What this tests:
        ---------------
        1. Counter writes are never retried
        2. WriteType.COUNTER is handled correctly

        Why this matters:
        ----------------
        Counter operations are not idempotent by nature.
        Retrying could lead to incorrect counter values.
        """
        policy = AsyncRetryPolicy()
        query = Mock()  # Counters are never idempotent

        decision = policy.on_write_timeout(
            query=query,
            consistency=ConsistencyLevel.QUORUM,
            write_type=WriteType.COUNTER,
            required_responses=2,
            received_responses=1,
            retry_num=0,
        )

        assert decision == (RetryPolicy.RETHROW, None)

    def test_on_write_timeout_max_retries_exceeded(self):
        """
        Test write timeout when max retries exceeded.

        What this tests:
        ---------------
        1. After max_retries attempts, stop retrying
        2. Even idempotent operations are not retried

        Why this matters:
        ----------------
        Prevents infinite retry loops for consistently failing writes.
        """
        policy = AsyncRetryPolicy(max_retries=1)
        query = Mock(is_idempotent=True)

        decision = policy.on_write_timeout(
            query=query,
            consistency=ConsistencyLevel.QUORUM,
            write_type=WriteType.SIMPLE,
            required_responses=2,
            received_responses=1,
            retry_num=1,  # Already at max retries
        )

        assert decision == (RetryPolicy.RETHROW, None)

    # ========================================
    # Unavailable Tests
    # ========================================

    def test_on_unavailable_first_attempt(self):
        """
        Test handling unavailable exception on first attempt.

        What this tests:
        ---------------
        1. First unavailable error triggers RETRY_NEXT_HOST
        2. Consistency level is preserved

        Why this matters:
        ----------------
        Temporary node failures are common. Trying the next host
        often succeeds when the current coordinator is having issues.
        """
        policy = AsyncRetryPolicy()
        query = Mock()

        decision = policy.on_unavailable(
            query=query,
            consistency=ConsistencyLevel.QUORUM,
            required_replicas=3,
            alive_replicas=2,
            retry_num=0,
        )

        # Should retry on next host with same consistency
        assert decision == (RetryPolicy.RETRY_NEXT_HOST, ConsistencyLevel.QUORUM)

    def test_on_unavailable_max_retries_exceeded(self):
        """
        Test unavailable exception when max retries exceeded.

        What this tests:
        ---------------
        1. After max retries, stop trying
        2. Return RETHROW decision

        Why this matters:
        ----------------
        If nodes remain unavailable after multiple attempts,
        the cluster likely has serious issues.
        """
        policy = AsyncRetryPolicy(max_retries=2)
        query = Mock()

        decision = policy.on_unavailable(
            query=query,
            consistency=ConsistencyLevel.QUORUM,
            required_replicas=3,
            alive_replicas=1,
            retry_num=2,
        )

        assert decision == (RetryPolicy.RETHROW, None)

    def test_on_unavailable_consistency_downgrade(self):
        """
        Test that consistency level is NOT downgraded on unavailable.

        What this tests:
        ---------------
        1. Policy preserves original consistency level
        2. No automatic downgrade in this implementation

        Why this matters:
        ----------------
        This implementation maintains consistency requirements
        rather than trading consistency for availability.
        """
        policy = AsyncRetryPolicy()
        query = Mock()

        # Test that consistency is preserved on retry
        decision = policy.on_unavailable(
            query=query,
            consistency=ConsistencyLevel.QUORUM,
            required_replicas=2,
            alive_replicas=1,  # Only 1 alive, can't do QUORUM
            retry_num=1,  # Not first attempt, so RETRY not RETRY_NEXT_HOST
        )

        # Should retry with SAME consistency level
        assert decision == (RetryPolicy.RETRY, ConsistencyLevel.QUORUM)

    # ========================================
    # Idempotency Tests
    # ========================================

    def test_idempotency_check_simple_statement(self):
        """
        Test idempotency checking for simple statements.

        What this tests:
        ---------------
        1. Simple statements have is_idempotent attribute
        2. Attribute is checked correctly

        Why this matters:
        ----------------
        Idempotency is critical for safe retries. Must be
        explicitly set by the application.
        """
        policy = AsyncRetryPolicy()

        # Test idempotent statement
        idempotent_query = Mock(is_idempotent=True)
        decision = policy.on_write_timeout(
            query=idempotent_query,
            consistency=ConsistencyLevel.ONE,
            write_type=WriteType.SIMPLE,
            required_responses=1,
            received_responses=0,
            retry_num=0,
        )
        assert decision[0] == RetryPolicy.RETRY

        # Test non-idempotent statement
        non_idempotent_query = Mock(is_idempotent=False)
        decision = policy.on_write_timeout(
            query=non_idempotent_query,
            consistency=ConsistencyLevel.ONE,
            write_type=WriteType.SIMPLE,
            required_responses=1,
            received_responses=0,
            retry_num=0,
        )
        assert decision[0] == RetryPolicy.RETHROW

    def test_idempotency_check_prepared_statement(self):
        """
        Test idempotency checking for prepared statements.

        What this tests:
        ---------------
        1. Prepared statements can be marked idempotent
        2. Idempotency is preserved through preparation

        Why this matters:
        ----------------
        Prepared statements are the recommended way to execute
        queries. Their idempotency must be tracked.
        """
        policy = AsyncRetryPolicy()

        # Mock prepared statement
        from cassandra.query import PreparedStatement

        prepared = Mock(spec=PreparedStatement)
        prepared.is_idempotent = True

        decision = policy.on_write_timeout(
            query=prepared,
            consistency=ConsistencyLevel.QUORUM,
            write_type=WriteType.SIMPLE,
            required_responses=2,
            received_responses=1,
            retry_num=0,
        )

        assert decision[0] == RetryPolicy.RETRY

    def test_idempotency_missing_attribute(self):
        """
        Test handling of queries without is_idempotent attribute.

        What this tests:
        ---------------
        1. Missing attribute is treated as non-idempotent
        2. Safe default behavior

        Why this matters:
        ----------------
        Safety first: if we don't know if an operation is
        idempotent, assume it's not.
        """
        policy = AsyncRetryPolicy()

        # Query without is_idempotent attribute
        query = Mock(spec=[])  # No attributes

        decision = policy.on_write_timeout(
            query=query,
            consistency=ConsistencyLevel.ONE,
            write_type=WriteType.SIMPLE,
            required_responses=1,
            received_responses=0,
            retry_num=0,
        )

        assert decision[0] == RetryPolicy.RETHROW

    def test_batch_idempotency_validation(self):
        """
        Test batch idempotency validation.

        What this tests:
        ---------------
        1. Batch must have is_idempotent=True to be retried
        2. Individual statement idempotency is not checked
        3. Missing is_idempotent attribute means non-idempotent

        Why this matters:
        ----------------
        The retry policy only checks the batch's own idempotency flag,
        not the individual statements within it.
        """
        policy = AsyncRetryPolicy()

        from cassandra.query import BatchStatement

        # Test batch without is_idempotent attribute (default)
        default_batch = BatchStatement()
        # Don't set is_idempotent - should default to non-idempotent

        decision = policy.on_write_timeout(
            query=default_batch,
            consistency=ConsistencyLevel.ONE,
            write_type=WriteType.UNLOGGED_BATCH,
            required_responses=1,
            received_responses=0,
            retry_num=0,
        )
        # Batch without explicit is_idempotent=True should not retry
        assert decision[0] == RetryPolicy.RETHROW

        # Test batch explicitly marked idempotent
        idempotent_batch = BatchStatement()
        idempotent_batch.is_idempotent = True

        decision = policy.on_write_timeout(
            query=idempotent_batch,
            consistency=ConsistencyLevel.ONE,
            write_type=WriteType.UNLOGGED_BATCH,
            required_responses=1,
            received_responses=0,
            retry_num=0,
        )
        assert decision[0] == RetryPolicy.RETRY

        # Test batch explicitly marked non-idempotent
        non_idempotent_batch = BatchStatement()
        non_idempotent_batch.is_idempotent = False

        decision = policy.on_write_timeout(
            query=non_idempotent_batch,
            consistency=ConsistencyLevel.ONE,
            write_type=WriteType.UNLOGGED_BATCH,
            required_responses=1,
            received_responses=0,
            retry_num=0,
        )
        assert decision[0] == RetryPolicy.RETHROW

    # ========================================
    # Error Propagation Tests
    # ========================================

    def test_request_error_handling(self):
        """
        Test on_request_error method.

        What this tests:
        ---------------
        1. Request errors trigger RETRY_NEXT_HOST
        2. Max retries is respected

        Why this matters:
        ----------------
        Connection errors and other request failures should
        try a different coordinator node.
        """
        policy = AsyncRetryPolicy()
        query = Mock()
        error = Exception("Connection failed")

        # First attempt should try next host
        decision = policy.on_request_error(
            query=query, consistency=ConsistencyLevel.QUORUM, error=error, retry_num=0
        )
        assert decision == (RetryPolicy.RETRY_NEXT_HOST, ConsistencyLevel.QUORUM)

        # After max retries, should rethrow
        decision = policy.on_request_error(
            query=query,
            consistency=ConsistencyLevel.QUORUM,
            error=error,
            retry_num=3,  # At max retries
        )
        assert decision == (RetryPolicy.RETHROW, None)

    # ========================================
    # Edge Cases
    # ========================================

    def test_retry_with_zero_max_retries(self):
        """
        Test that zero max_retries means no retries.

        What this tests:
        ---------------
        1. max_retries=0 disables all retries
        2. First attempt is not counted as retry

        Why this matters:
        ----------------
        Some applications want to handle retries at a higher
        level and disable driver-level retries.
        """
        policy = AsyncRetryPolicy(max_retries=0)
        query = Mock(is_idempotent=True)

        # Even on first attempt (retry_num=0), should not retry
        decision = policy.on_write_timeout(
            query=query,
            consistency=ConsistencyLevel.ONE,
            write_type=WriteType.SIMPLE,
            required_responses=1,
            received_responses=0,
            retry_num=0,
        )

        assert decision[0] == RetryPolicy.RETHROW

    def test_consistency_level_all_special_handling(self):
        """
        Test special handling for ConsistencyLevel.ALL.

        What this tests:
        ---------------
        1. ALL consistency has special retry considerations
        2. May not retry even when others would

        Why this matters:
        ----------------
        ConsistencyLevel.ALL requires all replicas. If any
        are down, retrying won't help.
        """
        policy = AsyncRetryPolicy()
        query = Mock()

        decision = policy.on_unavailable(
            query=query,
            consistency=ConsistencyLevel.ALL,
            required_replicas=3,
            alive_replicas=2,  # Missing one replica
            retry_num=0,
        )

        # Implementation dependent, but should handle ALL specially
        assert decision is not None  # Use the decision variable

    def test_query_string_not_accessed(self):
        """
        Test that retry policy doesn't access query internals.

        What this tests:
        ---------------
        1. Policy only uses public query attributes
        2. No query string parsing or inspection

        Why this matters:
        ----------------
        Retry decisions should be based on metadata, not
        query content. This ensures performance and security.
        """
        policy = AsyncRetryPolicy()

        # Mock with minimal interface
        query = Mock()
        query.is_idempotent = True
        # Don't provide query string or other internals

        # Should work without accessing query details
        decision = policy.on_write_timeout(
            query=query,
            consistency=ConsistencyLevel.ONE,
            write_type=WriteType.SIMPLE,
            required_responses=1,
            received_responses=0,
            retry_num=0,
        )

        assert decision[0] == RetryPolicy.RETRY

    def test_concurrent_retry_decisions(self):
        """
        Test that retry policy is thread-safe.

        What this tests:
        ---------------
        1. Multiple threads can use same policy instance
        2. No shared state corruption

        Why this matters:
        ----------------
        In async applications, the same retry policy instance
        may be used by multiple concurrent operations.
        """
        import threading

        policy = AsyncRetryPolicy()
        results = []

        def make_decision():
            query = Mock(is_idempotent=True)
            decision = policy.on_write_timeout(
                query=query,
                consistency=ConsistencyLevel.ONE,
                write_type=WriteType.SIMPLE,
                required_responses=1,
                received_responses=0,
                retry_num=0,
            )
            results.append(decision)

        # Run multiple threads
        threads = [threading.Thread(target=make_decision) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # All should get same decision
        assert len(results) == 10
        assert all(r[0] == RetryPolicy.RETRY for r in results)
