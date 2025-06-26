"""
Unit tests for async retry policies.
"""

from unittest.mock import Mock

import pytest
from cassandra.policies import RetryPolicy, WriteType
from cassandra.query import ConsistencyLevel

from async_cassandra.retry_policy import AsyncRetryPolicy


class TestAsyncRetryPolicy:
    """Test cases for AsyncRetryPolicy."""

    @pytest.fixture
    def policy(self):
        """Create a retry policy instance."""
        return AsyncRetryPolicy(max_retries=3)

    @pytest.fixture
    def mock_query(self):
        """Create a mock query."""
        return Mock()

    def test_init(self):
        """Test initialization with custom max retries."""
        policy = AsyncRetryPolicy(max_retries=5)
        assert policy.max_retries == 5

    def test_on_read_timeout_retry_with_data(self, policy, mock_query):
        """Test read timeout retry when data was retrieved."""
        decision, consistency = policy.on_read_timeout(
            query=mock_query,
            consistency=ConsistencyLevel.QUORUM,
            required_responses=2,
            received_responses=1,
            data_retrieved=True,
            retry_num=0,
        )

        assert decision == RetryPolicy.RETRY
        assert consistency == ConsistencyLevel.QUORUM

    def test_on_read_timeout_retry_enough_responses(self, policy, mock_query):
        """Test read timeout retry when enough responses received."""
        decision, consistency = policy.on_read_timeout(
            query=mock_query,
            consistency=ConsistencyLevel.QUORUM,
            required_responses=2,
            received_responses=2,
            data_retrieved=False,
            retry_num=0,
        )

        assert decision == RetryPolicy.RETRY
        assert consistency == ConsistencyLevel.QUORUM

    def test_on_read_timeout_rethrow_no_data(self, policy, mock_query):
        """Test read timeout rethrow when no data and insufficient responses."""
        decision, consistency = policy.on_read_timeout(
            query=mock_query,
            consistency=ConsistencyLevel.QUORUM,
            required_responses=3,
            received_responses=1,
            data_retrieved=False,
            retry_num=0,
        )

        assert decision == RetryPolicy.RETHROW
        assert consistency is None

    def test_on_read_timeout_max_retries(self, policy, mock_query):
        """Test read timeout when max retries exceeded."""
        decision, consistency = policy.on_read_timeout(
            query=mock_query,
            consistency=ConsistencyLevel.QUORUM,
            required_responses=2,
            received_responses=1,
            data_retrieved=True,
            retry_num=3,  # Max retries is 3
        )

        assert decision == RetryPolicy.RETHROW
        assert consistency is None

    def test_on_write_timeout_retry_simple(self, policy, mock_query):
        """Test write timeout retry for simple write."""
        # Mark query as idempotent for write retry
        mock_query.is_idempotent = True

        decision, consistency = policy.on_write_timeout(
            query=mock_query,
            consistency=ConsistencyLevel.QUORUM,
            write_type=WriteType.SIMPLE,
            required_responses=2,
            received_responses=1,
            retry_num=0,
        )

        assert decision == RetryPolicy.RETRY
        assert consistency == ConsistencyLevel.QUORUM

    def test_on_write_timeout_retry_batch(self, policy, mock_query):
        """Test write timeout retry for batch write."""
        # Mark query as idempotent for write retry
        mock_query.is_idempotent = True

        decision, consistency = policy.on_write_timeout(
            query=mock_query,
            consistency=ConsistencyLevel.QUORUM,
            write_type=WriteType.BATCH,
            required_responses=2,
            received_responses=1,
            retry_num=0,
        )

        assert decision == RetryPolicy.RETRY
        assert consistency == ConsistencyLevel.QUORUM

    def test_on_write_timeout_rethrow_other_type(self, policy, mock_query):
        """Test write timeout rethrow for non-simple/batch writes."""
        decision, consistency = policy.on_write_timeout(
            query=mock_query,
            consistency=ConsistencyLevel.QUORUM,
            write_type=WriteType.COUNTER,
            required_responses=2,
            received_responses=1,
            retry_num=0,
        )

        assert decision == RetryPolicy.RETHROW
        assert consistency is None

    def test_on_write_timeout_max_retries(self, policy, mock_query):
        """Test write timeout when max retries exceeded."""
        decision, consistency = policy.on_write_timeout(
            query=mock_query,
            consistency=ConsistencyLevel.QUORUM,
            write_type=WriteType.SIMPLE,
            required_responses=2,
            received_responses=1,
            retry_num=3,
        )

        assert decision == RetryPolicy.RETHROW
        assert consistency is None

    def test_on_unavailable_first_retry(self, policy, mock_query):
        """Test unavailable exception on first retry."""
        decision, consistency = policy.on_unavailable(
            query=mock_query,
            consistency=ConsistencyLevel.QUORUM,
            required_replicas=3,
            alive_replicas=2,
            retry_num=0,
        )

        assert decision == RetryPolicy.RETRY_NEXT_HOST
        assert consistency == ConsistencyLevel.QUORUM

    def test_on_unavailable_subsequent_retry(self, policy, mock_query):
        """Test unavailable exception on subsequent retry."""
        decision, consistency = policy.on_unavailable(
            query=mock_query,
            consistency=ConsistencyLevel.QUORUM,
            required_replicas=3,
            alive_replicas=2,
            retry_num=1,
        )

        assert decision == RetryPolicy.RETRY
        assert consistency == ConsistencyLevel.QUORUM

    def test_on_unavailable_max_retries(self, policy, mock_query):
        """Test unavailable exception when max retries exceeded."""
        decision, consistency = policy.on_unavailable(
            query=mock_query,
            consistency=ConsistencyLevel.QUORUM,
            required_replicas=3,
            alive_replicas=2,
            retry_num=3,
        )

        assert decision == RetryPolicy.RETHROW
        assert consistency is None

    def test_on_request_error_retry(self, policy, mock_query):
        """Test request error retry."""
        error = Exception("Connection error")

        decision, consistency = policy.on_request_error(
            query=mock_query, consistency=ConsistencyLevel.QUORUM, error=error, retry_num=0
        )

        assert decision == RetryPolicy.RETRY_NEXT_HOST
        assert consistency == ConsistencyLevel.QUORUM

    def test_on_request_error_max_retries(self, policy, mock_query):
        """Test request error when max retries exceeded."""
        error = Exception("Connection error")

        decision, consistency = policy.on_request_error(
            query=mock_query, consistency=ConsistencyLevel.QUORUM, error=error, retry_num=3
        )

        assert decision == RetryPolicy.RETHROW
        assert consistency is None
