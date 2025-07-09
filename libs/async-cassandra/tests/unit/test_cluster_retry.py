"""
Unit tests for cluster connection retry logic.
"""

import asyncio
from unittest.mock import Mock, patch

import pytest
from cassandra.cluster import NoHostAvailable

from async_cassandra.cluster import AsyncCluster
from async_cassandra.exceptions import ConnectionError


@pytest.mark.asyncio
class TestClusterConnectionRetry:
    """Test cluster connection retry behavior."""

    async def test_connection_retries_on_failure(self):
        """
        Test that connection attempts are retried on failure.

        What this tests:
        ---------------
        1. Failed connections retry
        2. Third attempt succeeds
        3. Total of 3 attempts
        4. Eventually returns session

        Why this matters:
        ----------------
        Connection failures are common:
        - Network hiccups
        - Node startup delays
        - Temporary unavailability

        Automatic retry improves
        reliability significantly.
        """
        mock_cluster = Mock()
        # Mock protocol version to pass validation
        mock_cluster.protocol_version = 5

        # Create a mock that fails twice then succeeds
        connect_attempts = 0
        mock_session = Mock()

        async def create_side_effect(cluster, keyspace):
            nonlocal connect_attempts
            connect_attempts += 1
            if connect_attempts < 3:
                raise NoHostAvailable("Unable to connect to any servers", {})
            return mock_session  # Return a mock session on third attempt

        with patch("async_cassandra.cluster.Cluster", return_value=mock_cluster):
            with patch(
                "async_cassandra.cluster.AsyncCassandraSession.create",
                side_effect=create_side_effect,
            ):
                cluster = AsyncCluster(["localhost"])

                # Should succeed after retries
                session = await cluster.connect()
                assert session is not None
                assert connect_attempts == 3

    async def test_connection_fails_after_max_retries(self):
        """
        Test that connection fails after maximum retry attempts.

        What this tests:
        ---------------
        1. Max retry limit enforced
        2. Exactly 3 attempts made
        3. ConnectionError raised
        4. Clear failure message

        Why this matters:
        ----------------
        Must give up eventually:
        - Prevent infinite loops
        - Fail with clear error
        - Allow app to handle

        Bounded retries prevent
        hanging applications.
        """
        mock_cluster = Mock()
        # Mock protocol version to pass validation
        mock_cluster.protocol_version = 5

        create_call_count = 0

        async def create_side_effect(cluster, keyspace):
            nonlocal create_call_count
            create_call_count += 1
            raise NoHostAvailable("Unable to connect to any servers", {})

        with patch("async_cassandra.cluster.Cluster", return_value=mock_cluster):
            with patch(
                "async_cassandra.cluster.AsyncCassandraSession.create",
                side_effect=create_side_effect,
            ):
                cluster = AsyncCluster(["localhost"])

                # Should fail after max retries (3)
                with pytest.raises(ConnectionError) as exc_info:
                    await cluster.connect()

                assert "Failed to connect to cluster after 3 attempts" in str(exc_info.value)
                assert create_call_count == 3

    async def test_connection_retry_with_increasing_delay(self):
        """
        Test that retry delays increase with each attempt.

        What this tests:
        ---------------
        1. Delays between retries
        2. Exponential backoff
        3. NoHostAvailable gets longer delays
        4. Prevents thundering herd

        Why this matters:
        ----------------
        Exponential backoff:
        - Reduces server load
        - Allows recovery time
        - Prevents retry storms

        Smart retry timing improves
        overall system stability.
        """
        mock_cluster = Mock()
        # Mock protocol version to pass validation
        mock_cluster.protocol_version = 5

        # Fail all attempts
        async def create_side_effect(cluster, keyspace):
            raise NoHostAvailable("Unable to connect to any servers", {})

        sleep_delays = []

        async def mock_sleep(delay):
            sleep_delays.append(delay)

        with patch("async_cassandra.cluster.Cluster", return_value=mock_cluster):
            with patch(
                "async_cassandra.cluster.AsyncCassandraSession.create",
                side_effect=create_side_effect,
            ):
                with patch("asyncio.sleep", side_effect=mock_sleep):
                    cluster = AsyncCluster(["localhost"])

                    with pytest.raises(ConnectionError):
                        await cluster.connect()

                    # Should have 2 sleep calls (between 3 attempts)
                    assert len(sleep_delays) == 2
                    # First delay should be 2.0 seconds (NoHostAvailable gets longer delay)
                    assert sleep_delays[0] == 2.0
                    # Second delay should be 4.0 seconds
                    assert sleep_delays[1] == 4.0

    async def test_timeout_error_not_retried(self):
        """
        Test that asyncio.TimeoutError is not retried.

        What this tests:
        ---------------
        1. Timeouts fail immediately
        2. No retry for timeouts
        3. TimeoutError propagated
        4. Fast failure mode

        Why this matters:
        ----------------
        Timeouts indicate:
        - User-specified limit hit
        - Operation too slow
        - Should fail fast

        Retrying timeouts would
        violate user expectations.
        """
        mock_cluster = Mock()

        # Create session that takes too long
        async def slow_connect(keyspace=None):
            await asyncio.sleep(20)  # Longer than timeout
            return Mock()

        mock_cluster.connect = Mock(side_effect=lambda k=None: Mock())

        with patch("async_cassandra.cluster.Cluster", return_value=mock_cluster):
            with patch(
                "async_cassandra.session.AsyncCassandraSession.create",
                side_effect=asyncio.TimeoutError(),
            ):
                cluster = AsyncCluster(["localhost"])

                # Should raise TimeoutError without retrying
                with pytest.raises(asyncio.TimeoutError):
                    await cluster.connect(timeout=0.1)

                # Should not have retried (create was called only once)

    async def test_other_exceptions_use_shorter_delay(self):
        """
        Test that non-NoHostAvailable exceptions use shorter retry delay.

        What this tests:
        ---------------
        1. Different delays by error type
        2. Generic errors get short delay
        3. NoHostAvailable gets long delay
        4. Smart backoff strategy

        Why this matters:
        ----------------
        Error-specific delays:
        - Network errors need more time
        - Generic errors retry quickly
        - Optimizes recovery time

        Adaptive retry delays improve
        connection success rates.
        """
        mock_cluster = Mock()
        # Mock protocol version to pass validation
        mock_cluster.protocol_version = 5

        # Fail with generic exception
        async def create_side_effect(cluster, keyspace):
            raise Exception("Generic error")

        sleep_delays = []

        async def mock_sleep(delay):
            sleep_delays.append(delay)

        with patch("async_cassandra.cluster.Cluster", return_value=mock_cluster):
            with patch(
                "async_cassandra.cluster.AsyncCassandraSession.create",
                side_effect=create_side_effect,
            ):
                with patch("asyncio.sleep", side_effect=mock_sleep):
                    cluster = AsyncCluster(["localhost"])

                    with pytest.raises(ConnectionError):
                        await cluster.connect()

                    # Should have 2 sleep calls
                    assert len(sleep_delays) == 2
                    # First delay should be 0.5 seconds (generic exception)
                    assert sleep_delays[0] == 0.5
                    # Second delay should be 1.0 seconds
                    assert sleep_delays[1] == 1.0
