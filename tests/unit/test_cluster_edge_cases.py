"""
Unit tests for cluster edge cases and failure scenarios.

Tests how the async wrapper handles various cluster-level failures and edge cases
within its existing functionality.
"""

import asyncio
import time
from unittest.mock import Mock, patch

import pytest
from cassandra.cluster import NoHostAvailable

from async_cassandra import AsyncCluster
from async_cassandra.exceptions import ConnectionError


class TestClusterEdgeCases:
    """Test cluster edge cases and failure scenarios."""

    def _create_mock_cluster(self):
        """Create a properly configured mock cluster."""
        mock_cluster = Mock()
        mock_cluster.protocol_version = 5
        mock_cluster.shutdown = Mock()
        return mock_cluster

    @pytest.mark.asyncio
    async def test_protocol_version_validation(self):
        """
        Test that protocol versions below v5 are rejected.

        What this tests:
        ---------------
        1. Protocol v4 and below rejected
        2. ConfigurationError at creation
        3. v5+ versions accepted
        4. Clear error messages

        Why this matters:
        ----------------
        async-cassandra requires v5+ for:
        - Required async features
        - Better performance
        - Modern functionality

        Failing early prevents confusing
        runtime errors.
        """
        from async_cassandra.exceptions import ConfigurationError

        # Should reject v4 and below
        with pytest.raises(ConfigurationError) as exc_info:
            AsyncCluster(protocol_version=4)

        assert "Protocol version 4 is not supported" in str(exc_info.value)
        assert "requires CQL protocol v5 or higher" in str(exc_info.value)

        # Should accept v5 and above
        with patch("async_cassandra.cluster.Cluster") as mock_cluster_class:
            mock_cluster = self._create_mock_cluster()
            mock_cluster_class.return_value = mock_cluster

            # v5 should work
            cluster5 = AsyncCluster(protocol_version=5)
            assert cluster5._cluster == mock_cluster

            # v6 should work
            cluster6 = AsyncCluster(protocol_version=6)
            assert cluster6._cluster == mock_cluster

    @pytest.mark.asyncio
    async def test_connection_retry_with_protocol_error(self):
        """
        Test that protocol version errors are not retried.

        What this tests:
        ---------------
        1. Protocol errors fail fast
        2. No retry for version mismatch
        3. Clear error message
        4. Single attempt only

        Why this matters:
        ----------------
        Protocol errors aren't transient:
        - Server won't change version
        - Retrying wastes time
        - User needs to upgrade

        Fast failure enables quick
        diagnosis and resolution.
        """
        with patch("async_cassandra.cluster.Cluster") as mock_cluster_class:
            mock_cluster = self._create_mock_cluster()
            mock_cluster_class.return_value = mock_cluster

            # Count connection attempts
            connect_count = 0

            def connect_side_effect(*args, **kwargs):
                nonlocal connect_count
                connect_count += 1
                # Create NoHostAvailable with protocol error details
                error = NoHostAvailable(
                    "Unable to connect to any servers",
                    {"127.0.0.1": Exception("ProtocolError: Cannot negotiate protocol version")},
                )
                raise error

            # Mock sync connect to fail with protocol error
            mock_cluster.connect.side_effect = connect_side_effect

            async_cluster = AsyncCluster()

            # Should fail immediately without retrying
            with pytest.raises(ConnectionError) as exc_info:
                await async_cluster.connect()

            # Should only try once (no retries for protocol errors)
            assert connect_count == 1
            assert "doesn't support protocol v5" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_connection_retry_with_reset_errors(self):
        """
        Test connection retry with connection reset errors.

        What this tests:
        ---------------
        1. Connection resets trigger retry
        2. Exponential backoff applied
        3. Eventually succeeds
        4. Retry timing increases

        Why this matters:
        ----------------
        Connection resets are transient:
        - Network hiccups
        - Server restarts
        - Load balancer changes

        Automatic retry with backoff
        handles temporary issues gracefully.
        """
        with patch("async_cassandra.cluster.Cluster") as mock_cluster_class:
            mock_cluster = self._create_mock_cluster()
            mock_cluster.protocol_version = 5  # Set a valid protocol version
            mock_cluster_class.return_value = mock_cluster

            # Track timing of retries
            call_times = []

            def connect_side_effect(*args, **kwargs):
                call_times.append(time.time())

                # Fail first 2 attempts with connection reset
                if len(call_times) <= 2:
                    error = NoHostAvailable(
                        "Unable to connect to any servers",
                        {"127.0.0.1": Exception("Connection reset by peer")},
                    )
                    raise error
                else:
                    # Third attempt succeeds
                    mock_session = Mock()
                    return mock_session

            mock_cluster.connect.side_effect = connect_side_effect

            async_cluster = AsyncCluster()

            # Should eventually succeed after retries
            session = await async_cluster.connect()
            assert session is not None

            # Should have retried 3 times total
            assert len(call_times) == 3

            # Check retry delays increased (connection reset uses longer delays)
            if len(call_times) > 2:
                delay1 = call_times[1] - call_times[0]
                delay2 = call_times[2] - call_times[1]
                # Second delay should be longer than first
                assert delay2 > delay1

    @pytest.mark.asyncio
    async def test_concurrent_connect_attempts(self):
        """
        Test handling of concurrent connection attempts.

        What this tests:
        ---------------
        1. Concurrent connects allowed
        2. Each gets separate session
        3. No connection reuse
        4. Thread-safe operation

        Why this matters:
        ----------------
        Real apps may connect concurrently:
        - Multiple workers starting
        - Parallel initialization
        - No singleton pattern

        Must handle concurrent connects
        without deadlock or corruption.
        """
        with patch("async_cassandra.cluster.Cluster") as mock_cluster_class:
            mock_cluster = self._create_mock_cluster()
            mock_cluster_class.return_value = mock_cluster

            # Make connect slow to ensure concurrency
            connect_count = 0
            sessions_created = []

            def slow_connect(*args, **kwargs):
                nonlocal connect_count
                connect_count += 1
                # This is called from an executor, so we can use time.sleep
                time.sleep(0.1)
                session = Mock()
                session.id = connect_count
                sessions_created.append(session)
                return session

            mock_cluster.connect = Mock(side_effect=slow_connect)

            async_cluster = AsyncCluster()

            # Try to connect concurrently
            tasks = [async_cluster.connect(), async_cluster.connect(), async_cluster.connect()]

            results = await asyncio.gather(*tasks)

            # All should return sessions
            assert all(r is not None for r in results)

            # Should have called connect multiple times
            # (no connection caching in current implementation)
            assert mock_cluster.connect.call_count == 3

    @pytest.mark.asyncio
    async def test_cluster_shutdown_timeout(self):
        """
        Test cluster shutdown with timeout.

        What this tests:
        ---------------
        1. Shutdown can timeout
        2. TimeoutError raised
        3. Hanging shutdown detected
        4. Async timeout works

        Why this matters:
        ----------------
        Shutdown can hang due to:
        - Network issues
        - Deadlocked threads
        - Resource cleanup bugs

        Timeout prevents app hanging
        during shutdown.
        """
        with patch("async_cassandra.cluster.Cluster") as mock_cluster_class:
            mock_cluster = self._create_mock_cluster()
            mock_cluster_class.return_value = mock_cluster

            # Make shutdown hang
            import threading

            def hanging_shutdown():
                # Use threading.Event to wait without consuming CPU
                event = threading.Event()
                event.wait(2)  # Short wait, will be interrupted by the test timeout

            mock_cluster.shutdown.side_effect = hanging_shutdown

            async_cluster = AsyncCluster()

            # Should timeout during shutdown
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(async_cluster.shutdown(), timeout=1.0)

    @pytest.mark.asyncio
    async def test_cluster_double_shutdown(self):
        """
        Test that cluster shutdown is idempotent.

        What this tests:
        ---------------
        1. Multiple shutdowns safe
        2. Only shuts down once
        3. is_closed flag works
        4. close() also idempotent

        Why this matters:
        ----------------
        Idempotent shutdown critical for:
        - Error handling paths
        - Cleanup in finally blocks
        - Multiple shutdown sources

        Prevents errors during cleanup
        and resource leaks.
        """
        with patch("async_cassandra.cluster.Cluster") as mock_cluster_class:
            mock_cluster = self._create_mock_cluster()
            mock_cluster_class.return_value = mock_cluster
            mock_cluster.shutdown = Mock()

            async_cluster = AsyncCluster()

            # First shutdown
            await async_cluster.shutdown()
            assert mock_cluster.shutdown.call_count == 1
            assert async_cluster.is_closed

            # Second shutdown should be safe
            await async_cluster.shutdown()
            # Should still only be called once
            assert mock_cluster.shutdown.call_count == 1
            assert async_cluster.is_closed

            # Third shutdown via close()
            await async_cluster.close()
            assert mock_cluster.shutdown.call_count == 1

    @pytest.mark.asyncio
    async def test_cluster_metadata_access(self):
        """
        Test accessing cluster metadata.

        What this tests:
        ---------------
        1. Metadata accessible
        2. Keyspace info available
        3. Direct passthrough
        4. No async wrapper needed

        Why this matters:
        ----------------
        Metadata access enables:
        - Schema discovery
        - Dynamic queries
        - ORM functionality

        Must work seamlessly through
        async wrapper.
        """
        with patch("async_cassandra.cluster.Cluster") as mock_cluster_class:
            mock_cluster = self._create_mock_cluster()
            mock_metadata = Mock()
            mock_metadata.keyspaces = {"system": Mock()}
            mock_cluster.metadata = mock_metadata
            mock_cluster_class.return_value = mock_cluster

            async_cluster = AsyncCluster()

            # Should provide access to metadata
            metadata = async_cluster.metadata
            assert metadata == mock_metadata
            assert "system" in metadata.keyspaces

    @pytest.mark.asyncio
    async def test_register_user_type(self):
        """
        Test user type registration.

        What this tests:
        ---------------
        1. UDT registration works
        2. Delegates to driver
        3. Parameters passed through
        4. Type mapping enabled

        Why this matters:
        ----------------
        User-defined types (UDTs):
        - Complex data modeling
        - Type-safe operations
        - ORM integration

        Registration must work for
        proper UDT handling.
        """
        with patch("async_cassandra.cluster.Cluster") as mock_cluster_class:
            mock_cluster = self._create_mock_cluster()
            mock_cluster.register_user_type = Mock()
            mock_cluster_class.return_value = mock_cluster

            async_cluster = AsyncCluster()

            # Register a user type
            class UserAddress:
                pass

            async_cluster.register_user_type("my_keyspace", "address", UserAddress)

            # Should delegate to underlying cluster
            mock_cluster.register_user_type.assert_called_once_with(
                "my_keyspace", "address", UserAddress
            )

    @pytest.mark.asyncio
    async def test_connection_with_auth_failure(self):
        """
        Test connection with authentication failure.

        What this tests:
        ---------------
        1. Auth failures retried
        2. Multiple attempts made
        3. Eventually fails
        4. Clear error message

        Why this matters:
        ----------------
        Auth failures might be transient:
        - Token expiration timing
        - Auth service hiccup
        - Race conditions

        Limited retry gives auth
        issues chance to resolve.
        """
        with patch("async_cassandra.cluster.Cluster") as mock_cluster_class:
            mock_cluster = self._create_mock_cluster()
            mock_cluster_class.return_value = mock_cluster

            from cassandra import AuthenticationFailed

            # Mock auth failure
            auth_error = NoHostAvailable(
                "Unable to connect to any servers",
                {"127.0.0.1": AuthenticationFailed("Bad credentials")},
            )
            mock_cluster.connect.side_effect = auth_error

            async_cluster = AsyncCluster()

            # Should fail after retries
            with pytest.raises(ConnectionError) as exc_info:
                await async_cluster.connect()

            # Should have retried (auth errors are retried in case of transient issues)
            assert mock_cluster.connect.call_count == 3
            assert "Failed to connect to cluster after 3 attempts" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_connection_with_mixed_errors(self):
        """
        Test connection with different errors on different attempts.

        What this tests:
        ---------------
        1. Different errors per attempt
        2. All attempts exhausted
        3. Last error reported
        4. Varied error handling

        Why this matters:
        ----------------
        Real failures are messy:
        - Different nodes fail differently
        - Errors change over time
        - Mixed failure modes

        Must handle varied errors
        during connection attempts.
        """
        with patch("async_cassandra.cluster.Cluster") as mock_cluster_class:
            mock_cluster = self._create_mock_cluster()
            mock_cluster_class.return_value = mock_cluster

            # Different error each attempt
            errors = [
                NoHostAvailable(
                    "Unable to connect", {"127.0.0.1": Exception("Connection refused")}
                ),
                NoHostAvailable(
                    "Unable to connect", {"127.0.0.1": Exception("Connection reset by peer")}
                ),
                Exception("Unexpected error"),
            ]

            attempt = 0

            def connect_side_effect(*args, **kwargs):
                nonlocal attempt
                error = errors[attempt]
                attempt += 1
                raise error

            mock_cluster.connect.side_effect = connect_side_effect

            async_cluster = AsyncCluster()

            # Should fail after all retries
            with pytest.raises(ConnectionError) as exc_info:
                await async_cluster.connect()

            # Should have tried all attempts
            assert mock_cluster.connect.call_count == 3
            assert "Unexpected error" in str(exc_info.value)  # Last error

    @pytest.mark.asyncio
    async def test_create_with_auth_convenience_method(self):
        """
        Test create_with_auth convenience method.

        What this tests:
        ---------------
        1. Auth provider created
        2. Credentials passed correctly
        3. Other params preserved
        4. Convenience method works

        Why this matters:
        ----------------
        Simple auth setup critical:
        - Common use case
        - Easy to get wrong
        - Security sensitive

        Convenience method reduces
        auth configuration errors.
        """
        with patch("async_cassandra.cluster.Cluster") as mock_cluster_class:
            mock_cluster = self._create_mock_cluster()
            mock_cluster_class.return_value = mock_cluster

            # Create with auth
            AsyncCluster.create_with_auth(
                contact_points=["10.0.0.1"], username="cassandra", password="cassandra", port=9043
            )

            # Verify auth provider was created
            call_kwargs = mock_cluster_class.call_args[1]
            assert "auth_provider" in call_kwargs
            auth_provider = call_kwargs["auth_provider"]
            assert auth_provider is not None
            # Verify other params
            assert call_kwargs["contact_points"] == ["10.0.0.1"]
            assert call_kwargs["port"] == 9043
