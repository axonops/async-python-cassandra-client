"""
Unit tests for authentication and authorization failures.

Tests how the async wrapper handles:
- Authentication failures during connection
- Authorization failures during operations
- Credential rotation scenarios
- Session invalidation due to auth changes
"""

import asyncio
from unittest.mock import Mock, patch

import pytest
from cassandra import AuthenticationFailed, Unauthorized
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import NoHostAvailable

from async_cassandra import AsyncCluster
from async_cassandra.exceptions import ConnectionError, QueryError


class TestAuthenticationFailures:
    """Test authentication failure scenarios."""

    def create_error_future(self, exception):
        """Create a mock future that raises the given exception."""
        future = Mock()
        callbacks = []
        errbacks = []

        def add_callbacks(callback=None, errback=None):
            if callback:
                callbacks.append(callback)
            if errback:
                errbacks.append(errback)
                # Call errback immediately with the error
                errback(exception)

        future.add_callbacks = add_callbacks
        future.has_more_pages = False
        future.timeout = None
        future.clear_callbacks = Mock()
        return future

    @pytest.mark.asyncio
    async def test_initial_auth_failure(self):
        """Test handling of authentication failure during initial connection."""
        with patch("async_cassandra.cluster.Cluster") as mock_cluster_class:
            # Create mock cluster instance
            mock_cluster = Mock()
            mock_cluster_class.return_value = mock_cluster

            # Configure cluster to fail authentication
            mock_cluster.connect.side_effect = NoHostAvailable(
                "Unable to connect to any servers",
                {"127.0.0.1": AuthenticationFailed("Bad credentials")},
            )

            async_cluster = AsyncCluster(
                contact_points=["127.0.0.1"],
                auth_provider=PlainTextAuthProvider("bad_user", "bad_pass"),
            )

            # Should raise connection error wrapping the auth failure
            with pytest.raises(ConnectionError) as exc_info:
                await async_cluster.connect()

            # Verify the error message contains auth failure
            assert "Failed to connect to cluster" in str(exc_info.value)

            await async_cluster.shutdown()

    @pytest.mark.asyncio
    async def test_auth_failure_during_operation(self):
        """Test handling of authentication failure during query execution."""
        with patch("async_cassandra.cluster.Cluster") as mock_cluster_class:
            # Create mock cluster and session
            mock_cluster = Mock()
            mock_cluster_class.return_value = mock_cluster
            mock_cluster.protocol_version = 5

            mock_session = Mock()
            mock_cluster.connect.return_value = mock_session

            # Create async cluster and connect
            async_cluster = AsyncCluster()
            session = await async_cluster.connect()

            # Configure query to fail with auth error
            mock_session.execute_async.return_value = self.create_error_future(
                Unauthorized("User has no SELECT permission on <table test.users>")
            )

            # Unauthorized is wrapped in QueryError
            with pytest.raises(QueryError) as exc_info:
                await session.execute("SELECT * FROM test.users")

            assert "Query execution failed: User has no SELECT permission" in str(exc_info.value)
            assert isinstance(exc_info.value.cause, Unauthorized)

            await session.close()
            await async_cluster.shutdown()

    @pytest.mark.asyncio
    async def test_credential_rotation_reconnect(self):
        """Test handling credential rotation requiring reconnection."""
        with patch("async_cassandra.cluster.Cluster") as mock_cluster_class:
            # Create mock cluster and session
            mock_cluster = Mock()
            mock_cluster_class.return_value = mock_cluster
            mock_cluster.protocol_version = 5

            mock_session = Mock()
            mock_cluster.connect.return_value = mock_session

            # Set initial auth provider
            old_auth = PlainTextAuthProvider("user1", "pass1")

            async_cluster = AsyncCluster(auth_provider=old_auth)
            session = await async_cluster.connect()

            # Simulate credential rotation
            new_auth = PlainTextAuthProvider("user1", "pass2")

            # Update auth provider on the underlying cluster
            async_cluster._cluster.auth_provider = new_auth

            # Next operation fails with auth error
            mock_session.execute_async.return_value = self.create_error_future(
                AuthenticationFailed("Password verification failed")
            )

            # AuthenticationFailed is wrapped in QueryError
            with pytest.raises(QueryError) as exc_info:
                await session.execute("SELECT * FROM test")

            assert "Password verification failed" in str(exc_info.value)
            assert isinstance(exc_info.value.cause, AuthenticationFailed)

            await session.close()
            await async_cluster.shutdown()

    @pytest.mark.asyncio
    async def test_authorization_failure_different_operations(self):
        """Test different authorization failures for various operations."""
        with patch("async_cassandra.cluster.Cluster") as mock_cluster_class:
            # Setup mock cluster and session
            mock_cluster = Mock()
            mock_cluster_class.return_value = mock_cluster
            mock_cluster.protocol_version = 5

            mock_session = Mock()
            mock_cluster.connect.return_value = mock_session

            async_cluster = AsyncCluster()
            session = await async_cluster.connect()

            # Test different permission failures
            permissions = [
                ("SELECT * FROM users", "User has no SELECT permission"),
                ("INSERT INTO users VALUES (1)", "User has no MODIFY permission"),
                ("CREATE TABLE test (id int)", "User has no CREATE permission"),
                ("DROP TABLE users", "User has no DROP permission"),
                ("ALTER TABLE users ADD col text", "User has no ALTER permission"),
            ]

            for query, error_msg in permissions:
                mock_session.execute_async.return_value = self.create_error_future(
                    Unauthorized(error_msg)
                )

                # Unauthorized is wrapped in QueryError
                with pytest.raises(QueryError) as exc_info:
                    await session.execute(query)

                assert error_msg in str(exc_info.value)
                assert isinstance(exc_info.value.cause, Unauthorized)

            await session.close()
            await async_cluster.shutdown()

    @pytest.mark.asyncio
    async def test_session_invalidation_on_auth_change(self):
        """Test session invalidation when authentication changes."""
        with patch("async_cassandra.cluster.Cluster") as mock_cluster_class:
            # Setup mock cluster and session
            mock_cluster = Mock()
            mock_cluster_class.return_value = mock_cluster
            mock_cluster.protocol_version = 5

            mock_session = Mock()
            mock_cluster.connect.return_value = mock_session

            async_cluster = AsyncCluster()
            session = await async_cluster.connect()

            # Mark session as needing re-authentication
            mock_session._auth_invalid = True

            # Operations should detect invalid auth state
            mock_session.execute_async.return_value = self.create_error_future(
                AuthenticationFailed("Session expired")
            )

            # AuthenticationFailed is wrapped in QueryError
            with pytest.raises(QueryError) as exc_info:
                await session.execute("SELECT * FROM test")

            assert "Session expired" in str(exc_info.value)
            assert isinstance(exc_info.value.cause, AuthenticationFailed)

            await session.close()
            await async_cluster.shutdown()

    @pytest.mark.asyncio
    async def test_concurrent_auth_failures(self):
        """Test handling of concurrent authentication failures."""
        with patch("async_cassandra.cluster.Cluster") as mock_cluster_class:
            # Setup mock cluster and session
            mock_cluster = Mock()
            mock_cluster_class.return_value = mock_cluster
            mock_cluster.protocol_version = 5

            mock_session = Mock()
            mock_cluster.connect.return_value = mock_session

            async_cluster = AsyncCluster()
            session = await async_cluster.connect()

            # All queries fail with auth error
            mock_session.execute_async.return_value = self.create_error_future(
                Unauthorized("No permission")
            )

            # Execute multiple concurrent queries
            tasks = [session.execute(f"SELECT * FROM table{i}") for i in range(5)]

            # All should fail with QueryError wrapping Unauthorized
            results = await asyncio.gather(*tasks, return_exceptions=True)
            assert all(isinstance(r, QueryError) for r in results)
            assert all(isinstance(r.cause, Unauthorized) for r in results)

            await session.close()
            await async_cluster.shutdown()

    @pytest.mark.asyncio
    async def test_auth_error_in_prepared_statement(self):
        """Test authorization failure with prepared statements."""
        with patch("async_cassandra.cluster.Cluster") as mock_cluster_class:
            # Setup mock cluster and session
            mock_cluster = Mock()
            mock_cluster_class.return_value = mock_cluster
            mock_cluster.protocol_version = 5

            mock_session = Mock()
            mock_cluster.connect.return_value = mock_session

            async_cluster = AsyncCluster()
            session = await async_cluster.connect()

            # Prepare succeeds
            prepared = Mock()
            prepared.query = "INSERT INTO users (id, name) VALUES (?, ?)"
            prepare_future = Mock()
            prepare_future.result = Mock(return_value=prepared)
            prepare_future.add_callbacks = Mock()
            prepare_future.has_more_pages = False
            prepare_future.timeout = None
            prepare_future.clear_callbacks = Mock()
            mock_session.prepare_async.return_value = prepare_future

            stmt = await session.prepare("INSERT INTO users (id, name) VALUES (?, ?)")

            # But execution fails with auth error
            mock_session.execute_async.return_value = self.create_error_future(
                Unauthorized("User has no MODIFY permission on <table test.users>")
            )

            # Unauthorized is wrapped in QueryError
            with pytest.raises(QueryError) as exc_info:
                await session.execute(stmt, [1, "test"])

            assert "no MODIFY permission" in str(exc_info.value)
            assert isinstance(exc_info.value.cause, Unauthorized)

            await session.close()
            await async_cluster.shutdown()

    @pytest.mark.asyncio
    async def test_keyspace_auth_failure(self):
        """Test authorization failure when switching keyspaces."""
        with patch("async_cassandra.cluster.Cluster") as mock_cluster_class:
            # Create mock cluster
            mock_cluster = Mock()
            mock_cluster_class.return_value = mock_cluster

            # Try to connect to specific keyspace with no access
            mock_cluster.connect.side_effect = NoHostAvailable(
                "Unable to connect to any servers",
                {
                    "127.0.0.1": Unauthorized(
                        "User has no ACCESS permission on <keyspace restricted_ks>"
                    )
                },
            )

            async_cluster = AsyncCluster()

            # Should fail with connection error
            with pytest.raises(ConnectionError) as exc_info:
                await async_cluster.connect("restricted_ks")

            assert "Failed to connect" in str(exc_info.value)

            await async_cluster.shutdown()

    @pytest.mark.asyncio
    async def test_auth_provider_callback_handling(self):
        """Test custom auth provider with async callbacks."""
        with patch("async_cassandra.cluster.Cluster") as mock_cluster_class:
            # Create mock cluster
            mock_cluster = Mock()
            mock_cluster_class.return_value = mock_cluster
            mock_cluster.protocol_version = 5

            # Create custom auth provider
            class AsyncAuthProvider:
                def __init__(self):
                    self.call_count = 0

                async def get_credentials(self):
                    self.call_count += 1
                    # Simulate async credential fetching
                    await asyncio.sleep(0.01)
                    return {"username": "user", "password": "pass"}

            auth_provider = AsyncAuthProvider()

            # AsyncCluster constructor accepts auth_provider
            async_cluster = AsyncCluster(auth_provider=auth_provider)

            # The driver handles auth internally, we just pass the provider
            await async_cluster.shutdown()

    @pytest.mark.asyncio
    async def test_auth_provider_refresh(self):
        """Test auth provider that refreshes credentials."""
        with patch("async_cassandra.cluster.Cluster") as mock_cluster_class:
            # Create mock cluster
            mock_cluster = Mock()
            mock_cluster_class.return_value = mock_cluster

            class RefreshableAuthProvider:
                def __init__(self):
                    self.refresh_count = 0
                    self.credentials = {"username": "user", "password": "initial"}

                async def refresh_credentials(self):
                    self.refresh_count += 1
                    self.credentials["password"] = f"refreshed_{self.refresh_count}"
                    return self.credentials

            auth_provider = RefreshableAuthProvider()

            async_cluster = AsyncCluster(auth_provider=auth_provider)

            # Note: The actual credential refresh would be handled by the driver
            # We're just testing that our wrapper can accept such providers

            await async_cluster.shutdown()
