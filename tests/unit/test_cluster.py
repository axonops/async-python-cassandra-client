"""
Unit tests for async cluster management.

This module tests AsyncCluster in detail, covering:
- Initialization with various configurations
- Connection establishment and error handling
- Protocol version validation (v5+ requirement)
- SSL/TLS support
- Resource cleanup and context managers
- Metadata access and user type registration

Key Testing Focus:
==================
1. Protocol Version Enforcement - We require v5+ for async operations
2. Connection Error Handling - Clear error messages for common issues
3. Thread Safety - Proper locking for shutdown operations
4. Resource Management - No leaks even with errors
"""

from ssl import PROTOCOL_TLS_CLIENT, SSLContext
from unittest.mock import Mock, patch

import pytest
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.policies import ExponentialReconnectionPolicy, TokenAwarePolicy

from async_cassandra.cluster import AsyncCluster
from async_cassandra.exceptions import ConfigurationError, ConnectionError
from async_cassandra.retry_policy import AsyncRetryPolicy
from async_cassandra.session import AsyncCassandraSession


class TestAsyncCluster:
    """
    Test cases for AsyncCluster.

    AsyncCluster is responsible for:
    - Managing connection to Cassandra nodes
    - Enforcing protocol version requirements
    - Providing session creation
    - Handling authentication and SSL
    """

    @pytest.fixture
    def mock_cluster(self):
        """
        Create a mock Cassandra cluster.

        This fixture patches the driver's Cluster class to avoid
        actual network connections during unit tests. The mock
        provides the minimal interface needed for our tests.
        """
        with patch("async_cassandra.cluster.Cluster") as mock_cluster_class:
            mock_instance = Mock(spec=Cluster)
            mock_instance.shutdown = Mock()
            mock_instance.metadata = {"test": "metadata"}
            mock_cluster_class.return_value = mock_instance
            yield mock_instance

    def test_init_with_defaults(self, mock_cluster):
        """
        Test initialization with default values.

        What this tests:
        ---------------
        1. AsyncCluster can be created without parameters
        2. Default contact point is localhost (127.0.0.1)
        3. Default port is 9042 (Cassandra standard)
        4. Default policies are applied:
           - TokenAwarePolicy for load balancing (data locality)
           - ExponentialReconnectionPolicy (gradual backoff)
           - AsyncRetryPolicy (our custom retry logic)

        Why this matters:
        ----------------
        Defaults should work for local development and common setups.
        The default policies provide good production behavior:
        - Token awareness reduces latency
        - Exponential backoff prevents connection storms
        - Async retry policy handles transient failures
        """
        async_cluster = AsyncCluster()

        # Verify cluster starts in open state
        assert not async_cluster.is_closed

        # Verify driver cluster was created with expected defaults
        from async_cassandra.cluster import Cluster as ClusterImport

        ClusterImport.assert_called_once()
        call_args = ClusterImport.call_args

        # Check connection defaults
        assert call_args.kwargs["contact_points"] == ["127.0.0.1"]
        assert call_args.kwargs["port"] == 9042

        # Check policy defaults
        assert isinstance(call_args.kwargs["load_balancing_policy"], TokenAwarePolicy)
        assert isinstance(call_args.kwargs["reconnection_policy"], ExponentialReconnectionPolicy)
        assert isinstance(call_args.kwargs["default_retry_policy"], AsyncRetryPolicy)

    def test_init_with_custom_values(self, mock_cluster):
        """
        Test initialization with custom values.

        What this tests:
        ---------------
        1. All custom parameters are passed to the driver
        2. Multiple contact points can be specified
        3. Authentication is configurable
        4. Thread pool size can be tuned
        5. Protocol version can be explicitly set

        Why this matters:
        ----------------
        Production deployments need:
        - Multiple nodes for high availability
        - Custom ports for security/routing
        - Authentication for access control
        - Thread tuning for workload optimization
        - Protocol version control for compatibility
        """
        contact_points = ["192.168.1.1", "192.168.1.2"]
        port = 9043
        auth_provider = PlainTextAuthProvider("user", "pass")

        AsyncCluster(
            contact_points=contact_points,
            port=port,
            auth_provider=auth_provider,
            executor_threads=4,  # Smaller pool for testing
            protocol_version=5,  # Explicit v5
        )

        from async_cassandra.cluster import Cluster as ClusterImport

        call_args = ClusterImport.call_args

        # Verify all custom values were passed through
        assert call_args.kwargs["contact_points"] == contact_points
        assert call_args.kwargs["port"] == port
        assert call_args.kwargs["auth_provider"] == auth_provider
        assert call_args.kwargs["executor_threads"] == 4
        assert call_args.kwargs["protocol_version"] == 5

    def test_create_with_auth(self, mock_cluster):
        """
        Test creating cluster with authentication.

        What this tests:
        ---------------
        1. create_with_auth() helper method works
        2. PlainTextAuthProvider is created automatically
        3. Username/password are properly configured

        Why this matters:
        ----------------
        This is a convenience method for the common case of
        username/password authentication. It saves users from:
        - Importing PlainTextAuthProvider
        - Creating the auth provider manually
        - Reduces boilerplate for simple auth setups

        Example usage:
        -------------
        cluster = AsyncCluster.create_with_auth(
            contact_points=['cassandra.example.com'],
            username='myuser',
            password='mypass'
        )
        """
        contact_points = ["localhost"]
        username = "testuser"
        password = "testpass"

        AsyncCluster.create_with_auth(
            contact_points=contact_points, username=username, password=password
        )

        from async_cassandra.cluster import Cluster as ClusterImport

        call_args = ClusterImport.call_args

        assert call_args.kwargs["contact_points"] == contact_points
        # Verify PlainTextAuthProvider was created
        auth_provider = call_args.kwargs["auth_provider"]
        assert isinstance(auth_provider, PlainTextAuthProvider)

    @pytest.mark.asyncio
    async def test_connect_without_keyspace(self, mock_cluster):
        """
        Test connecting without keyspace.

        What this tests:
        ---------------
        1. connect() can be called without specifying keyspace
        2. AsyncCassandraSession is created properly
        3. Protocol version is validated (must be v5+)
        4. None is passed as keyspace to session creation

        Why this matters:
        ----------------
        Users often connect first, then select keyspace later.
        This pattern is common for:
        - Creating keyspaces dynamically
        - Working with multiple keyspaces
        - Administrative operations

        Protocol validation ensures async features work correctly.
        """
        async_cluster = AsyncCluster()

        # Mock protocol version as v5 so it passes validation
        mock_cluster.protocol_version = 5

        with patch("async_cassandra.cluster.AsyncCassandraSession.create") as mock_create:
            mock_session = Mock(spec=AsyncCassandraSession)
            mock_create.return_value = mock_session

            session = await async_cluster.connect()

            assert session == mock_session
            # Verify keyspace=None was passed
            mock_create.assert_called_once_with(mock_cluster, None)

    @pytest.mark.asyncio
    async def test_connect_with_keyspace(self, mock_cluster):
        """
        Test connecting with keyspace.

        What this tests:
        ---------------
        1. connect() accepts keyspace parameter
        2. Keyspace is passed to session creation
        3. Session is pre-configured with the keyspace

        Why this matters:
        ----------------
        Specifying keyspace at connection time:
        - Saves an extra round trip (no USE statement)
        - Ensures all queries use the correct keyspace
        - Prevents accidental cross-keyspace queries
        - Common pattern for single-keyspace applications
        """
        async_cluster = AsyncCluster()
        keyspace = "test_keyspace"

        # Mock protocol version as v5 so it passes validation
        mock_cluster.protocol_version = 5

        with patch("async_cassandra.cluster.AsyncCassandraSession.create") as mock_create:
            mock_session = Mock(spec=AsyncCassandraSession)
            mock_create.return_value = mock_session

            session = await async_cluster.connect(keyspace)

            assert session == mock_session
            # Verify keyspace was passed through
            mock_create.assert_called_once_with(mock_cluster, keyspace)

    @pytest.mark.asyncio
    async def test_connect_error(self, mock_cluster):
        """
        Test handling connection error.

        What this tests:
        ---------------
        1. Generic exceptions are wrapped in ConnectionError
        2. Original exception is preserved as __cause__
        3. Error message provides context

        Why this matters:
        ----------------
        Connection failures need clear error messages:
        - Users need to know it's a connection issue
        - Original error details must be preserved
        - Stack traces should show the full context

        Common causes:
        - Network issues
        - Wrong contact points
        - Cassandra not running
        - Authentication failures
        """
        async_cluster = AsyncCluster()

        with patch("async_cassandra.cluster.AsyncCassandraSession.create") as mock_create:
            # Simulate connection failure
            mock_create.side_effect = Exception("Connection failed")

            with pytest.raises(ConnectionError) as exc_info:
                await async_cluster.connect()

            # Verify error wrapping
            assert "Failed to connect to cluster" in str(exc_info.value)
            # Verify original exception is preserved for debugging
            assert exc_info.value.__cause__ is not None

    @pytest.mark.asyncio
    async def test_connect_on_closed_cluster(self, mock_cluster):
        """
        Test connecting on closed cluster.

        What this tests:
        ---------------
        1. Cannot connect after shutdown()
        2. Clear error message is provided
        3. No resource leaks or hangs

        Why this matters:
        ----------------
        Prevents common programming errors:
        - Using cluster after cleanup
        - Race conditions in shutdown
        - Resource leaks from partial operations

        This ensures fail-fast behavior rather than
        mysterious hangs or corrupted state.
        """
        async_cluster = AsyncCluster()
        # Close the cluster first
        await async_cluster.shutdown()

        with pytest.raises(ConnectionError) as exc_info:
            await async_cluster.connect()

        # Verify clear error message
        assert "Cluster is closed" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_shutdown(self, mock_cluster):
        """
        Test shutting down the cluster.

        What this tests:
        ---------------
        1. shutdown() marks cluster as closed
        2. Driver's shutdown() is called
        3. is_closed property reflects state

        Why this matters:
        ----------------
        Proper shutdown is critical for:
        - Closing network connections
        - Stopping background threads
        - Releasing memory
        - Clean process termination
        """
        async_cluster = AsyncCluster()

        await async_cluster.shutdown()

        # Verify state change
        assert async_cluster.is_closed
        # Verify driver cleanup
        mock_cluster.shutdown.assert_called_once()

    @pytest.mark.asyncio
    async def test_shutdown_idempotent(self, mock_cluster):
        """
        Test that shutdown is idempotent.

        What this tests:
        ---------------
        1. Multiple shutdown() calls are safe
        2. Driver shutdown only happens once
        3. No errors on repeated calls

        Why this matters:
        ----------------
        Idempotent shutdown prevents:
        - Double-free errors
        - Race conditions in cleanup
        - Errors in finally blocks

        Users might call shutdown() multiple times:
        - In error handlers
        - In finally blocks
        - From different cleanup paths
        """
        async_cluster = AsyncCluster()

        # Call shutdown twice
        await async_cluster.shutdown()
        await async_cluster.shutdown()

        # Driver shutdown should only be called once
        mock_cluster.shutdown.assert_called_once()

    @pytest.mark.asyncio
    async def test_context_manager(self, mock_cluster):
        """
        Test using cluster as async context manager.

        What this tests:
        ---------------
        1. Cluster supports 'async with' syntax
        2. Cluster is open inside the context
        3. Automatic shutdown on context exit

        Why this matters:
        ----------------
        Context managers ensure cleanup:
        ```python
        async with AsyncCluster() as cluster:
            session = await cluster.connect()
            # ... use session ...
        # cluster.shutdown() called automatically
        ```

        Benefits:
        - No forgotten shutdowns
        - Exception safety
        - Cleaner code
        - Resource leak prevention
        """
        async with AsyncCluster() as cluster:
            # Inside context: cluster should be usable
            assert isinstance(cluster, AsyncCluster)
            assert not cluster.is_closed

        # After context: should be shut down
        mock_cluster.shutdown.assert_called_once()

    def test_is_closed_property(self, mock_cluster):
        """
        Test is_closed property.

        What this tests:
        ---------------
        1. is_closed starts as False
        2. Reflects internal _closed state
        3. Read-only property (no setter)

        Why this matters:
        ----------------
        Users need to check cluster state before operations.
        This property enables defensive programming:
        ```python
        if not cluster.is_closed:
            session = await cluster.connect()
        ```
        """
        async_cluster = AsyncCluster()

        # Initially open
        assert not async_cluster.is_closed
        # Simulate closed state
        async_cluster._closed = True
        assert async_cluster.is_closed

    def test_metadata_property(self, mock_cluster):
        """
        Test metadata property.

        What this tests:
        ---------------
        1. Metadata is accessible from async wrapper
        2. Returns driver's cluster metadata

        Why this matters:
        ----------------
        Metadata provides:
        - Keyspace definitions
        - Table schemas
        - Node topology
        - Token ranges

        Essential for advanced features like:
        - Schema discovery
        - Token-aware routing
        - Dynamic query building
        """
        async_cluster = AsyncCluster()

        assert async_cluster.metadata == {"test": "metadata"}

    def test_register_user_type(self, mock_cluster):
        """
        Test registering user-defined type.

        What this tests:
        ---------------
        1. User types can be registered
        2. Registration is delegated to driver
        3. Parameters are passed correctly

        Why this matters:
        ----------------
        Cassandra supports complex user-defined types (UDTs).
        Python classes must be registered to handle them:

        ```python
        class Address:
            def __init__(self, street, city, zip_code):
                self.street = street
                self.city = city
                self.zip_code = zip_code

        cluster.register_user_type('my_keyspace', 'address', Address)
        ```

        This enables seamless UDT handling in queries.
        """
        async_cluster = AsyncCluster()

        keyspace = "test_keyspace"
        user_type = "address"
        klass = type("Address", (), {})  # Dynamic class for testing

        async_cluster.register_user_type(keyspace, user_type, klass)

        # Verify delegation to driver
        mock_cluster.register_user_type.assert_called_once_with(keyspace, user_type, klass)

    def test_ssl_context(self, mock_cluster):
        """
        Test initialization with SSL context.

        What this tests:
        ---------------
        1. SSL/TLS can be configured
        2. SSL context is passed to driver

        Why this matters:
        ----------------
        Production Cassandra often requires encryption:
        - Client-to-node encryption
        - Compliance requirements
        - Network security

        Example usage:
        -------------
        ```python
        import ssl

        ssl_context = ssl.create_default_context()
        ssl_context.load_cert_chain('client.crt', 'client.key')
        ssl_context.load_verify_locations('ca.crt')

        cluster = AsyncCluster(ssl_context=ssl_context)
        ```
        """
        ssl_context = SSLContext(PROTOCOL_TLS_CLIENT)

        AsyncCluster(ssl_context=ssl_context)

        from async_cassandra.cluster import Cluster as ClusterImport

        call_args = ClusterImport.call_args

        # Verify SSL context passed through
        assert call_args.kwargs["ssl_context"] == ssl_context

    def test_protocol_version_validation_v1(self, mock_cluster):
        """
        Test that protocol version 1 is rejected.

        What this tests:
        ---------------
        1. Protocol v1 raises ConfigurationError
        2. Error message explains the requirement
        3. Suggests Cassandra upgrade path

        Why we require v5+:
        ------------------
        Protocol v5 (Cassandra 4.0+) provides:
        - Improved async operations
        - Better error handling
        - Enhanced performance features
        - Required for some async patterns

        Protocol v1-v4 limitations:
        - Missing features we depend on
        - Less efficient for async operations
        - Older Cassandra versions (pre-4.0)

        This ensures users have a compatible setup
        before they encounter runtime issues.
        """
        with pytest.raises(ConfigurationError) as exc_info:
            AsyncCluster(protocol_version=1)

        # Verify helpful error message
        assert "Protocol version 1 is not supported" in str(exc_info.value)
        assert "requires CQL protocol v5 or higher" in str(exc_info.value)
        assert "Cassandra 4.0" in str(exc_info.value)

    def test_protocol_version_validation_v2(self, mock_cluster):
        """
        Test that protocol version 2 is rejected.

        Protocol v2 was used in Cassandra 2.0.
        Too old for async operations.
        """
        with pytest.raises(ConfigurationError) as exc_info:
            AsyncCluster(protocol_version=2)

        assert "Protocol version 2 is not supported" in str(exc_info.value)
        assert "requires CQL protocol v5 or higher" in str(exc_info.value)

    def test_protocol_version_validation_v3(self, mock_cluster):
        """
        Test that protocol version 3 is rejected.

        Protocol v3 was used in Cassandra 2.1-2.2.
        Still lacks features needed for optimal async.
        """
        with pytest.raises(ConfigurationError) as exc_info:
            AsyncCluster(protocol_version=3)

        assert "Protocol version 3 is not supported" in str(exc_info.value)
        assert "requires CQL protocol v5 or higher" in str(exc_info.value)

    def test_protocol_version_validation_v4(self, mock_cluster):
        """
        Test that protocol version 4 is rejected.

        Protocol v4 was used in Cassandra 3.x.
        Close but still missing v5 improvements.
        Most common version users might try.
        """
        with pytest.raises(ConfigurationError) as exc_info:
            AsyncCluster(protocol_version=4)

        assert "Protocol version 4 is not supported" in str(exc_info.value)
        assert "requires CQL protocol v5 or higher" in str(exc_info.value)

    def test_protocol_version_validation_v5(self, mock_cluster):
        """
        Test that protocol version 5 is accepted.

        Protocol v5 (Cassandra 4.0+) is our minimum.
        This is the first version we fully support.
        """
        # Should not raise
        AsyncCluster(protocol_version=5)

        from async_cassandra.cluster import Cluster as ClusterImport

        call_args = ClusterImport.call_args
        assert call_args.kwargs["protocol_version"] == 5

    def test_protocol_version_validation_v6(self, mock_cluster):
        """
        Test that protocol version 6 is accepted.

        Protocol v6 (Cassandra 4.1+) adds more features.
        We support it for future compatibility.
        """
        # Should not raise
        AsyncCluster(protocol_version=6)

        from async_cassandra.cluster import Cluster as ClusterImport

        call_args = ClusterImport.call_args
        assert call_args.kwargs["protocol_version"] == 6

    def test_protocol_version_none(self, mock_cluster):
        """
        Test that no protocol version allows driver negotiation.

        What this tests:
        ---------------
        1. Protocol version is optional
        2. Driver can negotiate version
        3. We validate after connection

        Why this matters:
        ----------------
        Allows flexibility:
        - Driver picks best version
        - Works with various Cassandra versions
        - Fails clearly if negotiated version < 5
        """
        # Should not raise and should not set protocol_version
        AsyncCluster()

        from async_cassandra.cluster import Cluster as ClusterImport

        call_args = ClusterImport.call_args
        # No protocol_version means driver negotiates
        assert "protocol_version" not in call_args.kwargs

    @pytest.mark.asyncio
    async def test_protocol_version_mismatch_error(self, mock_cluster):
        """
        Test that protocol version mismatch errors are handled properly.

        What this tests:
        ---------------
        1. NoHostAvailable with protocol errors get special handling
        2. Clear error message about version mismatch
        3. Actionable advice (upgrade Cassandra)

        Why this matters:
        ----------------
        Common scenario:
        - User tries to connect to Cassandra 3.x
        - Driver requests protocol v5
        - Server only supports v4

        Without special handling:
        - Generic "NoHostAvailable" error
        - User doesn't know why connection failed

        With our handling:
        - Clear message about protocol version
        - Tells user to upgrade to Cassandra 4.0+
        """
        async_cluster = AsyncCluster()

        # Mock NoHostAvailable with protocol error
        from cassandra.cluster import NoHostAvailable

        protocol_error = Exception("ProtocolError: Server does not support protocol version 5")
        no_host_error = NoHostAvailable("Unable to connect", {"host1": protocol_error})

        with patch("async_cassandra.cluster.AsyncCassandraSession.create") as mock_create:
            mock_create.side_effect = no_host_error

            with pytest.raises(ConnectionError) as exc_info:
                await async_cluster.connect()

            # Verify helpful error message
            error_msg = str(exc_info.value)
            assert "Your Cassandra server doesn't support protocol v5" in error_msg
            assert "Cassandra 4.0+" in error_msg
            assert "Please upgrade your Cassandra cluster" in error_msg

    @pytest.mark.asyncio
    async def test_negotiated_protocol_version_too_low(self, mock_cluster):
        """
        Test that negotiated protocol version < 5 is rejected after connection.

        What this tests:
        ---------------
        1. Protocol validation happens after connection
        2. Session is properly closed on failure
        3. Clear error about negotiated version

        Why this matters:
        ----------------
        Scenario:
        - User doesn't specify protocol version
        - Driver negotiates with server
        - Server offers v4 (Cassandra 3.x)
        - We detect this and fail cleanly

        This catches the case where:
        - Connection succeeds (server is running)
        - But protocol is incompatible
        - Must clean up the session

        Without this check:
        - Async operations might fail mysteriously
        - Users get confusing errors later
        """
        async_cluster = AsyncCluster()

        # Mock the cluster to return protocol_version 4 after connection
        mock_cluster.protocol_version = 4

        mock_session = Mock(spec=AsyncCassandraSession)

        # Track if close was called
        close_called = False

        async def async_close():
            nonlocal close_called
            close_called = True

        mock_session.close = async_close

        with patch("async_cassandra.cluster.AsyncCassandraSession.create") as mock_create:
            # Make create return a coroutine that returns the session
            async def create_session(cluster, keyspace):
                return mock_session

            mock_create.side_effect = create_session

            with pytest.raises(ConnectionError) as exc_info:
                await async_cluster.connect()

            # Verify specific error about negotiated version
            error_msg = str(exc_info.value)
            assert "Connected with protocol v4 but v5+ is required" in error_msg
            assert "Your Cassandra server only supports up to protocol v4" in error_msg
            assert "Cassandra 4.0+" in error_msg

            # Verify cleanup happened
            assert close_called, "Session close() should have been called"
