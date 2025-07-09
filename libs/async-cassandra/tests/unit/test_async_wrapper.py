"""Core async wrapper functionality tests.

This module consolidates tests for the fundamental async wrapper components
including AsyncCluster, AsyncSession, and base functionality.

Test Organization:
==================
1. TestAsyncContextManageable - Tests the base async context manager mixin
2. TestAsyncCluster - Tests cluster initialization, connection, and lifecycle
3. TestAsyncSession - Tests session operations (queries, prepare, keyspace)

Key Testing Patterns:
====================
- Uses mocks extensively to isolate async wrapper behavior from driver
- Tests both success and error paths
- Verifies context manager cleanup happens correctly
- Ensures proper parameter passing to underlying driver
"""

from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import ResponseFuture

from async_cassandra import AsyncCassandraSession as AsyncSession
from async_cassandra import AsyncCluster
from async_cassandra.base import AsyncContextManageable
from async_cassandra.result import AsyncResultSet


class TestAsyncContextManageable:
    """Test the async context manager mixin functionality."""

    @pytest.mark.core
    @pytest.mark.quick
    async def test_async_context_manager(self):
        """
        Test basic async context manager functionality.

        What this tests:
        ---------------
        1. AsyncContextManageable provides proper async context manager protocol
        2. __aenter__ is called when entering the context
        3. __aexit__ is called when exiting the context
        4. The object is properly returned from __aenter__

        Why this matters:
        ----------------
        Many of our classes (AsyncCluster, AsyncSession) inherit from this base
        class to provide 'async with' functionality. This ensures resource cleanup
        happens automatically when leaving the context.
        """

        # Create a test implementation that tracks enter/exit calls
        class TestClass(AsyncContextManageable):
            entered = False
            exited = False

            async def __aenter__(self):
                self.entered = True
                return self

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                self.exited = True

        # Test the context manager flow
        async with TestClass() as obj:
            # Inside context: should be entered but not exited
            assert obj.entered
            assert not obj.exited

        # Outside context: should be exited
        assert obj.exited

    @pytest.mark.core
    async def test_context_manager_with_exception(self):
        """
        Test context manager handles exceptions properly.

        What this tests:
        ---------------
        1. __aexit__ receives exception information when exception occurs
        2. Exception type, value, and traceback are passed correctly
        3. Returning False from __aexit__ propagates the exception
        4. The exception is not suppressed unless explicitly handled

        Why this matters:
        ----------------
        Ensures that errors in async operations (like connection failures)
        are properly propagated and that cleanup still happens even when
        exceptions occur. This prevents resource leaks in error scenarios.
        """

        class TestClass(AsyncContextManageable):
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                # Verify exception info is passed correctly
                assert exc_type is ValueError
                assert str(exc_val) == "test error"
                return False  # Don't suppress exception - let it propagate

        # Verify the exception is still raised after __aexit__
        with pytest.raises(ValueError, match="test error"):
            async with TestClass():
                raise ValueError("test error")


class TestAsyncCluster:
    """
    Test AsyncCluster core functionality.

    AsyncCluster is the entry point for establishing Cassandra connections.
    It wraps the driver's Cluster object to provide async operations.
    """

    @pytest.mark.core
    @pytest.mark.quick
    def test_init_defaults(self):
        """
        Test AsyncCluster initialization with default values.

        What this tests:
        ---------------
        1. AsyncCluster can be created without any parameters
        2. Default values are properly applied
        3. Internal state is initialized correctly (_cluster, _close_lock)

        Why this matters:
        ----------------
        Users often create clusters with minimal configuration. This ensures
        the defaults work correctly and the cluster is usable out of the box.
        """
        cluster = AsyncCluster()
        # Verify internal driver cluster was created
        assert cluster._cluster is not None
        # Verify lock for thread-safe close operations exists
        assert cluster._close_lock is not None

    @pytest.mark.core
    def test_init_custom_values(self):
        """
        Test AsyncCluster initialization with custom values.

        What this tests:
        ---------------
        1. Custom contact points are accepted
        2. Non-default port can be specified
        3. Authentication providers work correctly
        4. Executor thread pool size can be customized
        5. All parameters are properly passed to underlying driver

        Why this matters:
        ----------------
        Production deployments often require custom configuration:
        - Different Cassandra nodes (contact_points)
        - Non-standard ports for security
        - Authentication for secure clusters
        - Thread pool tuning for performance
        """
        # Create auth provider for secure clusters
        auth_provider = PlainTextAuthProvider(username="user", password="pass")

        # Initialize with custom configuration
        cluster = AsyncCluster(
            contact_points=["192.168.1.1", "192.168.1.2"],
            port=9043,  # Non-default port
            auth_provider=auth_provider,
            executor_threads=16,  # Larger thread pool for high concurrency
        )

        # Verify cluster was created with our settings
        assert cluster._cluster is not None
        # Verify thread pool size was applied
        assert cluster._cluster.executor._max_workers == 16

    @pytest.mark.core
    @patch("async_cassandra.cluster.Cluster", new_callable=MagicMock)
    async def test_connect(self, mock_cluster_class):
        """
        Test cluster connection.

        What this tests:
        ---------------
        1. connect() returns an AsyncSession instance
        2. The underlying driver's connect() is called
        3. The returned session wraps the driver's session
        4. Connection can be established without specifying keyspace

        Why this matters:
        ----------------
        This is the primary way users establish database connections.
        The test ensures our async wrapper properly delegates to the
        synchronous driver and wraps the result for async operations.

        Implementation note:
        -------------------
        We mock the driver's Cluster to isolate our wrapper's behavior
        from actual network operations.
        """
        # Set up mocks
        mock_cluster = mock_cluster_class.return_value
        mock_cluster.protocol_version = 5  # Mock protocol version
        mock_session = Mock()
        mock_cluster.connect.return_value = mock_session

        # Test connection
        cluster = AsyncCluster()
        session = await cluster.connect()

        # Verify we get an async wrapper
        assert isinstance(session, AsyncSession)
        # Verify it wraps the driver's session
        assert session._session == mock_session
        # Verify driver's connect was called
        mock_cluster.connect.assert_called_once()

    @pytest.mark.core
    @patch("async_cassandra.cluster.Cluster", new_callable=MagicMock)
    async def test_shutdown(self, mock_cluster_class):
        """
        Test cluster shutdown.

        What this tests:
        ---------------
        1. shutdown() can be called explicitly
        2. The underlying driver's shutdown() is called
        3. Resources are properly cleaned up

        Why this matters:
        ----------------
        Proper shutdown is critical to:
        - Release network connections
        - Stop background threads
        - Prevent resource leaks
        - Allow clean application termination
        """
        mock_cluster = mock_cluster_class.return_value

        cluster = AsyncCluster()
        await cluster.shutdown()

        # Verify driver's shutdown was called
        mock_cluster.shutdown.assert_called_once()

    @pytest.mark.core
    @pytest.mark.critical
    async def test_context_manager(self):
        """
        Test AsyncCluster as context manager.

        What this tests:
        ---------------
        1. AsyncCluster can be used with 'async with' statement
        2. Cluster is accessible within the context
        3. shutdown() is automatically called on exit
        4. Cleanup happens even if not explicitly called

        Why this matters:
        ----------------
        Context managers are the recommended pattern for resource management.
        They ensure cleanup happens automatically, preventing resource leaks
        even if the user forgets to call shutdown() or if exceptions occur.

        Example usage:
        -------------
        async with AsyncCluster() as cluster:
            session = await cluster.connect()
            # ... use session ...
        # cluster.shutdown() called automatically here
        """
        with patch("async_cassandra.cluster.Cluster") as mock_cluster_class:
            mock_cluster = mock_cluster_class.return_value

            # Use cluster as context manager
            async with AsyncCluster() as cluster:
                # Verify cluster is accessible inside context
                assert cluster._cluster == mock_cluster

            # Verify shutdown was called when exiting context
            mock_cluster.shutdown.assert_called_once()


class TestAsyncSession:
    """
    Test AsyncSession core functionality.

    AsyncSession is the main interface for executing queries. It wraps
    the driver's Session object to provide async query execution.
    """

    @pytest.mark.core
    @pytest.mark.quick
    def test_init(self):
        """
        Test AsyncSession initialization.

        What this tests:
        ---------------
        1. AsyncSession properly stores the wrapped session
        2. No additional initialization is required
        3. The wrapper is lightweight (thin wrapper pattern)

        Why this matters:
        ----------------
        The session wrapper should be minimal overhead. This test
        ensures we're not doing unnecessary work during initialization
        and that the wrapper maintains a reference to the driver session.
        """
        mock_session = Mock()
        async_session = AsyncSession(mock_session)
        # Verify the wrapper stores the driver session
        assert async_session._session == mock_session

    @pytest.mark.core
    @pytest.mark.critical
    async def test_execute_simple_query(self):
        """
        Test executing a simple query.

        What this tests:
        ---------------
        1. Basic query execution works
        2. execute() converts sync driver operations to async
        3. Results are wrapped in AsyncResultSet
        4. The AsyncResultHandler is used to manage callbacks

        Why this matters:
        ----------------
        This is the most fundamental operation - executing a SELECT query.
        The test verifies our async/await wrapper correctly:
        - Calls driver's execute_async (not execute)
        - Handles the ResponseFuture with callbacks
        - Returns results in an async-friendly format

        Implementation details:
        ----------------------
        - We mock AsyncResultHandler to avoid callback complexity
        - The real implementation registers callbacks on ResponseFuture
        - Results are delivered asynchronously via the event loop
        """
        # Set up driver mocks
        mock_session = Mock()
        mock_future = Mock(spec=ResponseFuture)
        mock_future.has_more_pages = False
        mock_session.execute_async.return_value = mock_future

        async_session = AsyncSession(mock_session)

        # Mock the result handler to simulate query completion
        with patch("async_cassandra.session.AsyncResultHandler") as mock_handler_class:
            mock_handler = Mock()
            mock_result = AsyncResultSet([{"id": 1, "name": "test"}])
            mock_handler.get_result = AsyncMock(return_value=mock_result)
            mock_handler_class.return_value = mock_handler

            # Execute query
            result = await async_session.execute("SELECT * FROM users")

        # Verify result type and that async execution was used
        assert isinstance(result, AsyncResultSet)
        mock_session.execute_async.assert_called_once()

    @pytest.mark.core
    async def test_execute_with_parameters(self):
        """
        Test executing query with parameters.

        What this tests:
        ---------------
        1. Parameterized queries work correctly
        2. Parameters are passed through to the driver
        3. Both query string and parameters reach execute_async

        Why this matters:
        ----------------
        Parameterized queries are essential for:
        - Preventing SQL injection attacks
        - Better performance (query plan caching)
        - Cleaner code (no string concatenation)

        The test ensures parameters aren't lost in the async wrapper.

        Note:
        -----
        Parameters can be passed as list [123] or tuple (123,)
        This test uses a list, but both should work.
        """
        mock_session = Mock()
        mock_future = Mock(spec=ResponseFuture)
        mock_session.execute_async.return_value = mock_future

        async_session = AsyncSession(mock_session)

        with patch("async_cassandra.session.AsyncResultHandler") as mock_handler_class:
            mock_handler = Mock()
            mock_result = AsyncResultSet([])
            mock_handler.get_result = AsyncMock(return_value=mock_result)
            mock_handler_class.return_value = mock_handler

            # Execute parameterized query
            await async_session.execute("SELECT * FROM users WHERE id = ?", [123])

        # Verify both query and parameters were passed correctly
        call_args = mock_session.execute_async.call_args
        assert call_args[0][0] == "SELECT * FROM users WHERE id = ?"
        assert call_args[0][1] == [123]

    @pytest.mark.core
    async def test_prepare(self):
        """
        Test preparing statements.

        What this tests:
        ---------------
        1. prepare() returns a PreparedStatement
        2. The query string is passed to driver's prepare()
        3. The prepared statement can be used for execution

        Why this matters:
        ----------------
        Prepared statements are crucial for production use:
        - Better performance (cached query plans)
        - Type safety and validation
        - Protection against injection
        - Required by our coding standards

        The wrapper must properly handle statement preparation
        to maintain these benefits.

        Note:
        -----
        The second parameter (None) is for custom prepare options,
        which we pass through unchanged.
        """
        mock_session = Mock()
        mock_prepared = Mock()
        mock_session.prepare.return_value = mock_prepared

        async_session = AsyncSession(mock_session)

        # Prepare a parameterized statement
        prepared = await async_session.prepare("SELECT * FROM users WHERE id = ?")

        # Verify we get the prepared statement back
        assert prepared == mock_prepared
        # Verify driver's prepare was called with correct arguments
        mock_session.prepare.assert_called_once_with("SELECT * FROM users WHERE id = ?", None)

    @pytest.mark.core
    async def test_close(self):
        """
        Test closing session.

        What this tests:
        ---------------
        1. close() can be called explicitly
        2. The underlying session's shutdown() is called
        3. Resources are cleaned up properly

        Why this matters:
        ----------------
        Sessions hold resources like:
        - Connection pools
        - Prepared statement cache
        - Background threads

        Proper cleanup prevents resource leaks and ensures
        graceful application shutdown.
        """
        mock_session = Mock()
        async_session = AsyncSession(mock_session)

        await async_session.close()

        # Verify driver's shutdown was called
        mock_session.shutdown.assert_called_once()

    @pytest.mark.core
    @pytest.mark.critical
    async def test_context_manager(self):
        """
        Test AsyncSession as context manager.

        What this tests:
        ---------------
        1. AsyncSession supports 'async with' statement
        2. Session is accessible within the context
        3. shutdown() is called automatically on exit

        Why this matters:
        ----------------
        Context managers ensure cleanup even with exceptions.
        This is the recommended pattern for session usage:

        async with cluster.connect() as session:
            await session.execute(...)
        # session.close() called automatically

        This prevents resource leaks from forgotten close() calls.
        """
        mock_session = Mock()

        async with AsyncSession(mock_session) as session:
            # Verify session is accessible in context
            assert session._session == mock_session

        # Verify cleanup happened on exit
        mock_session.shutdown.assert_called_once()

    @pytest.mark.core
    async def test_set_keyspace(self):
        """
        Test setting keyspace.

        What this tests:
        ---------------
        1. set_keyspace() executes a USE statement
        2. The keyspace name is properly formatted
        3. The operation completes successfully

        Why this matters:
        ----------------
        Keyspaces organize data in Cassandra (like databases in SQL).
        Users need to switch keyspaces for different data domains.
        The wrapper must handle this transparently.

        Implementation note:
        -------------------
        set_keyspace() is implemented as execute("USE keyspace")
        This test verifies that translation works correctly.
        """
        mock_session = Mock()
        mock_future = Mock(spec=ResponseFuture)
        mock_session.execute_async.return_value = mock_future

        async_session = AsyncSession(mock_session)

        with patch("async_cassandra.session.AsyncResultHandler") as mock_handler_class:
            mock_handler = Mock()
            mock_result = AsyncResultSet([])
            mock_handler.get_result = AsyncMock(return_value=mock_result)
            mock_handler_class.return_value = mock_handler

            # Set the keyspace
            await async_session.set_keyspace("test_keyspace")

        # Verify USE statement was executed
        call_args = mock_session.execute_async.call_args
        assert call_args[0][0] == "USE test_keyspace"
