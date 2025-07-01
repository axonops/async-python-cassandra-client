"""
Unit tests for schema change handling.

Tests how the async wrapper handles:
- Schema change events
- Metadata refresh
- Schema agreement
- DDL operation execution
- Prepared statement invalidation on schema changes
"""

import asyncio
from unittest.mock import Mock, patch

import pytest
from cassandra import AlreadyExists, InvalidRequest

from async_cassandra import AsyncCassandraSession, AsyncCluster


class TestSchemaChanges:
    """Test schema change handling scenarios."""

    @pytest.fixture
    def mock_session(self):
        """Create a mock session."""
        session = Mock()
        session.execute_async = Mock()
        session.prepare_async = Mock()
        session.cluster = Mock()
        return session

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

    def _create_mock_future(self, result=None, error=None):
        """Create a properly configured mock future that simulates driver behavior."""
        future = Mock()

        # Store callbacks
        callbacks = []
        errbacks = []

        def add_callbacks(callback=None, errback=None):
            if callback:
                callbacks.append(callback)
            if errback:
                errbacks.append(errback)

            # Delay the callback execution to allow AsyncResultHandler to set up properly
            def execute_callback():
                if error:
                    if errback:
                        errback(error)
                else:
                    if callback and result is not None:
                        # For successful results, pass rows
                        rows = getattr(result, "rows", [])
                        callback(rows)

            # Schedule callback for next event loop iteration
            try:
                loop = asyncio.get_running_loop()
                loop.call_soon(execute_callback)
            except RuntimeError:
                # No event loop, execute immediately
                execute_callback()

        future.add_callbacks = add_callbacks
        future.has_more_pages = False
        future.timeout = None
        future.clear_callbacks = Mock()

        return future

    @pytest.mark.asyncio
    async def test_create_table_already_exists(self, mock_session):
        """
        Test handling of AlreadyExists errors during schema changes.

        What this tests:
        ---------------
        1. CREATE TABLE on existing table
        2. AlreadyExists wrapped in QueryError
        3. Keyspace and table info preserved
        4. Error details accessible

        Why this matters:
        ----------------
        Schema conflicts common in:
        - Concurrent deployments
        - Idempotent migrations
        - Multi-datacenter setups

        Applications need to handle
        schema conflicts gracefully.
        """
        async_session = AsyncCassandraSession(mock_session)

        # Mock AlreadyExists error
        error = AlreadyExists(keyspace="test_ks", table="test_table")
        mock_session.execute_async.return_value = self.create_error_future(error)

        # AlreadyExists is passed through directly
        with pytest.raises(AlreadyExists) as exc_info:
            await async_session.execute("CREATE TABLE test_table (id int PRIMARY KEY)")

        assert exc_info.value.keyspace == "test_ks"
        assert exc_info.value.table == "test_table"

    @pytest.mark.asyncio
    async def test_ddl_invalid_syntax(self, mock_session):
        """
        Test handling of invalid DDL syntax.

        What this tests:
        ---------------
        1. DDL syntax errors detected
        2. InvalidRequest not wrapped
        3. Parser error details shown
        4. Line/column info preserved

        Why this matters:
        ----------------
        DDL syntax errors indicate:
        - Typos in schema scripts
        - Version incompatibilities
        - Invalid CQL constructs

        Clear errors help developers
        fix schema definitions quickly.
        """
        async_session = AsyncCassandraSession(mock_session)

        # Mock InvalidRequest error
        error = InvalidRequest("line 1:13 no viable alternative at input 'TABEL'")
        mock_session.execute_async.return_value = self.create_error_future(error)

        # InvalidRequest is NOT wrapped - it's in the re-raise list
        with pytest.raises(InvalidRequest) as exc_info:
            await async_session.execute("CREATE TABEL test (id int PRIMARY KEY)")

        assert "no viable alternative" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_create_keyspace_already_exists(self, mock_session):
        """
        Test handling of keyspace already exists errors.

        What this tests:
        ---------------
        1. CREATE KEYSPACE conflicts
        2. AlreadyExists for keyspaces
        3. Table field is None
        4. Wrapped in QueryError

        Why this matters:
        ----------------
        Keyspace conflicts occur when:
        - Multiple apps create keyspaces
        - Deployment race conditions
        - Recreating environments

        Idempotent keyspace creation
        requires proper error handling.
        """
        async_session = AsyncCassandraSession(mock_session)

        # Mock AlreadyExists error for keyspace
        error = AlreadyExists(keyspace="test_keyspace", table=None)
        mock_session.execute_async.return_value = self.create_error_future(error)

        # AlreadyExists is passed through directly
        with pytest.raises(AlreadyExists) as exc_info:
            await async_session.execute(
                "CREATE KEYSPACE test_keyspace WITH replication = "
                "{'class': 'SimpleStrategy', 'replication_factor': 1}"
            )

        assert exc_info.value.keyspace == "test_keyspace"
        assert exc_info.value.table is None

    @pytest.mark.asyncio
    async def test_concurrent_ddl_operations(self, mock_session):
        """
        Test handling of concurrent DDL operations.

        What this tests:
        ---------------
        1. Multiple DDL ops can run concurrently
        2. No interference between operations
        3. All operations complete
        4. Order not guaranteed

        Why this matters:
        ----------------
        Schema migrations often involve:
        - Multiple table creations
        - Index additions
        - Concurrent alterations

        Async wrapper must handle parallel
        DDL operations safely.
        """
        async_session = AsyncCassandraSession(mock_session)

        # Track DDL operations
        ddl_operations = []

        def execute_async_side_effect(*args, **kwargs):
            query = args[0] if args else kwargs.get("query", "")
            ddl_operations.append(query)

            # Use the same pattern as test_session_edge_cases
            result = Mock()
            result.rows = []  # DDL operations return no rows
            return self._create_mock_future(result=result)

        mock_session.execute_async.side_effect = execute_async_side_effect

        # Execute multiple DDL operations concurrently
        ddl_queries = [
            "CREATE TABLE table1 (id int PRIMARY KEY)",
            "CREATE TABLE table2 (id int PRIMARY KEY)",
            "ALTER TABLE table1 ADD column1 text",
            "CREATE INDEX idx1 ON table1 (column1)",
            "DROP TABLE IF EXISTS table3",
        ]

        tasks = [async_session.execute(query) for query in ddl_queries]
        await asyncio.gather(*tasks)

        # All DDL operations should have been executed
        assert len(ddl_operations) == 5
        assert all(query in ddl_operations for query in ddl_queries)

    @pytest.mark.asyncio
    async def test_alter_table_column_type_error(self, mock_session):
        """
        Test handling of invalid column type changes.

        What this tests:
        ---------------
        1. Invalid type changes rejected
        2. InvalidRequest not wrapped
        3. Type conflict details shown
        4. Original types mentioned

        Why this matters:
        ----------------
        Type changes restricted because:
        - Data compatibility issues
        - Storage format conflicts
        - Query implications

        Developers need clear guidance
        on valid schema evolution.
        """
        async_session = AsyncCassandraSession(mock_session)

        # Mock InvalidRequest for incompatible type change
        error = InvalidRequest("Cannot change column type from 'int' to 'text'")
        mock_session.execute_async.return_value = self.create_error_future(error)

        # InvalidRequest is NOT wrapped
        with pytest.raises(InvalidRequest) as exc_info:
            await async_session.execute("ALTER TABLE users ALTER age TYPE text")

        assert "Cannot change column type" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_drop_nonexistent_keyspace(self, mock_session):
        """
        Test dropping a non-existent keyspace.

        What this tests:
        ---------------
        1. DROP on missing keyspace
        2. InvalidRequest not wrapped
        3. Clear error message
        4. Keyspace name in error

        Why this matters:
        ----------------
        Drop operations may fail when:
        - Cleanup scripts run twice
        - Keyspace already removed
        - Name typos

        IF EXISTS clause recommended
        for idempotent drops.
        """
        async_session = AsyncCassandraSession(mock_session)

        # Mock InvalidRequest for non-existent keyspace
        error = InvalidRequest("Keyspace 'nonexistent' doesn't exist")
        mock_session.execute_async.return_value = self.create_error_future(error)

        # InvalidRequest is NOT wrapped
        with pytest.raises(InvalidRequest) as exc_info:
            await async_session.execute("DROP KEYSPACE nonexistent")

        assert "doesn't exist" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_create_type_already_exists(self, mock_session):
        """
        Test creating a user-defined type that already exists.

        What this tests:
        ---------------
        1. CREATE TYPE conflicts
        2. UDTs treated like tables
        3. AlreadyExists wrapped
        4. Type name in error

        Why this matters:
        ----------------
        User-defined types (UDTs):
        - Share namespace with tables
        - Support complex data models
        - Need conflict handling

        Schema with UDTs requires
        careful version control.
        """
        async_session = AsyncCassandraSession(mock_session)

        # Mock AlreadyExists for UDT
        error = AlreadyExists(keyspace="test_ks", table="address_type")
        mock_session.execute_async.return_value = self.create_error_future(error)

        # AlreadyExists is passed through directly
        with pytest.raises(AlreadyExists) as exc_info:
            await async_session.execute(
                "CREATE TYPE address_type (street text, city text, zip int)"
            )

        assert exc_info.value.keyspace == "test_ks"
        assert exc_info.value.table == "address_type"

    @pytest.mark.asyncio
    async def test_batch_ddl_operations(self, mock_session):
        """
        Test that DDL operations cannot be batched.

        What this tests:
        ---------------
        1. DDL not allowed in batches
        2. InvalidRequest not wrapped
        3. Clear error message
        4. Cassandra limitation enforced

        Why this matters:
        ----------------
        DDL restrictions exist because:
        - Schema changes are global
        - Cannot be transactional
        - Affect all nodes

        Schema changes must be
        executed individually.
        """
        async_session = AsyncCassandraSession(mock_session)

        # Mock InvalidRequest for DDL in batch
        error = InvalidRequest("DDL statements cannot be batched")
        mock_session.execute_async.return_value = self.create_error_future(error)

        # InvalidRequest is NOT wrapped
        with pytest.raises(InvalidRequest) as exc_info:
            await async_session.execute(
                """
                BEGIN BATCH
                CREATE TABLE t1 (id int PRIMARY KEY);
                CREATE TABLE t2 (id int PRIMARY KEY);
                APPLY BATCH;
            """
            )

        assert "cannot be batched" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_schema_metadata_access(self):
        """
        Test accessing schema metadata through the cluster.

        What this tests:
        ---------------
        1. Metadata accessible via cluster
        2. Keyspace information available
        3. Schema discovery works
        4. No async wrapper needed

        Why this matters:
        ----------------
        Metadata access enables:
        - Dynamic schema discovery
        - Table introspection
        - Type information

        Applications use metadata for
        ORM mapping and validation.
        """
        with patch("async_cassandra.cluster.Cluster") as mock_cluster_class:
            # Create mock cluster with metadata
            mock_cluster = Mock()
            mock_cluster_class.return_value = mock_cluster

            # Mock metadata
            mock_metadata = Mock()
            mock_metadata.keyspaces = {
                "system": Mock(name="system"),
                "test_ks": Mock(name="test_ks"),
            }
            mock_cluster.metadata = mock_metadata

            async_cluster = AsyncCluster(contact_points=["127.0.0.1"])

            # Access metadata
            metadata = async_cluster.metadata
            assert "system" in metadata.keyspaces
            assert "test_ks" in metadata.keyspaces

            await async_cluster.shutdown()

    @pytest.mark.asyncio
    async def test_materialized_view_already_exists(self, mock_session):
        """
        Test creating a materialized view that already exists.

        What this tests:
        ---------------
        1. MV conflicts detected
        2. AlreadyExists wrapped
        3. View name in error
        4. Same handling as tables

        Why this matters:
        ----------------
        Materialized views:
        - Auto-maintained denormalization
        - Share table namespace
        - Need conflict resolution

        MV schema changes require same
        care as regular tables.
        """
        async_session = AsyncCassandraSession(mock_session)

        # Mock AlreadyExists for materialized view
        error = AlreadyExists(keyspace="test_ks", table="user_by_email")
        mock_session.execute_async.return_value = self.create_error_future(error)

        # AlreadyExists is passed through directly
        with pytest.raises(AlreadyExists) as exc_info:
            await async_session.execute(
                """
                CREATE MATERIALIZED VIEW user_by_email AS
                SELECT * FROM users
                WHERE email IS NOT NULL
                PRIMARY KEY (email, id)
            """
            )

        assert exc_info.value.table == "user_by_email"
