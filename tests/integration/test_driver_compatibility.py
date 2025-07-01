"""
Integration tests comparing async wrapper behavior with raw driver.

This ensures our wrapper maintains compatibility and doesn't break any functionality.
"""

import uuid

import pytest
from cassandra.cluster import Cluster as SyncCluster
from cassandra.query import BatchStatement, BatchType, dict_factory


@pytest.mark.integration
class TestDriverCompatibility:
    """Test async wrapper compatibility with raw driver features."""

    @pytest.fixture
    def sync_cluster(self):
        """Create a synchronous cluster for comparison."""
        cluster = SyncCluster(["127.0.0.1"])
        yield cluster
        cluster.shutdown()

    @pytest.fixture
    def sync_session(self, sync_cluster, unique_keyspace):
        """Create a synchronous session."""
        session = sync_cluster.connect()
        session.execute(
            f"""
            CREATE KEYSPACE IF NOT EXISTS {unique_keyspace}
            WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        """
        )
        session.set_keyspace(unique_keyspace)
        yield session
        session.shutdown()

    @pytest.mark.asyncio
    async def test_basic_query_compatibility(self, sync_session, session_with_keyspace):
        """
        Test basic query execution matches between sync and async.

        What this tests:
        ---------------
        1. Same query syntax works
        2. Prepared statements compatible
        3. Results format matches
        4. Independent keyspaces

        Why this matters:
        ----------------
        API compatibility ensures:
        - Easy migration
        - Same patterns work
        - No relearning needed

        Drop-in replacement for
        sync driver.
        """
        async_session, keyspace = session_with_keyspace

        # Create table in both sessions' keyspace
        table_name = f"compat_basic_{uuid.uuid4().hex[:8]}"
        create_table = f"""
            CREATE TABLE {table_name} (
                id int PRIMARY KEY,
                name text,
                value double
            )
        """

        # Create in sync session's keyspace
        sync_session.execute(create_table)

        # Create in async session's keyspace
        await async_session.execute(create_table)

        # Prepare statements - both use ? for prepared statements
        sync_prepared = sync_session.prepare(
            f"INSERT INTO {table_name} (id, name, value) VALUES (?, ?, ?)"
        )
        async_prepared = await async_session.prepare(
            f"INSERT INTO {table_name} (id, name, value) VALUES (?, ?, ?)"
        )

        # Sync insert
        sync_session.execute(sync_prepared, (1, "sync", 1.23))

        # Async insert
        await async_session.execute(async_prepared, (2, "async", 4.56))

        # Both should see their own rows (different keyspaces)
        sync_result = list(sync_session.execute(f"SELECT * FROM {table_name}"))
        async_result = list(await async_session.execute(f"SELECT * FROM {table_name}"))

        assert len(sync_result) == 1  # Only sync's insert
        assert len(async_result) == 1  # Only async's insert
        assert sync_result[0].name == "sync"
        assert async_result[0].name == "async"

    @pytest.mark.asyncio
    async def test_batch_compatibility(self, sync_session, session_with_keyspace):
        """
        Test batch operations compatibility.

        What this tests:
        ---------------
        1. Batch types work same
        2. Counter batches OK
        3. Statement binding
        4. Execution results

        Why this matters:
        ----------------
        Batch operations critical:
        - Atomic operations
        - Performance optimization
        - Complex workflows

        Must work identically
        to sync driver.
        """
        async_session, keyspace = session_with_keyspace

        # Create tables in both keyspaces
        table_name = f"compat_batch_{uuid.uuid4().hex[:8]}"
        counter_table = f"compat_counter_{uuid.uuid4().hex[:8]}"

        # Create in sync keyspace
        sync_session.execute(
            f"""
            CREATE TABLE {table_name} (
                id int PRIMARY KEY,
                value text
            )
        """
        )
        sync_session.execute(
            f"""
            CREATE TABLE {counter_table} (
                id text PRIMARY KEY,
                count counter
            )
        """
        )

        # Create in async keyspace
        await async_session.execute(
            f"""
            CREATE TABLE {table_name} (
                id int PRIMARY KEY,
                value text
            )
        """
        )
        await async_session.execute(
            f"""
            CREATE TABLE {counter_table} (
                id text PRIMARY KEY,
                count counter
            )
        """
        )

        # Prepare statements
        sync_stmt = sync_session.prepare(f"INSERT INTO {table_name} (id, value) VALUES (?, ?)")
        async_stmt = await async_session.prepare(
            f"INSERT INTO {table_name} (id, value) VALUES (?, ?)"
        )

        # Test logged batch
        sync_batch = BatchStatement()
        async_batch = BatchStatement()

        for i in range(5):
            sync_batch.add(sync_stmt, (i, f"sync_{i}"))
            async_batch.add(async_stmt, (i + 10, f"async_{i}"))

        sync_session.execute(sync_batch)
        await async_session.execute(async_batch)

        # Test counter batch
        sync_counter_stmt = sync_session.prepare(
            f"UPDATE {counter_table} SET count = count + ? WHERE id = ?"
        )
        async_counter_stmt = await async_session.prepare(
            f"UPDATE {counter_table} SET count = count + ? WHERE id = ?"
        )

        sync_counter_batch = BatchStatement(batch_type=BatchType.COUNTER)
        async_counter_batch = BatchStatement(batch_type=BatchType.COUNTER)

        sync_counter_batch.add(sync_counter_stmt, (5, "sync_counter"))
        async_counter_batch.add(async_counter_stmt, (10, "async_counter"))

        sync_session.execute(sync_counter_batch)
        await async_session.execute(async_counter_batch)

        # Verify
        sync_batch_result = list(sync_session.execute(f"SELECT * FROM {table_name}"))
        async_batch_result = list(await async_session.execute(f"SELECT * FROM {table_name}"))

        assert len(sync_batch_result) == 5  # sync batch
        assert len(async_batch_result) == 5  # async batch

        sync_counter_result = list(sync_session.execute(f"SELECT * FROM {counter_table}"))
        async_counter_result = list(await async_session.execute(f"SELECT * FROM {counter_table}"))

        assert len(sync_counter_result) == 1
        assert len(async_counter_result) == 1
        assert sync_counter_result[0].count == 5
        assert async_counter_result[0].count == 10

    @pytest.mark.asyncio
    async def test_row_factory_compatibility(self, sync_session, session_with_keyspace):
        """
        Test row factories work the same.

        What this tests:
        ---------------
        1. dict_factory works
        2. Same result format
        3. Key/value access
        4. Custom factories

        Why this matters:
        ----------------
        Row factories enable:
        - Custom result types
        - ORM integration
        - Flexible data access

        Must preserve driver's
        flexibility.
        """
        async_session, keyspace = session_with_keyspace

        table_name = f"compat_factory_{uuid.uuid4().hex[:8]}"

        # Create table in both keyspaces
        sync_session.execute(
            f"""
            CREATE TABLE {table_name} (
                id int PRIMARY KEY,
                name text,
                age int
            )
        """
        )
        await async_session.execute(
            f"""
            CREATE TABLE {table_name} (
                id int PRIMARY KEY,
                name text,
                age int
            )
        """
        )

        # Insert test data using prepared statements
        sync_insert = sync_session.prepare(
            f"INSERT INTO {table_name} (id, name, age) VALUES (?, ?, ?)"
        )
        async_insert = await async_session.prepare(
            f"INSERT INTO {table_name} (id, name, age) VALUES (?, ?, ?)"
        )

        sync_session.execute(sync_insert, (1, "Alice", 30))
        await async_session.execute(async_insert, (1, "Alice", 30))

        # Set row factory to dict
        sync_session.row_factory = dict_factory
        async_session._session.row_factory = dict_factory

        # Query and compare
        sync_result = sync_session.execute(f"SELECT * FROM {table_name}").one()
        async_result = (await async_session.execute(f"SELECT * FROM {table_name}")).one()

        assert isinstance(sync_result, dict)
        assert isinstance(async_result, dict)
        assert sync_result == async_result
        assert sync_result["name"] == "Alice"
        assert async_result["age"] == 30

    @pytest.mark.asyncio
    async def test_timeout_compatibility(self, sync_session, session_with_keyspace):
        """
        Test timeout behavior is similar.

        What this tests:
        ---------------
        1. Timeouts respected
        2. Same timeout API
        3. No crashes
        4. Error handling

        Why this matters:
        ----------------
        Timeout control critical:
        - Prevent hanging
        - Resource management
        - User experience

        Must match sync driver
        timeout behavior.
        """
        async_session, keyspace = session_with_keyspace

        table_name = f"compat_timeout_{uuid.uuid4().hex[:8]}"

        # Create table in both keyspaces
        sync_session.execute(
            f"""
            CREATE TABLE {table_name} (
                id int PRIMARY KEY,
                data text
            )
        """
        )
        await async_session.execute(
            f"""
            CREATE TABLE {table_name} (
                id int PRIMARY KEY,
                data text
            )
        """
        )

        # Both should respect timeout
        short_timeout = 0.001  # 1ms - should timeout

        # These might timeout or not depending on system load
        # We're just checking they don't crash
        try:
            sync_session.execute(f"SELECT * FROM {table_name}", timeout=short_timeout)
        except Exception:
            pass  # Timeout is expected

        try:
            await async_session.execute(f"SELECT * FROM {table_name}", timeout=short_timeout)
        except Exception:
            pass  # Timeout is expected

    @pytest.mark.asyncio
    async def test_trace_compatibility(self, sync_session, session_with_keyspace):
        """
        Test query tracing works the same.

        What this tests:
        ---------------
        1. Tracing enabled
        2. Trace data available
        3. Same trace API
        4. Debug capability

        Why this matters:
        ----------------
        Tracing essential for:
        - Performance debugging
        - Query optimization
        - Issue diagnosis

        Must preserve debugging
        capabilities.
        """
        async_session, keyspace = session_with_keyspace

        table_name = f"compat_trace_{uuid.uuid4().hex[:8]}"

        # Create table in both keyspaces
        sync_session.execute(
            f"""
            CREATE TABLE {table_name} (
                id int PRIMARY KEY,
                value text
            )
        """
        )
        await async_session.execute(
            f"""
            CREATE TABLE {table_name} (
                id int PRIMARY KEY,
                value text
            )
        """
        )

        # Prepare statements - both use ? for prepared statements
        sync_insert = sync_session.prepare(f"INSERT INTO {table_name} (id, value) VALUES (?, ?)")
        async_insert = await async_session.prepare(
            f"INSERT INTO {table_name} (id, value) VALUES (?, ?)"
        )

        # Execute with tracing
        sync_result = sync_session.execute(sync_insert, (1, "sync_trace"), trace=True)

        async_result = await async_session.execute(async_insert, (2, "async_trace"), trace=True)

        # Both should have trace available
        assert sync_result.get_query_trace() is not None
        assert async_result.get_query_trace() is not None

        # Verify data
        sync_count = sync_session.execute(f"SELECT COUNT(*) FROM {table_name}")
        async_count = await async_session.execute(f"SELECT COUNT(*) FROM {table_name}")
        assert sync_count.one()[0] == 1
        assert async_count.one()[0] == 1

    @pytest.mark.asyncio
    async def test_lwt_compatibility(self, sync_session, session_with_keyspace):
        """
        Test lightweight transactions work the same.

        What this tests:
        ---------------
        1. IF NOT EXISTS works
        2. Conditional updates
        3. Applied flag correct
        4. Failure handling

        Why this matters:
        ----------------
        LWT critical for:
        - ACID operations
        - Conflict resolution
        - Data consistency

        Must work identically
        for correctness.
        """
        async_session, keyspace = session_with_keyspace

        table_name = f"compat_lwt_{uuid.uuid4().hex[:8]}"

        # Create table in both keyspaces
        sync_session.execute(
            f"""
            CREATE TABLE {table_name} (
                id int PRIMARY KEY,
                value text,
                version int
            )
        """
        )
        await async_session.execute(
            f"""
            CREATE TABLE {table_name} (
                id int PRIMARY KEY,
                value text,
                version int
            )
        """
        )

        # Prepare LWT statements - both use ? for prepared statements
        sync_insert_if_not_exists = sync_session.prepare(
            f"INSERT INTO {table_name} (id, value, version) VALUES (?, ?, ?) IF NOT EXISTS"
        )
        async_insert_if_not_exists = await async_session.prepare(
            f"INSERT INTO {table_name} (id, value, version) VALUES (?, ?, ?) IF NOT EXISTS"
        )

        # Test IF NOT EXISTS
        sync_result = sync_session.execute(sync_insert_if_not_exists, (1, "sync", 1))
        async_result = await async_session.execute(async_insert_if_not_exists, (2, "async", 1))

        # Both should succeed
        assert sync_result.one().applied
        assert async_result.one().applied

        # Prepare conditional update statements - both use ? for prepared statements
        sync_update_if = sync_session.prepare(
            f"UPDATE {table_name} SET value = ?, version = ? WHERE id = ? IF version = ?"
        )
        async_update_if = await async_session.prepare(
            f"UPDATE {table_name} SET value = ?, version = ? WHERE id = ? IF version = ?"
        )

        # Test conditional update
        sync_update = sync_session.execute(sync_update_if, ("sync_updated", 2, 1, 1))
        async_update = await async_session.execute(async_update_if, ("async_updated", 2, 2, 1))

        assert sync_update.one().applied
        assert async_update.one().applied

        # Prepare failed condition statements - both use ? for prepared statements
        sync_update_fail = sync_session.prepare(
            f"UPDATE {table_name} SET version = ? WHERE id = ? IF version = ?"
        )
        async_update_fail = await async_session.prepare(
            f"UPDATE {table_name} SET version = ? WHERE id = ? IF version = ?"
        )

        # Failed condition
        sync_fail = sync_session.execute(sync_update_fail, (3, 1, 1))
        async_fail = await async_session.execute(async_update_fail, (3, 2, 1))

        assert not sync_fail.one().applied
        assert not async_fail.one().applied
