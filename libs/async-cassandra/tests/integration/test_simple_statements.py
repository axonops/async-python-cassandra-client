"""
Integration tests for SimpleStatement functionality.

This test module specifically tests SimpleStatement usage, which is generally
discouraged in favor of prepared statements but may be needed for:
- Setting consistency levels
- Legacy code compatibility
- Dynamic queries that can't be prepared
"""

import uuid

import pytest
from cassandra.query import SimpleStatement


@pytest.mark.integration
class TestSimpleStatements:
    """Test SimpleStatement functionality with real Cassandra."""

    @pytest.mark.asyncio
    async def test_simple_statement_basic_usage(self, cassandra_session):
        """
        Test basic SimpleStatement usage with parameters.

        What this tests:
        ---------------
        1. SimpleStatement creation
        2. Parameter binding with %s
        3. Query execution
        4. Result retrieval

        Why this matters:
        ----------------
        SimpleStatement needed for:
        - Legacy code compatibility
        - Dynamic queries
        - One-off statements

        Must work but prepared
        statements preferred.
        """
        # Get the unique table name
        users_table = cassandra_session._test_users_table

        # Create a SimpleStatement with parameters
        user_id = uuid.uuid4()
        insert_stmt = SimpleStatement(
            f"INSERT INTO {users_table} (id, name, email, age) VALUES (%s, %s, %s, %s)"
        )

        # Execute with parameters
        await cassandra_session.execute(insert_stmt, [user_id, "John Doe", "john@example.com", 30])

        # Verify with SELECT
        select_stmt = SimpleStatement(f"SELECT * FROM {users_table} WHERE id = %s")
        result = await cassandra_session.execute(select_stmt, [user_id])

        row = result.one()
        assert row is not None
        assert row.name == "John Doe"
        assert row.email == "john@example.com"
        assert row.age == 30

    @pytest.mark.asyncio
    async def test_simple_statement_without_parameters(self, cassandra_session):
        """
        Test SimpleStatement without parameters for queries.

        What this tests:
        ---------------
        1. Parameterless queries
        2. Fetch size configuration
        3. Result pagination
        4. Multiple row handling

        Why this matters:
        ----------------
        Some queries need no params:
        - Table scans
        - Aggregations
        - DDL operations

        SimpleStatement supports
        all query options.
        """
        # Get the unique table name
        users_table = cassandra_session._test_users_table

        # Insert some test data using prepared statement
        insert_prepared = await cassandra_session.prepare(
            f"INSERT INTO {users_table} (id, name, email, age) VALUES (?, ?, ?, ?)"
        )

        for i in range(5):
            await cassandra_session.execute(
                insert_prepared, [uuid.uuid4(), f"User {i}", f"user{i}@example.com", 20 + i]
            )

        # Use SimpleStatement for a parameter-less query
        select_all = SimpleStatement(
            f"SELECT * FROM {users_table}", fetch_size=2  # Test pagination
        )

        result = await cassandra_session.execute(select_all)
        rows = list(result)

        # Should have at least 5 rows
        assert len(rows) >= 5

    @pytest.mark.asyncio
    async def test_simple_statement_vs_prepared_performance(self, cassandra_session):
        """
        Compare SimpleStatement vs PreparedStatement (prepared should be faster).

        What this tests:
        ---------------
        1. Performance comparison
        2. Both statement types work
        3. Timing measurements
        4. Prepared advantages

        Why this matters:
        ----------------
        Shows why prepared better:
        - Query plan caching
        - Type validation
        - Network efficiency

        Educates on best
        practices.
        """
        import time

        # Get the unique table name
        users_table = cassandra_session._test_users_table

        # Time SimpleStatement execution
        simple_stmt = SimpleStatement(
            f"INSERT INTO {users_table} (id, name, email, age) VALUES (%s, %s, %s, %s)"
        )

        simple_start = time.perf_counter()
        for i in range(10):
            await cassandra_session.execute(
                simple_stmt, [uuid.uuid4(), f"Simple {i}", f"simple{i}@example.com", i]
            )
        simple_time = time.perf_counter() - simple_start

        # Time PreparedStatement execution
        prepared_stmt = await cassandra_session.prepare(
            f"INSERT INTO {users_table} (id, name, email, age) VALUES (?, ?, ?, ?)"
        )

        prepared_start = time.perf_counter()
        for i in range(10):
            await cassandra_session.execute(
                prepared_stmt, [uuid.uuid4(), f"Prepared {i}", f"prepared{i}@example.com", i]
            )
        prepared_time = time.perf_counter() - prepared_start

        # Log the times for debugging
        print(f"SimpleStatement time: {simple_time:.3f}s")
        print(f"PreparedStatement time: {prepared_time:.3f}s")

        # PreparedStatement should generally be faster, but we won't assert
        # this as it can vary based on network conditions

    @pytest.mark.asyncio
    async def test_simple_statement_with_custom_payload(self, cassandra_session):
        """
        Test SimpleStatement with custom payload.

        What this tests:
        ---------------
        1. Custom payload support
        2. Bytes payload format
        3. Payload passed through
        4. Query still works

        Why this matters:
        ----------------
        Custom payloads enable:
        - Request tracing
        - Application metadata
        - Cross-system correlation

        Advanced feature for
        observability.
        """
        # Get the unique table name
        users_table = cassandra_session._test_users_table

        # Create SimpleStatement with custom payload
        user_id = uuid.uuid4()
        stmt = SimpleStatement(
            f"INSERT INTO {users_table} (id, name, email, age) VALUES (%s, %s, %s, %s)"
        )

        # Execute with custom payload (payload is passed through to Cassandra)
        # Custom payload values must be bytes
        custom_payload = {b"application": b"test_suite", b"version": b"1.0"}
        await cassandra_session.execute(
            stmt,
            [user_id, "Payload User", "payload@example.com", 40],
            custom_payload=custom_payload,
        )

        # Verify insert worked
        result = await cassandra_session.execute(
            f"SELECT * FROM {users_table} WHERE id = %s", [user_id]
        )
        assert result.one() is not None

    @pytest.mark.asyncio
    async def test_simple_statement_batch_not_recommended(self, cassandra_session):
        """
        Test that SimpleStatements work in batches but prepared is preferred.

        What this tests:
        ---------------
        1. SimpleStatement in batches
        2. Batch execution works
        3. Not recommended pattern
        4. Compatibility maintained

        Why this matters:
        ----------------
        Shows anti-pattern:
        - Poor performance
        - No query plan reuse
        - Network inefficient

        Works but educates on
        better approaches.
        """
        from cassandra.query import BatchStatement, BatchType

        # Get the unique table name
        users_table = cassandra_session._test_users_table

        batch = BatchStatement(batch_type=BatchType.LOGGED)

        # Add SimpleStatements to batch (not recommended but should work)
        for i in range(3):
            stmt = SimpleStatement(
                f"INSERT INTO {users_table} (id, name, email, age) VALUES (%s, %s, %s, %s)"
            )
            batch.add(stmt, [uuid.uuid4(), f"Batch {i}", f"batch{i}@example.com", i])

        # Execute batch
        await cassandra_session.execute(batch)

        # Verify inserts
        result = await cassandra_session.execute(f"SELECT COUNT(*) FROM {users_table}")
        assert result.one()[0] >= 3
