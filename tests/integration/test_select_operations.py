"""
Integration tests for SELECT query operations.
Tests various SELECT scenarios with real Cassandra including consistency levels,
large result sets, prepared statements, and concurrent operations.
"""

import asyncio
import uuid

import pytest
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement


@pytest.mark.integration
class TestSelectOperations:
    """Test SELECT query operations in various scenarios with real Cassandra."""

    @pytest.mark.asyncio
    async def test_select_with_consistency_levels(self, cassandra_session):
        """Test SELECT queries with different consistency levels."""
        # Get the unique table name
        users_table = cassandra_session._test_users_table

        # Insert test data
        user_id = uuid.uuid4()
        await cassandra_session.execute(
            f"INSERT INTO {users_table} (id, name, email, age) VALUES (%s, %s, %s, %s)",
            [user_id, "Test User", "test@example.com", 25],
        )

        # Test with different consistency levels
        consistency_levels = [
            ConsistencyLevel.ONE,
            ConsistencyLevel.LOCAL_ONE,
            ConsistencyLevel.LOCAL_QUORUM,
        ]

        for cl in consistency_levels:
            statement = SimpleStatement(
                f"SELECT * FROM {users_table} WHERE id = %s",
                consistency_level=cl,
            )
            result = await cassandra_session.execute(statement, [user_id])
            rows = []
            async for row in result:
                rows.append(row)

            assert len(rows) == 1
            assert rows[0].name == "Test User"

    @pytest.mark.asyncio
    async def test_select_with_large_result_set(self, cassandra_session):
        """Test SELECT with large result sets to verify paging and retries work."""
        # Get the unique table name
        users_table = cassandra_session._test_users_table

        # Insert many rows
        insert_tasks = []
        for i in range(1000):
            task = cassandra_session.execute(
                f"INSERT INTO {users_table} (id, name, email, age) VALUES (%s, %s, %s, %s)",
                [uuid.uuid4(), f"User {i}", f"user{i}@example.com", 20 + (i % 50)],
            )
            insert_tasks.append(task)

        # Execute in batches to avoid overwhelming
        for i in range(0, len(insert_tasks), 100):
            await asyncio.gather(*insert_tasks[i : i + 100])

        # Query with small fetch size to test paging
        statement = SimpleStatement(
            f"SELECT * FROM {users_table} WHERE age >= 20 AND age <= 30 ALLOW FILTERING",
            fetch_size=50,
        )
        result = await cassandra_session.execute(statement)

        count = 0
        async for row in result:
            assert 20 <= row.age <= 30
            count += 1

        # Should have retrieved multiple pages
        assert count > 50

    @pytest.mark.asyncio
    async def test_select_with_prepared_statements(self, cassandra_session):
        """Test SELECT retry behavior with prepared statements."""
        # Get the unique table name
        users_table = cassandra_session._test_users_table

        # Prepare the statement
        select_stmt = await cassandra_session.prepare(f"SELECT * FROM {users_table} WHERE id = ?")

        # Insert and query multiple times
        for i in range(10):
            user_id = uuid.uuid4()
            # Insert
            await cassandra_session.execute(
                f"INSERT INTO {users_table} (id, name, email, age) VALUES (%s, %s, %s, %s)",
                [user_id, f"User {i}", f"user{i}@test.com", 25 + i],
            )

            # Query with prepared statement
            result = await cassandra_session.execute(select_stmt, [user_id])
            rows = []
            async for row in result:
                rows.append(row)

            assert len(rows) == 1
            assert rows[0].name == f"User {i}"

    @pytest.mark.asyncio
    async def test_concurrent_selects(self, cassandra_session):
        """Test concurrent SELECT queries to verify retry mechanism under load."""
        # Get the unique table name
        users_table = cassandra_session._test_users_table

        # Insert test data
        user_ids = []
        for i in range(100):
            user_id = uuid.uuid4()
            user_ids.append(user_id)
            await cassandra_session.execute(
                f"INSERT INTO {users_table} (id, name, email, age) VALUES (%s, %s, %s, %s)",
                [user_id, f"Concurrent User {i}", f"concurrent{i}@test.com", 30],
            )

        # Execute many concurrent selects
        async def select_user(user_id):
            result = await cassandra_session.execute(
                f"SELECT * FROM {users_table} WHERE id = %s", [user_id]
            )
            rows = []
            async for row in result:
                rows.append(row)
            return rows[0] if rows else None

        # Run concurrent queries
        results = await asyncio.gather(
            *[select_user(uid) for uid in user_ids], return_exceptions=True
        )

        # Verify all succeeded
        successful = [r for r in results if not isinstance(r, Exception)]
        assert len(successful) == 100

        # Check for any exceptions (there shouldn't be any)
        exceptions = [r for r in results if isinstance(r, Exception)]
        assert len(exceptions) == 0

    @pytest.mark.asyncio
    async def test_select_non_existent_data(self, cassandra_session):
        """Test SELECT behavior when querying non-existent data."""
        # Get the unique table name
        users_table = cassandra_session._test_users_table

        # Query for non-existent user
        result = await cassandra_session.execute(
            f"SELECT * FROM {users_table} WHERE id = %s", [uuid.uuid4()]
        )

        rows = []
        async for row in result:
            rows.append(row)

        # Should return empty result set without errors
        assert len(rows) == 0

    @pytest.mark.asyncio
    async def test_select_with_limit_and_ordering(self, cassandra_session):
        """Test SELECT with LIMIT and ordering to ensure retries preserve results."""
        # Create a table with clustering columns for ordering
        await cassandra_session.execute("DROP TABLE IF EXISTS time_series")
        await cassandra_session.execute(
            """
            CREATE TABLE time_series (
                partition_key UUID,
                timestamp TIMESTAMP,
                value DOUBLE,
                PRIMARY KEY (partition_key, timestamp)
            ) WITH CLUSTERING ORDER BY (timestamp DESC)
            """
        )

        # Insert time series data
        partition_key = uuid.uuid4()
        base_time = 1700000000000  # milliseconds

        for i in range(100):
            await cassandra_session.execute(
                "INSERT INTO time_series (partition_key, timestamp, value) VALUES (%s, %s, %s)",
                [partition_key, base_time + i * 1000, float(i)],
            )

        # Query with limit
        result = await cassandra_session.execute(
            "SELECT * FROM time_series WHERE partition_key = %s LIMIT 10",
            [partition_key],
        )

        rows = []
        async for row in result:
            rows.append(row)

        # Should get exactly 10 rows in descending order
        assert len(rows) == 10
        # Verify descending order (latest timestamps first)
        for i in range(1, len(rows)):
            assert rows[i - 1].timestamp > rows[i].timestamp
