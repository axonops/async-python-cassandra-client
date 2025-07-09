"""
Integration tests for SELECT query operations.

This file focuses on advanced SELECT scenarios: consistency levels, large result sets,
concurrent operations, and special query features. Basic SELECT operations have been
moved to test_crud_operations.py.
"""

import asyncio
import uuid

import pytest
from cassandra.query import SimpleStatement


@pytest.mark.integration
class TestSelectOperations:
    """Test advanced SELECT query operations with real Cassandra."""

    @pytest.mark.asyncio
    async def test_select_with_large_result_set(self, cassandra_session):
        """
        Test SELECT with large result sets to verify paging and retries work.

        What this tests:
        ---------------
        1. Large result sets (1000+ rows)
        2. Automatic paging with fetch_size
        3. Memory-efficient iteration
        4. ALLOW FILTERING queries

        Why this matters:
        ----------------
        Large result sets require:
        - Paging to avoid OOM
        - Streaming for efficiency
        - Proper retry handling

        Critical for analytics and
        bulk data processing.
        """
        # Get the unique table name
        users_table = cassandra_session._test_users_table

        # Insert many rows
        # Prepare statement once
        insert_stmt = await cassandra_session.prepare(
            f"INSERT INTO {users_table} (id, name, email, age) VALUES (?, ?, ?, ?)"
        )

        insert_tasks = []
        for i in range(1000):
            task = cassandra_session.execute(
                insert_stmt,
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
    async def test_select_with_limit_and_ordering(self, cassandra_session):
        """
        Test SELECT with LIMIT and ordering to ensure retries preserve results.

        What this tests:
        ---------------
        1. LIMIT clause respected
        2. Clustering order preserved
        3. Time series queries
        4. Result consistency

        Why this matters:
        ----------------
        Ordered queries critical for:
        - Time series data
        - Top-N queries
        - Pagination

        Order must be consistent
        across retries.
        """
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

        # Prepare insert statement
        insert_stmt = await cassandra_session.prepare(
            "INSERT INTO time_series (partition_key, timestamp, value) VALUES (?, ?, ?)"
        )

        for i in range(100):
            await cassandra_session.execute(
                insert_stmt,
                [partition_key, base_time + i * 1000, float(i)],
            )

        # Query with limit
        select_stmt = await cassandra_session.prepare(
            "SELECT * FROM time_series WHERE partition_key = ? LIMIT 10"
        )
        result = await cassandra_session.execute(select_stmt, [partition_key])

        rows = []
        async for row in result:
            rows.append(row)

        # Should get exactly 10 rows in descending order
        assert len(rows) == 10
        # Verify descending order (latest timestamps first)
        for i in range(1, len(rows)):
            assert rows[i - 1].timestamp > rows[i].timestamp
