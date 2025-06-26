"""
Integration tests for basic Cassandra operations.
"""

import uuid

import pytest
from test_utils import generate_unique_table


class TestBasicOperations:
    """Test basic CRUD operations with real Cassandra."""

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_connection_and_keyspace(
        self, cassandra_cluster, shared_keyspace_setup, pytestconfig
    ):
        """Test connecting to Cassandra and using shared keyspace."""
        session = await cassandra_cluster.connect()

        try:
            # Use the shared keyspace
            keyspace = pytestconfig.shared_test_keyspace
            await session.set_keyspace(keyspace)
            assert session.keyspace == keyspace

            # Create a test table in the shared keyspace
            table_name = generate_unique_table("test_conn")
            await session.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id INT PRIMARY KEY,
                    data TEXT
                )
            """
            )

            # Verify table exists
            await session.execute(f"SELECT * FROM {table_name} LIMIT 1")

            # Cleanup table
            await session.execute(f"DROP TABLE IF EXISTS {table_name}")
        finally:
            await session.close()

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_insert_and_select(self, cassandra_session):
        """Test inserting and selecting data."""
        # Use the unique users table created for this test
        users_table = cassandra_session._test_users_table

        user_id = uuid.uuid4()

        # Prepare statements with the unique table name
        insert_stmt = await cassandra_session.prepare(
            f"INSERT INTO {users_table} (id, name, email, age) VALUES (?, ?, ?, ?)"
        )
        select_stmt = await cassandra_session.prepare(f"SELECT * FROM {users_table} WHERE id = ?")

        # Insert data
        await cassandra_session.execute(insert_stmt, [user_id, "John Doe", "john@example.com", 30])

        # Select data
        result = await cassandra_session.execute(select_stmt, [user_id])

        rows = result.all()
        assert len(rows) == 1

        row = rows[0]
        assert row.id == user_id
        assert row.name == "John Doe"
        assert row.email == "john@example.com"
        assert row.age == 30

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_prepared_statements(self, cassandra_session):
        """Test using prepared statements."""
        # Use the unique users table created for this test
        users_table = cassandra_session._test_users_table

        # Prepare insert statement
        insert_stmt = await cassandra_session.prepare(
            f"""
            INSERT INTO {users_table} (id, name, email, age)
            VALUES (?, ?, ?, ?)
            """
        )

        # Prepare select statement
        select_stmt = await cassandra_session.prepare(f"SELECT * FROM {users_table} WHERE id = ?")

        # Insert multiple users
        users = [
            (uuid.uuid4(), "Alice", "alice@example.com", 25),
            (uuid.uuid4(), "Bob", "bob@example.com", 35),
            (uuid.uuid4(), "Charlie", "charlie@example.com", 45),
        ]

        for user in users:
            await cassandra_session.execute(insert_stmt, user)

        # Select each user
        for user_id, name, email, age in users:
            result = await cassandra_session.execute(select_stmt, [user_id])
            row = result.one()

            assert row.id == user_id
            assert row.name == name
            assert row.email == email
            assert row.age == age

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_batch_operations(self, cassandra_session):
        """Test batch insert operations."""
        from cassandra.query import BatchStatement, BatchType

        # Use the unique users table created for this test
        users_table = cassandra_session._test_users_table

        batch = BatchStatement(batch_type=BatchType.LOGGED)

        # Prepare statement
        insert_stmt = await cassandra_session.prepare(
            f"""
            INSERT INTO {users_table} (id, name, email, age)
            VALUES (?, ?, ?, ?)
            """
        )

        # Add multiple statements to batch
        user_ids = []
        for i in range(5):
            user_id = uuid.uuid4()
            user_ids.append(user_id)
            batch.add(insert_stmt, (user_id, f"User{i}", f"user{i}@example.com", 20 + i))

        # Execute batch
        await cassandra_session.execute_batch(batch)

        # Verify all users were inserted
        select_stmt = await cassandra_session.prepare(f"SELECT * FROM {users_table} WHERE id = ?")
        for i, user_id in enumerate(user_ids):
            result = await cassandra_session.execute(select_stmt, [user_id])
            row = result.one()
            assert row.name == f"User{i}"

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_async_iteration(self, cassandra_session):
        """Test async iteration over results."""
        # Use the unique users table created for this test
        users_table = cassandra_session._test_users_table

        # Insert test data
        insert_stmt = await cassandra_session.prepare(
            f"""
            INSERT INTO {users_table} (id, name, email, age)
            VALUES (?, ?, ?, ?)
            """
        )

        for i in range(10):
            await cassandra_session.execute(
                insert_stmt, (uuid.uuid4(), f"User{i}", f"user{i}@example.com", 20 + i)
            )

        # Select all users
        result = await cassandra_session.execute(f"SELECT * FROM {users_table}")

        # Iterate asynchronously
        count = 0
        async for row in result:
            assert hasattr(row, "name")
            assert row.name.startswith("User")
            count += 1

        assert count == 10  # Exactly our inserted users (isolated table)

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_error_handling(self, cassandra_session):
        """Test error handling for invalid queries."""
        from cassandra import InvalidRequest

        # Test invalid query
        with pytest.raises(InvalidRequest) as exc_info:
            await cassandra_session.execute("SELECT * FROM non_existent_table")

        assert "does not exist" in str(exc_info.value)

        # Test invalid keyspace
        with pytest.raises(InvalidRequest):
            await cassandra_session.set_keyspace("non_existent_keyspace")

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_concurrent_queries(self, cassandra_session):
        """Test executing multiple queries concurrently."""
        import asyncio

        # Get the unique table name
        users_table = cassandra_session._test_users_table

        # Prepare statement
        insert_stmt = await cassandra_session.prepare(
            f"""
            INSERT INTO {users_table} (id, name, email, age)
            VALUES (?, ?, ?, ?)
            """
        )

        # Execute multiple queries concurrently
        async def insert_user(i: int):
            user_id = uuid.uuid4()
            await cassandra_session.execute(
                insert_stmt, (user_id, f"Concurrent{i}", f"concurrent{i}@example.com", 30)
            )
            return user_id

        # Insert 20 users concurrently
        user_ids = await asyncio.gather(*[insert_user(i) for i in range(20)])

        # Verify all were inserted
        select_stmt = await cassandra_session.prepare(f"SELECT * FROM {users_table} WHERE id = ?")
        for user_id in user_ids:
            result = await cassandra_session.execute(select_stmt, [user_id])
            assert result.one() is not None
