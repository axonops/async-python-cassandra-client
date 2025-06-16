"""
Integration tests for basic Cassandra operations.
"""

import uuid

import pytest

from async_cassandra import AsyncCluster


class TestBasicOperations:
    """Test basic CRUD operations with real Cassandra."""

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_connection_and_keyspace(self):
        """Test connecting to Cassandra and creating keyspace."""
        cluster = AsyncCluster(contact_points=["localhost"])

        async with cluster:
            session = await cluster.connect()

            # Create keyspace
            await session.execute(
                """
                CREATE KEYSPACE IF NOT EXISTS test_connection
                WITH REPLICATION = {
                    'class': 'SimpleStrategy',
                    'replication_factor': 1
                }
            """
            )

            # Use keyspace
            await session.set_keyspace("test_connection")
            assert session.keyspace == "test_connection"

            # Cleanup
            await session.execute("DROP KEYSPACE test_connection")
            await session.close()

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_insert_and_select(self, cassandra_session):
        """Test inserting and selecting data."""
        # Clean up table
        await cassandra_session.execute("TRUNCATE users")

        user_id = uuid.uuid4()

        # Prepare statements
        insert_stmt = await cassandra_session.prepare(
            "INSERT INTO users (id, name, email, age) VALUES (?, ?, ?, ?)"
        )
        select_stmt = await cassandra_session.prepare("SELECT * FROM users WHERE id = ?")

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
        # Clean up table
        await cassandra_session.execute("TRUNCATE users")

        # Prepare insert statement
        insert_stmt = await cassandra_session.prepare(
            """
            INSERT INTO users (id, name, email, age)
            VALUES (?, ?, ?, ?)
            """
        )

        # Prepare select statement
        select_stmt = await cassandra_session.prepare("SELECT * FROM users WHERE id = ?")

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

        batch = BatchStatement(batch_type=BatchType.LOGGED)

        # Prepare statement
        insert_stmt = await cassandra_session.prepare(
            """
            INSERT INTO users (id, name, email, age)
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
        select_stmt = await cassandra_session.prepare("SELECT * FROM users WHERE id = ?")
        for i, user_id in enumerate(user_ids):
            result = await cassandra_session.execute(select_stmt, [user_id])
            row = result.one()
            assert row.name == f"User{i}"

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_async_iteration(self, cassandra_session):
        """Test async iteration over results."""
        # Clean up table
        await cassandra_session.execute("TRUNCATE users")

        # Insert test data
        insert_stmt = await cassandra_session.prepare(
            """
            INSERT INTO users (id, name, email, age)
            VALUES (?, ?, ?, ?)
            """
        )

        for i in range(10):
            await cassandra_session.execute(
                insert_stmt, (uuid.uuid4(), f"User{i}", f"user{i}@example.com", 20 + i)
            )

        # Select all users
        result = await cassandra_session.execute("SELECT * FROM users")

        # Iterate asynchronously
        count = 0
        async for row in result:
            assert hasattr(row, "name")
            assert row.name.startswith("User")
            count += 1

        assert count >= 10  # At least our inserted users

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

        # Prepare statement
        insert_stmt = await cassandra_session.prepare(
            """
            INSERT INTO users (id, name, email, age)
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
        select_stmt = await cassandra_session.prepare("SELECT * FROM users WHERE id = ?")
        for user_id in user_ids:
            result = await cassandra_session.execute(select_stmt, [user_id])
            assert result.one() is not None
