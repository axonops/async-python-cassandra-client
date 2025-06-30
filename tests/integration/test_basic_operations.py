"""
Integration tests for basic Cassandra operations.

Demonstrates proper context manager usage and error handling patterns
for production-ready code.
"""

import asyncio
import uuid

import pytest
from cassandra import InvalidRequest
from cassandra.query import BatchStatement, BatchType
from test_utils import generate_unique_table


@pytest.mark.asyncio
@pytest.mark.integration
class TestBasicOperations:
    """Test basic CRUD operations with real Cassandra."""

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
            try:
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

            except Exception as e:
                pytest.fail(f"Failed to create or query table: {e}")
            finally:
                # Cleanup table
                await session.execute(f"DROP TABLE IF EXISTS {table_name}")
        finally:
            await session.close()

    async def test_insert_and_select(self, cassandra_session):
        """Test inserting and selecting data with proper error handling."""
        # Use the unique users table created for this test
        users_table = cassandra_session._test_users_table

        try:
            user_id = uuid.uuid4()

            # Prepare statements with the unique table name
            insert_stmt = await cassandra_session.prepare(
                f"INSERT INTO {users_table} (id, name, email, age) VALUES (?, ?, ?, ?)"
            )
            select_stmt = await cassandra_session.prepare(
                f"SELECT * FROM {users_table} WHERE id = ?"
            )

            # Insert data with error handling
            try:
                await cassandra_session.execute(
                    insert_stmt, [user_id, "John Doe", "john@example.com", 30]
                )
            except Exception as e:
                pytest.fail(f"Failed to insert data: {e}")

            # Select data with error handling
            try:
                result = await cassandra_session.execute(select_stmt, [user_id])

                rows = result.all()
                assert len(rows) == 1

                row = rows[0]
                assert row.id == user_id
                assert row.name == "John Doe"
                assert row.email == "john@example.com"
                assert row.age == 30
            except Exception as e:
                pytest.fail(f"Failed to select data: {e}")

        except Exception as e:
            pytest.fail(f"Test setup failed: {e}")

    async def test_prepared_statements(self, cassandra_session):
        """Test using prepared statements with proper patterns."""
        # Use the unique users table created for this test
        users_table = cassandra_session._test_users_table

        try:
            # Prepare insert statement
            insert_stmt = await cassandra_session.prepare(
                f"""
                INSERT INTO {users_table} (id, name, email, age)
                VALUES (?, ?, ?, ?)
                """
            )

            # Prepare select statement
            select_stmt = await cassandra_session.prepare(
                f"SELECT * FROM {users_table} WHERE id = ?"
            )

            # Insert multiple users with error handling
            users = [
                (uuid.uuid4(), "Alice", "alice@example.com", 25),
                (uuid.uuid4(), "Bob", "bob@example.com", 35),
                (uuid.uuid4(), "Charlie", "charlie@example.com", 45),
            ]

            for user in users:
                try:
                    await cassandra_session.execute(insert_stmt, user)
                except Exception as e:
                    pytest.fail(f"Failed to insert user {user[1]}: {e}")

            # Select each user with error handling
            for user_id, name, email, age in users:
                try:
                    result = await cassandra_session.execute(select_stmt, [user_id])
                    row = result.one()

                    assert row.id == user_id
                    assert row.name == name
                    assert row.email == email
                    assert row.age == age
                except Exception as e:
                    pytest.fail(f"Failed to verify user {name}: {e}")

        except Exception as e:
            pytest.fail(f"Failed to prepare statements: {e}")

    async def test_batch_operations(self, cassandra_session):
        """Test batch insert operations with proper error handling."""
        # Use the unique users table created for this test
        users_table = cassandra_session._test_users_table

        try:
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
                batch.add(insert_stmt, [user_id, f"User{i}", f"user{i}@example.com", 20 + i])

            # Execute batch with error handling
            try:
                await cassandra_session.execute_batch(batch)
            except Exception as e:
                pytest.fail(f"Failed to execute batch: {e}")

            # Verify all users were inserted
            select_stmt = await cassandra_session.prepare(
                f"SELECT * FROM {users_table} WHERE id = ?"
            )

            for i, user_id in enumerate(user_ids):
                try:
                    result = await cassandra_session.execute(select_stmt, [user_id])
                    row = result.one()
                    assert row.name == f"User{i}"
                except Exception as e:
                    pytest.fail(f"Failed to verify batch insert for User{i}: {e}")

        except Exception as e:
            pytest.fail(f"Batch operation setup failed: {e}")

    async def test_async_iteration(self, cassandra_session):
        """Test async iteration over results with proper patterns."""
        # Use the unique users table created for this test
        users_table = cassandra_session._test_users_table

        try:
            # Insert test data
            insert_stmt = await cassandra_session.prepare(
                f"""
                INSERT INTO {users_table} (id, name, email, age)
                VALUES (?, ?, ?, ?)
                """
            )

            # Insert users with error handling
            for i in range(10):
                try:
                    await cassandra_session.execute(
                        insert_stmt, [uuid.uuid4(), f"User{i}", f"user{i}@example.com", 20 + i]
                    )
                except Exception as e:
                    pytest.fail(f"Failed to insert User{i}: {e}")

            # Select all users
            select_all_stmt = await cassandra_session.prepare(f"SELECT * FROM {users_table}")

            try:
                result = await cassandra_session.execute(select_all_stmt)

                # Iterate asynchronously with error handling
                count = 0
                async for row in result:
                    assert hasattr(row, "name")
                    assert row.name.startswith("User")
                    count += 1

                # We should have at least 10 users (may have more from other tests)
                assert count >= 10
            except Exception as e:
                pytest.fail(f"Failed to iterate over results: {e}")

        except Exception as e:
            pytest.fail(f"Test setup failed: {e}")

    async def test_error_handling(self, cassandra_session):
        """Test error handling for invalid queries."""
        # Test invalid table query
        with pytest.raises(InvalidRequest) as exc_info:
            await cassandra_session.execute("SELECT * FROM non_existent_table")
        assert "does not exist" in str(exc_info.value) or "unconfigured table" in str(
            exc_info.value
        )

        # Test invalid keyspace - should fail
        with pytest.raises(InvalidRequest) as exc_info:
            await cassandra_session.set_keyspace("non_existent_keyspace")
        assert "Keyspace" in str(exc_info.value) or "does not exist" in str(exc_info.value)

        # Test syntax error
        with pytest.raises(Exception) as exc_info:
            await cassandra_session.execute("INVALID SQL QUERY")
        # Could be SyntaxException or InvalidRequest depending on driver version
        assert "Syntax" in str(exc_info.value) or "Invalid" in str(exc_info.value)

    async def test_concurrent_queries(self, cassandra_session):
        """Test executing multiple queries concurrently with proper patterns."""
        # Get the unique table name
        users_table = cassandra_session._test_users_table

        try:
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
                try:
                    await cassandra_session.execute(
                        insert_stmt, [user_id, f"Concurrent{i}", f"concurrent{i}@example.com", 30]
                    )
                    return user_id
                except Exception as e:
                    pytest.fail(f"Failed to insert concurrent user {i}: {e}")

            # Insert 20 users concurrently
            try:
                user_ids = await asyncio.gather(*[insert_user(i) for i in range(20)])
            except Exception as e:
                pytest.fail(f"Failed concurrent insertion: {e}")

            # Verify all were inserted
            select_stmt = await cassandra_session.prepare(
                f"SELECT * FROM {users_table} WHERE id = ?"
            )

            for i, user_id in enumerate(user_ids):
                try:
                    result = await cassandra_session.execute(select_stmt, [user_id])
                    row = result.one()
                    assert row is not None
                    assert row.name == f"Concurrent{i}"
                except Exception as e:
                    pytest.fail(f"Failed to verify concurrent user {i}: {e}")

        except Exception as e:
            pytest.fail(f"Concurrent query test setup failed: {e}")

    async def test_update_and_delete(self, cassandra_session):
        """Test UPDATE and DELETE operations with proper error handling."""
        users_table = cassandra_session._test_users_table

        try:
            user_id = uuid.uuid4()

            # Insert initial data
            insert_stmt = await cassandra_session.prepare(
                f"INSERT INTO {users_table} (id, name, email, age) VALUES (?, ?, ?, ?)"
            )
            await cassandra_session.execute(
                insert_stmt, [user_id, "Update Test", "update@example.com", 25]
            )

            # Update data
            update_stmt = await cassandra_session.prepare(
                f"UPDATE {users_table} SET age = ? WHERE id = ?"
            )

            try:
                await cassandra_session.execute(update_stmt, [30, user_id])
            except Exception as e:
                pytest.fail(f"Failed to update user: {e}")

            # Verify update
            select_stmt = await cassandra_session.prepare(
                f"SELECT age FROM {users_table} WHERE id = ?"
            )
            result = await cassandra_session.execute(select_stmt, [user_id])
            assert result.one().age == 30

            # Delete data
            delete_stmt = await cassandra_session.prepare(f"DELETE FROM {users_table} WHERE id = ?")

            try:
                await cassandra_session.execute(delete_stmt, [user_id])
            except Exception as e:
                pytest.fail(f"Failed to delete user: {e}")

            # Verify deletion
            result = await cassandra_session.execute(select_stmt, [user_id])
            assert result.one() is None

        except Exception as e:
            pytest.fail(f"Update/delete test failed: {e}")
