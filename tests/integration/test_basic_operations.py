"""
Integration tests for basic Cassandra operations.

This file focuses on connection management, error handling, async patterns,
and concurrent operations. Basic CRUD operations have been moved to
test_crud_operations.py.
"""

import asyncio
import uuid

import pytest
from cassandra import InvalidRequest
from test_utils import generate_unique_table


@pytest.mark.asyncio
@pytest.mark.integration
class TestBasicOperations:
    """Test connection, error handling, and async patterns with real Cassandra."""

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
