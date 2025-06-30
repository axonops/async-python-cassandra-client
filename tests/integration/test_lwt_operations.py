"""
Integration tests for Lightweight Transaction (LWT) operations.

These tests verify LWT functionality with a real Cassandra instance:
- IF NOT EXISTS conditions
- IF EXISTS conditions
- Conditional updates
- Race conditions and concurrent access

Demonstrates proper context manager usage and error handling patterns.
"""

import asyncio
import uuid

import pytest


@pytest.mark.asyncio
@pytest.mark.integration
class TestLWTIntegration:
    """Test LWT operations with real Cassandra."""

    async def test_insert_if_not_exists_success(self, cassandra_session):
        """Test successful INSERT IF NOT EXISTS."""
        session = cassandra_session

        try:
            # Create test table
            await session.execute(
                """
                CREATE TABLE IF NOT EXISTS lwt_users (
                    id UUID PRIMARY KEY,
                    username TEXT,
                    email TEXT,
                    balance INT,
                    status TEXT
                )
                """
            )

            # Clean up table
            await session.execute("TRUNCATE lwt_users")

            user_id = uuid.uuid4()

            # Insert with IF NOT EXISTS
            insert_stmt = await session.prepare(
                """
                INSERT INTO lwt_users (id, username, email, balance)
                VALUES (?, ?, ?, ?)
                IF NOT EXISTS
                """
            )

            try:
                result = await session.execute(
                    insert_stmt, [user_id, "alice", "alice@example.com", 100]
                )

                # Should succeed
                assert len(result.rows) == 1
                assert result.rows[0][0] is True  # [applied] column
            except Exception as e:
                pytest.fail(f"Failed to insert with IF NOT EXISTS: {e}")

            # Verify data was inserted
            select_stmt = await session.prepare("SELECT * FROM lwt_users WHERE id = ?")
            try:
                select_result = await session.execute(select_stmt, [user_id])
                assert len(select_result.rows) == 1
                assert select_result.rows[0].username == "alice"
            except Exception as e:
                pytest.fail(f"Failed to verify inserted data: {e}")

        finally:
            # Cleanup
            await session.execute("DROP TABLE IF EXISTS lwt_users")

    async def test_insert_if_not_exists_conflict(self, cassandra_session):
        """Test INSERT IF NOT EXISTS when row already exists."""
        session = cassandra_session

        try:
            # Create test table
            await session.execute(
                """
                CREATE TABLE IF NOT EXISTS lwt_users2 (
                    id UUID PRIMARY KEY,
                    username TEXT,
                    email TEXT
                )
                """
            )

            await session.execute("TRUNCATE lwt_users2")

            user_id = uuid.uuid4()

            # First insert
            first_insert_stmt = await session.prepare(
                "INSERT INTO lwt_users2 (id, username) VALUES (?, ?)"
            )
            await session.execute(first_insert_stmt, [user_id, "bob"])

            # Try to insert again with IF NOT EXISTS
            insert_if_not_exists_stmt = await session.prepare(
                """
                INSERT INTO lwt_users2 (id, username, email)
                VALUES (?, ?, ?)
                IF NOT EXISTS
                """
            )

            try:
                result = await session.execute(
                    insert_if_not_exists_stmt, [user_id, "alice", "alice@example.com"]
                )

                # Should fail and return existing data
                assert len(result.rows) == 1
                assert result.rows[0][0] is False  # [applied] = false
                # The result includes existing values
                assert hasattr(result.rows[0], "username")
                assert result.rows[0].username == "bob"
            except Exception as e:
                pytest.fail(f"Failed to handle IF NOT EXISTS conflict: {e}")

        finally:
            await session.execute("DROP TABLE IF EXISTS lwt_users2")

    async def test_update_if_condition_match(self, cassandra_session):
        """Test conditional UPDATE when condition matches."""
        session = cassandra_session

        try:
            # Create test table
            await session.execute(
                """
                CREATE TABLE IF NOT EXISTS lwt_users3 (
                    id UUID PRIMARY KEY,
                    username TEXT,
                    balance INT
                )
                """
            )

            await session.execute("TRUNCATE lwt_users3")

            user_id = uuid.uuid4()

            # Insert initial data
            insert_stmt = await session.prepare(
                "INSERT INTO lwt_users3 (id, username, balance) VALUES (?, ?, ?)"
            )
            await session.execute(insert_stmt, [user_id, "charlie", 100])

            # Update with matching condition
            update_stmt = await session.prepare(
                """
                UPDATE lwt_users3
                SET balance = ?
                WHERE id = ?
                IF balance = ?
                """
            )

            try:
                result = await session.execute(update_stmt, [150, user_id, 100])

                # Should succeed
                assert result.rows[0][0] is True  # [applied] = true
            except Exception as e:
                pytest.fail(f"Failed conditional update: {e}")

            # Verify update
            select_stmt = await session.prepare("SELECT balance FROM lwt_users3 WHERE id = ?")
            select_result = await session.execute(select_stmt, [user_id])
            assert select_result.rows[0].balance == 150

        finally:
            await session.execute("DROP TABLE IF EXISTS lwt_users3")

    async def test_update_if_condition_no_match(self, cassandra_session):
        """Test conditional UPDATE when condition doesn't match."""
        session = cassandra_session

        try:
            # Create test table
            await session.execute(
                """
                CREATE TABLE IF NOT EXISTS lwt_users4 (
                    id UUID PRIMARY KEY,
                    username TEXT,
                    balance INT
                )
                """
            )

            await session.execute("TRUNCATE lwt_users4")

            user_id = uuid.uuid4()

            # Insert initial data
            insert_stmt = await session.prepare(
                "INSERT INTO lwt_users4 (id, username, balance) VALUES (?, ?, ?)"
            )
            await session.execute(insert_stmt, [user_id, "dave", 200])

            # Update with non-matching condition
            update_stmt = await session.prepare(
                """
                UPDATE lwt_users4
                SET balance = ?
                WHERE id = ?
                IF balance = ?
                """
            )

            try:
                result = await session.execute(
                    update_stmt, [150, user_id, 100]  # balance is actually 200
                )

                # Should fail and return current value
                assert result.rows[0][0] is False  # [applied] = false
                assert result.rows[0].balance == 200
            except Exception as e:
                pytest.fail(f"Failed to handle non-matching condition: {e}")

        finally:
            await session.execute("DROP TABLE IF EXISTS lwt_users4")

    async def test_delete_if_exists_success(self, cassandra_session):
        """Test DELETE IF EXISTS when row exists."""
        session = cassandra_session

        try:
            # Create test table
            await session.execute(
                """
                CREATE TABLE IF NOT EXISTS lwt_users5 (
                    id UUID PRIMARY KEY,
                    username TEXT
                )
                """
            )

            await session.execute("TRUNCATE lwt_users5")

            user_id = uuid.uuid4()

            # Insert data
            insert_stmt = await session.prepare(
                "INSERT INTO lwt_users5 (id, username) VALUES (?, ?)"
            )
            await session.execute(insert_stmt, [user_id, "eve"])

            # Delete with IF EXISTS
            delete_stmt = await session.prepare("DELETE FROM lwt_users5 WHERE id = ? IF EXISTS")

            try:
                result = await session.execute(delete_stmt, [user_id])

                # Should succeed
                assert result.rows[0][0] is True  # [applied] = true
            except Exception as e:
                pytest.fail(f"Failed DELETE IF EXISTS: {e}")

            # Verify deleted
            select_stmt = await session.prepare("SELECT * FROM lwt_users5 WHERE id = ?")
            select_result = await session.execute(select_stmt, [user_id])
            assert len(select_result.rows) == 0

        finally:
            await session.execute("DROP TABLE IF EXISTS lwt_users5")

    async def test_delete_if_exists_not_found(self, cassandra_session):
        """Test DELETE IF EXISTS when row doesn't exist."""
        session = cassandra_session

        try:
            # Create test table
            await session.execute(
                """
                CREATE TABLE IF NOT EXISTS lwt_users6 (
                    id UUID PRIMARY KEY,
                    username TEXT
                )
                """
            )

            await session.execute("TRUNCATE lwt_users6")

            # Try to delete non-existent row
            delete_stmt = await session.prepare("DELETE FROM lwt_users6 WHERE id = ? IF EXISTS")

            try:
                result = await session.execute(delete_stmt, [uuid.uuid4()])

                # Should fail
                assert result.rows[0][0] is False  # [applied] = false
            except Exception as e:
                pytest.fail(f"Failed to handle DELETE IF EXISTS for non-existent row: {e}")

        finally:
            await session.execute("DROP TABLE IF EXISTS lwt_users6")

    async def test_concurrent_inserts_if_not_exists(self, cassandra_session):
        """Test concurrent INSERT IF NOT EXISTS - only one should succeed."""
        session = cassandra_session

        try:
            # Create test table
            await session.execute(
                """
                CREATE TABLE IF NOT EXISTS lwt_users_concurrent (
                    id UUID PRIMARY KEY,
                    username TEXT,
                    email TEXT
                )
                """
            )

            await session.execute("TRUNCATE lwt_users_concurrent")

            user_id = uuid.uuid4()

            # Prepare statement outside the function to avoid multiple prepares
            insert_if_not_exists_stmt = await session.prepare(
                """
                INSERT INTO lwt_users_concurrent (id, username, email)
                VALUES (?, ?, ?)
                IF NOT EXISTS
                """
            )

            async def try_insert(username: str):
                try:
                    return await session.execute(
                        insert_if_not_exists_stmt,
                        [user_id, username, f"{username}@example.com"],
                    )
                except Exception as e:
                    pytest.fail(f"Failed concurrent insert: {e}")

            # Execute concurrently
            tasks = [try_insert(f"user_{i}") for i in range(10)]
            results = await asyncio.gather(*tasks)

            # Only one should succeed
            successful = [r for r in results if r.rows[0][0] is True]
            failed = [r for r in results if r.rows[0][0] is False]

            assert len(successful) == 1
            assert len(failed) == 9

            # All failed attempts should see the same winning value
            winning_username = failed[0].rows[0].username
            for result in failed:
                assert result.rows[0].username == winning_username

        finally:
            await session.execute("DROP TABLE IF EXISTS lwt_users_concurrent")

    async def test_optimistic_locking_pattern(self, cassandra_session):
        """Test optimistic locking pattern using LWT."""
        session = cassandra_session

        try:
            # Create test table
            await session.execute(
                """
                CREATE TABLE IF NOT EXISTS lwt_inventory (
                    product_id UUID PRIMARY KEY,
                    name TEXT,
                    quantity INT,
                    reserved INT
                )
                """
            )

            await session.execute("TRUNCATE lwt_inventory")

            product_id = uuid.uuid4()

            # Insert initial inventory
            insert_stmt = await session.prepare(
                "INSERT INTO lwt_inventory (product_id, name, quantity) VALUES (?, ?, ?)"
            )
            await session.execute(insert_stmt, [product_id, "Widget", 100])

            # Prepare statements for concurrent use
            select_stmt = await session.prepare(
                "SELECT quantity FROM lwt_inventory WHERE product_id = ?"
            )
            update_stmt = await session.prepare(
                """
                UPDATE lwt_inventory
                SET quantity = ?
                WHERE product_id = ?
                IF quantity = ?
                """
            )

            # Simulate concurrent inventory updates
            async def try_reserve_items(count: int):
                try:
                    # Read current quantity
                    result = await session.execute(select_stmt, [product_id])
                    current_quantity = result.rows[0].quantity

                    if current_quantity >= count:
                        # Try to update with optimistic lock
                        update_result = await session.execute(
                            update_stmt,
                            [current_quantity - count, product_id, current_quantity],
                        )
                        return update_result.rows[0][0]  # [applied]
                    return False
                except Exception as e:
                    pytest.fail(f"Failed inventory reservation: {e}")

            # Try multiple concurrent reservations
            tasks = []
            for i in range(5):
                tasks.append(try_reserve_items(30))  # Each tries to reserve 30

            results = await asyncio.gather(*tasks)

            # With 100 initial items and 5 requests of 30 each,
            # at most 3 should succeed (3 * 30 = 90)
            successful_count = sum(1 for r in results if r is True)
            assert successful_count <= 3

            # Check final quantity
            final_result = await session.execute(select_stmt, [product_id])
            final_quantity = final_result.rows[0].quantity
            assert final_quantity == 100 - (successful_count * 30)

        finally:
            await session.execute("DROP TABLE IF EXISTS lwt_inventory")

    async def test_complex_condition_with_multiple_columns(self, cassandra_session):
        """Test LWT with conditions on multiple columns."""
        session = cassandra_session

        try:
            # Create test table
            await session.execute(
                """
                CREATE TABLE IF NOT EXISTS lwt_users_complex (
                    id UUID PRIMARY KEY,
                    username TEXT,
                    email TEXT,
                    status TEXT
                )
                """
            )

            await session.execute("TRUNCATE lwt_users_complex")

            user_id = uuid.uuid4()

            # Insert initial data
            insert_stmt = await session.prepare(
                """
                INSERT INTO lwt_users_complex (id, username, email, status)
                VALUES (?, ?, ?, ?)
                """
            )
            await session.execute(insert_stmt, [user_id, "frank", "frank@example.com", "pending"])

            # Update with multiple conditions
            update_stmt = await session.prepare(
                """
                UPDATE lwt_users_complex
                SET status = ?
                WHERE id = ?
                IF username = ? AND email = ? AND status = ?
                """
            )

            try:
                result = await session.execute(
                    update_stmt,
                    ["active", user_id, "frank", "frank@example.com", "pending"],
                )

                # Should succeed
                assert result.rows[0][0] is True
            except Exception as e:
                pytest.fail(f"Failed multi-column condition update: {e}")

            # Try again with same conditions (should fail now)
            try:
                result = await session.execute(
                    update_stmt,
                    ["inactive", user_id, "frank", "frank@example.com", "pending"],
                )

                # Should fail because status is now 'active'
                assert result.rows[0][0] is False
                assert result.rows[0].status == "active"
            except Exception as e:
                pytest.fail(f"Failed to verify condition mismatch: {e}")

        finally:
            await session.execute("DROP TABLE IF EXISTS lwt_users_complex")

    async def test_prepared_statements_with_lwt(self, cassandra_session):
        """Test LWT operations with prepared statements."""
        session = cassandra_session

        try:
            # Create test table
            await session.execute(
                """
                CREATE TABLE IF NOT EXISTS lwt_users_prepared (
                    id UUID PRIMARY KEY,
                    username TEXT,
                    balance INT
                )
                """
            )

            await session.execute("TRUNCATE lwt_users_prepared")

            # Prepare LWT statements
            insert_prepared = await session.prepare(
                """
                INSERT INTO lwt_users_prepared (id, username, balance)
                VALUES (?, ?, ?)
                IF NOT EXISTS
                """
            )

            update_prepared = await session.prepare(
                """
                UPDATE lwt_users_prepared
                SET balance = balance + ?
                WHERE id = ?
                IF balance >= ?
                """
            )

            user_id = uuid.uuid4()

            # Execute prepared INSERT
            try:
                result = await session.execute(insert_prepared, [user_id, "prepared_user", 100])
                assert result.rows[0][0] is True
            except Exception as e:
                pytest.fail(f"Failed prepared INSERT LWT: {e}")

            # Execute prepared UPDATE with condition
            try:
                result = await session.execute(
                    update_prepared, [50, user_id, 50]  # Add 50 if balance >= 50
                )
                assert result.rows[0][0] is True
            except Exception as e:
                pytest.fail(f"Failed prepared UPDATE LWT: {e}")

            # Verify final balance
            select_stmt = await session.prepare(
                "SELECT balance FROM lwt_users_prepared WHERE id = ?"
            )
            select_result = await session.execute(select_stmt, [user_id])
            assert select_result.rows[0].balance == 150

        finally:
            await session.execute("DROP TABLE IF EXISTS lwt_users_prepared")

    async def test_lwt_error_handling(self, cassandra_session):
        """Test error handling in LWT operations."""
        session = cassandra_session

        try:
            # Create test table
            await session.execute(
                """
                CREATE TABLE IF NOT EXISTS lwt_users_errors (
                    id UUID PRIMARY KEY,
                    username TEXT
                )
                """
            )

            await session.execute("TRUNCATE lwt_users_errors")

            # Test invalid LWT syntax - this should fail during prepare
            with pytest.raises(Exception) as exc_info:
                # Invalid syntax: can't combine IF NOT EXISTS with other conditions
                await session.prepare(
                    """
                    INSERT INTO lwt_users_errors (id, username)
                    VALUES (?, ?)
                    IF NOT EXISTS AND username = ?
                    """
                )

            # Should raise syntax error
            assert "Syntax error" in str(exc_info.value) or "SyntaxException" in str(exc_info.value)

        finally:
            await session.execute("DROP TABLE IF EXISTS lwt_users_errors")
