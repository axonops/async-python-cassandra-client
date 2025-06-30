"""
Integration tests for Lightweight Transaction (LWT) operations.

These tests verify LWT functionality with a real Cassandra instance:
- IF NOT EXISTS conditions
- IF EXISTS conditions
- Conditional updates
- Race conditions and concurrent access
"""

import asyncio
import uuid

import pytest

from async_cassandra import AsyncCluster


class TestLWTIntegration:
    """Test LWT operations with real Cassandra."""

    @pytest.fixture(scope="class")
    async def cluster(self):
        """Create cluster instance."""
        cluster = AsyncCluster(contact_points=["127.0.0.1"])
        yield cluster
        await cluster.shutdown()

    @pytest.fixture(scope="class")
    async def session(self, cluster):
        """Create session and test keyspace."""
        session = await cluster.connect()

        # Create test keyspace
        await session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS test_lwt
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
        """
        )

        await session.set_keyspace("test_lwt")

        # Create test tables
        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id UUID PRIMARY KEY,
                username TEXT,
                email TEXT,
                balance INT,
                status TEXT
            )
        """
        )

        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS inventory (
                product_id UUID PRIMARY KEY,
                name TEXT,
                quantity INT,
                reserved INT
            )
        """
        )

        yield session

        # Cleanup
        await session.execute("DROP KEYSPACE IF EXISTS test_lwt")
        await session.close()

    async def cleanup_table(self, session, table_name: str):
        """Clean up table before test."""
        await session.execute(f"TRUNCATE {table_name}")

    @pytest.mark.asyncio
    async def test_insert_if_not_exists_success(self, session):
        """Test successful INSERT IF NOT EXISTS."""
        await self.cleanup_table(session, "users")

        user_id = uuid.uuid4()

        # Insert with IF NOT EXISTS
        result = await session.execute(
            """
            INSERT INTO users (id, username, email, balance)
            VALUES (?, ?, ?, ?)
            IF NOT EXISTS
            """,
            (user_id, "alice", "alice@example.com", 100),
        )

        # Should succeed
        assert len(result.rows) == 1
        assert result.rows[0][0] is True  # [applied] column

        # Verify data was inserted
        select_result = await session.execute("SELECT * FROM users WHERE id = ?", (user_id,))
        assert len(select_result.rows) == 1
        assert select_result.rows[0]["username"] == "alice"

    @pytest.mark.asyncio
    async def test_insert_if_not_exists_conflict(self, session):
        """Test INSERT IF NOT EXISTS when row already exists."""
        await self.cleanup_table(session, "users")

        user_id = uuid.uuid4()

        # First insert
        await session.execute("INSERT INTO users (id, username) VALUES (?, ?)", (user_id, "bob"))

        # Try to insert again with IF NOT EXISTS
        result = await session.execute(
            """
            INSERT INTO users (id, username, email)
            VALUES (?, ?, ?)
            IF NOT EXISTS
            """,
            (user_id, "alice", "alice@example.com"),
        )

        # Should fail and return existing data
        assert len(result.rows) == 1
        assert result.rows[0][0] is False  # [applied] = false
        # The result includes existing values
        assert "username" in result.rows[0]
        assert result.rows[0]["username"] == "bob"

    @pytest.mark.asyncio
    async def test_update_if_condition_match(self, session):
        """Test conditional UPDATE when condition matches."""
        await self.cleanup_table(session, "users")

        user_id = uuid.uuid4()

        # Insert initial data
        await session.execute(
            "INSERT INTO users (id, username, balance) VALUES (?, ?, ?)", (user_id, "charlie", 100)
        )

        # Update with matching condition
        result = await session.execute(
            """
            UPDATE users
            SET balance = ?
            WHERE id = ?
            IF balance = ?
            """,
            (150, user_id, 100),
        )

        # Should succeed
        assert result.rows[0][0] is True  # [applied] = true

        # Verify update
        select_result = await session.execute("SELECT balance FROM users WHERE id = ?", (user_id,))
        assert select_result.rows[0]["balance"] == 150

    @pytest.mark.asyncio
    async def test_update_if_condition_no_match(self, session):
        """Test conditional UPDATE when condition doesn't match."""
        await self.cleanup_table(session, "users")

        user_id = uuid.uuid4()

        # Insert initial data
        await session.execute(
            "INSERT INTO users (id, username, balance) VALUES (?, ?, ?)", (user_id, "dave", 200)
        )

        # Update with non-matching condition
        result = await session.execute(
            """
            UPDATE users
            SET balance = ?
            WHERE id = ?
            IF balance = ?
            """,
            (150, user_id, 100),  # balance is actually 200
        )

        # Should fail and return current value
        assert result.rows[0][0] is False  # [applied] = false
        assert result.rows[0]["balance"] == 200

    @pytest.mark.asyncio
    async def test_delete_if_exists_success(self, session):
        """Test DELETE IF EXISTS when row exists."""
        await self.cleanup_table(session, "users")

        user_id = uuid.uuid4()

        # Insert data
        await session.execute("INSERT INTO users (id, username) VALUES (?, ?)", (user_id, "eve"))

        # Delete with IF EXISTS
        result = await session.execute("DELETE FROM users WHERE id = ? IF EXISTS", (user_id,))

        # Should succeed
        assert result.rows[0][0] is True  # [applied] = true

        # Verify deleted
        select_result = await session.execute("SELECT * FROM users WHERE id = ?", (user_id,))
        assert len(select_result.rows) == 0

    @pytest.mark.asyncio
    async def test_delete_if_exists_not_found(self, session):
        """Test DELETE IF EXISTS when row doesn't exist."""
        await self.cleanup_table(session, "users")

        # Try to delete non-existent row
        result = await session.execute("DELETE FROM users WHERE id = ? IF EXISTS", (uuid.uuid4(),))

        # Should fail
        assert result.rows[0][0] is False  # [applied] = false

    @pytest.mark.asyncio
    async def test_concurrent_inserts_if_not_exists(self, session):
        """Test concurrent INSERT IF NOT EXISTS - only one should succeed."""
        await self.cleanup_table(session, "users")

        user_id = uuid.uuid4()

        # Create multiple concurrent insert tasks
        async def try_insert(username: str):
            return await session.execute(
                """
                INSERT INTO users (id, username, email)
                VALUES (?, ?, ?)
                IF NOT EXISTS
                """,
                (user_id, username, f"{username}@example.com"),
            )

        # Execute concurrently
        tasks = [try_insert(f"user_{i}") for i in range(10)]
        results = await asyncio.gather(*tasks)

        # Only one should succeed
        successful = [r for r in results if r.rows[0][0] is True]
        failed = [r for r in results if r.rows[0][0] is False]

        assert len(successful) == 1
        assert len(failed) == 9

        # All failed attempts should see the same winning value
        winning_username = failed[0].rows[0]["username"]
        for result in failed:
            assert result.rows[0]["username"] == winning_username

    @pytest.mark.asyncio
    async def test_optimistic_locking_pattern(self, session):
        """Test optimistic locking pattern using LWT."""
        await self.cleanup_table(session, "inventory")

        product_id = uuid.uuid4()

        # Insert initial inventory
        await session.execute(
            "INSERT INTO inventory (product_id, name, quantity) VALUES (?, ?, ?)",
            (product_id, "Widget", 100),
        )

        # Simulate concurrent inventory updates
        async def try_reserve_items(count: int):
            # Read current quantity
            result = await session.execute(
                "SELECT quantity FROM inventory WHERE product_id = ?", (product_id,)
            )
            current_quantity = result.rows[0]["quantity"]

            if current_quantity >= count:
                # Try to update with optimistic lock
                update_result = await session.execute(
                    """
                    UPDATE inventory
                    SET quantity = ?
                    WHERE product_id = ?
                    IF quantity = ?
                    """,
                    (current_quantity - count, product_id, current_quantity),
                )
                return update_result.rows[0][0]  # [applied]
            return False

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
        final_result = await session.execute(
            "SELECT quantity FROM inventory WHERE product_id = ?", (product_id,)
        )
        final_quantity = final_result.rows[0]["quantity"]
        assert final_quantity == 100 - (successful_count * 30)

    @pytest.mark.asyncio
    async def test_complex_condition_with_multiple_columns(self, session):
        """Test LWT with conditions on multiple columns."""
        await self.cleanup_table(session, "users")

        user_id = uuid.uuid4()

        # Insert initial data
        await session.execute(
            """
            INSERT INTO users (id, username, email, status)
            VALUES (?, ?, ?, ?)
            """,
            (user_id, "frank", "frank@example.com", "pending"),
        )

        # Update with multiple conditions
        result = await session.execute(
            """
            UPDATE users
            SET status = ?
            WHERE id = ?
            IF username = ? AND email = ? AND status = ?
            """,
            ("active", user_id, "frank", "frank@example.com", "pending"),
        )

        # Should succeed
        assert result.rows[0][0] is True

        # Try again with same conditions (should fail now)
        result = await session.execute(
            """
            UPDATE users
            SET status = ?
            WHERE id = ?
            IF username = ? AND email = ? AND status = ?
            """,
            ("inactive", user_id, "frank", "frank@example.com", "pending"),
        )

        # Should fail because status is now 'active'
        assert result.rows[0][0] is False
        assert result.rows[0]["status"] == "active"

    @pytest.mark.asyncio
    async def test_prepared_statements_with_lwt(self, session):
        """Test LWT operations with prepared statements."""
        await self.cleanup_table(session, "users")

        # Prepare LWT statements
        insert_prepared = await session.prepare(
            """
            INSERT INTO users (id, username, balance)
            VALUES (?, ?, ?)
            IF NOT EXISTS
            """
        )

        update_prepared = await session.prepare(
            """
            UPDATE users
            SET balance = balance + ?
            WHERE id = ?
            IF balance >= ?
            """
        )

        user_id = uuid.uuid4()

        # Execute prepared INSERT
        result = await session.execute(insert_prepared, (user_id, "prepared_user", 100))
        assert result.rows[0][0] is True

        # Execute prepared UPDATE with condition
        result = await session.execute(
            update_prepared, (50, user_id, 50)  # Add 50 if balance >= 50
        )
        assert result.rows[0][0] is True

        # Verify final balance
        select_result = await session.execute("SELECT balance FROM users WHERE id = ?", (user_id,))
        assert select_result.rows[0]["balance"] == 150

    @pytest.mark.asyncio
    async def test_lwt_error_handling(self, session):
        """Test error handling in LWT operations."""
        await self.cleanup_table(session, "users")

        # Test invalid LWT syntax
        with pytest.raises(Exception) as exc_info:
            await session.execute(
                """
                INSERT INTO users (id, username)
                VALUES (?, ?)
                IF NOT EXISTS AND username = ?
                """,
                (uuid.uuid4(), "invalid", "test"),
            )

        # Should raise syntax error
        assert "SyntaxException" in str(type(exc_info.value)) or "Invalid" in str(exc_info.value)
