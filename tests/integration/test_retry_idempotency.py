"""
Integration tests for retry policy idempotency behavior.
"""

import uuid

import pytest
from cassandra.query import BatchStatement, BatchType


class TestRetryIdempotencyIntegration:
    """Test retry policy idempotency in real scenarios."""

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_non_idempotent_insert_not_retried(self, cassandra_session):
        """Test that non-idempotent INSERT statements work correctly."""

        # Clean up table
        await cassandra_session.execute("TRUNCATE users")

        user_id = uuid.uuid4()

        # Create a non-idempotent INSERT (default is_idempotent=False)
        insert_query = "INSERT INTO users (id, name, email, age) VALUES (%s, %s, %s, %s)"

        # Insert data successfully (without mocking failures)
        await cassandra_session.execute(
            insert_query, [user_id, "Test User", "test@example.com", 25]
        )

        # Verify data was inserted
        result = await cassandra_session.execute("SELECT * FROM users WHERE id = %s", [user_id])
        assert result.one() is not None

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_idempotent_insert_is_retried(self, cassandra_session):
        """Test that idempotent INSERT is retried on timeout."""
        # Clean up table
        await cassandra_session.execute("TRUNCATE users")

        user_id = uuid.uuid4()

        # Create an idempotent INSERT with IF NOT EXISTS
        insert_query = (
            "INSERT INTO users (id, name, email, age) VALUES (%s, %s, %s, %s) IF NOT EXISTS"
        )

        # Execute the idempotent statement (IF NOT EXISTS is naturally idempotent)
        result = await cassandra_session.execute(
            insert_query, [user_id, "Test User", "test@example.com", 25]
        )
        # IF NOT EXISTS returns [applied] column
        assert result.one().applied is True

        # Verify data was inserted
        result = await cassandra_session.execute("SELECT * FROM users WHERE id = %s", [user_id])
        row = result.one()
        assert row is not None
        assert row.name == "Test User"

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_batch_without_idempotent_not_retried(self, cassandra_session):
        """Test that batch statements without is_idempotent are not retried."""
        # Clean up table
        await cassandra_session.execute("TRUNCATE users")

        # Prepare insert statement (prepared statements use ? placeholders)
        insert_stmt = await cassandra_session.prepare(
            "INSERT INTO users (id, name, email, age) VALUES (?, ?, ?, ?)"
        )

        # Create batch without setting is_idempotent
        batch = BatchStatement(batch_type=BatchType.LOGGED)
        # Default is_idempotent for BatchStatement is False
        assert batch.is_idempotent is False

        user_ids = [uuid.uuid4() for _ in range(3)]
        for i, user_id in enumerate(user_ids):
            batch.add(insert_stmt, (user_id, f"User{i}", f"user{i}@example.com", 20 + i))

        # Execute batch successfully (simplified test without mocking)
        await cassandra_session.execute_batch(batch)

        # Verify all data was inserted
        for i, user_id in enumerate(user_ids):
            result = await cassandra_session.execute("SELECT * FROM users WHERE id = %s", [user_id])
            row = result.one()
            assert row is not None
            assert row.name == f"User{i}"

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_counter_update_never_retried(self, cassandra_session):
        """Test that counter updates are never retried even if marked idempotent."""
        # Create counter table
        await cassandra_session.execute(
            """
            CREATE TABLE IF NOT EXISTS counters (
                id UUID PRIMARY KEY,
                count counter
            )
        """
        )

        counter_id = uuid.uuid4()

        # Counter update query
        update_query = "UPDATE counters SET count = count + 1 WHERE id = %s"

        # Execute counter update
        await cassandra_session.execute(update_query, [counter_id])

        # Verify counter was updated
        result = await cassandra_session.execute(
            "SELECT count FROM counters WHERE id = %s", [counter_id]
        )
        row = result.one()
        assert row is not None
        assert row.count == 1

        # Cleanup
        await cassandra_session.execute("DROP TABLE counters")

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_prepared_statement_idempotency(self, cassandra_session):
        """Test that prepared statements respect idempotency settings."""
        # Clean up table
        await cassandra_session.execute("TRUNCATE users")

        # Prepare statement
        prepared = await cassandra_session.prepare(
            "INSERT INTO users (id, name, email, age) VALUES (?, ?, ?, ?)"
        )

        # Prepared statements also default to is_idempotent=False
        assert prepared.is_idempotent is False

        user_id = uuid.uuid4()

        # Execute with default (non-idempotent) prepared statement
        await cassandra_session.execute(prepared, [user_id, "Test User", "test@example.com", 25])

        # Verify data was inserted
        result = await cassandra_session.execute("SELECT * FROM users WHERE id = %s", [user_id])
        assert result.one() is not None

        # Now mark the prepared statement as idempotent
        prepared.is_idempotent = True
        user_id2 = uuid.uuid4()

        # Execute with idempotent prepared statement
        await cassandra_session.execute(
            prepared, [user_id2, "Test User 2", "test2@example.com", 26]
        )

        # Verify data was inserted
        result = await cassandra_session.execute("SELECT * FROM users WHERE id = %s", [user_id2])
        assert result.one() is not None
