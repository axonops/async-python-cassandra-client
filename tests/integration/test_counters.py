"""
Integration tests for counter operations.

Counters are a special column type in Cassandra that support atomic increments/decrements.
This test suite ensures our async wrapper properly handles all counter operations.
"""

import asyncio
import uuid

import pytest


@pytest.mark.integration
class TestCounterOperations:
    """Test counter column operations."""

    @pytest.fixture
    async def counter_table(self, session_with_keyspace):
        """Create a table with counter columns."""
        session, keyspace = session_with_keyspace
        table_name = f"counters_{uuid.uuid4().hex[:8]}"

        # Create counter table
        await session.execute(
            f"""
            CREATE TABLE {table_name} (
                id text,
                category text,
                views counter,
                clicks counter,
                PRIMARY KEY (id, category)
            )
        """
        )

        yield table_name

        # Cleanup
        await session.execute(f"DROP TABLE IF EXISTS {table_name}")

    @pytest.mark.asyncio
    async def test_counter_increment(self, session_with_keyspace, counter_table):
        """Test basic counter increment operations."""
        session, _ = session_with_keyspace

        # Prepare statements
        increment_stmt = await session.prepare(
            f"UPDATE {counter_table} SET views = views + ? WHERE id = ? AND category = ?"
        )
        select_stmt = await session.prepare(
            f"SELECT views FROM {counter_table} WHERE id = ? AND category = ?"
        )

        # Increment counter
        await session.execute(increment_stmt, [1, "post1", "tech"])

        # Read counter value
        result = await session.execute(select_stmt, ["post1", "tech"])
        row = result.one()
        assert row.views == 1

        # Increment again
        await session.execute(increment_stmt, [5, "post1", "tech"])

        result = await session.execute(select_stmt, ["post1", "tech"])
        row = result.one()
        assert row.views == 6

    @pytest.mark.asyncio
    async def test_counter_decrement(self, session_with_keyspace, counter_table):
        """Test counter decrement operations."""
        session, _ = session_with_keyspace

        # Prepare statements
        update_stmt = await session.prepare(
            f"UPDATE {counter_table} SET clicks = clicks + ? WHERE id = ? AND category = ?"
        )
        select_stmt = await session.prepare(
            f"SELECT clicks FROM {counter_table} WHERE id = ? AND category = ?"
        )

        # Initialize counter
        await session.execute(update_stmt, [10, "post2", "news"])

        # Decrement counter (add negative value)
        await session.execute(update_stmt, [-3, "post2", "news"])

        result = await session.execute(select_stmt, ["post2", "news"])
        row = result.one()
        assert row.clicks == 7

    @pytest.mark.asyncio
    async def test_multiple_counters_same_row(self, session_with_keyspace, counter_table):
        """Test updating multiple counters in the same row."""
        session, _ = session_with_keyspace

        # Prepare statements
        multi_update_stmt = await session.prepare(
            f"UPDATE {counter_table} SET views = views + ?, clicks = clicks + ? "
            f"WHERE id = ? AND category = ?"
        )
        select_stmt = await session.prepare(
            f"SELECT views, clicks FROM {counter_table} WHERE id = ? AND category = ?"
        )

        # Update multiple counters
        await session.execute(multi_update_stmt, [1, 1, "post3", "sports"])

        result = await session.execute(select_stmt, ["post3", "sports"])
        row = result.one()
        assert row.views == 1
        assert row.clicks == 1

        # Update them differently
        await session.execute(multi_update_stmt, [10, 2, "post3", "sports"])

        result = await session.execute(select_stmt, ["post3", "sports"])
        row = result.one()
        assert row.views == 11
        assert row.clicks == 3

    @pytest.mark.asyncio
    async def test_counter_prepared_statements(self, session_with_keyspace, counter_table):
        """Test counters with prepared statements."""
        session, _ = session_with_keyspace

        # Prepare increment statement
        increment_stmt = await session.prepare(
            f"UPDATE {counter_table} SET views = views + ? WHERE id = ? AND category = ?"
        )

        # Execute prepared statement multiple times
        await session.execute(increment_stmt, [5, "post4", "tech"])
        await session.execute(increment_stmt, [3, "post4", "tech"])
        await session.execute(increment_stmt, [2, "post4", "tech"])

        # Verify total
        result = await session.execute(
            f"SELECT views FROM {counter_table} WHERE id = 'post4' AND category = 'tech'"
        )
        row = result.one()
        assert row.views == 10

    @pytest.mark.asyncio
    async def test_counter_concurrent_updates(self, session_with_keyspace, counter_table):
        """Test concurrent counter updates."""
        session, _ = session_with_keyspace

        # Prepare statement
        increment_stmt = await session.prepare(
            f"UPDATE {counter_table} SET views = views + 1 WHERE id = ? AND category = ?"
        )

        # Execute 100 concurrent increments
        tasks = []
        for i in range(100):
            task = session.execute(increment_stmt, ["post5", "concurrent"])
            tasks.append(task)

        await asyncio.gather(*tasks)

        # Verify all increments were applied
        result = await session.execute(
            f"SELECT views FROM {counter_table} WHERE id = 'post5' AND category = 'concurrent'"
        )
        row = result.one()
        assert row.views == 100

    @pytest.mark.asyncio
    async def test_counter_consistency_levels(self, session_with_keyspace, counter_table):
        """Test counter operations with different consistency levels."""
        from cassandra import ConsistencyLevel

        session, _ = session_with_keyspace

        # Prepare statements with consistency levels
        update_stmt = await session.prepare(
            f"UPDATE {counter_table} SET views = views + ? WHERE id = ? AND category = ?"
        )
        select_stmt = await session.prepare(
            f"SELECT views FROM {counter_table} WHERE id = ? AND category = ?"
        )

        # Set consistency level on the prepared statement
        update_stmt.consistency_level = ConsistencyLevel.QUORUM
        select_stmt.consistency_level = ConsistencyLevel.ONE

        # Counter write with QUORUM
        await session.execute(update_stmt, [1, "post6", "test"])

        # Counter read with ONE
        result = await session.execute(select_stmt, ["post6", "test"])
        row = result.one()
        assert row.views == 1

    @pytest.mark.asyncio
    async def test_counter_delete_operations(self, session_with_keyspace, counter_table):
        """Test deleting counter columns."""
        session, _ = session_with_keyspace

        # Prepare statements
        update_stmt = await session.prepare(
            f"UPDATE {counter_table} SET views = views + ?, clicks = clicks + ? "
            f"WHERE id = ? AND category = ?"
        )
        delete_column_stmt = await session.prepare(
            f"DELETE clicks FROM {counter_table} WHERE id = ? AND category = ?"
        )
        delete_row_stmt = await session.prepare(
            f"DELETE FROM {counter_table} WHERE id = ? AND category = ?"
        )
        select_stmt = await session.prepare(
            f"SELECT views, clicks FROM {counter_table} WHERE id = ? AND category = ?"
        )
        select_all_stmt = await session.prepare(
            f"SELECT * FROM {counter_table} WHERE id = ? AND category = ?"
        )

        # Initialize counters
        await session.execute(update_stmt, [10, 5, "post7", "delete-test"])

        # Delete specific counter column
        await session.execute(delete_column_stmt, ["post7", "delete-test"])

        # Verify views still exists but clicks is null
        result = await session.execute(select_stmt, ["post7", "delete-test"])
        row = result.one()
        assert row.views == 10
        assert row.clicks is None

        # Delete entire row
        await session.execute(delete_row_stmt, ["post7", "delete-test"])

        result = await session.execute(select_all_stmt, ["post7", "delete-test"])
        assert result.one() is None

    @pytest.mark.asyncio
    async def test_counter_ttl_not_allowed(self, session_with_keyspace, counter_table):
        """Test that TTL is not allowed on counter columns."""
        session, _ = session_with_keyspace

        # Counter updates with TTL should fail
        # Note: Cannot prepare statements with TTL on counters as it's invalid
        with pytest.raises(Exception) as exc_info:
            await session.execute(
                f"UPDATE {counter_table} USING TTL 3600 SET views = views + 1 "
                f"WHERE id = 'post8' AND category = 'ttl-test'"
            )

        # Verify it's an invalid request
        assert "TTL" in str(exc_info.value) or "counter" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_counter_null_operations(self, session_with_keyspace, counter_table):
        """Test counter behavior with null/unset values."""
        session, _ = session_with_keyspace

        # Prepare statements
        select_stmt = await session.prepare(
            f"SELECT views FROM {counter_table} WHERE id = ? AND category = ?"
        )
        update_stmt = await session.prepare(
            f"UPDATE {counter_table} SET views = views + ? WHERE id = ? AND category = ?"
        )

        # Reading non-existent counter returns null
        result = await session.execute(select_stmt, ["nonexistent", "null-test"])
        row = result.one()
        assert row is None

        # After first increment, counter exists
        await session.execute(update_stmt, [1, "new-post", "null-test"])

        result = await session.execute(select_stmt, ["new-post", "null-test"])
        row = result.one()
        assert row.views == 1
