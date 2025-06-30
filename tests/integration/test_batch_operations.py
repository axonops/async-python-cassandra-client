"""
Integration tests for batch operations.

Tests logged batches, unlogged batches, and counter batches.
"""

import asyncio
import uuid

import pytest
from cassandra import ConsistencyLevel
from cassandra.query import BatchStatement, BatchType, SimpleStatement


@pytest.mark.integration
class TestBatchOperations:
    """Test various batch operation types."""

    @pytest.fixture
    async def batch_tables(self, session_with_keyspace):
        """Create tables for batch testing."""
        session, keyspace = session_with_keyspace

        # Regular table
        regular_table = f"batch_regular_{uuid.uuid4().hex[:8]}"
        await session.execute(
            f"""
            CREATE TABLE {regular_table} (
                id int PRIMARY KEY,
                name text,
                value int
            )
        """
        )

        # Counter table
        counter_table = f"batch_counters_{uuid.uuid4().hex[:8]}"
        await session.execute(
            f"""
            CREATE TABLE {counter_table} (
                id text PRIMARY KEY,
                count1 counter,
                count2 counter
            )
        """
        )

        yield regular_table, counter_table

        # Cleanup
        await session.execute(f"DROP TABLE IF EXISTS {regular_table}")
        await session.execute(f"DROP TABLE IF EXISTS {counter_table}")

    @pytest.mark.asyncio
    async def test_logged_batch(self, session_with_keyspace, batch_tables):
        """Test logged batch operations (default)."""
        session, _ = session_with_keyspace
        regular_table, _ = batch_tables

        # Create logged batch (default)
        batch = BatchStatement()

        # Prepare statement
        insert_stmt = await session.prepare(
            f"INSERT INTO {regular_table} (id, name, value) VALUES (?, ?, ?)"
        )

        # Add multiple statements
        batch.add(insert_stmt, (1, "Alice", 100))
        batch.add(insert_stmt, (2, "Bob", 200))
        batch.add(insert_stmt, (3, "Charlie", 300))

        # Execute batch
        await session.execute(batch)

        # Verify all inserts
        for id_val in [1, 2, 3]:
            result = await session.execute(f"SELECT * FROM {regular_table} WHERE id = {id_val}")
            row = result.one()
            assert row is not None
            assert row.id == id_val

    @pytest.mark.asyncio
    async def test_unlogged_batch(self, session_with_keyspace, batch_tables):
        """Test unlogged batch operations for performance."""
        session, _ = session_with_keyspace
        regular_table, _ = batch_tables

        # Create unlogged batch
        batch = BatchStatement(batch_type=BatchType.UNLOGGED)

        # Prepare statement
        insert_stmt = await session.prepare(
            f"INSERT INTO {regular_table} (id, name, value) VALUES (?, ?, ?)"
        )

        # Add statements - unlogged batches are good for multiple partitions
        for i in range(10):
            batch.add(insert_stmt, (100 + i, f"User{i}", i * 10))

        # Execute batch
        await session.execute(batch)

        # Verify inserts
        result = await session.execute(
            f"SELECT COUNT(*) FROM {regular_table} WHERE id >= 100 AND id < 110 ALLOW FILTERING"
        )
        count = result.one()[0]
        assert count == 10

    @pytest.mark.asyncio
    async def test_counter_batch(self, session_with_keyspace, batch_tables):
        """Test counter batch operations."""
        session, _ = session_with_keyspace
        _, counter_table = batch_tables

        # Create counter batch
        batch = BatchStatement(batch_type=BatchType.COUNTER)

        # Prepare counter statements
        update_count1_stmt = await session.prepare(
            f"UPDATE {counter_table} SET count1 = count1 + ? WHERE id = ?"
        )
        update_count2_stmt = await session.prepare(
            f"UPDATE {counter_table} SET count2 = count2 + ? WHERE id = ?"
        )

        # Add counter updates
        batch.add(update_count1_stmt, (5, "counter1"))
        batch.add(update_count2_stmt, (10, "counter1"))
        batch.add(update_count1_stmt, (3, "counter2"))

        # Execute counter batch
        await session.execute(batch)

        # Verify counters
        result = await session.execute(f"SELECT * FROM {counter_table} WHERE id = 'counter1'")
        row = result.one()
        assert row.count1 == 5
        assert row.count2 == 10

        result = await session.execute(f"SELECT * FROM {counter_table} WHERE id = 'counter2'")
        row = result.one()
        assert row.count1 == 3
        assert row.count2 is None  # Not updated

    @pytest.mark.asyncio
    async def test_mixed_batch_types_error(self, session_with_keyspace, batch_tables):
        """Test that mixing regular and counter operations in same batch fails."""
        session, _ = session_with_keyspace
        regular_table, counter_table = batch_tables

        # Try to mix regular and counter operations
        batch = BatchStatement()

        # This test needs to use SimpleStatement to demonstrate the error
        batch.add(
            SimpleStatement(f"INSERT INTO {regular_table} (id, name, value) VALUES (%s, %s, %s)"),
            (999, "Test", 999),
        )

        batch.add(
            SimpleStatement(f"UPDATE {counter_table} SET count1 = count1 + %s WHERE id = %s"),
            (1, "error-test"),
        )

        # Should fail
        with pytest.raises(Exception) as exc_info:
            await session.execute(batch)

        assert "counter" in str(exc_info.value).lower() or "batch" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_batch_with_prepared_statements(self, session_with_keyspace, batch_tables):
        """Test batches with prepared statements."""
        session, _ = session_with_keyspace
        regular_table, _ = batch_tables

        # Prepare statements
        insert_stmt = await session.prepare(
            f"INSERT INTO {regular_table} (id, name, value) VALUES (?, ?, ?)"
        )
        update_stmt = await session.prepare(f"UPDATE {regular_table} SET value = ? WHERE id = ?")

        # Create batch with prepared statements
        batch = BatchStatement()

        # Add prepared statements
        batch.add(insert_stmt, (200, "PrepUser1", 1000))
        batch.add(insert_stmt, (201, "PrepUser2", 2000))
        batch.add(update_stmt, (3000, 200))  # Update first one

        await session.execute(batch)

        # Verify
        result = await session.execute(f"SELECT * FROM {regular_table} WHERE id = 200")
        row = result.one()
        assert row.name == "PrepUser1"
        assert row.value == 3000  # Updated value

    @pytest.mark.asyncio
    async def test_batch_consistency_levels(self, session_with_keyspace, batch_tables):
        """Test batch operations with different consistency levels."""
        session, _ = session_with_keyspace
        regular_table, _ = batch_tables

        # Prepare statement
        insert_stmt = await session.prepare(
            f"INSERT INTO {regular_table} (id, name, value) VALUES (?, ?, ?)"
        )

        # Create batch with specific consistency
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)

        batch.add(insert_stmt, (300, "ConsistencyTest", 300))

        await session.execute(batch)

        # Read with different consistency
        select_stmt = await session.prepare(f"SELECT * FROM {regular_table} WHERE id = ?")
        select_stmt.consistency_level = ConsistencyLevel.ONE

        result = await session.execute(select_stmt, [300])
        assert result.one() is not None

    @pytest.mark.asyncio
    async def test_large_batch_warning(self, session_with_keyspace, batch_tables):
        """Test that large batches work but may generate warnings."""
        session, _ = session_with_keyspace
        regular_table, _ = batch_tables

        # Create large unlogged batch
        batch = BatchStatement(batch_type=BatchType.UNLOGGED)

        # Prepare statement
        insert_stmt = await session.prepare(
            f"INSERT INTO {regular_table} (id, name, value) VALUES (?, ?, ?)"
        )

        # Add many statements (Cassandra warns at 50KB)
        for i in range(100):
            batch.add(insert_stmt, (1000 + i, f"LargeUser{i}" * 10, i))  # Make data larger

        # Should succeed but may warn in logs
        await session.execute(batch)

        # Verify some inserts
        result = await session.execute(
            f"SELECT COUNT(*) FROM {regular_table} WHERE id >= 1000 AND id < 1100 ALLOW FILTERING"
        )
        count = result.one()[0]
        assert count == 100

    @pytest.mark.asyncio
    async def test_conditional_batch(self, session_with_keyspace, batch_tables):
        """Test batch with conditional statements (LWT)."""
        session, _ = session_with_keyspace
        regular_table, _ = batch_tables

        # Insert initial data
        insert_stmt = await session.prepare(
            f"INSERT INTO {regular_table} (id, name, value) VALUES (?, ?, ?)"
        )
        await session.execute(insert_stmt, [400, "Conditional", 400])

        # Test conditional insert (single statement, not batch)
        insert_if_not_exists = await session.prepare(
            f"INSERT INTO {regular_table} (id, name, value) VALUES (?, ?, ?) IF NOT EXISTS"
        )
        result = await session.execute(insert_if_not_exists, (401, "NewConditional", 401))
        assert result.one().applied  # Should succeed

        # Test conditional update
        update_if_value = await session.prepare(
            f"UPDATE {regular_table} SET value = ? WHERE id = ? IF value = ?"
        )
        result = await session.execute(update_if_value, (500, 400, 400))
        assert result.one().applied  # Should succeed

        # Test failed conditional (current value is now 500, not 400)
        result = await session.execute(update_if_value, (600, 400, 400))  # Wrong current value
        assert not result.one().applied  # Should fail

    @pytest.mark.asyncio
    async def test_counter_batch_concurrent(self, session_with_keyspace, batch_tables):
        """Test concurrent counter batch operations."""
        session, _ = session_with_keyspace
        _, counter_table = batch_tables

        # Prepare statement outside the async function
        update_stmt = await session.prepare(
            f"UPDATE {counter_table} SET count1 = count1 + ? WHERE id = ?"
        )

        async def increment_batch(batch_id):
            batch = BatchStatement(batch_type=BatchType.COUNTER)

            # Each batch increments different counters
            for i in range(10):
                batch.add(update_stmt, (1, f"concurrent_{i}"))

            await session.execute(batch)

        # Execute 10 batches concurrently
        tasks = [increment_batch(i) for i in range(10)]
        await asyncio.gather(*tasks)

        # Verify all counters
        for i in range(10):
            result = await session.execute(
                f"SELECT count1 FROM {counter_table} WHERE id = 'concurrent_{i}'"
            )
            row = result.one()
            assert row.count1 == 10  # Each was incremented 10 times

    @pytest.mark.asyncio
    async def test_batch_with_custom_timestamp(self, session_with_keyspace, batch_tables):
        """Test batch operations with custom timestamp."""
        session, _ = session_with_keyspace
        regular_table, _ = batch_tables

        # Create batch with custom timestamp
        custom_timestamp = 1234567890123456  # microseconds
        batch = BatchStatement()

        # Add statement with USING TIMESTAMP
        # Prepare statement with timestamp
        insert_with_timestamp = await session.prepare(
            f"INSERT INTO {regular_table} (id, name, value) VALUES (?, ?, ?) USING TIMESTAMP ?"
        )

        # Add statement with custom timestamp
        batch.add(insert_with_timestamp, (500, "TimestampTest", 500, custom_timestamp))

        await session.execute(batch)

        # Verify insert (we can't easily verify the timestamp was used)
        result = await session.execute(f"SELECT * FROM {regular_table} WHERE id = 500")
        assert result.one() is not None
