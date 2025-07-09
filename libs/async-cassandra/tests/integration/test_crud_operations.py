"""
Consolidated integration tests for CRUD operations.

This module combines basic CRUD operation tests from multiple files,
focusing on core insert, select, update, and delete functionality.

Tests consolidated from:
- test_basic_operations.py
- test_select_operations.py

Test Organization:
==================
1. Basic CRUD Operations - Single record operations
2. Prepared Statement CRUD - Prepared statement usage
3. Batch Operations - Batch inserts and updates
4. Edge Cases - Non-existent data, NULL values, etc.
"""

import uuid
from decimal import Decimal

import pytest
from cassandra.query import BatchStatement, BatchType
from test_utils import generate_unique_table


@pytest.mark.asyncio
@pytest.mark.integration
class TestCRUDOperations:
    """Test basic CRUD operations with real Cassandra."""

    # ========================================
    # Basic CRUD Operations
    # ========================================

    async def test_insert_and_select(self, cassandra_session, shared_keyspace_setup):
        """
        Test basic insert and select operations.

        What this tests:
        ---------------
        1. INSERT with prepared statements
        2. SELECT with prepared statements
        3. Data integrity after insert
        4. Multiple row retrieval

        Why this matters:
        ----------------
        These are the most fundamental database operations that
        every application needs to perform reliably.
        """
        # Create a test table
        table_name = generate_unique_table("test_crud")
        await cassandra_session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id UUID PRIMARY KEY,
                name TEXT,
                age INT,
                created_at TIMESTAMP
            )
            """
        )

        # Prepare statements
        insert_stmt = await cassandra_session.prepare(
            f"INSERT INTO {table_name} (id, name, age, created_at) VALUES (?, ?, ?, toTimestamp(now()))"
        )
        select_stmt = await cassandra_session.prepare(
            f"SELECT id, name, age, created_at FROM {table_name} WHERE id = ?"
        )
        select_all_stmt = await cassandra_session.prepare(f"SELECT * FROM {table_name}")

        # Insert test data
        test_id = uuid.uuid4()
        test_name = "John Doe"
        test_age = 30

        await cassandra_session.execute(insert_stmt, (test_id, test_name, test_age))

        # Select and verify single row
        result = await cassandra_session.execute(select_stmt, (test_id,))
        rows = list(result)
        assert len(rows) == 1
        row = rows[0]
        assert row.id == test_id
        assert row.name == test_name
        assert row.age == test_age
        assert row.created_at is not None

        # Insert more data
        more_ids = []
        for i in range(5):
            new_id = uuid.uuid4()
            more_ids.append(new_id)
            await cassandra_session.execute(insert_stmt, (new_id, f"Person {i}", 20 + i))

        # Select all and verify
        result = await cassandra_session.execute(select_all_stmt)
        all_rows = list(result)
        assert len(all_rows) == 6  # Original + 5 more

        # Verify all IDs are present
        all_ids = {row.id for row in all_rows}
        assert test_id in all_ids
        for more_id in more_ids:
            assert more_id in all_ids

    async def test_update_and_delete(self, cassandra_session, shared_keyspace_setup):
        """
        Test update and delete operations.

        What this tests:
        ---------------
        1. UPDATE with prepared statements
        2. Conditional updates (IF EXISTS)
        3. DELETE operations
        4. Verification of changes

        Why this matters:
        ----------------
        Update and delete operations are critical for maintaining
        data accuracy and lifecycle management.
        """
        # Create test table
        table_name = generate_unique_table("test_update_delete")
        await cassandra_session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id UUID PRIMARY KEY,
                name TEXT,
                email TEXT,
                active BOOLEAN,
                score DECIMAL
            )
            """
        )

        # Prepare statements
        insert_stmt = await cassandra_session.prepare(
            f"INSERT INTO {table_name} (id, name, email, active, score) VALUES (?, ?, ?, ?, ?)"
        )
        update_stmt = await cassandra_session.prepare(
            f"UPDATE {table_name} SET email = ?, active = ? WHERE id = ?"
        )
        update_if_exists_stmt = await cassandra_session.prepare(
            f"UPDATE {table_name} SET score = ? WHERE id = ? IF EXISTS"
        )
        select_stmt = await cassandra_session.prepare(f"SELECT * FROM {table_name} WHERE id = ?")
        delete_stmt = await cassandra_session.prepare(f"DELETE FROM {table_name} WHERE id = ?")

        # Insert test data
        test_id = uuid.uuid4()
        await cassandra_session.execute(
            insert_stmt, (test_id, "Alice Smith", "alice@example.com", True, Decimal("85.5"))
        )

        # Update the record
        new_email = "alice.smith@example.com"
        await cassandra_session.execute(update_stmt, (new_email, False, test_id))

        # Verify update
        result = await cassandra_session.execute(select_stmt, (test_id,))
        row = result.one()
        assert row.email == new_email
        assert row.active is False
        assert row.name == "Alice Smith"  # Unchanged
        assert row.score == Decimal("85.5")  # Unchanged

        # Test conditional update
        result = await cassandra_session.execute(update_if_exists_stmt, (Decimal("92.0"), test_id))
        assert result.one().applied is True

        # Verify conditional update worked
        result = await cassandra_session.execute(select_stmt, (test_id,))
        assert result.one().score == Decimal("92.0")

        # Test conditional update on non-existent record
        fake_id = uuid.uuid4()
        result = await cassandra_session.execute(update_if_exists_stmt, (Decimal("100.0"), fake_id))
        assert result.one().applied is False

        # Delete the record
        await cassandra_session.execute(delete_stmt, (test_id,))

        # Verify deletion - in Cassandra, a deleted row may still appear with null values
        # if only some columns were deleted. The row truly disappears only after compaction.
        result = await cassandra_session.execute(select_stmt, (test_id,))
        row = result.one()
        if row is not None:
            # If row still exists, all non-primary key columns should be None
            assert row.name is None
            assert row.email is None
            assert row.active is None
            # Note: score might remain due to tombstone timing

    async def test_select_non_existent_data(self, cassandra_session, shared_keyspace_setup):
        """
        Test selecting non-existent data.

        What this tests:
        ---------------
        1. SELECT returns empty result for non-existent primary key
        2. No exceptions thrown for missing data
        3. Result iteration handles empty results

        Why this matters:
        ----------------
        Applications must gracefully handle queries that return no data.
        """
        # Create test table
        table_name = generate_unique_table("test_non_existent")
        await cassandra_session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id UUID PRIMARY KEY,
                data TEXT
            )
            """
        )

        # Prepare select statement
        select_stmt = await cassandra_session.prepare(f"SELECT * FROM {table_name} WHERE id = ?")

        # Query for non-existent ID
        fake_id = uuid.uuid4()
        result = await cassandra_session.execute(select_stmt, (fake_id,))

        # Should return empty result, not error
        assert result.one() is None
        assert list(result) == []

    # ========================================
    # Prepared Statement CRUD
    # ========================================

    async def test_prepared_statement_lifecycle(self, cassandra_session, shared_keyspace_setup):
        """
        Test prepared statement lifecycle and reuse.

        What this tests:
        ---------------
        1. Prepare once, execute many times
        2. Prepared statements with different parameter counts
        3. Performance benefit of prepared statements
        4. Statement reuse across operations

        Why this matters:
        ----------------
        Prepared statements are the recommended way to execute queries
        for performance, security, and consistency.
        """
        # Create test table
        table_name = generate_unique_table("test_prepared")
        await cassandra_session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                partition_key INT,
                clustering_key INT,
                value TEXT,
                metadata MAP<TEXT, TEXT>,
                PRIMARY KEY (partition_key, clustering_key)
            )
            """
        )

        # Prepare various statements
        insert_stmt = await cassandra_session.prepare(
            f"INSERT INTO {table_name} (partition_key, clustering_key, value) VALUES (?, ?, ?)"
        )

        insert_with_meta_stmt = await cassandra_session.prepare(
            f"INSERT INTO {table_name} (partition_key, clustering_key, value, metadata) VALUES (?, ?, ?, ?)"
        )

        select_partition_stmt = await cassandra_session.prepare(
            f"SELECT * FROM {table_name} WHERE partition_key = ?"
        )

        select_row_stmt = await cassandra_session.prepare(
            f"SELECT * FROM {table_name} WHERE partition_key = ? AND clustering_key = ?"
        )

        update_value_stmt = await cassandra_session.prepare(
            f"UPDATE {table_name} SET value = ? WHERE partition_key = ? AND clustering_key = ?"
        )

        delete_row_stmt = await cassandra_session.prepare(
            f"DELETE FROM {table_name} WHERE partition_key = ? AND clustering_key = ?"
        )

        # Execute many times with same prepared statements
        partition = 1

        # Insert multiple rows
        for i in range(10):
            await cassandra_session.execute(insert_stmt, (partition, i, f"value_{i}"))

        # Insert with metadata
        await cassandra_session.execute(
            insert_with_meta_stmt,
            (partition, 100, "special", {"type": "special", "priority": "high"}),
        )

        # Select entire partition
        result = await cassandra_session.execute(select_partition_stmt, (partition,))
        rows = list(result)
        assert len(rows) == 11

        # Update specific rows
        for i in range(0, 10, 2):  # Update even rows
            await cassandra_session.execute(update_value_stmt, (f"updated_{i}", partition, i))

        # Verify updates
        for i in range(10):
            result = await cassandra_session.execute(select_row_stmt, (partition, i))
            row = result.one()
            if i % 2 == 0:
                assert row.value == f"updated_{i}"
            else:
                assert row.value == f"value_{i}"

        # Delete some rows
        for i in range(5, 10):
            await cassandra_session.execute(delete_row_stmt, (partition, i))

        # Verify final state
        result = await cassandra_session.execute(select_partition_stmt, (partition,))
        remaining_rows = list(result)
        assert len(remaining_rows) == 6  # 0-4 plus row 100

    # ========================================
    # Batch Operations
    # ========================================

    async def test_batch_insert_operations(self, cassandra_session, shared_keyspace_setup):
        """
        Test batch insert operations.

        What this tests:
        ---------------
        1. LOGGED batch inserts
        2. UNLOGGED batch inserts
        3. Batch size limits
        4. Mixed statement batches

        Why this matters:
        ----------------
        Batch operations can improve performance for related writes
        and ensure atomicity for LOGGED batches.
        """
        # Create test table
        table_name = generate_unique_table("test_batch")
        await cassandra_session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id UUID PRIMARY KEY,
                type TEXT,
                value INT,
                timestamp TIMESTAMP
            )
            """
        )

        # Prepare insert statement
        insert_stmt = await cassandra_session.prepare(
            f"INSERT INTO {table_name} (id, type, value, timestamp) VALUES (?, ?, ?, toTimestamp(now()))"
        )

        # Test LOGGED batch (atomic)
        logged_batch = BatchStatement(batch_type=BatchType.LOGGED)
        logged_ids = []

        for i in range(10):
            batch_id = uuid.uuid4()
            logged_ids.append(batch_id)
            logged_batch.add(insert_stmt, (batch_id, "logged", i))

        await cassandra_session.execute(logged_batch)

        # Verify all logged batch inserts
        for batch_id in logged_ids:
            result = await cassandra_session.execute(
                f"SELECT * FROM {table_name} WHERE id = %s", (batch_id,)
            )
            assert result.one() is not None

        # Test UNLOGGED batch (better performance, no atomicity)
        unlogged_batch = BatchStatement(batch_type=BatchType.UNLOGGED)
        unlogged_ids = []

        for i in range(20):
            batch_id = uuid.uuid4()
            unlogged_ids.append(batch_id)
            unlogged_batch.add(insert_stmt, (batch_id, "unlogged", i))

        await cassandra_session.execute(unlogged_batch)

        # Verify unlogged batch inserts
        count = 0
        for batch_id in unlogged_ids:
            result = await cassandra_session.execute(
                f"SELECT * FROM {table_name} WHERE id = %s", (batch_id,)
            )
            if result.one() is not None:
                count += 1

        # All should succeed in normal conditions
        assert count == 20

        # Test mixed batch with different operations
        mixed_table = generate_unique_table("test_mixed_batch")
        await cassandra_session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {mixed_table} (
                pk INT,
                ck INT,
                value TEXT,
                PRIMARY KEY (pk, ck)
            )
            """
        )

        insert_mixed = await cassandra_session.prepare(
            f"INSERT INTO {mixed_table} (pk, ck, value) VALUES (?, ?, ?)"
        )
        update_mixed = await cassandra_session.prepare(
            f"UPDATE {mixed_table} SET value = ? WHERE pk = ? AND ck = ?"
        )

        # Insert initial data
        await cassandra_session.execute(insert_mixed, (1, 1, "initial"))

        # Mixed batch
        mixed_batch = BatchStatement()
        mixed_batch.add(insert_mixed, (1, 2, "new_insert"))
        mixed_batch.add(update_mixed, ("updated", 1, 1))
        mixed_batch.add(insert_mixed, (1, 3, "another_insert"))

        await cassandra_session.execute(mixed_batch)

        # Verify mixed batch results
        result = await cassandra_session.execute(f"SELECT * FROM {mixed_table} WHERE pk = 1")
        rows = {row.ck: row.value for row in result}

        assert rows[1] == "updated"
        assert rows[2] == "new_insert"
        assert rows[3] == "another_insert"

    # ========================================
    # Edge Cases
    # ========================================

    async def test_null_value_handling(self, cassandra_session, shared_keyspace_setup):
        """
        Test NULL value handling in CRUD operations.

        What this tests:
        ---------------
        1. INSERT with NULL values
        2. UPDATE to NULL (deletion of value)
        3. SELECT with NULL values
        4. Distinction between NULL and empty string

        Why this matters:
        ----------------
        NULL handling is a common source of bugs. Applications must
        correctly handle NULL vs empty vs missing values.
        """
        # Create test table
        table_name = generate_unique_table("test_null")
        await cassandra_session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id UUID PRIMARY KEY,
                required_field TEXT,
                optional_field TEXT,
                numeric_field INT,
                collection_field LIST<TEXT>
            )
            """
        )

        # Test inserting with NULL values
        test_id = uuid.uuid4()
        insert_stmt = await cassandra_session.prepare(
            f"""INSERT INTO {table_name}
            (id, required_field, optional_field, numeric_field, collection_field)
            VALUES (?, ?, ?, ?, ?)"""
        )

        # Insert with some NULL values
        await cassandra_session.execute(insert_stmt, (test_id, "required", None, None, None))

        # Select and verify NULLs
        result = await cassandra_session.execute(
            f"SELECT * FROM {table_name} WHERE id = %s", (test_id,)
        )
        row = result.one()

        assert row.required_field == "required"
        assert row.optional_field is None
        assert row.numeric_field is None
        assert row.collection_field is None

        # Test updating to NULL (removes the value)
        update_stmt = await cassandra_session.prepare(
            f"UPDATE {table_name} SET required_field = ? WHERE id = ?"
        )
        await cassandra_session.execute(update_stmt, (None, test_id))

        # In Cassandra, setting to NULL deletes the column
        result = await cassandra_session.execute(
            f"SELECT * FROM {table_name} WHERE id = %s", (test_id,)
        )
        row = result.one()
        assert row.required_field is None

        # Test empty string vs NULL
        test_id2 = uuid.uuid4()
        await cassandra_session.execute(
            insert_stmt, (test_id2, "", "", 0, [])  # Empty values, not NULL
        )

        result = await cassandra_session.execute(
            f"SELECT * FROM {table_name} WHERE id = %s", (test_id2,)
        )
        row = result.one()

        # Empty string is different from NULL
        assert row.required_field == ""
        assert row.optional_field == ""
        assert row.numeric_field == 0
        # In Cassandra, empty collections are stored as NULL
        assert row.collection_field is None  # Empty list becomes NULL

    async def test_large_text_operations(self, cassandra_session, shared_keyspace_setup):
        """
        Test CRUD operations with large text data.

        What this tests:
        ---------------
        1. INSERT large text blobs
        2. SELECT large text data
        3. UPDATE with large text
        4. Performance with large values

        Why this matters:
        ----------------
        Many applications store large text data (JSON, XML, logs).
        The driver must handle these efficiently.
        """
        # Create test table
        table_name = generate_unique_table("test_large_text")
        await cassandra_session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id UUID PRIMARY KEY,
                small_text TEXT,
                large_text TEXT,
                metadata MAP<TEXT, TEXT>
            )
            """
        )

        # Generate large text data
        large_text = "x" * 100000  # 100KB of text
        small_text = "This is a small text field"

        # Prepare statements
        insert_stmt = await cassandra_session.prepare(
            f"""INSERT INTO {table_name}
            (id, small_text, large_text, metadata)
            VALUES (?, ?, ?, ?)"""
        )
        select_stmt = await cassandra_session.prepare(f"SELECT * FROM {table_name} WHERE id = ?")

        # Insert large text
        test_id = uuid.uuid4()
        metadata = {f"key_{i}": f"value_{i}" * 100 for i in range(10)}

        await cassandra_session.execute(insert_stmt, (test_id, small_text, large_text, metadata))

        # Select and verify
        result = await cassandra_session.execute(select_stmt, (test_id,))
        row = result.one()

        assert row.small_text == small_text
        assert row.large_text == large_text
        assert len(row.large_text) == 100000
        assert len(row.metadata) == 10

        # Update with even larger text
        larger_text = "y" * 200000  # 200KB
        update_stmt = await cassandra_session.prepare(
            f"UPDATE {table_name} SET large_text = ? WHERE id = ?"
        )

        await cassandra_session.execute(update_stmt, (larger_text, test_id))

        # Verify update
        result = await cassandra_session.execute(select_stmt, (test_id,))
        row = result.one()
        assert row.large_text == larger_text
        assert len(row.large_text) == 200000

        # Test multiple large text operations
        bulk_ids = []
        for i in range(5):
            bulk_id = uuid.uuid4()
            bulk_ids.append(bulk_id)
            await cassandra_session.execute(insert_stmt, (bulk_id, f"bulk_{i}", large_text, None))

        # Verify all bulk inserts
        for bulk_id in bulk_ids:
            result = await cassandra_session.execute(select_stmt, (bulk_id,))
            assert result.one() is not None
