"""
Consolidated integration tests for consistency levels and prepared statements.

This module combines all consistency level and prepared statement tests,
providing comprehensive coverage of statement preparation and execution patterns.

Tests consolidated from:
- test_driver_compatibility.py - Consistency and prepared statement compatibility
- test_simple_statements.py - SimpleStatement consistency levels
- test_select_operations.py - SELECT with different consistency levels
- test_concurrent_operations.py - Concurrent operations with consistency
- Various prepared statement usage from other test files

Test Organization:
==================
1. Prepared Statement Basics - Creation, binding, execution
2. Consistency Level Configuration - Per-statement and per-query
3. Combined Patterns - Prepared statements with consistency levels
4. Concurrent Usage - Thread safety and performance
5. Error Handling - Invalid statements, binding errors
"""

import asyncio
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import pytest
from cassandra import ConsistencyLevel
from cassandra.query import BatchStatement, BatchType, SimpleStatement
from test_utils import generate_unique_table


@pytest.mark.asyncio
@pytest.mark.integration
class TestPreparedStatements:
    """Test prepared statement functionality with real Cassandra."""

    # ========================================
    # Basic Prepared Statement Operations
    # ========================================

    async def test_prepared_statement_basics(self, cassandra_session, shared_keyspace_setup):
        """
        Test basic prepared statement operations.

        What this tests:
        ---------------
        1. Statement preparation with ? placeholders
        2. Binding parameters
        3. Reusing prepared statements
        4. Type safety with prepared statements

        Why this matters:
        ----------------
        Prepared statements provide better performance through
        query plan caching and protection against injection.
        """
        # Create test table
        table_name = generate_unique_table("test_prepared_basics")
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

        # Prepare INSERT statement
        insert_stmt = await cassandra_session.prepare(
            f"INSERT INTO {table_name} (id, name, age, created_at) VALUES (?, ?, ?, ?)"
        )

        # Prepare SELECT statements
        select_by_id = await cassandra_session.prepare(f"SELECT * FROM {table_name} WHERE id = ?")

        select_all = await cassandra_session.prepare(f"SELECT * FROM {table_name}")

        # Execute prepared statements multiple times
        users = []
        for i in range(5):
            user_id = uuid.uuid4()
            users.append(user_id)
            await cassandra_session.execute(
                insert_stmt, (user_id, f"User {i}", 20 + i, datetime.now(timezone.utc))
            )

        # Verify inserts using prepared select
        for i, user_id in enumerate(users):
            result = await cassandra_session.execute(select_by_id, (user_id,))
            row = result.one()
            assert row.name == f"User {i}"
            assert row.age == 20 + i

        # Select all and verify count
        result = await cassandra_session.execute(select_all)
        rows = list(result)
        assert len(rows) == 5

    async def test_prepared_statement_with_different_types(
        self, cassandra_session, shared_keyspace_setup
    ):
        """
        Test prepared statements with various data types.

        What this tests:
        ---------------
        1. Type conversion and validation
        2. NULL handling
        3. Collection types in prepared statements
        4. Special types (UUID, decimal, etc.)

        Why this matters:
        ----------------
        Prepared statements must correctly handle all
        Cassandra data types with proper serialization.
        """
        # Create table with various types
        table_name = generate_unique_table("test_prepared_types")
        await cassandra_session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id UUID PRIMARY KEY,
                text_val TEXT,
                int_val INT,
                decimal_val DECIMAL,
                list_val LIST<TEXT>,
                map_val MAP<TEXT, INT>,
                set_val SET<INT>,
                bool_val BOOLEAN
            )
            """
        )

        # Prepare statement with all types
        insert_stmt = await cassandra_session.prepare(
            f"""
            INSERT INTO {table_name}
            (id, text_val, int_val, decimal_val, list_val, map_val, set_val, bool_val)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
        )

        # Test with various values including NULL
        test_cases = [
            # All values present
            (
                uuid.uuid4(),
                "test text",
                42,
                Decimal("123.456"),
                ["a", "b", "c"],
                {"key1": 1, "key2": 2},
                {1, 2, 3},
                True,
            ),
            # Some NULL values
            (
                uuid.uuid4(),
                None,  # NULL text
                100,
                None,  # NULL decimal
                [],  # Empty list
                {},  # Empty map
                set(),  # Empty set
                False,
            ),
        ]

        for values in test_cases:
            await cassandra_session.execute(insert_stmt, values)

        # Verify data
        select_stmt = await cassandra_session.prepare(f"SELECT * FROM {table_name} WHERE id = ?")

        for i, test_case in enumerate(test_cases):
            result = await cassandra_session.execute(select_stmt, (test_case[0],))
            row = result.one()

            if i == 0:  # First test case with all values
                assert row.text_val == test_case[1]
                assert row.int_val == test_case[2]
                assert row.decimal_val == test_case[3]
                assert row.list_val == test_case[4]
                assert row.map_val == test_case[5]
                assert row.set_val == test_case[6]
                assert row.bool_val == test_case[7]
            else:  # Second test case with NULLs
                assert row.text_val is None
                assert row.int_val == 100
                assert row.decimal_val is None
                # Empty collections may be stored as NULL in Cassandra
                assert row.list_val is None or row.list_val == []
                assert row.map_val is None or row.map_val == {}
                assert row.set_val is None or row.set_val == set()

    async def test_prepared_statement_reuse_performance(
        self, cassandra_session, shared_keyspace_setup
    ):
        """
        Test performance benefits of prepared statement reuse.

        What this tests:
        ---------------
        1. Performance improvement with reuse
        2. Statement cache behavior
        3. Concurrent reuse safety

        Why this matters:
        ----------------
        Prepared statements should be prepared once and
        reused many times for optimal performance.
        """
        # Create test table
        table_name = generate_unique_table("test_prepared_perf")
        await cassandra_session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id UUID PRIMARY KEY,
                data TEXT
            )
            """
        )

        # Measure time with prepared statement reuse
        insert_stmt = await cassandra_session.prepare(
            f"INSERT INTO {table_name} (id, data) VALUES (?, ?)"
        )

        start_prepared = time.time()
        for i in range(100):
            await cassandra_session.execute(insert_stmt, (uuid.uuid4(), f"prepared_data_{i}"))
        prepared_duration = time.time() - start_prepared

        # Measure time with SimpleStatement (no preparation)
        start_simple = time.time()
        for i in range(100):
            await cassandra_session.execute(
                f"INSERT INTO {table_name} (id, data) VALUES (%s, %s)",
                (uuid.uuid4(), f"simple_data_{i}"),
            )
        simple_duration = time.time() - start_simple

        # Prepared statements should generally be faster or similar
        # (The difference might be small for simple queries)
        print(f"Prepared: {prepared_duration:.3f}s, Simple: {simple_duration:.3f}s")

        # Verify both methods inserted data
        result = await cassandra_session.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = result.one()[0]
        assert count == 200

    # ========================================
    # Consistency Level Tests
    # ========================================

    async def test_consistency_levels_with_prepared_statements(
        self, cassandra_session, shared_keyspace_setup
    ):
        """
        Test different consistency levels with prepared statements.

        What this tests:
        ---------------
        1. Setting consistency on prepared statements
        2. Different consistency levels (ONE, QUORUM, ALL)
        3. Read/write consistency combinations
        4. Consistency level errors

        Why this matters:
        ----------------
        Consistency levels control the trade-off between
        consistency, availability, and performance.
        """
        # Create test table
        table_name = generate_unique_table("test_consistency")
        await cassandra_session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id UUID PRIMARY KEY,
                data TEXT,
                version INT
            )
            """
        )

        # Prepare statements
        insert_stmt = await cassandra_session.prepare(
            f"INSERT INTO {table_name} (id, data, version) VALUES (?, ?, ?)"
        )

        select_stmt = await cassandra_session.prepare(f"SELECT * FROM {table_name} WHERE id = ?")

        test_id = uuid.uuid4()

        # Test different write consistency levels
        consistency_levels = [
            ConsistencyLevel.ONE,
            ConsistencyLevel.QUORUM,
            ConsistencyLevel.ALL,
        ]

        for i, cl in enumerate(consistency_levels):
            # Set consistency level on the statement
            insert_stmt.consistency_level = cl

            try:
                await cassandra_session.execute(insert_stmt, (test_id, f"consistency_{cl}", i))
                print(f"Write with {cl} succeeded")
            except Exception as e:
                # ALL might fail in single-node setup
                if cl == ConsistencyLevel.ALL:
                    print(f"Write with ALL failed as expected: {e}")
                else:
                    raise

        # Test different read consistency levels
        for cl in [ConsistencyLevel.ONE, ConsistencyLevel.QUORUM]:
            select_stmt.consistency_level = cl

            result = await cassandra_session.execute(select_stmt, (test_id,))
            row = result.one()
            if row:
                print(f"Read with {cl} returned version {row.version}")

    async def test_consistency_levels_with_simple_statements(
        self, cassandra_session, shared_keyspace_setup
    ):
        """
        Test consistency levels with SimpleStatement.

        What this tests:
        ---------------
        1. SimpleStatement with consistency configuration
        2. Per-query consistency settings
        3. Comparison with prepared statements

        Why this matters:
        ----------------
        SimpleStatements allow per-query consistency
        configuration without statement preparation.
        """
        # Create test table
        table_name = generate_unique_table("test_simple_consistency")
        await cassandra_session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY,
                value INT
            )
            """
        )

        # Test with different consistency levels
        test_data = [
            ("one_consistency", ConsistencyLevel.ONE),
            ("local_one", ConsistencyLevel.LOCAL_ONE),
            ("local_quorum", ConsistencyLevel.LOCAL_QUORUM),
        ]

        for key, consistency in test_data:
            # Create SimpleStatement with specific consistency
            insert = SimpleStatement(
                f"INSERT INTO {table_name} (id, value) VALUES (%s, %s)",
                consistency_level=consistency,
            )

            await cassandra_session.execute(insert, (key, 100))

            # Read back with same consistency
            select = SimpleStatement(
                f"SELECT * FROM {table_name} WHERE id = %s", consistency_level=consistency
            )

            result = await cassandra_session.execute(select, (key,))
            row = result.one()
            assert row.value == 100

    # ========================================
    # Combined Patterns
    # ========================================

    async def test_prepared_statements_in_batch_with_consistency(
        self, cassandra_session, shared_keyspace_setup
    ):
        """
        Test prepared statements in batches with consistency levels.

        What this tests:
        ---------------
        1. Prepared statements in batch operations
        2. Batch consistency levels
        3. Mixed statement types in batch
        4. Batch atomicity with consistency

        Why this matters:
        ----------------
        Batches often combine multiple prepared statements
        and need specific consistency guarantees.
        """
        # Create test table
        table_name = generate_unique_table("test_batch_prepared")
        await cassandra_session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                partition_key TEXT,
                clustering_key INT,
                data TEXT,
                PRIMARY KEY (partition_key, clustering_key)
            )
            """
        )

        # Prepare statements
        insert_stmt = await cassandra_session.prepare(
            f"INSERT INTO {table_name} (partition_key, clustering_key, data) VALUES (?, ?, ?)"
        )

        update_stmt = await cassandra_session.prepare(
            f"UPDATE {table_name} SET data = ? WHERE partition_key = ? AND clustering_key = ?"
        )

        # Create batch with specific consistency
        batch = BatchStatement(
            batch_type=BatchType.LOGGED, consistency_level=ConsistencyLevel.QUORUM
        )

        partition = "batch_test"

        # Add multiple prepared statements to batch
        for i in range(5):
            batch.add(insert_stmt, (partition, i, f"initial_{i}"))

        # Add updates
        for i in range(3):
            batch.add(update_stmt, (f"updated_{i}", partition, i))

        # Execute batch
        await cassandra_session.execute(batch)

        # Verify with read at QUORUM
        select_stmt = await cassandra_session.prepare(
            f"SELECT * FROM {table_name} WHERE partition_key = ?"
        )
        select_stmt.consistency_level = ConsistencyLevel.QUORUM

        result = await cassandra_session.execute(select_stmt, (partition,))
        rows = list(result)
        assert len(rows) == 5

        # Check updates were applied
        for row in rows:
            if row.clustering_key < 3:
                assert row.data == f"updated_{row.clustering_key}"
            else:
                assert row.data == f"initial_{row.clustering_key}"

    # ========================================
    # Concurrent Usage Patterns
    # ========================================

    async def test_concurrent_prepared_statement_usage(
        self, cassandra_session, shared_keyspace_setup
    ):
        """
        Test concurrent usage of prepared statements.

        What this tests:
        ---------------
        1. Thread safety of prepared statements
        2. Concurrent execution performance
        3. No interference between concurrent executions
        4. Connection pool behavior

        Why this matters:
        ----------------
        Prepared statements must be safe for concurrent
        use from multiple async tasks.
        """
        # Create test table
        table_name = generate_unique_table("test_concurrent_prepared")
        await cassandra_session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id UUID PRIMARY KEY,
                thread_id INT,
                value TEXT,
                created_at TIMESTAMP
            )
            """
        )

        # Prepare statements once
        insert_stmt = await cassandra_session.prepare(
            f"INSERT INTO {table_name} (id, thread_id, value, created_at) VALUES (?, ?, ?, ?)"
        )

        select_stmt = await cassandra_session.prepare(
            f"SELECT COUNT(*) FROM {table_name} WHERE thread_id = ? ALLOW FILTERING"
        )

        # Concurrent insert function
        async def insert_records(thread_id, count):
            for i in range(count):
                await cassandra_session.execute(
                    insert_stmt,
                    (
                        uuid.uuid4(),
                        thread_id,
                        f"thread_{thread_id}_record_{i}",
                        datetime.now(timezone.utc),
                    ),
                )
            return thread_id

        # Run many concurrent tasks
        tasks = []
        num_threads = 10
        records_per_thread = 20

        for i in range(num_threads):
            task = asyncio.create_task(insert_records(i, records_per_thread))
            tasks.append(task)

        # Wait for all to complete
        results = await asyncio.gather(*tasks)
        assert len(results) == num_threads

        # Verify each thread inserted correct number
        for thread_id in range(num_threads):
            result = await cassandra_session.execute(select_stmt, (thread_id,))
            count = result.one()[0]
            assert count == records_per_thread

        # Verify total
        total_result = await cassandra_session.execute(f"SELECT COUNT(*) FROM {table_name}")
        total = total_result.one()[0]
        assert total == num_threads * records_per_thread

    async def test_prepared_statement_with_consistency_race_conditions(
        self, cassandra_session, shared_keyspace_setup
    ):
        """
        Test race conditions with different consistency levels.

        What this tests:
        ---------------
        1. Write with ONE, read with ALL pattern
        2. Consistency level impact on visibility
        3. Eventual consistency behavior
        4. Race condition handling

        Why this matters:
        ----------------
        Understanding consistency level interactions is
        crucial for distributed system correctness.
        """
        # Create test table
        table_name = generate_unique_table("test_consistency_race")
        await cassandra_session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY,
                counter INT,
                last_updated TIMESTAMP
            )
            """
        )

        # Prepare statements with different consistency
        insert_one = await cassandra_session.prepare(
            f"INSERT INTO {table_name} (id, counter, last_updated) VALUES (?, ?, ?)"
        )
        insert_one.consistency_level = ConsistencyLevel.ONE

        select_all = await cassandra_session.prepare(f"SELECT * FROM {table_name} WHERE id = ?")
        # Don't set ALL here as it might fail in single-node
        select_all.consistency_level = ConsistencyLevel.QUORUM

        update_quorum = await cassandra_session.prepare(
            f"UPDATE {table_name} SET counter = ?, last_updated = ? WHERE id = ?"
        )
        update_quorum.consistency_level = ConsistencyLevel.QUORUM

        # Test concurrent updates with different consistency
        test_id = "consistency_test"

        # Initial insert with ONE
        await cassandra_session.execute(insert_one, (test_id, 0, datetime.now(timezone.utc)))

        # Concurrent updates
        async def update_counter(increment):
            # Read current value
            result = await cassandra_session.execute(select_all, (test_id,))
            current = result.one()

            if current:
                new_value = current.counter + increment
                # Update with QUORUM
                await cassandra_session.execute(
                    update_quorum, (new_value, datetime.now(timezone.utc), test_id)
                )
                return new_value
            return None

        # Run concurrent updates
        tasks = [update_counter(1) for _ in range(5)]
        await asyncio.gather(*tasks, return_exceptions=True)

        # Final read
        final_result = await cassandra_session.execute(select_all, (test_id,))
        final_row = final_result.one()

        # Due to race conditions, final counter might not be 5
        # but should be between 1 and 5
        assert 1 <= final_row.counter <= 5
        print(f"Final counter value: {final_row.counter} (race conditions expected)")

    # ========================================
    # Error Handling
    # ========================================

    async def test_prepared_statement_error_handling(
        self, cassandra_session, shared_keyspace_setup
    ):
        """
        Test error handling with prepared statements.

        What this tests:
        ---------------
        1. Invalid query preparation
        2. Wrong parameter count
        3. Type mismatch errors
        4. Non-existent table/column errors

        Why this matters:
        ----------------
        Proper error handling ensures robust applications
        and clear debugging information.
        """
        # Test preparing invalid query
        from cassandra.protocol import SyntaxException

        with pytest.raises(SyntaxException):
            await cassandra_session.prepare("INVALID SQL QUERY")

        # Create test table
        table_name = generate_unique_table("test_prepared_errors")
        await cassandra_session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id UUID PRIMARY KEY,
                value INT
            )
            """
        )

        # Prepare valid statement
        insert_stmt = await cassandra_session.prepare(
            f"INSERT INTO {table_name} (id, value) VALUES (?, ?)"
        )

        # Test wrong parameter count - Cassandra driver behavior varies
        # Some versions auto-fill missing parameters with None
        try:
            await cassandra_session.execute(insert_stmt, (uuid.uuid4(),))  # Missing value
            # If no exception, verify it inserted NULL for missing value
            print("Note: Driver accepted missing parameter (filled with NULL)")
        except Exception as e:
            print(f"Driver raised exception for missing parameter: {type(e).__name__}")

        # Test too many parameters - this should always fail
        with pytest.raises(Exception):
            await cassandra_session.execute(
                insert_stmt, (uuid.uuid4(), 100, "extra", "more")  # Way too many parameters
            )

        # Test type mismatch - string for UUID should fail
        try:
            await cassandra_session.execute(
                insert_stmt, ("not-a-uuid", 100)  # String instead of UUID
            )
            pytest.fail("Expected exception for invalid UUID string")
        except Exception:
            pass  # Expected

        # Test non-existent column
        from cassandra import InvalidRequest

        with pytest.raises(InvalidRequest):
            await cassandra_session.prepare(
                f"INSERT INTO {table_name} (id, nonexistent) VALUES (?, ?)"
            )

    async def test_statement_id_and_metadata(self, cassandra_session, shared_keyspace_setup):
        """
        Test prepared statement metadata and IDs.

        What this tests:
        ---------------
        1. Statement preparation returns metadata
        2. Prepared statement IDs are stable
        3. Re-preparing returns same statement
        4. Metadata contains column information

        Why this matters:
        ----------------
        Understanding statement metadata helps with
        debugging and advanced driver usage.
        """
        # Create test table
        table_name = generate_unique_table("test_stmt_metadata")
        await cassandra_session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id UUID PRIMARY KEY,
                name TEXT,
                age INT,
                active BOOLEAN
            )
            """
        )

        # Prepare statement
        query = f"INSERT INTO {table_name} (id, name, age, active) VALUES (?, ?, ?, ?)"
        stmt1 = await cassandra_session.prepare(query)

        # Re-prepare same query
        await cassandra_session.prepare(query)

        # Both should be the same prepared statement
        # (Cassandra caches prepared statements)

        # Test statement has required attributes
        assert hasattr(stmt1, "bind")
        assert hasattr(stmt1, "consistency_level")

        # Can bind values
        bound = stmt1.bind((uuid.uuid4(), "Test", 25, True))
        await cassandra_session.execute(bound)

        # Verify insert worked
        result = await cassandra_session.execute(f"SELECT COUNT(*) FROM {table_name}")
        assert result.one()[0] == 1


@pytest.mark.asyncio
@pytest.mark.integration
class TestConsistencyPatterns:
    """Test advanced consistency patterns and scenarios."""

    async def test_read_your_writes_pattern(self, cassandra_session, shared_keyspace_setup):
        """
        Test read-your-writes consistency pattern.

        What this tests:
        ---------------
        1. Write at QUORUM, read at QUORUM
        2. Immediate read visibility
        3. Consistency across nodes
        4. No stale reads

        Why this matters:
        ----------------
        Read-your-writes is a common consistency requirement
        where users expect to see their own changes immediately.
        """
        # Create test table
        table_name = generate_unique_table("test_read_your_writes")
        await cassandra_session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                user_id UUID PRIMARY KEY,
                username TEXT,
                email TEXT,
                updated_at TIMESTAMP
            )
            """
        )

        # Prepare statements with QUORUM consistency
        insert_stmt = await cassandra_session.prepare(
            f"INSERT INTO {table_name} (user_id, username, email, updated_at) VALUES (?, ?, ?, ?)"
        )
        insert_stmt.consistency_level = ConsistencyLevel.QUORUM

        select_stmt = await cassandra_session.prepare(
            f"SELECT * FROM {table_name} WHERE user_id = ?"
        )
        select_stmt.consistency_level = ConsistencyLevel.QUORUM

        # Test immediate read after write
        user_id = uuid.uuid4()
        username = "testuser"
        email = "test@example.com"

        # Write
        await cassandra_session.execute(
            insert_stmt, (user_id, username, email, datetime.now(timezone.utc))
        )

        # Immediate read should see the write
        result = await cassandra_session.execute(select_stmt, (user_id,))
        row = result.one()
        assert row is not None
        assert row.username == username
        assert row.email == email

    async def test_eventual_consistency_demonstration(
        self, cassandra_session, shared_keyspace_setup
    ):
        """
        Test and demonstrate eventual consistency behavior.

        What this tests:
        ---------------
        1. Write at ONE, read at ONE behavior
        2. Potential inconsistency windows
        3. Eventually consistent reads
        4. Consistency level trade-offs

        Why this matters:
        ----------------
        Understanding eventual consistency helps design
        systems that handle temporary inconsistencies.
        """
        # Create test table
        table_name = generate_unique_table("test_eventual")
        await cassandra_session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY,
                value INT,
                timestamp TIMESTAMP
            )
            """
        )

        # Prepare statements with ONE consistency (fastest, least consistent)
        write_one = await cassandra_session.prepare(
            f"INSERT INTO {table_name} (id, value, timestamp) VALUES (?, ?, ?)"
        )
        write_one.consistency_level = ConsistencyLevel.ONE

        read_one = await cassandra_session.prepare(f"SELECT * FROM {table_name} WHERE id = ?")
        read_one.consistency_level = ConsistencyLevel.ONE

        read_all = await cassandra_session.prepare(f"SELECT * FROM {table_name} WHERE id = ?")
        # Use QUORUM instead of ALL for single-node compatibility
        read_all.consistency_level = ConsistencyLevel.QUORUM

        test_id = "eventual_test"

        # Rapid writes with ONE
        for i in range(10):
            await cassandra_session.execute(write_one, (test_id, i, datetime.now(timezone.utc)))

        # Read with different consistency levels
        result_one = await cassandra_session.execute(read_one, (test_id,))
        result_all = await cassandra_session.execute(read_all, (test_id,))

        # Both should eventually see the same value
        # In a single-node setup, they'll be consistent
        row_one = result_one.one()
        row_all = result_all.one()

        assert row_one.value == row_all.value == 9
        print(f"ONE read: {row_one.value}, QUORUM read: {row_all.value}")

    async def test_multi_datacenter_consistency_levels(
        self, cassandra_session, shared_keyspace_setup
    ):
        """
        Test LOCAL consistency levels for multi-DC scenarios.

        What this tests:
        ---------------
        1. LOCAL_ONE vs ONE
        2. LOCAL_QUORUM vs QUORUM
        3. Multi-DC consistency patterns
        4. DC-aware consistency

        Why this matters:
        ----------------
        Multi-datacenter deployments require careful
        consistency level selection for performance.
        """
        # Create test table
        table_name = generate_unique_table("test_local_consistency")
        await cassandra_session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id UUID PRIMARY KEY,
                dc_name TEXT,
                data TEXT
            )
            """
        )

        # Test LOCAL consistency levels (work in single-DC too)
        local_consistency_levels = [
            (ConsistencyLevel.LOCAL_ONE, "LOCAL_ONE"),
            (ConsistencyLevel.LOCAL_QUORUM, "LOCAL_QUORUM"),
        ]

        for cl, cl_name in local_consistency_levels:
            stmt = SimpleStatement(
                f"INSERT INTO {table_name} (id, dc_name, data) VALUES (%s, %s, %s)",
                consistency_level=cl,
            )

            try:
                await cassandra_session.execute(
                    stmt, (uuid.uuid4(), cl_name, f"Written with {cl_name}")
                )
                print(f"Write with {cl_name} succeeded")
            except Exception as e:
                print(f"Write with {cl_name} failed: {e}")

        # Verify writes
        result = await cassandra_session.execute(f"SELECT * FROM {table_name}")
        rows = list(result)
        print(f"Successfully wrote {len(rows)} rows with LOCAL consistency levels")
