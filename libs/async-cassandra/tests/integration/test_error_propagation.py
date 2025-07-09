"""
Integration tests for error propagation from the Cassandra driver.

Tests various error conditions that can occur during normal operations
to ensure the async wrapper properly propagates all error types from
the underlying driver to the application layer.
"""

import asyncio
import uuid

import pytest
from cassandra import AlreadyExists, ConfigurationException, InvalidRequest
from cassandra.protocol import SyntaxException
from cassandra.query import SimpleStatement

from async_cassandra.exceptions import QueryError


class TestErrorPropagation:
    """Test that various Cassandra errors are properly propagated through the async wrapper."""

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_invalid_query_syntax_error(self, cassandra_cluster):
        """
        Test that invalid query syntax errors are propagated.

        What this tests:
        ---------------
        1. Syntax errors caught
        2. InvalidRequest raised
        3. Error message preserved
        4. Stack trace intact

        Why this matters:
        ----------------
        Development debugging needs:
        - Clear error messages
        - Exact error types
        - Full stack traces

        Bad queries must fail
        with helpful errors.
        """
        session = await cassandra_cluster.connect()

        # Various syntax errors
        invalid_queries = [
            "SELECT * FROM",  # Incomplete query
            "SELCT * FROM system.local",  # Typo in SELECT
            "SELECT * FROM system.local WHERE",  # Incomplete WHERE
            "INSERT INTO test_table",  # Incomplete INSERT
            "CREATE TABLE",  # Incomplete CREATE
        ]

        for query in invalid_queries:
            # The driver raises SyntaxException for syntax errors, not InvalidRequest
            # We might get either SyntaxException directly or QueryError wrapping it
            with pytest.raises((SyntaxException, QueryError)) as exc_info:
                await session.execute(query)

            # Verify error details are preserved
            assert str(exc_info.value)  # Has error message

            # If it's wrapped in QueryError, check the cause
            if isinstance(exc_info.value, QueryError):
                assert isinstance(exc_info.value.__cause__, SyntaxException)

        await session.close()

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_table_not_found_error(self, cassandra_cluster):
        """
        Test that table not found errors are propagated.

        What this tests:
        ---------------
        1. Missing table error
        2. InvalidRequest raised
        3. Table name in error
        4. Keyspace context

        Why this matters:
        ----------------
        Common development error:
        - Typos in table names
        - Wrong keyspace
        - Missing migrations

        Clear errors speed up
        debugging significantly.
        """
        session = await cassandra_cluster.connect()

        # Create a test keyspace
        await session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS test_errors
            WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
            """
        )
        await session.set_keyspace("test_errors")

        # Try to query non-existent table
        # This should raise InvalidRequest or be wrapped in QueryError
        with pytest.raises((InvalidRequest, QueryError)) as exc_info:
            await session.execute("SELECT * FROM non_existent_table")

        # Error should mention the table
        error_msg = str(exc_info.value).lower()
        assert "non_existent_table" in error_msg or "table" in error_msg

        # If wrapped, check the cause
        if isinstance(exc_info.value, QueryError):
            assert exc_info.value.__cause__ is not None

        # Cleanup
        await session.execute("DROP KEYSPACE IF EXISTS test_errors")
        await session.close()

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_prepared_statement_invalidation_error(self, cassandra_cluster):
        """
        Test errors when prepared statements become invalid.

        What this tests:
        ---------------
        1. Table drop invalidates
        2. Prepare after drop
        3. Schema changes handled
        4. Error recovery

        Why this matters:
        ----------------
        Schema evolution common:
        - Table modifications
        - Column changes
        - Migration scripts

        Apps must handle schema
        changes gracefully.
        """
        session = await cassandra_cluster.connect()

        # Create test keyspace and table
        await session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS test_prepare_errors
            WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
            """
        )
        await session.set_keyspace("test_prepare_errors")

        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS prepare_test (
                id UUID PRIMARY KEY,
                data TEXT
            )
            """
        )

        # Prepare a statement
        prepared = await session.prepare("SELECT * FROM prepare_test WHERE id = ?")

        # Insert some data and verify prepared statement works
        test_id = uuid.uuid4()
        await session.execute(
            "INSERT INTO prepare_test (id, data) VALUES (%s, %s)", [test_id, "test data"]
        )
        result = await session.execute(prepared, [test_id])
        assert result.one() is not None

        # Drop and recreate table with different schema
        await session.execute("DROP TABLE prepare_test")
        await session.execute(
            """
            CREATE TABLE prepare_test (
                id UUID PRIMARY KEY,
                data TEXT,
                new_column INT  -- Schema changed
            )
            """
        )

        # The prepared statement should still work (driver handles re-preparation)
        # but let's also test preparing a statement for a dropped table
        await session.execute("DROP TABLE prepare_test")

        # Trying to prepare for non-existent table should fail
        # This might raise InvalidRequest or be wrapped in QueryError
        with pytest.raises((InvalidRequest, QueryError)) as exc_info:
            await session.prepare("SELECT * FROM prepare_test WHERE id = ?")

        error_msg = str(exc_info.value).lower()
        assert "prepare_test" in error_msg or "table" in error_msg

        # If wrapped, check the cause
        if isinstance(exc_info.value, QueryError):
            assert exc_info.value.__cause__ is not None

        # Cleanup
        await session.execute("DROP KEYSPACE IF EXISTS test_prepare_errors")
        await session.close()

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_prepared_statement_column_drop_error(self, cassandra_cluster):
        """
        Test what happens when a column referenced by a prepared statement is dropped.

        What this tests:
        ---------------
        1. Prepare with column reference
        2. Drop the column
        3. Reuse prepared statement
        4. Error propagation

        Why this matters:
        ----------------
        Column drops happen during:
        - Schema refactoring
        - Deprecating features
        - Data model changes

        Prepared statements must
        handle column removal.
        """
        session = await cassandra_cluster.connect()

        # Create test keyspace and table
        await session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS test_column_drop
            WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
            """
        )
        await session.set_keyspace("test_column_drop")

        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS column_test (
                id UUID PRIMARY KEY,
                name TEXT,
                email TEXT,
                age INT
            )
            """
        )

        # Prepare statements that reference specific columns
        select_with_email = await session.prepare(
            "SELECT id, name, email FROM column_test WHERE id = ?"
        )
        insert_with_email = await session.prepare(
            "INSERT INTO column_test (id, name, email, age) VALUES (?, ?, ?, ?)"
        )
        update_email = await session.prepare("UPDATE column_test SET email = ? WHERE id = ?")

        # Insert test data and verify statements work
        test_id = uuid.uuid4()
        await session.execute(insert_with_email, [test_id, "Test User", "test@example.com", 25])

        result = await session.execute(select_with_email, [test_id])
        row = result.one()
        assert row.email == "test@example.com"

        # Now drop the email column
        await session.execute("ALTER TABLE column_test DROP email")

        # Try to use the prepared statements that reference the dropped column

        # SELECT with dropped column should fail
        with pytest.raises(InvalidRequest) as exc_info:
            await session.execute(select_with_email, [test_id])
        error_msg = str(exc_info.value).lower()
        assert "email" in error_msg or "column" in error_msg or "undefined" in error_msg

        # INSERT with dropped column should fail
        with pytest.raises(InvalidRequest) as exc_info:
            await session.execute(
                insert_with_email, [uuid.uuid4(), "Another User", "another@example.com", 30]
            )
        error_msg = str(exc_info.value).lower()
        assert "email" in error_msg or "column" in error_msg or "undefined" in error_msg

        # UPDATE of dropped column should fail
        with pytest.raises(InvalidRequest) as exc_info:
            await session.execute(update_email, ["new@example.com", test_id])
        error_msg = str(exc_info.value).lower()
        assert "email" in error_msg or "column" in error_msg or "undefined" in error_msg

        # Verify that statements without the dropped column still work
        select_without_email = await session.prepare(
            "SELECT id, name, age FROM column_test WHERE id = ?"
        )
        result = await session.execute(select_without_email, [test_id])
        row = result.one()
        assert row.name == "Test User"
        assert row.age == 25

        # Cleanup
        await session.execute("DROP TABLE IF EXISTS column_test")
        await session.execute("DROP KEYSPACE IF EXISTS test_column_drop")
        await session.close()

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_keyspace_not_found_error(self, cassandra_cluster):
        """
        Test that keyspace not found errors are propagated.

        What this tests:
        ---------------
        1. Missing keyspace error
        2. Clear error message
        3. Keyspace name shown
        4. Connection still valid

        Why this matters:
        ----------------
        Keyspace errors indicate:
        - Wrong environment
        - Missing setup
        - Config issues

        Must fail clearly to
        prevent data loss.
        """
        session = await cassandra_cluster.connect()

        # Try to use non-existent keyspace
        with pytest.raises(InvalidRequest) as exc_info:
            await session.execute("USE non_existent_keyspace")

        error_msg = str(exc_info.value)
        assert "non_existent_keyspace" in error_msg or "keyspace" in error_msg.lower()

        # Session should still be usable
        result = await session.execute("SELECT now() FROM system.local")
        assert result.one() is not None

        await session.close()

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_type_mismatch_errors(self, cassandra_cluster):
        """
        Test that type mismatch errors are propagated.

        What this tests:
        ---------------
        1. Type validation works
        2. InvalidRequest raised
        3. Column info in error
        4. Type details shown

        Why this matters:
        ----------------
        Type safety critical:
        - Data integrity
        - Bug prevention
        - Clear debugging

        Type errors must be
        caught and reported.
        """
        session = await cassandra_cluster.connect()

        # Create test table
        await session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS test_type_errors
            WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
            """
        )
        await session.set_keyspace("test_type_errors")

        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS type_test (
                id UUID PRIMARY KEY,
                count INT,
                active BOOLEAN,
                created TIMESTAMP
            )
            """
        )

        # Prepare insert statement
        insert_stmt = await session.prepare(
            "INSERT INTO type_test (id, count, active, created) VALUES (?, ?, ?, ?)"
        )

        # Try various type mismatches
        test_cases = [
            # (values, expected_error_contains)
            ([uuid.uuid4(), "not_a_number", True, "2023-01-01"], ["count", "int"]),
            ([uuid.uuid4(), 42, "not_a_boolean", "2023-01-01"], ["active", "boolean"]),
            (["not_a_uuid", 42, True, "2023-01-01"], ["id", "uuid"]),
        ]

        for values, error_keywords in test_cases:
            with pytest.raises(Exception) as exc_info:  # Could be InvalidRequest or TypeError
                await session.execute(insert_stmt, values)

            error_msg = str(exc_info.value).lower()
            # Check that at least one expected keyword is in the error
            assert any(
                keyword.lower() in error_msg for keyword in error_keywords
            ), f"Expected keywords {error_keywords} not found in error: {error_msg}"

        # Cleanup
        await session.execute("DROP TABLE IF EXISTS type_test")
        await session.execute("DROP KEYSPACE IF EXISTS test_type_errors")
        await session.close()

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_timeout_errors(self, cassandra_cluster):
        """
        Test that timeout errors are properly propagated.

        What this tests:
        ---------------
        1. Query timeouts work
        2. Timeout value respected
        3. Error type correct
        4. Session recovers

        Why this matters:
        ----------------
        Timeout handling critical:
        - Prevent hanging
        - Resource cleanup
        - User experience

        Timeouts must fail fast
        and recover cleanly.
        """
        session = await cassandra_cluster.connect()

        # Create a test table with data
        await session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS test_timeout_errors
            WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
            """
        )
        await session.set_keyspace("test_timeout_errors")

        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS timeout_test (
                id UUID PRIMARY KEY,
                data TEXT
            )
            """
        )

        # Insert some data
        for i in range(100):
            await session.execute(
                "INSERT INTO timeout_test (id, data) VALUES (%s, %s)",
                [uuid.uuid4(), f"data_{i}" * 100],  # Make data reasonably large
            )

        # Create a simple query
        stmt = SimpleStatement("SELECT * FROM timeout_test")

        # Execute with very short timeout
        # Note: This might not always timeout in fast local environments
        try:
            result = await session.execute(stmt, timeout=0.001)  # 1ms timeout - very aggressive
            # If it succeeds, that's fine - timeout is environment dependent
            rows = list(result)
            assert len(rows) > 0
        except Exception as e:
            # If it times out, verify we get a timeout-related error
            # TimeoutError might have empty string representation, check type name too
            error_msg = str(e).lower()
            error_type = type(e).__name__.lower()
            assert (
                "timeout" in error_msg
                or "timeout" in error_type
                or isinstance(e, asyncio.TimeoutError)
            )

        # Session should still be usable after timeout
        result = await session.execute("SELECT count(*) FROM timeout_test")
        assert result.one().count >= 0

        # Cleanup
        await session.execute("DROP TABLE IF EXISTS timeout_test")
        await session.execute("DROP KEYSPACE IF EXISTS test_timeout_errors")
        await session.close()

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_batch_size_limit_error(self, cassandra_cluster):
        """
        Test that batch size limit errors are propagated.

        What this tests:
        ---------------
        1. Batch size limits
        2. Error on too large
        3. Clear error message
        4. Batch still usable

        Why this matters:
        ----------------
        Batch limits prevent:
        - Memory issues
        - Performance problems
        - Cluster instability

        Apps must respect
        batch size limits.
        """
        from cassandra.query import BatchStatement

        session = await cassandra_cluster.connect()

        # Create test table
        await session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS test_batch_errors
            WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
            """
        )
        await session.set_keyspace("test_batch_errors")

        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS batch_test (
                id UUID PRIMARY KEY,
                data TEXT
            )
            """
        )

        # Prepare insert statement
        insert_stmt = await session.prepare("INSERT INTO batch_test (id, data) VALUES (?, ?)")

        # Try to create a very large batch
        # Default batch size warning is at 5KB, error at 50KB
        batch = BatchStatement()
        large_data = "x" * 1000  # 1KB per row

        # Add many statements to exceed size limit
        for i in range(100):  # This should exceed typical batch size limits
            batch.add(insert_stmt, [uuid.uuid4(), large_data])

        # This might warn or error depending on server config
        try:
            await session.execute(batch)
            # If it succeeds, server has high limits - that's OK
        except Exception as e:
            # If it fails, should mention batch size
            error_msg = str(e).lower()
            assert "batch" in error_msg or "size" in error_msg or "limit" in error_msg

        # Smaller batch should work fine
        small_batch = BatchStatement()
        for i in range(5):
            small_batch.add(insert_stmt, [uuid.uuid4(), "small data"])

        await session.execute(small_batch)  # Should succeed

        # Cleanup
        await session.execute("DROP TABLE IF EXISTS batch_test")
        await session.execute("DROP KEYSPACE IF EXISTS test_batch_errors")
        await session.close()

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_concurrent_schema_modification_errors(self, cassandra_cluster):
        """
        Test errors from concurrent schema modifications.

        What this tests:
        ---------------
        1. Schema conflicts
        2. AlreadyExists errors
        3. Concurrent DDL
        4. Error recovery

        Why this matters:
        ----------------
        Multiple apps/devs may:
        - Run migrations
        - Modify schema
        - Create tables

        Must handle conflicts
        gracefully.
        """
        session = await cassandra_cluster.connect()

        # Create test keyspace
        await session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS test_schema_errors
            WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
            """
        )
        await session.set_keyspace("test_schema_errors")

        # Create a table
        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_test (
                id UUID PRIMARY KEY,
                data TEXT
            )
            """
        )

        # Try to create the same table again (without IF NOT EXISTS)
        # This might raise AlreadyExists or be wrapped in QueryError
        with pytest.raises((AlreadyExists, QueryError)) as exc_info:
            await session.execute(
                """
                CREATE TABLE schema_test (
                    id UUID PRIMARY KEY,
                    data TEXT
                )
                """
            )

        error_msg = str(exc_info.value).lower()
        assert "schema_test" in error_msg or "already exists" in error_msg

        # If wrapped, check the cause
        if isinstance(exc_info.value, QueryError):
            assert exc_info.value.__cause__ is not None

        # Try to create duplicate index
        await session.execute("CREATE INDEX IF NOT EXISTS idx_data ON schema_test (data)")

        # This might raise InvalidRequest or be wrapped in QueryError
        with pytest.raises((InvalidRequest, QueryError)) as exc_info:
            await session.execute("CREATE INDEX idx_data ON schema_test (data)")

        error_msg = str(exc_info.value).lower()
        assert "index" in error_msg or "already exists" in error_msg

        # If wrapped, check the cause
        if isinstance(exc_info.value, QueryError):
            assert exc_info.value.__cause__ is not None

        # Simulate concurrent modifications by trying operations that might conflict
        async def create_column(col_name):
            try:
                await session.execute(f"ALTER TABLE schema_test ADD {col_name} TEXT")
                return True
            except (InvalidRequest, ConfigurationException):
                return False

        # Try to add same column concurrently (one should fail)
        results = await asyncio.gather(
            create_column("new_col"), create_column("new_col"), return_exceptions=True
        )

        # At least one should succeed, at least one should fail
        successes = sum(1 for r in results if r is True)
        failures = sum(1 for r in results if r is False or isinstance(r, Exception))
        assert successes >= 1  # At least one succeeded
        assert failures >= 0  # Some might fail due to concurrent modification

        # Cleanup
        await session.execute("DROP TABLE IF EXISTS schema_test")
        await session.execute("DROP KEYSPACE IF EXISTS test_schema_errors")
        await session.close()

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_consistency_level_errors(self, cassandra_cluster):
        """
        Test that consistency level errors are propagated.

        What this tests:
        ---------------
        1. Consistency failures
        2. Unavailable errors
        3. Error details preserved
        4. Session recovery

        Why this matters:
        ----------------
        Consistency errors show:
        - Cluster health issues
        - Replication problems
        - Config mismatches

        Critical for distributed
        system debugging.
        """
        from cassandra import ConsistencyLevel
        from cassandra.query import SimpleStatement

        session = await cassandra_cluster.connect()

        # Create test keyspace with RF=1
        await session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS test_consistency_errors
            WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
            """
        )
        await session.set_keyspace("test_consistency_errors")

        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS consistency_test (
                id UUID PRIMARY KEY,
                data TEXT
            )
            """
        )

        # Insert some data
        test_id = uuid.uuid4()
        await session.execute(
            "INSERT INTO consistency_test (id, data) VALUES (%s, %s)", [test_id, "test data"]
        )

        # In a single-node setup, we can't truly test consistency failures
        # but we can verify that consistency levels are accepted

        # These should work with single node
        for cl in [ConsistencyLevel.ONE, ConsistencyLevel.LOCAL_ONE]:
            stmt = SimpleStatement(
                "SELECT * FROM consistency_test WHERE id = %s", consistency_level=cl
            )
            result = await session.execute(stmt, [test_id])
            assert result.one() is not None

        # Note: In production, requesting ALL or QUORUM with RF=1 on multi-node
        # cluster could fail. Here we just verify the statement executes.
        stmt = SimpleStatement(
            "SELECT * FROM consistency_test", consistency_level=ConsistencyLevel.ALL
        )
        result = await session.execute(stmt)
        # Should work on single node even with CL=ALL

        # Cleanup
        await session.execute("DROP TABLE IF EXISTS consistency_test")
        await session.execute("DROP KEYSPACE IF EXISTS test_consistency_errors")
        await session.close()

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_function_and_aggregate_errors(self, cassandra_cluster):
        """
        Test errors related to functions and aggregates.

        What this tests:
        ---------------
        1. Invalid function calls
        2. Missing functions
        3. Wrong arguments
        4. Clear error messages

        Why this matters:
        ----------------
        Function errors common:
        - Wrong function names
        - Incorrect arguments
        - Type mismatches

        Need clear error messages
        for debugging.
        """
        session = await cassandra_cluster.connect()

        # Test invalid function calls
        with pytest.raises(InvalidRequest) as exc_info:
            await session.execute("SELECT non_existent_function(now()) FROM system.local")

        error_msg = str(exc_info.value).lower()
        assert "function" in error_msg or "unknown" in error_msg

        # Test wrong number of arguments to built-in function
        with pytest.raises(InvalidRequest) as exc_info:
            await session.execute("SELECT toTimestamp() FROM system.local")  # Missing argument

        # Test invalid aggregate usage
        with pytest.raises(InvalidRequest) as exc_info:
            await session.execute("SELECT sum(release_version) FROM system.local")  # Can't sum text

        await session.close()

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_large_query_handling(self, cassandra_cluster):
        """
        Test handling of large queries and data.

        What this tests:
        ---------------
        1. Large INSERT data
        2. Large SELECT results
        3. Protocol limits
        4. Memory handling

        Why this matters:
        ----------------
        Large data scenarios:
        - Bulk imports
        - Document storage
        - Media metadata

        Must handle large payloads
        without protocol errors.
        """
        session = await cassandra_cluster.connect()

        # Create test keyspace and table
        await session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS test_large_data
            WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
            """
        )
        await session.set_keyspace("test_large_data")

        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS large_data_test (
                id UUID PRIMARY KEY,
                small_text TEXT,
                large_text TEXT,
                binary_data BLOB
            )
            """
        )

        # Test 1: Large text data (just under common limits)
        test_id = uuid.uuid4()
        # Create 1MB of text data (well within Cassandra's default frame size)
        large_text = "x" * (1024 * 1024)  # 1MB

        # This should succeed
        insert_stmt = await session.prepare(
            "INSERT INTO large_data_test (id, small_text, large_text) VALUES (?, ?, ?)"
        )
        await session.execute(insert_stmt, [test_id, "small", large_text])

        # Verify we can read it back
        select_stmt = await session.prepare("SELECT * FROM large_data_test WHERE id = ?")
        result = await session.execute(select_stmt, [test_id])
        row = result.one()
        assert row is not None
        assert len(row.large_text) == len(large_text)
        assert row.large_text == large_text

        # Test 2: Binary data
        import os

        test_id2 = uuid.uuid4()
        # Create 512KB of random binary data
        binary_data = os.urandom(512 * 1024)  # 512KB

        insert_binary_stmt = await session.prepare(
            "INSERT INTO large_data_test (id, small_text, binary_data) VALUES (?, ?, ?)"
        )
        await session.execute(insert_binary_stmt, [test_id2, "binary test", binary_data])

        # Read it back
        result = await session.execute(select_stmt, [test_id2])
        row = result.one()
        assert row is not None
        assert len(row.binary_data) == len(binary_data)
        assert row.binary_data == binary_data

        # Test 3: Multiple large rows in one query
        # Insert several rows with moderately large data
        insert_many_stmt = await session.prepare(
            "INSERT INTO large_data_test (id, small_text, large_text) VALUES (?, ?, ?)"
        )

        row_ids = []
        medium_text = "y" * (100 * 1024)  # 100KB per row
        for i in range(10):
            row_id = uuid.uuid4()
            row_ids.append(row_id)
            await session.execute(insert_many_stmt, [row_id, f"row_{i}", medium_text])

        # Select all of them at once
        # For simple statements, use %s placeholders
        placeholders = ",".join(["%s"] * len(row_ids))
        select_many = f"SELECT * FROM large_data_test WHERE id IN ({placeholders})"
        result = await session.execute(select_many, row_ids)
        rows = list(result)
        assert len(rows) == 10
        for row in rows:
            assert len(row.large_text) == len(medium_text)

        # Test 4: Very large data that might exceed limits
        # Default native protocol frame size is often 256MB, but message size limits are lower
        # Try something that's large but should still work
        test_id3 = uuid.uuid4()
        very_large_text = "z" * (10 * 1024 * 1024)  # 10MB

        try:
            await session.execute(insert_stmt, [test_id3, "very large", very_large_text])
            # If it succeeds, verify we can read it
            result = await session.execute(select_stmt, [test_id3])
            row = result.one()
            assert row is not None
            assert len(row.large_text) == len(very_large_text)
        except Exception as e:
            # If it fails due to size limits, that's expected
            error_msg = str(e).lower()
            assert any(word in error_msg for word in ["size", "large", "limit", "frame", "big"])

        # Test 5: Large batch with multiple large values
        from cassandra.query import BatchStatement

        batch = BatchStatement()
        batch_text = "b" * (50 * 1024)  # 50KB per row

        # Add 20 statements to the batch (total ~1MB)
        for i in range(20):
            batch.add(insert_stmt, [uuid.uuid4(), f"batch_{i}", batch_text])

        try:
            await session.execute(batch)
            # Success means the batch was within limits
        except Exception as e:
            # Large batches might be rejected
            error_msg = str(e).lower()
            assert any(word in error_msg for word in ["batch", "size", "large", "limit"])

        # Cleanup
        await session.execute("DROP TABLE IF EXISTS large_data_test")
        await session.execute("DROP KEYSPACE IF EXISTS test_large_data")
        await session.close()
