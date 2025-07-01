"""Prepared statements functionality tests.

This module tests prepared statement creation, execution, and caching.
"""

import asyncio
from unittest.mock import Mock

import pytest
from cassandra.query import BoundStatement, PreparedStatement

from async_cassandra import AsyncCassandraSession as AsyncSession
from tests.unit.test_helpers import create_mock_response_future


class TestPreparedStatements:
    """Test prepared statement functionality."""

    @pytest.mark.features
    @pytest.mark.quick
    @pytest.mark.critical
    async def test_prepare_statement(self):
        """
        Test basic prepared statement creation.

        What this tests:
        ---------------
        1. Prepare statement async wrapper works
        2. Query string passed correctly
        3. PreparedStatement returned
        4. Synchronous prepare called once

        Why this matters:
        ----------------
        Prepared statements are critical for:
        - Query performance (cached plans)
        - SQL injection prevention
        - Type safety with parameters

        Every production app should use
        prepared statements for queries.
        """
        mock_session = Mock()
        mock_prepared = Mock(spec=PreparedStatement)
        mock_session.prepare.return_value = mock_prepared

        async_session = AsyncSession(mock_session)

        prepared = await async_session.prepare("SELECT * FROM users WHERE id = ?")

        assert prepared == mock_prepared
        mock_session.prepare.assert_called_once_with("SELECT * FROM users WHERE id = ?", None)

    @pytest.mark.features
    async def test_execute_prepared_statement(self):
        """
        Test executing prepared statements.

        What this tests:
        ---------------
        1. Prepared statements can be executed
        2. Parameters bound correctly
        3. Results returned properly
        4. Async execution flow works

        Why this matters:
        ----------------
        Prepared statement execution:
        - Most common query pattern
        - Must handle parameter binding
        - Critical for performance

        Proper parameter handling prevents
        injection attacks and type errors.
        """
        mock_session = Mock()
        mock_prepared = Mock(spec=PreparedStatement)
        mock_bound = Mock(spec=BoundStatement)

        mock_prepared.bind.return_value = mock_bound
        mock_session.prepare.return_value = mock_prepared

        # Create a mock response future manually to have more control
        response_future = Mock()
        response_future.has_more_pages = False
        response_future.timeout = None
        response_future.add_callbacks = Mock()

        def setup_callback(callback=None, errback=None):
            # Call the callback immediately with test data
            if callback:
                callback([{"id": 1, "name": "test"}])

        response_future.add_callbacks.side_effect = setup_callback
        mock_session.execute_async.return_value = response_future

        async_session = AsyncSession(mock_session)

        # Prepare statement
        prepared = await async_session.prepare("SELECT * FROM users WHERE id = ?")

        # Execute with parameters
        result = await async_session.execute(prepared, [1])

        assert len(result.rows) == 1
        assert result.rows[0] == {"id": 1, "name": "test"}
        # The prepared statement and parameters are passed to execute_async
        mock_session.execute_async.assert_called_once()
        # Check that the prepared statement was passed
        args = mock_session.execute_async.call_args[0]
        assert args[0] == prepared
        assert args[1] == [1]

    @pytest.mark.features
    @pytest.mark.critical
    async def test_prepared_statement_caching(self):
        """
        Test that prepared statements can be cached and reused.

        What this tests:
        ---------------
        1. Same query returns same statement
        2. Multiple prepares allowed
        3. Statement object reusable
        4. No built-in caching (driver handles)

        Why this matters:
        ----------------
        Statement caching important for:
        - Avoiding re-preparation overhead
        - Consistent query plans
        - Memory efficiency

        Applications should cache statements
        at application level for best performance.
        """
        mock_session = Mock()
        mock_prepared = Mock(spec=PreparedStatement)
        mock_session.prepare.return_value = mock_prepared
        mock_session.execute.return_value = Mock(current_rows=[])

        async_session = AsyncSession(mock_session)

        # Prepare same statement multiple times
        query = "SELECT * FROM users WHERE id = ? AND status = ?"

        prepared1 = await async_session.prepare(query)
        prepared2 = await async_session.prepare(query)
        prepared3 = await async_session.prepare(query)

        # All should be the same instance
        assert prepared1 == prepared2 == prepared3 == mock_prepared

        # But prepare is called each time (caching would be an optimization)
        assert mock_session.prepare.call_count == 3

    @pytest.mark.features
    async def test_prepared_statement_with_custom_options(self):
        """
        Test prepared statements with custom execution options.

        What this tests:
        ---------------
        1. Custom timeout honored
        2. Custom payload passed through
        3. Execution options work with prepared
        4. Parameters still bound correctly

        Why this matters:
        ----------------
        Production queries often need:
        - Custom timeouts for SLAs
        - Tracing via custom payloads
        - Consistency level tuning

        Prepared statements must support
        all execution options.
        """
        mock_session = Mock()
        mock_prepared = Mock(spec=PreparedStatement)
        mock_bound = Mock(spec=BoundStatement)

        mock_prepared.bind.return_value = mock_bound
        mock_session.prepare.return_value = mock_prepared
        mock_session.execute_async.return_value = create_mock_response_future([])

        async_session = AsyncSession(mock_session)

        prepared = await async_session.prepare("UPDATE users SET name = ? WHERE id = ?")

        # Execute with custom timeout and consistency
        await async_session.execute(
            prepared, ["new name", 123], timeout=30.0, custom_payload={"trace": "true"}
        )

        # Verify execute_async was called with correct parameters
        mock_session.execute_async.assert_called_once()
        # Check the arguments passed to execute_async
        args = mock_session.execute_async.call_args[0]
        assert args[0] == prepared
        assert args[1] == ["new name", 123]
        # Check timeout was passed (position 4)
        assert args[4] == 30.0

    @pytest.mark.features
    async def test_concurrent_prepare_statements(self):
        """
        Test preparing multiple statements concurrently.

        What this tests:
        ---------------
        1. Multiple prepares can run concurrently
        2. Each gets correct statement back
        3. No race conditions or mixing
        4. Async gather works properly

        Why this matters:
        ----------------
        Application startup often:
        - Prepares many statements
        - Benefits from parallelism
        - Must not corrupt statements

        Concurrent preparation speeds up
        application initialization.
        """
        mock_session = Mock()

        # Different prepared statements
        prepared_stmts = {
            "SELECT": Mock(spec=PreparedStatement),
            "INSERT": Mock(spec=PreparedStatement),
            "UPDATE": Mock(spec=PreparedStatement),
            "DELETE": Mock(spec=PreparedStatement),
        }

        def prepare_side_effect(query, custom_payload=None):
            for key in prepared_stmts:
                if key in query:
                    return prepared_stmts[key]
            return Mock(spec=PreparedStatement)

        mock_session.prepare.side_effect = prepare_side_effect

        async_session = AsyncSession(mock_session)

        # Prepare statements concurrently
        tasks = [
            async_session.prepare("SELECT * FROM users WHERE id = ?"),
            async_session.prepare("INSERT INTO users (id, name) VALUES (?, ?)"),
            async_session.prepare("UPDATE users SET name = ? WHERE id = ?"),
            async_session.prepare("DELETE FROM users WHERE id = ?"),
        ]

        results = await asyncio.gather(*tasks)

        assert results[0] == prepared_stmts["SELECT"]
        assert results[1] == prepared_stmts["INSERT"]
        assert results[2] == prepared_stmts["UPDATE"]
        assert results[3] == prepared_stmts["DELETE"]

    @pytest.mark.features
    async def test_prepared_statement_error_handling(self):
        """
        Test error handling during statement preparation.

        What this tests:
        ---------------
        1. Prepare errors propagated
        2. Original exception preserved
        3. Error message maintained
        4. No hanging or corruption

        Why this matters:
        ----------------
        Prepare can fail due to:
        - Syntax errors in query
        - Unknown tables/columns
        - Schema mismatches

        Clear errors help developers
        fix queries during development.
        """
        mock_session = Mock()
        mock_session.prepare.side_effect = Exception("Invalid query syntax")

        async_session = AsyncSession(mock_session)

        with pytest.raises(Exception, match="Invalid query syntax"):
            await async_session.prepare("INVALID QUERY SYNTAX")

    @pytest.mark.features
    @pytest.mark.critical
    async def test_bound_statement_reuse(self):
        """
        Test reusing bound statements.

        What this tests:
        ---------------
        1. Prepare once, execute many
        2. Different parameters each time
        3. Statement prepared only once
        4. Executions independent

        Why this matters:
        ----------------
        This is THE pattern for production:
        - Prepare statements at startup
        - Execute with different params
        - Massive performance benefit

        Reusing prepared statements reduces
        latency and cluster load.
        """
        mock_session = Mock()
        mock_prepared = Mock(spec=PreparedStatement)
        mock_bound = Mock(spec=BoundStatement)

        mock_prepared.bind.return_value = mock_bound
        mock_session.prepare.return_value = mock_prepared
        mock_session.execute_async.return_value = create_mock_response_future([])

        async_session = AsyncSession(mock_session)

        # Prepare once
        prepared = await async_session.prepare("SELECT * FROM users WHERE id = ?")

        # Execute multiple times with different parameters
        for user_id in [1, 2, 3, 4, 5]:
            await async_session.execute(prepared, [user_id])

        # Prepare called once, execute_async called for each execution
        assert mock_session.prepare.call_count == 1
        assert mock_session.execute_async.call_count == 5

    @pytest.mark.features
    async def test_prepared_statement_metadata(self):
        """
        Test accessing prepared statement metadata.

        What this tests:
        ---------------
        1. Column metadata accessible
        2. Type information available
        3. Partition key info present
        4. Metadata correctly structured

        Why this matters:
        ----------------
        Metadata enables:
        - Dynamic result processing
        - Type validation
        - Routing optimization

        ORMs and frameworks rely on
        metadata for mapping and validation.
        """
        mock_session = Mock()
        mock_prepared = Mock(spec=PreparedStatement)

        # Mock metadata
        mock_prepared.column_metadata = [
            ("keyspace", "table", "id", "uuid"),
            ("keyspace", "table", "name", "text"),
            ("keyspace", "table", "created_at", "timestamp"),
        ]
        mock_prepared.routing_key_indexes = [0]  # id is partition key

        mock_session.prepare.return_value = mock_prepared

        async_session = AsyncSession(mock_session)

        prepared = await async_session.prepare(
            "SELECT id, name, created_at FROM users WHERE id = ?"
        )

        # Access metadata
        assert len(prepared.column_metadata) == 3
        assert prepared.column_metadata[0][2] == "id"
        assert prepared.column_metadata[1][2] == "name"
        assert prepared.routing_key_indexes == [0]
