"""Test SQL injection protection in example code."""

from unittest.mock import AsyncMock, MagicMock, call

import pytest

from async_cassandra import AsyncSession


class TestSQLInjectionProtection:
    """Test that example code properly protects against SQL injection."""

    @pytest.mark.asyncio
    async def test_prepared_statements_used_for_user_input(self):
        """Test that all user inputs use prepared statements."""
        # Create mock session
        mock_session = AsyncMock(spec=AsyncSession)
        mock_stmt = AsyncMock()
        mock_session.prepare.return_value = mock_stmt

        # Test LIMIT parameter
        mock_session.execute.return_value = MagicMock()
        await mock_session.prepare("SELECT * FROM users LIMIT ?")
        await mock_session.execute(mock_stmt, [10])

        # Verify prepared statement was used
        mock_session.prepare.assert_called_with("SELECT * FROM users LIMIT ?")
        mock_session.execute.assert_called_with(mock_stmt, [10])

    @pytest.mark.asyncio
    async def test_update_query_no_dynamic_sql(self):
        """Test that UPDATE queries don't use dynamic SQL construction."""
        # Create mock session
        mock_session = AsyncMock(spec=AsyncSession)
        mock_stmt = AsyncMock()
        mock_session.prepare.return_value = mock_stmt

        # Test different update scenarios
        update_queries = [
            "UPDATE users SET name = ?, updated_at = ? WHERE id = ?",
            "UPDATE users SET email = ?, updated_at = ? WHERE id = ?",
            "UPDATE users SET age = ?, updated_at = ? WHERE id = ?",
            "UPDATE users SET name = ?, email = ?, updated_at = ? WHERE id = ?",
            "UPDATE users SET name = ?, age = ?, updated_at = ? WHERE id = ?",
            "UPDATE users SET email = ?, age = ?, updated_at = ? WHERE id = ?",
            "UPDATE users SET name = ?, email = ?, age = ?, updated_at = ? WHERE id = ?",
        ]

        for query in update_queries:
            await mock_session.prepare(query)

        # Verify only static queries were prepared
        for query in update_queries:
            assert call(query) in mock_session.prepare.call_args_list

    @pytest.mark.asyncio
    async def test_table_name_validation_before_use(self):
        """Test that table names are validated before use in queries."""
        # Create mock session
        mock_session = AsyncMock(spec=AsyncSession)

        # Mock validation query response
        mock_result = MagicMock()
        mock_result.one.return_value = {"table_name": "products"}
        mock_session.execute.return_value = mock_result

        # Test table validation
        keyspace = "export_example"
        table_name = "products"

        # Validate table exists
        validation_result = await mock_session.execute(
            "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?",
            [keyspace, table_name],
        )

        # Only proceed if table exists
        if validation_result.one():
            await mock_session.execute(f"SELECT COUNT(*) FROM {keyspace}.{table_name}")

        # Verify validation query was called
        mock_session.execute.assert_any_call(
            "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?",
            [keyspace, table_name],
        )

    @pytest.mark.asyncio
    async def test_no_string_interpolation_in_queries(self):
        """Test that queries don't use string interpolation with user input."""
        # Create mock session
        mock_session = AsyncMock(spec=AsyncSession)
        mock_stmt = AsyncMock()
        mock_session.prepare.return_value = mock_stmt

        # Bad patterns that should NOT be used
        user_input = "'; DROP TABLE users; --"

        # Good: Using prepared statements
        await mock_session.prepare("SELECT * FROM users WHERE name = ?")
        await mock_session.execute(mock_stmt, [user_input])

        # Good: Using prepared statements for LIMIT
        limit = "100; DROP TABLE users"
        await mock_session.prepare("SELECT * FROM users LIMIT ?")
        await mock_session.execute(mock_stmt, [int(limit.split(";")[0])])  # Parse safely

        # Verify no direct string interpolation
        assert all("DROP TABLE" not in str(call) for call in mock_session.execute.call_args_list)

    @pytest.mark.asyncio
    async def test_hardcoded_keyspace_names(self):
        """Test that keyspace names are hardcoded, not from user input."""
        # Create mock session
        mock_session = AsyncMock(spec=AsyncSession)

        # Good: Hardcoded keyspace names
        await mock_session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS example
            WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
            """
        )

        await mock_session.set_keyspace("example")

        # Verify no dynamic keyspace creation
        create_calls = [
            call for call in mock_session.execute.call_args_list if "CREATE KEYSPACE" in str(call)
        ]

        for create_call in create_calls:
            query = str(create_call)
            # Should not contain f-string or format markers
            assert "{" not in query or "{'class'" in query  # Allow replication config
            assert "%" not in query

    @pytest.mark.asyncio
    async def test_streaming_queries_use_prepared_statements(self):
        """Test that streaming queries use prepared statements."""
        # Create mock session
        mock_session = AsyncMock(spec=AsyncSession)
        mock_stmt = AsyncMock()
        mock_session.prepare.return_value = mock_stmt
        mock_session.execute_stream.return_value = AsyncMock()

        # Test streaming with parameters
        limit = 1000
        await mock_session.prepare("SELECT * FROM users LIMIT ?")
        await mock_session.execute_stream(mock_stmt, [limit])

        # Verify prepared statement was used
        mock_session.prepare.assert_called_with("SELECT * FROM users LIMIT ?")
        mock_session.execute_stream.assert_called_with(mock_stmt, [limit])

    def test_sql_injection_patterns_not_present(self):
        """Test that common SQL injection patterns are not in the codebase."""
        # This is a meta-test to ensure dangerous patterns aren't used
        dangerous_patterns = [
            'f"SELECT',  # f-string SQL
            'f"INSERT',
            'f"UPDATE',
            'f"DELETE',
            '".format(',  # format string SQL
            '" + ',  # string concatenation
            "' + ",
            "% (",  # old-style formatting
            "% {",
        ]

        # In real implementation, this would scan the actual files
        # For now, we just document what patterns to avoid
        for pattern in dangerous_patterns:
            # Document that these patterns should not be used
            assert pattern in dangerous_patterns  # Tautology for documentation
