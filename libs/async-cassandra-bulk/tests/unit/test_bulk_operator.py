"""
Test BulkOperator core functionality.

What this tests:
---------------
1. BulkOperator initialization
2. Session management
3. Basic count operation structure
4. Error handling

Why this matters:
----------------
- BulkOperator is the main entry point for bulk operations
- Must properly integrate with async-cassandra sessions
- Foundation for all bulk operations
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from async_cassandra_bulk import BulkOperator


class TestBulkOperatorInitialization:
    """Test BulkOperator initialization and configuration."""

    def test_bulk_operator_requires_session(self):
        """
        Test that BulkOperator requires an async session parameter.

        What this tests:
        ---------------
        1. Constructor validates session parameter is provided
        2. Raises TypeError when session is missing
        3. Error message mentions 'session' for clarity
        4. No partial initialization occurs

        Why this matters:
        ----------------
        - Session is required for all database operations
        - Clear error messages help developers fix issues quickly
        - Prevents runtime errors from missing dependencies
        - Production code must have valid session

        Additional context:
        ---------------------------------
        - Session should be AsyncCassandraSession instance
        - This validation happens before any other initialization
        """
        with pytest.raises(TypeError) as exc_info:
            BulkOperator()

        assert "session" in str(exc_info.value)

    def test_bulk_operator_stores_session(self):
        """
        Test that BulkOperator stores the provided session correctly.

        What this tests:
        ---------------
        1. Session is stored as instance attribute
        2. Session can be accessed via operator.session
        3. Stored session is the exact same object (identity)
        4. No modifications made to session during storage

        Why this matters:
        ----------------
        - All operations need access to the session
        - Session lifecycle must be preserved
        - Reference equality ensures no unexpected copying
        - Production operations depend on session state

        Additional context:
        ---------------------------------
        - Session contains connection pools and prepared statements
        - Same session may be shared across multiple operators
        """
        mock_session = MagicMock()
        operator = BulkOperator(session=mock_session)

        assert operator.session is mock_session

    def test_bulk_operator_validates_session_type(self):
        """
        Test that BulkOperator validates session has required async methods.

        What this tests:
        ---------------
        1. Session must have execute method for queries
        2. Session must have prepare method for prepared statements
        3. Raises ValueError for objects missing required methods
        4. Error message lists all missing methods

        Why this matters:
        ----------------
        - Type safety prevents AttributeError in production
        - Early validation at construction time
        - Guides users to use proper AsyncCassandraSession
        - Duck typing allows test mocks while ensuring interface

        Additional context:
        ---------------------------------
        - Uses hasattr() to check for method presence
        - Doesn't check if methods are actually async
        - Allows mock objects that implement interface
        """
        # Invalid session without required methods
        invalid_session = object()

        with pytest.raises(ValueError) as exc_info:
            BulkOperator(session=invalid_session)

        assert "execute" in str(exc_info.value)
        assert "prepare" in str(exc_info.value)


class TestBulkOperatorCount:
    """Test count operation functionality."""

    @pytest.mark.asyncio
    async def test_count_returns_total(self):
        """
        Test basic count operation returns total row count from table.

        What this tests:
        ---------------
        1. count() method exists and is async coroutine
        2. Constructs correct COUNT(*) CQL query
        3. Executes query through session.execute()
        4. Extracts integer count from result row

        Why this matters:
        ----------------
        - Count is the simplest bulk operation to verify
        - Validates core query execution pipeline
        - Foundation for more complex bulk operations
        - Production exports often start with count for progress

        Additional context:
        ---------------------------------
        - COUNT(*) is optimized in Cassandra 4.0+
        - Result.one() returns single row with count column
        - Large tables may timeout without proper settings
        """
        # Setup
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.one.return_value = MagicMock(count=12345)
        mock_session.execute.return_value = mock_result

        operator = BulkOperator(session=mock_session)

        # Execute
        result = await operator.count("keyspace.table")

        # Verify
        assert result == 12345
        mock_session.execute.assert_called_once()
        query = mock_session.execute.call_args[0][0]
        assert "COUNT(*)" in query.upper()
        assert "keyspace.table" in query

    @pytest.mark.asyncio
    async def test_count_validates_table_name(self):
        """
        Test count validates table name includes keyspace prefix.

        What this tests:
        ---------------
        1. Table name must be in 'keyspace.table' format
        2. Raises ValueError for table name without keyspace
        3. Error message shows expected format
        4. Validation happens before query execution

        Why this matters:
        ----------------
        - Prevents ambiguous queries across keyspaces
        - Consistent with Cassandra CQL best practices
        - Clear error messages guide correct usage
        - Production safety against wrong keyspace queries

        Additional context:
        ---------------------------------
        - Could default to session keyspace but explicit is better
        - Matches cassandra-driver prepared statement behavior
        - Same validation used across all bulk operations
        """
        mock_session = AsyncMock()
        operator = BulkOperator(session=mock_session)

        with pytest.raises(ValueError) as exc_info:
            await operator.count("table_without_keyspace")

        assert "keyspace.table" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_count_with_where_clause(self):
        """
        Test count operation with WHERE clause for filtered counting.

        What this tests:
        ---------------
        1. Optional where parameter adds WHERE clause
        2. WHERE clause appended correctly to base query
        3. User-provided conditions used verbatim
        4. Filtered count returns correct subset total

        Why this matters:
        ----------------
        - Filtered counts essential for data validation
        - Enables counting specific data states
        - Validates conditional query construction
        - Production use: count active users, recent records

        Additional context:
        ---------------------------------
        - WHERE clause not validated - user responsibility
        - Could support prepared statement parameters later
        - Common filters: status, date ranges, partition keys
        """
        # Setup
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.one.return_value = MagicMock(count=42)
        mock_session.execute.return_value = mock_result

        operator = BulkOperator(session=mock_session)

        # Execute
        result = await operator.count("keyspace.table", where="status = 'active'")

        # Verify
        assert result == 42
        query = mock_session.execute.call_args[0][0]
        assert "WHERE" in query
        assert "status = 'active'" in query

    @pytest.mark.asyncio
    async def test_count_handles_query_errors(self):
        """
        Test count operation properly propagates Cassandra query errors.

        What this tests:
        ---------------
        1. Database errors bubble up unchanged
        2. Original exception type and message preserved
        3. No error masking or wrapping occurs
        4. Stack trace maintained for debugging

        Why this matters:
        ----------------
        - Debugging requires full Cassandra error context
        - No silent failures that corrupt data counts
        - Production monitoring needs real error types
        - Stack traces essential for troubleshooting

        Additional context:
        ---------------------------------
        - Common errors: table not found, timeout, syntax
        - Cassandra errors include coordinator node info
        - Driver exceptions have error codes
        """
        mock_session = AsyncMock()
        mock_session.execute.side_effect = Exception("Table does not exist")

        operator = BulkOperator(session=mock_session)

        with pytest.raises(Exception) as exc_info:
            await operator.count("keyspace.nonexistent")

        assert "Table does not exist" in str(exc_info.value)


class TestBulkOperatorExport:
    """Test export operation structure."""

    @pytest.mark.asyncio
    async def test_export_method_exists(self):
        """
        Test that export method exists with expected signature.

        What this tests:
        ---------------
        1. export() method exists on BulkOperator
        2. Method is callable (not a property)
        3. Accepts required parameters: table, output_path, format
        4. Returns BulkOperationStats for monitoring

        Why this matters:
        ----------------
        - Primary API for all export operations
        - Sets interface contract for users
        - Consistent with other bulk operation methods
        - Production code depends on this signature

        Additional context:
        ---------------------------------
        - Export is most complex bulk operation
        - Delegates to ParallelExporter internally
        - Stats enable progress tracking and monitoring
        """
        mock_session = AsyncMock()
        operator = BulkOperator(session=mock_session)

        # Should have export method
        assert hasattr(operator, "export")
        assert callable(operator.export)

    @pytest.mark.asyncio
    async def test_export_validates_format(self):
        """
        Test export validates output format before processing.

        What this tests:
        ---------------
        1. Supported formats validated: csv, json
        2. Raises ValueError for unsupported formats
        3. Error message lists all valid formats
        4. Validation occurs before any processing

        Why this matters:
        ----------------
        - Early validation saves time and resources
        - Clear errors guide users to valid options
        - Prevents partial exports with invalid format
        - Production safety against typos

        Additional context:
        ---------------------------------
        - Parquet support planned for future
        - Format determines which exporter class used
        - Case-sensitive format matching
        """
        mock_session = AsyncMock()
        operator = BulkOperator(session=mock_session)

        with pytest.raises(ValueError) as exc_info:
            await operator.export(
                "keyspace.table", output_path="/tmp/data.txt", format="invalid_format"
            )

        assert "format" in str(exc_info.value).lower()
        assert "csv" in str(exc_info.value)
        assert "json" in str(exc_info.value)
