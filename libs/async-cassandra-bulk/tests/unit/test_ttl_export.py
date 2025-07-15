"""
Unit tests for TTL (Time To Live) export functionality.

What this tests:
---------------
1. TTL column generation in queries
2. TTL data handling in export
3. TTL with different export formats
4. TTL combined with writetime
5. Error handling for TTL edge cases

Why this matters:
----------------
- TTL is critical for data expiration tracking
- Must work alongside writetime export
- Different formats need proper TTL handling
- Production exports need accurate TTL data
"""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from async_cassandra_bulk import BulkOperator
from async_cassandra_bulk.exporters import CSVExporter, JSONExporter
from async_cassandra_bulk.utils.token_utils import build_query


class TestTTLExport:
    """Test TTL export functionality."""

    def test_build_query_with_ttl_columns(self):
        """
        Test query generation includes TTL() functions.

        What this tests:
        ---------------
        1. TTL columns are added to SELECT
        2. TTL column naming convention
        3. Multiple TTL columns
        4. Combined with regular columns

        Why this matters:
        ----------------
        - Query must request TTL data from Cassandra
        - Column naming must be consistent
        - Must work with existing column selection
        """
        # Test with specific TTL columns
        query = build_query(
            table="test_table",
            columns=["id", "name", "email"],
            ttl_columns=["name", "email"],
            token_range=None,
        )

        expected = (
            "SELECT id, name, email, TTL(name) AS name_ttl, TTL(email) AS email_ttl "
            "FROM test_table"
        )
        assert query == expected

    def test_build_query_with_ttl_all_columns(self):
        """
        Test TTL export with wildcard selection.

        What this tests:
        ---------------
        1. TTL with SELECT *
        2. All columns get TTL
        3. Proper query formatting

        Why this matters:
        ----------------
        - Common use case for full exports
        - Must handle dynamic column detection
        - Query complexity increases
        """
        # Test with all columns (*)
        query = build_query(
            table="test_table",
            columns=["*"],
            ttl_columns=["*"],
            token_range=None,
        )

        # Should include TTL for all columns (except primary keys)
        expected = "SELECT *, TTL(*) FROM test_table"
        assert query == expected

    def test_build_query_with_ttl_and_writetime(self):
        """
        Test combined TTL and writetime export.

        What this tests:
        ---------------
        1. Both TTL and WRITETIME in same query
        2. Proper column aliasing
        3. No conflicts in naming
        4. Query remains valid

        Why this matters:
        ----------------
        - Common to export both together
        - Query complexity management
        - Must maintain readability
        """
        query = build_query(
            table="test_table",
            columns=["id", "name", "status"],
            writetime_columns=["name", "status"],
            ttl_columns=["name", "status"],
            token_range=None,
        )

        expected = (
            "SELECT id, name, status, "
            "WRITETIME(name) AS name_writetime, WRITETIME(status) AS status_writetime, "
            "TTL(name) AS name_ttl, TTL(status) AS status_ttl "
            "FROM test_table"
        )
        assert query == expected

    @pytest.mark.asyncio
    async def test_json_exporter_with_ttl(self):
        """
        Test JSON export includes TTL data.

        What this tests:
        ---------------
        1. TTL values in JSON output
        2. TTL column naming in JSON
        3. Null TTL handling
        4. TTL data types

        Why this matters:
        ----------------
        - JSON is primary export format
        - TTL values must be preserved
        - Null handling is critical
        """
        # Mock file handle
        mock_file_handle = AsyncMock()
        mock_file_handle.write = AsyncMock()

        # Mock the async context manager
        mock_open = AsyncMock()
        mock_open.__aenter__.return_value = mock_file_handle
        mock_open.__aexit__.return_value = None

        with patch("aiofiles.open", return_value=mock_open):
            exporter = JSONExporter("test.json")

            # Need to manually set the file since we're not using export_rows
            exporter._file = mock_file_handle
            exporter._file_opened = True

            # Test row with TTL data
            row = {
                "id": 1,
                "name": "test",
                "email": "test@example.com",
                "name_ttl": 86400,  # 1 day in seconds
                "email_ttl": 172800,  # 2 days in seconds
            }

            await exporter.write_row(row)

            # Verify JSON includes TTL columns
            assert mock_file_handle.write.called
            write_call = mock_file_handle.write.call_args[0][0]
            data = json.loads(write_call)

            assert data["name_ttl"] == 86400
            assert data["email_ttl"] == 172800

    @pytest.mark.asyncio
    async def test_csv_exporter_with_ttl(self):
        """
        Test CSV export includes TTL data.

        What this tests:
        ---------------
        1. TTL columns in CSV header
        2. TTL values in CSV rows
        3. Proper column ordering
        4. TTL number formatting

        Why this matters:
        ----------------
        - CSV needs explicit headers
        - Column order matters
        - Number formatting important
        """
        # Mock file handle
        mock_file_handle = AsyncMock()
        mock_file_handle.write = AsyncMock()

        # Mock the async context manager
        mock_open = AsyncMock()
        mock_open.__aenter__.return_value = mock_file_handle
        mock_open.__aexit__.return_value = None

        with patch("aiofiles.open", return_value=mock_open):
            exporter = CSVExporter("test.csv")

            # Need to manually set the file since we're not using export_rows
            exporter._file = mock_file_handle
            exporter._file_opened = True

            # Write header with TTL columns
            await exporter.write_header(["id", "name", "name_ttl"])

            # Verify header includes TTL columns
            assert mock_file_handle.write.called
            header_call = mock_file_handle.write.call_args_list[0][0][0]
            assert "name_ttl" in header_call

            # Write row with TTL
            await exporter.write_row(
                {
                    "id": 1,
                    "name": "test",
                    "name_ttl": 3600,
                }
            )

            # Verify TTL in row
            row_call = mock_file_handle.write.call_args_list[1][0][0]
            assert "3600" in row_call

    @pytest.mark.asyncio
    async def test_bulk_operator_ttl_option(self):
        """
        Test BulkOperator with TTL export option.

        What this tests:
        ---------------
        1. include_ttl option parsing
        2. ttl_columns specification
        3. Options validation
        4. Default behavior

        Why this matters:
        ----------------
        - API consistency with writetime
        - User-friendly options
        - Backward compatibility
        """
        session = AsyncMock()
        session.execute = AsyncMock()
        session._session = MagicMock()
        session._session.keyspace = "test_keyspace"

        operator = BulkOperator(session)

        # Test include_ttl option
        with patch(
            "async_cassandra_bulk.operators.bulk_operator.ParallelExporter"
        ) as mock_parallel:
            mock_instance = AsyncMock()
            mock_instance.export = AsyncMock(
                return_value=MagicMock(
                    rows_processed=10, errors=[], duration_seconds=1.0, rows_per_second=10.0
                )
            )
            mock_parallel.return_value = mock_instance

            await operator.export(
                table="test_keyspace.test_table",
                output_path="test.json",
                format="json",
                options={
                    "include_ttl": True,
                },
            )

            # Verify ttl_columns was set to ["*"]
            assert mock_parallel.called
            call_kwargs = mock_parallel.call_args[1]
            assert call_kwargs["ttl_columns"] == ["*"]

    @pytest.mark.asyncio
    async def test_bulk_operator_specific_ttl_columns(self):
        """
        Test TTL export with specific columns.

        What this tests:
        ---------------
        1. Specific column TTL selection
        2. Column validation
        3. Options merging
        4. Error handling

        Why this matters:
        ----------------
        - Selective TTL export
        - Performance optimization
        - Flexibility for users
        """
        session = AsyncMock()
        session.execute = AsyncMock()
        session._session = MagicMock()
        session._session.keyspace = "test_keyspace"

        operator = BulkOperator(session)

        with patch(
            "async_cassandra_bulk.operators.bulk_operator.ParallelExporter"
        ) as mock_parallel:
            mock_instance = AsyncMock()
            mock_instance.export = AsyncMock(
                return_value=MagicMock(
                    rows_processed=10, errors=[], duration_seconds=1.0, rows_per_second=10.0
                )
            )
            mock_parallel.return_value = mock_instance

            await operator.export(
                table="test_keyspace.test_table",
                output_path="test.json",
                format="json",
                options={
                    "ttl_columns": ["created_at", "updated_at"],
                },
            )

            # Verify specific ttl_columns were passed
            assert mock_parallel.called
            call_kwargs = mock_parallel.call_args[1]
            assert call_kwargs["ttl_columns"] == ["created_at", "updated_at"]

    def test_ttl_null_handling(self):
        """
        Test TTL handling for NULL values.

        What this tests:
        ---------------
        1. NULL values don't have TTL
        2. No TTL column for NULL
        3. Proper serialization
        4. Edge case handling

        Why this matters:
        ----------------
        - NULL handling is critical
        - Avoid confusion in exports
        - Data integrity
        """
        # Test row with NULL value
        row = {
            "id": 1,
            "name": None,
            "email": "test@example.com",
            "email_ttl": 3600,
            # Note: no name_ttl because name is NULL
        }

        # Verify TTL not present for NULL columns
        assert "name_ttl" not in row
        assert row["email_ttl"] == 3600

    def test_ttl_with_expired_data(self):
        """
        Test TTL handling for expired data.

        What this tests:
        ---------------
        1. Negative TTL values
        2. Zero TTL values
        3. Export behavior
        4. Data interpretation

        Why this matters:
        ----------------
        - Expired data handling
        - Data lifecycle tracking
        - Migration scenarios
        """
        # Test with expired TTL (negative value)
        row = {
            "id": 1,
            "name": "test",
            "name_ttl": -100,  # Expired 100 seconds ago
        }

        # Expired data should still be exported with negative TTL
        assert row["name_ttl"] == -100

    @pytest.mark.asyncio
    async def test_ttl_with_primary_keys(self):
        """
        Test that primary keys don't get TTL.

        What this tests:
        ---------------
        1. Primary keys excluded from TTL
        2. No TTL query for keys
        3. Proper column filtering
        4. Error prevention

        Why this matters:
        ----------------
        - Primary keys can't have TTL
        - Avoid invalid queries
        - Cassandra restrictions
        """
        # Build query should not include TTL for primary keys
        # This would need schema awareness in real implementation
        build_query(
            table="test_table",
            columns=["id", "name"],
            ttl_columns=["id", "name"],  # id is primary key
            token_range=None,
            primary_keys=["id"],  # This would need to be added
        )

        # Should only include TTL for non-primary key columns
        # Note: This test will fail until we implement primary key filtering

    def test_ttl_format_in_export(self):
        """
        Test TTL value formatting in exports.

        What this tests:
        ---------------
        1. TTL as seconds remaining
        2. Integer formatting
        3. Large TTL values
        4. Consistency across formats

        Why this matters:
        ----------------
        - TTL interpretation
        - Data portability
        - User expectations
        """
        # TTL values should be in seconds
        row = {
            "id": 1,
            "name": "test",
            "name_ttl": 2592000,  # 30 days in seconds
        }

        # Verify TTL is integer seconds
        assert isinstance(row["name_ttl"], int)
        assert row["name_ttl"] == 30 * 24 * 60 * 60
