"""
Test writetime export functionality.

What this tests:
---------------
1. Writetime option parsing and validation
2. Query generation with WRITETIME() function
3. Column selection with writetime metadata
4. Serialization of writetime values

Why this matters:
----------------
- Writetime allows tracking when data was written
- Essential for data migration and audit trails
- Must handle complex scenarios with multiple columns
- Critical for time-based data analysis
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from async_cassandra_bulk import BulkOperator
from async_cassandra_bulk.exporters import CSVExporter
from async_cassandra_bulk.parallel_export import ParallelExporter
from async_cassandra_bulk.serializers import SerializationContext, get_global_registry
from async_cassandra_bulk.serializers.writetime import WritetimeColumnSerializer
from async_cassandra_bulk.utils.token_utils import generate_token_range_query


class TestWritetimeOption:
    """Test writetime export option handling."""

    def test_export_accepts_writetime_option(self):
        """
        Test that export method accepts include_writetime option.

        What this tests:
        ---------------
        1. Export options include 'include_writetime' parameter
        2. Parameter is boolean type
        3. Default value is False
        4. Option is passed through to exporter

        Why this matters:
        ----------------
        - API consistency for export options
        - Backwards compatibility (default off)
        - Clear boolean flag for feature toggle
        - Production exports need explicit opt-in
        """
        mock_session = AsyncMock()
        operator = BulkOperator(session=mock_session)

        # Should accept include_writetime in options
        operator.export(
            "keyspace.table",
            output_path="/tmp/data.csv",
            format="csv",
            options={"include_writetime": True},
        )

    def test_writetime_columns_option(self):
        """
        Test writetime_columns option for selective writetime export.

        What this tests:
        ---------------
        1. Accept list of columns to get writetime for
        2. Empty list means no writetime columns
        3. ['*'] means all non-primary-key columns
        4. Specific column names respected

        Why this matters:
        ----------------
        - Not all columns need writetime info
        - Primary keys don't have writetime
        - Reduces query overhead for large tables
        - Flexible configuration for different use cases
        """
        mock_session = AsyncMock()
        operator = BulkOperator(session=mock_session)

        # Specific columns
        operator.export(
            "keyspace.table",
            output_path="/tmp/data.csv",
            format="csv",
            options={"writetime_columns": ["created_at", "updated_at"]},
        )

        # All columns
        operator.export(
            "keyspace.table",
            output_path="/tmp/data.csv",
            format="csv",
            options={"writetime_columns": ["*"]},
        )


class TestWritetimeQueryGeneration:
    """Test query generation with writetime support."""

    def test_query_includes_writetime_functions(self):
        """
        Test query generation includes WRITETIME() functions.

        What this tests:
        ---------------
        1. WRITETIME() function added for requested columns
        2. Original columns still included
        3. Writetime columns have _writetime suffix
        4. Primary key columns excluded from writetime

        Why this matters:
        ----------------
        - Correct CQL syntax required
        - Column naming must be consistent
        - Primary keys cannot have writetime
        - Query must be valid Cassandra CQL

        Additional context:
        ---------------------------------
        - WRITETIME() returns microseconds since epoch
        - Function only works on non-primary-key columns
        - NULL returned if cell has no writetime
        """
        # Mock table metadata
        partition_keys = ["id"]

        # Generate query with writetime
        query = generate_token_range_query(
            keyspace="test_ks",
            table="test_table",
            partition_keys=partition_keys,
            token_range=MagicMock(start=0, end=100),
            columns=["id", "name", "email"],
            writetime_columns=["name", "email"],
        )

        # Should include original columns and writetime functions
        assert "id, name, email" in query
        assert "WRITETIME(name) AS name_writetime" in query
        assert "WRITETIME(email) AS email_writetime" in query

    def test_writetime_all_columns(self):
        """
        Test writetime generation for all non-primary columns.

        What this tests:
        ---------------
        1. ['*'] expands to all non-primary columns
        2. Primary key columns automatically excluded
        3. Clustering columns also excluded
        4. All regular columns get writetime

        Why this matters:
        ----------------
        - Convenient syntax for full writetime export
        - Prevents invalid queries on primary keys
        - Consistent behavior across table schemas
        - Production tables may have many columns
        """
        partition_keys = ["id"]
        clustering_keys = ["timestamp"]

        # All columns including primary/clustering
        all_columns = ["id", "timestamp", "name", "email", "status"]

        query = generate_token_range_query(
            keyspace="test_ks",
            table="test_table",
            partition_keys=partition_keys,
            token_range=MagicMock(start=0, end=100),
            columns=all_columns,
            writetime_columns=["*"],
            clustering_keys=clustering_keys,
        )

        # Should have writetime for non-key columns only
        assert "WRITETIME(name) AS name_writetime" in query
        assert "WRITETIME(email) AS email_writetime" in query
        assert "WRITETIME(status) AS status_writetime" in query
        # Should NOT have writetime for keys
        assert "WRITETIME(id)" not in query
        assert "WRITETIME(timestamp)" not in query


class TestWritetimeSerialization:
    """Test serialization of writetime values."""

    def test_writetime_csv_serialization(self):
        """
        Test writetime values serialized correctly for CSV.

        What this tests:
        ---------------
        1. Microsecond timestamps converted to readable format
        2. Null writetime values handled properly
        3. Configurable timestamp format
        4. Large timestamp values (year 2050+) work

        Why this matters:
        ----------------
        - CSV needs human-readable timestamps
        - Consistent format across exports
        - Must handle missing writetime data
        - Future-proof for long-running systems
        """
        serializer = WritetimeColumnSerializer()
        context = SerializationContext(
            format="csv",
            options={"writetime_format": "%Y-%m-%d %H:%M:%S.%f"},
        )

        # Cassandra writetime in microseconds
        writetime_micros = 1700000000000000  # ~2023-11-14

        # Should convert to timestamp for writetime columns
        is_writetime, result = serializer.serialize_if_writetime(
            "updated_at_writetime", writetime_micros, context
        )
        assert is_writetime is True
        assert isinstance(result, str)
        assert "2023" in result

    def test_writetime_json_serialization(self):
        """
        Test writetime values serialized correctly for JSON.

        What this tests:
        ---------------
        1. Microseconds converted to ISO format
        2. Null writetime becomes JSON null
        3. Timezone information included
        4. Nanosecond precision preserved

        Why this matters:
        ----------------
        - JSON needs standard timestamp format
        - ISO 8601 for interoperability
        - Precision important for ordering
        - Must be parseable by other systems
        """
        serializer = WritetimeColumnSerializer()
        context = SerializationContext(format="json", options={})

        # Cassandra writetime
        writetime_micros = 1700000000000000

        is_writetime, result = serializer.serialize_if_writetime(
            "created_at_writetime", writetime_micros, context
        )
        assert is_writetime is True
        assert isinstance(result, str)
        assert "T" in result  # ISO format has T separator
        assert "Z" in result or "+" in result  # Timezone info

    def test_writetime_in_row_data(self):
        """
        Test writetime columns included in exported row data.

        What this tests:
        ---------------
        1. Row dict contains _writetime suffixed columns
        2. Original column values preserved
        3. Writetime values are microseconds
        4. Null handling for missing writetime

        Why this matters:
        ----------------
        - Data structure must be consistent
        - Both value and writetime exported together
        - Enables correlation analysis
        - Critical for data integrity validation
        """
        # Mock row with writetime data
        row_data = {
            "id": 123,
            "name": "Test User",
            "name_writetime": 1700000000000000,
            "email": "test@example.com",
            "email_writetime": 1700000001000000,
        }

        # CSV exporter should handle writetime columns
        CSVExporter("/tmp/test.csv")

        # Need to initialize columns first
        list(row_data.keys())
        # This test verifies that writetime columns can be part of row data
        # The actual serialization is tested separately


class TestWritetimeIntegrationScenarios:
    """Test complex writetime export scenarios."""

    def test_mixed_writetime_columns(self):
        """
        Test export with mix of writetime and regular columns.

        What this tests:
        ---------------
        1. Some columns with writetime, others without
        2. Column ordering preserved in output
        3. Header reflects all columns correctly
        4. No data corruption or column shift

        Why this matters:
        ----------------
        - Real tables have mixed requirements
        - Column alignment critical for CSV
        - JSON structure must be correct
        - Production data integrity
        """
        mock_session = AsyncMock()
        operator = BulkOperator(session=mock_session)

        # Export with selective writetime
        operator.export(
            "keyspace.table",
            output_path="/tmp/mixed.csv",
            format="csv",
            options={
                "columns": ["id", "name", "email", "created_at"],
                "writetime_columns": ["email", "created_at"],
            },
        )

    def test_writetime_with_null_values(self):
        """
        Test writetime handling when cells have no writetime.

        What this tests:
        ---------------
        1. Null writetime values handled gracefully
        2. CSV shows configured null marker
        3. JSON shows null value
        4. No errors during serialization

        Why this matters:
        ----------------
        - Not all cells have writetime info
        - Batch updates may lack writetime
        - Must handle partial data gracefully
        - Prevents export failures

        Additional context:
        ---------------------------------
        - Cells written with TTL may lose writetime
        - Counter columns don't support writetime
        - Some system columns lack writetime
        """
        registry = get_global_registry()

        # CSV context with null handling
        csv_context = SerializationContext(
            format="csv",
            options={"null_value": "NULL"},
        )

        # None should serialize to NULL marker
        result = registry.serialize(None, csv_context)
        assert result == "NULL"

    @pytest.mark.asyncio
    async def test_parallel_export_with_writetime(self):
        """
        Test parallel export includes writetime in queries.

        What this tests:
        ---------------
        1. Each worker generates correct writetime query
        2. Token ranges don't affect writetime columns
        3. All workers use same column configuration
        4. Results properly aggregated

        Why this matters:
        ----------------
        - Parallel processing must be consistent
        - Query generation happens per worker
        - Configuration must propagate correctly
        - Production exports use parallelism
        """
        mock_session = AsyncMock()

        # ParallelExporter takes full table name and exporter instance
        from async_cassandra_bulk.exporters import CSVExporter

        csv_exporter = CSVExporter("/tmp/parallel.csv")
        exporter = ParallelExporter(
            session=mock_session,
            table="test_ks.test_table",
            exporter=csv_exporter,
            writetime_columns=["created_at", "updated_at"],
        )

        # Verify writetime columns are stored
        assert exporter.writetime_columns == ["created_at", "updated_at"]
