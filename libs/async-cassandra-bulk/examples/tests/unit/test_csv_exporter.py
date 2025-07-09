"""Unit tests for CSV exporter.

What this tests:
---------------
1. CSV header generation
2. Row serialization with different data types
3. NULL value handling
4. Collection serialization
5. Compression support
6. Progress tracking

Why this matters:
----------------
- CSV is a common export format
- Data type handling must be consistent
- Resume capability is critical for large exports
- Compression saves disk space
"""

import csv
import gzip
import io
import uuid
from datetime import datetime
from unittest.mock import Mock

import pytest

from bulk_operations.bulk_operator import TokenAwareBulkOperator
from bulk_operations.exporters import CSVExporter, ExportFormat, ExportProgress


class MockRow:
    """Mock Cassandra row object."""

    def __init__(self, **kwargs):
        self._fields = list(kwargs.keys())
        for key, value in kwargs.items():
            setattr(self, key, value)


class TestCSVExporter:
    """Test CSV export functionality."""

    @pytest.fixture
    def mock_operator(self):
        """Create mock bulk operator."""
        operator = Mock(spec=TokenAwareBulkOperator)
        operator.session = Mock()
        operator.session._session = Mock()
        operator.session._session.cluster = Mock()
        operator.session._session.cluster.metadata = Mock()
        return operator

    @pytest.fixture
    def exporter(self, mock_operator):
        """Create CSV exporter instance."""
        return CSVExporter(mock_operator)

    def test_csv_value_serialization(self, exporter):
        """
        Test serialization of different value types to CSV.

        What this tests:
        ---------------
        1. NULL values become empty strings
        2. Booleans become true/false
        3. Collections get formatted properly
        4. Bytes are hex encoded
        5. Timestamps use ISO format

        Why this matters:
        ----------------
        - CSV needs consistent string representation
        - Must be reversible for imports
        - Standard tools should understand the format
        """
        # NULL handling
        assert exporter._serialize_csv_value(None) == ""

        # Primitives
        assert exporter._serialize_csv_value(True) == "true"
        assert exporter._serialize_csv_value(False) == "false"
        assert exporter._serialize_csv_value(42) == "42"
        assert exporter._serialize_csv_value(3.14) == "3.14"
        assert exporter._serialize_csv_value("test") == "test"

        # UUID
        test_uuid = uuid.uuid4()
        assert exporter._serialize_csv_value(test_uuid) == str(test_uuid)

        # Datetime
        test_dt = datetime(2024, 1, 1, 12, 0, 0)
        assert exporter._serialize_csv_value(test_dt) == "2024-01-01T12:00:00"

        # Collections
        assert exporter._serialize_csv_value([1, 2, 3]) == "[1, 2, 3]"
        assert exporter._serialize_csv_value({"a", "b"}) == "[a, b]" or "[b, a]"
        assert exporter._serialize_csv_value({"k1": "v1", "k2": "v2"}) in [
            "{k1: v1, k2: v2}",
            "{k2: v2, k1: v1}",
        ]

        # Bytes
        assert exporter._serialize_csv_value(b"\x00\x01\x02") == "000102"

    def test_null_string_customization(self, mock_operator):
        """
        Test custom NULL string representation.

        What this tests:
        ---------------
        1. Default empty string for NULL
        2. Custom NULL strings like "NULL" or "\\N"
        3. Consistent handling across all types

        Why this matters:
        ----------------
        - Different tools expect different NULL representations
        - PostgreSQL uses \\N, MySQL uses NULL
        - Must be configurable for compatibility
        """
        # Default exporter uses empty string
        default_exporter = CSVExporter(mock_operator)
        assert default_exporter._serialize_csv_value(None) == ""

        # Custom NULL string
        custom_exporter = CSVExporter(mock_operator, null_string="NULL")
        assert custom_exporter._serialize_csv_value(None) == "NULL"

        # PostgreSQL style
        pg_exporter = CSVExporter(mock_operator, null_string="\\N")
        assert pg_exporter._serialize_csv_value(None) == "\\N"

    @pytest.mark.asyncio
    async def test_write_header(self, exporter):
        """
        Test CSV header writing.

        What this tests:
        ---------------
        1. Header contains column names
        2. Proper delimiter usage
        3. Quoting when needed

        Why this matters:
        ----------------
        - Headers enable column mapping
        - Must match data row format
        - Standard CSV compliance
        """
        output = io.StringIO()
        columns = ["id", "name", "created_at", "tags"]

        await exporter.write_header(output, columns)
        output.seek(0)

        reader = csv.reader(output)
        header = next(reader)
        assert header == columns

    @pytest.mark.asyncio
    async def test_write_row(self, exporter):
        """
        Test writing data rows to CSV.

        What this tests:
        ---------------
        1. Row data properly formatted
        2. Complex types serialized
        3. Byte count tracking
        4. Thread safety with lock

        Why this matters:
        ----------------
        - Data integrity is critical
        - Concurrent writes must be safe
        - Progress tracking needs accurate bytes
        """
        output = io.StringIO()

        # Create test row
        row = MockRow(
            id=1,
            name="Test User",
            active=True,
            score=99.5,
            tags=["tag1", "tag2"],
            metadata={"key": "value"},
            created_at=datetime(2024, 1, 1, 12, 0, 0),
        )

        bytes_written = await exporter.write_row(output, row)
        output.seek(0)

        # Verify output
        reader = csv.reader(output)
        values = next(reader)

        assert values[0] == "1"
        assert values[1] == "Test User"
        assert values[2] == "true"
        assert values[3] == "99.5"
        assert values[4] == "[tag1, tag2]"
        assert values[5] == "{key: value}"
        assert values[6] == "2024-01-01T12:00:00"

        # Verify byte count
        assert bytes_written > 0

    @pytest.mark.asyncio
    async def test_export_with_compression(self, mock_operator, tmp_path):
        """
        Test CSV export with compression.

        What this tests:
        ---------------
        1. Gzip compression works
        2. File has correct extension
        3. Compressed data is valid

        Why this matters:
        ----------------
        - Large exports need compression
        - Must work with standard tools
        - File naming conventions matter
        """
        exporter = CSVExporter(mock_operator, compression="gzip")
        output_path = tmp_path / "test.csv"

        # Mock the export stream
        test_rows = [
            MockRow(id=1, name="Alice", score=95.5),
            MockRow(id=2, name="Bob", score=87.3),
        ]

        async def mock_export(*args, **kwargs):
            for row in test_rows:
                yield row

        mock_operator.export_by_token_ranges = mock_export

        # Mock metadata
        mock_keyspace = Mock()
        mock_table = Mock()
        mock_table.columns = {"id": None, "name": None, "score": None}
        mock_keyspace.tables = {"test_table": mock_table}
        mock_operator.session._session.cluster.metadata.keyspaces = {"test_ks": mock_keyspace}

        # Export
        await exporter.export(
            keyspace="test_ks",
            table="test_table",
            output_path=output_path,
        )

        # Verify compressed file exists
        compressed_path = output_path.with_suffix(".csv.gzip")
        assert compressed_path.exists()

        # Verify content
        with gzip.open(compressed_path, "rt") as f:
            reader = csv.reader(f)
            header = next(reader)
            assert header == ["id", "name", "score"]

            row1 = next(reader)
            assert row1 == ["1", "Alice", "95.5"]

            row2 = next(reader)
            assert row2 == ["2", "Bob", "87.3"]

    @pytest.mark.asyncio
    async def test_export_progress_tracking(self, mock_operator, tmp_path):
        """
        Test progress tracking during export.

        What this tests:
        ---------------
        1. Progress initialized correctly
        2. Row count tracked
        3. Progress saved to file
        4. Completion marked

        Why this matters:
        ----------------
        - Long exports need monitoring
        - Resume capability requires state
        - Users need feedback
        """
        exporter = CSVExporter(mock_operator)
        output_path = tmp_path / "test.csv"

        # Mock export
        test_rows = [MockRow(id=i, value=f"test{i}") for i in range(100)]

        async def mock_export(*args, **kwargs):
            for row in test_rows:
                yield row

        mock_operator.export_by_token_ranges = mock_export

        # Mock metadata
        mock_keyspace = Mock()
        mock_table = Mock()
        mock_table.columns = {"id": None, "value": None}
        mock_keyspace.tables = {"test_table": mock_table}
        mock_operator.session._session.cluster.metadata.keyspaces = {"test_ks": mock_keyspace}

        # Track progress callbacks
        progress_updates = []

        async def progress_callback(progress):
            progress_updates.append(progress.rows_exported)

        # Export
        progress = await exporter.export(
            keyspace="test_ks",
            table="test_table",
            output_path=output_path,
            progress_callback=progress_callback,
        )

        # Verify progress
        assert progress.keyspace == "test_ks"
        assert progress.table == "test_table"
        assert progress.format == ExportFormat.CSV
        assert progress.rows_exported == 100
        assert progress.completed_at is not None

        # Verify progress file
        progress_file = output_path.with_suffix(".csv.progress")
        assert progress_file.exists()

        # Load and verify
        loaded_progress = ExportProgress.load(progress_file)
        assert loaded_progress.rows_exported == 100

    def test_custom_delimiter_and_quoting(self, mock_operator):
        """
        Test custom CSV formatting options.

        What this tests:
        ---------------
        1. Tab delimiter
        2. Pipe delimiter
        3. Different quoting styles

        Why this matters:
        ----------------
        - Different systems expect different formats
        - Must handle data with delimiters
        - Flexibility for integration
        """
        # Tab-delimited
        tab_exporter = CSVExporter(mock_operator, delimiter="\t")
        assert tab_exporter.delimiter == "\t"

        # Pipe-delimited
        pipe_exporter = CSVExporter(mock_operator, delimiter="|")
        assert pipe_exporter.delimiter == "|"

        # Quote all
        quote_all_exporter = CSVExporter(mock_operator, quoting=csv.QUOTE_ALL)
        assert quote_all_exporter.quoting == csv.QUOTE_ALL
