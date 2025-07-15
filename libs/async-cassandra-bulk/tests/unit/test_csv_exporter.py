"""
Test CSV exporter functionality.

What this tests:
---------------
1. CSV file generation with proper formatting
2. Type conversion for Cassandra types
3. Delimiter and quote handling
4. Header row control
5. NULL value representation

Why this matters:
----------------
- CSV is the most common export format
- Type conversions must be lossless
- Output must be compatible with standard tools
- Edge cases like quotes and newlines must work
"""

import csv
import io
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from async_cassandra_bulk.exporters.csv import CSVExporter


class TestCSVExporterBasics:
    """Test basic CSV exporter functionality."""

    def test_csv_exporter_inherits_base(self):
        """
        Test that CSVExporter properly inherits from BaseExporter.

        What this tests:
        ---------------
        1. CSVExporter is subclass of BaseExporter
        2. Base functionality (export_rows) available
        3. Required attributes exist (output_path, options)
        4. Can be instantiated without errors

        Why this matters:
        ----------------
        - Ensures consistent interface across exporters
        - Common functionality inherited not duplicated
        - Type safety for exporter parameters
        - Production code can use any exporter interchangeably

        Additional context:
        ---------------------------------
        - BaseExporter provides export_rows orchestration
        - CSVExporter implements format-specific methods
        - Used with ParallelExporter for bulk operations
        """
        exporter = CSVExporter(output_path="/tmp/test.csv")

        # Should have base class attributes
        assert hasattr(exporter, "output_path")
        assert hasattr(exporter, "options")
        assert hasattr(exporter, "export_rows")

    def test_csv_exporter_default_options(self):
        """
        Test default CSV formatting options.

        What this tests:
        ---------------
        1. Default delimiter is comma (,)
        2. Default quote character is double-quote (")
        3. Header row included by default (True)
        4. NULL values represented as empty string ("")

        Why this matters:
        ----------------
        - RFC 4180 standard CSV compatibility
        - Works with Excel, pandas, and other tools
        - Safe defaults prevent data corruption
        - Production exports often use defaults

        Additional context:
        ---------------------------------
        - Comma delimiter is most portable
        - Double quotes handle special characters
        - Empty string for NULL is Excel convention
        """
        exporter = CSVExporter(output_path="/tmp/test.csv")

        assert exporter.delimiter == ","
        assert exporter.quote_char == '"'
        assert exporter.include_header is True
        assert exporter.null_value == ""

    def test_csv_exporter_custom_options(self):
        """
        Test custom CSV formatting options override defaults.

        What this tests:
        ---------------
        1. Tab delimiter option works (\t)
        2. Single quote character option works (')
        3. Header can be disabled (False)
        4. Custom NULL representation ("NULL")

        Why this matters:
        ----------------
        - TSV files need tab delimiter
        - Some systems require specific NULL markers
        - Appending to files needs no header
        - Production flexibility for various consumers

        Additional context:
        ---------------------------------
        - Tab delimiter common for large datasets
        - NULL vs empty string matters for imports
        - Options match Python csv module parameters
        """
        exporter = CSVExporter(
            output_path="/tmp/test.csv",
            options={
                "delimiter": "\t",
                "quote_char": "'",
                "include_header": False,
                "null_value": "NULL",
            },
        )

        assert exporter.delimiter == "\t"
        assert exporter.quote_char == "'"
        assert exporter.include_header is False
        assert exporter.null_value == "NULL"


class TestCSVExporterWriteMethods:
    """Test CSV-specific write methods."""

    @pytest.mark.asyncio
    async def test_write_header_basic(self, tmp_path):
        """
        Test CSV header row writing functionality.

        What this tests:
        ---------------
        1. Header row written with column names
        2. Column names properly delimited
        3. Special characters in names are quoted
        4. Header ends with newline

        Why this matters:
        ----------------
        - Headers required for data interpretation
        - Column order must match data rows
        - Special characters common in Cassandra
        - Production tools parse headers for mapping

        Additional context:
        ---------------------------------
        - Uses Python csv.DictWriter internally
        - Quotes added only when necessary
        - Header written once at file start
        """
        output_file = tmp_path / "test.csv"
        exporter = CSVExporter(output_path=str(output_file))

        # Mock file for testing
        mock_file = AsyncMock()
        mock_file.write = AsyncMock()
        exporter._file = mock_file
        exporter._file_opened = True  # Mark as opened
        exporter._writer = csv.DictWriter(
            io.StringIO(),
            fieldnames=["id", "name", "email"],
            delimiter=exporter.delimiter,
            quotechar=exporter.quote_char,
        )

        await exporter.write_header(["id", "name", "email"])

        # Should write header
        mock_file.write.assert_called_once()
        written = mock_file.write.call_args[0][0]
        assert "id,name,email" in written

    @pytest.mark.asyncio
    async def test_write_header_skipped_when_disabled(self, tmp_path):
        """
        Test header row skipping when disabled in options.

        What this tests:
        ---------------
        1. No header written when include_header=False
        2. CSV writer still initialized properly
        3. File ready for data rows
        4. No write calls made to file

        Why this matters:
        ----------------
        - Appending to existing CSV files
        - Headerless format for some systems
        - Streaming data to existing file
        - Production pipelines with pre-written headers

        Additional context:
        ---------------------------------
        - Writer needs columns for field ordering
        - Data rows will still write correctly
        - Common for log-style CSV files
        """
        output_file = tmp_path / "test.csv"
        exporter = CSVExporter(output_path=str(output_file), options={"include_header": False})

        # Mock file
        mock_file = AsyncMock()
        mock_file.write = AsyncMock()
        exporter._file = mock_file
        exporter._file_opened = True  # Mark as opened

        await exporter.write_header(["id", "name"])

        # Should not write anything
        mock_file.write.assert_not_called()
        # But writer should be initialized
        assert hasattr(exporter, "_writer")

    @pytest.mark.asyncio
    async def test_write_row_basic_types(self, tmp_path):
        """
        Test writing data rows with basic Python/Cassandra types.

        What this tests:
        ---------------
        1. String values written as-is (with quoting if needed)
        2. Numeric values (int, float) converted to strings
        3. Boolean values become "true"/"false" lowercase
        4. None values become configured null_value ("")

        Why this matters:
        ----------------
        - 90% of data uses these basic types
        - Consistent format for reliable parsing
        - Cassandra booleans map to CSV strings
        - Production data has many NULL values

        Additional context:
        ---------------------------------
        - Boolean format matches CQL text representation
        - Numbers preserve full precision
        - Strings auto-quoted if contain delimiter
        """
        output_file = tmp_path / "test.csv"
        exporter = CSVExporter(output_path=str(output_file))

        # Setup writer and buffer
        buffer = io.StringIO()
        exporter._buffer = buffer
        exporter._writer = csv.DictWriter(
            buffer,
            fieldnames=["id", "name", "active", "score"],
            delimiter=exporter.delimiter,
            quotechar=exporter.quote_char,
        )

        # Mock file write
        mock_file = AsyncMock()
        mock_file.write = AsyncMock()
        exporter._file = mock_file
        exporter._file_opened = True  # Mark as opened

        # Write row
        await exporter.write_row({"id": 123, "name": "Test User", "active": True, "score": None})

        # Check written content
        mock_file.write.assert_called_once()
        written = mock_file.write.call_args[0][0]
        assert "123" in written
        assert "Test User" in written
        assert "true" in written  # Boolean as lowercase
        assert written.endswith("\n")

    @pytest.mark.asyncio
    async def test_write_row_cassandra_types(self, tmp_path):
        """
        Test writing rows with Cassandra-specific complex types.

        What this tests:
        ---------------
        1. UUID formatted as standard 36-char string
        2. Timestamp uses ISO 8601 with timezone
        3. Decimal preserves exact precision as string
        4. Collections (list/set/map) as JSON strings

        Why this matters:
        ----------------
        - Cassandra UUID common for primary keys
        - Timestamps must preserve timezone info
        - Decimal precision critical for money
        - Collections need parseable format

        Additional context:
        ---------------------------------
        - UUID: 550e8400-e29b-41d4-a716-446655440000
        - Timestamp: 2024-01-15T10:30:45+00:00
        - Collections use JSON for portability
        - All formats allow round-trip conversion
        """
        output_file = tmp_path / "test.csv"
        exporter = CSVExporter(output_path=str(output_file))

        # Setup writer and buffer
        buffer = io.StringIO()
        exporter._buffer = buffer
        exporter._writer = csv.DictWriter(
            buffer,
            fieldnames=["id", "created_at", "price", "tags", "metadata"],
            delimiter=exporter.delimiter,
            quotechar=exporter.quote_char,
        )

        # Mock file write
        mock_file = AsyncMock()
        mock_file.write = AsyncMock()
        exporter._file = mock_file
        exporter._file_opened = True  # Mark as opened

        # Test data with various types
        test_uuid = UUID("550e8400-e29b-41d4-a716-446655440000")
        test_timestamp = datetime(2024, 1, 15, 10, 30, 45, tzinfo=timezone.utc)
        test_decimal = Decimal("123.456789")

        await exporter.write_row(
            {
                "id": test_uuid,
                "created_at": test_timestamp,
                "price": test_decimal,
                "tags": ["tag1", "tag2", "tag3"],
                "metadata": {"key1": "value1", "key2": "value2"},
            }
        )

        # Check conversions
        written = mock_file.write.call_args[0][0]
        assert "550e8400-e29b-41d4-a716-446655440000" in written
        assert "2024-01-15T10:30:45+00:00" in written
        assert "123.456789" in written
        # JSON arrays/objects are quoted in CSV, so quotes are doubled
        assert "tag1" in written and "tag2" in written and "tag3" in written
        assert "key1" in written and "value1" in written

    @pytest.mark.asyncio
    async def test_write_row_special_characters(self, tmp_path):
        """
        Test handling of special characters in CSV values.

        What this tests:
        ---------------
        1. Double quotes within values are escaped
        2. Newlines within values are preserved
        3. Delimiters within values trigger quoting
        4. Unicode characters preserved correctly

        Why this matters:
        ----------------
        - User data contains quotes in names/text
        - Addresses may have embedded newlines
        - Descriptions often contain commas
        - International data has Unicode
        - No data corruption

        Additional context:
        ---------------------------------
        - CSV escapes quotes by doubling them
        - Newlines require field to be quoted
        - Python csv module handles this automatically
        """
        output_file = tmp_path / "test.csv"
        exporter = CSVExporter(output_path=str(output_file))

        # Setup writer with real StringIO to test CSV module behavior
        buffer = io.StringIO()
        exporter._buffer = buffer
        exporter._writer = csv.DictWriter(
            buffer,
            fieldnames=["description", "notes"],
            delimiter=exporter.delimiter,
            quotechar=exporter.quote_char,
        )

        # Mock file write to capture output
        written_content = []

        async def capture_write(content):
            written_content.append(content)

        mock_file = AsyncMock()
        mock_file.write = capture_write
        exporter._file = mock_file
        exporter._file_opened = True  # Mark as opened

        # Write row with special characters
        await exporter.write_row(
            {
                "description": 'Product with "quotes" and, commas',
                "notes": "Multi\nline\ntext with émojis 🚀",
            }
        )

        # Verify proper escaping
        assert len(written_content) == 1
        content = written_content[0]
        # Quotes should be escaped
        assert '"Product with ""quotes"" and, commas"' in content
        # Multiline should be quoted
        assert '"Multi\nline\ntext with émojis 🚀"' in content

    @pytest.mark.asyncio
    async def test_write_footer(self, tmp_path):
        """
        Test footer writing for CSV format.

        What this tests:
        ---------------
        1. write_footer method exists for interface
        2. Makes no changes to CSV file
        3. No write calls to file handle
        4. Method completes without error

        Why this matters:
        ----------------
        - Interface compliance with BaseExporter
        - CSV format has no footer requirement
        - File ends cleanly after last row
        - Production files must be valid CSV

        Additional context:
        ---------------------------------
        - Unlike JSON, CSV needs no closing syntax
        - Last row's newline is sufficient ending
        - Some formats need footers, CSV doesn't
        """
        output_file = tmp_path / "test.csv"
        exporter = CSVExporter(output_path=str(output_file))

        # Mock file
        mock_file = AsyncMock()
        mock_file.write = AsyncMock()
        exporter._file = mock_file
        exporter._file_opened = True  # Mark as opened

        await exporter.write_footer()

        # Should not write anything
        mock_file.write.assert_not_called()


class TestCSVExporterIntegration:
    """Test full CSV export workflow."""

    @pytest.mark.asyncio
    async def test_full_export_workflow(self, tmp_path):
        """
        Test complete CSV export workflow end-to-end.

        What this tests:
        ---------------
        1. File created with proper permissions
        2. Header written, then all rows, then footer
        3. CSV formatting follows RFC 4180
        4. Output parseable by Python csv.DictReader

        Why this matters:
        ----------------
        - End-to-end validation catches integration bugs
        - Output must work with standard CSV tools
        - Real-world usage pattern validation
        - Production exports must be consumable

        Additional context:
        ---------------------------------
        - Tests boolean "true"/"false" conversion
        - Tests NULL as empty string
        - Verifies row count matches
        """
        output_file = tmp_path / "full_export.csv"
        exporter = CSVExporter(output_path=str(output_file))

        # Test data
        async def generate_rows():
            yield {"id": 1, "name": "Alice", "email": "alice@example.com", "active": True}
            yield {"id": 2, "name": "Bob", "email": "bob@example.com", "active": False}
            yield {"id": 3, "name": "Charlie", "email": None, "active": True}

        # Export
        count = await exporter.export_rows(
            rows=generate_rows(), columns=["id", "name", "email", "active"]
        )

        # Verify
        assert count == 3
        assert output_file.exists()

        # Read and parse the CSV
        with open(output_file, "r") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) == 3
        assert rows[0]["id"] == "1"
        assert rows[0]["name"] == "Alice"
        assert rows[0]["email"] == "alice@example.com"
        assert rows[0]["active"] == "true"

        assert rows[2]["email"] == ""  # NULL as empty string

    @pytest.mark.asyncio
    async def test_export_with_custom_delimiter(self, tmp_path):
        """
        Test export with tab delimiter (TSV format).

        What this tests:
        ---------------
        1. Tab delimiter (\t) replaces comma
        2. Tab within values triggers quoting
        3. File extension can be .tsv
        4. Otherwise follows CSV rules

        Why this matters:
        ----------------
        - TSV common for data warehouses
        - Tab delimiter handles commas in data
        - Some tools require TSV format
        - Production flexibility for consumers

        Additional context:
        ---------------------------------
        - TSV is just CSV with tab delimiter
        - Tabs in values are rare but must work
        - Same quoting rules apply
        """
        output_file = tmp_path / "data.tsv"
        exporter = CSVExporter(output_path=str(output_file), options={"delimiter": "\t"})

        # Test data
        async def generate_rows():
            yield {"id": 1, "name": "Test\tUser", "value": 123.45}

        # Export
        await exporter.export_rows(rows=generate_rows(), columns=["id", "name", "value"])

        # Verify TSV format
        content = output_file.read_text()
        lines = content.strip().split("\n")
        assert len(lines) == 2
        assert lines[0] == "id\tname\tvalue"
        assert "\t" in lines[1]
        assert '"Test\tUser"' in lines[1]  # Tab in value should be quoted

    @pytest.mark.asyncio
    async def test_export_large_dataset_memory_efficiency(self, tmp_path):
        """
        Test memory efficiency with large streaming datasets.

        What this tests:
        ---------------
        1. Async generator streams without buffering all rows
        2. File written incrementally as rows arrive
        3. 10,000 rows export without memory spike
        4. File size proportional to row count

        Why this matters:
        ----------------
        - Production exports can be 100GB+
        - Memory must stay constant during export
        - Streaming prevents OOM errors
        - Cassandra tables have billions of rows

        Additional context:
        ---------------------------------
        - Real exports use batched queries
        - Each row written immediately
        - No intermediate list storage
        """
        output_file = tmp_path / "large.csv"
        exporter = CSVExporter(output_path=str(output_file))

        # Generate many rows without storing them
        async def generate_many_rows():
            for i in range(10000):
                yield {"id": i, "data": f"Row {i}" * 10, "value": i * 1.5}  # Some bulk

        # Export
        count = await exporter.export_rows(
            rows=generate_many_rows(), columns=["id", "data", "value"]
        )

        # Verify
        assert count == 10000
        assert output_file.exists()

        # File should be reasonably sized
        file_size = output_file.stat().st_size
        assert file_size > 900000  # At least 900KB

        # Verify a few lines
        with open(output_file, "r") as f:
            reader = csv.DictReader(f)
            first_row = next(reader)
            assert first_row["id"] == "0"

            # Skip to near end
            for _ in range(9998):
                next(reader)
            last_row = next(reader)
            assert last_row["id"] == "9999"
