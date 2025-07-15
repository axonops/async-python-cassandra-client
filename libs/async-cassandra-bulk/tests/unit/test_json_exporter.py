"""
Test JSON exporter functionality.

What this tests:
---------------
1. JSON file generation with proper formatting
2. Type conversion for Cassandra types
3. Different JSON formats (object vs array)
4. Streaming vs full document modes
5. Custom JSON encoders

Why this matters:
----------------
- JSON is widely used for data interchange
- Must handle complex nested structures
- Streaming mode for large datasets
- Type preservation for round-trip compatibility
"""

import json
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from async_cassandra_bulk.exporters.json import JSONExporter


class TestJSONExporterBasics:
    """Test basic JSON exporter functionality."""

    def test_json_exporter_inherits_base(self):
        """
        Test that JSONExporter inherits from BaseExporter.

        What this tests:
        ---------------
        1. Proper inheritance hierarchy
        2. Base functionality available

        Why this matters:
        ----------------
        - Ensures consistent interface
        - Common functionality is reused
        """
        exporter = JSONExporter(output_path="/tmp/test.json")

        # Should have base class attributes
        assert hasattr(exporter, "output_path")
        assert hasattr(exporter, "options")
        assert hasattr(exporter, "export_rows")

    def test_json_exporter_default_options(self):
        """
        Test default JSON options.

        What this tests:
        ---------------
        1. Default mode is 'array'
        2. Pretty printing disabled by default
        3. Streaming disabled by default

        Why this matters:
        ----------------
        - Sensible defaults for common use
        - Compact output by default
        """
        exporter = JSONExporter(output_path="/tmp/test.json")

        assert exporter.mode == "array"
        assert exporter.pretty is False
        assert exporter.streaming is False

    def test_json_exporter_custom_options(self):
        """
        Test custom JSON options.

        What this tests:
        ---------------
        1. Options override defaults
        2. All options are applied

        Why this matters:
        ----------------
        - Flexibility for different requirements
        - Support various JSON structures
        """
        exporter = JSONExporter(
            output_path="/tmp/test.json",
            options={
                "mode": "objects",
                "pretty": True,
                "streaming": True,
            },
        )

        assert exporter.mode == "objects"
        assert exporter.pretty is True
        assert exporter.streaming is True


class TestJSONExporterWriteMethods:
    """Test JSON-specific write methods."""

    @pytest.mark.asyncio
    async def test_write_header_array_mode(self, tmp_path):
        """
        Test header writing in array mode.

        What this tests:
        ---------------
        1. Opens JSON array with '['
        2. Stores columns for later use

        Why this matters:
        ----------------
        - Array mode needs proper opening
        - Columns needed for consistent ordering
        """
        output_file = tmp_path / "test.json"
        exporter = JSONExporter(output_path=str(output_file))

        # Mock file
        mock_file = AsyncMock()
        mock_file.write = AsyncMock()
        exporter._file = mock_file
        exporter._file_opened = True  # Mark as opened

        await exporter.write_header(["id", "name", "email"])

        # Should write array opening
        mock_file.write.assert_called_once_with("[")
        assert exporter._columns == ["id", "name", "email"]
        assert exporter._first_row is True

    @pytest.mark.asyncio
    async def test_write_header_objects_mode(self, tmp_path):
        """
        Test header writing in objects mode.

        What this tests:
        ---------------
        1. No header in objects mode
        2. Still stores columns

        Why this matters:
        ----------------
        - Objects mode is newline-delimited
        - No array wrapper needed
        """
        output_file = tmp_path / "test.json"
        exporter = JSONExporter(output_path=str(output_file), options={"mode": "objects"})

        # Mock file
        mock_file = AsyncMock()
        mock_file.write = AsyncMock()
        exporter._file = mock_file
        exporter._file_opened = True  # Mark as opened

        await exporter.write_header(["id", "name"])

        # Should not write anything in objects mode
        mock_file.write.assert_not_called()
        assert exporter._columns == ["id", "name"]

    @pytest.mark.asyncio
    async def test_write_row_basic_types(self, tmp_path):
        """
        Test writing rows with basic types.

        What this tests:
        ---------------
        1. String, numeric, boolean values
        2. None becomes null
        3. Proper JSON formatting

        Why this matters:
        ----------------
        - Most common data types
        - Valid JSON output
        """
        output_file = tmp_path / "test.json"
        exporter = JSONExporter(output_path=str(output_file))
        exporter._columns = ["id", "name", "active", "score"]
        exporter._first_row = True

        # Mock file
        mock_file = AsyncMock()
        mock_file.write = AsyncMock()
        exporter._file = mock_file
        exporter._file_opened = True  # Mark as opened

        # Write row
        await exporter.write_row({"id": 123, "name": "Test User", "active": True, "score": None})

        # Check written content
        written = mock_file.write.call_args[0][0]
        data = json.loads(written)
        assert data["id"] == 123
        assert data["name"] == "Test User"
        assert data["active"] is True
        assert data["score"] is None

    @pytest.mark.asyncio
    async def test_write_row_cassandra_types(self, tmp_path):
        """
        Test writing rows with Cassandra-specific types.

        What this tests:
        ---------------
        1. UUID serialization
        2. Timestamp formatting
        3. Decimal handling
        4. Collections preservation

        Why this matters:
        ----------------
        - Cassandra type compatibility
        - Round-trip data integrity
        """
        output_file = tmp_path / "test.json"
        exporter = JSONExporter(output_path=str(output_file))
        exporter._columns = ["id", "created_at", "price", "tags", "metadata"]
        exporter._first_row = True

        # Mock file
        mock_file = AsyncMock()
        mock_file.write = AsyncMock()
        exporter._file = mock_file
        exporter._file_opened = True  # Mark as opened

        # Test data
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

        # Parse and verify
        written = mock_file.write.call_args[0][0]
        data = json.loads(written)
        assert data["id"] == "550e8400-e29b-41d4-a716-446655440000"
        assert data["created_at"] == "2024-01-15T10:30:45+00:00"
        assert data["price"] == "123.456789"
        assert data["tags"] == ["tag1", "tag2", "tag3"]
        assert data["metadata"] == {"key1": "value1", "key2": "value2"}

    @pytest.mark.asyncio
    async def test_write_row_array_mode_multiple(self, tmp_path):
        """
        Test writing multiple rows in array mode.

        What this tests:
        ---------------
        1. First row has no comma
        2. Subsequent rows have comma prefix
        3. Proper array formatting

        Why this matters:
        ----------------
        - Valid JSON array syntax
        - Streaming compatibility
        """
        output_file = tmp_path / "test.json"
        exporter = JSONExporter(output_path=str(output_file))
        exporter._columns = ["id", "name"]
        exporter._first_row = True

        # Mock file
        written_content = []

        async def capture_write(content):
            written_content.append(content)

        mock_file = AsyncMock()
        mock_file.write = capture_write
        exporter._file = mock_file
        exporter._file_opened = True  # Mark as opened

        # Write multiple rows
        await exporter.write_row({"id": 1, "name": "Alice"})
        await exporter.write_row({"id": 2, "name": "Bob"})

        # First row should not have comma
        assert len(written_content) == 2
        assert not written_content[0].startswith(",")
        # Second row should have comma
        assert written_content[1].startswith(",")

        # Both should be valid JSON
        json.loads(written_content[0])
        json.loads(written_content[1][1:])  # Skip comma

    @pytest.mark.asyncio
    async def test_write_row_objects_mode(self, tmp_path):
        """
        Test writing rows in objects mode (JSONL).

        What this tests:
        ---------------
        1. Each row on separate line
        2. No commas between objects
        3. Valid JSONL format

        Why this matters:
        ----------------
        - JSONL is streamable
        - Each line is valid JSON
        """
        output_file = tmp_path / "test.json"
        exporter = JSONExporter(output_path=str(output_file), options={"mode": "objects"})
        exporter._columns = ["id", "name"]

        # Mock file
        written_content = []

        async def capture_write(content):
            written_content.append(content)

        mock_file = AsyncMock()
        mock_file.write = capture_write
        exporter._file = mock_file
        exporter._file_opened = True  # Mark as opened

        # Write multiple rows
        await exporter.write_row({"id": 1, "name": "Alice"})
        await exporter.write_row({"id": 2, "name": "Bob"})

        # Each write should end with newline
        assert all(content.endswith("\n") for content in written_content)

        # Each line should be valid JSON
        for content in written_content:
            json.loads(content.strip())

    @pytest.mark.asyncio
    async def test_write_footer_array_mode(self, tmp_path):
        """
        Test footer writing in array mode.

        What this tests:
        ---------------
        1. Closes array with ']'
        2. Adds newline for clean ending

        Why this matters:
        ----------------
        - Valid JSON requires closing
        - Clean file ending
        """
        output_file = tmp_path / "test.json"
        exporter = JSONExporter(output_path=str(output_file))

        # Mock file
        mock_file = AsyncMock()
        mock_file.write = AsyncMock()
        exporter._file = mock_file
        exporter._file_opened = True  # Mark as opened

        await exporter.write_footer()

        # Should close array
        mock_file.write.assert_called_once_with("]\n")

    @pytest.mark.asyncio
    async def test_write_footer_objects_mode(self, tmp_path):
        """
        Test footer writing in objects mode.

        What this tests:
        ---------------
        1. No footer in objects mode
        2. File ends naturally

        Why this matters:
        ----------------
        - JSONL has no footer
        - Clean streaming format
        """
        output_file = tmp_path / "test.json"
        exporter = JSONExporter(output_path=str(output_file), options={"mode": "objects"})

        # Mock file
        mock_file = AsyncMock()
        mock_file.write = AsyncMock()
        exporter._file = mock_file
        exporter._file_opened = True  # Mark as opened

        await exporter.write_footer()

        # Should not write anything
        mock_file.write.assert_not_called()


class TestJSONExporterIntegration:
    """Test full JSON export workflow."""

    @pytest.mark.asyncio
    async def test_full_export_array_mode(self, tmp_path):
        """
        Test complete export in array mode.

        What this tests:
        ---------------
        1. Valid JSON array output
        2. All rows included
        3. Proper formatting

        Why this matters:
        ----------------
        - End-to-end validation
        - Output is valid JSON
        """
        output_file = tmp_path / "export.json"
        exporter = JSONExporter(output_path=str(output_file))

        # Test data
        async def generate_rows():
            yield {"id": 1, "name": "Alice", "active": True}
            yield {"id": 2, "name": "Bob", "active": False}
            yield {"id": 3, "name": "Charlie", "active": True}

        # Export
        count = await exporter.export_rows(rows=generate_rows(), columns=["id", "name", "active"])

        # Verify
        assert count == 3
        assert output_file.exists()

        # Parse and validate JSON
        with open(output_file) as f:
            data = json.load(f)

        assert isinstance(data, list)
        assert len(data) == 3
        assert data[0]["id"] == 1
        assert data[0]["name"] == "Alice"
        assert data[0]["active"] is True

    @pytest.mark.asyncio
    async def test_full_export_objects_mode(self, tmp_path):
        """
        Test complete export in objects mode (JSONL).

        What this tests:
        ---------------
        1. Valid JSONL output
        2. Each line is valid JSON
        3. No array wrapper

        Why this matters:
        ----------------
        - JSONL is streamable
        - Common for data pipelines
        """
        output_file = tmp_path / "export.jsonl"
        exporter = JSONExporter(output_path=str(output_file), options={"mode": "objects"})

        # Test data
        async def generate_rows():
            yield {"id": 1, "name": "Alice"}
            yield {"id": 2, "name": "Bob"}

        # Export
        count = await exporter.export_rows(rows=generate_rows(), columns=["id", "name"])

        # Verify
        assert count == 2
        assert output_file.exists()

        # Parse each line
        lines = output_file.read_text().strip().split("\n")
        assert len(lines) == 2

        for i, line in enumerate(lines):
            data = json.loads(line)
            assert data["id"] == i + 1

    @pytest.mark.asyncio
    async def test_export_with_pretty_printing(self, tmp_path):
        """
        Test export with pretty printing enabled.

        What this tests:
        ---------------
        1. Indented JSON output
        2. Human-readable format
        3. Still valid JSON

        Why this matters:
        ----------------
        - Debugging and inspection
        - Human-readable output
        """
        output_file = tmp_path / "pretty.json"
        exporter = JSONExporter(output_path=str(output_file), options={"pretty": True})

        # Test data
        async def generate_rows():
            yield {"id": 1, "name": "Test User", "metadata": {"key": "value"}}

        # Export
        await exporter.export_rows(rows=generate_rows(), columns=["id", "name", "metadata"])

        # Verify formatting
        content = output_file.read_text()
        assert "  " in content  # Should have indentation
        assert content.count("\n") > 3  # Multiple lines

        # Still valid JSON
        data = json.loads(content)
        assert data[0]["metadata"]["key"] == "value"

    @pytest.mark.asyncio
    async def test_export_empty_dataset(self, tmp_path):
        """
        Test exporting empty dataset.

        What this tests:
        ---------------
        1. Empty array for array mode
        2. Empty file for objects mode
        3. Still valid JSON

        Why this matters:
        ----------------
        - Edge case handling
        - Valid output even when empty
        """
        output_file = tmp_path / "empty.json"
        exporter = JSONExporter(output_path=str(output_file))

        # Empty data
        async def generate_rows():
            return
            yield  # Make it a generator

        # Export
        count = await exporter.export_rows(rows=generate_rows(), columns=["id", "name"])

        # Verify
        assert count == 0
        assert output_file.exists()

        # Should be empty array
        with open(output_file) as f:
            data = json.load(f)
        assert data == []
