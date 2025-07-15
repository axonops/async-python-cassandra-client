"""
Integration tests for CSV and JSON exporters with real data.

What this tests:
---------------
1. Type conversions with actual Cassandra types
2. Large file handling and streaming
3. Special characters and edge cases from real data
4. Performance with different formats
5. Round-trip data integrity

Why this matters:
----------------
- Real Cassandra types differ from Python natives
- File I/O performance needs validation
- Character encoding issues only appear with real data
- Format-specific optimizations need testing
"""

import csv
import json
from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4

import pytest

from async_cassandra_bulk import CSVExporter, JSONExporter, ParallelExporter


class TestCSVExporterIntegration:
    """Test CSV exporter with real Cassandra data types."""

    @pytest.mark.asyncio
    async def test_csv_export_all_cassandra_types(self, session, tmp_path):
        """
        Test CSV export with all Cassandra data types.

        What this tests:
        ---------------
        1. UUID converts to standard string format
        2. Timestamps convert to ISO 8601 format
        3. Collections convert to JSON strings
        4. Booleans become lowercase true/false

        Why this matters:
        ----------------
        - Type conversion errors cause data loss
        - CSV must be importable to other systems
        - Round-trip compatibility required
        - Production data uses all types

        Additional context:
        ---------------------------------
        - Real Cassandra returns native types
        - Driver handles type conversions
        - CSV must represent all types as strings
        """
        # Create table with all types
        table_name = f"all_types_{int(datetime.now().timestamp() * 1000)}"

        await session.execute(
            f"""
            CREATE TABLE test_bulk.{table_name} (
                id uuid PRIMARY KEY,
                text_col text,
                int_col int,
                bigint_col bigint,
                float_col float,
                double_col double,
                decimal_col decimal,
                boolean_col boolean,
                timestamp_col timestamp,
                date_col date,
                time_col time,
                list_col list<text>,
                set_col set<int>,
                map_col map<text, int>
            )
        """
        )

        # Insert test data
        test_uuid = uuid4()
        test_timestamp = datetime.now(timezone.utc)
        test_decimal = Decimal("123.456789")

        await session.execute(
            f"""
            INSERT INTO test_bulk.{table_name} (
                id, text_col, int_col, bigint_col, float_col, double_col,
                decimal_col, boolean_col, timestamp_col, date_col, time_col,
                list_col, set_col, map_col
            ) VALUES (
                {test_uuid}, 'test text', 42, 9223372036854775807, 3.14, 2.71828,
                {test_decimal}, true, '{test_timestamp.isoformat()}', '2024-01-15', '14:30:45',
                ['item1', 'item2'], {{1, 2, 3}}, {{'key1': 10, 'key2': 20}}
            )
        """
        )

        try:
            # Export to CSV
            output_file = tmp_path / "all_types.csv"
            exporter = CSVExporter(output_path=str(output_file))

            parallel = ParallelExporter(
                session=session, table=f"test_bulk.{table_name}", exporter=exporter
            )

            stats = await parallel.export()

            assert stats.rows_processed == 1

            # Read and verify CSV
            with open(output_file, "r") as f:
                reader = csv.DictReader(f)
                row = next(reader)

            # Verify type conversions
            assert row["id"] == str(test_uuid)
            assert row["text_col"] == "test text"
            assert row["int_col"] == "42"
            assert row["bigint_col"] == "9223372036854775807"
            assert row["boolean_col"] == "true"
            assert row["decimal_col"] == str(test_decimal)

            # Collections should be JSON
            assert '["item1", "item2"]' in row["list_col"] or '["item1","item2"]' in row["list_col"]
            assert "[1, 2, 3]" in row["set_col"] or "[1,2,3]" in row["set_col"]

        finally:
            await session.execute(f"DROP TABLE test_bulk.{table_name}")

    @pytest.mark.asyncio
    async def test_csv_export_special_characters(self, session, tmp_path):
        """
        Test CSV export with special characters and edge cases.

        What this tests:
        ---------------
        1. Quotes within values are escaped properly
        2. Newlines within values are preserved
        3. Commas in values don't break parsing
        4. Unicode characters handled correctly

        Why this matters:
        ----------------
        - Real data contains messy strings
        - CSV parsers must handle escaped data
        - Data integrity across systems
        - Production data has international characters

        Additional context:
        ---------------------------------
        - CSV escaping rules are complex
        - Python csv module handles RFC 4180
        - Must test with actual file I/O
        """
        table_name = f"special_chars_{int(datetime.now().timestamp() * 1000)}"

        await session.execute(
            f"""
            CREATE TABLE test_bulk.{table_name} (
                id uuid PRIMARY KEY,
                description text,
                notes text
            )
        """
        )

        # Insert data with special characters
        test_data = [
            (uuid4(), "Normal text", "Simple note"),
            (uuid4(), 'Text with "quotes"', "Note with, comma"),
            (uuid4(), "Multi\nline\ntext", "Unicode: émojis 🚀 work"),
            (uuid4(), "Tab\tseparated", "All special: \",\n\t'"),
        ]

        insert_stmt = await session.prepare(
            f"""
            INSERT INTO test_bulk.{table_name} (id, description, notes)
            VALUES (?, ?, ?)
        """
        )

        for row in test_data:
            await session.execute(insert_stmt, row)

        try:
            # Export to CSV
            output_file = tmp_path / "special_chars.csv"
            exporter = CSVExporter(output_path=str(output_file))

            parallel = ParallelExporter(
                session=session, table=f"test_bulk.{table_name}", exporter=exporter
            )

            stats = await parallel.export()

            assert stats.rows_processed == len(test_data)

            # Read back and verify
            with open(output_file, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                rows = list(reader)

            # Find each test case
            for original in test_data:
                found = next(r for r in rows if r["id"] == str(original[0]))
                assert found["description"] == original[1]
                assert found["notes"] == original[2]

        finally:
            await session.execute(f"DROP TABLE test_bulk.{table_name}")

    @pytest.mark.asyncio
    async def test_csv_export_null_handling(self, session, tmp_path):
        """
        Test CSV export with NULL values.

        What this tests:
        ---------------
        1. NULL values export as configured null_value
        2. Empty strings distinct from NULL
        3. Consistent NULL representation
        4. Custom NULL markers work

        Why this matters:
        ----------------
        - NULL vs empty string semantics
        - Import systems need NULL detection
        - Data warehouse compatibility
        - Production data has many NULLs

        Additional context:
        ---------------------------------
        - Default NULL is empty string
        - Can configure as "NULL", "\\N", etc.
        - Important for data integrity
        """
        table_name = f"null_test_{int(datetime.now().timestamp() * 1000)}"

        await session.execute(
            f"""
            CREATE TABLE test_bulk.{table_name} (
                id uuid PRIMARY KEY,
                required_field text,
                optional_field text,
                numeric_field int
            )
        """
        )

        # Insert mix of NULL and non-NULL
        test_data = [
            (uuid4(), "value1", "optional1", 100),
            (uuid4(), "value2", None, 200),
            (uuid4(), "value3", "", None),  # Empty string vs NULL
            (uuid4(), "value4", None, None),
        ]

        for row in test_data:
            if row[2] is None and row[3] is None:
                await session.execute(
                    f"""
                    INSERT INTO test_bulk.{table_name} (id, required_field)
                    VALUES ({row[0]}, '{row[1]}')
                """
                )
            elif row[2] is None:
                await session.execute(
                    f"""
                    INSERT INTO test_bulk.{table_name} (id, required_field, numeric_field)
                    VALUES ({row[0]}, '{row[1]}', {row[3]})
                """
                )
            elif row[3] is None:
                await session.execute(
                    f"""
                    INSERT INTO test_bulk.{table_name} (id, required_field, optional_field)
                    VALUES ({row[0]}, '{row[1]}', '{row[2]}')
                """
                )
            else:
                await session.execute(
                    f"""
                    INSERT INTO test_bulk.{table_name} (id, required_field, optional_field, numeric_field)
                    VALUES ({row[0]}, '{row[1]}', '{row[2]}', {row[3]})
                """
                )

        try:
            # Test with custom NULL marker
            output_file = tmp_path / "null_handling.csv"
            exporter = CSVExporter(output_path=str(output_file), options={"null_value": "NULL"})

            parallel = ParallelExporter(
                session=session, table=f"test_bulk.{table_name}", exporter=exporter
            )

            stats = await parallel.export()

            assert stats.rows_processed == len(test_data)

            # Verify NULL handling
            with open(output_file, "r") as f:
                content = f.read()
                assert "NULL" in content  # Custom null marker used

            with open(output_file, "r") as f:
                reader = csv.DictReader(f)
                rows = {r["id"]: r for r in reader}

            # Check specific NULL vs empty string handling
            for original in test_data:
                row = rows[str(original[0])]
                if original[2] is None:
                    assert row["optional_field"] == "NULL"
                elif original[2] == "":
                    assert row["optional_field"] == ""  # Empty preserved

        finally:
            await session.execute(f"DROP TABLE test_bulk.{table_name}")


class TestJSONExporterIntegration:
    """Test JSON exporter with real Cassandra data."""

    @pytest.mark.asyncio
    async def test_json_export_nested_collections(self, session, tmp_path):
        """
        Test JSON export with nested collection types.

        What this tests:
        ---------------
        1. Nested collections serialize correctly
        2. Complex types preserve structure
        3. JSON remains valid with deep nesting
        4. Large collections handled efficiently

        Why this matters:
        ----------------
        - Modern apps use complex data structures
        - JSON must preserve nesting
        - NoSQL patterns use nested data
        - Production data has deep structures

        Additional context:
        ---------------------------------
        - Cassandra supports list<frozen<map>>
        - JSON natural format for collections
        - Must handle arbitrary nesting depth
        """
        table_name = f"nested_json_{int(datetime.now().timestamp() * 1000)}"

        await session.execute(
            f"""
            CREATE TABLE test_bulk.{table_name} (
                id uuid PRIMARY KEY,
                metadata map<text, text>,
                tags set<text>,
                events list<frozen<map<text, text>>>
            )
        """
        )

        # Insert complex nested data
        test_id = uuid4()
        await session.execute(
            f"""
            INSERT INTO test_bulk.{table_name} (id, metadata, tags, events)
            VALUES (
                {test_id},
                {{'version': '1.0', 'type': 'user', 'nested': '{{"key": "value"}}'}},
                {{'tag1', 'tag2', 'special-tag'}},
                [
                    {{'event': 'login', 'timestamp': '2024-01-01T10:00:00Z'}},
                    {{'event': 'purchase', 'amount': '99.99'}}
                ]
            )
        """
        )

        try:
            # Export to JSON
            output_file = tmp_path / "nested.json"
            exporter = JSONExporter(output_path=str(output_file))

            parallel = ParallelExporter(
                session=session, table=f"test_bulk.{table_name}", exporter=exporter
            )

            stats = await parallel.export()

            assert stats.rows_processed == 1

            # Parse and verify JSON structure
            with open(output_file, "r") as f:
                data = json.load(f)

            assert len(data) == 1
            row = data[0]

            # Verify nested structures preserved
            assert isinstance(row["metadata"], dict)
            assert row["metadata"]["version"] == "1.0"
            assert isinstance(row["tags"], list)  # Set becomes list
            assert "tag1" in row["tags"]
            assert isinstance(row["events"], list)
            assert len(row["events"]) == 2
            assert row["events"][0]["event"] == "login"

        finally:
            await session.execute(f"DROP TABLE test_bulk.{table_name}")

    @pytest.mark.asyncio
    async def test_json_export_streaming_mode(self, session, tmp_path):
        """
        Test JSON export in streaming/objects mode (JSONL).

        What this tests:
        ---------------
        1. Each row on separate line (JSONL format)
        2. No array wrapper for streaming
        3. Each line is valid JSON object
        4. Supports incremental processing

        Why this matters:
        ----------------
        - Streaming allows processing during export
        - JSONL standard for data pipelines
        - Memory efficient for huge exports
        - Production ETL uses JSONL

        Additional context:
        ---------------------------------
        - One JSON object per line
        - Can process line-by-line
        - Common for Kafka, log processing
        """
        table_name = f"jsonl_test_{int(datetime.now().timestamp() * 1000)}"

        await session.execute(
            f"""
            CREATE TABLE test_bulk.{table_name} (
                id uuid PRIMARY KEY,
                event text,
                timestamp timestamp
            )
        """
        )

        # Insert multiple events
        num_events = 100
        for i in range(num_events):
            await session.execute(
                f"""
                INSERT INTO test_bulk.{table_name} (id, event, timestamp)
                VALUES (
                    {uuid4()},
                    'event_{i}',
                    '{datetime.now(timezone.utc).isoformat()}'
                )
            """
            )

        try:
            # Export as JSONL
            output_file = tmp_path / "streaming.jsonl"
            exporter = JSONExporter(output_path=str(output_file), options={"mode": "objects"})

            parallel = ParallelExporter(
                session=session, table=f"test_bulk.{table_name}", exporter=exporter
            )

            stats = await parallel.export()

            assert stats.rows_processed == num_events

            # Verify JSONL format
            lines = output_file.read_text().strip().split("\n")
            assert len(lines) == num_events

            # Each line should be valid JSON
            for line in lines:
                obj = json.loads(line)
                assert "id" in obj
                assert "event" in obj
                assert obj["event"].startswith("event_")

        finally:
            await session.execute(f"DROP TABLE test_bulk.{table_name}")

    @pytest.mark.asyncio
    async def test_json_export_pretty_printing(self, session, populated_table, tmp_path):
        """
        Test JSON export with pretty printing enabled.

        What this tests:
        ---------------
        1. Pretty printing adds proper indentation
        2. Human-readable format maintained
        3. File size increases with formatting
        4. Still valid parseable JSON

        Why this matters:
        ----------------
        - Debugging requires readable output
        - Config files need pretty printing
        - Human review of exported data
        - Production debugging scenarios

        Additional context:
        ---------------------------------
        - Indent level 2 spaces standard
        - Increases file size significantly
        - Not for production bulk exports
        """
        # Export with pretty printing
        output_pretty = tmp_path / "pretty.json"
        exporter_pretty = JSONExporter(output_path=str(output_pretty), options={"pretty": True})

        parallel_pretty = ParallelExporter(
            session=session,
            table=f"test_bulk.{populated_table}",
            exporter=exporter_pretty,
            batch_size=100,  # Smaller batch for test
        )

        stats_pretty = await parallel_pretty.export()

        # Export without pretty printing for comparison
        output_compact = tmp_path / "compact.json"
        exporter_compact = JSONExporter(output_path=str(output_compact))

        parallel_compact = ParallelExporter(
            session=session,
            table=f"test_bulk.{populated_table}",
            exporter=exporter_compact,
            batch_size=100,
        )

        stats_compact = await parallel_compact.export()

        # Both should export same number of rows
        assert stats_pretty.rows_processed == stats_compact.rows_processed

        # Pretty printed should be larger
        size_pretty = output_pretty.stat().st_size
        size_compact = output_compact.stat().st_size
        assert size_pretty > size_compact

        # Verify pretty printing
        content_pretty = output_pretty.read_text()
        assert "  " in content_pretty  # Has indentation
        assert content_pretty.count("\n") > 10  # Multiple lines

        # Both should be valid JSON
        with open(output_pretty, "r") as f:
            data_pretty = json.load(f)
        with open(output_compact, "r") as f:
            data_compact = json.load(f)

        assert len(data_pretty) == len(data_compact)


class TestExporterComparison:
    """Compare different export formats with same data."""

    @pytest.mark.asyncio
    async def test_csv_vs_json_data_integrity(self, session, populated_table, tmp_path):
        """
        Test data integrity between CSV and JSON exports.

        What this tests:
        ---------------
        1. Same data exported to both formats
        2. Row counts match exactly
        3. Data values consistent across formats
        4. Type conversions preserve information

        Why this matters:
        ----------------
        - Format choice shouldn't affect data
        - Round-trip integrity critical
        - Cross-format validation
        - Production may use multiple formats

        Additional context:
        ---------------------------------
        - CSV is text-based, JSON preserves types
        - Both must represent same information
        - Critical for data warehouse imports
        """
        # Export to CSV
        csv_file = tmp_path / "data.csv"
        csv_exporter = CSVExporter(output_path=str(csv_file))

        parallel_csv = ParallelExporter(
            session=session, table=f"test_bulk.{populated_table}", exporter=csv_exporter
        )

        stats_csv = await parallel_csv.export()

        # Export to JSON
        json_file = tmp_path / "data.json"
        json_exporter = JSONExporter(output_path=str(json_file))

        parallel_json = ParallelExporter(
            session=session, table=f"test_bulk.{populated_table}", exporter=json_exporter
        )

        stats_json = await parallel_json.export()

        # Same row count
        assert stats_csv.rows_processed == stats_json.rows_processed == 1000

        # Load both formats
        with open(csv_file, "r") as f:
            csv_reader = csv.DictReader(f)
            csv_data = {row["id"]: row for row in csv_reader}

        with open(json_file, "r") as f:
            json_data = {row["id"]: row for row in json.load(f)}

        # Verify same IDs exported
        assert set(csv_data.keys()) == set(json_data.keys())

        # Spot check data consistency
        for id_val in list(csv_data.keys())[:10]:
            csv_row = csv_data[id_val]
            json_row = json_data[id_val]

            # Name should match exactly
            assert csv_row["name"] == json_row["name"]

            # Boolean conversion
            if csv_row["active"] == "true":
                assert json_row["active"] is True
            else:
                assert json_row["active"] is False
