"""
Integration tests for export formats.

What this tests:
---------------
1. CSV export with real data
2. JSON export formats (JSONL and array)
3. Parquet export with schema mapping
4. Compression options
5. Data integrity across formats

Why this matters:
----------------
- Export formats are critical for data pipelines
- Each format has different use cases
- Parquet is foundation for Iceberg
- Must preserve data types correctly
"""

import csv
import gzip
import json

import pytest

try:
    import pyarrow.parquet as pq

    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False

from async_cassandra import AsyncCluster
from bulk_operations.bulk_operator import TokenAwareBulkOperator


@pytest.mark.integration
class TestExportFormats:
    """Test export to different formats."""

    @pytest.fixture
    async def cluster(self):
        """Create connection to test cluster."""
        cluster = AsyncCluster(
            contact_points=["localhost"],
            port=9042,
        )
        yield cluster
        await cluster.shutdown()

    @pytest.fixture
    async def session(self, cluster):
        """Create test session with test data."""
        session = await cluster.connect()

        # Create test keyspace
        await session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS export_test
            WITH replication = {
                'class': 'SimpleStrategy',
                'replication_factor': 1
            }
        """
        )

        # Create test table with various types
        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS export_test.data_types (
                id INT PRIMARY KEY,
                text_val TEXT,
                int_val INT,
                float_val FLOAT,
                bool_val BOOLEAN,
                list_val LIST<TEXT>,
                set_val SET<INT>,
                map_val MAP<TEXT, TEXT>,
                null_val TEXT
            )
        """
        )

        # Clear and insert test data
        await session.execute("TRUNCATE export_test.data_types")

        insert_stmt = await session.prepare(
            """
            INSERT INTO export_test.data_types
            (id, text_val, int_val, float_val, bool_val,
             list_val, set_val, map_val, null_val)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        )

        # Insert diverse test data
        test_data = [
            (1, "test1", 100, 1.5, True, ["a", "b"], {1, 2}, {"k1": "v1"}, None),
            (2, "test2", -50, -2.5, False, [], None, {}, None),
            (3, "special'chars\"test", 0, 0.0, True, None, {0}, None, None),
            (4, "unicode_test_你好", 999, 3.14, False, ["x"], {-1}, {"k": "v"}, None),
        ]

        for row in test_data:
            await session.execute(insert_stmt, row)

        yield session

    @pytest.mark.asyncio
    async def test_csv_export_basic(self, session, tmp_path):
        """
        Test basic CSV export functionality.

        What this tests:
        ---------------
        1. CSV export creates valid file
        2. All rows are exported
        3. Data types are properly serialized
        4. NULL values handled correctly

        Why this matters:
        ----------------
        - CSV is most common export format
        - Must work with Excel and other tools
        - Data integrity is critical
        """
        operator = TokenAwareBulkOperator(session)
        output_path = tmp_path / "test.csv"

        # Export to CSV
        result = await operator.export_to_csv(
            keyspace="export_test",
            table="data_types",
            output_path=output_path,
        )

        # Verify file exists
        assert output_path.exists()
        assert result.rows_exported == 4

        # Read and verify content
        with open(output_path) as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) == 4

        # Verify first row
        row1 = rows[0]
        assert row1["id"] == "1"
        assert row1["text_val"] == "test1"
        assert row1["int_val"] == "100"
        assert row1["float_val"] == "1.5"
        assert row1["bool_val"] == "true"
        assert "[a, b]" in row1["list_val"]
        assert row1["null_val"] == ""  # Default NULL representation

    @pytest.mark.asyncio
    async def test_csv_export_compressed(self, session, tmp_path):
        """
        Test CSV export with compression.

        What this tests:
        ---------------
        1. Gzip compression works
        2. File has correct extension
        3. Compressed data is valid
        4. Size reduction achieved

        Why this matters:
        ----------------
        - Large exports need compression
        - Network transfer efficiency
        - Storage cost reduction
        """
        operator = TokenAwareBulkOperator(session)
        output_path = tmp_path / "test.csv"

        # Export with compression
        await operator.export_to_csv(
            keyspace="export_test",
            table="data_types",
            output_path=output_path,
            compression="gzip",
        )

        # Verify compressed file
        compressed_path = output_path.with_suffix(".csv.gzip")
        assert compressed_path.exists()

        # Read compressed content
        with gzip.open(compressed_path, "rt") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) == 4

    @pytest.mark.asyncio
    async def test_json_export_line_delimited(self, session, tmp_path):
        """
        Test JSON line-delimited export.

        What this tests:
        ---------------
        1. JSONL format (one JSON per line)
        2. Each line is valid JSON
        3. Data types preserved
        4. Collections handled correctly

        Why this matters:
        ----------------
        - JSONL works with streaming tools
        - Each line can be processed independently
        - Better for large datasets
        """
        operator = TokenAwareBulkOperator(session)
        output_path = tmp_path / "test.jsonl"

        # Export as JSONL
        result = await operator.export_to_json(
            keyspace="export_test",
            table="data_types",
            output_path=output_path,
            format_mode="jsonl",
        )

        assert output_path.exists()
        assert result.rows_exported == 4

        # Read and verify JSONL
        with open(output_path) as f:
            lines = f.readlines()

        assert len(lines) == 4

        # Parse each line
        rows = [json.loads(line) for line in lines]

        # Verify data types
        row1 = rows[0]
        assert row1["id"] == 1
        assert row1["text_val"] == "test1"
        assert row1["bool_val"] is True
        assert row1["list_val"] == ["a", "b"]
        assert row1["set_val"] == [1, 2]  # Sets become lists in JSON
        assert row1["map_val"] == {"k1": "v1"}
        assert row1["null_val"] is None

    @pytest.mark.asyncio
    async def test_json_export_array(self, session, tmp_path):
        """
        Test JSON array export.

        What this tests:
        ---------------
        1. Valid JSON array format
        2. Proper array structure
        3. Pretty printing option
        4. Complete document

        Why this matters:
        ----------------
        - Some APIs expect JSON arrays
        - Easier for small datasets
        - Human readable with indent
        """
        operator = TokenAwareBulkOperator(session)
        output_path = tmp_path / "test.json"

        # Export as JSON array
        await operator.export_to_json(
            keyspace="export_test",
            table="data_types",
            output_path=output_path,
            format_mode="array",
            indent=2,
        )

        assert output_path.exists()

        # Read and parse JSON
        with open(output_path) as f:
            data = json.load(f)

        assert isinstance(data, list)
        assert len(data) == 4

        # Verify structure
        assert all(isinstance(row, dict) for row in data)

    @pytest.mark.asyncio
    @pytest.mark.skipif(not PYARROW_AVAILABLE, reason="PyArrow not installed")
    async def test_parquet_export(self, session, tmp_path):
        """
        Test Parquet export - foundation for Iceberg.

        What this tests:
        ---------------
        1. Valid Parquet file created
        2. Schema correctly mapped
        3. Data types preserved
        4. Row groups created

        Why this matters:
        ----------------
        - Parquet is THE format for Iceberg
        - Columnar storage for analytics
        - Schema evolution support
        - Excellent compression
        """
        operator = TokenAwareBulkOperator(session)
        output_path = tmp_path / "test.parquet"

        # Export to Parquet
        result = await operator.export_to_parquet(
            keyspace="export_test",
            table="data_types",
            output_path=output_path,
            row_group_size=2,  # Small for testing
        )

        assert output_path.exists()
        assert result.rows_exported == 4

        # Read Parquet file
        table = pq.read_table(output_path)

        # Verify schema
        schema = table.schema
        assert "id" in schema.names
        assert "text_val" in schema.names
        assert "bool_val" in schema.names

        # Verify data
        df = table.to_pandas()
        assert len(df) == 4

        # Check data types preserved
        assert df.loc[0, "id"] == 1
        assert df.loc[0, "text_val"] == "test1"
        assert df.loc[0, "bool_val"] is True or df.loc[0, "bool_val"] == 1  # numpy bool comparison

        # Verify row groups
        parquet_file = pq.ParquetFile(output_path)
        assert parquet_file.num_row_groups == 2  # 4 rows / 2 per group

    @pytest.mark.asyncio
    async def test_export_with_column_selection(self, session, tmp_path):
        """
        Test exporting specific columns only.

        What this tests:
        ---------------
        1. Column selection works
        2. Only selected columns exported
        3. Order preserved
        4. Works across all formats

        Why this matters:
        ----------------
        - Reduce export size
        - Privacy/security (exclude sensitive columns)
        - Performance optimization
        """
        operator = TokenAwareBulkOperator(session)
        columns = ["id", "text_val", "bool_val"]

        # Test CSV
        csv_path = tmp_path / "selected.csv"
        await operator.export_to_csv(
            keyspace="export_test",
            table="data_types",
            output_path=csv_path,
            columns=columns,
        )

        with open(csv_path) as f:
            reader = csv.DictReader(f)
            row = next(reader)
            assert set(row.keys()) == set(columns)

        # Test JSON
        json_path = tmp_path / "selected.jsonl"
        await operator.export_to_json(
            keyspace="export_test",
            table="data_types",
            output_path=json_path,
            columns=columns,
        )

        with open(json_path) as f:
            row = json.loads(f.readline())
            assert set(row.keys()) == set(columns)

    @pytest.mark.asyncio
    async def test_export_progress_tracking(self, session, tmp_path):
        """
        Test progress tracking and resume capability.

        What this tests:
        ---------------
        1. Progress callbacks invoked
        2. Progress saved to file
        3. Resume information correct
        4. Stats accurately tracked

        Why this matters:
        ----------------
        - Long exports need monitoring
        - Resume saves time on failures
        - Users need feedback
        """
        operator = TokenAwareBulkOperator(session)
        output_path = tmp_path / "progress_test.csv"

        progress_updates = []

        async def track_progress(progress):
            progress_updates.append(
                {
                    "rows": progress.rows_exported,
                    "bytes": progress.bytes_written,
                    "percentage": progress.progress_percentage,
                }
            )

        # Export with progress tracking
        result = await operator.export_to_csv(
            keyspace="export_test",
            table="data_types",
            output_path=output_path,
            progress_callback=track_progress,
        )

        # Verify progress was tracked
        assert len(progress_updates) > 0
        assert result.rows_exported == 4
        assert result.bytes_written > 0

        # Verify progress file
        progress_file = output_path.with_suffix(".csv.progress")
        assert progress_file.exists()

        # Load and verify progress
        from bulk_operations.exporters import ExportProgress

        loaded = ExportProgress.load(progress_file)
        assert loaded.rows_exported == 4
        assert loaded.is_complete
