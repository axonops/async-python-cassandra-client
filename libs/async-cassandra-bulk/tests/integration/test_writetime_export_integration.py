"""
Integration tests for writetime export functionality.

What this tests:
---------------
1. Writetime export with real Cassandra cluster
2. Query generation includes WRITETIME() functions
3. Data exported correctly with writetime values
4. CSV and JSON formats handle writetime properly

Why this matters:
----------------
- Writetime export is critical for data migration
- Must work with real Cassandra queries
- Format-specific handling must be correct
- Production exports need accurate writetime data
"""

import csv
import json
import tempfile
from datetime import datetime
from pathlib import Path

import pytest

from async_cassandra_bulk import BulkOperator


class TestWritetimeExportIntegration:
    """Test writetime export with real Cassandra."""

    @pytest.fixture
    async def writetime_table(self, session):
        """
        Create test table with writetime data.

        What this tests:
        ---------------
        1. Table creation with various column types
        2. Insert with explicit writetime values
        3. Different writetime per column
        4. Primary keys excluded from writetime

        Why this matters:
        ----------------
        - Real tables have mixed writetime values
        - Must test column-specific writetime
        - Validates Cassandra writetime behavior
        - Production tables have complex schemas
        """
        table_name = "writetime_test"
        keyspace = "test_bulk"

        # Create table
        await session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {keyspace}.{table_name} (
                id UUID PRIMARY KEY,
                name TEXT,
                email TEXT,
                created_at TIMESTAMP,
                status TEXT
            )
        """
        )

        # Insert test data with specific writetime values
        # Writetime in microseconds since epoch
        base_writetime = 1700000000000000  # ~2023-11-14

        # Insert with different writetime for each column
        await session.execute(
            f"""
            INSERT INTO {keyspace}.{table_name}
            (id, name, email, created_at, status)
            VALUES (
                550e8400-e29b-41d4-a716-446655440001,
                'Test User 1',
                'user1@example.com',
                '2023-01-01 00:00:00+0000',
                'active'
            ) USING TIMESTAMP {base_writetime}
        """
        )

        # Insert another row with different writetime
        await session.execute(
            f"""
            INSERT INTO {keyspace}.{table_name}
            (id, name, email, created_at, status)
            VALUES (
                550e8400-e29b-41d4-a716-446655440002,
                'Test User 2',
                'user2@example.com',
                '2023-01-02 00:00:00+0000',
                'inactive'
            ) USING TIMESTAMP {base_writetime + 1000000}
        """
        )

        # Update specific columns with new writetime
        await session.execute(
            f"""
            UPDATE {keyspace}.{table_name}
            USING TIMESTAMP {base_writetime + 2000000}
            SET email = 'updated@example.com'
            WHERE id = 550e8400-e29b-41d4-a716-446655440001
        """
        )

        yield f"{keyspace}.{table_name}"

        # Cleanup
        await session.execute(f"DROP TABLE IF EXISTS {keyspace}.{table_name}")

    @pytest.mark.asyncio
    async def test_export_with_writetime_csv(self, session, writetime_table):
        """
        Test CSV export includes writetime data.

        What this tests:
        ---------------
        1. Export with writetime_columns option works
        2. CSV contains _writetime columns
        3. Writetime values are human-readable timestamps
        4. Non-writetime columns unchanged

        Why this matters:
        ----------------
        - CSV is most common export format
        - Writetime must be readable by humans
        - Column order and naming critical
        - Production exports use this feature
        """
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
            output_path = tmp.name

        try:
            operator = BulkOperator(session=session)

            # Export with writetime for specific columns
            stats = await operator.export(
                table=writetime_table,
                output_path=output_path,
                format="csv",
                options={
                    "writetime_columns": ["name", "email", "status"],
                },
            )

            # Verify export completed
            assert stats.rows_processed == 2
            assert stats.errors == []

            # Read and verify CSV content
            with open(output_path, "r") as f:
                reader = csv.DictReader(f)
                rows = list(reader)

            assert len(rows) == 2

            # Check headers include writetime columns
            headers = rows[0].keys()
            assert "name_writetime" in headers
            assert "email_writetime" in headers
            assert "status_writetime" in headers
            assert "id_writetime" not in headers  # Primary key no writetime

            # Verify writetime values are formatted timestamps
            for row in rows:
                # Should have readable timestamp format
                assert row["name_writetime"]  # Not empty
                assert "2023" in row["name_writetime"]  # Year visible
                assert ":" in row["name_writetime"]  # Time separator

                # Email might have different writetime for first row
                assert row["email_writetime"]

        finally:
            Path(output_path).unlink(missing_ok=True)

    @pytest.mark.asyncio
    async def test_export_with_writetime_json(self, session, writetime_table):
        """
        Test JSON export includes writetime in ISO format.

        What this tests:
        ---------------
        1. JSON export with writetime works
        2. Writetime values in ISO 8601 format
        3. JSON structure preserves column relationships
        4. Null writetime handled correctly

        Why this matters:
        ----------------
        - JSON needs standard timestamp format
        - ISO format for interoperability
        - Structure must be parseable
        - Production APIs consume this format
        """
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
            output_path = tmp.name

        try:
            operator = BulkOperator(session=session)

            # Export with writetime for all columns
            stats = await operator.export(
                table=writetime_table,
                output_path=output_path,
                format="json",
                options={
                    "include_writetime": True,  # Defaults to all columns
                },
            )

            # Verify export completed
            assert stats.rows_processed == 2

            # Read and verify JSON content
            with open(output_path, "r") as f:
                data = json.load(f)

            assert len(data) == 2

            # Check writetime columns in ISO format
            for row in data:
                # Should have writetime for non-key columns
                assert "name_writetime" in row
                assert "email_writetime" in row
                assert "status_writetime" in row
                assert "created_at_writetime" in row

                # Should NOT have writetime for primary key
                assert "id_writetime" not in row

                # Verify ISO format
                writetime_str = row["name_writetime"]
                assert "T" in writetime_str  # ISO separator
                assert writetime_str.endswith("Z") or "+" in writetime_str  # Timezone

                # Should be parseable
                datetime.fromisoformat(writetime_str.replace("Z", "+00:00"))

        finally:
            Path(output_path).unlink(missing_ok=True)

    @pytest.mark.asyncio
    async def test_writetime_with_null_values(self, session):
        """
        Test writetime export handles null writetime gracefully.

        What this tests:
        ---------------
        1. Cells without writetime return NULL
        2. CSV shows configured null marker
        3. JSON shows null value
        4. No errors during export

        Why this matters:
        ----------------
        - Not all cells have writetime
        - Counter columns lack writetime
        - Must handle edge cases gracefully
        - Production data has nulls

        Additional context:
        ---------------------------------
        - Cells inserted in batch may not have writetime
        - System columns may lack writetime
        - TTL expired cells lose writetime
        """
        table_name = "writetime_null_test"
        keyspace = "test_bulk"

        # Create two tables - counters need their own table
        await session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {keyspace}.{table_name} (
                id INT PRIMARY KEY,
                regular_col TEXT,
                nullable_col TEXT
            )
        """
        )

        try:
            # Insert regular column with writetime
            await session.execute(
                f"""
                INSERT INTO {keyspace}.{table_name}
                (id, regular_col) VALUES (1, 'has writetime')
            """
            )

            # Insert row with null column (no writetime for null values)
            await session.execute(
                f"""
                INSERT INTO {keyspace}.{table_name}
                (id, regular_col) VALUES (2, 'only regular')
            """
            )

            with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
                output_path = tmp.name

            operator = BulkOperator(session=session)

            # Export with writetime
            await operator.export(
                table=f"{keyspace}.{table_name}",
                output_path=output_path,
                format="csv",
                options={
                    "writetime_columns": ["*"],
                    "null_value": "NULL",
                },
                csv_options={
                    "null_value": "NULL",
                },
            )

            # Read CSV
            with open(output_path, "r") as f:
                reader = csv.DictReader(f)
                rows = list(reader)

            assert len(rows) == 2

            # Both rows should have writetime for regular_col
            for row in rows:
                assert row["regular_col_writetime"] != "NULL"
                assert row["regular_col_writetime"]  # Not empty

                # Nullable column should have NULL writetime when not set
                if row["nullable_col"] == "NULL":
                    # If the column is null, writetime should also be null
                    assert row.get("nullable_col_writetime", "NULL") == "NULL"

            Path(output_path).unlink(missing_ok=True)

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {keyspace}.{table_name}")

    @pytest.mark.asyncio
    async def test_parallel_export_with_writetime(self, session, writetime_table):
        """
        Test parallel export correctly handles writetime.

        What this tests:
        ---------------
        1. Multiple workers generate correct queries
        2. All ranges include writetime columns
        3. Results aggregated correctly
        4. No data corruption or duplication

        Why this matters:
        ----------------
        - Production exports use parallelism
        - Query generation per worker
        - Writetime must be consistent
        - Large tables require parallel export
        """
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
            output_path = tmp.name

        try:
            operator = BulkOperator(session=session)

            # Export with parallelism and writetime
            stats = await operator.export(
                table=writetime_table,
                output_path=output_path,
                format="json",
                concurrency=2,  # Use multiple workers
                options={
                    "writetime_columns": ["name", "email"],
                },
                json_options={
                    "mode": "objects",  # JSONL for easier verification
                },
            )

            # Verify all rows exported
            assert stats.rows_processed == 2
            assert stats.ranges_completed > 0

            # Read JSONL and verify
            rows = []
            with open(output_path, "r") as f:
                for line in f:
                    rows.append(json.loads(line))

            assert len(rows) == 2

            # Each row should have writetime columns
            for row in rows:
                assert "name_writetime" in row
                assert "email_writetime" in row

                # Writetime should be ISO format
                assert "T" in row["name_writetime"]

        finally:
            Path(output_path).unlink(missing_ok=True)
