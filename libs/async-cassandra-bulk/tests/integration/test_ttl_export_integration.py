"""
Integration tests for TTL (Time To Live) export functionality.

What this tests:
---------------
1. TTL export with real Cassandra cluster
2. Query generation includes TTL() functions
3. Data exported correctly with TTL values
4. CSV and JSON formats handle TTL properly
5. TTL combined with writetime export

Why this matters:
----------------
- TTL is critical for data expiration tracking
- Must work with real Cassandra queries
- Format-specific handling must be correct
- Production exports need accurate TTL data
"""

import asyncio
import csv
import json
import tempfile
import time
from pathlib import Path

import pytest

from async_cassandra_bulk import BulkOperator


class TestTTLExportIntegration:
    """Test TTL export with real Cassandra."""

    @pytest.fixture
    async def ttl_table(self, session):
        """
        Create test table with TTL data.

        What this tests:
        ---------------
        1. Table creation with various column types
        2. Insert with TTL values
        3. Different TTL per column
        4. Primary keys excluded from TTL

        Why this matters:
        ----------------
        - Real tables have mixed TTL values
        - Must test column-specific TTL
        - Validates Cassandra TTL behavior
        - Production tables have complex schemas
        """
        table_name = f"test_ttl_{int(time.time() * 1000)}"
        full_table_name = f"test_bulk.{table_name}"

        # Create table
        await session.execute(
            f"""
            CREATE TABLE {table_name} (
                id INT PRIMARY KEY,
                name TEXT,
                email TEXT,
                status TEXT,
                created_at TIMESTAMP
            )
            """
        )

        # Insert data with different TTL values
        # Row 1: Different TTL per column
        await session.execute(
            f"""
            INSERT INTO {table_name} (id, name, email, status, created_at)
            VALUES (1, 'Alice', 'alice@example.com', 'active', toTimestamp(now()))
            USING TTL 3600
            """
        )

        # Update specific column with different TTL
        await session.execute(
            f"""
            UPDATE {table_name} USING TTL 7200
            SET email = 'alice.new@example.com'
            WHERE id = 1
            """
        )

        # Row 2: No TTL (permanent data)
        await session.execute(
            f"""
            INSERT INTO {table_name} (id, name, email, status, created_at)
            VALUES (2, 'Bob', 'bob@example.com', 'inactive', toTimestamp(now()))
            """
        )

        # Row 3: Some columns with TTL
        await session.execute(
            f"""
            INSERT INTO {table_name} (id, name, status, created_at)
            VALUES (3, 'Charlie', 'pending', toTimestamp(now()))
            """
        )

        # Set TTL on status only
        await session.execute(
            f"""
            UPDATE {table_name} USING TTL 1800
            SET status = 'temporary'
            WHERE id = 3
            """
        )

        yield full_table_name

        # Cleanup
        await session.execute(f"DROP TABLE {table_name}")

    @pytest.mark.asyncio
    async def test_export_with_ttl_json(self, session, ttl_table):
        """
        Test JSON export includes TTL values.

        What this tests:
        ---------------
        1. TTL columns in JSON output
        2. Correct TTL values exported
        3. NULL handling for no TTL
        4. TTL column naming convention

        Why this matters:
        ----------------
        - JSON is primary export format
        - TTL accuracy is critical
        - Must handle missing TTL
        - Production APIs consume this format
        """
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
            output_path = tmp.name

        try:
            operator = BulkOperator(session=session)

            # Export with TTL for all columns
            stats = await operator.export(
                table=ttl_table,
                output_path=output_path,
                format="json",
                options={
                    "include_ttl": True,  # Should include TTL for all columns
                },
            )

            # Verify export completed
            assert stats.rows_processed == 3

            # Read and verify JSON content
            with open(output_path, "r") as f:
                data = json.load(f)

            assert len(data) == 3

            # Check TTL columns
            for row in data:
                if row["id"] == 1:
                    # Should have TTL for columns (except primary key)
                    assert "name_ttl" in row
                    assert "email_ttl" in row
                    assert "status_ttl" in row
                    assert "created_at_ttl" in row

                    # Should NOT have TTL for primary key
                    assert "id_ttl" not in row

                    # Email should have longer TTL (7200) than others (3600)
                    assert row["email_ttl"] > row["name_ttl"]

                elif row["id"] == 2:
                    # No TTL set - values should be null/missing
                    assert row.get("name_ttl") is None or "name_ttl" not in row
                    assert row.get("email_ttl") is None or "email_ttl" not in row

                elif row["id"] == 3:
                    # Only status has TTL
                    assert row["status_ttl"] > 0
                    assert row["status_ttl"] <= 1800
                    assert row.get("name_ttl") is None or "name_ttl" not in row

        finally:
            Path(output_path).unlink(missing_ok=True)

    @pytest.mark.asyncio
    async def test_export_with_ttl_csv(self, session, ttl_table):
        """
        Test CSV export includes TTL values.

        What this tests:
        ---------------
        1. TTL columns in CSV header
        2. TTL values in CSV data
        3. NULL representation for no TTL
        4. Column ordering with TTL

        Why this matters:
        ----------------
        - CSV needs explicit headers
        - TTL must be clearly labeled
        - NULL handling important
        - Production data pipelines use CSV
        """
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
            output_path = tmp.name

        try:
            operator = BulkOperator(session=session)

            # Export with specific TTL columns
            stats = await operator.export(
                table=ttl_table,
                output_path=output_path,
                format="csv",
                options={
                    "ttl_columns": ["name", "email", "status"],
                },
                csv_options={
                    "null_value": "NULL",
                },
            )

            assert stats.rows_processed == 3

            # Read and verify CSV content
            with open(output_path, "r") as f:
                reader = csv.DictReader(f)
                rows = list(reader)

            assert len(rows) == 3

            # Check headers include TTL columns
            headers = rows[0].keys()
            assert "name_ttl" in headers
            assert "email_ttl" in headers
            assert "status_ttl" in headers

            # Verify TTL values
            for row in rows:
                if row["id"] == "1":
                    assert row["name_ttl"] != "NULL"
                    assert row["email_ttl"] != "NULL"
                    assert int(row["email_ttl"]) > int(row["name_ttl"])

                elif row["id"] == "2":
                    assert row["name_ttl"] == "NULL"
                    assert row["email_ttl"] == "NULL"

        finally:
            Path(output_path).unlink(missing_ok=True)

    @pytest.mark.asyncio
    async def test_ttl_with_writetime_combined(self, session, ttl_table):
        """
        Test exporting both TTL and writetime together.

        What this tests:
        ---------------
        1. Combined TTL and writetime export
        2. Column naming doesn't conflict
        3. Both values exported correctly
        4. Performance with double metadata

        Why this matters:
        ----------------
        - Common use case for full metadata
        - Must handle query complexity
        - Data migration scenarios
        - Production debugging needs both
        """
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
            output_path = tmp.name

        try:
            operator = BulkOperator(session=session)

            # Export with both writetime and TTL
            await operator.export(
                table=ttl_table,
                output_path=output_path,
                format="json",
                options={
                    "include_writetime": True,
                    "include_ttl": True,
                },
            )

            with open(output_path, "r") as f:
                data = json.load(f)

            # Verify both TTL and writetime columns present
            for row in data:
                if row["id"] == 1:
                    # Should have both writetime and TTL
                    assert "name_writetime" in row
                    assert "name_ttl" in row
                    assert "email_writetime" in row
                    assert "email_ttl" in row

                    # Values should be reasonable
                    # Writetime is serialized as ISO datetime string
                    assert isinstance(row["name_writetime"], str)
                    assert row["name_writetime"].startswith("20")  # Year 20xx

                    # TTL is numeric seconds
                    assert isinstance(row["name_ttl"], int)
                    assert row["name_ttl"] > 0
                    assert row["name_ttl"] <= 3600

        finally:
            Path(output_path).unlink(missing_ok=True)

    @pytest.mark.asyncio
    async def test_ttl_decreasing_over_time(self, session):
        """
        Test that TTL values decrease over time.

        What this tests:
        ---------------
        1. TTL countdown behavior
        2. TTL accuracy over time
        3. Near-expiration handling
        4. Real-time TTL tracking

        Why this matters:
        ----------------
        - TTL is time-sensitive
        - Export timing affects values
        - Migration planning needs accuracy
        - Production monitoring use case
        """
        table_name = f"test_ttl_decrease_{int(time.time() * 1000)}"
        full_table_name = f"test_bulk.{table_name}"

        output1 = None
        output2 = None

        try:
            # Create table and insert with short TTL
            await session.execute(
                f"""
                CREATE TABLE {table_name} (
                    id INT PRIMARY KEY,
                    data TEXT
                )
                """
            )

            # Insert with 10 second TTL
            await session.execute(
                f"""
                INSERT INTO {table_name} (id, data)
                VALUES (1, 'expires soon')
                USING TTL 10
                """
            )

            operator = BulkOperator(session=session)

            # Export immediately
            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
                output1 = tmp.name

            await operator.export(
                table=full_table_name,
                output_path=output1,
                format="json",
                options={"include_ttl": True},
            )

            # Wait 2 seconds
            await asyncio.sleep(2)

            # Export again
            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
                output2 = tmp.name

            await operator.export(
                table=full_table_name,
                output_path=output2,
                format="json",
                options={"include_ttl": True},
            )

            # Compare TTL values
            with open(output1, "r") as f:
                data1 = json.load(f)[0]

            with open(output2, "r") as f:
                data2 = json.load(f)[0]

            # TTL should have decreased
            ttl1 = data1["data_ttl"]
            ttl2 = data2["data_ttl"]

            assert ttl1 > ttl2
            assert ttl1 - ttl2 >= 1  # At least 1 second difference
            assert ttl1 <= 10
            assert ttl2 <= 8

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {table_name}")
            if output1:
                Path(output1).unlink(missing_ok=True)
            if output2:
                Path(output2).unlink(missing_ok=True)

    @pytest.mark.asyncio
    async def test_ttl_with_collections(self, session):
        """
        Test TTL export with collection types.

        What this tests:
        ---------------
        1. TTL on collection columns
        2. Collection element TTL
        3. TTL serialization for complex types
        4. Edge cases with collections

        Why this matters:
        ----------------
        - Collections have special TTL semantics
        - Element-level TTL complexity
        - Production schemas use collections
        - Export accuracy for complex types
        """
        table_name = f"test_ttl_collections_{int(time.time() * 1000)}"
        full_table_name = f"test_bulk.{table_name}"

        try:
            await session.execute(
                f"""
                CREATE TABLE {table_name} (
                    id INT PRIMARY KEY,
                    tags SET<TEXT>,
                    scores LIST<INT>,
                    metadata MAP<TEXT, TEXT>
                )
                """
            )

            # Insert with TTL on collections
            await session.execute(
                f"""
                INSERT INTO {table_name} (id, tags, scores, metadata)
                VALUES (
                    1,
                    {{'tag1', 'tag2', 'tag3'}},
                    [100, 200, 300],
                    {{'key1': 'value1', 'key2': 'value2'}}
                )
                USING TTL 3600
                """
            )

            # Update individual collection elements with different TTL
            await session.execute(
                f"""
                UPDATE {table_name} USING TTL 7200
                SET tags = tags + {{'tag4'}}
                WHERE id = 1
                """
            )

            operator = BulkOperator(session=session)

            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
                output_path = tmp.name

            await operator.export(
                table=full_table_name,
                output_path=output_path,
                format="json",
                options={"include_ttl": True},
            )

            with open(output_path, "r") as f:
                data = json.load(f)[0]

            # Collections should have TTL
            assert "tags_ttl" in data
            assert "scores_ttl" in data
            assert "metadata_ttl" in data

            # TTL values should be reasonable
            # Collections return list of TTL values (one per element)
            assert isinstance(data["tags_ttl"], list)
            assert isinstance(data["scores_ttl"], list)
            assert isinstance(data["metadata_ttl"], list)

            # All elements should have TTL > 0
            assert all(ttl > 0 for ttl in data["tags_ttl"] if ttl is not None)
            assert all(ttl > 0 for ttl in data["scores_ttl"] if ttl is not None)
            assert all(ttl > 0 for ttl in data["metadata_ttl"] if ttl is not None)

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {table_name}")
            Path(output_path).unlink(missing_ok=True)

    @pytest.mark.asyncio
    async def test_ttl_null_handling(self, session):
        """
        Test TTL behavior with NULL values.

        What this tests:
        ---------------
        1. NULL values have no TTL
        2. TTL export handles NULL correctly
        3. Mixed NULL/non-NULL in same row
        4. TTL updates on NULL columns

        Why this matters:
        ----------------
        - NULL handling is critical
        - TTL only applies to actual values
        - Common edge case in production
        - Data integrity validation
        """
        table_name = f"test_ttl_null_{int(time.time() * 1000)}"
        full_table_name = f"test_bulk.{table_name}"

        try:
            await session.execute(
                f"""
                CREATE TABLE {table_name} (
                    id INT PRIMARY KEY,
                    col_a TEXT,
                    col_b TEXT,
                    col_c TEXT
                )
                """
            )

            # Insert with some NULL values
            await session.execute(
                f"""
                INSERT INTO {table_name} (id, col_a, col_b, col_c)
                VALUES (1, 'value_a', NULL, 'value_c')
                USING TTL 3600
                """
            )

            # Insert with no TTL and NULL
            await session.execute(
                f"""
                INSERT INTO {table_name} (id, col_a, col_b)
                VALUES (2, 'value_a2', NULL)
                """
            )

            operator = BulkOperator(session=session)

            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
                output_path = tmp.name

            await operator.export(
                table=full_table_name,
                output_path=output_path,
                format="json",
                options={"include_ttl": True},
            )

            with open(output_path, "r") as f:
                data = json.load(f)

            # Row 1: NULL column should have None TTL
            row1 = next(r for r in data if r["id"] == 1)
            assert "col_a_ttl" in row1
            assert row1["col_a_ttl"] > 0
            assert "col_b_ttl" in row1
            assert row1["col_b_ttl"] is None  # NULL value has None TTL
            assert "col_c_ttl" in row1
            assert row1["col_c_ttl"] > 0

            # Row 2: No TTL set - values should be None
            row2 = next(r for r in data if r["id"] == 2)
            assert row2.get("col_a_ttl") is None
            assert row2.get("col_b_ttl") is None  # NULL value

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {table_name}")
            Path(output_path).unlink(missing_ok=True)
