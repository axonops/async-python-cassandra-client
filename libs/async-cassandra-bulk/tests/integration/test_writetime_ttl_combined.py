"""
Integration tests combining writetime filtering and TTL export.

What this tests:
---------------
1. Writetime filtering with TTL export
2. Complex queries with both WRITETIME() and TTL()
3. Filtering based on writetime while exporting TTL
4. Performance with combined metadata export
5. Edge cases with both features active

Why this matters:
----------------
- Common use case for data migration
- Query complexity validation
- Performance impact assessment
- Production scenario testing
"""

import asyncio
import json
import tempfile
import time
from pathlib import Path

import pytest

from async_cassandra_bulk import BulkOperator


class TestWritetimeTTLCombined:
    """Test combined writetime filtering and TTL export."""

    @pytest.fixture
    async def combined_table(self, session):
        """
        Create test table with varied writetime and TTL data.

        What this tests:
        ---------------
        1. Table with multiple data patterns
        2. Different writetime values
        3. Different TTL values
        4. Complex filtering scenarios

        Why this matters:
        ----------------
        - Real tables have mixed data
        - Migration requires filtering
        - TTL preservation is critical
        - Production complexity
        """
        table_name = f"test_combined_{int(time.time() * 1000)}"
        full_table_name = f"test_bulk.{table_name}"

        # Create table
        await session.execute(
            f"""
            CREATE TABLE {table_name} (
                id INT PRIMARY KEY,
                name TEXT,
                email TEXT,
                status TEXT,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )
            """
        )

        # Get current time for calculations
        now_micros = int(time.time() * 1_000_000)
        now_micros - (3600 * 1_000_000)
        now_micros - (7200 * 1_000_000)
        now_micros - (86400 * 1_000_000)

        # Insert old data with short TTL (use prepared statements for consistency)
        insert_stmt = await session.prepare(
            f"""
            INSERT INTO {table_name} (id, name, email, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, toTimestamp(now()), toTimestamp(now()))
            USING TTL ?
            """
        )

        await session.execute(insert_stmt, (1, "Old User", "old@example.com", "active", 3600))

        # Wait to get different writetime
        await asyncio.sleep(0.1)

        # Insert recent data with long TTL
        await session.execute(insert_stmt, (2, "New User", "new@example.com", "active", 86400))

        # Insert data with no TTL but recent writetime
        insert_no_ttl = await session.prepare(
            f"""
            INSERT INTO {table_name} (id, name, email, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, toTimestamp(now()), toTimestamp(now()))
            """
        )
        await session.execute(
            insert_no_ttl, (3, "Permanent User", "permanent@example.com", "active")
        )

        # Don't update for now to keep test simple

        # Store writetime boundaries for tests
        await asyncio.sleep(0.5)  # Increased delay
        boundary_time = int(time.time() * 1_000_000)

        # Insert very recent data
        await asyncio.sleep(0.1)  # Ensure it's after boundary
        await session.execute(insert_stmt, (4, "Latest User", "latest@example.com", "active", 1800))

        yield full_table_name, boundary_time

        # Cleanup
        await session.execute(f"DROP TABLE {table_name}")

    @pytest.mark.asyncio
    async def test_export_recent_with_ttl(self, session, combined_table):
        """
        Test exporting only recent data with TTL values.

        What this tests:
        ---------------
        1. Writetime filtering (after threshold)
        2. TTL values for filtered rows
        3. Older rows excluded
        4. TTL accuracy for exported data

        Why this matters:
        ----------------
        - Common migration pattern
        - Fresh data identification
        - TTL preservation for recent data
        - Production use case
        """
        table_name, boundary_time = combined_table

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
            output_path = tmp.name

        try:
            operator = BulkOperator(session=session)

            # First check actual writetime values
            table_short = table_name.split(".")[1]
            result = await session.execute(
                f"SELECT id, name, WRITETIME(name), WRITETIME(email), WRITETIME(created_at) FROM {table_short}"
            )
            rows = list(result)
            print("DEBUG: Writetime values in table:")
            print(f"Boundary time: {boundary_time}")
            for row in rows:
                print(f"  ID {row.id}: name_wt={row[2]}, email_wt={row[3]}, created_wt={row[4]}")

            # First export without filtering to see writetime columns
            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as debug_tmp:
                debug_path = debug_tmp.name

            await operator.export(
                table=table_name,
                output_path=debug_path,
                format="json",
                options={
                    "include_writetime": True,
                    "include_ttl": True,
                },
            )

            with open(debug_path, "r") as f:
                debug_data = json.load(f)

            print("\nDEBUG: Writetime columns in export:")
            if debug_data:
                row4 = next((r for r in debug_data if r["id"] == 4), None)
                if row4:
                    for k, v in row4.items():
                        if k.endswith("_writetime"):
                            print(f"  {k}: {v}")

            Path(debug_path).unlink(missing_ok=True)

            # Export only data written after boundary time - test with specific columns
            stats = await operator.export(
                table=table_name,
                output_path=output_path,
                format="json",
                options={
                    "writetime_columns": ["name", "email"],  # Specific columns
                    "include_ttl": True,
                    "writetime_after": boundary_time,
                },
            )

            with open(output_path, "r") as f:
                data = json.load(f)

            # Debug info
            print(f"Boundary time: {boundary_time}")
            print(f"Exported {len(data)} rows")
            print(f"Stats: {stats.rows_processed} rows processed")

            # Should only have row 4 (Latest User)
            assert len(data) == 1
            assert data[0]["id"] == 4
            assert data[0]["name"] == "Latest User"

            # Should have both writetime and TTL
            assert "name_writetime" in data[0]
            assert "name_ttl" in data[0]
            assert isinstance(data[0]["name_writetime"], str)  # ISO format
            assert data[0]["name_ttl"] > 0
            assert data[0]["name_ttl"] <= 1800

        finally:
            Path(output_path).unlink(missing_ok=True)

    @pytest.mark.asyncio
    async def test_export_old_with_ttl(self, session, combined_table):
        """
        Test exporting only old data with TTL values.

        What this tests:
        ---------------
        1. Writetime filtering (before threshold)
        2. TTL values for old data
        3. Recent rows excluded
        4. Short TTL detection

        Why this matters:
        ----------------
        - Archive old data before expiry
        - Identify expiring data
        - Historical data export
        - Cleanup planning
        """
        table_name, boundary_time = combined_table

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
            output_path = tmp.name

        try:
            operator = BulkOperator(session=session)

            # Export only data written before boundary time
            await operator.export(
                table=table_name,
                output_path=output_path,
                format="json",
                options={
                    "include_writetime": True,
                    "include_ttl": True,
                    "writetime_before": boundary_time,
                },
            )

            with open(output_path, "r") as f:
                data = json.load(f)

            # Should have rows 1, 2, and 3
            assert len(data) == 3
            ids = [row["id"] for row in data]
            assert sorted(ids) == [1, 2, 3]

            # Check TTL values
            for row in data:
                if row["id"] == 1:
                    # Short TTL
                    assert row.get("name_ttl", 0) > 0
                    assert row.get("name_ttl", 0) <= 3600
                elif row["id"] == 2:
                    # Long TTL (1 day = 86400 seconds)
                    assert row.get("name_ttl", 0) > 0
                    assert row.get("name_ttl", 0) <= 86400
                    assert row.get("status_ttl", 0) > 0
                    assert row.get("status_ttl", 0) <= 86400
                elif row["id"] == 3:
                    # No TTL
                    assert row.get("name_ttl") is None

        finally:
            Path(output_path).unlink(missing_ok=True)

    @pytest.mark.asyncio
    async def test_export_range_with_ttl(self, session, combined_table):
        """
        Test exporting data in writetime range with TTL.

        What this tests:
        ---------------
        1. Writetime range filtering
        2. TTL for range-filtered data
        3. Boundary condition handling
        4. Complex filter combinations

        Why this matters:
        ----------------
        - Time window exports
        - Incremental migrations
        - Batch processing
        - Audit trail exports
        """
        table_name, boundary_time = combined_table

        # Calculate range: from row 2 to just before row 4
        # This should capture rows 2 and 3 but not 1 or 4
        start_time = boundary_time - 600_000  # 600ms before (should include row 2 and 3)
        end_time = boundary_time + 50_000  # 50ms after (should exclude row 4)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
            output_path = tmp.name

        try:
            operator = BulkOperator(session=session)

            # Export data in time range
            await operator.export(
                table=table_name,
                output_path=output_path,
                format="json",
                options={
                    "include_writetime": True,
                    "include_ttl": True,
                    "writetime_after": start_time,
                    "writetime_before": end_time,
                },
            )

            with open(output_path, "r") as f:
                data = json.load(f)

            # Should have some but not all rows
            assert len(data) > 0
            assert len(data) < 4  # Not all rows

            # All exported rows should have TTL data
            for row in data:
                assert "name_writetime" in row
                assert "name_ttl" in row

        finally:
            Path(output_path).unlink(missing_ok=True)

    @pytest.mark.asyncio
    async def test_specific_columns_writetime_ttl(self, session, combined_table):
        """
        Test specific column selection with writetime and TTL.

        What this tests:
        ---------------
        1. Specific writetime columns
        2. Specific TTL columns
        3. Different column sets
        4. Metadata precision

        Why this matters:
        ----------------
        - Selective metadata export
        - Performance optimization
        - Storage efficiency
        - Targeted analysis
        """
        table_name, boundary_time = combined_table

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
            output_path = tmp.name

        try:
            operator = BulkOperator(session=session)

            # Export with specific columns for writetime and TTL
            await operator.export(
                table=table_name,
                output_path=output_path,
                format="json",
                columns=["id", "name", "email", "status"],
                options={
                    "writetime_columns": ["name", "email"],
                    "ttl_columns": ["status"],
                },
            )

            with open(output_path, "r") as f:
                data = json.load(f)

            assert len(data) == 4

            for row in data:
                # Should have writetime for name and email
                assert "name_writetime" in row
                assert "email_writetime" in row
                # Should NOT have writetime for status
                assert "status_writetime" not in row

                # Should have TTL only for status
                assert "status_ttl" in row
                # Should NOT have TTL for name or email
                assert "name_ttl" not in row
                assert "email_ttl" not in row

        finally:
            Path(output_path).unlink(missing_ok=True)

    @pytest.mark.asyncio
    async def test_writetime_filter_mode_with_ttl(self, session):
        """
        Test writetime filter modes (any/all) with TTL export.

        What this tests:
        ---------------
        1. ANY mode filtering with TTL
        2. ALL mode filtering with TTL
        3. Mixed writetime columns
        4. TTL preservation accuracy

        Why this matters:
        ----------------
        - Complex filtering logic
        - Partial updates handling
        - Migration precision
        - Data consistency
        """
        table_name = f"test_filter_mode_{int(time.time() * 1000)}"
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

            # Insert base data
            await session.execute(
                f"""
                INSERT INTO {table_name} (id, col_a, col_b, col_c)
                VALUES (1, 'a1', 'b1', 'c1')
                USING TTL 3600
                """
            )

            # Get writetime boundary
            await asyncio.sleep(0.1)
            boundary_time = int(time.time() * 1_000_000)

            # Update only one column after boundary
            await session.execute(
                f"""
                UPDATE {table_name} USING TTL 7200
                SET col_a = 'a1_new'
                WHERE id = 1
                """
            )

            operator = BulkOperator(session=session)

            # Test ANY mode - should include row
            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
                output_any = tmp.name

            await operator.export(
                table=full_table_name,
                output_path=output_any,
                format="json",
                options={
                    "include_writetime": True,
                    "include_ttl": True,
                    "writetime_after": boundary_time,
                    "writetime_filter_mode": "any",
                },
            )

            with open(output_any, "r") as f:
                data_any = json.load(f)

            # Should include the row (col_a matches)
            assert len(data_any) == 1
            assert data_any[0]["col_a"] == "a1_new"
            # Should have different TTL values
            assert data_any[0]["col_a_ttl"] > data_any[0]["col_b_ttl"]

            # Test ALL mode - should exclude row
            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
                output_all = tmp.name

            await operator.export(
                table=full_table_name,
                output_path=output_all,
                format="json",
                options={
                    "include_writetime": True,
                    "include_ttl": True,
                    "writetime_after": boundary_time,
                    "writetime_filter_mode": "all",
                },
            )

            with open(output_all, "r") as f:
                data_all = json.load(f)

            # Should exclude the row (not all columns match)
            assert len(data_all) == 0

            Path(output_any).unlink(missing_ok=True)
            Path(output_all).unlink(missing_ok=True)

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {table_name}")

    @pytest.mark.asyncio
    async def test_csv_export_writetime_ttl(self, session, combined_table):
        """
        Test CSV export with writetime and TTL.

        What this tests:
        ---------------
        1. CSV format handling
        2. Header generation
        3. Value formatting
        4. Metadata columns

        Why this matters:
        ----------------
        - CSV is common format
        - Header complexity
        - Type preservation
        - Import compatibility
        """
        table_name, boundary_time = combined_table

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
            output_path = tmp.name

        try:
            operator = BulkOperator(session=session)

            # Export all with writetime and TTL
            await operator.export(
                table=table_name,
                output_path=output_path,
                format="csv",
                options={
                    "include_writetime": True,
                    "include_ttl": True,
                },
            )

            # Read and verify CSV
            import csv

            with open(output_path, "r") as f:
                reader = csv.DictReader(f)
                rows = list(reader)

            assert len(rows) == 4

            # Check headers include both writetime and TTL
            headers = rows[0].keys()
            assert "name_writetime" in headers
            assert "name_ttl" in headers
            assert "email_writetime" in headers
            assert "email_ttl" in headers

            # Verify data format
            for row in rows:
                # Writetime should be formatted datetime
                if row["name_writetime"]:
                    assert len(row["name_writetime"]) > 10  # Datetime string
                # TTL should be numeric or empty
                if row["name_ttl"]:
                    assert row["name_ttl"].isdigit() or row["name_ttl"] == ""

        finally:
            Path(output_path).unlink(missing_ok=True)

    @pytest.mark.asyncio
    async def test_performance_impact(self, session):
        """
        Test performance with both writetime and TTL export.

        What this tests:
        ---------------
        1. Query complexity impact
        2. Large result handling
        3. Memory efficiency
        4. Export speed

        Why this matters:
        ----------------
        - Production performance
        - Resource planning
        - Optimization needs
        - Scalability validation
        """
        table_name = f"test_performance_{int(time.time() * 1000)}"
        full_table_name = f"test_bulk.{table_name}"

        try:
            await session.execute(
                f"""
                CREATE TABLE {table_name} (
                    id INT PRIMARY KEY,
                    data1 TEXT,
                    data2 TEXT,
                    data3 TEXT,
                    data4 TEXT,
                    data5 TEXT
                )
                """
            )

            # Insert 100 rows with TTL
            for i in range(100):
                await session.execute(
                    f"""
                    INSERT INTO {table_name} (id, data1, data2, data3, data4, data5)
                    VALUES ({i}, 'value1_{i}', 'value2_{i}', 'value3_{i}',
                            'value4_{i}', 'value5_{i}')
                    USING TTL {3600 + i * 10}
                    """
                )

            operator = BulkOperator(session=session)

            # Time export without metadata
            start = time.time()
            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
                output1 = tmp.name

            await operator.export(
                table=full_table_name,
                output_path=output1,
                format="json",
            )
            time_without = time.time() - start

            # Time export with both writetime and TTL
            start = time.time()
            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
                output2 = tmp.name

            await operator.export(
                table=full_table_name,
                output_path=output2,
                format="json",
                options={
                    "include_writetime": True,
                    "include_ttl": True,
                },
            )
            time_with = time.time() - start

            # Performance should be reasonable (less than 3x slower)
            assert time_with < time_without * 3

            # Verify data completeness
            with open(output2, "r") as f:
                data = json.load(f)

            assert len(data) == 100
            # Each row should have metadata
            assert all("data1_writetime" in row for row in data)
            assert all("data1_ttl" in row for row in data)

            Path(output1).unlink(missing_ok=True)
            Path(output2).unlink(missing_ok=True)

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {table_name}")
