"""
Integration tests for data integrity - verifying inserted data is correctly returned.

What this tests:
---------------
1. Data inserted is exactly what gets exported
2. All data types are preserved correctly
3. No data corruption during token range queries
4. Prepared statements maintain data integrity

Why this matters:
----------------
- Proves end-to-end data correctness
- Validates our token range implementation
- Ensures no data loss or corruption
- Critical for production confidence
"""

import asyncio
import uuid
from datetime import datetime
from decimal import Decimal

import pytest

from async_cassandra import AsyncCluster
from bulk_operations.bulk_operator import TokenAwareBulkOperator


@pytest.mark.integration
class TestDataIntegrity:
    """Test that data inserted equals data exported."""

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
        """Create test session with keyspace and tables."""
        session = await cluster.connect()

        # Create test keyspace
        await session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS bulk_test
            WITH replication = {
                'class': 'SimpleStrategy',
                'replication_factor': 1
            }
        """
        )

        yield session

    @pytest.mark.asyncio
    async def test_simple_data_round_trip(self, session):
        """
        Test that simple data inserted is exactly what we get back.

        What this tests:
        ---------------
        1. Insert known dataset with various values
        2. Export using token ranges
        3. Verify every field matches exactly
        4. No missing or corrupted data

        Why this matters:
        ----------------
        - Basic data integrity validation
        - Ensures token range queries don't corrupt data
        - Validates prepared statement parameter handling
        - Foundation for trusting bulk operations
        """
        # Create a simple test table
        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS bulk_test.integrity_test (
                id INT PRIMARY KEY,
                name TEXT,
                value DOUBLE,
                active BOOLEAN
            )
        """
        )

        await session.execute("TRUNCATE bulk_test.integrity_test")

        # Insert test data with prepared statement
        insert_stmt = await session.prepare(
            """
            INSERT INTO bulk_test.integrity_test (id, name, value, active)
            VALUES (?, ?, ?, ?)
        """
        )

        # Create test dataset with various values
        test_data = [
            (1, "Alice", 100.5, True),
            (2, "Bob", -50.25, False),
            (3, "Charlie", 0.0, True),
            (4, None, 999.999, None),  # Test NULLs
            (5, "", -0.001, False),  # Empty string
            (6, "Special chars: 'quotes' \"double\"", 3.14159, True),
            (7, "Unicode: 你好 🌟", 2.71828, False),
            (8, "Very long name " * 100, 1.23456, True),  # Long string
        ]

        # Insert all test data
        for row in test_data:
            await session.execute(insert_stmt, row)

        # Export using bulk operator
        operator = TokenAwareBulkOperator(session)
        exported_data = []

        async for row in operator.export_by_token_ranges(
            keyspace="bulk_test",
            table="integrity_test",
            split_count=4,  # Use multiple ranges to test splitting
        ):
            exported_data.append((row.id, row.name, row.value, row.active))

        # Sort both datasets by ID for comparison
        test_data_sorted = sorted(test_data, key=lambda x: x[0])
        exported_data_sorted = sorted(exported_data, key=lambda x: x[0])

        # Verify we got all rows
        assert len(exported_data_sorted) == len(
            test_data_sorted
        ), f"Row count mismatch: exported {len(exported_data_sorted)} vs inserted {len(test_data_sorted)}"

        # Verify each row matches exactly
        for inserted, exported in zip(test_data_sorted, exported_data_sorted, strict=False):
            assert (
                inserted == exported
            ), f"Data mismatch for ID {inserted[0]}: inserted {inserted} vs exported {exported}"

        print(f"\n✓ All {len(test_data)} rows verified - data integrity maintained")

    @pytest.mark.asyncio
    async def test_complex_data_types_round_trip(self, session):
        """
        Test complex CQL data types maintain integrity.

        What this tests:
        ---------------
        1. Collections (list, set, map)
        2. UUID types
        3. Timestamp/date types
        4. Decimal types
        5. Large text/blob data

        Why this matters:
        ----------------
        - Real tables use complex types
        - Collections need special handling
        - Precision must be maintained
        - Production data is complex
        """
        # Create table with complex types
        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS bulk_test.complex_integrity (
                id UUID PRIMARY KEY,
                created TIMESTAMP,
                amount DECIMAL,
                tags SET<TEXT>,
                metadata MAP<TEXT, INT>,
                events LIST<TIMESTAMP>,
                data BLOB
            )
        """
        )

        await session.execute("TRUNCATE bulk_test.complex_integrity")

        # Insert test data
        insert_stmt = await session.prepare(
            """
            INSERT INTO bulk_test.complex_integrity
            (id, created, amount, tags, metadata, events, data)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        )

        # Create test data
        test_id = uuid.uuid4()
        test_created = datetime.utcnow().replace(microsecond=0)  # Cassandra timestamp precision
        test_amount = Decimal("12345.6789")
        test_tags = {"python", "cassandra", "async", "test"}
        test_metadata = {"version": 1, "retries": 3, "timeout": 30}
        test_events = [
            datetime(2024, 1, 1, 10, 0, 0),
            datetime(2024, 1, 2, 11, 30, 0),
            datetime(2024, 1, 3, 15, 45, 0),
        ]
        test_data = b"Binary data with \x00 null bytes and \xff high bytes"

        # Insert the data
        await session.execute(
            insert_stmt,
            (
                test_id,
                test_created,
                test_amount,
                test_tags,
                test_metadata,
                test_events,
                test_data,
            ),
        )

        # Export and verify
        operator = TokenAwareBulkOperator(session)
        exported_rows = []

        async for row in operator.export_by_token_ranges(
            keyspace="bulk_test",
            table="complex_integrity",
            split_count=2,
        ):
            exported_rows.append(row)

        # Should have exactly one row
        assert len(exported_rows) == 1, f"Expected 1 row, got {len(exported_rows)}"

        row = exported_rows[0]

        # Verify each field
        assert row.id == test_id, f"UUID mismatch: {row.id} vs {test_id}"
        assert row.created == test_created, f"Timestamp mismatch: {row.created} vs {test_created}"
        assert row.amount == test_amount, f"Decimal mismatch: {row.amount} vs {test_amount}"
        assert set(row.tags) == test_tags, f"Set mismatch: {set(row.tags)} vs {test_tags}"
        assert (
            dict(row.metadata) == test_metadata
        ), f"Map mismatch: {dict(row.metadata)} vs {test_metadata}"
        assert (
            list(row.events) == test_events
        ), f"List mismatch: {list(row.events)} vs {test_events}"
        assert bytes(row.data) == test_data, f"Blob mismatch: {bytes(row.data)} vs {test_data}"

        print("\n✓ Complex data types verified - all types preserved correctly")

    @pytest.mark.asyncio
    async def test_large_dataset_integrity(self, session):  # noqa: C901
        """
        Test integrity with larger dataset across many token ranges.

        What this tests:
        ---------------
        1. 50K rows with computed values
        2. Verify no rows lost in token ranges
        3. Verify no duplicate rows
        4. Check computed values match

        Why this matters:
        ----------------
        - Production tables are large
        - Token range bugs appear at scale
        - Wraparound ranges must work correctly
        - Performance under load
        """
        # Create table
        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS bulk_test.large_integrity (
                id INT PRIMARY KEY,
                computed_value DOUBLE,
                hash_value TEXT
            )
        """
        )

        await session.execute("TRUNCATE bulk_test.large_integrity")

        # Insert data with computed values
        insert_stmt = await session.prepare(
            """
            INSERT INTO bulk_test.large_integrity (id, computed_value, hash_value)
            VALUES (?, ?, ?)
        """
        )

        # Function to compute expected values
        def compute_value(id_val):
            return float(id_val * 3.14159 + id_val**0.5)

        def compute_hash(id_val):
            return f"hash_{id_val % 1000:03d}_{id_val}"

        # Insert 50K rows in batches
        total_rows = 50000
        batch_size = 1000

        print(f"\nInserting {total_rows} rows for large dataset test...")

        for batch_start in range(0, total_rows, batch_size):
            tasks = []
            for i in range(batch_start, min(batch_start + batch_size, total_rows)):
                tasks.append(
                    session.execute(
                        insert_stmt,
                        (
                            i,
                            compute_value(i),
                            compute_hash(i),
                        ),
                    )
                )
            await asyncio.gather(*tasks)

            if (batch_start + batch_size) % 10000 == 0:
                print(f"  Inserted {batch_start + batch_size} rows...")

        # Export all data
        operator = TokenAwareBulkOperator(session)
        exported_ids = set()
        value_mismatches = []
        hash_mismatches = []

        print("\nExporting and verifying data...")

        async for row in operator.export_by_token_ranges(
            keyspace="bulk_test",
            table="large_integrity",
            split_count=32,  # Many splits to test range handling
        ):
            # Check for duplicates
            if row.id in exported_ids:
                pytest.fail(f"Duplicate ID exported: {row.id}")
            exported_ids.add(row.id)

            # Verify computed values
            expected_value = compute_value(row.id)
            if abs(row.computed_value - expected_value) > 0.0001:  # Float precision
                value_mismatches.append((row.id, row.computed_value, expected_value))

            expected_hash = compute_hash(row.id)
            if row.hash_value != expected_hash:
                hash_mismatches.append((row.id, row.hash_value, expected_hash))

        # Verify completeness
        assert (
            len(exported_ids) == total_rows
        ), f"Missing rows: exported {len(exported_ids)} vs inserted {total_rows}"

        # Check for missing IDs
        expected_ids = set(range(total_rows))
        missing_ids = expected_ids - exported_ids
        if missing_ids:
            pytest.fail(f"Missing IDs: {sorted(list(missing_ids))[:10]}...")  # Show first 10

        # Check for value mismatches
        if value_mismatches:
            pytest.fail(f"Value mismatches found: {value_mismatches[:5]}...")  # Show first 5

        if hash_mismatches:
            pytest.fail(f"Hash mismatches found: {hash_mismatches[:5]}...")  # Show first 5

        print(f"\n✓ All {total_rows} rows verified - large dataset integrity maintained")
        print("  - No missing rows")
        print("  - No duplicate rows")
        print("  - All computed values correct")
        print("  - All hash values correct")

    @pytest.mark.asyncio
    async def test_wraparound_range_data_integrity(self, session):
        """
        Test data integrity specifically for wraparound token ranges.

        What this tests:
        ---------------
        1. Insert data with known tokens that span wraparound
        2. Verify wraparound range handling preserves data
        3. No data lost at ring boundaries
        4. Prepared statements work correctly with wraparound

        Why this matters:
        ----------------
        - Wraparound ranges are error-prone
        - Must split into two queries correctly
        - Data at ring boundaries is critical
        - Common source of data loss bugs
        """
        # Create table
        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS bulk_test.wraparound_test (
                id INT PRIMARY KEY,
                token_value BIGINT,
                data TEXT
            )
        """
        )

        await session.execute("TRUNCATE bulk_test.wraparound_test")

        # First, let's find some IDs that hash to extreme token values
        print("\nFinding IDs with extreme token values...")

        # Insert some data and check their tokens
        insert_stmt = await session.prepare(
            """
            INSERT INTO bulk_test.wraparound_test (id, token_value, data)
            VALUES (?, ?, ?)
        """
        )

        # Try different IDs to find ones with extreme tokens
        test_ids = []
        for i in range(100000, 200000):
            # First insert a dummy row to query the token
            await session.execute(insert_stmt, (i, 0, f"dummy_{i}"))
            result = await session.execute(
                f"SELECT token(id) as t FROM bulk_test.wraparound_test WHERE id = {i}"
            )
            row = result.one()
            if row:
                token = row.t
                # Remove the dummy row
                await session.execute(f"DELETE FROM bulk_test.wraparound_test WHERE id = {i}")

                # Look for very high positive or very low negative tokens
                if token > 9000000000000000000 or token < -9000000000000000000:
                    test_ids.append((i, token))
                    await session.execute(insert_stmt, (i, token, f"data_{i}"))

            if len(test_ids) >= 20:
                break

        print(f"  Found {len(test_ids)} IDs with extreme tokens")

        # Export and verify
        operator = TokenAwareBulkOperator(session)
        exported_data = {}

        async for row in operator.export_by_token_ranges(
            keyspace="bulk_test",
            table="wraparound_test",
            split_count=8,
        ):
            exported_data[row.id] = (row.token_value, row.data)

        # Verify all data was exported
        for id_val, token_val in test_ids:
            assert id_val in exported_data, f"Missing ID {id_val} with token {token_val}"

            exported_token, exported_data_val = exported_data[id_val]
            assert (
                exported_token == token_val
            ), f"Token mismatch for ID {id_val}: {exported_token} vs {token_val}"
            assert (
                exported_data_val == f"data_{id_val}"
            ), f"Data mismatch for ID {id_val}: {exported_data_val} vs data_{id_val}"

        print("\n✓ Wraparound range data integrity verified")
        print(f"  - All {len(test_ids)} extreme token rows exported correctly")
        print("  - Token values preserved")
        print("  - Data values preserved")
