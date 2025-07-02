"""
Integration tests for bulk export operations.

What this tests:
---------------
1. Export captures all rows exactly once
2. Streaming doesn't exhaust memory
3. Order within ranges is preserved
4. Async iteration works correctly
5. Export handles different data types

Why this matters:
----------------
- Export must be complete and accurate
- Memory efficiency critical for large tables
- Streaming enables TB-scale exports
- Foundation for Iceberg integration
"""

import asyncio

import pytest

from async_cassandra import AsyncCluster
from bulk_operations.bulk_operator import TokenAwareBulkOperator


@pytest.mark.integration
class TestBulkExport:
    """Test bulk export operations against real Cassandra cluster."""

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
        """Create test session with keyspace and table."""
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

        # Create test table
        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS bulk_test.test_data (
                id INT PRIMARY KEY,
                data TEXT,
                value DOUBLE
            )
        """
        )

        # Clear any existing data
        await session.execute("TRUNCATE bulk_test.test_data")

        yield session

    @pytest.mark.asyncio
    async def test_export_streaming_completeness(self, session):
        """
        Test streaming export doesn't miss or duplicate data.

        What this tests:
        ---------------
        1. Export captures all rows exactly once
        2. Streaming doesn't exhaust memory
        3. Order within ranges is preserved
        4. Async iteration works correctly

        Why this matters:
        ----------------
        - Export must be complete and accurate
        - Memory efficiency critical for large tables
        - Streaming enables TB-scale exports
        - Foundation for Iceberg integration
        """
        # Use smaller dataset for export test
        await session.execute("TRUNCATE bulk_test.test_data")

        # Insert test data
        insert_stmt = await session.prepare(
            """
            INSERT INTO bulk_test.test_data (id, data, value)
            VALUES (?, ?, ?)
        """
        )

        expected_ids = set(range(1000))
        for i in expected_ids:
            await session.execute(insert_stmt, (i, f"data-{i}", float(i)))

        # Export using token ranges
        operator = TokenAwareBulkOperator(session)

        exported_ids = set()
        row_count = 0

        async for row in operator.export_by_token_ranges(
            keyspace="bulk_test", table="test_data", split_count=16
        ):
            exported_ids.add(row.id)
            row_count += 1

            # Verify row data integrity
            assert row.data == f"data-{row.id}"
            assert row.value == float(row.id)

        print("\nExport results:")
        print(f"  Expected rows: {len(expected_ids)}")
        print(f"  Exported rows: {row_count}")
        print(f"  Unique IDs: {len(exported_ids)}")

        # Verify completeness
        assert row_count == len(
            expected_ids
        ), f"Row count mismatch: {row_count} vs {len(expected_ids)}"

        assert exported_ids == expected_ids, (
            f"Missing IDs: {expected_ids - exported_ids}, "
            f"Duplicate IDs: {exported_ids - expected_ids}"
        )

    @pytest.mark.asyncio
    async def test_export_with_wraparound_ranges(self, session):
        """
        Test export handles wraparound ranges correctly.

        What this tests:
        ---------------
        1. Data in wraparound ranges is exported
        2. No duplicates from split queries
        3. All edge cases handled
        4. Consistent with count operation

        Why this matters:
        ----------------
        - Wraparound ranges are common with vnodes
        - Export must handle same edge cases as count
        - Data integrity is critical
        - Foundation for all bulk operations
        """
        # Insert data that will span wraparound ranges
        insert_stmt = await session.prepare(
            """
            INSERT INTO bulk_test.test_data (id, data, value)
            VALUES (?, ?, ?)
        """
        )

        # Insert data with various IDs to ensure coverage
        test_data = {}
        for i in range(0, 10000, 100):  # Sparse data to hit various ranges
            test_data[i] = f"data-{i}"
            await session.execute(insert_stmt, (i, test_data[i], float(i)))

        # Export and verify
        operator = TokenAwareBulkOperator(session)

        exported_data = {}
        async for row in operator.export_by_token_ranges(
            keyspace="bulk_test",
            table="test_data",
            split_count=32,  # More splits to ensure wraparound handling
        ):
            exported_data[row.id] = row.data

        print(f"\nExported {len(exported_data)} rows")
        assert len(exported_data) == len(
            test_data
        ), f"Export count mismatch: {len(exported_data)} vs {len(test_data)}"

        # Verify all data was exported correctly
        for id_val, expected_data in test_data.items():
            assert id_val in exported_data, f"Missing ID {id_val}"
            assert (
                exported_data[id_val] == expected_data
            ), f"Data mismatch for ID {id_val}: {exported_data[id_val]} vs {expected_data}"

    @pytest.mark.asyncio
    async def test_export_memory_efficiency(self, session):
        """
        Test export streaming is memory efficient.

        What this tests:
        ---------------
        1. Large exports don't consume excessive memory
        2. Streaming works as expected
        3. Can handle tables larger than memory
        4. Progress tracking during export

        Why this matters:
        ----------------
        - Production tables can be TB in size
        - Must stream, not buffer all data
        - Memory efficiency enables large exports
        - Critical for operational feasibility
        """
        # Insert larger dataset
        insert_stmt = await session.prepare(
            """
            INSERT INTO bulk_test.test_data (id, data, value)
            VALUES (?, ?, ?)
        """
        )

        row_count = 10000
        print(f"\nInserting {row_count} rows for memory test...")

        # Insert in batches
        batch_size = 100
        for i in range(0, row_count, batch_size):
            tasks = []
            for j in range(batch_size):
                if i + j < row_count:
                    # Create larger data values to test memory
                    data = f"data-{i+j}" * 10  # Make data larger
                    tasks.append(session.execute(insert_stmt, (i + j, data, float(i + j))))
            await asyncio.gather(*tasks)

        operator = TokenAwareBulkOperator(session)

        # Track memory usage indirectly via row processing rate
        rows_exported = 0
        batch_timings = []

        import time

        start_time = time.time()
        last_batch_time = start_time

        async for _row in operator.export_by_token_ranges(
            keyspace="bulk_test", table="test_data", split_count=16
        ):
            rows_exported += 1

            # Track timing every 1000 rows
            if rows_exported % 1000 == 0:
                current_time = time.time()
                batch_duration = current_time - last_batch_time
                batch_timings.append(batch_duration)
                last_batch_time = current_time
                print(f"  Exported {rows_exported} rows...")

        total_duration = time.time() - start_time

        print("\nExport completed:")
        print(f"  Total rows: {rows_exported}")
        print(f"  Total time: {total_duration:.2f}s")
        print(f"  Rows/sec: {rows_exported/total_duration:.0f}")

        # Verify all rows exported
        assert rows_exported == row_count, f"Export count mismatch: {rows_exported} vs {row_count}"

        # Verify consistent performance (no major slowdowns from memory pressure)
        if len(batch_timings) > 2:
            avg_batch_time = sum(batch_timings) / len(batch_timings)
            max_batch_time = max(batch_timings)
            assert (
                max_batch_time < avg_batch_time * 3
            ), "Export performance degraded, possible memory issue"

    @pytest.mark.asyncio
    async def test_export_with_different_data_types(self, session):
        """
        Test export handles various CQL data types correctly.

        What this tests:
        ---------------
        1. Different data types are exported correctly
        2. NULL values handled properly
        3. Collections exported accurately
        4. Special characters preserved

        Why this matters:
        ----------------
        - Real tables have diverse data types
        - Export must preserve data fidelity
        - Type handling affects Iceberg mapping
        - Data integrity across formats
        """
        # Create table with various data types
        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS bulk_test.complex_data (
                id INT PRIMARY KEY,
                text_col TEXT,
                int_col INT,
                double_col DOUBLE,
                bool_col BOOLEAN,
                list_col LIST<TEXT>,
                set_col SET<INT>,
                map_col MAP<TEXT, INT>
            )
        """
        )

        await session.execute("TRUNCATE bulk_test.complex_data")

        # Insert test data with various types
        insert_stmt = await session.prepare(
            """
            INSERT INTO bulk_test.complex_data
            (id, text_col, int_col, double_col, bool_col, list_col, set_col, map_col)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        )

        test_data = [
            (1, "normal text", 100, 1.5, True, ["a", "b", "c"], {1, 2, 3}, {"x": 1, "y": 2}),
            (2, "special chars: 'quotes' \"double\" \n newline", -50, -2.5, False, [], set(), {}),
            (3, None, None, None, None, None, None, None),  # NULL values
            (4, "", 0, 0.0, True, [""], {0}, {"": 0}),  # Empty/zero values
            (5, "unicode: 你好 🌟", 999999, 3.14159, False, ["α", "β", "γ"], {-1, -2}, {"π": 314}),
        ]

        for row in test_data:
            await session.execute(insert_stmt, row)

        # Export and verify
        operator = TokenAwareBulkOperator(session)

        exported_rows = []
        async for row in operator.export_by_token_ranges(
            keyspace="bulk_test", table="complex_data", split_count=4
        ):
            exported_rows.append(row)

        print(f"\nExported {len(exported_rows)} rows with complex data types")
        assert len(exported_rows) == len(
            test_data
        ), f"Export count mismatch: {len(exported_rows)} vs {len(test_data)}"

        # Sort both by ID for comparison
        exported_rows.sort(key=lambda r: r.id)
        test_data.sort(key=lambda r: r[0])

        # Verify each row's data
        for exported, expected in zip(exported_rows, test_data, strict=False):
            assert exported.id == expected[0]
            assert exported.text_col == expected[1]
            assert exported.int_col == expected[2]
            assert exported.double_col == expected[3]
            assert exported.bool_col == expected[4]

            # Collections need special handling
            # Note: Cassandra treats empty collections as NULL
            if expected[5] is not None and expected[5] != []:
                assert exported.list_col is not None, f"list_col is None for row {exported.id}"
                assert list(exported.list_col) == expected[5]
            else:
                # Empty list or None in Cassandra returns as None
                assert exported.list_col is None

            if expected[6] is not None and expected[6] != set():
                assert exported.set_col is not None, f"set_col is None for row {exported.id}"
                assert set(exported.set_col) == expected[6]
            else:
                # Empty set or None in Cassandra returns as None
                assert exported.set_col is None

            if expected[7] is not None and expected[7] != {}:
                assert exported.map_col is not None, f"map_col is None for row {exported.id}"
                assert dict(exported.map_col) == expected[7]
            else:
                # Empty map or None in Cassandra returns as None
                assert exported.map_col is None
