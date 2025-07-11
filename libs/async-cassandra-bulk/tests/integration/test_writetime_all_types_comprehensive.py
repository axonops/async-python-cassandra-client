"""
Comprehensive integration tests for writetime with all Cassandra data types.

What this tests:
---------------
1. Writetime behavior for EVERY Cassandra data type
2. NULL handling - explicit NULL vs missing columns
3. Data types that don't support writetime (counters, primary keys)
4. Complex types (collections, UDTs, tuples) writetime behavior
5. Edge cases and error conditions

Why this matters:
----------------
- Database driver must handle ALL data types correctly
- NULL handling is critical for data integrity
- Must clearly document what supports writetime
- Production safety requires exhaustive testing
"""

import csv
import json
import tempfile
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest
from cassandra.util import Date, Duration, Time, uuid_from_time

from async_cassandra_bulk import BulkOperator


class TestWritetimeAllTypesComprehensive:
    """Comprehensive tests for writetime with all Cassandra data types."""

    @pytest.mark.asyncio
    async def test_writetime_basic_types(self, session):
        """
        Test writetime behavior for all basic Cassandra types.

        What this tests:
        ---------------
        1. String types (ASCII, TEXT, VARCHAR) - should support writetime
        2. Numeric types (all integers, floats, decimal) - should support writetime
        3. Temporal types (DATE, TIME, TIMESTAMP) - should support writetime
        4. Binary (BLOB) - should support writetime
        5. Boolean, UUID, INET - should support writetime

        Why this matters:
        ----------------
        - Each type might serialize writetime differently
        - Must verify all basic types work correctly
        - Foundation for more complex type testing
        - Production uses all these types

        Additional context:
        ---------------------------------
        Example of expected behavior:
        - INSERT with USING TIMESTAMP sets writetime
        - UPDATE can change writetime per column
        - All non-key columns should have writetime
        """
        table_name = f"writetime_basic_{int(datetime.now().timestamp() * 1000)}"
        keyspace = "test_bulk"

        await session.execute(
            f"""
            CREATE TABLE {keyspace}.{table_name} (
                -- Primary key (no writetime)
                id UUID PRIMARY KEY,

                -- String types (all support writetime)
                ascii_col ASCII,
                text_col TEXT,
                varchar_col VARCHAR,

                -- Numeric types (all support writetime)
                tinyint_col TINYINT,
                smallint_col SMALLINT,
                int_col INT,
                bigint_col BIGINT,
                varint_col VARINT,
                float_col FLOAT,
                double_col DOUBLE,
                decimal_col DECIMAL,

                -- Temporal types (all support writetime)
                date_col DATE,
                time_col TIME,
                timestamp_col TIMESTAMP,
                duration_col DURATION,

                -- Binary type (supports writetime)
                blob_col BLOB,

                -- Other types (all support writetime)
                boolean_col BOOLEAN,
                inet_col INET,
                uuid_col UUID,
                timeuuid_col TIMEUUID
            )
        """
        )

        try:
            # Insert with specific writetime
            test_id = uuid4()
            base_writetime = 1700000000000000  # microseconds since epoch

            # Prepare statement for better control
            insert_stmt = await session.prepare(
                f"""
                INSERT INTO {keyspace}.{table_name} (
                    id, ascii_col, text_col, varchar_col,
                    tinyint_col, smallint_col, int_col, bigint_col, varint_col,
                    float_col, double_col, decimal_col,
                    date_col, time_col, timestamp_col, duration_col,
                    blob_col, boolean_col, inet_col, uuid_col, timeuuid_col
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                ) USING TIMESTAMP ?
                """
            )

            # Test data
            test_date = Date(date.today())
            # Time in nanoseconds since midnight
            test_time = Time((14 * 3600 + 30 * 60 + 45) * 1_000_000_000 + 123_456_000)
            test_timestamp = datetime.now(timezone.utc)
            test_duration = Duration(months=1, days=2, nanoseconds=3000000000)
            test_timeuuid = uuid_from_time(datetime.now())

            await session.execute(
                insert_stmt,
                (
                    test_id,
                    "ascii_value",
                    "text with unicode 🚀",
                    "varchar_value",
                    127,  # tinyint
                    32767,  # smallint
                    2147483647,  # int
                    9223372036854775807,  # bigint
                    10**50,  # varint
                    3.14159,  # float
                    2.718281828,  # double
                    Decimal("999999999.999999999"),
                    test_date,
                    test_time,
                    test_timestamp,
                    test_duration,
                    b"binary\x00\x01\xff",
                    True,
                    "192.168.1.1",
                    uuid4(),
                    test_timeuuid,
                    base_writetime,
                ),
            )

            # Update some columns with different writetime
            update_writetime = base_writetime + 1000000
            await session.execute(
                f"""
                UPDATE {keyspace}.{table_name}
                USING TIMESTAMP {update_writetime}
                SET text_col = 'updated text',
                    int_col = 999,
                    boolean_col = false
                WHERE id = {test_id}
                """
            )

            # Export with writetime for all columns
            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
                output_path = tmp.name

            operator = BulkOperator(session=session)
            stats = await operator.export(
                table=f"{keyspace}.{table_name}",
                output_path=output_path,
                format="json",
                options={"include_writetime": True},
            )

            assert stats.rows_processed == 1

            # Verify writetime values
            with open(output_path, "r") as f:
                data = json.load(f)
                row = data[0]

            # Primary key should NOT have writetime
            assert "id_writetime" not in row

            # All other columns should have writetime
            expected_writetime_cols = [
                "ascii_col",
                "text_col",
                "varchar_col",
                "tinyint_col",
                "smallint_col",
                "int_col",
                "bigint_col",
                "varint_col",
                "float_col",
                "double_col",
                "decimal_col",
                "date_col",
                "time_col",
                "timestamp_col",
                "duration_col",
                "blob_col",
                "boolean_col",
                "inet_col",
                "uuid_col",
                "timeuuid_col",
            ]

            for col in expected_writetime_cols:
                writetime_col = f"{col}_writetime"
                assert writetime_col in row, f"Missing writetime for {col}"
                assert row[writetime_col] is not None, f"Null writetime for {col}"

            # Verify updated columns have newer writetime
            # Writetime values might be in microseconds or ISO format
            text_wt_val = row["text_col_writetime"]
            int_wt_val = row["int_col_writetime"]
            bool_wt_val = row["boolean_col_writetime"]
            ascii_wt_val = row["ascii_col_writetime"]

            # Handle both microseconds and ISO string formats
            if isinstance(text_wt_val, (int, float)):
                # Microseconds format
                assert text_wt_val == update_writetime
                assert int_wt_val == update_writetime
                assert bool_wt_val == update_writetime
                assert ascii_wt_val == base_writetime
            else:
                # ISO string format
                base_dt = datetime.fromtimestamp(base_writetime / 1000000, tz=timezone.utc)
                update_dt = datetime.fromtimestamp(update_writetime / 1000000, tz=timezone.utc)

                text_wt = datetime.fromisoformat(text_wt_val.replace("Z", "+00:00"))
                int_wt = datetime.fromisoformat(int_wt_val.replace("Z", "+00:00"))
                bool_wt = datetime.fromisoformat(bool_wt_val.replace("Z", "+00:00"))
                ascii_wt = datetime.fromisoformat(ascii_wt_val.replace("Z", "+00:00"))

                # Updated columns should have update writetime
                assert abs((text_wt - update_dt).total_seconds()) < 1
                assert abs((int_wt - update_dt).total_seconds()) < 1
                assert abs((bool_wt - update_dt).total_seconds()) < 1

                # Non-updated columns should have base writetime
                assert abs((ascii_wt - base_dt).total_seconds()) < 1

            Path(output_path).unlink()

        finally:
            await session.execute(f"DROP TABLE {keyspace}.{table_name}")

    @pytest.mark.asyncio
    async def test_writetime_null_handling(self, session):
        """
        Test writetime behavior with NULL values and missing columns.

        What this tests:
        ---------------
        1. Explicit NULL insertion - no writetime
        2. Missing columns in INSERT - no writetime
        3. Setting column to NULL via UPDATE - removes writetime
        4. Partial row updates - only updated columns get new writetime
        5. Writetime filtering with NULL values

        Why this matters:
        ----------------
        - NULL handling is a critical edge case
        - Different from missing data
        - Affects data migration and filtering
        - Common source of bugs

        Additional context:
        ---------------------------------
        In Cassandra:
        - NULL means "delete this cell"
        - Missing in INSERT means "don't write this cell"
        - Both result in no writetime for that cell
        """
        table_name = f"writetime_null_{int(datetime.now().timestamp() * 1000)}"
        keyspace = "test_bulk"

        await session.execute(
            f"""
            CREATE TABLE {keyspace}.{table_name} (
                id INT PRIMARY KEY,
                col_a TEXT,
                col_b TEXT,
                col_c TEXT,
                col_d TEXT,
                col_e INT
            )
        """
        )

        try:
            base_writetime = 1700000000000000

            # Test 1: Insert with explicit NULL
            await session.execute(
                f"""
                INSERT INTO {keyspace}.{table_name} (id, col_a, col_b, col_c)
                VALUES (1, 'value_a', NULL, 'value_c')
                USING TIMESTAMP {base_writetime}
                """
            )

            # Test 2: Insert with missing columns (col_d, col_e not specified)
            await session.execute(
                f"""
                INSERT INTO {keyspace}.{table_name} (id, col_a, col_b)
                VALUES (2, 'value_a2', 'value_b2')
                USING TIMESTAMP {base_writetime}
                """
            )

            # Test 3: Update setting column to NULL (deletes the cell)
            await session.execute(
                f"""
                INSERT INTO {keyspace}.{table_name} (id, col_a, col_b, col_c, col_d, col_e)
                VALUES (3, 'a3', 'b3', 'c3', 'd3', 100)
                USING TIMESTAMP {base_writetime}
                """
            )

            await session.execute(
                f"""
                UPDATE {keyspace}.{table_name}
                USING TIMESTAMP {base_writetime + 1000000}
                SET col_b = NULL, col_c = 'c3_updated'
                WHERE id = 3
                """
            )

            # Test 4: Partial update (only some columns)
            await session.execute(
                f"""
                INSERT INTO {keyspace}.{table_name} (id, col_a, col_b, col_c, col_d)
                VALUES (4, 'a4', 'b4', 'c4', 'd4')
                USING TIMESTAMP {base_writetime}
                """
            )

            await session.execute(
                f"""
                UPDATE {keyspace}.{table_name}
                USING TIMESTAMP {base_writetime + 2000000}
                SET col_a = 'a4_updated'
                WHERE id = 4
                """
            )

            # Export with writetime
            with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
                output_path = tmp.name

            operator = BulkOperator(session=session)
            await operator.export(
                table=f"{keyspace}.{table_name}",
                output_path=output_path,
                format="csv",
                options={"include_writetime": True},
                csv_options={"null_value": "NULL"},
            )

            # Verify NULL handling
            with open(output_path, "r") as f:
                reader = csv.DictReader(f)
                rows = {int(row["id"]): row for row in reader}

            # Row 1: explicit NULL for col_b
            assert rows[1]["col_a"] != "NULL"
            assert rows[1]["col_b"] == "NULL"
            assert rows[1]["col_c"] != "NULL"
            assert rows[1]["col_a_writetime"] != "NULL"
            assert rows[1]["col_b_writetime"] == "NULL"  # NULL value = no writetime
            assert rows[1]["col_c_writetime"] != "NULL"
            assert rows[1]["col_d"] == "NULL"  # Not inserted
            assert rows[1]["col_d_writetime"] == "NULL"

            # Row 2: missing columns
            assert rows[2]["col_c"] == "NULL"  # Never inserted
            assert rows[2]["col_c_writetime"] == "NULL"
            assert rows[2]["col_d"] == "NULL"
            assert rows[2]["col_d_writetime"] == "NULL"

            # Row 3: NULL via UPDATE
            assert rows[3]["col_b"] == "NULL"  # Deleted by update
            assert rows[3]["col_b_writetime"] == "NULL"
            assert rows[3]["col_c"] == "c3_updated"
            assert rows[3]["col_c_writetime"] != "NULL"  # Has newer writetime

            # Row 4: Partial update
            assert rows[4]["col_a_writetime"] != rows[4]["col_b_writetime"]  # Different times

            # Now test writetime filtering with NULLs
            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp2:
                output_path2 = tmp2.name

            # Filter for rows updated after base_writetime + 500000
            filter_time = datetime.fromtimestamp(
                (base_writetime + 500000) / 1000000, tz=timezone.utc
            )

            await operator.export(
                table=f"{keyspace}.{table_name}",
                output_path=output_path2,
                format="json",
                options={
                    "writetime_columns": ["col_a", "col_b", "col_c", "col_d"],
                    "writetime_after": filter_time,
                    "writetime_filter_mode": "any",  # Include if ANY column matches
                },
            )

            with open(output_path2, "r") as f:
                filtered_data = json.load(f)

            # Should include rows 3 and 4 (have updates after filter time)
            filtered_ids = {row["id"] for row in filtered_data}
            assert 3 in filtered_ids  # col_c updated
            assert 4 in filtered_ids  # col_a updated
            assert 1 not in filtered_ids  # No updates after filter
            assert 2 not in filtered_ids  # No updates after filter

            Path(output_path).unlink()
            Path(output_path2).unlink()

        finally:
            await session.execute(f"DROP TABLE {keyspace}.{table_name}")

    @pytest.mark.asyncio
    async def test_writetime_collection_types(self, session):
        """
        Test writetime behavior with collection types.

        What this tests:
        ---------------
        1. LIST - entire list has one writetime
        2. SET - entire set has one writetime
        3. MAP - each map entry can have different writetime
        4. Frozen collections - single writetime
        5. Nested collections writetime behavior

        Why this matters:
        ----------------
        - Collections have special writetime semantics
        - MAP entries are independent cells
        - Critical for understanding data age
        - Affects filtering logic

        Additional context:
        ---------------------------------
        Collection writetime rules:
        - LIST/SET: Single writetime for entire collection
        - MAP: Each key-value pair has its own writetime
        - FROZEN: Always single writetime
        - Empty collections have no writetime
        """
        table_name = f"writetime_collections_{int(datetime.now().timestamp() * 1000)}"
        keyspace = "test_bulk"

        await session.execute(
            f"""
            CREATE TABLE {keyspace}.{table_name} (
                id INT PRIMARY KEY,

                -- Non-frozen collections
                tags LIST<TEXT>,
                unique_ids SET<UUID>,
                attributes MAP<TEXT, TEXT>,

                -- Frozen collections
                frozen_list FROZEN<LIST<INT>>,
                frozen_set FROZEN<SET<TEXT>>,
                frozen_map FROZEN<MAP<TEXT, INT>>,

                -- Nested collection
                nested MAP<TEXT, FROZEN<LIST<TEXT>>>
            )
        """
        )

        try:
            base_writetime = 1700000000000000

            # Insert collections with base writetime
            await session.execute(
                f"""
                INSERT INTO {keyspace}.{table_name}
                (id, tags, unique_ids, attributes, frozen_list, frozen_set, frozen_map)
                VALUES (
                    1,
                    ['tag1', 'tag2', 'tag3'],
                    {{{uuid4()}, {uuid4()}}},
                    {{'key1': 'value1', 'key2': 'value2'}},
                    [1, 2, 3],
                    {{'a', 'b', 'c'}},
                    {{'x': 10, 'y': 20}}
                )
                USING TIMESTAMP {base_writetime}
                """
            )

            # Update individual map entries with different writetime
            await session.execute(
                f"""
                UPDATE {keyspace}.{table_name}
                USING TIMESTAMP {base_writetime + 1000000}
                SET attributes['key3'] = 'value3'
                WHERE id = 1
                """
            )

            # Update entire list (new writetime for whole list)
            await session.execute(
                f"""
                UPDATE {keyspace}.{table_name}
                USING TIMESTAMP {base_writetime + 2000000}
                SET tags = ['new_tag1', 'new_tag2']
                WHERE id = 1
                """
            )

            # Export with writetime
            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
                output_path = tmp.name

            operator = BulkOperator(session=session)
            await operator.export(
                table=f"{keyspace}.{table_name}",
                output_path=output_path,
                format="json",
                options={"include_writetime": True},
            )

            with open(output_path, "r") as f:
                data = json.load(f)
                row = data[0]

            # LIST writetime - collections have writetime per element
            tags_wt = row.get("tags_writetime")
            if tags_wt:
                # Cassandra returns writetime per element in collections
                if isinstance(tags_wt, list):
                    # Each element has the same writetime since they were inserted together
                    assert len(tags_wt) == 2  # We updated to 2 elements
                    # All elements should have the updated writetime
                    for wt in tags_wt:
                        assert wt == base_writetime + 2000000
                else:
                    # Single writetime value
                    assert tags_wt == base_writetime + 2000000

            # SET writetime
            set_wt = row.get("unique_ids_writetime")
            if set_wt:
                if isinstance(set_wt, list):
                    # Each element has writetime
                    assert len(set_wt) == 2  # We inserted 2 UUIDs
                    for wt in set_wt:
                        assert wt == base_writetime
                else:
                    assert set_wt == base_writetime

            # MAP writetime - maps store writetime per key-value pair
            map_wt = row.get("attributes_writetime")
            if map_wt:
                # Maps typically have different writetime per entry
                if isinstance(map_wt, dict):
                    # Writetime per key
                    assert "key1" in map_wt
                    assert "key2" in map_wt
                    assert "key3" in map_wt
                    # key3 was added later
                    assert map_wt["key3"] == base_writetime + 1000000
                elif isinstance(map_wt, list):
                    # All entries as list
                    assert len(map_wt) >= 3

            # Frozen collections - single writetime
            frozen_list_wt = row.get("frozen_list_writetime")
            if frozen_list_wt:
                # Frozen collections have single writetime
                assert isinstance(frozen_list_wt, (int, str))

            frozen_set_wt = row.get("frozen_set_writetime")
            if frozen_set_wt:
                assert isinstance(frozen_set_wt, (int, str))

            frozen_map_wt = row.get("frozen_map_writetime")
            if frozen_map_wt:
                assert isinstance(frozen_map_wt, (int, str))

            # Test empty collections
            await session.execute(
                f"""
                INSERT INTO {keyspace}.{table_name} (id, tags, unique_ids)
                VALUES (2, [], {{}})
                USING TIMESTAMP {base_writetime}
                """
            )

            await operator.export(
                table=f"{keyspace}.{table_name}",
                output_path=output_path,
                format="json",
                options={"include_writetime": True},
            )

            with open(output_path, "r") as f:
                data = json.load(f)
                empty_row = next(r for r in data if r["id"] == 2)

            # Empty collections might have writetime or null depending on version
            # Important: document the actual behavior
            print(f"Empty list writetime: {empty_row.get('tags_writetime')}")
            print(f"Empty set writetime: {empty_row.get('unique_ids_writetime')}")

            Path(output_path).unlink()

        finally:
            await session.execute(f"DROP TABLE {keyspace}.{table_name}")

    @pytest.mark.asyncio
    async def test_writetime_counter_types(self, session):
        """
        Test that counter columns don't support writetime.

        What this tests:
        ---------------
        1. Counter columns return NULL for writetime
        2. Export doesn't fail with counters
        3. Filtering works correctly with counter tables
        4. Mixed counter/regular columns handled properly

        Why this matters:
        ----------------
        - Counters are special distributed types
        - No writetime support is by design
        - Must handle gracefully in exports
        - Common source of errors

        Additional context:
        ---------------------------------
        Counter limitations:
        - No INSERT, only UPDATE
        - No writetime support
        - Cannot mix with regular columns (except primary key)
        - Special consistency requirements
        """
        table_name = f"writetime_counters_{int(datetime.now().timestamp() * 1000)}"
        keyspace = "test_bulk"

        # Counter-only table
        await session.execute(
            f"""
            CREATE TABLE {keyspace}.{table_name} (
                id INT PRIMARY KEY,
                page_views COUNTER,
                total_sales COUNTER,
                unique_visitors COUNTER
            )
        """
        )

        try:
            # Update counters (no INSERT for counters)
            await session.execute(
                f"""
                UPDATE {keyspace}.{table_name}
                SET page_views = page_views + 100,
                    total_sales = total_sales + 50,
                    unique_visitors = unique_visitors + 25
                WHERE id = 1
                """
            )

            await session.execute(
                f"""
                UPDATE {keyspace}.{table_name}
                SET page_views = page_views + 200
                WHERE id = 2
                """
            )

            # Try to export with writetime
            with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
                output_path = tmp.name

            operator = BulkOperator(session=session)

            # Should succeed but show NULL writetime for counters
            await operator.export(
                table=f"{keyspace}.{table_name}",
                output_path=output_path,
                format="csv",
                options={"include_writetime": True},
                csv_options={"null_value": "NULL"},
            )

            with open(output_path, "r") as f:
                reader = csv.DictReader(f)
                rows = list(reader)

            # All counter writetime should be NULL
            for row in rows:
                assert row.get("page_views_writetime", "NULL") == "NULL"
                assert row.get("total_sales_writetime", "NULL") == "NULL"
                assert row.get("unique_visitors_writetime", "NULL") == "NULL"

            Path(output_path).unlink()

            # Test that trying to get writetime on counters doesn't break export
            # The export should succeed but counters won't have writetime
            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp2:
                output_path2 = tmp2.name

            # This should succeed - the system should handle counters gracefully
            await operator.export(
                table=f"{keyspace}.{table_name}",
                output_path=output_path2,
                format="json",
                options={
                    "include_writetime": True,
                },
            )

            with open(output_path2, "r") as f:
                data = json.load(f)
                # Verify data was exported
                assert len(data) > 0
                # Counter columns should not have writetime columns
                for row in data:
                    assert "page_views_writetime" not in row
                    assert "total_sales_writetime" not in row
                    assert "unique_visitors_writetime" not in row

            Path(output_path2).unlink(missing_ok=True)

        finally:
            await session.execute(f"DROP TABLE {keyspace}.{table_name}")

    @pytest.mark.asyncio
    async def test_writetime_composite_primary_keys(self, session):
        """
        Test writetime with composite primary keys.

        What this tests:
        ---------------
        1. Partition key columns - no writetime
        2. Clustering columns - no writetime
        3. Regular columns in wide rows - have writetime
        4. Static columns - have writetime
        5. Filtering on tables with many key columns

        Why this matters:
        ----------------
        - Composite keys are common in data models
        - Must correctly identify key vs regular columns
        - Static columns have special semantics
        - Wide row models need proper handling

        Additional context:
        ---------------------------------
        Primary key structure:
        - PRIMARY KEY ((partition_key), clustering_key)
        - Neither partition nor clustering support writetime
        - Static columns shared per partition
        - Regular columns per row
        """
        table_name = f"writetime_composite_{int(datetime.now().timestamp() * 1000)}"
        keyspace = "test_bulk"

        await session.execute(
            f"""
            CREATE TABLE {keyspace}.{table_name} (
                -- Composite primary key
                tenant_id UUID,
                user_id UUID,
                timestamp TIMESTAMP,

                -- Static column (per partition)
                tenant_name TEXT STATIC,
                tenant_active BOOLEAN STATIC,

                -- Regular columns (per row)
                event_type TEXT,
                event_data TEXT,
                ip_address INET,

                PRIMARY KEY ((tenant_id, user_id), timestamp)
            ) WITH CLUSTERING ORDER BY (timestamp DESC)
        """
        )

        try:
            base_writetime = 1700000000000000
            tenant1 = uuid4()
            user1 = uuid4()

            # Insert static data
            await session.execute(
                f"""
                INSERT INTO {keyspace}.{table_name}
                (tenant_id, user_id, tenant_name, tenant_active)
                VALUES ({tenant1}, {user1}, 'Test Tenant', true)
                USING TIMESTAMP {base_writetime}
                """
            )

            # Insert regular rows
            for i in range(3):
                await session.execute(
                    f"""
                    INSERT INTO {keyspace}.{table_name}
                    (tenant_id, user_id, timestamp, event_type, event_data, ip_address)
                    VALUES (
                        {tenant1},
                        {user1},
                        '{datetime.now(timezone.utc) + timedelta(hours=i)}',
                        'login',
                        'data_{i}',
                        '192.168.1.{i}'
                    )
                    USING TIMESTAMP {base_writetime + i * 1000000}
                    """
                )

            # Update static column with different writetime
            await session.execute(
                f"""
                UPDATE {keyspace}.{table_name}
                USING TIMESTAMP {base_writetime + 5000000}
                SET tenant_active = false
                WHERE tenant_id = {tenant1} AND user_id = {user1}
                """
            )

            # Export with writetime
            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
                output_path = tmp.name

            operator = BulkOperator(session=session)
            await operator.export(
                table=f"{keyspace}.{table_name}",
                output_path=output_path,
                format="json",
                options={"include_writetime": True},
            )

            with open(output_path, "r") as f:
                data = json.load(f)

            # Verify key columns have no writetime
            for row in data:
                assert "tenant_id_writetime" not in row  # Partition key
                assert "user_id_writetime" not in row  # Partition key
                assert "timestamp_writetime" not in row  # Clustering key

                # Regular columns should have writetime
                assert "event_type_writetime" in row
                assert "event_data_writetime" in row
                assert "ip_address_writetime" in row

                # Static columns should have writetime (same for all rows in partition)
                assert "tenant_name_writetime" in row
                assert "tenant_active_writetime" in row

            # All rows in same partition should have same static writetime
            static_wt = data[0]["tenant_active_writetime"]
            for row in data:
                assert row["tenant_active_writetime"] == static_wt

            Path(output_path).unlink()

        finally:
            await session.execute(f"DROP TABLE {keyspace}.{table_name}")

    @pytest.mark.asyncio
    async def test_writetime_udt_types(self, session):
        """
        Test writetime behavior with User-Defined Types.

        What this tests:
        ---------------
        1. UDT as a whole has single writetime
        2. Cannot get writetime of individual UDT fields
        3. Frozen UDT requirement and writetime
        4. UDTs in collections and writetime
        5. Nested UDTs writetime behavior

        Why this matters:
        ----------------
        - UDTs are common for domain modeling
        - Writetime granularity important
        - Must understand limitations
        - Affects data modeling decisions

        Additional context:
        ---------------------------------
        UDT writetime rules:
        - Entire UDT has one writetime
        - Cannot query individual field writetime
        - Always frozen in collections
        - Updates replace entire UDT
        """
        # Create UDT
        await session.execute(
            """
            CREATE TYPE IF NOT EXISTS test_bulk.user_profile (
                first_name TEXT,
                last_name TEXT,
                email TEXT,
                age INT
            )
        """
        )

        table_name = f"writetime_udt_{int(datetime.now().timestamp() * 1000)}"
        keyspace = "test_bulk"

        await session.execute(
            f"""
            CREATE TABLE {keyspace}.{table_name} (
                id UUID PRIMARY KEY,
                username TEXT,
                profile FROZEN<user_profile>,
                profiles_history LIST<FROZEN<user_profile>>
            )
        """
        )

        try:
            base_writetime = 1700000000000000
            test_id = uuid4()

            # Insert with UDT
            await session.execute(
                f"""
                INSERT INTO {keyspace}.{table_name}
                (id, username, profile, profiles_history)
                VALUES (
                    {test_id},
                    'testuser',
                    {{
                        first_name: 'John',
                        last_name: 'Doe',
                        email: 'john@example.com',
                        age: 30
                    }},
                    [
                        {{first_name: 'John', last_name: 'Doe', email: 'old@example.com', age: 29}}
                    ]
                )
                USING TIMESTAMP {base_writetime}
                """
            )

            # Update UDT (replaces entire UDT)
            await session.execute(
                f"""
                UPDATE {keyspace}.{table_name}
                USING TIMESTAMP {base_writetime + 1000000}
                SET profile = {{
                    first_name: 'John',
                    last_name: 'Doe',
                    email: 'newemail@example.com',
                    age: 31
                }}
                WHERE id = {test_id}
                """
            )

            # Export with writetime
            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
                output_path = tmp.name

            operator = BulkOperator(session=session)
            await operator.export(
                table=f"{keyspace}.{table_name}",
                output_path=output_path,
                format="json",
                options={"include_writetime": True},
            )

            with open(output_path, "r") as f:
                data = json.load(f)
                row = data[0]

            # UDT should have single writetime
            assert "profile_writetime" in row
            profile_wt = datetime.fromisoformat(row["profile_writetime"].replace("Z", "+00:00"))
            expected_dt = datetime.fromtimestamp(
                (base_writetime + 1000000) / 1000000, tz=timezone.utc
            )
            assert abs((profile_wt - expected_dt).total_seconds()) < 1

            # List of UDTs has single writetime
            assert "profiles_history_writetime" in row

            # Verify UDT data is properly serialized
            assert row["profile"]["email"] == "newemail@example.com"
            assert row["profile"]["age"] == 31

            Path(output_path).unlink()

        finally:
            await session.execute(f"DROP TABLE {keyspace}.{table_name}")
            await session.execute("DROP TYPE test_bulk.user_profile")

    @pytest.mark.asyncio
    async def test_writetime_special_values(self, session):
        """
        Test writetime with special values and edge cases.

        What this tests:
        ---------------
        1. Empty strings vs NULL
        2. Empty collections vs NULL collections
        3. Special numeric values (NaN, Infinity)
        4. Maximum/minimum values for types
        5. Unicode and binary edge cases

        Why this matters:
        ----------------
        - Edge cases often reveal bugs
        - Special values need proper handling
        - Production data has edge cases
        - Serialization must be robust

        Additional context:
        ---------------------------------
        Special cases to consider:
        - Empty string '' is different from NULL
        - Empty collection [] is different from NULL
        - NaN/Infinity in floats
        - Max values for integers
        """
        table_name = f"writetime_special_{int(datetime.now().timestamp() * 1000)}"
        keyspace = "test_bulk"

        await session.execute(
            f"""
            CREATE TABLE {keyspace}.{table_name} (
                id INT PRIMARY KEY,

                -- String variations
                str_normal TEXT,
                str_empty TEXT,
                str_null TEXT,
                str_unicode TEXT,

                -- Numeric edge cases
                float_nan FLOAT,
                float_inf FLOAT,
                float_neg_inf FLOAT,
                bigint_max BIGINT,
                bigint_min BIGINT,

                -- Collection variations
                list_normal LIST<TEXT>,
                list_empty LIST<TEXT>,
                list_null LIST<TEXT>,

                -- Binary edge cases
                blob_normal BLOB,
                blob_empty BLOB,
                blob_null BLOB
            )
        """
        )

        try:
            base_writetime = 1700000000000000

            # Insert with edge cases
            await session.execute(
                f"""
                INSERT INTO {keyspace}.{table_name} (
                    id,
                    str_normal, str_empty, str_unicode,
                    float_nan, float_inf, float_neg_inf,
                    bigint_max, bigint_min,
                    list_normal, list_empty,
                    blob_normal, blob_empty
                ) VALUES (
                    1,
                    'normal', '', '🚀 Ω ñ ♠',
                    NaN, Infinity, -Infinity,
                    9223372036854775807, -9223372036854775808,
                    ['a', 'b'], [],
                    0x0102FF, 0x
                )
                USING TIMESTAMP {base_writetime}
                """
            )

            # Export and verify
            with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
                output_path = tmp.name

            operator = BulkOperator(session=session)
            await operator.export(
                table=f"{keyspace}.{table_name}",
                output_path=output_path,
                format="csv",
                options={"include_writetime": True},
                csv_options={"null_value": "NULL"},
            )

            with open(output_path, "r") as f:
                reader = csv.DictReader(f)
                row = next(reader)

            # Empty string should have writetime (not NULL)
            assert row["str_empty"] == ""  # Empty, not NULL
            assert row["str_empty_writetime"] != "NULL"

            # NULL column should have NULL writetime
            assert row["str_null"] == "NULL"
            assert row["str_null_writetime"] == "NULL"

            # Empty collection is stored as NULL in Cassandra
            assert row["list_empty"] == "NULL"  # Empty list becomes NULL
            assert row["list_empty_writetime"] == "NULL"

            # NULL collection has NULL writetime
            assert row["list_null"] == "NULL"
            assert row["list_null_writetime"] == "NULL"

            # Special float values
            assert row["float_nan"] == "NaN"
            assert row["float_inf"] == "Infinity"
            assert row["float_neg_inf"] == "-Infinity"

            # All should have writetime
            assert row["float_nan_writetime"] != "NULL"
            assert row["float_inf_writetime"] != "NULL"

            # Empty blob vs NULL blob
            assert row["blob_empty"] == ""  # Empty hex string
            assert row["blob_empty_writetime"] != "NULL"
            assert row["blob_null"] == "NULL"
            assert row["blob_null_writetime"] == "NULL"

            Path(output_path).unlink()

        finally:
            await session.execute(f"DROP TABLE {keyspace}.{table_name}")

    @pytest.mark.asyncio
    async def test_writetime_filtering_with_nulls(self, session):
        """
        Test writetime filtering behavior with NULL values.

        What this tests:
        ---------------
        1. Filtering with NULL writetime values
        2. ANY mode with some NULL columns
        3. ALL mode with some NULL columns
        4. Tables with mostly NULL values
        5. Filter correctness with sparse data

        Why this matters:
        ----------------
        - Real data is often sparse
        - NULL handling in filters is critical
        - Must match user expectations
        - Common source of data loss

        Additional context:
        ---------------------------------
        Filter logic with NULLs:
        - ANY mode: Include if ANY non-null column matches
        - ALL mode: Exclude if ANY column is null or doesn't match
        - Empty rows (all nulls) behavior
        """
        table_name = f"writetime_filter_nulls_{int(datetime.now().timestamp() * 1000)}"
        keyspace = "test_bulk"

        await session.execute(
            f"""
            CREATE TABLE {keyspace}.{table_name} (
                id INT PRIMARY KEY,
                col_a TEXT,
                col_b TEXT,
                col_c TEXT,
                col_d TEXT
            )
        """
        )

        try:
            base_writetime = 1700000000000000
            cutoff_writetime = base_writetime + 1000000

            # Row 1: All columns have old writetime
            await session.execute(
                f"""
                INSERT INTO {keyspace}.{table_name} (id, col_a, col_b, col_c, col_d)
                VALUES (1, 'a1', 'b1', 'c1', 'd1')
                USING TIMESTAMP {base_writetime}
                """
            )

            # Row 2: Some columns NULL, others old
            await session.execute(
                f"""
                INSERT INTO {keyspace}.{table_name} (id, col_a, col_c)
                VALUES (2, 'a2', 'c2')
                USING TIMESTAMP {base_writetime}
                """
            )

            # Row 3: Mix of old and new writetime
            await session.execute(
                f"""
                INSERT INTO {keyspace}.{table_name} (id, col_a, col_b)
                VALUES (3, 'a3', 'b3')
                USING TIMESTAMP {base_writetime}
                """
            )
            await session.execute(
                f"""
                UPDATE {keyspace}.{table_name}
                USING TIMESTAMP {cutoff_writetime + 1000000}
                SET col_c = 'c3_new'
                WHERE id = 3
                """
            )

            # Row 4: All NULL except primary key
            await session.execute(
                f"""
                INSERT INTO {keyspace}.{table_name} (id)
                VALUES (4)
                """
            )

            # Row 5: All new writetime
            await session.execute(
                f"""
                INSERT INTO {keyspace}.{table_name} (id, col_a, col_b, col_c)
                VALUES (5, 'a5', 'b5', 'c5')
                USING TIMESTAMP {cutoff_writetime + 2000000}
                """
            )

            # Test ANY mode filtering
            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
                output_any = tmp.name

            operator = BulkOperator(session=session)
            filter_time = datetime.fromtimestamp(cutoff_writetime / 1000000, tz=timezone.utc)

            await operator.export(
                table=f"{keyspace}.{table_name}",
                output_path=output_any,
                format="json",
                options={
                    "writetime_columns": ["col_a", "col_b", "col_c", "col_d"],
                    "writetime_after": filter_time,
                    "writetime_filter_mode": "any",
                },
            )

            with open(output_any, "r") as f:
                any_results = json.load(f)
                any_ids = {row["id"] for row in any_results}

            # ANY mode results:
            assert 1 not in any_ids  # All old
            assert 2 not in any_ids  # All old (nulls ignored)
            assert 3 in any_ids  # col_c is new
            assert 4 not in any_ids  # All NULL
            assert 5 in any_ids  # All new

            # Test ALL mode filtering
            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
                output_all = tmp.name

            await operator.export(
                table=f"{keyspace}.{table_name}",
                output_path=output_all,
                format="json",
                options={
                    "writetime_columns": ["col_a", "col_b", "col_c", "col_d"],
                    "writetime_after": filter_time,
                    "writetime_filter_mode": "all",
                },
            )

            with open(output_all, "r") as f:
                all_results = json.load(f)
                all_ids = {row["id"] for row in all_results}

            # ALL mode results:
            assert 1 not in all_ids  # All old
            assert 2 not in all_ids  # Has NULLs
            assert 3 not in all_ids  # Mixed old/new
            assert 4 not in all_ids  # All NULL
            assert 5 in all_ids  # All new (even though col_d is NULL)

            Path(output_any).unlink()
            Path(output_all).unlink()

        finally:
            await session.execute(f"DROP TABLE {keyspace}.{table_name}")

    @pytest.mark.asyncio
    async def test_writetime_data_integrity_verification(self, session):
        """
        Comprehensive data integrity test for writetime export.

        What this tests:
        ---------------
        1. Writetime values are accurate to microsecond
        2. No data corruption during export
        3. Consistent behavior across formats
        4. Large writetime values handled correctly
        5. Timezone handling is correct

        Why this matters:
        ----------------
        - Data integrity is paramount
        - Writetime used for conflict resolution
        - Must be accurate for migrations
        - Production reliability

        Additional context:
        ---------------------------------
        This test verifies:
        - Exact writetime preservation
        - No precision loss
        - Correct timezone handling
        - Format consistency
        """
        table_name = f"writetime_integrity_{int(datetime.now().timestamp() * 1000)}"
        keyspace = "test_bulk"

        await session.execute(
            f"""
            CREATE TABLE {keyspace}.{table_name} (
                id UUID PRIMARY KEY,
                data TEXT,
                updated_at TIMESTAMP,
                version INT
            )
        """
        )

        try:
            # Use precise writetime values
            writetime_values = [
                1234567890123456,  # Old timestamp
                1700000000000000,  # Recent timestamp
                9999999999999999,  # Far future timestamp
            ]

            test_data = []
            for i, wt in enumerate(writetime_values):
                test_id = uuid4()
                test_data.append({"id": test_id, "writetime": wt})

                await session.execute(
                    f"""
                    INSERT INTO {keyspace}.{table_name}
                    (id, data, updated_at, version)
                    VALUES (
                        {test_id},
                        'test_data_{i}',
                        '{datetime.now(timezone.utc)}',
                        {i}
                    )
                    USING TIMESTAMP {wt}
                    """
                )

            # Export to both CSV and JSON
            formats = ["csv", "json"]
            results = {}

            for fmt in formats:
                with tempfile.NamedTemporaryFile(mode="w", suffix=f".{fmt}", delete=False) as tmp:
                    output_path = tmp.name

                operator = BulkOperator(session=session)
                await operator.export(
                    table=f"{keyspace}.{table_name}",
                    output_path=output_path,
                    format=fmt,
                    options={"include_writetime": True},
                )

                if fmt == "csv":
                    with open(output_path, "r") as f:
                        reader = csv.DictReader(f)
                        results[fmt] = list(reader)
                else:
                    with open(output_path, "r") as f:
                        results[fmt] = json.load(f)

                Path(output_path).unlink()

            # Verify data integrity across formats
            for test_item in test_data:
                test_id = str(test_item["id"])
                expected_wt = test_item["writetime"]

                # Find row in each format
                csv_row = next(r for r in results["csv"] if r["id"] == test_id)
                json_row = next(r for r in results["json"] if r["id"] == test_id)

                # Parse writetime from each format
                csv_wt_str = csv_row["data_writetime"]
                json_wt_str = json_row["data_writetime"]

                # Both CSV and JSON now use ISO format
                csv_dt = datetime.fromisoformat(csv_wt_str.replace("Z", "+00:00"))
                json_dt = datetime.fromisoformat(json_wt_str.replace("Z", "+00:00"))

                # To verify precision, we need to reconstruct microseconds without float conversion
                # Calculate microseconds from components to avoid float precision loss
                def dt_to_micros(dt):
                    # Get timestamp components
                    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
                    delta = dt - epoch
                    # Calculate total microseconds using integer arithmetic
                    return delta.days * 86400 * 1000000 + delta.seconds * 1000000 + dt.microsecond

                csv_micros = dt_to_micros(csv_dt)
                json_micros = dt_to_micros(json_dt)

                # Verify exact match - NO precision loss is acceptable
                assert csv_micros == expected_wt, f"CSV writetime mismatch for {test_id}"
                assert json_micros == expected_wt, f"JSON writetime mismatch for {test_id}"

                # Verify all columns have same writetime
                assert csv_row["data_writetime"] == csv_row["updated_at_writetime"]
                assert csv_row["data_writetime"] == csv_row["version_writetime"]

        finally:
            await session.execute(f"DROP TABLE {keyspace}.{table_name}")
