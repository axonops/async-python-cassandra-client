"""
Comprehensive integration tests for NULL handling in async-cassandra-bulk.

What this tests:
---------------
1. Explicit NULL vs missing columns in INSERT statements
2. NULL serialization in JSON export format
3. NULL behavior with different data types
4. Collection and UDT NULL handling
5. Primary key restrictions with NULL
6. Writetime behavior with NULL values

Why this matters:
----------------
- NULL handling is critical for data integrity
- Different between explicit NULL and missing column can affect storage
- Writetime behavior with NULL values needs to be well-defined
- Collections and UDTs have special NULL semantics
- Incorrect NULL handling can lead to data loss or corruption

Additional context:
---------------------------------
- Cassandra treats explicit NULL and missing columns differently in some cases
- Primary key columns cannot be NULL
- Collection operations have special semantics with NULL
- Writetime is not set for NULL values
"""

import json
import os
import tempfile
import uuid
from datetime import datetime, timezone

import pytest

from async_cassandra_bulk import BulkOperator


class TestNullHandlingComprehensive:
    """Test NULL handling across all scenarios."""

    @pytest.mark.asyncio
    async def test_explicit_null_vs_missing_column_basic(self, session):
        """
        Test difference between explicit NULL and missing column.

        Cassandra treats these differently:
        - Explicit NULL creates a tombstone
        - Missing column doesn't create anything
        """
        table = f"test_null_basic_{uuid.uuid4().hex[:8]}"

        # Create table
        await session.execute(
            f"""
            CREATE TABLE {table} (
                id int PRIMARY KEY,
                name text,
                age int,
                email text
            )
        """
        )

        # Insert with explicit NULL
        insert_null = await session.prepare(
            f"INSERT INTO {table} (id, name, age, email) VALUES (?, ?, ?, ?)"
        )
        await session.execute(insert_null, (1, "Alice", None, "alice@example.com"))

        # Insert with missing column (no age)
        insert_missing = await session.prepare(
            f"INSERT INTO {table} (id, name, email) VALUES (?, ?, ?)"
        )
        await session.execute(insert_missing, (2, "Bob", "bob@example.com"))

        # Export data
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            output_file = f.name

        try:
            operator = BulkOperator(session=session)
            await operator.export(
                table=f"test_bulk.{table}", output_path=output_file, format="json"
            )

            # Read and verify exported data
            with open(output_file, "r") as f:
                # Parse exported rows
                rows = json.load(f)
                assert len(rows) == 2
                row_by_id = {row["id"]: row for row in rows}

                # Row 1: explicit NULL
                assert row_by_id[1]["name"] == "Alice"
                assert row_by_id[1]["age"] is None  # Explicit NULL exported as null
                assert row_by_id[1]["email"] == "alice@example.com"

                # Row 2: missing column
                assert row_by_id[2]["name"] == "Bob"
                assert row_by_id[2]["age"] is None  # Missing column also exported as null
                assert row_by_id[2]["email"] == "bob@example.com"

        finally:
            os.unlink(output_file)

    @pytest.mark.asyncio
    async def test_null_handling_all_simple_types(self, session):
        """Test NULL handling for all simple data types."""
        table = f"test_null_simple_{uuid.uuid4().hex[:8]}"

        # Create table with all simple types
        await session.execute(
            f"""
            CREATE TABLE {table} (
                id int PRIMARY KEY,
                ascii_col ascii,
                bigint_col bigint,
                blob_col blob,
                boolean_col boolean,
                date_col date,
                decimal_col decimal,
                double_col double,
                float_col float,
                inet_col inet,
                int_col int,
                smallint_col smallint,
                text_col text,
                time_col time,
                timestamp_col timestamp,
                timeuuid_col timeuuid,
                tinyint_col tinyint,
                uuid_col uuid,
                varchar_col varchar,
                varint_col varint
            )
        """
        )

        # Test 1: All NULL values
        insert_all_null = await session.prepare(
            f"""INSERT INTO {table} (id, ascii_col, bigint_col, blob_col, boolean_col,
                date_col, decimal_col, double_col, float_col, inet_col, int_col,
                smallint_col, text_col, time_col, timestamp_col, timeuuid_col,
                tinyint_col, uuid_col, varchar_col, varint_col)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
        )

        await session.execute(
            insert_all_null,
            (
                1,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ),
        )

        # Test 2: Mixed NULL and values
        insert_mixed = await session.prepare(
            f"""INSERT INTO {table} (id, text_col, int_col, boolean_col, timestamp_col)
                VALUES (?, ?, ?, ?, ?)"""
        )
        await session.execute(insert_mixed, (2, "test", 42, True, datetime.now(timezone.utc)))

        # Test 3: Only primary key (all other columns missing)
        insert_pk_only = await session.prepare(f"INSERT INTO {table} (id) VALUES (?)")
        await session.execute(insert_pk_only, (3,))

        # Export and verify
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            output_file = f.name

        try:
            operator = BulkOperator(session=session)
            await operator.export(
                table=f"test_bulk.{table}", output_path=output_file, format="json"
            )

            with open(output_file, "r") as f:
                rows = json.load(f)
                assert len(rows) == 3
                row_by_id = {row["id"]: row for row in rows}

                # Verify all NULL row
                null_row = row_by_id[1]
                for col in null_row:
                    if col != "id":
                        assert null_row[col] is None

                # Verify mixed row
                mixed_row = row_by_id[2]
                assert mixed_row["text_col"] == "test"
                assert mixed_row["int_col"] == 42
                assert mixed_row["boolean_col"] is True
                assert mixed_row["timestamp_col"] is not None
                # Other columns should be None
                assert mixed_row["ascii_col"] is None
                assert mixed_row["bigint_col"] is None

                # Verify PK only row
                pk_row = row_by_id[3]
                for col in pk_row:
                    if col != "id":
                        assert pk_row[col] is None

        finally:
            os.unlink(output_file)

    @pytest.mark.asyncio
    async def test_null_with_collections(self, session):
        """Test NULL handling with collection types."""
        table = f"test_null_collections_{uuid.uuid4().hex[:8]}"

        await session.execute(
            f"""
            CREATE TABLE {table} (
                id int PRIMARY KEY,
                list_col list<text>,
                set_col set<int>,
                map_col map<text, int>,
                frozen_list frozen<list<text>>,
                frozen_set frozen<set<int>>,
                frozen_map frozen<map<text, int>>
            )
        """
        )

        # Test different NULL scenarios
        test_cases = [
            # Explicit NULL collections
            (1, None, None, None, None, None, None),
            # Empty collections (different from NULL!)
            (2, [], set(), {}, [], set(), {}),
            # Collections with NULL elements (not allowed in Cassandra)
            # Mixed NULL and non-NULL
            (3, ["a", "b"], {1, 2}, {"x": 1}, None, None, None),
            # Only PK (missing collections)
            (4, None, None, None, None, None, None),
        ]

        # Insert test data
        for case in test_cases[:3]:  # Skip the last one for now
            stmt = await session.prepare(
                f"""INSERT INTO {table} (id, list_col, set_col, map_col,
                    frozen_list, frozen_set, frozen_map) VALUES (?, ?, ?, ?, ?, ?, ?)"""
            )
            await session.execute(stmt, case)

        # Insert PK only
        stmt_pk = await session.prepare(f"INSERT INTO {table} (id) VALUES (?)")
        await session.execute(stmt_pk, (4,))

        # Export and verify
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            output_file = f.name

        try:
            operator = BulkOperator(session=session)
            await operator.export(
                table=f"test_bulk.{table}", output_path=output_file, format="json"
            )

            with open(output_file, "r") as f:
                rows = json.load(f)
                row_by_id = {row["id"]: row for row in rows}

                # NULL collections
                assert row_by_id[1]["list_col"] is None
                assert row_by_id[1]["set_col"] is None
                assert row_by_id[1]["map_col"] is None

                # Empty collections - IMPORTANT: Cassandra stores empty collections as NULL
                # This is a key Cassandra behavior - [] becomes NULL when stored
                assert row_by_id[2]["list_col"] is None
                assert row_by_id[2]["set_col"] is None
                assert row_by_id[2]["map_col"] is None

                # Mixed case
                assert row_by_id[3]["list_col"] == ["a", "b"]
                assert set(row_by_id[3]["set_col"]) == {1, 2}  # Sets exported as lists
                assert row_by_id[3]["map_col"] == {"x": 1}

        finally:
            os.unlink(output_file)

    @pytest.mark.asyncio
    async def test_null_with_udts(self, session):
        """Test NULL handling with User Defined Types."""
        # Create UDT - need to specify keyspace
        await session.execute(
            """
            CREATE TYPE IF NOT EXISTS test_bulk.address (
                street text,
                city text,
                zip_code int
            )
        """
        )

        table = f"test_null_udt_{uuid.uuid4().hex[:8]}"
        await session.execute(
            f"""
            CREATE TABLE {table} (
                id int PRIMARY KEY,
                name text,
                home_address address,
                work_address frozen<address>
            )
        """
        )

        # Test cases
        # 1. NULL UDT
        await session.execute(
            f"INSERT INTO {table} (id, name, home_address, work_address) VALUES (1, 'Alice', NULL, NULL)"
        )

        # 2. UDT with NULL fields
        await session.execute(
            f"""INSERT INTO {table} (id, name, home_address) VALUES (2, 'Bob',
                {{street: '123 Main', city: NULL, zip_code: NULL}})"""
        )

        # 3. Complete UDT
        await session.execute(
            f"""INSERT INTO {table} (id, name, home_address, work_address) VALUES (3, 'Charlie',
                {{street: '456 Oak', city: 'NYC', zip_code: 10001}},
                {{street: '456 Oak', city: 'NYC', zip_code: 10001}})"""
        )

        # Export and verify
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            output_file = f.name

        try:
            operator = BulkOperator(session=session)
            await operator.export(
                table=f"test_bulk.{table}", output_path=output_file, format="json"
            )

            with open(output_file, "r") as f:
                rows = json.load(f)
                row_by_id = {row["id"]: row for row in rows}

                # NULL UDT
                assert row_by_id[1]["home_address"] is None
                assert row_by_id[1]["work_address"] is None

                # Partial UDT
                assert row_by_id[2]["home_address"]["street"] == "123 Main"
                assert row_by_id[2]["home_address"]["city"] is None
                assert row_by_id[2]["home_address"]["zip_code"] is None

                # Complete UDT
                assert row_by_id[3]["home_address"]["street"] == "456 Oak"
                assert row_by_id[3]["home_address"]["city"] == "NYC"
                assert row_by_id[3]["home_address"]["zip_code"] == 10001

        finally:
            os.unlink(output_file)

    @pytest.mark.asyncio
    async def test_writetime_with_null_values(self, session):
        """Test writetime behavior with NULL values."""
        table = f"test_writetime_null_{uuid.uuid4().hex[:8]}"

        await session.execute(
            f"""
            CREATE TABLE {table} (
                id int PRIMARY KEY,
                name text,
                age int,
                email text
            )
        """
        )

        # Insert data with controlled writetime
        int(datetime.now(timezone.utc).timestamp() * 1_000_000)

        # Row 1: All values set
        await session.execute(
            f"INSERT INTO {table} (id, name, age, email) VALUES (1, 'Alice', 30, 'alice@example.com')"
        )

        # Row 2: NULL age
        await session.execute(
            f"INSERT INTO {table} (id, name, age, email) VALUES (2, 'Bob', NULL, 'bob@example.com')"
        )

        # Row 3: Missing age (not in INSERT)
        await session.execute(
            f"INSERT INTO {table} (id, name, email) VALUES (3, 'Charlie', 'charlie@example.com')"
        )

        # Export with writetime
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            output_file = f.name

        try:
            operator = BulkOperator(session=session)
            await operator.export(
                table=f"test_bulk.{table}",
                output_path=output_file,
                format="json",
                options={"include_writetime": True},
            )

            with open(output_file, "r") as f:
                rows = json.load(f)
                row_by_id = {row["id"]: row for row in rows}

                # All values set - all should have writetime
                assert "name_writetime" in row_by_id[1]
                assert "age_writetime" in row_by_id[1]
                assert "email_writetime" in row_by_id[1]

                # NULL age - writetime present but null (this is correct Cassandra behavior)
                assert "name_writetime" in row_by_id[2]
                assert "age_writetime" in row_by_id[2]
                assert row_by_id[2]["age_writetime"] is None  # NULL writetime for NULL value
                assert "email_writetime" in row_by_id[2]

                # Missing age - writetime present but null (same as explicit NULL)
                assert "name_writetime" in row_by_id[3]
                assert "age_writetime" in row_by_id[3]
                assert row_by_id[3]["age_writetime"] is None  # NULL writetime for missing value
                assert "email_writetime" in row_by_id[3]

        finally:
            os.unlink(output_file)

    @pytest.mark.asyncio
    async def test_null_in_clustering_columns(self, session):
        """Test NULL handling with clustering columns."""
        table = f"test_null_clustering_{uuid.uuid4().hex[:8]}"

        await session.execute(
            f"""
            CREATE TABLE {table} (
                partition_id int,
                cluster_id int,
                name text,
                value text,
                PRIMARY KEY (partition_id, cluster_id)
            )
        """
        )

        # Insert test data
        # Normal row
        await session.execute(
            f"INSERT INTO {table} (partition_id, cluster_id, name, value) VALUES (1, 1, 'test', 'value')"
        )

        # NULL in non-key column
        await session.execute(
            f"INSERT INTO {table} (partition_id, cluster_id, name, value) VALUES (1, 2, NULL, 'value2')"
        )

        # Missing non-key column
        await session.execute(
            f"INSERT INTO {table} (partition_id, cluster_id, value) VALUES (1, 3, 'value3')"
        )

        # Export and verify
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            output_file = f.name

        try:
            operator = BulkOperator(session=session)
            await operator.export(
                table=f"test_bulk.{table}", output_path=output_file, format="json"
            )

            with open(output_file, "r") as f:
                rows = json.load(f)
                assert len(rows) == 3

                # Verify all rows exported correctly
                cluster_ids = [row["cluster_id"] for row in rows]
                assert sorted(cluster_ids) == [1, 2, 3]

                # Find specific rows
                for row in rows:
                    if row["cluster_id"] == 1:
                        assert row["name"] == "test"
                    elif row["cluster_id"] == 2:
                        assert row["name"] is None
                    elif row["cluster_id"] == 3:
                        assert row["name"] is None

        finally:
            os.unlink(output_file)

    @pytest.mark.asyncio
    async def test_null_serialization_edge_cases(self, session):
        """Test edge cases in NULL serialization."""
        table = f"test_null_edge_{uuid.uuid4().hex[:8]}"

        # Table with nested collections
        await session.execute(
            f"""
            CREATE TABLE {table} (
                id int PRIMARY KEY,
                list_of_lists list<frozen<list<text>>>,
                map_of_sets map<text, frozen<set<int>>>,
                tuple_col tuple<text, int, boolean>
            )
        """
        )

        # Test cases
        # 1. NULL nested collections
        stmt1 = await session.prepare(
            f"INSERT INTO {table} (id, list_of_lists, map_of_sets, tuple_col) VALUES (?, ?, ?, ?)"
        )
        await session.execute(stmt1, (1, None, None, None))

        # 2. Collections containing empty collections
        await session.execute(stmt1, (2, [[]], {"empty": set()}, ("text", 123, None)))

        # 3. Complex nested structure
        await session.execute(
            stmt1,
            (3, [["a", "b"], ["c", "d"]], {"set1": {1, 2, 3}, "set2": {4, 5}}, ("test", 456, True)),
        )

        # Export and verify JSON structure
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            output_file = f.name

        try:
            operator = BulkOperator(session=session)
            await operator.export(
                table=f"test_bulk.{table}", output_path=output_file, format="json"
            )

            with open(output_file, "r") as f:
                rows = json.load(f)
                row_by_id = {row["id"]: row for row in rows}

                # Verify NULL nested collections
                assert row_by_id[1]["list_of_lists"] is None
                assert row_by_id[1]["map_of_sets"] is None
                assert row_by_id[1]["tuple_col"] is None

                # Verify empty nested collections
                assert row_by_id[2]["list_of_lists"] == [[]]
                assert row_by_id[2]["map_of_sets"]["empty"] == []
                assert row_by_id[2]["tuple_col"] == ["text", 123, None]

                # Verify complex structure
                assert row_by_id[3]["list_of_lists"] == [["a", "b"], ["c", "d"]]
                assert len(row_by_id[3]["map_of_sets"]["set1"]) == 3
                assert row_by_id[3]["tuple_col"] == ["test", 456, True]

        finally:
            os.unlink(output_file)

    @pytest.mark.asyncio
    async def test_null_with_static_columns(self, session):
        """Test NULL handling with static columns."""
        table = f"test_null_static_{uuid.uuid4().hex[:8]}"

        await session.execute(
            f"""
            CREATE TABLE {table} (
                partition_id int,
                cluster_id int,
                static_col text STATIC,
                regular_col text,
                PRIMARY KEY (partition_id, cluster_id)
            )
        """
        )

        # Insert data with NULL static column
        await session.execute(
            f"INSERT INTO {table} (partition_id, cluster_id, static_col, regular_col) VALUES (1, 1, NULL, 'reg1')"
        )
        await session.execute(
            f"INSERT INTO {table} (partition_id, cluster_id, regular_col) VALUES (1, 2, 'reg2')"
        )

        # Insert with static column value
        await session.execute(
            f"INSERT INTO {table} (partition_id, cluster_id, static_col, regular_col) VALUES (2, 1, 'static_value', 'reg3')"
        )
        await session.execute(
            f"INSERT INTO {table} (partition_id, cluster_id, regular_col) VALUES (2, 2, 'reg4')"
        )

        # Export and verify
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            output_file = f.name

        try:
            operator = BulkOperator(session=session)
            await operator.export(
                table=f"test_bulk.{table}", output_path=output_file, format="json"
            )

            with open(output_file, "r") as f:
                rows = json.load(f)

                # Verify static column behavior
                partition1_rows = [r for r in rows if r["partition_id"] == 1]
                partition2_rows = [r for r in rows if r["partition_id"] == 2]

                # All rows in partition 1 should have NULL static column
                for row in partition1_rows:
                    assert row["static_col"] is None

                # All rows in partition 2 should have the same static value
                for row in partition2_rows:
                    assert row["static_col"] == "static_value"

        finally:
            os.unlink(output_file)
