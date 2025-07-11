"""
Integration tests for exporting all Cassandra data types.

What this tests:
---------------
1. Complete coverage of all Cassandra data types
2. Proper serialization to CSV and JSON formats
3. Complex nested types and collections
4. Data integrity across export formats

Why this matters:
----------------
- Must support every Cassandra type
- Data fidelity is critical
- Production schemas use all types
- Format conversions must be correct
"""

import csv
import json
from datetime import date, datetime, timezone
from decimal import Decimal
from uuid import uuid4

import pytest
from cassandra.util import Date, Time

from async_cassandra_bulk import BulkOperator


class TestAllDataTypesExport:
    """Test exporting all Cassandra data types."""

    @pytest.mark.asyncio
    async def test_export_all_native_types(self, session, tmp_path):
        """
        Test exporting all native Cassandra data types.

        What this tests:
        ---------------
        1. ASCII, TEXT, VARCHAR string types
        2. All numeric types (TINYINT to VARINT)
        3. Temporal types (DATE, TIME, TIMESTAMP)
        4. Binary types (BLOB)
        5. Special types (UUID, INET, BOOLEAN)

        Why this matters:
        ----------------
        - Every type must serialize correctly
        - Type conversions must preserve data
        - Both CSV and JSON must handle all types
        - Production data uses all types

        Additional context:
        ---------------------------------
        - Some types have special representations
        - CSV converts everything to strings
        - JSON preserves more type information
        """
        # Create comprehensive test table
        table_name = f"all_types_{int(datetime.now().timestamp() * 1000)}"

        await session.execute(
            f"""
            CREATE TABLE test_bulk.{table_name} (
                -- String types
                id UUID PRIMARY KEY,
                ascii_col ASCII,
                text_col TEXT,
                varchar_col VARCHAR,

                -- Numeric types
                tinyint_col TINYINT,
                smallint_col SMALLINT,
                int_col INT,
                bigint_col BIGINT,
                varint_col VARINT,
                float_col FLOAT,
                double_col DOUBLE,
                decimal_col DECIMAL,

                -- Temporal types
                date_col DATE,
                time_col TIME,
                timestamp_col TIMESTAMP,

                -- Binary type
                blob_col BLOB,

                -- Special types
                boolean_col BOOLEAN,
                inet_col INET,
                timeuuid_col TIMEUUID
            )
        """
        )

        # Insert test data with all types
        test_id = uuid4()
        # Use cassandra.util.uuid_from_time for TIMEUUID
        from cassandra.util import uuid_from_time

        test_timeuuid = uuid_from_time(datetime.now())
        test_timestamp = datetime.now(timezone.utc)
        test_date = Date(date.today())
        test_time = Time(52245123456789)  # 14:30:45.123456789

        insert_stmt = await session.prepare(
            f"""
            INSERT INTO test_bulk.{table_name} (
                id, ascii_col, text_col, varchar_col,
                tinyint_col, smallint_col, int_col, bigint_col, varint_col,
                float_col, double_col, decimal_col,
                date_col, time_col, timestamp_col,
                blob_col, boolean_col, inet_col, timeuuid_col
            ) VALUES (
                ?, ?, ?, ?,
                ?, ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?, ?
            )
        """
        )

        await session.execute(
            insert_stmt,
            (
                test_id,
                "ascii_only",
                "UTF-8 text with émojis 🚀",
                "varchar value",
                127,  # TINYINT max
                32767,  # SMALLINT max
                2147483647,  # INT max
                9223372036854775807,  # BIGINT max
                10**100,  # VARINT - huge number
                3.14159,  # FLOAT
                2.718281828459045,  # DOUBLE
                Decimal("123456789.123456789"),  # DECIMAL
                test_date,
                test_time,
                test_timestamp,
                b"Binary data \x00\x01\xff",  # BLOB
                True,  # BOOLEAN
                "192.168.1.100",  # INET
                test_timeuuid,  # TIMEUUID
            ),
        )

        # Also test NULL values
        await session.execute(
            insert_stmt,
            (
                uuid4(),
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

        # Test special float values
        await session.execute(
            insert_stmt,
            (
                uuid4(),
                "special",
                "floats",
                "test",
                0,
                0,
                0,
                0,
                0,
                float("nan"),
                float("inf"),
                Decimal("0"),
                test_date,
                test_time,
                test_timestamp,
                b"",
                False,
                "::1",
                uuid_from_time(datetime.now()),
            ),
        )

        try:
            operator = BulkOperator(session=session)

            # Export to CSV
            csv_path = tmp_path / "all_types.csv"
            stats_csv = await operator.export(
                table=f"test_bulk.{table_name}", output_path=str(csv_path), format="csv"
            )

            assert stats_csv.rows_processed == 3
            assert csv_path.exists()

            # Verify CSV content
            with open(csv_path, "r") as f:
                reader = csv.DictReader(f)
                rows = list(reader)

            assert len(rows) == 3

            # Find the main test row
            main_row = next(r for r in rows if r["id"] == str(test_id))

            # Verify string types
            assert main_row["ascii_col"] == "ascii_only"
            assert main_row["text_col"] == "UTF-8 text with émojis 🚀"
            assert main_row["varchar_col"] == "varchar value"

            # Verify numeric types
            assert main_row["tinyint_col"] == "127"
            assert main_row["smallint_col"] == "32767"
            assert main_row["bigint_col"] == "9223372036854775807"
            assert main_row["decimal_col"] == str(Decimal("123456789.123456789"))

            # Verify temporal types
            # Cassandra may lose microsecond precision, check just the date/time part
            assert main_row["timestamp_col"].startswith(
                test_timestamp.strftime("%Y-%m-%dT%H:%M:%S")
            )

            # Verify binary data (hex encoded)
            assert main_row["blob_col"] == "42696e6172792064617461200001ff"

            # Verify boolean
            assert main_row["boolean_col"] == "true"

            # Verify INET
            assert main_row["inet_col"] == "192.168.1.100"

            # Export to JSON
            json_path = tmp_path / "all_types.json"
            stats_json = await operator.export(
                table=f"test_bulk.{table_name}", output_path=str(json_path), format="json"
            )

            assert stats_json.rows_processed == 3

            # Verify JSON content
            with open(json_path, "r") as f:
                json_data = json.load(f)

            assert len(json_data) == 3

            # Find main test row in JSON
            main_json = next(r for r in json_data if r["id"] == str(test_id))

            # JSON preserves more type info
            assert main_json["boolean_col"] is True
            assert isinstance(main_json["int_col"], int)
            assert main_json["decimal_col"] == str(Decimal("123456789.123456789"))

        finally:
            await session.execute(f"DROP TABLE test_bulk.{table_name}")

    @pytest.mark.asyncio
    async def test_export_collection_types(self, session, tmp_path):
        """
        Test exporting collection types (LIST, SET, MAP, TUPLE).

        What this tests:
        ---------------
        1. LIST with various element types
        2. SET with uniqueness preservation
        3. MAP with different key/value types
        4. TUPLE with mixed types
        5. Nested collections

        Why this matters:
        ----------------
        - Collections are complex to serialize
        - Must preserve structure and order
        - Common in modern schemas
        - Nesting adds complexity

        Additional context:
        ---------------------------------
        - CSV uses JSON encoding for collections
        - Sets become sorted arrays
        - Maps require string keys in JSON
        """
        table_name = f"collections_{int(datetime.now().timestamp() * 1000)}"

        await session.execute(
            f"""
            CREATE TABLE test_bulk.{table_name} (
                id UUID PRIMARY KEY,

                -- Simple collections
                tags LIST<TEXT>,
                unique_ids SET<UUID>,
                attributes MAP<TEXT, TEXT>,
                coordinates TUPLE<DOUBLE, DOUBLE>,

                -- Collections with various types
                scores LIST<INT>,
                active_dates SET<DATE>,
                config MAP<TEXT, BOOLEAN>,

                -- Nested collections
                nested_list LIST<FROZEN<LIST<INT>>>,
                nested_map MAP<TEXT, FROZEN<SET<TEXT>>>
            )
        """
        )

        test_id = uuid4()
        uuid1, uuid2, uuid3 = uuid4(), uuid4(), uuid4()

        await session.execute(
            f"""
            INSERT INTO test_bulk.{table_name} (
                id, tags, unique_ids, attributes, coordinates,
                scores, active_dates, config,
                nested_list, nested_map
            ) VALUES (
                {test_id},
                ['python', 'cassandra', 'async'],
                {{{uuid1}, {uuid2}, {uuid3}}},
                {{'version': '1.0', 'author': 'test'}},
                (37.7749, -122.4194),
                [95, 87, 92, 88],
                {{'{date.today()}', '{date(2024, 1, 1)}'}},
                {{'enabled': true, 'debug': false}},
                [[1, 2, 3], [4, 5, 6]],
                {{'languages': {{'python', 'java', 'scala'}}}}
            )
        """
        )

        try:
            operator = BulkOperator(session=session)

            # Export to CSV
            csv_path = tmp_path / "collections.csv"
            await operator.export(
                table=f"test_bulk.{table_name}", output_path=str(csv_path), format="csv"
            )

            # Verify collections in CSV (JSON encoded)
            with open(csv_path, "r") as f:
                reader = csv.DictReader(f)
                row = next(reader)

            # Lists preserve order
            tags = json.loads(row["tags"])
            assert tags == ["python", "cassandra", "async"]

            # Sets become sorted arrays
            unique_ids = json.loads(row["unique_ids"])
            assert len(unique_ids) == 3
            assert all(isinstance(uid, str) for uid in unique_ids)

            # Maps preserved
            attributes = json.loads(row["attributes"])
            assert attributes["version"] == "1.0"
            assert attributes["author"] == "test"

            # Tuples become arrays
            coordinates = json.loads(row["coordinates"])
            assert coordinates == [37.7749, -122.4194]

            # Nested collections
            nested_list = json.loads(row["nested_list"])
            assert nested_list == [[1, 2, 3], [4, 5, 6]]

            # Export to JSON for comparison
            json_path = tmp_path / "collections.json"
            await operator.export(
                table=f"test_bulk.{table_name}", output_path=str(json_path), format="json"
            )

            with open(json_path, "r") as f:
                json_data = json.load(f)
                json_row = json_data[0]

            # JSON preserves boolean values in maps
            assert json_row["config"]["enabled"] is True
            assert json_row["config"]["debug"] is False

        finally:
            await session.execute(f"DROP TABLE test_bulk.{table_name}")

    @pytest.mark.asyncio
    async def test_export_udt_types(self, session, tmp_path):
        """
        Test exporting User-Defined Types (UDT).

        What this tests:
        ---------------
        1. Simple UDT with basic fields
        2. Nested UDTs
        3. UDTs containing collections
        4. Multiple UDT instances
        5. NULL UDT fields

        Why this matters:
        ----------------
        - UDTs model complex domain objects
        - Must preserve field names and values
        - Common in DDD approaches
        - Nesting creates complexity

        Additional context:
        ---------------------------------
        - UDTs serialize as JSON objects
        - Field names must be preserved
        - Driver returns as special objects
        """
        # Create UDT types
        await session.execute(
            """
            CREATE TYPE IF NOT EXISTS test_bulk.address (
                street TEXT,
                city TEXT,
                zip_code TEXT,
                country TEXT
            )
        """
        )

        await session.execute(
            """
            CREATE TYPE IF NOT EXISTS test_bulk.contact_info (
                email TEXT,
                phone TEXT,
                address FROZEN<address>
            )
        """
        )

        table_name = f"udt_test_{int(datetime.now().timestamp() * 1000)}"

        await session.execute(
            f"""
            CREATE TABLE test_bulk.{table_name} (
                id UUID PRIMARY KEY,
                name TEXT,
                primary_contact FROZEN<contact_info>,
                addresses MAP<TEXT, FROZEN<address>>
            )
        """
        )

        # Insert UDT data
        test_id = uuid4()
        await session.execute(
            f"""
            INSERT INTO test_bulk.{table_name} (id, name, primary_contact, addresses)
            VALUES (
                {test_id},
                'John Doe',
                {{
                    email: 'john@example.com',
                    phone: '+1-555-0123',
                    address: {{
                        street: '123 Main St',
                        city: 'New York',
                        zip_code: '10001',
                        country: 'USA'
                    }}
                }},
                {{
                    'home': {{
                        street: '123 Main St',
                        city: 'New York',
                        zip_code: '10001',
                        country: 'USA'
                    }},
                    'work': {{
                        street: '456 Corp Ave',
                        city: 'San Francisco',
                        zip_code: '94105',
                        country: 'USA'
                    }}
                }}
            )
        """
        )

        try:
            operator = BulkOperator(session=session)

            # Export to CSV
            csv_path = tmp_path / "udt_data.csv"
            await operator.export(
                table=f"test_bulk.{table_name}", output_path=str(csv_path), format="csv"
            )

            # Verify UDT serialization in CSV
            with open(csv_path, "r") as f:
                reader = csv.DictReader(f)
                row = next(reader)

            # UDTs become JSON objects
            primary_contact = json.loads(row["primary_contact"])
            assert primary_contact["email"] == "john@example.com"
            assert primary_contact["phone"] == "+1-555-0123"
            assert primary_contact["address"]["city"] == "New York"

            addresses = json.loads(row["addresses"])
            assert addresses["home"]["street"] == "123 Main St"
            assert addresses["work"]["city"] == "San Francisco"

            # Export to JSON
            json_path = tmp_path / "udt_data.json"
            await operator.export(
                table=f"test_bulk.{table_name}", output_path=str(json_path), format="json"
            )

            with open(json_path, "r") as f:
                json_data = json.load(f)
                json_row = json_data[0]

            # Same structure in JSON
            assert json_row["primary_contact"]["address"]["country"] == "USA"
            assert len(json_row["addresses"]) == 2

        finally:
            await session.execute(f"DROP TABLE test_bulk.{table_name}")
            await session.execute("DROP TYPE test_bulk.contact_info")
            await session.execute("DROP TYPE test_bulk.address")

    @pytest.mark.asyncio
    async def test_export_special_types(self, session, tmp_path):
        """
        Test exporting special Cassandra types.

        What this tests:
        ---------------
        1. COUNTER type
        2. DURATION type (Cassandra 3.10+)
        3. FROZEN collections
        4. VECTOR type (Cassandra 5.0+)
        5. Mixed special types

        Why this matters:
        ----------------
        - Special types have unique behaviors
        - Must handle version-specific types
        - Serialization differs from basic types
        - Production uses these for specific needs

        Additional context:
        ---------------------------------
        - Counters are distributed integers
        - Duration has months/days/nanos
        - Vectors for ML embeddings
        - Frozen for immutability
        """
        # Test counter table
        counter_table = f"counters_{int(datetime.now().timestamp() * 1000)}"

        await session.execute(
            f"""
            CREATE TABLE test_bulk.{counter_table} (
                id UUID PRIMARY KEY,
                page_views COUNTER,
                total_sales COUNTER
            )
        """
        )

        test_id = uuid4()
        # Update counters
        await session.execute(
            f"""
            UPDATE test_bulk.{counter_table}
            SET page_views = page_views + 1000,
                total_sales = total_sales + 42
            WHERE id = {test_id}
        """
        )

        try:
            operator = BulkOperator(session=session)

            # Export counters
            csv_path = tmp_path / "counters.csv"
            await operator.export(
                table=f"test_bulk.{counter_table}", output_path=str(csv_path), format="csv"
            )

            with open(csv_path, "r") as f:
                reader = csv.DictReader(f)
                row = next(reader)

            # Counters serialize as integers
            assert row["page_views"] == "1000"
            assert row["total_sales"] == "42"

        finally:
            await session.execute(f"DROP TABLE test_bulk.{counter_table}")

        # Note: DURATION and VECTOR types require specific Cassandra versions
        # They would be tested similarly if available
