"""
Integration tests for all Cassandra data types.

This test demonstrates proper context manager usage and error handling
for production-ready code.
"""

import decimal
import uuid
from datetime import date, datetime, time
from ipaddress import IPv4Address, IPv6Address

import pytest


@pytest.mark.asyncio
@pytest.mark.integration
class TestCassandraDataTypes:
    """Test handling of all Cassandra data types with proper resource management."""

    async def test_numeric_types(self, cassandra_session):
        """Test numeric data types: int, bigint, float, double, decimal, varint."""
        session = cassandra_session

        try:
            # Create table
            await session.execute(
                """
                CREATE TABLE IF NOT EXISTS numeric_types (
            id int PRIMARY KEY,
            tiny_int tinyint,
            small_int smallint,
            regular_int int,
            big_int bigint,
            float_val float,
            double_val double,
            decimal_val decimal,
            varint_val varint
                )
                """
            )

            # Clear any existing data
            await session.execute("TRUNCATE numeric_types")

            # Prepare insert statement
            insert_stmt = await session.prepare(
                """
                INSERT INTO numeric_types
                (id, tiny_int, small_int, regular_int, big_int, float_val, double_val, decimal_val, varint_val)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
            )

            # Insert test data with error handling
            try:
                await session.execute(
                    insert_stmt,
                    [
                        1,
                        127,  # tinyint max
                        32767,  # smallint max
                        2147483647,  # int max
                        9223372036854775807,  # bigint max
                        3.14159,  # float
                        2.718281828,  # double
                        decimal.Decimal("123456789.123456789"),  # decimal
                        12345678901234567890,  # varint
                    ],
                )
            except Exception as e:
                pytest.fail(f"Failed to insert numeric data: {e}")

            # Query and verify with prepared statement
            select_stmt = await session.prepare("SELECT * FROM numeric_types WHERE id = ?")
            try:
                result = await session.execute(select_stmt, [1])
                row = result.one()

                assert row.tiny_int == 127
                assert row.small_int == 32767
                assert row.regular_int == 2147483647
                assert row.big_int == 9223372036854775807
                assert abs(row.float_val - 3.14159) < 0.00001
                assert abs(row.double_val - 2.718281828) < 0.000000001
                assert abs(
                    row.decimal_val - decimal.Decimal("123456789.123456789")
                ) < decimal.Decimal("0.00000001")
                assert row.varint_val == 12345678901234567890

            except Exception as e:
                pytest.fail(f"Failed to query numeric data: {e}")

        finally:
            # Cleanup - drop table
            await session.execute("DROP TABLE IF EXISTS numeric_types")

    async def test_text_types(self, cassandra_session):
        """Test text data types: text, varchar, ascii."""
        session = cassandra_session

        try:
            await session.execute(
                """
                CREATE TABLE IF NOT EXISTS text_types (
                    id int PRIMARY KEY,
                    text_val text,
                    varchar_val varchar,
                    ascii_val ascii
                )
                """
            )

            # Clear existing data
            await session.execute("TRUNCATE text_types")

            # Test various text values
            test_cases = [
                (1, "Hello World", "Variable Char", "ASCII_ONLY"),
                (2, "Unicode: 你好世界 🌍", "Special chars: @#$%", "simple_ascii"),
                (3, "", "", ""),  # Empty strings
                (4, " " * 1000, "x" * 100, "a" * 50),  # Long strings
            ]

            # Prepare insert statement
            insert_stmt = await session.prepare(
                "INSERT INTO text_types (id, text_val, varchar_val, ascii_val) VALUES (?, ?, ?, ?)"
            )

            # Insert test data with error handling
            for case in test_cases:
                try:
                    await session.execute(insert_stmt, case)
                except Exception as e:
                    pytest.fail(f"Failed to insert text data {case[0]}: {e}")

            # Verify with prepared select
            select_stmt = await session.prepare("SELECT * FROM text_types WHERE id = ?")
            for case in test_cases:
                try:
                    result = await session.execute(select_stmt, [case[0]])
                    row = result.one()
                    assert row.text_val == case[1]
                    assert row.varchar_val == case[2]
                    assert row.ascii_val == case[3]
                except Exception as e:
                    pytest.fail(f"Failed to verify text data {case[0]}: {e}")

        finally:
            # Cleanup
            await session.execute("DROP TABLE IF EXISTS text_types")

    async def test_temporal_types(self, cassandra_session):
        """Test temporal data types: timestamp, date, time."""
        session = cassandra_session

        try:
            await session.execute(
                """
                CREATE TABLE IF NOT EXISTS temporal_types (
                    id int PRIMARY KEY,
                    timestamp_val timestamp,
                    date_val date,
                    time_val time
                )
                """
            )

            await session.execute("TRUNCATE temporal_types")

            # Insert test data
            now = datetime.now()
            today = date.today()
            current_time = time(14, 30, 45, 123456)  # 14:30:45.123456

            insert_stmt = await session.prepare(
                "INSERT INTO temporal_types (id, timestamp_val, date_val, time_val) VALUES (?, ?, ?, ?)"
            )

            try:
                await session.execute(insert_stmt, [1, now, today, current_time])
            except Exception as e:
                pytest.fail(f"Failed to insert temporal data: {e}")

            # Query and verify
            select_stmt = await session.prepare("SELECT * FROM temporal_types WHERE id = ?")
            try:
                result = await session.execute(select_stmt, [1])
                row = result.one()

                # Timestamp comparison (within 1 second due to precision)
                assert abs((row.timestamp_val - now).total_seconds()) < 1
                assert row.date_val == today
                # Time is stored as nanoseconds since midnight
                assert row.time_val == (
                    current_time.hour * 3600 * 1_000_000_000
                    + current_time.minute * 60 * 1_000_000_000
                    + current_time.second * 1_000_000_000
                    + current_time.microsecond * 1_000
                )
            except Exception as e:
                pytest.fail(f"Failed to verify temporal data: {e}")

        finally:
            await session.execute("DROP TABLE IF EXISTS temporal_types")

    async def test_uuid_types(self, cassandra_session):
        """Test UUID types: uuid, timeuuid."""
        session = cassandra_session

        try:
            await session.execute(
                """
                CREATE TABLE IF NOT EXISTS uuid_types (
                    id int PRIMARY KEY,
                    uuid_val uuid,
                    timeuuid_val timeuuid
                )
                """
            )

            await session.execute("TRUNCATE uuid_types")

            # Generate UUIDs
            regular_uuid = uuid.uuid4()
            time_uuid = uuid.uuid1()

            insert_stmt = await session.prepare(
                "INSERT INTO uuid_types (id, uuid_val, timeuuid_val) VALUES (?, ?, ?)"
            )
            await session.execute(insert_stmt, [1, regular_uuid, time_uuid])

            # Query and verify
            select_stmt = await session.prepare("SELECT * FROM uuid_types WHERE id = ?")
            result = await session.execute(select_stmt, [1])
            row = result.one()

            assert row.uuid_val == regular_uuid
            assert row.timeuuid_val == time_uuid

        finally:
            await session.execute("DROP TABLE IF EXISTS uuid_types")

    async def test_binary_types(self, cassandra_session):
        """Test binary data type: blob."""
        session = cassandra_session

        try:
            await session.execute(
                """
                CREATE TABLE IF NOT EXISTS binary_types (
                    id int PRIMARY KEY,
                    blob_val blob
                )
                """
            )

            await session.execute("TRUNCATE binary_types")

            # Test various binary data
            test_data = [
                (1, b"Hello World"),
                (2, b"\x00\x01\x02\x03\x04\x05"),  # Binary data
                (3, b""),  # Empty blob
                (4, bytes(range(256))),  # All byte values
            ]

            # Prepare insert statement
            insert_stmt = await session.prepare(
                "INSERT INTO binary_types (id, blob_val) VALUES (?, ?)"
            )

            for id_val, blob_data in test_data:
                await session.execute(insert_stmt, [id_val, blob_data])

            # Verify
            select_stmt = await session.prepare("SELECT * FROM binary_types WHERE id = ?")
            for id_val, expected_blob in test_data:
                result = await session.execute(select_stmt, [id_val])
                row = result.one()
                assert row.blob_val == expected_blob

        finally:
            await session.execute("DROP TABLE IF EXISTS binary_types")

    async def test_boolean_type(self, cassandra_session):
        """Test boolean data type."""
        session = cassandra_session

        try:
            await session.execute(
                """
                CREATE TABLE IF NOT EXISTS boolean_types (
                    id int PRIMARY KEY,
                    bool_val boolean
                )
                """
            )

            await session.execute("TRUNCATE boolean_types")

            # Test true, false, and null
            insert_stmt = await session.prepare(
                "INSERT INTO boolean_types (id, bool_val) VALUES (?, ?)"
            )
            insert_null_stmt = await session.prepare("INSERT INTO boolean_types (id) VALUES (?)")

            await session.execute(insert_stmt, [1, True])
            await session.execute(insert_stmt, [2, False])
            await session.execute(insert_null_stmt, [3])  # NULL

            # Verify
            select_stmt = await session.prepare("SELECT * FROM boolean_types WHERE id = ?")

            result = await session.execute(select_stmt, [1])
            assert result.one().bool_val is True

            result = await session.execute(select_stmt, [2])
            assert result.one().bool_val is False

            result = await session.execute(select_stmt, [3])
            assert result.one().bool_val is None

        finally:
            await session.execute("DROP TABLE IF EXISTS boolean_types")

    async def test_inet_types(self, cassandra_session):
        """Test inet data type."""
        session = cassandra_session

        try:
            await session.execute(
                """
                CREATE TABLE IF NOT EXISTS inet_types (
                    id int PRIMARY KEY,
                    ipv4_val inet,
                    ipv6_val inet
                )
                """
            )

            await session.execute("TRUNCATE inet_types")

            # Test IPv4 and IPv6
            ipv4 = IPv4Address("192.168.1.1")
            ipv6 = IPv6Address("2001:0db8:85a3:0000:0000:8a2e:0370:7334")

            insert_stmt = await session.prepare(
                "INSERT INTO inet_types (id, ipv4_val, ipv6_val) VALUES (?, ?, ?)"
            )
            await session.execute(insert_stmt, [1, str(ipv4), str(ipv6)])

            # Query and verify
            select_stmt = await session.prepare("SELECT * FROM inet_types WHERE id = ?")
            result = await session.execute(select_stmt, [1])
            row = result.one()

            assert str(row.ipv4_val) == str(ipv4)
            assert str(row.ipv6_val) == str(ipv6)

        finally:
            await session.execute("DROP TABLE IF EXISTS inet_types")

    async def test_collection_types(self, cassandra_session):
        """Test collection types: list, set, map."""
        session = cassandra_session

        try:
            await session.execute(
                """
                CREATE TABLE IF NOT EXISTS collection_types (
                    id int PRIMARY KEY,
                    list_val list<int>,
                    set_val set<text>,
                    map_val map<text, int>
                )
                """
            )

            await session.execute("TRUNCATE collection_types")

            # Insert collections
            list_data = [1, 2, 3, 4, 5]
            set_data = {"apple", "banana", "cherry"}
            map_data = {"one": 1, "two": 2, "three": 3}

            insert_stmt = await session.prepare(
                "INSERT INTO collection_types (id, list_val, set_val, map_val) VALUES (?, ?, ?, ?)"
            )
            await session.execute(insert_stmt, [1, list_data, set_data, map_data])

            # Query and verify
            select_stmt = await session.prepare("SELECT * FROM collection_types WHERE id = ?")
            result = await session.execute(select_stmt, [1])
            row = result.one()

            assert row.list_val == list_data
            assert row.set_val == set_data
            assert row.map_val == map_data

        finally:
            await session.execute("DROP TABLE IF EXISTS collection_types")

    async def test_frozen_collections(self, cassandra_session):
        """Test frozen collection types."""
        session = cassandra_session

        try:
            await session.execute(
                """
                CREATE TABLE IF NOT EXISTS frozen_types (
                    id int PRIMARY KEY,
                    frozen_list frozen<list<int>>,
                    frozen_set frozen<set<text>>,
                    frozen_map frozen<map<text, int>>
                )
                """
            )

            await session.execute("TRUNCATE frozen_types")

            # Insert frozen collections
            list_data = [10, 20, 30]
            set_data = {"x", "y", "z"}
            map_data = {"a": 1, "b": 2}

            insert_stmt = await session.prepare(
                "INSERT INTO frozen_types (id, frozen_list, frozen_set, frozen_map) VALUES (?, ?, ?, ?)"
            )
            await session.execute(insert_stmt, [1, list_data, set_data, map_data])

            # Query and verify
            select_stmt = await session.prepare("SELECT * FROM frozen_types WHERE id = ?")
            result = await session.execute(select_stmt, [1])
            row = result.one()

            # In newer Cassandra versions, frozen lists might not convert to tuples
            assert row.frozen_list == list_data or row.frozen_list == tuple(list_data)
            assert row.frozen_set == frozenset(set_data)  # Frozen sets
            # Frozen maps are returned as regular dicts
            assert row.frozen_map == map_data

        finally:
            await session.execute("DROP TABLE IF EXISTS frozen_types")

    async def test_tuple_type(self, cassandra_session):
        """Test tuple data type."""
        session = cassandra_session

        try:
            await session.execute(
                """
                CREATE TABLE IF NOT EXISTS tuple_types (
                    id int PRIMARY KEY,
                    tuple_val tuple<int, text, boolean>
                )
                """
            )

            await session.execute("TRUNCATE tuple_types")

            # Insert tuple - Cassandra requires individual values for tuple insertion
            insert_stmt = await session.prepare(
                "INSERT INTO tuple_types (id, tuple_val) VALUES (?, (?, ?, ?))"
            )
            await session.execute(insert_stmt, [1, 42, "hello", True])

            # Query and verify
            select_stmt = await session.prepare("SELECT * FROM tuple_types WHERE id = ?")
            result = await session.execute(select_stmt, [1])
            row = result.one()

            assert row.tuple_val == (42, "hello", True)

        finally:
            await session.execute("DROP TABLE IF EXISTS tuple_types")

    async def test_counter_type(self, cassandra_session):
        """Test counter data type."""
        session = cassandra_session

        try:
            await session.execute(
                """
                CREATE TABLE IF NOT EXISTS counter_types (
                    id int PRIMARY KEY,
                    counter_val counter
                )
                """
            )

            # Counters can't be truncated, drop and recreate
            await session.execute("DROP TABLE IF EXISTS counter_types")
            await session.execute(
                """
                CREATE TABLE counter_types (
                    id int PRIMARY KEY,
                    counter_val counter
                )
                """
            )

            # Update counter - counters use special syntax, prepare statements
            update_add_stmt = await session.prepare(
                "UPDATE counter_types SET counter_val = counter_val + ? WHERE id = ?"
            )
            update_sub_stmt = await session.prepare(
                "UPDATE counter_types SET counter_val = counter_val - ? WHERE id = ?"
            )

            await session.execute(update_add_stmt, [5, 1])
            await session.execute(update_add_stmt, [3, 1])
            await session.execute(update_sub_stmt, [2, 1])

            # Query and verify
            select_stmt = await session.prepare("SELECT * FROM counter_types WHERE id = ?")
            result = await session.execute(select_stmt, [1])
            row = result.one()

            assert row.counter_val == 6  # 5 + 3 - 2

        finally:
            await session.execute("DROP TABLE IF EXISTS counter_types")

    async def test_null_values(self, cassandra_session):
        """Test NULL values for various types."""
        session = cassandra_session

        try:
            await session.execute(
                """
                CREATE TABLE IF NOT EXISTS null_types (
                    id int PRIMARY KEY,
                    text_val text,
                    int_val int,
                    list_val list<int>,
                    map_val map<text, int>
                )
                """
            )

            await session.execute("TRUNCATE null_types")

            # Insert row with only primary key
            insert_stmt = await session.prepare("INSERT INTO null_types (id) VALUES (?)")
            await session.execute(insert_stmt, [1])

            # Query and verify all are None
            select_stmt = await session.prepare("SELECT * FROM null_types WHERE id = ?")
            result = await session.execute(select_stmt, [1])
            row = result.one()

            assert row.text_val is None
            assert row.int_val is None
            assert row.list_val is None
            assert row.map_val is None

        finally:
            await session.execute("DROP TABLE IF EXISTS null_types")

    async def test_prepared_statements_with_types(self, cassandra_session):
        """Test prepared statements with various data types."""
        session = cassandra_session

        try:
            await session.execute(
                """
                CREATE TABLE IF NOT EXISTS prepared_types (
                    id uuid PRIMARY KEY,
                    name text,
                    age int,
                    balance decimal,
                    created timestamp,
                    tags set<text>
                )
                """
            )

            await session.execute("TRUNCATE prepared_types")

            # Prepare statement - prepared statements use ? placeholders
            stmt = await session.prepare(
                "INSERT INTO prepared_types (id, name, age, balance, created, tags) VALUES (?, ?, ?, ?, ?, ?)"
            )

            # Execute with various types
            test_id = uuid.uuid4()
            test_time = datetime.now()

            await session.execute(
                stmt,
                [
                    test_id,
                    "John Doe",
                    30,
                    decimal.Decimal("1234.56"),
                    test_time,
                    {"tag1", "tag2", "tag3"},
                ],
            )

            # Query and verify
            select_stmt = await session.prepare("SELECT * FROM prepared_types WHERE id = ?")
            result = await session.execute(select_stmt, [test_id])
            row = result.one()

            assert row.id == test_id
            assert row.name == "John Doe"
            assert row.age == 30
            assert row.balance == decimal.Decimal("1234.56")
            assert abs((row.created - test_time).total_seconds()) < 1
            assert row.tags == {"tag1", "tag2", "tag3"}

        finally:
            await session.execute("DROP TABLE IF EXISTS prepared_types")
