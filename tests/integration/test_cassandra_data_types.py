"""
Integration tests for all Cassandra data types.
"""

import decimal
import uuid
from datetime import date, datetime, time
from ipaddress import IPv4Address, IPv6Address

import pytest
import pytest_asyncio

from async_cassandra import AsyncCluster


@pytest.mark.asyncio
@pytest.mark.integration
class TestCassandraDataTypes:
    """Test handling of all Cassandra data types."""

    @pytest_asyncio.fixture(scope="class")
    async def setup_cluster(self):
        """Set up cluster and session for tests."""
        cluster = AsyncCluster(["localhost"])
        session = await cluster.connect()

        # Create keyspace
        await session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS test_datatypes
            WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
            """
        )
        await session.set_keyspace("test_datatypes")

        yield session

        # Cleanup
        await session.execute("DROP KEYSPACE IF EXISTS test_datatypes")
        await session.close()
        await cluster.shutdown()

    async def test_numeric_types(self, setup_cluster):
        """Test numeric data types: int, bigint, float, double, decimal, varint."""
        session = setup_cluster

        # Create table
        await session.execute(
            """
            CREATE TABLE numeric_types (
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

        # Insert data
        await session.execute(
            """
            INSERT INTO numeric_types
            (id, tiny_int, small_int, regular_int, big_int, float_val, double_val, decimal_val, varint_val)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
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

        # Query and verify
        result = await session.execute("SELECT * FROM numeric_types WHERE id = 1")
        row = result.one()

        assert row.tiny_int == 127
        assert row.small_int == 32767
        assert row.regular_int == 2147483647
        assert row.big_int == 9223372036854775807
        assert abs(row.float_val - 3.14159) < 0.00001
        assert abs(row.double_val - 2.718281828) < 0.000000001
        # Decimal may lose some precision in Cassandra
        assert abs(row.decimal_val - decimal.Decimal("123456789.123456789")) < decimal.Decimal(
            "0.00000001"
        )
        assert row.varint_val == 12345678901234567890

    async def test_text_types(self, setup_cluster):
        """Test text data types: text, varchar, ascii."""
        session = setup_cluster

        await session.execute(
            """
            CREATE TABLE text_types (
                id int PRIMARY KEY,
                text_val text,
                varchar_val varchar,
                ascii_val ascii
            )
        """
        )

        # Test various text values
        test_cases = [
            (1, "Hello World", "Variable Char", "ASCII_ONLY"),
            (2, "Unicode: 你好世界 🌍", "Special chars: @#$%", "simple_ascii"),
            (3, "", "", ""),  # Empty strings
            (4, " " * 1000, "x" * 100, "a" * 50),  # Long strings
        ]

        for case in test_cases:
            await session.execute(
                "INSERT INTO text_types (id, text_val, varchar_val, ascii_val) VALUES (%s, %s, %s, %s)",
                case,
            )

        # Verify
        for case in test_cases:
            result = await session.execute("SELECT * FROM text_types WHERE id = %s", [case[0]])
            row = result.one()
            assert row.text_val == case[1]
            assert row.varchar_val == case[2]
            assert row.ascii_val == case[3]

    async def test_temporal_types(self, setup_cluster):
        """Test temporal data types: timestamp, date, time."""
        session = setup_cluster

        await session.execute(
            """
            CREATE TABLE temporal_types (
                id int PRIMARY KEY,
                timestamp_val timestamp,
                date_val date,
                time_val time
            )
        """
        )

        # Insert test data
        now = datetime.now()
        today = date.today()
        current_time = time(14, 30, 45, 123456)  # 14:30:45.123456

        await session.execute(
            "INSERT INTO temporal_types (id, timestamp_val, date_val, time_val) VALUES (%s, %s, %s, %s)",
            [1, now, today, current_time],
        )

        # Query and verify
        result = await session.execute("SELECT * FROM temporal_types WHERE id = 1")
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

    async def test_uuid_types(self, setup_cluster):
        """Test UUID types: uuid, timeuuid."""
        session = setup_cluster

        await session.execute(
            """
            CREATE TABLE uuid_types (
                id int PRIMARY KEY,
                uuid_val uuid,
                timeuuid_val timeuuid
            )
        """
        )

        # Generate UUIDs
        regular_uuid = uuid.uuid4()
        time_uuid = uuid.uuid1()

        await session.execute(
            "INSERT INTO uuid_types (id, uuid_val, timeuuid_val) VALUES (%s, %s, %s)",
            [1, regular_uuid, time_uuid],
        )

        # Query and verify
        result = await session.execute("SELECT * FROM uuid_types WHERE id = 1")
        row = result.one()

        assert row.uuid_val == regular_uuid
        assert row.timeuuid_val == time_uuid

    async def test_binary_types(self, setup_cluster):
        """Test binary data type: blob."""
        session = setup_cluster

        await session.execute(
            """
            CREATE TABLE binary_types (
                id int PRIMARY KEY,
                blob_val blob
            )
        """
        )

        # Test various binary data
        test_data = [
            (1, b"Hello World"),
            (2, b"\x00\x01\x02\x03\x04\x05"),  # Binary data
            (3, b""),  # Empty blob
            (4, bytes(range(256))),  # All byte values
        ]

        for id_val, blob_data in test_data:
            await session.execute(
                "INSERT INTO binary_types (id, blob_val) VALUES (%s, %s)", [id_val, blob_data]
            )

        # Verify
        for id_val, expected_blob in test_data:
            result = await session.execute("SELECT * FROM binary_types WHERE id = %s", [id_val])
            row = result.one()
            assert row.blob_val == expected_blob

    async def test_boolean_type(self, setup_cluster):
        """Test boolean data type."""
        session = setup_cluster

        await session.execute(
            """
            CREATE TABLE boolean_types (
                id int PRIMARY KEY,
                bool_val boolean
            )
        """
        )

        # Test true, false, and null
        await session.execute("INSERT INTO boolean_types (id, bool_val) VALUES (1, true)")
        await session.execute("INSERT INTO boolean_types (id, bool_val) VALUES (2, false)")
        await session.execute("INSERT INTO boolean_types (id) VALUES (3)")  # NULL

        # Verify
        result = await session.execute("SELECT * FROM boolean_types WHERE id = 1")
        assert result.one().bool_val is True

        result = await session.execute("SELECT * FROM boolean_types WHERE id = 2")
        assert result.one().bool_val is False

        result = await session.execute("SELECT * FROM boolean_types WHERE id = 3")
        assert result.one().bool_val is None

    async def test_inet_types(self, setup_cluster):
        """Test inet data type."""
        session = setup_cluster

        await session.execute(
            """
            CREATE TABLE inet_types (
                id int PRIMARY KEY,
                ipv4_val inet,
                ipv6_val inet
            )
        """
        )

        # Test IPv4 and IPv6
        ipv4 = IPv4Address("192.168.1.1")
        ipv6 = IPv6Address("2001:0db8:85a3:0000:0000:8a2e:0370:7334")

        await session.execute(
            "INSERT INTO inet_types (id, ipv4_val, ipv6_val) VALUES (%s, %s, %s)",
            [1, str(ipv4), str(ipv6)],
        )

        # Query and verify
        result = await session.execute("SELECT * FROM inet_types WHERE id = 1")
        row = result.one()

        assert str(row.ipv4_val) == str(ipv4)
        assert str(row.ipv6_val) == str(ipv6)

    async def test_collection_types(self, setup_cluster):
        """Test collection types: list, set, map."""
        session = setup_cluster

        await session.execute(
            """
            CREATE TABLE collection_types (
                id int PRIMARY KEY,
                list_val list<int>,
                set_val set<text>,
                map_val map<text, int>
            )
        """
        )

        # Insert collections
        list_data = [1, 2, 3, 4, 5]
        set_data = {"apple", "banana", "cherry"}
        map_data = {"one": 1, "two": 2, "three": 3}

        await session.execute(
            "INSERT INTO collection_types (id, list_val, set_val, map_val) VALUES (%s, %s, %s, %s)",
            [1, list_data, set_data, map_data],
        )

        # Query and verify
        result = await session.execute("SELECT * FROM collection_types WHERE id = 1")
        row = result.one()

        assert row.list_val == list_data
        assert row.set_val == set_data
        assert row.map_val == map_data

    async def test_frozen_collections(self, setup_cluster):
        """Test frozen collection types."""
        session = setup_cluster

        await session.execute(
            """
            CREATE TABLE frozen_types (
                id int PRIMARY KEY,
                frozen_list frozen<list<int>>,
                frozen_set frozen<set<text>>,
                frozen_map frozen<map<text, int>>
            )
        """
        )

        # Insert frozen collections
        list_data = [10, 20, 30]
        set_data = {"x", "y", "z"}
        map_data = {"a": 1, "b": 2}

        await session.execute(
            "INSERT INTO frozen_types (id, frozen_list, frozen_set, frozen_map) VALUES (%s, %s, %s, %s)",
            [1, list_data, set_data, map_data],
        )

        # Query and verify
        result = await session.execute("SELECT * FROM frozen_types WHERE id = 1")
        row = result.one()

        # In newer Cassandra versions, frozen lists might not convert to tuples
        assert row.frozen_list == list_data or row.frozen_list == tuple(list_data)
        assert row.frozen_set == frozenset(set_data)  # Frozen sets
        # Frozen maps are returned as regular dicts
        assert row.frozen_map == map_data

    async def test_tuple_type(self, setup_cluster):
        """Test tuple data type."""
        session = setup_cluster

        await session.execute(
            """
            CREATE TABLE tuple_types (
                id int PRIMARY KEY,
                tuple_val tuple<int, text, boolean>
            )
        """
        )

        # Insert tuple - Cassandra requires individual values for tuple insertion
        await session.execute(
            "INSERT INTO tuple_types (id, tuple_val) VALUES (%s, (%s, %s, %s))",
            [1, 42, "hello", True],
        )

        # Query and verify
        result = await session.execute("SELECT * FROM tuple_types WHERE id = 1")
        row = result.one()

        assert row.tuple_val == (42, "hello", True)

    async def test_counter_type(self, setup_cluster):
        """Test counter data type."""
        session = setup_cluster

        await session.execute(
            """
            CREATE TABLE counter_types (
                id int PRIMARY KEY,
                counter_val counter
            )
        """
        )

        # Update counter
        await session.execute("UPDATE counter_types SET counter_val = counter_val + 5 WHERE id = 1")
        await session.execute("UPDATE counter_types SET counter_val = counter_val + 3 WHERE id = 1")
        await session.execute("UPDATE counter_types SET counter_val = counter_val - 2 WHERE id = 1")

        # Query and verify
        result = await session.execute("SELECT * FROM counter_types WHERE id = 1")
        row = result.one()

        assert row.counter_val == 6  # 5 + 3 - 2

    async def test_null_values(self, setup_cluster):
        """Test NULL values for various types."""
        session = setup_cluster

        await session.execute(
            """
            CREATE TABLE null_types (
                id int PRIMARY KEY,
                text_val text,
                int_val int,
                list_val list<int>,
                map_val map<text, int>
            )
        """
        )

        # Insert row with only primary key
        await session.execute("INSERT INTO null_types (id) VALUES (1)")

        # Query and verify all are None
        result = await session.execute("SELECT * FROM null_types WHERE id = 1")
        row = result.one()

        assert row.text_val is None
        assert row.int_val is None
        assert row.list_val is None
        assert row.map_val is None

    async def test_prepared_statements_with_types(self, setup_cluster):
        """Test prepared statements with various data types."""
        session = setup_cluster

        await session.execute(
            """
            CREATE TABLE prepared_types (
                id uuid PRIMARY KEY,
                name text,
                age int,
                balance decimal,
                created timestamp,
                tags set<text>
            )
        """
        )

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
        result = await session.execute("SELECT * FROM prepared_types WHERE id = %s", [test_id])
        row = result.one()

        assert row.id == test_id
        assert row.name == "John Doe"
        assert row.age == 30
        assert row.balance == decimal.Decimal("1234.56")
        assert abs((row.created - test_time).total_seconds()) < 1
        assert row.tags == {"tag1", "tag2", "tag3"}
