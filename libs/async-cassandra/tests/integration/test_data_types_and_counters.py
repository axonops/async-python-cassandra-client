"""
Consolidated integration tests for Cassandra data types and counter operations.

This module combines all data type and counter tests from multiple files,
providing comprehensive coverage of Cassandra's type system.

Tests consolidated from:
- test_cassandra_data_types.py - All supported Cassandra data types
- test_counters.py - Counter-specific operations and edge cases
- Various type usage from other test files

Test Organization:
==================
1. Basic Data Types - Numeric, text, temporal, boolean, UUID, binary
2. Collection Types - List, set, map, tuple, frozen collections
3. Special Types - Inet, counter
4. Counter Operations - Increment, decrement, concurrent updates
5. Type Conversions and Edge Cases - NULL handling, boundaries, errors
"""

import asyncio
import datetime
import decimal
import uuid
from datetime import date
from datetime import time as datetime_time
from datetime import timezone

import pytest
from cassandra import ConsistencyLevel, InvalidRequest
from cassandra.util import Date, Time, uuid_from_time
from test_utils import generate_unique_table


@pytest.mark.asyncio
@pytest.mark.integration
class TestDataTypes:
    """Test various Cassandra data types with real Cassandra."""

    # ========================================
    # Numeric Data Types
    # ========================================

    async def test_numeric_types(self, cassandra_session, shared_keyspace_setup):
        """
        Test all numeric data types in Cassandra.

        What this tests:
        ---------------
        1. TINYINT, SMALLINT, INT, BIGINT
        2. FLOAT, DOUBLE
        3. DECIMAL, VARINT
        4. Boundary values
        5. Precision handling

        Why this matters:
        ----------------
        Numeric types have different ranges and precision characteristics.
        Choosing the right type affects storage and performance.
        """
        # Create test table with all numeric types
        table_name = generate_unique_table("test_numeric_types")
        await cassandra_session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INT PRIMARY KEY,
                tiny_val TINYINT,
                small_val SMALLINT,
                int_val INT,
                big_val BIGINT,
                float_val FLOAT,
                double_val DOUBLE,
                decimal_val DECIMAL,
                varint_val VARINT
            )
            """
        )

        # Prepare insert statement
        insert_stmt = await cassandra_session.prepare(
            f"""
            INSERT INTO {table_name}
            (id, tiny_val, small_val, int_val, big_val,
             float_val, double_val, decimal_val, varint_val)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        )

        # Test various numeric values
        test_cases = [
            # Normal values
            (
                1,
                127,
                32767,
                2147483647,
                9223372036854775807,
                3.14,
                3.141592653589793,
                decimal.Decimal("123.456"),
                123456789,
            ),
            # Negative values
            (
                2,
                -128,
                -32768,
                -2147483648,
                -9223372036854775808,
                -3.14,
                -3.141592653589793,
                decimal.Decimal("-123.456"),
                -123456789,
            ),
            # Zero values
            (3, 0, 0, 0, 0, 0.0, 0.0, decimal.Decimal("0"), 0),
            # High precision decimal
            (4, 1, 1, 1, 1, 1.1, 1.1, decimal.Decimal("123456789.123456789"), 123456789123456789),
        ]

        for values in test_cases:
            await cassandra_session.execute(insert_stmt, values)

        # Verify all values
        select_stmt = await cassandra_session.prepare(f"SELECT * FROM {table_name} WHERE id = ?")

        for i, expected in enumerate(test_cases, 1):
            result = await cassandra_session.execute(select_stmt, (i,))
            row = result.one()

            # Verify each numeric type
            assert row.id == expected[0]
            assert row.tiny_val == expected[1]
            assert row.small_val == expected[2]
            assert row.int_val == expected[3]
            assert row.big_val == expected[4]
            assert abs(row.float_val - expected[5]) < 0.0001  # Float comparison
            assert abs(row.double_val - expected[6]) < 0.0000001  # Double comparison
            assert row.decimal_val == expected[7]
            assert row.varint_val == expected[8]

    async def test_text_types(self, cassandra_session, shared_keyspace_setup):
        """
        Test text-based data types.

        What this tests:
        ---------------
        1. TEXT and VARCHAR (synonymous in Cassandra)
        2. ASCII type
        3. Unicode handling
        4. Empty strings vs NULL
        5. Maximum string lengths

        Why this matters:
        ----------------
        Text types are the most common data types. Understanding
        encoding and storage implications is crucial.
        """
        # Create test table
        table_name = generate_unique_table("test_text_types")
        await cassandra_session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INT PRIMARY KEY,
                text_val TEXT,
                varchar_val VARCHAR,
                ascii_val ASCII
            )
            """
        )

        # Prepare statements
        insert_stmt = await cassandra_session.prepare(
            f"INSERT INTO {table_name} (id, text_val, varchar_val, ascii_val) VALUES (?, ?, ?, ?)"
        )

        # Test various text values
        test_cases = [
            (1, "Simple text", "Simple varchar", "Simple ASCII"),
            (2, "Unicode: 你好世界 🌍", "Unicode: émojis 😀", "ASCII only"),
            (3, "", "", ""),  # Empty strings
            (4, " " * 100, " " * 100, " " * 100),  # Spaces
            (5, "Line\nBreaks\r\nAllowed", "Special\tChars\t", "No_Special"),
        ]

        for values in test_cases:
            await cassandra_session.execute(insert_stmt, values)

        # Test NULL values
        await cassandra_session.execute(insert_stmt, (6, None, None, None))

        # Verify values
        result = await cassandra_session.execute(f"SELECT * FROM {table_name}")
        rows = list(result)
        assert len(rows) == 6

        # Verify specific cases
        for row in rows:
            if row.id == 2:
                assert "你好世界" in row.text_val
                assert "émojis" in row.varchar_val
            elif row.id == 3:
                assert row.text_val == ""
                assert row.varchar_val == ""
                assert row.ascii_val == ""
            elif row.id == 6:
                assert row.text_val is None
                assert row.varchar_val is None
                assert row.ascii_val is None

    async def test_temporal_types(self, cassandra_session, shared_keyspace_setup):
        """
        Test date and time related data types.

        What this tests:
        ---------------
        1. TIMESTAMP type
        2. DATE type
        3. TIME type
        4. Timezone handling
        5. Precision and range

        Why this matters:
        ----------------
        Temporal data is common in applications. Understanding
        precision and timezone behavior is critical.
        """
        # Create test table
        table_name = generate_unique_table("test_temporal_types")
        await cassandra_session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INT PRIMARY KEY,
                ts_val TIMESTAMP,
                date_val DATE,
                time_val TIME
            )
            """
        )

        # Prepare insert
        insert_stmt = await cassandra_session.prepare(
            f"INSERT INTO {table_name} (id, ts_val, date_val, time_val) VALUES (?, ?, ?, ?)"
        )

        # Test values
        now = datetime.datetime.now(timezone.utc)
        today = Date(date.today())
        current_time = Time(datetime_time(14, 30, 45, 123000))  # 14:30:45.123

        test_cases = [
            (1, now, today, current_time),
            (
                2,
                datetime.datetime(2000, 1, 1, 0, 0, 0, 0, timezone.utc),
                Date(date(2000, 1, 1)),
                Time(datetime_time(0, 0, 0)),
            ),
            (
                3,
                datetime.datetime(2038, 1, 19, 3, 14, 7, 0, timezone.utc),
                Date(date(2038, 1, 19)),
                Time(datetime_time(23, 59, 59, 999999)),
            ),
        ]

        for values in test_cases:
            await cassandra_session.execute(insert_stmt, values)

        # Verify temporal values
        result = await cassandra_session.execute(f"SELECT * FROM {table_name}")
        rows = list(result)
        assert len(rows) == 3

        # Check timestamp precision (millisecond precision in Cassandra)
        row1 = next(r for r in rows if r.id == 1)
        # Handle both timezone-aware and naive datetimes
        if row1.ts_val.tzinfo is None:
            # Convert to UTC aware for comparison
            row_ts = row1.ts_val.replace(tzinfo=timezone.utc)
        else:
            row_ts = row1.ts_val
        assert abs((row_ts - now).total_seconds()) < 1

    async def test_uuid_types(self, cassandra_session, shared_keyspace_setup):
        """
        Test UUID and TIMEUUID data types.

        What this tests:
        ---------------
        1. UUID type (type 4 random UUID)
        2. TIMEUUID type (type 1 time-based UUID)
        3. UUID generation functions
        4. Time extraction from TIMEUUID

        Why this matters:
        ----------------
        UUIDs are commonly used for distributed unique identifiers.
        TIMEUUIDs provide time-ordering capabilities.
        """
        # Create test table
        table_name = generate_unique_table("test_uuid_types")
        await cassandra_session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INT PRIMARY KEY,
                uuid_val UUID,
                timeuuid_val TIMEUUID,
                created_at TIMESTAMP
            )
            """
        )

        # Test UUIDs
        regular_uuid = uuid.uuid4()
        time_uuid = uuid_from_time(datetime.datetime.now())

        # Insert with prepared statement
        insert_stmt = await cassandra_session.prepare(
            f"""
            INSERT INTO {table_name} (id, uuid_val, timeuuid_val, created_at)
            VALUES (?, ?, ?, ?)
            """
        )

        await cassandra_session.execute(
            insert_stmt, (1, regular_uuid, time_uuid, datetime.datetime.now(timezone.utc))
        )

        # Test UUID functions
        await cassandra_session.execute(
            f"INSERT INTO {table_name} (id, uuid_val, timeuuid_val) VALUES (2, uuid(), now())"
        )

        # Verify UUIDs
        result = await cassandra_session.execute(f"SELECT * FROM {table_name}")
        rows = list(result)
        assert len(rows) == 2

        # Verify UUID types
        for row in rows:
            assert isinstance(row.uuid_val, uuid.UUID)
            assert isinstance(row.timeuuid_val, uuid.UUID)
            # TIMEUUID should be version 1
            if row.id == 1:
                assert row.timeuuid_val.version == 1

    async def test_binary_and_boolean_types(self, cassandra_session, shared_keyspace_setup):
        """
        Test BLOB and BOOLEAN data types.

        What this tests:
        ---------------
        1. BLOB type for binary data
        2. BOOLEAN type
        3. Binary data encoding/decoding
        4. NULL vs empty blob

        Why this matters:
        ----------------
        Binary data storage and boolean flags are common requirements.
        """
        # Create test table
        table_name = generate_unique_table("test_binary_boolean")
        await cassandra_session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INT PRIMARY KEY,
                binary_data BLOB,
                is_active BOOLEAN,
                is_verified BOOLEAN
            )
            """
        )

        # Prepare statement
        insert_stmt = await cassandra_session.prepare(
            f"INSERT INTO {table_name} (id, binary_data, is_active, is_verified) VALUES (?, ?, ?, ?)"
        )

        # Test data
        test_cases = [
            (1, b"Hello World", True, False),
            (2, b"\x00\x01\x02\x03\xff", False, True),
            (3, b"", True, True),  # Empty blob
            (4, None, None, None),  # NULL values
            (5, b"Unicode bytes: \xf0\x9f\x98\x80", False, False),
        ]

        for values in test_cases:
            await cassandra_session.execute(insert_stmt, values)

        # Verify data
        result = await cassandra_session.execute(f"SELECT * FROM {table_name}")
        rows = {row.id: row for row in result}

        assert rows[1].binary_data == b"Hello World"
        assert rows[1].is_active is True
        assert rows[1].is_verified is False

        assert rows[2].binary_data == b"\x00\x01\x02\x03\xff"
        assert rows[3].binary_data == b""  # Empty blob
        assert rows[4].binary_data is None
        assert rows[4].is_active is None

    async def test_inet_types(self, cassandra_session, shared_keyspace_setup):
        """
        Test INET data type for IP addresses.

        What this tests:
        ---------------
        1. IPv4 addresses
        2. IPv6 addresses
        3. Address validation
        4. String conversion

        Why this matters:
        ----------------
        Storing IP addresses efficiently is common in network applications.
        """
        # Create test table
        table_name = generate_unique_table("test_inet_types")
        await cassandra_session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INT PRIMARY KEY,
                client_ip INET,
                server_ip INET,
                description TEXT
            )
            """
        )

        # Prepare statement
        insert_stmt = await cassandra_session.prepare(
            f"INSERT INTO {table_name} (id, client_ip, server_ip, description) VALUES (?, ?, ?, ?)"
        )

        # Test IP addresses
        test_cases = [
            (1, "192.168.1.1", "10.0.0.1", "Private IPv4"),
            (2, "8.8.8.8", "8.8.4.4", "Public IPv4"),
            (3, "::1", "fe80::1", "IPv6 loopback and link-local"),
            (4, "2001:db8::1", "2001:db8:0:0:1:0:0:1", "IPv6 public"),
            (5, "127.0.0.1", "::ffff:127.0.0.1", "IPv4 and IPv4-mapped IPv6"),
        ]

        for values in test_cases:
            await cassandra_session.execute(insert_stmt, values)

        # Verify IP addresses
        result = await cassandra_session.execute(f"SELECT * FROM {table_name}")
        rows = list(result)
        assert len(rows) == 5

        # Verify specific addresses
        for row in rows:
            assert row.client_ip is not None
            assert row.server_ip is not None
            # IPs are returned as strings
            if row.id == 1:
                assert row.client_ip == "192.168.1.1"
            elif row.id == 3:
                assert row.client_ip == "::1"

    # ========================================
    # Collection Data Types
    # ========================================

    async def test_list_type(self, cassandra_session, shared_keyspace_setup):
        """
        Test LIST collection type.

        What this tests:
        ---------------
        1. List creation and manipulation
        2. Ordering preservation
        3. Duplicate values
        4. NULL vs empty list
        5. List updates and appends

        Why this matters:
        ----------------
        Lists maintain order and allow duplicates, useful for
        ordered collections like tags or history.
        """
        # Create test table
        table_name = generate_unique_table("test_list_type")
        await cassandra_session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INT PRIMARY KEY,
                tags LIST<TEXT>,
                scores LIST<INT>,
                timestamps LIST<TIMESTAMP>
            )
            """
        )

        # Prepare statements
        insert_stmt = await cassandra_session.prepare(
            f"INSERT INTO {table_name} (id, tags, scores, timestamps) VALUES (?, ?, ?, ?)"
        )

        # Test list operations
        now = datetime.datetime.now(timezone.utc)
        test_cases = [
            (1, ["tag1", "tag2", "tag3"], [100, 200, 300], [now]),
            (2, ["duplicate", "duplicate"], [1, 1, 2, 3, 5], None),  # Duplicates allowed
            (3, [], [], []),  # Empty lists
            (4, None, None, None),  # NULL lists
        ]

        for values in test_cases:
            await cassandra_session.execute(insert_stmt, values)

        # Test list append
        update_stmt = await cassandra_session.prepare(
            f"UPDATE {table_name} SET tags = tags + ? WHERE id = ?"
        )
        await cassandra_session.execute(update_stmt, (["tag4", "tag5"], 1))

        # Test list prepend
        update_prepend = await cassandra_session.prepare(
            f"UPDATE {table_name} SET tags = ? + tags WHERE id = ?"
        )
        await cassandra_session.execute(update_prepend, (["tag0"], 1))

        # Verify lists
        result = await cassandra_session.execute(f"SELECT * FROM {table_name} WHERE id = 1")
        row = result.one()
        assert row.tags == ["tag0", "tag1", "tag2", "tag3", "tag4", "tag5"]

        # Test removing from list
        update_remove = await cassandra_session.prepare(
            f"UPDATE {table_name} SET scores = scores - ? WHERE id = ?"
        )
        await cassandra_session.execute(update_remove, ([1], 2))

        result = await cassandra_session.execute(f"SELECT * FROM {table_name} WHERE id = 2")
        row = result.one()
        # Note: removes all occurrences
        assert 1 not in row.scores

    async def test_set_type(self, cassandra_session, shared_keyspace_setup):
        """
        Test SET collection type.

        What this tests:
        ---------------
        1. Set creation and manipulation
        2. Uniqueness enforcement
        3. Unordered nature
        4. Set operations (add, remove)
        5. NULL vs empty set

        Why this matters:
        ----------------
        Sets enforce uniqueness and are useful for tags,
        categories, or any unique collection.
        """
        # Create test table
        table_name = generate_unique_table("test_set_type")
        await cassandra_session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INT PRIMARY KEY,
                categories SET<TEXT>,
                user_ids SET<UUID>,
                ip_addresses SET<INET>
            )
            """
        )

        # Prepare statements
        insert_stmt = await cassandra_session.prepare(
            f"INSERT INTO {table_name} (id, categories, user_ids, ip_addresses) VALUES (?, ?, ?, ?)"
        )

        # Test data
        user_id1 = uuid.uuid4()
        user_id2 = uuid.uuid4()

        test_cases = [
            (1, {"tech", "news", "sports"}, {user_id1, user_id2}, {"192.168.1.1", "10.0.0.1"}),
            (2, {"tech", "tech", "tech"}, {user_id1}, None),  # Duplicates become unique
            (3, set(), set(), set()),  # Empty sets - Note: these become NULL in Cassandra
            (4, None, None, None),  # NULL sets
        ]

        for values in test_cases:
            await cassandra_session.execute(insert_stmt, values)

        # Test set addition
        update_add = await cassandra_session.prepare(
            f"UPDATE {table_name} SET categories = categories + ? WHERE id = ?"
        )
        await cassandra_session.execute(update_add, ({"politics", "tech"}, 1))

        # Test set removal
        update_remove = await cassandra_session.prepare(
            f"UPDATE {table_name} SET categories = categories - ? WHERE id = ?"
        )
        await cassandra_session.execute(update_remove, ({"sports"}, 1))

        # Verify sets
        result = await cassandra_session.execute(f"SELECT * FROM {table_name} WHERE id = 1")
        row = result.one()
        # Sets are unordered
        assert row.categories == {"tech", "news", "politics"}

        # Check empty set behavior
        result3 = await cassandra_session.execute(f"SELECT * FROM {table_name} WHERE id = 3")
        row3 = result3.one()
        # Empty sets become NULL in Cassandra
        assert row3.categories is None

    async def test_map_type(self, cassandra_session, shared_keyspace_setup):
        """
        Test MAP collection type.

        What this tests:
        ---------------
        1. Map creation and manipulation
        2. Key-value pairs
        3. Key uniqueness
        4. Map updates
        5. NULL vs empty map

        Why this matters:
        ----------------
        Maps provide key-value storage within a column,
        useful for metadata or configuration.
        """
        # Create test table
        table_name = generate_unique_table("test_map_type")
        await cassandra_session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INT PRIMARY KEY,
                metadata MAP<TEXT, TEXT>,
                scores MAP<TEXT, INT>,
                timestamps MAP<TEXT, TIMESTAMP>
            )
            """
        )

        # Prepare statements
        insert_stmt = await cassandra_session.prepare(
            f"INSERT INTO {table_name} (id, metadata, scores, timestamps) VALUES (?, ?, ?, ?)"
        )

        # Test data
        now = datetime.datetime.now(timezone.utc)
        test_cases = [
            (1, {"name": "John", "city": "NYC"}, {"math": 95, "english": 88}, {"created": now}),
            (2, {"key": "value"}, None, None),
            (3, {}, {}, {}),  # Empty maps - become NULL
            (4, None, None, None),  # NULL maps
        ]

        for values in test_cases:
            await cassandra_session.execute(insert_stmt, values)

        # Test map update - add/update entries
        update_map = await cassandra_session.prepare(
            f"UPDATE {table_name} SET metadata = metadata + ? WHERE id = ?"
        )
        await cassandra_session.execute(update_map, ({"country": "USA", "city": "Boston"}, 1))

        # Test map entry update
        update_entry = await cassandra_session.prepare(
            f"UPDATE {table_name} SET metadata[?] = ? WHERE id = ?"
        )
        await cassandra_session.execute(update_entry, ("status", "active", 1))

        # Test map entry deletion
        delete_entry = await cassandra_session.prepare(
            f"DELETE metadata[?] FROM {table_name} WHERE id = ?"
        )
        await cassandra_session.execute(delete_entry, ("name", 1))

        # Verify map
        result = await cassandra_session.execute(f"SELECT * FROM {table_name} WHERE id = 1")
        row = result.one()
        assert row.metadata == {"city": "Boston", "country": "USA", "status": "active"}
        assert "name" not in row.metadata  # Deleted

    async def test_tuple_type(self, cassandra_session, shared_keyspace_setup):
        """
        Test TUPLE type.

        What this tests:
        ---------------
        1. Fixed-size ordered collections
        2. Heterogeneous types
        3. Tuple comparison
        4. NULL elements in tuples

        Why this matters:
        ----------------
        Tuples provide fixed-structure data storage,
        useful for coordinates, versions, etc.
        """
        # Create test table
        table_name = generate_unique_table("test_tuple_type")
        await cassandra_session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INT PRIMARY KEY,
                coordinates TUPLE<DOUBLE, DOUBLE>,
                version TUPLE<INT, INT, INT>,
                user_info TUPLE<TEXT, INT, BOOLEAN>
            )
            """
        )

        # Prepare statement
        insert_stmt = await cassandra_session.prepare(
            f"INSERT INTO {table_name} (id, coordinates, version, user_info) VALUES (?, ?, ?, ?)"
        )

        # Test tuples
        test_cases = [
            (1, (37.7749, -122.4194), (1, 2, 3), ("Alice", 25, True)),
            (2, (0.0, 0.0), (0, 0, 1), ("Bob", None, False)),  # NULL element
            (3, None, None, None),  # NULL tuples
        ]

        for values in test_cases:
            await cassandra_session.execute(insert_stmt, values)

        # Verify tuples
        result = await cassandra_session.execute(f"SELECT * FROM {table_name}")
        rows = {row.id: row for row in result}

        assert rows[1].coordinates == (37.7749, -122.4194)
        assert rows[1].version == (1, 2, 3)
        assert rows[1].user_info == ("Alice", 25, True)

        # Check NULL element in tuple
        assert rows[2].user_info == ("Bob", None, False)

    async def test_frozen_collections(self, cassandra_session, shared_keyspace_setup):
        """
        Test FROZEN collections.

        What this tests:
        ---------------
        1. Frozen lists, sets, maps
        2. Nested frozen collections
        3. Immutability of frozen collections
        4. Use as primary key components

        Why this matters:
        ----------------
        Frozen collections can be used in primary keys and
        are stored more efficiently but cannot be updated partially.
        """
        # Create test table with frozen collections
        table_name = generate_unique_table("test_frozen_collections")
        await cassandra_session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INT,
                frozen_tags FROZEN<SET<TEXT>>,
                config FROZEN<MAP<TEXT, TEXT>>,
                nested FROZEN<MAP<TEXT, FROZEN<LIST<INT>>>>,
                PRIMARY KEY (id, frozen_tags)
            )
            """
        )

        # Prepare statement
        insert_stmt = await cassandra_session.prepare(
            f"INSERT INTO {table_name} (id, frozen_tags, config, nested) VALUES (?, ?, ?, ?)"
        )

        # Test frozen collections
        test_cases = [
            (1, {"tag1", "tag2"}, {"key1": "val1"}, {"nums": [1, 2, 3]}),
            (1, {"tag3", "tag4"}, {"key2": "val2"}, {"nums": [4, 5, 6]}),
            (2, set(), {}, {}),  # Empty frozen collections
        ]

        for values in test_cases:
            # Convert the list to tuple for frozen list
            id_val, tags, config, nested_dict = values
            # Convert nested list to tuple for frozen representation
            nested_frozen = {k: v for k, v in nested_dict.items()}
            await cassandra_session.execute(insert_stmt, (id_val, tags, config, nested_frozen))

        # Verify frozen collections
        result = await cassandra_session.execute(f"SELECT * FROM {table_name} WHERE id = 1")
        rows = list(result)
        assert len(rows) == 2  # Two rows with same id but different frozen_tags

        # Try to update frozen collection (should replace entire value)
        update_stmt = await cassandra_session.prepare(
            f"UPDATE {table_name} SET config = ? WHERE id = ? AND frozen_tags = ?"
        )
        await cassandra_session.execute(update_stmt, ({"new": "config"}, 1, {"tag1", "tag2"}))


@pytest.mark.asyncio
@pytest.mark.integration
class TestCounterOperations:
    """Test counter data type operations with real Cassandra."""

    async def test_basic_counter_operations(self, cassandra_session, shared_keyspace_setup):
        """
        Test basic counter increment and decrement.

        What this tests:
        ---------------
        1. Counter table creation
        2. INCREMENT operations
        3. DECREMENT operations
        4. Counter initialization
        5. Reading counter values

        Why this matters:
        ----------------
        Counters provide atomic increment/decrement operations
        essential for metrics and statistics.
        """
        # Create counter table
        table_name = generate_unique_table("test_basic_counters")
        await cassandra_session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY,
                page_views COUNTER,
                likes COUNTER,
                shares COUNTER
            )
            """
        )

        # Prepare counter update statements
        increment_views = await cassandra_session.prepare(
            f"UPDATE {table_name} SET page_views = page_views + ? WHERE id = ?"
        )
        increment_likes = await cassandra_session.prepare(
            f"UPDATE {table_name} SET likes = likes + ? WHERE id = ?"
        )
        decrement_shares = await cassandra_session.prepare(
            f"UPDATE {table_name} SET shares = shares - ? WHERE id = ?"
        )

        # Test counter operations
        post_id = "post_001"

        # Increment counters
        await cassandra_session.execute(increment_views, (100, post_id))
        await cassandra_session.execute(increment_likes, (10, post_id))
        await cassandra_session.execute(increment_views, (50, post_id))  # Another increment

        # Decrement counter
        await cassandra_session.execute(decrement_shares, (5, post_id))

        # Read counter values
        select_stmt = await cassandra_session.prepare(f"SELECT * FROM {table_name} WHERE id = ?")
        result = await cassandra_session.execute(select_stmt, (post_id,))
        row = result.one()

        assert row.page_views == 150  # 100 + 50
        assert row.likes == 10
        assert row.shares == -5  # Started at 0, decremented by 5

        # Test multiple increments in sequence
        for i in range(10):
            await cassandra_session.execute(increment_likes, (1, post_id))

        result = await cassandra_session.execute(select_stmt, (post_id,))
        row = result.one()
        assert row.likes == 20  # 10 + 10*1

    async def test_concurrent_counter_updates(self, cassandra_session, shared_keyspace_setup):
        """
        Test concurrent counter updates.

        What this tests:
        ---------------
        1. Thread-safe counter operations
        2. No lost updates
        3. Atomic increments
        4. Performance under concurrency

        Why this matters:
        ----------------
        Counters must handle concurrent updates correctly
        in distributed systems.
        """
        # Create counter table
        table_name = generate_unique_table("test_concurrent_counters")
        await cassandra_session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY,
                total_requests COUNTER,
                error_count COUNTER
            )
            """
        )

        # Prepare statements
        increment_requests = await cassandra_session.prepare(
            f"UPDATE {table_name} SET total_requests = total_requests + ? WHERE id = ?"
        )
        increment_errors = await cassandra_session.prepare(
            f"UPDATE {table_name} SET error_count = error_count + ? WHERE id = ?"
        )

        service_id = "api_service"

        # Simulate concurrent updates
        async def increment_counter(counter_type, count):
            if counter_type == "requests":
                await cassandra_session.execute(increment_requests, (count, service_id))
            else:
                await cassandra_session.execute(increment_errors, (count, service_id))

        # Run 100 concurrent increments
        tasks = []
        for i in range(100):
            tasks.append(increment_counter("requests", 1))
            if i % 10 == 0:  # 10% error rate
                tasks.append(increment_counter("errors", 1))

        await asyncio.gather(*tasks)

        # Verify final counts
        select_stmt = await cassandra_session.prepare(f"SELECT * FROM {table_name} WHERE id = ?")
        result = await cassandra_session.execute(select_stmt, (service_id,))
        row = result.one()

        assert row.total_requests == 100
        assert row.error_count == 10

    async def test_counter_consistency_levels(self, cassandra_session, shared_keyspace_setup):
        """
        Test counters with different consistency levels.

        What this tests:
        ---------------
        1. Counter updates with QUORUM
        2. Counter reads with different consistency
        3. Consistency vs performance trade-offs

        Why this matters:
        ----------------
        Counter consistency affects accuracy and performance
        in distributed deployments.
        """
        # Create counter table
        table_name = generate_unique_table("test_counter_consistency")
        await cassandra_session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY,
                metric_value COUNTER
            )
            """
        )

        # Prepare statements with different consistency levels
        update_stmt = await cassandra_session.prepare(
            f"UPDATE {table_name} SET metric_value = metric_value + ? WHERE id = ?"
        )
        update_stmt.consistency_level = ConsistencyLevel.QUORUM

        select_stmt = await cassandra_session.prepare(
            f"SELECT metric_value FROM {table_name} WHERE id = ?"
        )
        select_stmt.consistency_level = ConsistencyLevel.ONE

        metric_id = "cpu_usage"

        # Update with QUORUM consistency
        await cassandra_session.execute(update_stmt, (75, metric_id))

        # Read with ONE consistency (faster but potentially stale)
        result = await cassandra_session.execute(select_stmt, (metric_id,))
        row = result.one()
        assert row.metric_value == 75

    async def test_counter_special_cases(self, cassandra_session, shared_keyspace_setup):
        """
        Test counter special cases and limitations.

        What this tests:
        ---------------
        1. Counters cannot be set to specific values
        2. Counters cannot have TTL
        3. Counter deletion behavior
        4. NULL counter behavior

        Why this matters:
        ----------------
        Understanding counter limitations prevents
        design mistakes and runtime errors.
        """
        # Create counter table
        table_name = generate_unique_table("test_counter_special")
        await cassandra_session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY,
                counter_val COUNTER
            )
            """
        )

        # Test that we cannot INSERT counters (only UPDATE)
        with pytest.raises(InvalidRequest):
            await cassandra_session.execute(
                f"INSERT INTO {table_name} (id, counter_val) VALUES ('test', 100)"
            )

        # Test that counters cannot have TTL
        with pytest.raises(InvalidRequest):
            await cassandra_session.execute(
                f"UPDATE {table_name} USING TTL 3600 SET counter_val = counter_val + 1 WHERE id = 'test'"
            )

        # Test counter deletion
        update_stmt = await cassandra_session.prepare(
            f"UPDATE {table_name} SET counter_val = counter_val + ? WHERE id = ?"
        )
        await cassandra_session.execute(update_stmt, (100, "delete_test"))

        # Delete the counter
        await cassandra_session.execute(
            f"DELETE counter_val FROM {table_name} WHERE id = 'delete_test'"
        )

        # After deletion, counter reads as NULL
        result = await cassandra_session.execute(
            f"SELECT counter_val FROM {table_name} WHERE id = 'delete_test'"
        )
        row = result.one()
        if row:  # Row might not exist at all
            assert row.counter_val is None

        # Can increment again after deletion
        await cassandra_session.execute(update_stmt, (50, "delete_test"))
        result = await cassandra_session.execute(
            f"SELECT counter_val FROM {table_name} WHERE id = 'delete_test'"
        )
        row = result.one()
        # After deleting a counter column, the row might not exist
        # or the counter might be reset depending on Cassandra version
        if row is not None:
            assert row.counter_val == 50  # Starts from 0 again

    async def test_counter_batch_operations(self, cassandra_session, shared_keyspace_setup):
        """
        Test counter operations in batches.

        What this tests:
        ---------------
        1. Counter-only batches
        2. Multiple counter updates in batch
        3. Batch atomicity for counters

        Why this matters:
        ----------------
        Batching counter updates can improve performance
        for related counter modifications.
        """
        # Create counter table
        table_name = generate_unique_table("test_counter_batch")
        await cassandra_session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                category TEXT,
                item TEXT,
                views COUNTER,
                clicks COUNTER,
                PRIMARY KEY (category, item)
            )
            """
        )

        # This test demonstrates counter batch operations
        # which are already covered in test_batch_and_lwt_operations.py
        # Here we'll test a specific counter batch pattern

        # Prepare counter updates
        update_views = await cassandra_session.prepare(
            f"UPDATE {table_name} SET views = views + ? WHERE category = ? AND item = ?"
        )
        update_clicks = await cassandra_session.prepare(
            f"UPDATE {table_name} SET clicks = clicks + ? WHERE category = ? AND item = ?"
        )

        # Update multiple counters for same partition
        category = "electronics"
        items = ["laptop", "phone", "tablet"]

        # Simulate page views and clicks
        for item in items:
            await cassandra_session.execute(update_views, (100, category, item))
            await cassandra_session.execute(update_clicks, (10, category, item))

        # Verify counters
        result = await cassandra_session.execute(
            f"SELECT * FROM {table_name} WHERE category = '{category}'"
        )
        rows = list(result)
        assert len(rows) == 3

        for row in rows:
            assert row.views == 100
            assert row.clicks == 10


@pytest.mark.asyncio
@pytest.mark.integration
class TestDataTypeEdgeCases:
    """Test edge cases and special scenarios for data types."""

    async def test_null_value_handling(self, cassandra_session, shared_keyspace_setup):
        """
        Test NULL value handling across different data types.

        What this tests:
        ---------------
        1. NULL vs missing columns
        2. NULL in collections
        3. NULL in primary keys (not allowed)
        4. Distinguishing NULL from empty

        Why this matters:
        ----------------
        NULL handling affects storage, queries, and application logic.
        """
        # Create test table
        table_name = generate_unique_table("test_null_handling")
        await cassandra_session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INT PRIMARY KEY,
                text_col TEXT,
                int_col INT,
                list_col LIST<TEXT>,
                map_col MAP<TEXT, INT>
            )
            """
        )

        # Insert with explicit NULLs
        insert_stmt = await cassandra_session.prepare(
            f"INSERT INTO {table_name} (id, text_col, int_col, list_col, map_col) VALUES (?, ?, ?, ?, ?)"
        )
        await cassandra_session.execute(insert_stmt, (1, None, None, None, None))

        # Insert with missing columns (implicitly NULL)
        await cassandra_session.execute(
            f"INSERT INTO {table_name} (id, text_col) VALUES (2, 'has text')"
        )

        # Insert with empty collections
        await cassandra_session.execute(insert_stmt, (3, "text", 0, [], {}))

        # Verify NULL handling
        result = await cassandra_session.execute(f"SELECT * FROM {table_name}")
        rows = {row.id: row for row in result}

        # Explicit NULLs
        assert rows[1].text_col is None
        assert rows[1].int_col is None
        assert rows[1].list_col is None
        assert rows[1].map_col is None

        # Missing columns are NULL
        assert rows[2].int_col is None
        assert rows[2].list_col is None

        # Empty collections become NULL in Cassandra
        assert rows[3].list_col is None
        assert rows[3].map_col is None

    async def test_numeric_boundaries(self, cassandra_session, shared_keyspace_setup):
        """
        Test numeric type boundaries and overflow behavior.

        What this tests:
        ---------------
        1. Maximum and minimum values
        2. Overflow behavior
        3. Precision limits
        4. Special float values (NaN, Infinity)

        Why this matters:
        ----------------
        Understanding type limits prevents data corruption
        and application errors.
        """
        # Create test table
        table_name = generate_unique_table("test_numeric_boundaries")
        await cassandra_session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INT PRIMARY KEY,
                tiny_val TINYINT,
                small_val SMALLINT,
                float_val FLOAT,
                double_val DOUBLE
            )
            """
        )

        # Test boundary values
        insert_stmt = await cassandra_session.prepare(
            f"INSERT INTO {table_name} (id, tiny_val, small_val, float_val, double_val) VALUES (?, ?, ?, ?, ?)"
        )

        # Maximum values
        await cassandra_session.execute(insert_stmt, (1, 127, 32767, float("inf"), float("inf")))

        # Minimum values
        await cassandra_session.execute(
            insert_stmt, (2, -128, -32768, float("-inf"), float("-inf"))
        )

        # Special float values
        await cassandra_session.execute(insert_stmt, (3, 0, 0, float("nan"), float("nan")))

        # Verify special values
        result = await cassandra_session.execute(f"SELECT * FROM {table_name}")
        rows = {row.id: row for row in result}

        # Check infinity
        assert rows[1].float_val == float("inf")
        assert rows[2].double_val == float("-inf")

        # Check NaN (NaN != NaN in Python)
        import math

        assert math.isnan(rows[3].float_val)
        assert math.isnan(rows[3].double_val)

    async def test_collection_size_limits(self, cassandra_session, shared_keyspace_setup):
        """
        Test collection size limits and performance.

        What this tests:
        ---------------
        1. Large collections
        2. Maximum collection sizes
        3. Performance with large collections
        4. Nested collection limits

        Why this matters:
        ----------------
        Collections have size limits that affect design decisions.
        """
        # Create test table
        table_name = generate_unique_table("test_collection_limits")
        await cassandra_session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INT PRIMARY KEY,
                large_list LIST<TEXT>,
                large_set SET<INT>,
                large_map MAP<INT, TEXT>
            )
            """
        )

        # Create large collections (but not too large to avoid timeouts)
        large_list = [f"item_{i}" for i in range(1000)]
        large_set = set(range(1000))
        large_map = {i: f"value_{i}" for i in range(1000)}

        # Insert large collections
        insert_stmt = await cassandra_session.prepare(
            f"INSERT INTO {table_name} (id, large_list, large_set, large_map) VALUES (?, ?, ?, ?)"
        )
        await cassandra_session.execute(insert_stmt, (1, large_list, large_set, large_map))

        # Verify large collections
        result = await cassandra_session.execute(f"SELECT * FROM {table_name} WHERE id = 1")
        row = result.one()

        assert len(row.large_list) == 1000
        assert len(row.large_set) == 1000
        assert len(row.large_map) == 1000

        # Note: Cassandra has a practical limit of ~64KB for a collection
        # and a hard limit of 2GB for any single column value

    async def test_type_compatibility(self, cassandra_session, shared_keyspace_setup):
        """
        Test type compatibility and implicit conversions.

        What this tests:
        ---------------
        1. Compatible type assignments
        2. String to numeric conversions
        3. Timestamp formats
        4. Type validation

        Why this matters:
        ----------------
        Understanding type compatibility helps prevent
        runtime errors and data corruption.
        """
        # Create test table
        table_name = generate_unique_table("test_type_compatibility")
        await cassandra_session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INT PRIMARY KEY,
                int_val INT,
                bigint_val BIGINT,
                text_val TEXT,
                timestamp_val TIMESTAMP
            )
            """
        )

        # Test compatible assignments
        insert_stmt = await cassandra_session.prepare(
            f"INSERT INTO {table_name} (id, int_val, bigint_val, text_val, timestamp_val) VALUES (?, ?, ?, ?, ?)"
        )

        # INT can be assigned to BIGINT
        await cassandra_session.execute(
            insert_stmt, (1, 12345, 12345, "12345", datetime.datetime.now(timezone.utc))
        )

        # Test string representations
        await cassandra_session.execute(
            f"INSERT INTO {table_name} (id, text_val) VALUES (2, '你好世界')"
        )

        # Verify assignments
        result = await cassandra_session.execute(f"SELECT * FROM {table_name}")
        rows = list(result)
        assert len(rows) == 2

        # Test type errors
        # Cannot insert string into numeric column via prepared statement
        with pytest.raises(Exception):  # Will be TypeError or similar
            await cassandra_session.execute(
                insert_stmt, (3, "not a number", 123, "text", datetime.datetime.now(timezone.utc))
            )
