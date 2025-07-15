"""
Test predicate pushdown validation for partition keys.

What this tests:
---------------
1. Validates partition keys are included in predicates
2. Prevents inefficient queries without partition keys
3. Allows queries with all required keys
4. Handles composite partition keys correctly

Why this matters:
----------------
- Prevents full table scans in Cassandra
- Ensures efficient query execution
- Protects against performance disasters
- Maintains best practices for Cassandra usage

Additional context:
---------------------------------
- Cassandra requires partition keys for efficient queries
- Missing partition keys cause cluster-wide scans
- This validation prevents accidental performance issues
"""

import pytest

from async_cassandra_dataframe.reader import CassandraDataFrameReader


class TestPredicatePushdownValidation:
    """Test suite for predicate pushdown validation."""

    @pytest.mark.asyncio
    async def test_missing_partition_key_raises_error(self, session, test_table_name):
        """
        Test that missing partition key in predicates raises error.

        Given: A table with partition key
        When: Querying with predicates missing the partition key
        Then: Raises ValueError with clear message
        """
        # Given
        table = test_table_name
        await session.execute(
            f"""
            CREATE TABLE {table} (
                user_id int,
                timestamp int,
                value text,
                PRIMARY KEY (user_id, timestamp)
            )
        """
        )

        # When/Then
        reader = CassandraDataFrameReader(session, table)

        # Predicate on clustering key only - missing partition key
        predicates = [{"column": "timestamp", "operator": ">=", "value": 100}]

        with pytest.raises(ValueError) as exc_info:
            await reader.read(predicates=predicates, require_partition_key_predicate=True)

        assert "partition key" in str(exc_info.value).lower()
        assert "user_id" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_composite_partition_key_validation(self, session, test_table_name):
        """
        Test validation with composite partition keys.

        Given: A table with composite partition key (a, b)
        When: Providing predicates for only one key
        Then: Raises error requiring all partition keys
        """
        # Given
        table = test_table_name
        await session.execute(
            f"""
            CREATE TABLE {table} (
                region text,
                user_id int,
                timestamp int,
                value text,
                PRIMARY KEY ((region, user_id), timestamp)
            )
        """
        )

        reader = CassandraDataFrameReader(session, table)

        # When/Then - missing one partition key
        predicates = [
            {"column": "region", "operator": "=", "value": "US"}
            # Missing user_id!
        ]

        with pytest.raises(ValueError) as exc_info:
            await reader.read(predicates=predicates, require_partition_key_predicate=True)

        assert "all partition keys" in str(exc_info.value).lower()
        assert "user_id" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_valid_partition_key_predicates_succeed(self, session, test_table_name):
        """
        Test that valid predicates with all partition keys work.

        Given: A table with partition keys
        When: Providing predicates for all partition keys
        Then: Query executes successfully
        """
        # Given
        table = test_table_name
        await session.execute(
            f"""
            CREATE TABLE {table} (
                user_id int,
                timestamp int,
                value text,
                PRIMARY KEY (user_id, timestamp)
            )
        """
        )

        insert_stmt = await session.prepare(
            f"INSERT INTO {table} (user_id, timestamp, value) VALUES (?, ?, ?)"
        )

        for user in range(5):
            for ts in range(10):
                await session.execute(insert_stmt, (user, ts, f"val_{user}_{ts}"))

        # When
        reader = CassandraDataFrameReader(session, table)

        # Valid predicate with partition key
        predicates = [{"column": "user_id", "operator": "=", "value": 2}]

        df = await reader.read(predicates=predicates, require_partition_key_predicate=True)

        # Then
        result = df.compute()
        assert len(result) == 10  # One user with 10 timestamps
        assert all(result["user_id"] == 2)

    @pytest.mark.asyncio
    async def test_in_operator_with_partition_key(self, session, test_table_name):
        """
        Test IN operator satisfies partition key requirement.

        Given: A table with partition key
        When: Using IN operator on partition key
        Then: Query is allowed
        """
        # Given
        table = test_table_name
        await session.execute(
            f"""
            CREATE TABLE {table} (
                id int PRIMARY KEY,
                name text
            )
        """
        )

        insert_stmt = await session.prepare(f"INSERT INTO {table} (id, name) VALUES (?, ?)")

        for i in range(20):
            await session.execute(insert_stmt, (i, f"name_{i}"))

        # When
        reader = CassandraDataFrameReader(session, table)

        predicates = [{"column": "id", "operator": "IN", "value": [1, 5, 10, 15]}]

        df = await reader.read(predicates=predicates, require_partition_key_predicate=True)

        # Then
        result = df.compute()
        assert len(result) == 4
        assert set(result["id"]) == {1, 5, 10, 15}

    @pytest.mark.asyncio
    async def test_range_query_on_partition_key_warning(self, session, test_table_name):
        """
        Test range queries on partition key show warning.

        Given: A table with partition key
        When: Using range operator on partition key
        Then: Works but logs warning about efficiency
        """
        # Given
        table = test_table_name
        await session.execute(
            f"""
            CREATE TABLE {table} (
                id int PRIMARY KEY,
                value text
            )
        """
        )

        insert_stmt = await session.prepare(f"INSERT INTO {table} (id, value) VALUES (?, ?)")

        for i in range(100):
            await session.execute(insert_stmt, (i, f"value_{i}"))

        # When
        reader = CassandraDataFrameReader(session, table)

        # Range query on partition key - less efficient
        predicates = [{"column": "id", "operator": ">=", "value": 50}]

        # Should work but less efficient than = or IN
        df = await reader.read(predicates=predicates, require_partition_key_predicate=True)

        # Then
        result = df.compute()
        assert len(result) == 50
        assert all(result["id"] >= 50)

    @pytest.mark.asyncio
    async def test_opt_out_of_validation(self, session, test_table_name):
        """
        Test ability to opt out of partition key validation.

        Given: A table with partition key
        When: Explicitly disabling validation
        Then: Allows queries without partition key (at user's risk)
        """
        # Given
        table = test_table_name
        await session.execute(
            f"""
            CREATE TABLE {table} (
                user_id int,
                timestamp int,
                status text,
                PRIMARY KEY (user_id, timestamp)
            )
        """
        )

        insert_stmt = await session.prepare(
            f"INSERT INTO {table} (user_id, timestamp, status) VALUES (?, ?, ?)"
        )

        for user in range(3):
            for ts in range(5):
                status = "active" if ts % 2 == 0 else "inactive"
                await session.execute(insert_stmt, (user, ts, status))

        # When - query without partition key but validation disabled
        reader = CassandraDataFrameReader(session, table)

        predicates = [{"column": "status", "operator": "=", "value": "active"}]

        # This would normally fail validation
        df = await reader.read(
            predicates=predicates,
            require_partition_key_predicate=False,  # Explicitly opt out
            allow_filtering=True,  # Required for this query
        )

        # Then
        result = df.compute()
        assert all(result["status"] == "active")
        # Should have all active records across all partitions
        assert len(result) == 9  # 3 users * 3 active timestamps each

    @pytest.mark.asyncio
    async def test_validation_with_all_partition_keys_composite(self, session, test_table_name):
        """
        Test success with all keys in composite partition key.

        Given: Table with composite partition key
        When: Providing predicates for all partition key components
        Then: Query executes successfully
        """
        # Given
        table = test_table_name
        await session.execute(
            f"""
            CREATE TABLE {table} (
                region text,
                user_id int,
                timestamp int,
                value decimal,
                PRIMARY KEY ((region, user_id), timestamp)
            )
        """
        )

        insert_stmt = await session.prepare(
            f"INSERT INTO {table} (region, user_id, timestamp, value) VALUES (?, ?, ?, ?)"
        )

        # Insert data
        regions = ["US", "EU", "ASIA"]
        for region in regions:
            for user in range(5):
                for ts in range(10):
                    value = user * 10 + ts
                    await session.execute(insert_stmt, (region, user, ts, float(value)))

        # When - valid predicates with all partition keys
        reader = CassandraDataFrameReader(session, table)

        predicates = [
            {"column": "region", "operator": "=", "value": "US"},
            {"column": "user_id", "operator": "=", "value": 3},
        ]

        df = await reader.read(predicates=predicates, require_partition_key_predicate=True)

        # Then
        result = df.compute()
        assert len(result) == 10  # One user in one region
        assert all(result["region"] == "US")
        assert all(result["user_id"] == 3)
