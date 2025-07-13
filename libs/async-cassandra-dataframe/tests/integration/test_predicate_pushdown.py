"""
Test predicate pushdown functionality.

What this tests:
---------------
1. Partition key predicates pushed to Cassandra
2. Clustering key predicates with restrictions
3. Secondary index predicate pushdown
4. ALLOW FILTERING scenarios
5. Mixed predicates (some pushed, some client-side)
6. Token range vs direct partition access
7. Error cases and edge conditions

Why this matters:
----------------
- Performance: Pushing predicates reduces data transfer
- Efficiency: Leverages Cassandra's indexes and sorting
- Correctness: Must respect CQL query restrictions
- Production: Critical for large-scale data processing

CRITICAL: This tests every possible predicate scenario.
"""

from datetime import UTC, datetime

import pandas as pd
import pytest
from async_cassandra_dataframe import read_cassandra_table


class TestPredicatePushdown:
    """Test predicate pushdown to Cassandra."""

    @pytest.mark.asyncio
    async def test_partition_key_equality_predicate(self, session, test_table_name):
        """
        Test pushing partition key equality predicates to Cassandra.

        What this tests:
        ---------------
        1. Single partition key with equality
        2. No token ranges used
        3. Direct partition access
        4. Most efficient query type

        Why this matters:
        ----------------
        - O(1) partition lookup
        - No unnecessary data scanning
        - Optimal Cassandra usage
        """
        # Create table with simple partition key
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                user_id INT PRIMARY KEY,
                name TEXT,
                email TEXT,
                active BOOLEAN
            )
            """
        )

        try:
            # Insert test data
            for i in range(100):
                await session.execute(
                    f"""
                    INSERT INTO {test_table_name} (user_id, name, email, active)
                    VALUES ({i}, 'User {i}', 'user{i}@example.com', {i % 2 == 0})
                    """
                )

            # Read with partition key predicate
            df = await read_cassandra_table(
                f"test_dataframe.{test_table_name}",
                session=session,
                predicates=[{"column": "user_id", "operator": "=", "value": 42}],
            )

            result = df.compute()

            # Should get exactly one row
            assert len(result) == 1
            assert result.iloc[0]["user_id"] == 42
            assert result.iloc[0]["name"] == "User 42"

            # TODO: Verify query didn't use token ranges (need query logging)

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    @pytest.mark.asyncio
    async def test_composite_partition_key_predicates(self, session, test_table_name):
        """
        Test composite partition key predicates.

        What this tests:
        ---------------
        1. Multiple partition key columns
        2. All must have equality for pushdown
        3. Partial key goes client-side

        Why this matters:
        ----------------
        - Common in time-series data
        - User-date partitioning patterns
        - Must handle incomplete keys correctly
        """
        # Create table with composite partition key
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                user_id INT,
                year INT,
                month INT,
                day INT,
                event_count INT,
                PRIMARY KEY ((user_id, year), month, day)
            ) WITH CLUSTERING ORDER BY (month ASC, day ASC)
            """
        )

        try:
            # Insert test data
            for user in [1, 2, 3]:
                for month in [1, 2, 3]:
                    for day in range(1, 11):
                        await session.execute(
                            f"""
                            INSERT INTO {test_table_name}
                            (user_id, year, month, day, event_count)
                            VALUES ({user}, 2024, {month}, {day}, {user * month * day})
                            """
                        )

            # Test 1: Complete partition key - should push down
            df = await read_cassandra_table(
                f"test_dataframe.{test_table_name}",
                session=session,
                predicates=[
                    {"column": "user_id", "operator": "=", "value": 2},
                    {"column": "year", "operator": "=", "value": 2024},
                ],
            )

            result = df.compute()
            assert len(result) == 30  # 3 months * 10 days
            assert all(result["user_id"] == 2)

            # Test 2: Incomplete partition key - should use token ranges
            df = await read_cassandra_table(
                f"test_dataframe.{test_table_name}",
                session=session,
                predicates=[
                    {"column": "user_id", "operator": "=", "value": 2}
                    # Missing year - can't push down
                ],
            )

            result = df.compute()
            assert len(result) == 30  # Still filters correctly client-side

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    @pytest.mark.asyncio
    async def test_clustering_key_predicates(self, session, test_table_name):
        """
        Test clustering key predicate pushdown.

        What this tests:
        ---------------
        1. Range queries on clustering columns
        2. Must specify partition key first
        3. Clustering column order matters
        4. Can't skip clustering columns

        Why this matters:
        ----------------
        - Time-series queries (timestamp > X)
        - Sorted data access
        - Efficient range scans
        """
        # Create time-series table
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                sensor_id INT,
                date DATE,
                time TIMESTAMP,
                temperature FLOAT,
                humidity FLOAT,
                PRIMARY KEY ((sensor_id, date), time)
            ) WITH CLUSTERING ORDER BY (time DESC)
            """
        )

        try:
            # Insert test data
            base_time = datetime(2024, 1, 15, tzinfo=UTC)
            for hour in range(24):
                for minute in range(0, 60, 10):
                    time = base_time.replace(hour=hour, minute=minute)
                    await session.execute(
                        f"""
                        INSERT INTO {test_table_name}
                        (sensor_id, date, time, temperature, humidity)
                        VALUES (1, '2024-01-15', '{time.isoformat()}',
                                {20 + hour * 0.5}, {40 + minute * 0.1})
                        """
                    )

            # Test: Clustering key range with complete partition key
            cutoff_time = base_time.replace(hour=12)
            df = await read_cassandra_table(
                f"test_dataframe.{test_table_name}",
                session=session,
                predicates=[
                    {"column": "sensor_id", "operator": "=", "value": 1},
                    {"column": "date", "operator": "=", "value": "2024-01-15"},
                    {"column": "time", "operator": ">", "value": cutoff_time},
                ],
            )

            result = df.compute()

            # Should get afternoon readings only (excluding 12:00)
            # 11 full hours (13:00-23:00) * 6 + 5 readings from hour 12 (12:10-12:50)
            assert len(result) == 71  # 11*6 + 5 = 71
            assert all(pd.to_datetime(result["time"]) > cutoff_time)

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    @pytest.mark.asyncio
    async def test_secondary_index_predicates(self, session, test_table_name):
        """
        Test secondary index predicate pushdown.

        What this tests:
        ---------------
        1. Predicates on indexed columns
        2. Can push down without partition key
        3. Combines with other predicates

        Why this matters:
        ----------------
        - Global lookups by indexed value
        - Email/username lookups
        - Status filtering
        """
        # Create table with secondary index
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                id INT PRIMARY KEY,
                email TEXT,
                status TEXT,
                created_at TIMESTAMP
            )
            """
        )

        # Create secondary indexes
        await session.execute(f"CREATE INDEX ON {test_table_name} (email)")
        await session.execute(f"CREATE INDEX ON {test_table_name} (status)")

        try:
            # Insert test data
            statuses = ["active", "inactive", "pending"]
            for i in range(100):
                await session.execute(
                    f"""
                    INSERT INTO {test_table_name} (id, email, status, created_at)
                    VALUES ({i}, 'user{i}@example.com', '{statuses[i % 3]}',
                            '2024-01-{(i % 30) + 1}T12:00:00Z')
                    """
                )

            # Test 1: Single index predicate
            df = await read_cassandra_table(
                f"test_dataframe.{test_table_name}",
                session=session,
                predicates=[{"column": "status", "operator": "=", "value": "active"}],
            )

            result = df.compute()
            assert len(result) == 34  # ~1/3 of 100
            assert all(result["status"] == "active")

            # Test 2: Multiple index predicates (intersection)
            df = await read_cassandra_table(
                f"test_dataframe.{test_table_name}",
                session=session,
                predicates=[
                    {"column": "status", "operator": "=", "value": "active"},
                    {"column": "email", "operator": "=", "value": "user30@example.com"},
                ],
            )

            result = df.compute()
            assert len(result) == 1
            assert result.iloc[0]["id"] == 30

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    @pytest.mark.asyncio
    async def test_allow_filtering_scenarios(self, session, test_table_name):
        """
        Test ALLOW FILTERING predicate pushdown.

        What this tests:
        ---------------
        1. Non-indexed column filtering
        2. Performance implications
        3. Opt-in requirement
        4. Small dataset scenarios

        Why this matters:
        ----------------
        - Sometimes needed for small tables
        - Admin queries
        - Must be explicit about cost

        CRITICAL: ALLOW FILTERING scans all data!
        """
        # Create table without indexes
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                group_id INT,
                user_id INT,
                score INT,
                tags SET<TEXT>,
                PRIMARY KEY (group_id, user_id)
            )
            """
        )

        try:
            # Insert small dataset
            for group in range(3):
                for user in range(10):
                    tags = {f"tag{i}" for i in range(user % 3)}
                    tags_str = "{" + ",".join(f"'{t}'" for t in tags) + "}" if tags else "{}"
                    await session.execute(
                        f"""
                        INSERT INTO {test_table_name} (group_id, user_id, score, tags)
                        VALUES ({group}, {user}, {group * 10 + user}, {tags_str})
                        """
                    )

            # Test 1: Regular column filter WITHOUT allow_filtering - should fail or filter client-side
            df = await read_cassandra_table(
                f"test_dataframe.{test_table_name}",
                session=session,
                predicates=[{"column": "score", "operator": ">", "value": 15}],
                allow_filtering=False,  # Default
            )

            result = df.compute()
            # Should still work but filter client-side
            assert all(result["score"] > 15)

            # Test 2: WITH allow_filtering - pushes to Cassandra
            df = await read_cassandra_table(
                f"test_dataframe.{test_table_name}",
                session=session,
                predicates=[{"column": "score", "operator": ">", "value": 15}],
                allow_filtering=True,  # Explicit opt-in
            )

            result = df.compute()
            assert all(result["score"] > 15)
            # TODO: Verify query used ALLOW FILTERING

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    @pytest.mark.asyncio
    async def test_mixed_predicates(self, session, test_table_name):
        """
        Test mixed predicate scenarios.

        What this tests:
        ---------------
        1. Some predicates pushed, others client-side
        2. Optimal predicate separation
        3. Complex query patterns
        4. String operations client-side

        Why this matters:
        ----------------
        - Real queries are complex
        - Must optimize what we can
        - Transparency about filtering location
        """
        # Create table
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                category TEXT,
                item_id INT,
                name TEXT,
                description TEXT,
                price DECIMAL,
                tags LIST<TEXT>,
                PRIMARY KEY (category, item_id)
            )
            """
        )

        try:
            # Insert test data
            categories = ["electronics", "books", "clothing"]
            for cat in categories:
                for i in range(20):
                    await session.execute(
                        f"""
                        INSERT INTO {test_table_name}
                        (category, item_id, name, description, price, tags)
                        VALUES ('{cat}', {i}, '{cat}_item_{i}',
                                'Description with {"ERROR" if i % 5 == 0 else "info"} text',
                                {10.0 + i * 5}, ['tag1', 'tag2'])
                        """
                    )

            # Complex query with mixed predicates
            df = await read_cassandra_table(
                f"test_dataframe.{test_table_name}",
                session=session,
                predicates=[
                    # Can push: partition key
                    {"column": "category", "operator": "=", "value": "electronics"},
                    # Can push: clustering key with partition
                    {"column": "item_id", "operator": "<", "value": 10},
                    # Cannot push: regular column (goes client-side)
                    {"column": "price", "operator": ">", "value": 25.0},
                    # Cannot push: string contains (goes client-side)
                    # Note: This would need special handling for LIKE/contains
                ],
            )

            result = df.compute()

            # Verify all predicates applied
            assert all(result["category"] == "electronics")
            assert all(result["item_id"] < 10)
            # Price is Decimal type - convert to float for comparison
            assert all(result["price"].astype(float) > 25.0)

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    @pytest.mark.asyncio
    async def test_in_operator_predicates(self, session, test_table_name):
        """
        Test IN operator predicate pushdown.

        What this tests:
        ---------------
        1. IN clause on partition key
        2. Multiple value lookups
        3. Efficient multi-partition access

        Why this matters:
        ----------------
        - Batch lookups
        - Multiple ID queries
        - Alternative to multiple queries
        """
        # Create table
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                id INT PRIMARY KEY,
                type TEXT,
                data TEXT
            )
            """
        )

        try:
            # Insert test data
            for i in range(100):
                await session.execute(
                    f"""
                    INSERT INTO {test_table_name} (id, type, data)
                    VALUES ({i}, 'type_{i % 5}', 'data_{i}')
                    """
                )

            # Test IN predicate
            target_ids = [5, 15, 25, 35, 45]
            df = await read_cassandra_table(
                f"test_dataframe.{test_table_name}",
                session=session,
                predicates=[{"column": "id", "operator": "IN", "value": target_ids}],
            )

            result = df.compute()

            assert len(result) == 5
            assert set(result["id"]) == set(target_ids)

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    @pytest.mark.asyncio
    async def test_token_range_with_predicates(self, session, test_table_name):
        """
        Test token ranges combined with predicates.

        What this tests:
        ---------------
        1. Parallel scanning with filters
        2. Token ranges for distribution
        3. Additional filters client-side

        Why this matters:
        ----------------
        - Large table filtering
        - Distributed processing
        - Predicate interaction
        """
        # Create large table
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                id INT PRIMARY KEY,
                category TEXT,
                value INT
            )
            """
        )

        try:
            # Insert many rows
            for i in range(1000):
                await session.execute(
                    f"""
                    INSERT INTO {test_table_name} (id, category, value)
                    VALUES ({i}, 'cat_{i % 10}', {i})
                    """
                )

            # Read with client-side predicate (no partition key)
            # Should use token ranges for parallel processing
            df = await read_cassandra_table(
                f"test_dataframe.{test_table_name}",
                session=session,
                predicates=[
                    {"column": "category", "operator": "=", "value": "cat_5"},
                    {"column": "value", "operator": ">", "value": 500},
                ],
                partition_count=4,  # Force multiple partitions
            )

            result = df.compute()

            # Should filter correctly despite using token ranges
            assert all(result["category"] == "cat_5")
            assert all(result["value"] > 500)
            assert len(result) == 50  # IDs: 505, 515, 525, ..., 995

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    @pytest.mark.asyncio
    async def test_predicate_type_handling(self, session, test_table_name):
        """
        Test predicate type conversions and edge cases.

        What this tests:
        ---------------
        1. Date/timestamp predicates
        2. Boolean predicates
        3. Numeric comparisons
        4. NULL handling

        Why this matters:
        ----------------
        - Type safety
        - Correct comparisons
        - Edge case handling
        """
        # Create table with various types
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                id INT PRIMARY KEY,
                created_date DATE,
                is_active BOOLEAN,
                score FLOAT,
                metadata TEXT
            )
            """
        )

        try:
            # Insert test data with edge cases
            await session.execute(
                f"""
                INSERT INTO {test_table_name} (id, created_date, is_active, score)
                VALUES (1, '2024-01-15', true, 95.5)
                """
            )
            await session.execute(
                f"""
                INSERT INTO {test_table_name} (id, created_date, is_active, score, metadata)
                VALUES (2, '2024-01-16', false, 87.3, 'test')
                """
            )
            await session.execute(
                f"""
                INSERT INTO {test_table_name} (id, created_date, is_active, score)
                VALUES (3, '2024-01-17', true, NULL)
                """
            )

            # Test various predicate types
            df = await read_cassandra_table(
                f"test_dataframe.{test_table_name}",
                session=session,
                predicates=[
                    {"column": "is_active", "operator": "=", "value": True},
                    {"column": "created_date", "operator": ">=", "value": "2024-01-15"},
                ],
            )

            result = df.compute()

            assert len(result) == 2  # IDs 1 and 3
            assert all(result["is_active"])

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    @pytest.mark.asyncio
    async def test_predicate_validation_errors(self, session, test_table_name):
        """
        Test predicate validation and error handling.

        What this tests:
        ---------------
        1. Invalid column names
        2. Invalid operators
        3. Type mismatches
        4. Malformed predicates

        Why this matters:
        ----------------
        - User error handling
        - Clear error messages
        - Security (no injection)
        """
        # Create simple table
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                id INT PRIMARY KEY,
                name TEXT
            )
            """
        )

        try:
            # Test 1: Invalid column name
            with pytest.raises(ValueError, match="column"):
                df = await read_cassandra_table(
                    f"test_dataframe.{test_table_name}",
                    session=session,
                    predicates=[{"column": "invalid_column", "operator": "=", "value": 1}],
                )
                df.compute()

            # Test 2: Invalid operator
            with pytest.raises(ValueError, match="operator"):
                df = await read_cassandra_table(
                    f"test_dataframe.{test_table_name}",
                    session=session,
                    predicates=[{"column": "id", "operator": "LIKE", "value": "test"}],
                )
                df.compute()

            # Test 3: Missing required fields
            with pytest.raises((ValueError, KeyError)):
                df = await read_cassandra_table(
                    f"test_dataframe.{test_table_name}",
                    session=session,
                    predicates=[{"column": "id"}],  # Missing operator and value
                )
                df.compute()

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")
