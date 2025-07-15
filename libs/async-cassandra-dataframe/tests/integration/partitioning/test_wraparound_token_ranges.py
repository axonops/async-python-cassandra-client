"""
Comprehensive tests for wraparound token range handling.

What this tests:
---------------
1. Correct handling of token ranges that wrap from MAX to MIN
2. Data at the edges of the token ring is not lost
3. Queries for wraparound ranges are split correctly
4. All partitioning strategies handle wraparound correctly

Why this matters:
----------------
- Wraparound ranges have been a source of bugs
- Data loss can occur if wraparound is handled incorrectly
- Critical for correctness in production systems
"""

import logging
import struct

import pytest
from cassandra.metadata import Murmur3Token

import async_cassandra_dataframe as cdf
from async_cassandra_dataframe.token_ranges import (
    MAX_TOKEN,
    MIN_TOKEN,
    TokenRange,
    discover_token_ranges,
    generate_token_range_query,
    handle_wraparound_ranges,
)

logger = logging.getLogger(__name__)


class TestWraparoundTokenRanges:
    """Test wraparound token range handling in depth."""

    @pytest.mark.asyncio
    async def test_wraparound_detection(self, session):
        """
        Test that wraparound ranges are correctly identified.

        Given: Token ranges from cluster
        When: Examining ranges
        Then: Last range should be wraparound if it goes from high positive to MIN_TOKEN
        """
        # Get token ranges
        token_ranges = await discover_token_ranges(session, "test_dataframe")

        # Find wraparound ranges
        wraparound_ranges = [tr for tr in token_ranges if tr.is_wraparound]

        logger.info(
            f"Found {len(wraparound_ranges)} wraparound ranges out of {len(token_ranges)} total"
        )

        # Log the ranges for debugging
        for tr in token_ranges[-3:]:  # Last 3 ranges
            logger.info(f"Range: [{tr.start}, {tr.end}], wraparound={tr.is_wraparound}")

    @pytest.mark.asyncio
    async def test_wraparound_query_generation(self, session):
        """
        Test query generation for wraparound ranges.

        Given: A wraparound token range
        When: Generating queries
        Then: Should create proper WHERE clauses
        """
        # Create a wraparound range
        wraparound_range = TokenRange(
            start=MAX_TOKEN - 1000, end=MIN_TOKEN + 1000, replicas=["127.0.0.1"]
        )

        # This should be detected as wraparound
        assert wraparound_range.is_wraparound

        # Split the wraparound range
        split_ranges = handle_wraparound_ranges([wraparound_range])

        # Should be split into 2 ranges
        assert len(split_ranges) == 2

        # First part: from start to MAX_TOKEN
        assert split_ranges[0].start == MAX_TOKEN - 1000
        assert split_ranges[0].end == MAX_TOKEN
        assert not split_ranges[0].is_wraparound

        # Second part: from MIN_TOKEN to end
        assert split_ranges[1].start == MIN_TOKEN
        assert split_ranges[1].end == MIN_TOKEN + 1000
        assert not split_ranges[1].is_wraparound

        # Generate queries for both parts
        query1 = generate_token_range_query("test_keyspace", "test_table", ["pk"], split_ranges[0])
        query2 = generate_token_range_query("test_keyspace", "test_table", ["pk"], split_ranges[1])

        logger.info(f"Query 1 (high tokens): {query1}")
        logger.info(f"Query 2 (low tokens): {query2}")

        # Verify queries
        assert f"token(pk) > {MAX_TOKEN - 1000}" in query1
        assert f"token(pk) <= {MAX_TOKEN}" in query1

        assert f"token(pk) >= {MIN_TOKEN}" in query2
        assert f"token(pk) <= {MIN_TOKEN + 1000}" in query2

    @pytest.mark.asyncio
    async def test_data_at_token_extremes(self, session):
        """
        Test that data at token range extremes is handled correctly.

        Given: Data that hashes to very high and very low tokens
        When: Reading with token-based partitioning
        Then: All extreme data should be captured
        """
        # Create table
        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS wraparound_extremes (
                pk BIGINT PRIMARY KEY,
                token_value BIGINT,
                location TEXT
            )
            """
        )

        insert_stmt = await session.prepare(
            """
            INSERT INTO wraparound_extremes (pk, token_value, location) VALUES (?, ?, ?)
            """
        )

        # Find PKs that hash to extreme tokens
        extreme_data = []

        # Search for high tokens (near MAX_TOKEN)
        logger.info("Searching for PKs with extreme token values...")
        for i in range(0, 10000000, 1000):
            pk_bytes = struct.pack(">q", i)
            token = Murmur3Token.hash_fn(pk_bytes)

            if token > MAX_TOKEN - 100000000:  # Within 100M of MAX
                await session.execute(insert_stmt, (i, token, "near_max"))
                extreme_data.append((i, token, "near_max"))
                logger.info(f"Found near-max PK: {i} -> token {token}")

            elif token < MIN_TOKEN + 100000000:  # Within 100M of MIN
                await session.execute(insert_stmt, (i, token, "near_min"))
                extreme_data.append((i, token, "near_min"))
                logger.info(f"Found near-min PK: {i} -> token {token}")

            if len(extreme_data) >= 20:
                break

        logger.info(f"Found {len(extreme_data)} extreme PKs")

        # Read with different strategies
        for strategy in ["auto", "natural", "split"]:
            extra_args = {"split_factor": 2} if strategy == "split" else {}

            df = await cdf.read_cassandra_table(
                "wraparound_extremes", session=session, partitioning_strategy=strategy, **extra_args
            )

            # Verify all extreme data is captured
            result = df.compute()
            captured_pks = set(result["pk"].tolist())

            missing = []
            for pk, token, location in extreme_data:
                if pk not in captured_pks:
                    missing.append((pk, token, location))

            assert (
                len(missing) == 0
            ), f"Strategy {strategy} missed {len(missing)} extreme PKs: {missing}"

    @pytest.mark.asyncio
    async def test_wraparound_with_real_data_distribution(self, session):
        """
        Test wraparound handling with realistic data distribution.

        Given: Data distributed across entire token ring including wraparound
        When: Reading with partitioning
        Then: Wraparound partition should contain correct data
        """
        # Create table
        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS wraparound_real_dist (
                pk INT PRIMARY KEY,
                value TEXT,
                token_value BIGINT
            )
            """
        )

        # Insert data and track tokens
        insert_stmt = await session.prepare(
            """
            INSERT INTO wraparound_real_dist (pk, value, token_value) VALUES (?, ?, ?)
            """
        )

        token_distribution = []
        for i in range(10000):
            pk_bytes = struct.pack(">i", i)
            token = Murmur3Token.hash_fn(pk_bytes)
            await session.execute(insert_stmt, (i, f"value_{i}", token))
            token_distribution.append((i, token))

        # Sort by token to understand distribution
        token_distribution.sort(key=lambda x: x[1])

        # Log token range coverage
        min_token_in_data = token_distribution[0][1]
        max_token_in_data = token_distribution[-1][1]
        logger.info(f"Token range in data: [{min_token_in_data}, {max_token_in_data}]")

        # Get actual token ranges
        token_ranges = await discover_token_ranges(session, "test_dataframe")

        # Read with NATURAL strategy to get one partition per range
        df = await cdf.read_cassandra_table(
            "wraparound_real_dist",
            session=session,
            partitioning_strategy="natural",
        )

        # For each token range, verify correct data assignment
        for i, tr in enumerate(token_ranges):
            partition_data = df.get_partition(i).compute()
            if len(partition_data) == 0:
                continue

            # Get tokens in this partition
            partition_tokens = partition_data["token_value"].tolist()

            # Verify all tokens belong to this range
            errors = []
            for token in partition_tokens:
                if tr.is_wraparound:
                    # Wraparound: token >= start OR token <= end
                    if not (token >= tr.start or token <= tr.end):
                        errors.append(
                            f"Token {token} outside wraparound range [{tr.start}, {tr.end}]"
                        )
                else:
                    # Normal range
                    if tr.start == MIN_TOKEN:
                        # First range uses >=
                        if not (tr.start <= token <= tr.end):
                            errors.append(
                                f"Token {token} outside first range [{tr.start}, {tr.end}]"
                            )
                    else:
                        # Other ranges use >
                        if not (tr.start < token <= tr.end):
                            errors.append(f"Token {token} outside range ({tr.start}, {tr.end}]")

            assert len(errors) == 0, f"Range {i} errors: {errors[:5]}"

    @pytest.mark.asyncio
    async def test_split_strategy_wraparound_handling(self, session):
        """
        Test that SPLIT strategy correctly handles wraparound ranges.

        Given: Wraparound token ranges
        When: Applying SPLIT strategy
        Then: Wraparound should be handled before splitting
        """
        # Create table
        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS wraparound_split_test (
                pk INT PRIMARY KEY,
                value TEXT
            )
            """
        )

        # Insert data
        insert_stmt = await session.prepare(
            """
            INSERT INTO wraparound_split_test (pk, value) VALUES (?, ?)
            """
        )

        for i in range(5000):
            await session.execute(insert_stmt, (i, f"value_{i}"))

        # Read with SPLIT strategy
        df = await cdf.read_cassandra_table(
            "wraparound_split_test",
            session=session,
            partitioning_strategy="split",
            split_factor=3,
        )

        # Collect all data to ensure nothing is lost
        all_data = df.compute()
        assert len(all_data) == 5000, f"Expected 5000 rows, got {len(all_data)}"

        # Verify no duplicates
        pk_counts = all_data["pk"].value_counts()
        duplicates = pk_counts[pk_counts > 1]
        assert len(duplicates) == 0, f"Found duplicates: {duplicates.head()}"

    @pytest.mark.asyncio
    async def test_fixed_partition_wraparound(self, session):
        """
        Test FIXED strategy with wraparound ranges.

        Given: Request for specific partition count
        When: Token ranges include wraparound
        Then: Should handle correctly without data loss
        """
        # Create table
        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS wraparound_fixed_test (
                pk INT PRIMARY KEY,
                value TEXT
            )
            """
        )

        # Insert data
        insert_stmt = await session.prepare(
            """
            INSERT INTO wraparound_fixed_test (pk, value) VALUES (?, ?)
            """
        )

        for i in range(3000):
            await session.execute(insert_stmt, (i, f"value_{i}"))

        # Read with FIXED strategy
        df = await cdf.read_cassandra_table(
            "wraparound_fixed_test",
            session=session,
            partitioning_strategy="fixed",
            partition_count=10,
        )

        # Should create requested partitions (or close to it)
        assert df.npartitions <= 10

        # Verify all data is captured
        all_data = df.compute()
        assert len(all_data) == 3000, f"Expected 3000 rows, got {len(all_data)}"

        # Log partition sizes for verification
        for i in range(df.npartitions):
            partition_size = len(df.get_partition(i).compute())
            logger.info(f"FIXED partition {i}: {partition_size} rows")
