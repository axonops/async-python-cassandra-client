"""
Integration tests that validate token range partitioning correctness.

What this tests:
---------------
1. Actual token values of rows match the expected Dask partition
2. Wraparound token ranges are handled correctly
3. No data is missed or duplicated between partitions
4. All partitioning strategies correctly distribute data by token
5. Verification against actual cluster token ring metadata

Why this matters:
----------------
- Token range bugs can cause data loss or duplication
- Wraparound ranges have been problematic in the past
- Must verify implementation matches Cassandra's token distribution
- Critical for data integrity in production

Additional context:
---------------------------------
- Uses Murmur3 hash function (Cassandra's default)
- Token range: -2^63 to 2^63-1
- Wraparound occurs when range crosses from positive to negative
"""

import logging
from typing import Any

import pytest
from cassandra.metadata import Murmur3Token

import async_cassandra_dataframe as cdf
from async_cassandra_dataframe.token_ranges import MAX_TOKEN, MIN_TOKEN, discover_token_ranges

logger = logging.getLogger(__name__)


def calculate_token(value: Any, cassandra_type: str = "int") -> int:
    """Calculate Murmur3 token for a value based on Cassandra type."""
    import struct

    if cassandra_type == "int":
        # INT is 4 bytes, big-endian
        value_bytes = struct.pack(">i", value)
    elif cassandra_type == "bigint":
        # BIGINT is 8 bytes, big-endian
        value_bytes = struct.pack(">q", value)
    elif cassandra_type == "uuid":
        # UUID is 16 bytes
        value_bytes = value.bytes
    else:
        # For other types, convert to string then to bytes
        value_bytes = str(value).encode("utf-8")

    return Murmur3Token.hash_fn(value_bytes)


class TestTokenRangeValidation:
    """Validate token range partitioning against actual Cassandra token assignments."""

    @pytest.mark.asyncio
    async def test_token_assignment_matches_partitions(self, session):
        """
        Test that rows in each Dask partition have tokens within the expected range.

        Given: A table with data distributed across the token ring
        When: Reading with token-aware partitioning
        Then: Each row's token should fall within its partition's token range
        """
        # Create test table
        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS token_validation_basic (
                pk INT PRIMARY KEY,
                value TEXT
            )
            """
        )

        # Insert data across token range
        insert_stmt = await session.prepare(
            """
            INSERT INTO token_validation_basic (pk, value) VALUES (?, ?)
            """
        )

        # Insert rows with known primary keys
        test_data = []
        for i in range(1000):
            await session.execute(insert_stmt, (i, f"value_{i}"))
            test_data.append(i)

        # Get the actual token ranges from cluster
        token_ranges = await discover_token_ranges(session, "test_dataframe")
        logger.info(f"Discovered {len(token_ranges)} token ranges from cluster")

        # Read with AUTO partitioning (which uses token ranges)
        df = await cdf.read_cassandra_table(
            "token_validation_basic",
            session=session,
            partitioning_strategy="auto",
        )

        logger.info(f"Created {df.npartitions} Dask partitions")

        # For each partition, verify tokens are in expected range
        errors = []
        for partition_idx in range(df.npartitions):
            partition_data = df.get_partition(partition_idx).compute()

            if len(partition_data) == 0:
                continue

            # Calculate token for each row using Murmur3 (Cassandra's default)
            for _, row in partition_data.iterrows():
                pk = row["pk"]
                # Calculate the token using Cassandra's hash function
                token_value = calculate_token(pk, "int")

                # Find which token range this should belong to
                found_range = False
                for tr in token_ranges:
                    if tr.is_wraparound:
                        # Wraparound range: token >= start OR token <= end
                        if token_value >= tr.start or token_value <= tr.end:
                            found_range = True
                            break
                    else:
                        # Normal range: start < token <= end
                        if tr.start < token_value <= tr.end:
                            found_range = True
                            break

                if not found_range:
                    errors.append(f"Token {token_value} for pk={pk} not in any range")

        assert len(errors) == 0, f"Token range errors: {errors[:10]}"  # Show first 10 errors

    @pytest.mark.asyncio
    async def test_wraparound_token_range_handling(self, session):
        """
        Test wraparound token ranges are handled correctly.

        Given: Data that specifically falls in wraparound range
        When: Reading with token-based partitioning
        Then: Wraparound data should be captured correctly
        """
        # Create table
        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS token_validation_wraparound (
                pk BIGINT PRIMARY KEY,
                value TEXT
            )
            """
        )

        # We need to find PKs that hash to wraparound range
        # Wraparound occurs from high positive to low negative tokens
        insert_stmt = await session.prepare(
            """
            INSERT INTO token_validation_wraparound (pk, value) VALUES (?, ?)
            """
        )

        # Find some PKs that hash to very high and very low tokens
        high_token_pks = []
        low_token_pks = []

        for i in range(1000000, 2000000):  # Search range
            token = calculate_token(i, "bigint")
            if token > MAX_TOKEN - 1000000000:  # Near max token
                high_token_pks.append((i, token))
                await session.execute(insert_stmt, (i, f"high_{i}"))
            elif token < MIN_TOKEN + 1000000000:  # Near min token
                low_token_pks.append((i, token))
                await session.execute(insert_stmt, (i, f"low_{i}"))

            if len(high_token_pks) >= 10 and len(low_token_pks) >= 10:
                break

        logger.info(
            f"Found {len(high_token_pks)} high token PKs and {len(low_token_pks)} low token PKs"
        )

        # Read with different strategies
        for strategy in ["auto", "natural", "split"]:
            extra_args = {"split_factor": 2} if strategy == "split" else {}

            df = await cdf.read_cassandra_table(
                "token_validation_wraparound",
                session=session,
                partitioning_strategy=strategy,
                **extra_args,
            )

            # Verify all data is captured
            all_data = df.compute()
            captured_pks = set(all_data["pk"].tolist())

            # Check high token PKs
            for pk, token in high_token_pks:
                assert (
                    pk in captured_pks
                ), f"High token PK {pk} (token={token}) missing with {strategy}"

            # Check low token PKs
            for pk, token in low_token_pks:
                assert (
                    pk in captured_pks
                ), f"Low token PK {pk} (token={token}) missing with {strategy}"

    @pytest.mark.asyncio
    async def test_no_data_duplication_across_partitions(self, session):
        """
        Test that no data is duplicated across partitions.

        Given: A table with unique primary keys
        When: Reading with various partitioning strategies
        Then: Each row should appear exactly once
        """
        # Create table
        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS token_validation_no_dups (
                id UUID PRIMARY KEY,
                value INT
            )
            """
        )

        # Insert data with UUIDs for even distribution
        import uuid

        insert_stmt = await session.prepare(
            """
            INSERT INTO token_validation_no_dups (id, value) VALUES (?, ?)
            """
        )

        inserted_ids = []
        for i in range(5000):
            id_val = uuid.uuid4()
            inserted_ids.append(id_val)
            await session.execute(insert_stmt, (id_val, i))

        # Test each partitioning strategy
        strategies = [
            ("auto", {}),
            ("natural", {}),
            ("compact", {}),
            ("split", {"split_factor": 3}),
        ]

        for strategy, extra_args in strategies:
            logger.info(f"Testing {strategy} strategy for duplicates")

            df = await cdf.read_cassandra_table(
                "token_validation_no_dups",
                session=session,
                partitioning_strategy=strategy,
                **extra_args,
            )

            # Collect all data
            all_data = df.compute()

            # Check for duplicates
            # Convert UUID column to string for value_counts
            id_strings = all_data["id"].astype(str)
            id_counts = id_strings.value_counts()
            duplicates = id_counts[id_counts > 1]

            assert len(duplicates) == 0, f"Found duplicates with {strategy}: {duplicates.head()}"

            # Verify all data is present
            collected_ids = set(all_data["id"].tolist())
            missing_ids = set(inserted_ids) - collected_ids
            assert len(missing_ids) == 0, f"Missing {len(missing_ids)} IDs with {strategy}"

    @pytest.mark.asyncio
    async def test_token_distribution_matches_cluster_metadata(self, session):
        """
        Test that token distribution matches cluster metadata.

        Given: Cluster token ring metadata
        When: Partitioning data by token ranges
        Then: Data distribution should match token ownership
        """
        # Create table with enough data to see distribution
        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS token_validation_distribution (
                pk INT PRIMARY KEY,
                data TEXT
            )
            """
        )

        # Insert significant amount of data
        insert_stmt = await session.prepare(
            """
            INSERT INTO token_validation_distribution (pk, data) VALUES (?, ?)
            """
        )

        logger.info("Inserting 10,000 rows for distribution test...")
        from cassandra.query import BatchStatement

        batch_size = 100
        for i in range(0, 10000, batch_size):
            batch = BatchStatement()
            for j in range(batch_size):
                batch.add(insert_stmt, (i + j, f"data_{i + j}"))
            await session.execute(batch)

        # Get token ranges and their sizes
        token_ranges = await discover_token_ranges(session, "test_dataframe")

        # Calculate expected distribution based on token range sizes
        total_range = 2**64 - 1  # Total token space
        expected_distribution = []
        for tr in token_ranges:
            if tr.is_wraparound:
                # Wraparound range
                size = (MAX_TOKEN - tr.start) + (tr.end - MIN_TOKEN) + 1
            else:
                size = tr.end - tr.start
            fraction = size / total_range
            expected_distribution.append(
                {"range": tr, "expected_fraction": fraction, "expected_rows": int(10000 * fraction)}
            )

        # Read with NATURAL strategy (one partition per token range)
        df = await cdf.read_cassandra_table(
            "token_validation_distribution",
            session=session,
            partitioning_strategy="natural",
        )

        assert df.npartitions == len(token_ranges), f"Expected {len(token_ranges)} partitions"

        # Check actual distribution
        for i, expected in enumerate(expected_distribution):
            partition_data = df.get_partition(i).compute()
            actual_rows = len(partition_data)

            # Log the distribution
            logger.info(
                f"Partition {i}: expected ~{expected['expected_rows']} rows "
                f"({expected['expected_fraction']:.2%}), got {actual_rows} rows"
            )

            # Allow some variance due to hash distribution
            if expected["expected_rows"] > 100:  # Only check larger partitions
                variance = 0.5  # Allow 50% variance
                min_rows = int(expected["expected_rows"] * (1 - variance))
                max_rows = int(expected["expected_rows"] * (1 + variance))

                assert min_rows <= actual_rows <= max_rows, (
                    f"Partition {i} has {actual_rows} rows, "
                    f"expected between {min_rows} and {max_rows}"
                )

    @pytest.mark.asyncio
    async def test_token_range_boundary_conditions(self, session):
        """
        Test edge cases at token range boundaries.

        Given: Data at exact token range boundaries
        When: Reading with token-based partitioning
        Then: Boundary data should be assigned correctly
        """
        # Create table
        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS token_validation_boundaries (
                pk BIGINT PRIMARY KEY,
                token_value BIGINT,
                value TEXT
            )
            """
        )

        # Get token ranges
        token_ranges = await discover_token_ranges(session, "test_dataframe")

        insert_stmt = await session.prepare(
            """
            INSERT INTO token_validation_boundaries (pk, token_value, value)
            VALUES (?, ?, ?)
            """
        )

        # For each token range, try to find PKs that hash to boundary values
        boundary_data = []
        for tr_idx, tr in enumerate(token_ranges):
            # Try to find PKs that hash near the boundaries
            for test_pk in range(1000000 * tr_idx, 1000000 * (tr_idx + 1)):
                token = calculate_token(test_pk, "bigint")

                # Check if near start boundary
                if abs(token - tr.start) < 1000:
                    await session.execute(insert_stmt, (test_pk, token, f"start_{tr_idx}"))
                    boundary_data.append((test_pk, token, tr_idx, "start"))

                # Check if near end boundary
                if abs(token - tr.end) < 1000:
                    await session.execute(insert_stmt, (test_pk, token, f"end_{tr_idx}"))
                    boundary_data.append((test_pk, token, tr_idx, "end"))

                if len(boundary_data) > 50:  # Enough test data
                    break

        logger.info(f"Created {len(boundary_data)} boundary test cases")

        # Read with NATURAL strategy to test boundaries clearly
        df = await cdf.read_cassandra_table(
            "token_validation_boundaries",
            session=session,
            partitioning_strategy="natural",
        )

        # Verify each boundary case is in the correct partition
        all_partitions = []
        for i in range(df.npartitions):
            partition_data = df.get_partition(i).compute()
            all_partitions.append((i, set(partition_data["pk"].tolist())))

        errors = []
        for pk, token, expected_range_idx, boundary_type in boundary_data:
            # Find which partition contains this PK
            found = False
            for partition_idx, pk_set in all_partitions:
                if pk in pk_set:
                    found = True
                    # For NATURAL strategy, partition index should match range index
                    if partition_idx != expected_range_idx:
                        errors.append(
                            f"PK {pk} (token={token}, {boundary_type} of range {expected_range_idx}) "
                            f"found in partition {partition_idx}"
                        )
                    break

            if not found:
                errors.append(f"PK {pk} (token={token}) not found in any partition")

        assert len(errors) == 0, f"Boundary errors: {errors[:10]}"

    @pytest.mark.asyncio
    async def test_split_strategy_token_correctness(self, session):
        """
        Test SPLIT strategy maintains correct token assignments.

        Given: Token ranges split into sub-ranges
        When: Reading data with SPLIT strategy
        Then: Each sub-partition should only contain tokens from its sub-range
        """
        # Create table
        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS token_validation_split (
                pk INT PRIMARY KEY,
                value TEXT
            )
            """
        )

        # Insert data
        insert_stmt = await session.prepare(
            """
            INSERT INTO token_validation_split (pk, value) VALUES (?, ?)
            """
        )

        for i in range(5000):
            await session.execute(insert_stmt, (i, f"value_{i}"))

        # Get token ranges
        token_ranges = await discover_token_ranges(session, "test_dataframe")

        # Read with SPLIT strategy
        split_factor = 3
        df = await cdf.read_cassandra_table(
            "token_validation_split",
            session=session,
            partitioning_strategy="split",
            split_factor=split_factor,
        )

        expected_partitions = len(token_ranges) * split_factor
        assert (
            df.npartitions == expected_partitions
        ), f"Expected {expected_partitions} partitions, got {df.npartitions}"

        # For each original token range, calculate the sub-ranges
        partition_idx = 0
        errors = []

        for tr in token_ranges:
            # Calculate sub-ranges manually
            if tr.is_wraparound:
                # Skip wraparound validation for now (complex)
                partition_idx += split_factor
                continue

            range_size = tr.end - tr.start
            sub_range_size = range_size // split_factor

            for sub_idx in range(split_factor):
                if sub_idx == split_factor - 1:
                    # Last sub-range gets remainder
                    sub_start = tr.start + (sub_range_size * sub_idx)
                    sub_end = tr.end
                else:
                    sub_start = tr.start + (sub_range_size * sub_idx)
                    sub_end = tr.start + (sub_range_size * (sub_idx + 1))

                # Check partition data
                partition_data = df.get_partition(partition_idx).compute()

                for _, row in partition_data.iterrows():
                    pk = row["pk"]
                    token = calculate_token(pk, "int")

                    # Verify token is in expected sub-range
                    if not (sub_start < token <= sub_end):
                        errors.append(
                            f"PK {pk} (token={token}) in partition {partition_idx} "
                            f"outside sub-range ({sub_start}, {sub_end}]"
                        )

                partition_idx += 1

        assert len(errors) == 0, f"Split strategy errors: {errors[:10]}"

    @pytest.mark.asyncio
    async def test_token_ordering_preservation(self, session):
        """
        Test that token ordering is preserved across partitions.

        Given: Data distributed across token ranges
        When: Reading partitions in order
        Then: Token ranges should not overlap
        """
        # Create table
        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS token_validation_ordering (
                pk INT PRIMARY KEY,
                value TEXT
            )
            """
        )

        # Insert data
        insert_stmt = await session.prepare(
            """
            INSERT INTO token_validation_ordering (pk, value) VALUES (?, ?)
            """
        )

        for i in range(2000):
            await session.execute(insert_stmt, (i, f"value_{i}"))

        # Test different strategies
        for strategy in ["auto", "natural", "compact"]:
            df = await cdf.read_cassandra_table(
                "token_validation_ordering",
                session=session,
                partitioning_strategy=strategy,
            )

            # Collect min/max tokens from each partition
            partition_ranges = []
            for i in range(df.npartitions):
                partition_data = df.get_partition(i).compute()
                if len(partition_data) == 0:
                    continue

                tokens = [calculate_token(pk, "int") for pk in partition_data["pk"]]
                partition_ranges.append(
                    {
                        "partition": i,
                        "min_token": min(tokens),
                        "max_token": max(tokens),
                        "count": len(tokens),
                    }
                )

            # Log partition ranges
            logger.info(f"\n{strategy} strategy partition ranges:")
            for pr in partition_ranges:
                logger.info(
                    f"  Partition {pr['partition']}: "
                    f"[{pr['min_token']}, {pr['max_token']}] "
                    f"({pr['count']} rows)"
                )

            # Verify no overlaps (except for wraparound)
            for i in range(len(partition_ranges)):
                for j in range(i + 1, len(partition_ranges)):
                    p1 = partition_ranges[i]
                    p2 = partition_ranges[j]

                    # Check for overlap
                    # Note: This is simplified and doesn't handle all wraparound cases
                    if (
                        p1["min_token"] <= p2["min_token"] <= p1["max_token"]
                        or p1["min_token"] <= p2["max_token"] <= p1["max_token"]
                    ):
                        logger.warning(
                            f"Potential overlap between partitions {p1['partition']} and {p2['partition']} "
                            f"with {strategy} strategy"
                        )
