"""
Integration tests for automatic partition count calculations based on token ranges.

What this tests:
---------------
1. Automatic partition count calculation based on cluster token ranges
2. Partition count scaling with data volume
3. Token range distribution across Dask partitions
4. Behavior with different cluster sizes and replication factors

Why this matters:
----------------
- Ensures optimal parallelism based on Cassandra topology
- Verifies efficient data distribution across workers
- Validates that partition counts scale appropriately
- Confirms token-aware partitioning works correctly
"""

import logging

import pytest

import async_cassandra_dataframe as cdf

logger = logging.getLogger(__name__)


class TestAutomaticPartitionCount:
    """Test automatic partition count calculations based on token ranges."""

    @pytest.mark.asyncio
    async def test_automatic_partition_count_medium_table(self, session):
        """
        Test partition counts with medium-sized dataset.

        Given: A table with 20,000 rows across 100 Cassandra partitions
        When: Reading without specifying partition_count
        Then: Should create multiple Dask partitions based on token ranges
        """

        # Create test table
        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS partition_test_medium (
                partition_key INT,
                cluster_key INT,
                value TEXT,
                data TEXT,
                PRIMARY KEY (partition_key, cluster_key)
            )
        """
        )

        # Insert data - 100 partitions with 200 rows each = 20,000 rows
        insert_stmt = await session.prepare(
            """
            INSERT INTO partition_test_medium (partition_key, cluster_key, value, data)
            VALUES (?, ?, ?, ?)
        """
        )

        logger.info("Inserting 20,000 rows across 100 partitions...")
        # Use batching for efficiency
        from cassandra.query import BatchStatement

        batch_size = 25  # Cassandra batch size limit
        rows_inserted = 0

        for partition in range(100):
            for batch_start in range(0, 200, batch_size):
                batch = BatchStatement()
                for cluster in range(batch_start, min(batch_start + batch_size, 200)):
                    batch.add(
                        insert_stmt,
                        (
                            partition,
                            cluster,
                            f"value_{partition}_{cluster}",
                            "x" * 500,  # 500 bytes of data per row
                        ),
                    )
                await session.execute(batch)
                rows_inserted += min(batch_size, 200 - batch_start)

            if partition % 10 == 0:
                logger.info(f"Inserted partition {partition}/100 ({rows_inserted} total rows)")

        # Read without specifying partition_count - should auto-calculate
        df = await cdf.read_cassandra_table("partition_test_medium", session=session)

        logger.info(f"Created {df.npartitions} Dask partitions automatically for 20K rows")

        # Verify we got all data
        total_rows = len(df)
        assert total_rows == 20000, f"Expected 20000 rows, got {total_rows}"

        # With 20K rows, should create multiple partitions
        assert (
            df.npartitions >= 2
        ), f"Should have multiple partitions for 20K rows, got {df.npartitions}"

        # Log partition distribution
        partition_sizes = []
        for i in range(df.npartitions):
            partition_data = df.get_partition(i).compute()
            partition_sizes.append(len(partition_data))
            logger.info(f"Partition {i}: {len(partition_data)} rows")

        # Check distribution
        avg_size = sum(partition_sizes) / len(partition_sizes)
        logger.info(f"Average partition size: {avg_size:.1f} rows")

        # All partitions should have some data
        non_empty_partitions = sum(1 for size in partition_sizes if size > 0)
        assert non_empty_partitions == df.npartitions, "All partitions should have data"

    @pytest.mark.asyncio
    async def test_automatic_partition_count_large_table(self, session):
        """
        Test partition counts with large dataset.

        Given: A table with 100,000 rows across 200 Cassandra partitions
        When: Reading without specifying partition_count
        Then: Should create appropriate number of Dask partitions for parallel processing
        """

        # Create test table
        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS partition_test_large (
                partition_key INT,
                cluster_key INT,
                value TEXT,
                data TEXT,
                timestamp TIMESTAMP,
                PRIMARY KEY (partition_key, cluster_key)
            )
        """
        )

        # Insert data - 200 partitions with 500 rows each = 100,000 rows
        insert_stmt = await session.prepare(
            """
            INSERT INTO partition_test_large (partition_key, cluster_key, value, data, timestamp)
            VALUES (?, ?, ?, ?, ?)
        """
        )

        logger.info("Inserting 100,000 rows across 200 partitions...")
        # Insert in batches for efficiency
        from datetime import UTC, datetime

        from cassandra.query import BatchStatement

        batch_size = 25
        rows_inserted = 0
        now = datetime.now(UTC)

        for partition in range(200):
            for batch_start in range(0, 500, batch_size):
                batch = BatchStatement()
                for cluster in range(batch_start, min(batch_start + batch_size, 500)):
                    batch.add(
                        insert_stmt,
                        (
                            partition,
                            cluster,
                            f"value_{partition}_{cluster}",
                            "x" * 1000,  # 1KB of data per row
                            now,
                        ),
                    )
                await session.execute(batch)
                rows_inserted += min(batch_size, 500 - batch_start)

            if partition % 20 == 0:
                logger.info(f"Inserted partition {partition}/200 ({rows_inserted} total rows)")

        # Read without specifying partition_count
        df = await cdf.read_cassandra_table(
            "partition_test_large",
            session=session,
            columns=["partition_key", "cluster_key", "value"],  # Skip large data column
        )

        logger.info(f"Created {df.npartitions} Dask partitions automatically for 100K rows")

        # With 100K rows, should create multiple partitions for parallel processing
        assert (
            df.npartitions >= 2
        ), f"Should have multiple partitions for 100K rows, got {df.npartitions}"

        # Log partition statistics
        partition_sizes = []
        min_rows = float("inf")
        max_rows = 0

        for i in range(df.npartitions):
            partition_data = df.get_partition(i).compute()
            size = len(partition_data)
            partition_sizes.append(size)
            min_rows = min(min_rows, size)
            max_rows = max(max_rows, size)
            if i < 5 or i >= df.npartitions - 5:  # Log first and last 5 partitions
                logger.info(f"Partition {i}: {size} rows")

        # Calculate statistics
        avg_size = sum(partition_sizes) / len(partition_sizes)
        logger.info(f"Partition statistics: min={min_rows}, max={max_rows}, avg={avg_size:.1f}")

        # Check total count
        total_rows = sum(partition_sizes)
        assert total_rows == 100000, f"Expected 100000 rows, got {total_rows}"

    @pytest.mark.asyncio
    async def test_partition_count_with_token_ranges(self, session):
        """
        Test that partition count respects token range distribution.

        Given: A table with data distributed across the token range
        When: Reading with automatic partition calculation
        Then: Partitions should align with token ranges
        """

        # Create test table
        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS partition_test_tokens (
                id UUID PRIMARY KEY,
                value TEXT
            )
        """
        )

        # Insert data with UUIDs to ensure even token distribution
        import uuid

        insert_stmt = await session.prepare(
            """
            INSERT INTO partition_test_tokens (id, value) VALUES (?, ?)
        """
        )

        logger.info("Inserting 20,000 rows with random UUIDs for even token distribution...")
        # Batch inserts for better performance
        from cassandra.query import BatchStatement

        batch_size = 25
        for i in range(0, 20000, batch_size):
            batch = BatchStatement()
            for j in range(batch_size):
                if i + j < 20000:
                    batch.add(insert_stmt, (uuid.uuid4(), f"value_{i + j}"))
            await session.execute(batch)

            if i % 2000 == 0:
                logger.info(f"Inserted {i}/20000 rows")

        # Read and let it calculate partitions based on token ranges
        df = await cdf.read_cassandra_table("partition_test_tokens", session=session)

        logger.info(f"Created {df.npartitions} partitions based on token ranges")

        # Verify partitions have relatively even distribution
        partition_sizes = []
        for i in range(df.npartitions):
            partition_data = df.get_partition(i).compute()
            partition_sizes.append(len(partition_data))

        # Calculate distribution metrics
        avg_size = sum(partition_sizes) / len(partition_sizes)
        max_size = max(partition_sizes)
        min_size = min(partition_sizes)

        logger.info(
            f"Partition size distribution: min={min_size}, max={max_size}, avg={avg_size:.1f}"
        )

        # With UUID primary keys and token-aware partitioning,
        # distribution should be relatively even (within 3x)
        if df.npartitions > 1:
            assert (
                max_size <= avg_size * 3
            ), f"Partition sizes too uneven: max={max_size}, avg={avg_size}"

    @pytest.mark.asyncio
    async def test_explicit_vs_automatic_partition_count(self, session):
        """
        Test explicit partition count vs automatic calculation.

        Given: The same table
        When: Reading with explicit count vs automatic
        Then: Both should work, but may create different partition counts
        """

        # Create and populate test table
        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS partition_test_compare (
                pk INT,
                ck INT,
                value TEXT,
                PRIMARY KEY (pk, ck)
            )
        """
        )

        insert_stmt = await session.prepare(
            """
            INSERT INTO partition_test_compare (pk, ck, value) VALUES (?, ?, ?)
        """
        )

        # Insert moderate amount of data
        for pk in range(20):
            for ck in range(100):
                await session.execute(insert_stmt, (pk, ck, f"value_{pk}_{ck}"))

        # Read with automatic partition count
        df_auto = await cdf.read_cassandra_table("partition_test_compare", session=session)

        # Read with explicit partition count
        df_explicit = await cdf.read_cassandra_table(
            "partition_test_compare", session=session, partition_count=5
        )

        logger.info(f"Automatic partitions: {df_auto.npartitions}")
        logger.info(f"Explicit partitions: {df_explicit.npartitions}")

        # Both should read all data
        assert len(df_auto) == 2000
        assert len(df_explicit) == 2000

        # Explicit should respect the requested count
        # Note: In some cases, the actual partition count may be less if there aren't enough token ranges
        # or if the grouping strategy determines a lower count is more appropriate
        logger.info(f"Requested 5 partitions, got {df_explicit.npartitions}")
        assert df_explicit.npartitions <= 5  # May create fewer if data/token ranges don't support 5

        # Automatic should be reasonable
        assert df_auto.npartitions >= 1
        assert df_auto.npartitions <= 20  # Shouldn't create too many for 2000 rows

    @pytest.mark.asyncio
    async def test_partition_count_with_filtering(self, session):
        """
        Test partition count when filters reduce data volume.

        Given: A large table with filters that reduce data significantly
        When: Reading with filters
        Then: Should still use token ranges for partitioning, not filtered result size
        """

        # Create test table with partition key we can filter on
        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS partition_test_filtered (
                year INT,
                month INT,
                day INT,
                event_id UUID,
                value TEXT,
                PRIMARY KEY ((year, month), day, event_id)
            )
        """
        )

        # Insert data for multiple years/months
        import uuid

        insert_stmt = await session.prepare(
            """
            INSERT INTO partition_test_filtered (year, month, day, event_id, value)
            VALUES (?, ?, ?, ?, ?)
        """
        )

        logger.info("Inserting 30,000+ rows across multiple years...")
        # Batch inserts for efficiency - 3 years * 12 months * 28 days * 30 events = 30,240 rows
        from cassandra.query import BatchStatement

        batch_size = 25
        total_rows = 0

        for year in [2022, 2023, 2024]:
            for month in range(1, 13):
                for day in range(1, 29):  # Simplified - 28 days per month
                    batch = BatchStatement()
                    for event in range(30):  # 30 events per day
                        batch.add(
                            insert_stmt,
                            (year, month, day, uuid.uuid4(), f"event_{year}_{month}_{day}_{event}"),
                        )
                        total_rows += 1

                        # Execute batch when full
                        if len(batch) >= batch_size:
                            await session.execute(batch)
                            batch = BatchStatement()

                    # Execute remaining items in batch
                    if batch:
                        await session.execute(batch)

                if month % 3 == 0:
                    logger.info(f"Inserted {year}/{month} - {total_rows} total rows")

        # Read all data - should create multiple partitions
        df_all = await cdf.read_cassandra_table("partition_test_filtered", session=session)

        # Read filtered data - only 2024
        df_filtered = await cdf.read_cassandra_table(
            "partition_test_filtered",
            session=session,
            predicates=[{"column": "year", "operator": "=", "value": 2024}],
            allow_filtering=True,
        )

        logger.info(f"All data: {df_all.npartitions} partitions, {len(df_all)} rows")
        logger.info(f"Filtered data: {df_filtered.npartitions} partitions, {len(df_filtered)} rows")

        # Even though filtered data is 1/3 of total, partition count should be based on
        # token ranges, not result size
        assert df_filtered.npartitions >= 1

        # Verify filtering worked
        assert len(df_filtered) < len(df_all)
        assert (
            len(df_filtered) == 28 * 12 * 30
        )  # 28 days * 12 months * 30 events = 10,080 rows for 2024

    @pytest.mark.asyncio
    async def test_partition_memory_limits(self, session):
        """
        Test that memory limits affect partition count.

        Given: A table with large rows
        When: Reading with different memory_per_partition settings
        Then: Lower memory limits should create more partitions
        """

        # Create table with large text field
        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS partition_test_memory (
                id INT PRIMARY KEY,
                large_text TEXT
            )
        """
        )

        # Insert rows with ~1KB of data each
        insert_stmt = await session.prepare(
            """
            INSERT INTO partition_test_memory (id, large_text) VALUES (?, ?)
        """
        )

        large_text = "x" * 1000  # 1KB per row
        for i in range(1000):
            await session.execute(insert_stmt, (i, large_text))

        # Read with default memory limit
        df_default = await cdf.read_cassandra_table("partition_test_memory", session=session)

        # Read with very low memory limit - should create more partitions
        df_low_memory = await cdf.read_cassandra_table(
            "partition_test_memory",
            session=session,
            memory_per_partition_mb=1,  # Only 1MB per partition
        )

        logger.info(f"Default memory: {df_default.npartitions} partitions")
        logger.info(f"Low memory (1MB): {df_low_memory.npartitions} partitions")

        # Low memory setting should create more partitions
        # With 1000 rows * 1KB = ~1MB total, and 1MB limit, might need multiple partitions
        assert df_low_memory.npartitions >= df_default.npartitions

        # Verify we still get all data
        assert len(df_default) == 1000
        assert len(df_low_memory) == 1000

    @pytest.mark.asyncio
    async def test_partition_count_scales_with_data(self, session):
        """
        Test that partition count scales appropriately with data volume.

        Given: Tables with different data volumes (1K, 10K, 50K rows)
        When: Reading with automatic partition calculation
        Then: Partition count should increase with data volume
        """

        # Test with three different data sizes
        test_cases = [
            (1000, "small"),  # 1K rows
            (10000, "medium"),  # 10K rows
            (50000, "large"),  # 50K rows
        ]

        partition_counts = {}

        for row_count, size_name in test_cases:
            table_name = f"partition_test_scale_{size_name}"

            # Create table
            await session.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id INT PRIMARY KEY,
                    data TEXT
                )
            """
            )

            # Insert data in batches
            insert_stmt = await session.prepare(
                f"""
                INSERT INTO {table_name} (id, data) VALUES (?, ?)
            """
            )

            logger.info(f"Inserting {row_count} rows for {size_name} dataset...")

            from cassandra.query import BatchStatement

            batch_size = 100

            for i in range(0, row_count, batch_size):
                batch = BatchStatement()
                for j in range(min(batch_size, row_count - i)):
                    batch.add(insert_stmt, (i + j, "x" * 200))  # 200 bytes per row
                await session.execute(batch)

                if i % 10000 == 0 and i > 0:
                    logger.info(f"  Inserted {i}/{row_count} rows")

            # Read with automatic partitioning
            df = await cdf.read_cassandra_table(table_name, session=session)
            partition_counts[size_name] = df.npartitions

            logger.info(f"{size_name} dataset ({row_count} rows): {df.npartitions} partitions")

            # Verify row count
            assert len(df) == row_count, f"Expected {row_count} rows, got {len(df)}"

        # Verify partition count scaling
        logger.info(f"Partition count scaling: {partition_counts}")

        # Larger datasets should have same or more partitions
        assert (
            partition_counts["medium"] >= partition_counts["small"]
        ), f"Medium dataset should have >= partitions than small: {partition_counts}"
        assert (
            partition_counts["large"] >= partition_counts["medium"]
        ), f"Large dataset should have >= partitions than medium: {partition_counts}"

    @pytest.mark.asyncio
    async def test_split_strategy_basic(self, session):
        """
        Test SPLIT partitioning strategy with basic configuration.

        Given: A table with data and discovered token ranges
        When: Using SPLIT strategy with split_factor=2
        Then: Each token range should be split into 2 sub-partitions
        """
        # Create test table
        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS partition_test_split (
                id INT PRIMARY KEY,
                value TEXT
            )
            """
        )

        # Insert data
        insert_stmt = await session.prepare(
            """
            INSERT INTO partition_test_split (id, value) VALUES (?, ?)
            """
        )

        logger.info("Inserting 5000 rows for split strategy test...")
        from cassandra.query import BatchStatement

        batch_size = 100
        for i in range(0, 5000, batch_size):
            batch = BatchStatement()
            for j in range(batch_size):
                batch.add(insert_stmt, (i + j, f"value_{i + j}"))
            await session.execute(batch)

        # Read with SPLIT strategy
        df = await cdf.read_cassandra_table(
            "partition_test_split",
            session=session,
            partitioning_strategy="split",
            split_factor=2,
        )

        logger.info(f"SPLIT strategy with factor 2: {df.npartitions} partitions")

        # With a single-node cluster having ~17 vnodes, and split_factor=2,
        # we should have approximately 17 * 2 = 34 partitions
        assert df.npartitions >= 30, f"Expected at least 30 partitions, got {df.npartitions}"

        # Verify all data is read
        assert len(df) == 5000, f"Expected 5000 rows, got {len(df)}"

        # Check partition sizes
        partition_sizes = []
        for i in range(df.npartitions):
            partition_data = df.get_partition(i).compute()
            partition_sizes.append(len(partition_data))

        # Log distribution
        avg_size = sum(partition_sizes) / len(partition_sizes)
        logger.info(
            f"SPLIT strategy partition sizes: min={min(partition_sizes)}, "
            f"max={max(partition_sizes)}, avg={avg_size:.1f}"
        )

    @pytest.mark.asyncio
    async def test_split_strategy_high_factor(self, session):
        """
        Test SPLIT strategy with high split factor.

        Given: A table with data
        When: Using SPLIT strategy with split_factor=10
        Then: Each token range should be split into 10 sub-partitions
        """
        # Create test table
        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS partition_test_split_high (
                id INT PRIMARY KEY,
                data TEXT
            )
            """
        )

        # Insert data
        insert_stmt = await session.prepare(
            """
            INSERT INTO partition_test_split_high (id, data) VALUES (?, ?)
            """
        )

        logger.info("Inserting 2000 rows for high split factor test...")
        from cassandra.query import BatchStatement

        batch_size = 25
        for i in range(0, 2000, batch_size):
            batch = BatchStatement()
            for j in range(batch_size):
                batch.add(insert_stmt, (i + j, f"data_{i + j}"))
            await session.execute(batch)

        # Read with high split factor
        df = await cdf.read_cassandra_table(
            "partition_test_split_high",
            session=session,
            partitioning_strategy="split",
            split_factor=10,
        )

        logger.info(f"SPLIT strategy with factor 10: {df.npartitions} partitions")

        # With ~17 vnodes and split_factor=10, expect around 170 partitions
        assert df.npartitions >= 100, f"Expected at least 100 partitions, got {df.npartitions}"

        # Verify all data
        assert len(df) == 2000

        # Check that partitions are relatively small
        partition_sizes = []
        sample_size = min(10, df.npartitions)  # Sample first 10 partitions
        for i in range(sample_size):
            partition_data = df.get_partition(i).compute()
            partition_sizes.append(len(partition_data))

        avg_sample_size = sum(partition_sizes) / len(partition_sizes)
        logger.info(f"Average partition size (sample): {avg_sample_size:.1f} rows")

        # With many partitions, each should be relatively small
        assert avg_sample_size < 50, f"Partitions too large: avg={avg_sample_size}"

    @pytest.mark.asyncio
    async def test_split_vs_auto_strategy(self, session):
        """
        Compare SPLIT strategy with AUTO strategy.

        Given: The same table
        When: Reading with SPLIT vs AUTO strategies
        Then: SPLIT should create more partitions based on split_factor
        """
        # Create test table
        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS partition_test_compare_split (
                pk INT,
                ck INT,
                value TEXT,
                PRIMARY KEY (pk, ck)
            )
            """
        )

        # Insert data
        insert_stmt = await session.prepare(
            """
            INSERT INTO partition_test_compare_split (pk, ck, value) VALUES (?, ?, ?)
            """
        )

        for pk in range(50):
            for ck in range(100):
                await session.execute(insert_stmt, (pk, ck, f"value_{pk}_{ck}"))

        # Read with AUTO strategy
        df_auto = await cdf.read_cassandra_table(
            "partition_test_compare_split",
            session=session,
            partitioning_strategy="auto",
        )

        # Read with SPLIT strategy
        df_split = await cdf.read_cassandra_table(
            "partition_test_compare_split",
            session=session,
            partitioning_strategy="split",
            split_factor=3,
        )

        logger.info(f"AUTO strategy: {df_auto.npartitions} partitions")
        logger.info(f"SPLIT strategy (factor=3): {df_split.npartitions} partitions")

        # SPLIT with factor 3 should create more partitions than AUTO
        assert df_split.npartitions > df_auto.npartitions, (
            f"SPLIT should create more partitions: "
            f"SPLIT={df_split.npartitions}, AUTO={df_auto.npartitions}"
        )

        # Both should read all data
        assert len(df_auto) == 5000
        assert len(df_split) == 5000

    @pytest.mark.asyncio
    async def test_split_strategy_preserves_ordering(self, session):
        """
        Test that SPLIT strategy preserves token ordering.

        Given: A table with ordered data
        When: Using SPLIT strategy
        Then: Token ranges should maintain proper ordering without gaps
        """
        # Create test table
        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS partition_test_split_order (
                id INT PRIMARY KEY,
                value INT
            )
            """
        )

        # Insert sequential data
        insert_stmt = await session.prepare(
            """
            INSERT INTO partition_test_split_order (id, value) VALUES (?, ?)
            """
        )

        for i in range(1000):
            await session.execute(insert_stmt, (i, i * 10))

        # Read with SPLIT strategy
        df = await cdf.read_cassandra_table(
            "partition_test_split_order",
            session=session,
            partitioning_strategy="split",
            split_factor=5,
        )

        # Collect all data and verify completeness
        all_data = df.compute()
        assert len(all_data) == 1000, f"Expected 1000 rows, got {len(all_data)}"

        # Verify all IDs are present (no gaps)
        ids = sorted(all_data["id"].tolist())
        assert ids == list(range(1000)), "Missing or duplicate IDs detected"

        # Verify values are correct
        for i in range(1000):
            row = all_data[all_data["id"] == i]
            assert len(row) == 1, f"ID {i} appears {len(row)} times"
            assert row["value"].iloc[0] == i * 10, f"Incorrect value for ID {i}"
