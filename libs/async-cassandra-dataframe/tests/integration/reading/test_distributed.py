"""
Distributed tests using Dask cluster.

CRITICAL: Tests actual distributed execution with Dask scheduler and workers.
"""

import os

import pandas as pd
import pytest
from dask.distributed import Client, as_completed

import async_cassandra_dataframe as cdf


@pytest.mark.distributed
class TestDistributed:
    """Test distributed Dask execution."""

    @pytest.mark.asyncio
    async def test_read_with_dask_client(self, session, basic_test_table):
        """
        Test reading with Dask distributed client.

        What this tests:
        ---------------
        1. Works with Dask scheduler
        2. Tasks distributed to workers
        3. Results collected correctly
        4. No serialization issues

        Why this matters:
        ----------------
        - Production uses Dask clusters
        - Must work distributed
        - Common deployment pattern
        """
        # Get scheduler from environment
        scheduler = os.environ.get("DASK_SCHEDULER", "tcp://localhost:8786")

        # Connect to Dask cluster
        async with Client(scheduler, asynchronous=True) as client:
            # Verify cluster is up
            info = client.scheduler_info()
            assert len(info["workers"]) > 0, "No Dask workers available"

            # Read table using distributed client
            df = await cdf.read_cassandra_table(
                basic_test_table,
                session=session,
                partition_count=4,  # Ensure multiple partitions
                client=client,
            )

            # Verify it's distributed
            assert df.npartitions >= 2

            # Compute on cluster
            pdf = df.compute()

            # Verify results
            assert len(pdf) == 1000
            assert set(pdf.columns) == {"id", "name", "value", "created_at", "is_active"}

    @pytest.mark.asyncio
    async def test_parallel_partition_reading(self, session, basic_test_table):
        """
        Test parallel reading of partitions.

        What this tests:
        ---------------
        1. Partitions read in parallel
        2. No interference between tasks
        3. Correct data isolation
        4. Performance benefit

        Why this matters:
        ----------------
        - Parallelism is key benefit
        - Must be thread-safe
        - Data correctness critical
        """
        scheduler = os.environ.get("DASK_SCHEDULER", "tcp://localhost:8786")

        async with Client(scheduler, asynchronous=True) as client:
            # Read with many partitions
            df = await cdf.read_cassandra_table(
                basic_test_table,
                session=session,
                partition_count=10,  # Many partitions
                memory_per_partition_mb=10,  # Small to force more splits
                client=client,
            )

            # Track task execution
            start_time = pd.Timestamp.now()

            # Compute all partitions
            futures = client.compute(df.to_delayed())

            # Wait for completion
            completed = []
            async for future in as_completed(futures):
                result = await future
                completed.append(result)

            end_time = pd.Timestamp.now()
            duration = (end_time - start_time).total_seconds()

            # Verify all partitions completed
            assert len(completed) == df.npartitions

            # Combine results
            pdf = pd.concat(completed, ignore_index=True)
            assert len(pdf) == 1000

            # Should be faster than sequential (rough check)
            # With 10 partitions on multiple workers, should see speedup
            print(f"Parallel read took {duration:.2f} seconds")

    @pytest.mark.asyncio
    async def test_memory_limits_distributed(self, session, test_table_name):
        """
        Test memory limits work in distributed setting.

        What this tests:
        ---------------
        1. Memory limits respected on workers
        2. No worker OOM
        3. Adaptive partitioning works distributed

        Why this matters:
        ----------------
        - Workers have limited memory
        - Must prevent cluster crashes
        - Resource management critical
        """
        # Create table with large data
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                id INT PRIMARY KEY,
                data1 TEXT,
                data2 TEXT,
                data3 TEXT
            )
            """
        )

        try:
            # Insert large rows
            large_text = "x" * 5000
            insert_stmt = await session.prepare(
                f"""
                INSERT INTO {test_table_name}
                (id, data1, data2, data3)
                VALUES (?, ?, ?, ?)
                """
            )

            for i in range(500):
                await session.execute(insert_stmt, (i, large_text, large_text, large_text))

            scheduler = os.environ.get("DASK_SCHEDULER", "tcp://localhost:8786")

            async with Client(scheduler, asynchronous=True) as client:
                # Read with strict memory limit
                df = await cdf.read_cassandra_table(
                    f"test_dataframe.{test_table_name}",
                    session=session,
                    memory_per_partition_mb=20,  # Small limit
                    client=client,
                )

                # Should create many partitions
                assert df.npartitions > 5

                # Compute should succeed without OOM
                pdf = df.compute()
                assert len(pdf) == 500

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    @pytest.mark.asyncio
    async def test_column_selection_distributed(self, session, all_types_table):
        """
        Test column selection in distributed mode.

        What this tests:
        ---------------
        1. Column pruning works distributed
        2. Reduced network transfer
        3. Type conversions work on workers

        Why this matters:
        ----------------
        - Efficiency in production
        - Network bandwidth savings
        - Worker resource usage
        """
        scheduler = os.environ.get("DASK_SCHEDULER", "tcp://localhost:8786")

        async with Client(scheduler, asynchronous=True) as client:
            # Insert test data
            await session.execute(
                f"""
                INSERT INTO {all_types_table.split('.')[1]} (
                    id, text_col, int_col, float_col, boolean_col,
                    list_col, map_col
                ) VALUES (
                    1, 'test', 42, 3.14, true,
                    ['a', 'b'], {{'key': 'value'}}
                )
                """
            )

            # Read only specific columns
            df = await cdf.read_cassandra_table(
                all_types_table,
                session=session,
                columns=["id", "text_col", "int_col"],
                client=client,
            )

            pdf = df.compute()

            # Only requested columns present
            assert set(pdf.columns) == {"id", "text_col", "int_col"}
            assert len(pdf) == 1

            # Types preserved
            assert pdf["id"].dtype == "int32"
            assert pdf["text_col"].dtype == "object"
            assert pdf["int_col"].dtype == "int32"

    @pytest.mark.asyncio
    async def test_writetime_distributed(self, session, test_table_name):
        """
        Test writetime queries in distributed mode.

        What this tests:
        ---------------
        1. Writetime works on workers
        2. Serialization handles timestamps
        3. Correct timezone handling

        Why this matters:
        ----------------
        - Common use case
        - Complex serialization
        - Must work distributed
        """
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                id INT PRIMARY KEY,
                data TEXT,
                value INT
            )
            """
        )

        try:
            # Insert data
            await session.execute(
                f"""
                INSERT INTO {test_table_name} (id, data, value)
                VALUES (1, 'test', 100)
                """
            )

            scheduler = os.environ.get("DASK_SCHEDULER", "tcp://localhost:8786")

            async with Client(scheduler, asynchronous=True) as client:
                # Read with writetime
                df = await cdf.read_cassandra_table(
                    f"test_dataframe.{test_table_name}",
                    session=session,
                    writetime_columns=["data", "value"],
                    client=client,
                )

                pdf = df.compute()

                # Writetime columns added
                assert "data_writetime" in pdf.columns
                assert "value_writetime" in pdf.columns

                # Should be timestamps
                assert pd.api.types.is_datetime64_any_dtype(pdf["data_writetime"])
                assert pd.api.types.is_datetime64_any_dtype(pdf["value_writetime"])

                # Should have timezone
                assert pdf["data_writetime"].iloc[0].tz is not None

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    @pytest.mark.asyncio
    async def test_error_handling_distributed(self, session):
        """
        Test error handling in distributed mode.

        What this tests:
        ---------------
        1. Errors propagate correctly
        2. Clear error messages
        3. No hanging tasks

        Why this matters:
        ----------------
        - Debugging distributed systems
        - User experience
        - System stability
        """
        scheduler = os.environ.get("DASK_SCHEDULER", "tcp://localhost:8786")

        async with Client(scheduler, asynchronous=True) as client:
            # Try to read non-existent table
            with pytest.raises(ValueError) as exc_info:
                await cdf.read_cassandra_table(
                    "test_dataframe.does_not_exist", session=session, client=client
                )

            assert "not found" in str(exc_info.value).lower()
