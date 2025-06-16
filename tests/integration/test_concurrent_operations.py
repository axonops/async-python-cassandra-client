"""
Comprehensive integration tests for concurrent operations against real Cassandra.
"""

import asyncio
import random
import time
import uuid
from datetime import datetime, timezone

import pytest
from cassandra import ConsistencyLevel

from async_cassandra import AsyncCassandraSession


@pytest.mark.integration
class TestConcurrentOperations:
    """Test concurrent operations with real Cassandra."""

    @pytest.mark.asyncio
    async def test_concurrent_reads(self, cassandra_session: AsyncCassandraSession):
        """Test high-concurrency read operations."""
        # Insert test data first
        insert_stmt = await cassandra_session.prepare(
            "INSERT INTO users (id, name, email, age) VALUES (?, ?, ?, ?)"
        )

        test_ids = []
        for i in range(100):
            test_id = uuid.uuid4()
            test_ids.append(test_id)
            await cassandra_session.execute(
                insert_stmt, [test_id, f"User {i}", f"user{i}@test.com", 20 + (i % 50)]
            )

        # Perform 1000 concurrent reads
        select_stmt = await cassandra_session.prepare("SELECT * FROM users WHERE id = ?")

        async def read_record(record_id):
            start = time.time()
            result = await cassandra_session.execute(select_stmt, [record_id])
            duration = time.time() - start
            rows = []
            async for row in result:
                rows.append(row)
            return rows[0] if rows else None, duration

        # Create 1000 read tasks (reading the same 100 records multiple times)
        tasks = []
        for i in range(1000):
            record_id = test_ids[i % len(test_ids)]
            tasks.append(read_record(record_id))

        start_time = time.time()
        results = await asyncio.gather(*tasks)
        total_time = time.time() - start_time

        # Verify results
        successful_reads = [r for r, _ in results if r is not None]
        assert len(successful_reads) == 1000

        # Check performance
        durations = [d for _, d in results]
        avg_duration = sum(durations) / len(durations)

        print("\nConcurrent read test results:")
        print(f"  Total time: {total_time:.2f}s")
        print(f"  Average read latency: {avg_duration*1000:.2f}ms")
        print(f"  Reads per second: {1000/total_time:.0f}")

        # Performance assertions (relaxed for CI environments)
        assert total_time < 15.0  # Should complete within 15 seconds
        assert avg_duration < 0.5  # Average latency under 500ms (relaxed for CI)

    @pytest.mark.asyncio
    async def test_concurrent_writes(self, cassandra_session: AsyncCassandraSession):
        """Test high-concurrency write operations."""
        insert_stmt = await cassandra_session.prepare(
            "INSERT INTO users (id, name, email, age) VALUES (?, ?, ?, ?)"
        )

        async def write_record(i):
            start = time.time()
            try:
                await cassandra_session.execute(
                    insert_stmt,
                    [uuid.uuid4(), f"Concurrent User {i}", f"concurrent{i}@test.com", 25],
                )
                return True, time.time() - start
            except Exception:
                return False, time.time() - start

        # Create 500 concurrent write tasks
        tasks = [write_record(i) for i in range(500)]

        start_time = time.time()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        total_time = time.time() - start_time

        # Count successes
        successful_writes = sum(1 for r in results if isinstance(r, tuple) and r[0])
        failed_writes = 500 - successful_writes

        print("\nConcurrent write test results:")
        print(f"  Total time: {total_time:.2f}s")
        print(f"  Successful writes: {successful_writes}")
        print(f"  Failed writes: {failed_writes}")
        print(f"  Writes per second: {successful_writes/total_time:.0f}")

        # Should have very high success rate
        assert successful_writes >= 495  # Allow up to 1% failure
        assert total_time < 10.0  # Should complete within 10 seconds

    @pytest.mark.asyncio
    async def test_mixed_concurrent_operations(self, cassandra_session: AsyncCassandraSession):
        """Test mixed read/write operations under high concurrency."""
        # Prepare statements
        insert_stmt = await cassandra_session.prepare(
            "INSERT INTO users (id, name, email, age) VALUES (?, ?, ?, ?)"
        )
        select_stmt = await cassandra_session.prepare("SELECT * FROM users WHERE id = ?")
        update_stmt = await cassandra_session.prepare("UPDATE users SET age = ? WHERE id = ?")

        # Pre-populate some data
        existing_ids = []
        for i in range(50):
            user_id = uuid.uuid4()
            existing_ids.append(user_id)
            await cassandra_session.execute(
                insert_stmt, [user_id, f"Existing User {i}", f"existing{i}@test.com", 30]
            )

        # Define operation types
        async def insert_operation(i):
            return await cassandra_session.execute(
                insert_stmt,
                [uuid.uuid4(), f"New User {i}", f"new{i}@test.com", 25],
            )

        async def select_operation(user_id):
            result = await cassandra_session.execute(select_stmt, [user_id])
            rows = []
            async for row in result:
                rows.append(row)
            return rows

        async def update_operation(user_id):
            new_age = random.randint(20, 60)
            return await cassandra_session.execute(update_stmt, [new_age, user_id])

        # Create mixed operations
        operations = []

        # 200 inserts
        for i in range(200):
            operations.append(insert_operation(i))

        # 300 selects
        for _ in range(300):
            user_id = random.choice(existing_ids)
            operations.append(select_operation(user_id))

        # 100 updates
        for _ in range(100):
            user_id = random.choice(existing_ids)
            operations.append(update_operation(user_id))

        # Shuffle to mix operation types
        random.shuffle(operations)

        # Execute all operations concurrently
        start_time = time.time()
        results = await asyncio.gather(*operations, return_exceptions=True)
        total_time = time.time() - start_time

        # Count results
        successful = sum(1 for r in results if not isinstance(r, Exception))
        failed = sum(1 for r in results if isinstance(r, Exception))

        print("\nMixed operations test results:")
        print(f"  Total operations: {len(operations)}")
        print(f"  Successful: {successful}")
        print(f"  Failed: {failed}")
        print(f"  Total time: {total_time:.2f}s")
        print(f"  Operations per second: {successful/total_time:.0f}")

        # Should have very high success rate
        assert successful >= 590  # Allow up to ~2% failure
        assert total_time < 15.0  # Should complete within 15 seconds

    @pytest.mark.asyncio
    async def test_consistency_levels_concurrent(self, cassandra_session: AsyncCassandraSession):
        """Test concurrent operations with different consistency levels."""
        # Insert with QUORUM, read with ONE and ALL
        insert_stmt = await cassandra_session.prepare(
            "INSERT INTO users (id, name, email, age) VALUES (?, ?, ?, ?)"
        )
        insert_stmt.consistency_level = ConsistencyLevel.ONE

        select_one = await cassandra_session.prepare("SELECT * FROM users WHERE id = ?")
        select_one.consistency_level = ConsistencyLevel.ONE

        select_all = await cassandra_session.prepare("SELECT * FROM users WHERE id = ?")
        select_all.consistency_level = ConsistencyLevel.ALL

        # Insert and immediately read with different consistency levels
        async def insert_and_read(i):
            user_id = uuid.uuid4()

            # Insert
            await cassandra_session.execute(
                insert_stmt, [user_id, f"CL User {i}", f"cl{i}@test.com", 35]
            )

            # Read with ONE
            result_one = await cassandra_session.execute(select_one, [user_id])
            rows_one = []
            async for row in result_one:
                rows_one.append(row)

            # Read with ALL
            result_all = await cassandra_session.execute(select_all, [user_id])
            rows_all = []
            async for row in result_all:
                rows_all.append(row)

            return len(rows_one) > 0, len(rows_all) > 0

        # Run 100 concurrent insert+read operations
        tasks = [insert_and_read(i) for i in range(100)]
        results = await asyncio.gather(*tasks)

        # Check results
        one_success = sum(1 for r1, _ in results if r1)
        all_success = sum(1 for _, r2 in results if r2)

        print("\nConsistency level test results:")
        print(f"  CL.ONE successful reads: {one_success}/100")
        print(f"  CL.ALL successful reads: {all_success}/100")

        # With single node, both should succeed equally
        assert one_success == 100
        assert all_success == 100

    @pytest.mark.asyncio
    async def test_prepared_statement_concurrency(self, cassandra_session: AsyncCassandraSession):
        """Test that prepared statements work correctly under high concurrency."""
        # Create a table for testing prepared statement performance
        await cassandra_session.execute("DROP TABLE IF EXISTS prepared_test")
        await cassandra_session.execute(
            """
            CREATE TABLE prepared_test (
                partition_key INT,
                cluster_key INT,
                data TEXT,
                timestamp TIMESTAMP,
                PRIMARY KEY (partition_key, cluster_key)
            )
            """
        )

        # Prepare statements once
        insert_stmt = await cassandra_session.prepare(
            "INSERT INTO prepared_test (partition_key, cluster_key, data, timestamp) VALUES (?, ?, ?, ?)"
        )
        select_stmt = await cassandra_session.prepare(
            "SELECT * FROM prepared_test WHERE partition_key = ? AND cluster_key = ?"
        )

        # Track timing for prepared vs non-prepared
        prepared_times = []

        # Function to use prepared statement
        async def use_prepared_statement(partition, cluster):
            start = time.time()

            # Insert with prepared statement
            await cassandra_session.execute(
                insert_stmt,
                [partition, cluster, f"data_{partition}_{cluster}", datetime.now(timezone.utc)],
            )

            # Select with prepared statement
            result = await cassandra_session.execute(select_stmt, [partition, cluster])
            async for _ in result:
                pass

            prepared_times.append(time.time() - start)

        # Run many concurrent operations with prepared statements
        print("\nTesting prepared statement concurrency...")
        start_time = time.time()

        tasks = []
        for i in range(100):
            for j in range(10):
                tasks.append(use_prepared_statement(i, j))

        # Execute 1000 operations concurrently
        await asyncio.gather(*tasks)

        total_time = time.time() - start_time
        avg_time = sum(prepared_times) / len(prepared_times)

        print(f"  Total operations: {len(tasks)}")
        print(f"  Total time: {total_time:.2f}s")
        print(f"  Average operation time: {avg_time*1000:.2f}ms")
        print(f"  Operations per second: {len(tasks)/total_time:.0f}")

        # Verify all data was written correctly
        result = await cassandra_session.execute("SELECT COUNT(*) FROM prepared_test")
        count = result.one()[0]
        assert count == 1000, f"Expected 1000 rows, got {count}"

        # Prepared statements should handle high concurrency efficiently
        assert total_time < 30.0  # Should complete 1000 ops in under 30 seconds
        assert avg_time < 0.1  # Each operation should average under 100ms
