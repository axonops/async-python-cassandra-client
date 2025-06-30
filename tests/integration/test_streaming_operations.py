"""
Integration tests for streaming functionality.

Demonstrates CRITICAL context manager usage for streaming operations
to prevent memory leaks.
"""

import asyncio
import uuid

import pytest

from async_cassandra import StreamConfig, create_streaming_statement


@pytest.mark.integration
@pytest.mark.asyncio
class TestStreamingIntegration:
    """Test streaming operations with real Cassandra using proper context managers."""

    async def test_basic_streaming(self, cassandra_session):
        """Test basic streaming functionality with context managers."""
        # Get the unique table name
        users_table = cassandra_session._test_users_table

        try:
            # Insert test data
            insert_stmt = await cassandra_session.prepare(
                f"INSERT INTO {users_table} (id, name, email, age) VALUES (?, ?, ?, ?)"
            )

            # Insert 100 test records
            tasks = []
            for i in range(100):
                task = cassandra_session.execute(
                    insert_stmt, [uuid.uuid4(), f"User {i}", f"user{i}@test.com", 20 + (i % 50)]
                )
                tasks.append(task)

            await asyncio.gather(*tasks)

            # Stream through all users WITH CONTEXT MANAGER
            stream_config = StreamConfig(fetch_size=20)

            # CRITICAL: Use context manager to prevent memory leaks
            async with await cassandra_session.execute_stream(
                f"SELECT * FROM {users_table}", stream_config=stream_config
            ) as result:
                # Count rows
                row_count = 0
                async for row in result:
                    assert hasattr(row, "id")
                    assert hasattr(row, "name")
                    assert hasattr(row, "email")
                    assert hasattr(row, "age")
                    row_count += 1

                assert row_count >= 100  # At least the records we inserted
                assert result.total_rows_fetched >= 100

        except Exception as e:
            pytest.fail(f"Streaming test failed: {e}")

    async def test_page_based_streaming(self, cassandra_session):
        """Test streaming by pages with proper context managers."""
        # Get the unique table name
        users_table = cassandra_session._test_users_table

        try:
            # Insert test data
            insert_stmt = await cassandra_session.prepare(
                f"INSERT INTO {users_table} (id, name, email, age) VALUES (?, ?, ?, ?)"
            )

            # Insert 50 test records
            for i in range(50):
                await cassandra_session.execute(
                    insert_stmt, [uuid.uuid4(), f"PageUser {i}", f"pageuser{i}@test.com", 25]
                )

            # Stream by pages WITH CONTEXT MANAGER
            stream_config = StreamConfig(fetch_size=10)

            async with await cassandra_session.execute_stream(
                f"SELECT * FROM {users_table} WHERE age = 25 ALLOW FILTERING",
                stream_config=stream_config,
            ) as result:
                page_count = 0
                total_rows = 0

                async for page in result.pages():
                    page_count += 1
                    total_rows += len(page)
                    assert len(page) <= 10  # Should not exceed fetch_size

                    # Verify all rows in page have age = 25
                    for row in page:
                        assert row.age == 25

                assert page_count >= 5  # Should have multiple pages
                assert total_rows >= 50

        except Exception as e:
            pytest.fail(f"Page-based streaming test failed: {e}")

    async def test_streaming_with_progress_callback(self, cassandra_session):
        """Test streaming with progress callback using context managers."""
        # Get the unique table name
        users_table = cassandra_session._test_users_table

        try:
            progress_calls = []

            def progress_callback(page_num, row_count):
                progress_calls.append((page_num, row_count))

            stream_config = StreamConfig(fetch_size=15, page_callback=progress_callback)

            # Use context manager for streaming
            async with await cassandra_session.execute_stream(
                f"SELECT * FROM {users_table} LIMIT 50", stream_config=stream_config
            ) as result:
                # Consume the stream
                row_count = 0
                async for row in result:
                    row_count += 1

            # Should have received progress callbacks
            assert len(progress_calls) > 0
            assert all(isinstance(call[0], int) for call in progress_calls)  # page numbers
            assert all(isinstance(call[1], int) for call in progress_calls)  # row counts

        except Exception as e:
            pytest.fail(f"Progress callback test failed: {e}")

    async def test_streaming_statement_helper(self, cassandra_session):
        """Test using the streaming statement helper with context managers."""
        # Get the unique table name
        users_table = cassandra_session._test_users_table

        try:
            statement = create_streaming_statement(
                f"SELECT * FROM {users_table} LIMIT 30", fetch_size=10
            )

            # Use context manager
            async with await cassandra_session.execute_stream(statement) as result:
                rows = []
                async for row in result:
                    rows.append(row)

                assert len(rows) <= 30  # Respects LIMIT
                assert result.page_number >= 1

        except Exception as e:
            pytest.fail(f"Streaming statement helper test failed: {e}")

    async def test_streaming_with_parameters(self, cassandra_session):
        """Test streaming with parameterized queries using context managers."""
        # Get the unique table name
        users_table = cassandra_session._test_users_table

        try:
            # Insert some specific test data
            user_id = uuid.uuid4()
            # Prepare statement first
            insert_stmt = await cassandra_session.prepare(
                f"INSERT INTO {users_table} (id, name, email, age) VALUES (?, ?, ?, ?)"
            )
            await cassandra_session.execute(
                insert_stmt, [user_id, "StreamTest", "streamtest@test.com", 99]
            )

            # Stream with parameters - prepare statement first
            stream_stmt = await cassandra_session.prepare(
                f"SELECT * FROM {users_table} WHERE age = ? ALLOW FILTERING"
            )

            # Use context manager
            async with await cassandra_session.execute_stream(
                stream_stmt,
                parameters=[99],
                stream_config=StreamConfig(fetch_size=5),
            ) as result:
                found_user = False
                async for row in result:
                    if str(row.id) == str(user_id):
                        found_user = True
                        assert row.name == "StreamTest"
                        assert row.age == 99

                assert found_user

        except Exception as e:
            pytest.fail(f"Parameterized streaming test failed: {e}")

    async def test_streaming_empty_result(self, cassandra_session):
        """Test streaming with empty result set using context managers."""
        # Get the unique table name
        users_table = cassandra_session._test_users_table

        try:
            # Use context manager even for empty results
            async with await cassandra_session.execute_stream(
                f"SELECT * FROM {users_table} WHERE age = 999 ALLOW FILTERING"
            ) as result:
                rows = []
                async for row in result:
                    rows.append(row)

                assert len(rows) == 0
                assert result.total_rows_fetched == 0

        except Exception as e:
            pytest.fail(f"Empty result streaming test failed: {e}")

    async def test_streaming_vs_regular_results(self, cassandra_session):
        """Test that streaming and regular execute return same data."""
        # Get the unique table name
        users_table = cassandra_session._test_users_table

        try:
            query = f"SELECT * FROM {users_table} LIMIT 20"

            # Get results with regular execute
            regular_result = await cassandra_session.execute(query)
            regular_rows = []
            async for row in regular_result:
                regular_rows.append(row)

            # Get results with streaming USING CONTEXT MANAGER
            async with await cassandra_session.execute_stream(query) as stream_result:
                stream_rows = []
                async for row in stream_result:
                    stream_rows.append(row)

            # Should have same number of rows
            assert len(regular_rows) == len(stream_rows)

            # Convert to sets of IDs for comparison (order might differ)
            regular_ids = {str(row.id) for row in regular_rows}
            stream_ids = {str(row.id) for row in stream_rows}

            assert regular_ids == stream_ids

        except Exception as e:
            pytest.fail(f"Streaming vs regular comparison failed: {e}")

    async def test_streaming_max_pages_limit(self, cassandra_session):
        """Test streaming with maximum pages limit using context managers."""
        # Get the unique table name
        users_table = cassandra_session._test_users_table

        try:
            stream_config = StreamConfig(fetch_size=5, max_pages=2)  # Limit to 2 pages only

            # Use context manager
            async with await cassandra_session.execute_stream(
                f"SELECT * FROM {users_table}", stream_config=stream_config
            ) as result:
                rows = []
                async for row in result:
                    rows.append(row)

                # Should stop after 2 pages max
                assert len(rows) <= 10  # 2 pages * 5 rows per page
                assert result.page_number <= 2

        except Exception as e:
            pytest.fail(f"Max pages limit test failed: {e}")

    async def test_streaming_early_exit(self, cassandra_session):
        """Test early exit from streaming with proper cleanup."""
        # Get the unique table name
        users_table = cassandra_session._test_users_table

        try:
            # Insert enough data to have multiple pages
            insert_stmt = await cassandra_session.prepare(
                f"INSERT INTO {users_table} (id, name, email, age) VALUES (?, ?, ?, ?)"
            )

            for i in range(50):
                await cassandra_session.execute(
                    insert_stmt, [uuid.uuid4(), f"EarlyExit {i}", f"early{i}@test.com", 30]
                )

            stream_config = StreamConfig(fetch_size=10)

            # Context manager ensures cleanup even with early exit
            async with await cassandra_session.execute_stream(
                f"SELECT * FROM {users_table} WHERE age = 30 ALLOW FILTERING",
                stream_config=stream_config,
            ) as result:
                count = 0
                async for row in result:
                    count += 1
                    if count >= 15:  # Exit early
                        break

                assert count == 15
                # Context manager ensures cleanup happens here

        except Exception as e:
            pytest.fail(f"Early exit test failed: {e}")

    async def test_streaming_exception_handling(self, cassandra_session):
        """Test exception handling during streaming with context managers."""
        # Get the unique table name
        users_table = cassandra_session._test_users_table

        class TestError(Exception):
            pass

        try:
            # Insert test data
            insert_stmt = await cassandra_session.prepare(
                f"INSERT INTO {users_table} (id, name, email, age) VALUES (?, ?, ?, ?)"
            )

            for i in range(20):
                await cassandra_session.execute(
                    insert_stmt, [uuid.uuid4(), f"ExceptionTest {i}", f"exc{i}@test.com", 40]
                )

            # Test that context manager cleans up even on exception
            with pytest.raises(TestError):
                async with await cassandra_session.execute_stream(
                    f"SELECT * FROM {users_table} WHERE age = 40 ALLOW FILTERING"
                ) as result:
                    count = 0
                    async for row in result:
                        count += 1
                        if count >= 10:
                            raise TestError("Simulated error during streaming")

            # Context manager should have cleaned up despite exception

        except TestError:
            # This is expected - re-raise it for pytest
            raise
        except Exception as e:
            pytest.fail(f"Exception handling test failed: {e}")
