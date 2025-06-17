"""
Integration tests for streaming functionality.
"""

import asyncio
import uuid

import pytest

from async_cassandra import StreamConfig, create_streaming_statement


@pytest.mark.integration
class TestStreamingIntegration:
    """Test streaming operations with real Cassandra."""

    @pytest.mark.asyncio
    async def test_basic_streaming(self, cassandra_session):
        """Test basic streaming functionality."""
        # Get the unique table name
        users_table = cassandra_session._test_users_table

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

        # Stream through all users
        stream_config = StreamConfig(fetch_size=20)
        result = await cassandra_session.execute_stream(
            f"SELECT * FROM {users_table}", stream_config=stream_config
        )

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

    @pytest.mark.asyncio
    async def test_page_based_streaming(self, cassandra_session):
        """Test streaming by pages."""
        # Get the unique table name
        users_table = cassandra_session._test_users_table

        # Insert test data
        insert_stmt = await cassandra_session.prepare(
            f"INSERT INTO {users_table} (id, name, email, age) VALUES (?, ?, ?, ?)"
        )

        # Insert 50 test records
        for i in range(50):
            await cassandra_session.execute(
                insert_stmt, [uuid.uuid4(), f"PageUser {i}", f"pageuser{i}@test.com", 25]
            )

        # Stream by pages
        stream_config = StreamConfig(fetch_size=10)
        result = await cassandra_session.execute_stream(
            f"SELECT * FROM {users_table} WHERE age = 25 ALLOW FILTERING",
            stream_config=stream_config,
        )

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

    @pytest.mark.asyncio
    async def test_streaming_with_progress_callback(self, cassandra_session):
        """Test streaming with progress callback."""
        # Get the unique table name
        users_table = cassandra_session._test_users_table

        progress_calls = []

        def progress_callback(page_num, row_count):
            progress_calls.append((page_num, row_count))

        stream_config = StreamConfig(fetch_size=15, page_callback=progress_callback)

        result = await cassandra_session.execute_stream(
            f"SELECT * FROM {users_table} LIMIT 50", stream_config=stream_config
        )

        # Consume the stream
        row_count = 0
        async for row in result:
            row_count += 1

        # Should have received progress callbacks
        assert len(progress_calls) > 0
        assert all(isinstance(call[0], int) for call in progress_calls)  # page numbers
        assert all(isinstance(call[1], int) for call in progress_calls)  # row counts

    @pytest.mark.asyncio
    async def test_streaming_statement_helper(self, cassandra_session):
        """Test using the streaming statement helper."""
        # Get the unique table name
        users_table = cassandra_session._test_users_table

        statement = create_streaming_statement(
            f"SELECT * FROM {users_table} LIMIT 30", fetch_size=10
        )

        result = await cassandra_session.execute_stream(statement)

        rows = []
        async for row in result:
            rows.append(row)

        assert len(rows) <= 30  # Respects LIMIT
        assert result.page_number >= 1

    @pytest.mark.asyncio
    async def test_streaming_with_parameters(self, cassandra_session):
        """Test streaming with parameterized queries."""
        # Get the unique table name
        users_table = cassandra_session._test_users_table

        # Insert some specific test data
        user_id = uuid.uuid4()
        # Prepare statement first
        insert_stmt = await cassandra_session.prepare(
            f"INSERT INTO {users_table} (id, name, email, age) VALUES (?, ?, ?, ?)"
        )
        await cassandra_session.execute(
            insert_stmt, [user_id, "StreamTest", "streamtest@test.com", 99]
        )

        # Stream with parameters - use non-prepared statement with ALLOW FILTERING
        result = await cassandra_session.execute_stream(
            f"SELECT * FROM {users_table} WHERE age = %s ALLOW FILTERING",
            parameters=[99],
            stream_config=StreamConfig(fetch_size=5),
        )

        found_user = False
        async for row in result:
            if str(row.id) == str(user_id):
                found_user = True
                assert row.name == "StreamTest"
                assert row.age == 99

        assert found_user

    @pytest.mark.asyncio
    async def test_streaming_empty_result(self, cassandra_session):
        """Test streaming with empty result set."""
        # Get the unique table name
        users_table = cassandra_session._test_users_table

        result = await cassandra_session.execute_stream(
            f"SELECT * FROM {users_table} WHERE age = 999 ALLOW FILTERING"  # Should return no results
        )

        rows = []
        async for row in result:
            rows.append(row)

        assert len(rows) == 0
        assert result.total_rows_fetched == 0

    @pytest.mark.asyncio
    async def test_streaming_vs_regular_results(self, cassandra_session):
        """Test that streaming and regular execute return same data."""
        # Get the unique table name
        users_table = cassandra_session._test_users_table

        query = f"SELECT * FROM {users_table} LIMIT 20"

        # Get results with regular execute
        regular_result = await cassandra_session.execute(query)
        regular_rows = []
        async for row in regular_result:
            regular_rows.append(row)

        # Get results with streaming
        stream_result = await cassandra_session.execute_stream(query)
        stream_rows = []
        async for row in stream_result:
            stream_rows.append(row)

        # Should have same number of rows
        assert len(regular_rows) == len(stream_rows)

        # Convert to sets of IDs for comparison (order might differ)
        regular_ids = {str(row.id) for row in regular_rows}
        stream_ids = {str(row.id) for row in stream_rows}

        assert regular_ids == stream_ids

    @pytest.mark.asyncio
    async def test_streaming_max_pages_limit(self, cassandra_session):
        """Test streaming with maximum pages limit."""
        # Get the unique table name
        users_table = cassandra_session._test_users_table

        stream_config = StreamConfig(fetch_size=5, max_pages=2)  # Limit to 2 pages only

        result = await cassandra_session.execute_stream(
            f"SELECT * FROM {users_table}", stream_config=stream_config
        )

        rows = []
        async for row in result:
            rows.append(row)

        # Should stop after 2 pages max
        assert len(rows) <= 10  # 2 pages * 5 rows per page
        assert result.page_number <= 2
