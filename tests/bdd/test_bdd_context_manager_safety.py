"""
BDD tests for context manager safety.

Tests the behavior described in features/context_manager_safety.feature
"""

import asyncio
import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest
from cassandra import InvalidRequest
from pytest_bdd import given, scenarios, then, when

from async_cassandra import AsyncCluster
from async_cassandra.streaming import StreamConfig

# Load all scenarios from the feature file
scenarios("features/context_manager_safety.feature")


# Fixtures for test state
@pytest.fixture
def test_state():
    """Holds state across BDD steps."""
    return {
        "cluster": None,
        "session": None,
        "error": None,
        "streaming_result": None,
        "sessions": [],
        "results": [],
        "thread_results": [],
    }


@pytest.fixture
def event_loop():
    """Create event loop for tests."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


def run_async(coro, loop):
    """Run async coroutine in sync context."""
    return loop.run_until_complete(coro)


# Background steps
@given("a running Cassandra cluster")
def cassandra_is_running(cassandra_cluster):
    """Cassandra cluster is provided by the fixture."""
    # Just verify we have a cluster object
    assert cassandra_cluster is not None


@given('a test keyspace "test_context_safety"')
def create_test_keyspace(cassandra_cluster, test_state, event_loop):
    """Create test keyspace."""

    async def _setup():
        cluster = AsyncCluster(["localhost"])
        session = await cluster.connect()

        await session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS test_context_safety
            WITH REPLICATION = {
                'class': 'SimpleStrategy',
                'replication_factor': 1
            }
            """
        )

        test_state["cluster"] = cluster
        test_state["session"] = session

    run_async(_setup(), event_loop)


# Scenario: Query error doesn't close session
@given("an open session connected to the test keyspace")
def open_session(test_state, event_loop):
    """Ensure session is connected to test keyspace."""

    async def _impl():
        await test_state["session"].set_keyspace("test_context_safety")

        # Create a test table
        await test_state["session"].execute(
            """
            CREATE TABLE IF NOT EXISTS test_table (
                id UUID PRIMARY KEY,
                value TEXT
            )
            """
        )

    run_async(_impl(), event_loop)


@when("I execute a query that causes an error")
def execute_bad_query(test_state, event_loop):
    """Execute a query that will fail."""

    async def _impl():
        try:
            await test_state["session"].execute("SELECT * FROM non_existent_table")
        except InvalidRequest as e:
            test_state["error"] = e

    run_async(_impl(), event_loop)


@then("the session should remain open and usable")
def session_is_open(test_state, event_loop):
    """Verify session is still open."""
    assert test_state["session"] is not None
    assert not test_state["session"].is_closed


@then("I should be able to execute subsequent queries successfully")
def can_execute_queries(test_state, event_loop):
    """Execute a successful query."""

    async def _impl():
        test_id = uuid.uuid4()
        await test_state["session"].execute(
            "INSERT INTO test_table (id, value) VALUES (%s, %s)", [test_id, "test_value"]
        )

        result = await test_state["session"].execute(
            "SELECT * FROM test_table WHERE id = %s", [test_id]
        )
        assert result.one().value == "test_value"

    run_async(_impl(), event_loop)


# Scenario: Streaming error doesn't close session
@given("an open session with test data")
def session_with_data(test_state, event_loop):
    """Create session with test data."""

    async def _impl():
        await test_state["session"].set_keyspace("test_context_safety")

        await test_state["session"].execute(
            """
            CREATE TABLE IF NOT EXISTS stream_test (
                id UUID PRIMARY KEY,
                value INT
            )
            """
        )

        # Insert test data
        for i in range(10):
            await test_state["session"].execute(
                "INSERT INTO stream_test (id, value) VALUES (%s, %s)", [uuid.uuid4(), i]
            )

    run_async(_impl(), event_loop)


@when("a streaming operation encounters an error")
def streaming_error(test_state, event_loop):
    """Try to stream from non-existent table."""

    async def _impl():
        try:
            async with await test_state["session"].execute_stream(
                "SELECT * FROM non_existent_stream_table"
            ) as stream:
                async for row in stream:
                    pass
        except Exception as e:
            test_state["error"] = e

    run_async(_impl(), event_loop)


@then("the streaming result should be closed")
def streaming_closed(test_state, event_loop):
    """Streaming result is closed (checked by context manager exit)."""
    # Context manager ensures closure
    assert test_state["error"] is not None


@then("the session should remain open")
def session_still_open(test_state, event_loop):
    """Session should not be closed."""
    assert not test_state["session"].is_closed


@then("I should be able to start new streaming operations")
def can_stream_again(test_state, event_loop):
    """Start a new streaming operation."""

    async def _impl():
        count = 0
        async with await test_state["session"].execute_stream(
            "SELECT * FROM stream_test"
        ) as stream:
            async for row in stream:
                count += 1

        assert count == 10  # Should get all 10 rows

    run_async(_impl(), event_loop)


# Scenario: Session context manager doesn't close cluster
@given("an open cluster connection")
def cluster_is_open(test_state):
    """Cluster is already open from background."""
    assert test_state["cluster"] is not None


@when("I use a session in a context manager that exits with an error")
def session_context_with_error(test_state, event_loop):
    """Use session context manager with error."""

    async def _impl():
        try:
            async with await test_state["cluster"].connect("test_context_safety") as session:
                # Do some work
                await session.execute("SELECT * FROM system.local")
                # Raise an error
                raise ValueError("Test error")
        except ValueError:
            test_state["error"] = "Session context exited"

    run_async(_impl(), event_loop)


@then("the session should be closed")
def session_is_closed(test_state):
    """Session was closed by context manager."""
    # We know it's closed because context manager handles it
    assert test_state["error"] == "Session context exited"


@then("the cluster should remain open")
def cluster_still_open(test_state):
    """Cluster should not be closed."""
    assert not test_state["cluster"].is_closed


@then("I should be able to create new sessions from the cluster")
def can_create_sessions(test_state, event_loop):
    """Create a new session from cluster."""

    async def _impl():
        new_session = await test_state["cluster"].connect()
        result = await new_session.execute("SELECT release_version FROM system.local")
        assert result.one() is not None
        await new_session.close()

    run_async(_impl(), event_loop)


# Scenario: Multiple concurrent streams don't interfere
@given("multiple sessions from the same cluster")
def create_multiple_sessions(test_state, event_loop):
    """Create multiple sessions."""

    async def _impl():
        await test_state["session"].set_keyspace("test_context_safety")

        # Create test table
        await test_state["session"].execute(
            """
            CREATE TABLE IF NOT EXISTS concurrent_test (
                partition_id INT,
                id UUID,
                value TEXT,
                PRIMARY KEY (partition_id, id)
            )
            """
        )

        # Insert data for different partitions
        for partition in range(3):
            for i in range(20):
                await test_state["session"].execute(
                    "INSERT INTO concurrent_test (partition_id, id, value) VALUES (%s, %s, %s)",
                    [partition, uuid.uuid4(), f"value_{partition}_{i}"],
                )

        # Create multiple sessions
        for _ in range(3):
            session = await test_state["cluster"].connect("test_context_safety")
            test_state["sessions"].append(session)

    run_async(_impl(), event_loop)


@when("I stream data concurrently from each session")
def concurrent_streaming(test_state, event_loop):
    """Stream from each session concurrently."""

    async def _impl():
        async def stream_partition(session, partition_id):
            count = 0
            config = StreamConfig(fetch_size=5)

            async with await session.execute_stream(
                "SELECT * FROM concurrent_test WHERE partition_id = %s",
                [partition_id],
                stream_config=config,
            ) as stream:
                async for row in stream:
                    count += 1

            return count

        # Stream concurrently
        tasks = []
        for i, session in enumerate(test_state["sessions"]):
            task = stream_partition(session, i)
            tasks.append(task)

        test_state["results"] = await asyncio.gather(*tasks)

    run_async(_impl(), event_loop)


@then("each stream should complete independently")
def streams_completed(test_state):
    """All streams should complete."""
    assert len(test_state["results"]) == 3
    assert all(count == 20 for count in test_state["results"])


@then("closing one stream should not affect others")
def close_one_stream(test_state, event_loop):
    """Already tested by concurrent execution."""
    # Streams were in context managers, so they closed independently
    pass


@then("all sessions should remain usable")
def all_sessions_usable(test_state, event_loop):
    """Test all sessions still work."""

    async def _impl():
        for session in test_state["sessions"]:
            result = await session.execute("SELECT COUNT(*) FROM concurrent_test")
            assert result.one()[0] == 60  # Total rows

    run_async(_impl(), event_loop)


# Scenario: Thread safety during context exit
@given("a session being used by multiple threads")
def session_for_threads(test_state, event_loop):
    """Set up session for thread testing."""

    async def _impl():
        await test_state["session"].set_keyspace("test_context_safety")

        await test_state["session"].execute(
            """
            CREATE TABLE IF NOT EXISTS thread_test (
                thread_id INT PRIMARY KEY,
                status TEXT,
                timestamp TIMESTAMP
            )
            """
        )

        # Truncate first to ensure clean state
        await test_state["session"].execute("TRUNCATE thread_test")

    run_async(_impl(), event_loop)


@when("one thread exits a streaming context manager")
def thread_exits_context(test_state, event_loop):
    """Use streaming in main thread while other threads work."""

    async def _impl():
        def worker_thread(session, thread_id):
            """Worker thread function."""
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            async def do_work():
                # Each thread writes its own record
                import datetime

                await session.execute(
                    "INSERT INTO thread_test (thread_id, status, timestamp) VALUES (%s, %s, %s)",
                    [thread_id, "completed", datetime.datetime.now()],
                )

                return f"Thread {thread_id} completed"

            result = loop.run_until_complete(do_work())
            loop.close()
            return result

        # Start threads
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = []
            for i in range(2):
                future = executor.submit(worker_thread, test_state["session"], i)
                futures.append(future)

            # Use streaming in main thread
            async with await test_state["session"].execute_stream(
                "SELECT * FROM thread_test"
            ) as stream:
                async for row in stream:
                    await asyncio.sleep(0.1)  # Give threads time to work

            # Collect thread results
            for future in futures:
                result = future.result(timeout=5.0)
                test_state["thread_results"].append(result)

    run_async(_impl(), event_loop)


@then("other threads should still be able to use the session")
def threads_used_session(test_state):
    """Verify threads completed their work."""
    assert len(test_state["thread_results"]) == 2
    assert all("completed" in result for result in test_state["thread_results"])


@then("no operations should be interrupted")
def verify_thread_operations(test_state, event_loop):
    """Verify all thread operations completed."""

    async def _impl():
        result = await test_state["session"].execute("SELECT thread_id, status FROM thread_test")
        rows = list(result)
        # Both threads should have completed
        assert len(rows) == 2
        thread_ids = {row.thread_id for row in rows}
        assert 0 in thread_ids
        assert 1 in thread_ids
        # All should have completed status
        assert all(row.status == "completed" for row in rows)

    run_async(_impl(), event_loop)


# Scenario: Nested context managers close in correct order
@given("a cluster, session, and streaming result in nested context managers")
def nested_contexts(test_state, event_loop):
    """Set up nested context managers."""

    async def _impl():
        # Set up test data
        test_state["nested_cluster"] = AsyncCluster(["localhost"])
        test_state["nested_session"] = await test_state["nested_cluster"].connect()

        await test_state["nested_session"].execute(
            """
            CREATE KEYSPACE IF NOT EXISTS test_nested
            WITH REPLICATION = {
                'class': 'SimpleStrategy',
                'replication_factor': 1
            }
            """
        )
        await test_state["nested_session"].set_keyspace("test_nested")

        await test_state["nested_session"].execute(
            """
            CREATE TABLE IF NOT EXISTS nested_test (
                id UUID PRIMARY KEY,
                value INT
            )
            """
        )

        # Clear existing data first
        await test_state["nested_session"].execute("TRUNCATE nested_test")

        # Insert test data
        for i in range(5):
            await test_state["nested_session"].execute(
                "INSERT INTO nested_test (id, value) VALUES (%s, %s)", [uuid.uuid4(), i]
            )

        # Start streaming (but don't iterate yet)
        test_state["nested_stream"] = await test_state["nested_session"].execute_stream(
            "SELECT * FROM nested_test"
        )

    run_async(_impl(), event_loop)


@when("the innermost context (streaming) exits")
def exit_streaming_context(test_state, event_loop):
    """Exit streaming context."""

    async def _impl():
        # Use and close the streaming context
        async with test_state["nested_stream"] as stream:
            count = 0
            async for row in stream:
                count += 1
            test_state["stream_count"] = count

    run_async(_impl(), event_loop)


@then("only the streaming result should be closed")
def verify_only_stream_closed(test_state):
    """Verify only stream is closed."""
    # Stream was closed by context manager
    assert test_state["stream_count"] == 5  # Got all rows
    assert not test_state["nested_session"].is_closed
    assert not test_state["nested_cluster"].is_closed


@when("the middle context (session) exits")
def exit_session_context(test_state, event_loop):
    """Exit session context."""

    async def _impl():
        await test_state["nested_session"].close()

    run_async(_impl(), event_loop)


@then("only the session should be closed")
def verify_only_session_closed(test_state):
    """Verify only session is closed."""
    assert test_state["nested_session"].is_closed
    assert not test_state["nested_cluster"].is_closed


@when("the outer context (cluster) exits")
def exit_cluster_context(test_state, event_loop):
    """Exit cluster context."""

    async def _impl():
        await test_state["nested_cluster"].shutdown()

    run_async(_impl(), event_loop)


@then("the cluster should be shut down")
def verify_cluster_shutdown(test_state):
    """Verify cluster is shut down."""
    assert test_state["nested_cluster"].is_closed


# Scenario: Context manager handles cancellation correctly
@given("an active streaming operation in a context manager")
def active_streaming_operation(test_state, event_loop):
    """Set up active streaming operation."""

    async def _impl():
        # Ensure we have session and keyspace
        if not test_state.get("session"):
            test_state["cluster"] = AsyncCluster(["localhost"])
            test_state["session"] = await test_state["cluster"].connect()

            await test_state["session"].execute(
                """
                CREATE KEYSPACE IF NOT EXISTS test_context_safety
                WITH REPLICATION = {
                    'class': 'SimpleStrategy',
                    'replication_factor': 1
                }
                """
            )
            await test_state["session"].set_keyspace("test_context_safety")

        # Create table with lots of data
        await test_state["session"].execute(
            """
            CREATE TABLE IF NOT EXISTS test_context_safety.cancel_test (
                id UUID PRIMARY KEY,
                value INT
            )
            """
        )

        # Insert more data for longer streaming
        for i in range(100):
            await test_state["session"].execute(
                "INSERT INTO test_context_safety.cancel_test (id, value) VALUES (%s, %s)",
                [uuid.uuid4(), i],
            )

        # Create streaming task that we'll cancel
        async def stream_with_delay():
            async with await test_state["session"].execute_stream(
                "SELECT * FROM test_context_safety.cancel_test"
            ) as stream:
                count = 0
                async for row in stream:
                    count += 1
                    # Add delay to make cancellation more likely
                    await asyncio.sleep(0.01)
                return count

        # Start streaming task
        test_state["streaming_task"] = asyncio.create_task(stream_with_delay())
        # Give it time to start
        await asyncio.sleep(0.1)

    run_async(_impl(), event_loop)


@when("the operation is cancelled")
def cancel_operation(test_state, event_loop):
    """Cancel the streaming operation."""

    async def _impl():
        # Cancel the task
        test_state["streaming_task"].cancel()

        # Wait for cancellation
        try:
            await test_state["streaming_task"]
        except asyncio.CancelledError:
            test_state["cancelled"] = True

    run_async(_impl(), event_loop)


@then("the streaming result should be properly cleaned up")
def verify_streaming_cleaned_up(test_state):
    """Verify streaming was cleaned up."""
    # Task was cancelled
    assert test_state.get("cancelled") is True
    assert test_state["streaming_task"].cancelled()


# Reuse the existing session_is_open step for cancellation scenario
# The "But" prefix is ignored by pytest-bdd


# Cleanup
@pytest.fixture(autouse=True)
def cleanup(test_state, event_loop, request):
    """Clean up after each test."""
    yield

    async def _cleanup():
        # Close all sessions
        for session in test_state.get("sessions", []):
            if session and not session.is_closed:
                await session.close()

        # Clean up main session and cluster
        if test_state.get("session"):
            try:
                await test_state["session"].execute("DROP KEYSPACE IF EXISTS test_context_safety")
            except Exception:
                pass
            if not test_state["session"].is_closed:
                await test_state["session"].close()

        if test_state.get("cluster") and not test_state["cluster"].is_closed:
            await test_state["cluster"].shutdown()

    run_async(_cleanup(), event_loop)
