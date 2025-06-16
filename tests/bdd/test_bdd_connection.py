"""BDD tests for connection management with real Cassandra."""

import asyncio
import time

import pytest
from pytest_bdd import given, parsers, scenario, then, when

from async_cassandra import AsyncCluster

# Import the cassandra_container fixture
pytest_plugins = ["tests._fixtures.cassandra"]


@scenario("features/connection_management.feature", "Create and close a simple connection")
def test_create_and_close_connection():
    """Test simple connection scenario."""
    pass


@scenario("features/connection_management.feature", "Handle multiple concurrent connections")
def test_concurrent_connections():
    """Test concurrent connections."""
    pass


@pytest.fixture
def bdd_context(cassandra_container):
    """Context for sharing data between BDD steps."""
    return {
        "cluster": None,
        "session": None,
        "sessions": [],
        "error": None,
        "container": cassandra_container,
        "start_time": None,
        "duration": None,
        "query_result": None,
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


# Given steps
@given("a Cassandra cluster is running")
def cassandra_running(bdd_context):
    """Verify Cassandra container is running."""
    assert bdd_context["container"].is_running()


@given("the async-cassandra driver is configured")
def driver_configured(bdd_context):
    """Driver is configured with container connection details."""
    # Contact points are always localhost when using container
    pass


@given("I have connection parameters for the local cluster")
def connection_parameters(bdd_context):
    """Connection parameters are set for local cluster."""
    # Using default localhost:9042 from container
    pass


@given(parsers.parse("I need to handle {count:d} concurrent requests"))
def set_concurrent_count(count, bdd_context):
    """Set concurrent request count."""
    bdd_context["concurrent_count"] = count


# When steps
@when("I create an async session")
def create_session(bdd_context, event_loop):
    """Create an async session."""

    async def _create():
        cluster = AsyncCluster(["127.0.0.1"])
        bdd_context["cluster"] = cluster
        session = await cluster.connect()
        await session.set_keyspace("test_keyspace")
        bdd_context["session"] = session
        return session

    run_async(_create(), event_loop)


@when("I close the session")
def close_session(bdd_context, event_loop):
    """Close the session."""

    async def _close():
        await bdd_context["session"].close()
        await bdd_context["cluster"].shutdown()

    run_async(_close(), event_loop)


@when(parsers.parse('I execute "{query}"'))
def execute_query(query, bdd_context, event_loop):
    """Execute a query."""

    async def _execute():
        result = await bdd_context["session"].execute(query)
        bdd_context["query_result"] = result
        return result

    run_async(_execute(), event_loop)


@when(parsers.parse("I create {count:d} async sessions simultaneously"))
def create_multiple_sessions(count, bdd_context, event_loop):
    """Create multiple sessions."""

    async def _create_multiple():
        async def create_one():
            cluster = AsyncCluster(["127.0.0.1"])
            session = await cluster.connect()
            await session.set_keyspace("test_keyspace")
            return {"cluster": cluster, "session": session}

        start = time.time()
        tasks = [create_one() for _ in range(count)]
        results = await asyncio.gather(*tasks)
        end = time.time()

        bdd_context["sessions"] = results
        bdd_context["duration"] = end - start

    run_async(_create_multiple(), event_loop)


# Then steps
@then("the session should be connected")
def verify_connected(bdd_context):
    """Verify session is connected."""
    assert bdd_context["session"] is not None
    assert bdd_context["cluster"] is not None
    assert not bdd_context["session"].is_closed


@then("all resources should be cleaned up")
def verify_cleaned(bdd_context):
    """Verify resources are cleaned up."""
    assert bdd_context["session"].is_closed
    assert bdd_context["cluster"].is_closed


@then("I should be able to execute a simple query")
def verify_can_execute(bdd_context, event_loop):
    """Verify we can execute queries."""
    execute_query("SELECT release_version FROM system.local", bdd_context, event_loop)
    assert bdd_context.get("query_result") is not None
    assert len(bdd_context["query_result"].rows) > 0


@then("all sessions should connect successfully")
def verify_all_connected(bdd_context):
    """Verify all sessions connected."""
    assert len(bdd_context["sessions"]) == bdd_context["concurrent_count"]
    for item in bdd_context["sessions"]:
        assert item["session"] is not None
        assert not item["session"].is_closed


@then("no resources should be leaked")
def verify_no_leaks(bdd_context, event_loop):
    """Verify no resource leaks by cleaning up sessions."""

    async def _cleanup():
        for item in bdd_context["sessions"]:
            await item["session"].close()
            await item["cluster"].shutdown()

    run_async(_cleanup(), event_loop)

    # All sessions should be closed
    for item in bdd_context["sessions"]:
        assert item["session"].is_closed


@then("the event loop should not be blocked")
def verify_event_loop_not_blocked(bdd_context):
    """Verify event loop wasn't blocked."""
    # Creating 100 connections should be fast if truly async
    # With blocking code, this would take much longer
    assert bdd_context["duration"] < 5.0  # Should take less than 5 seconds


@then(parsers.parse("all queries should complete within {timeout:d} seconds"))
def verify_timeout(timeout, bdd_context):
    """Verify completion time."""
    assert bdd_context["duration"] < timeout
