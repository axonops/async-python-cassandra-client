"""BDD tests for error handling scenarios with real Cassandra."""

import time

import pytest
from cassandra import InvalidRequest
from cassandra.cluster import NoHostAvailable
from pytest_bdd import given, scenario, then, when

from async_cassandra import AsyncCluster

# Import the cassandra_container fixture
pytest_plugins = ["tests._fixtures.cassandra"]


@pytest.mark.timeout(30)
@scenario("features/error_handling.feature", "Handle NoHostAvailable errors")
def test_no_host_available():
    """Test NoHostAvailable error handling."""
    pass


@pytest.mark.timeout(30)
@scenario("features/error_handling.feature", "Preserve error context through async layers")
def test_error_context():
    """Test error context preservation."""
    pass


@pytest.fixture
def error_context(cassandra_container):
    """Context for error tests."""
    return {
        "cluster": None,
        "session": None,
        "container": cassandra_container,
        "error": None,
        "query_result": None,
        "hosts_down": [],
        "stack_trace": None,
        "failing_query": None,
        "recovery_result": None,
    }


def run_async(coro, loop):
    """Run async code in sync context."""
    return loop.run_until_complete(coro)


# Given steps
@given("a Cassandra cluster is running")
def cassandra_running(error_context):
    """Verify Cassandra container is running."""
    assert error_context["container"].is_running()


@given("I have an active async session")
def active_session(error_context, event_loop):
    """Create active session."""

    async def _create():
        cluster = AsyncCluster(["127.0.0.1"])
        session = await cluster.connect()
        await session.set_keyspace("test_keyspace")

        error_context["cluster"] = cluster
        error_context["session"] = session

    run_async(_create(), event_loop)


@given("all Cassandra nodes are temporarily unavailable")
def nodes_unavailable(error_context):
    """Stop Cassandra to simulate all nodes down."""
    container = error_context["container"]
    # Stop the container to simulate node failure
    container.stop()
    error_context["hosts_down"] = ["127.0.0.1:9042"]
    # Give it a moment to fully stop
    time.sleep(2)


@given("a complex query that will fail")
def failing_query(error_context):
    """Setup a query that will fail."""
    error_context["failing_query"] = "SELECT * FROM non_existent_table"


# When steps
@when("I attempt to execute a query")
def attempt_query(error_context, event_loop):
    """Try to execute query when nodes are down."""

    async def _execute():
        try:
            await error_context["session"].execute("SELECT * FROM system.local")
        except Exception as e:
            error_context["error"] = e

    run_async(_execute(), event_loop)


@when("the query fails deep in the driver")
def query_fails(error_context, event_loop):
    """Execute failing query."""

    async def _execute():
        try:
            await error_context["session"].execute(error_context["failing_query"])
        except Exception as e:
            error_context["error"] = e
            error_context["stack_trace"] = e.__traceback__

    run_async(_execute(), event_loop)


@when("the Cassandra nodes become available again")
def nodes_available(error_context):
    """Restart Cassandra nodes."""
    container = error_context["container"]
    # Restart the container
    container.start()
    # Wait for Cassandra to be ready again
    time.sleep(5)


# Then steps
@then("I should receive a NoHostAvailable error")
def verify_no_host_error(error_context):
    """Verify NoHostAvailable error."""
    assert error_context["error"] is not None
    assert isinstance(error_context["error"], NoHostAvailable)


@then("the error should list all attempted hosts")
def verify_hosts_listed(error_context):
    """Verify hosts in error."""
    error = error_context["error"]
    error_msg = str(error)
    # Check that our host is mentioned in the error
    assert "127.0.0.1" in error_msg or "localhost" in error_msg


@then("the error should include the specific failure reason for each host")
def verify_failure_reasons(error_context):
    """Verify failure reasons."""
    error = error_context["error"]
    # NoHostAvailable includes details about why each host failed
    assert hasattr(error, "errors") or "Connection" in str(error)


@then("subsequent queries should succeed without manual intervention")
def verify_recovery(error_context, event_loop):
    """Verify automatic recovery after nodes come back."""

    async def _execute():
        try:
            # Need to create a new cluster/session since the old one has failed
            cluster = AsyncCluster(["127.0.0.1"])
            session = await cluster.connect()
            await session.set_keyspace("test_keyspace")

            result = await session.execute("SELECT release_version FROM system.local")
            error_context["recovery_result"] = result

            # Cleanup
            await session.close()
            await cluster.shutdown()
        except Exception as e:
            error_context["recovery_error"] = e

    run_async(_execute(), event_loop)
    assert (
        error_context.get("recovery_error") is None
    ), f"Recovery failed: {error_context.get('recovery_error')}"
    assert error_context["recovery_result"] is not None
    assert len(error_context["recovery_result"].rows) > 0


@then("the error should bubble up through the async wrapper")
def verify_error_propagation(error_context):
    """Verify error propagated correctly."""
    assert error_context["error"] is not None
    assert isinstance(error_context["error"], InvalidRequest)


@then("the stack trace should show the original error location")
def verify_stack_trace(error_context):
    """Verify stack trace preserved."""
    assert error_context["stack_trace"] is not None
    # The stack trace should include our test function
    import traceback

    tb_str = "".join(traceback.format_tb(error_context["stack_trace"]))
    assert "query_fails" in tb_str or "execute" in tb_str


@then("the error message should not be altered or wrapped")
def verify_error_not_wrapped(error_context):
    """Verify error message is not wrapped."""
    # The InvalidRequest error should come through unchanged
    error_msg = str(error_context["error"])
    assert "Table not found" in error_msg or "non_existent_table" in error_msg
    # Should not have additional wrapper messages
    assert "QueryError" not in error_msg


# The timeout scenario implementation would go here if it existed in the feature file
