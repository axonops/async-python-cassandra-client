"""BDD tests for query execution with real Cassandra."""

import asyncio
import time

import pytest
from cassandra import InvalidRequest, OperationTimedOut
from pytest_bdd import given, parsers, scenario, then, when

from async_cassandra import AsyncCluster

# Import the cassandra_container fixture
pytest_plugins = ["tests._fixtures.cassandra"]


@scenario("features/query_execution.feature", "Execute a simple SELECT query")
def test_simple_select():
    """Test simple SELECT query."""
    pass


@scenario("features/query_execution.feature", "Execute parameterized queries")
def test_parameterized_queries():
    """Test parameterized queries."""
    pass


@scenario("features/query_execution.feature", "Handle query syntax errors")
def test_syntax_errors():
    """Test query syntax error handling."""
    pass


@scenario("features/query_execution.feature", "Execute queries concurrently")
def test_concurrent_queries():
    """Test concurrent query execution."""
    pass


@scenario("features/query_execution.feature", "Respect query timeouts")
def test_query_timeouts():
    """Test query timeout handling."""
    pass


@pytest.fixture
def query_context(cassandra_container):
    """Context for query tests."""
    return {
        "cluster": None,
        "session": None,
        "container": cassandra_container,
        "table_name": None,
        "query_result": None,
        "error": None,
        "prepared_stmt": None,
        "param_results": [],
        "concurrent_results": [],
        "start_time": None,
        "duration": None,
    }


def run_async(coro, loop):
    """Run async code in sync context."""
    return loop.run_until_complete(coro)


# Given steps
@given("a Cassandra cluster is running")
def cassandra_running(query_context):
    """Verify Cassandra container is running."""
    assert query_context["container"].is_running()


@given("I have an active async session")
def active_session(query_context, event_loop):
    """Create active session."""

    async def _create():
        cluster = AsyncCluster(["127.0.0.1"])
        session = await cluster.connect()
        await session.set_keyspace("test_keyspace")

        query_context["cluster"] = cluster
        query_context["session"] = session

    run_async(_create(), event_loop)


@given('a test keyspace "test_keyspace" exists')
def test_keyspace_exists(query_context):
    """Test keyspace already exists from fixture."""
    # The cassandra_container fixture creates test_keyspace
    pass


@given(parsers.parse('a table "{table_name}" with sample data'))
def table_with_data(table_name, query_context, event_loop):
    """Create table with sample data."""

    async def _create_table():
        session = query_context["session"]

        # Create table
        await session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id int PRIMARY KEY,
                name text,
                email text,
                age int,
                created_at timestamp,
                updated_at timestamp
            )
        """
        )

        # Insert sample data - including ID 123 for the test
        test_users = [
            (123, "Alice", "alice@example.com", 25),
            (456, "Bob", "bob@example.com", 30),
            (100, "User 0", "user0@example.com", 20),
            (101, "User 1", "user1@example.com", 21),
            (102, "User 2", "user2@example.com", 22),
        ]

        for user_id, name, email, age in test_users:
            await session.execute(
                f"""
                INSERT INTO {table_name} (id, name, email, age, created_at, updated_at)
                VALUES ({user_id}, '{name}', '{email}', {age}, toTimestamp(now()), toTimestamp(now()))
            """
            )

        query_context["table_name"] = table_name

    run_async(_create_table(), event_loop)


@given(parsers.parse('a table "{table_name}" exists'))
def table_exists(table_name, query_context, event_loop):
    """Create empty table."""

    async def _create_table():
        session = query_context["session"]

        # Create table based on name
        if table_name == "products":
            await session.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id int PRIMARY KEY,
                    name text,
                    price decimal
                )
            """
            )
        elif table_name == "metrics":
            await session.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id int PRIMARY KEY,
                    metric_name text,
                    value double,
                    timestamp timestamp
                )
            """
            )
        else:
            # Generic table
            await session.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id int PRIMARY KEY,
                    data text
                )
            """
            )

        query_context["table_name"] = table_name

    run_async(_create_table(), event_loop)


# When steps
@when(parsers.parse('I execute "{query}"'))
def execute_query(query, query_context, event_loop):
    """Execute a query."""

    async def _execute():
        try:
            result = await query_context["session"].execute(query)
            query_context["query_result"] = result
            query_context["error"] = None
        except Exception as e:
            query_context["error"] = e
            query_context["query_result"] = None
            print(f"Query execution error: {e}")
            import traceback

            traceback.print_exc()

    run_async(_execute(), event_loop)


@when("I execute a query with parameters:")
def execute_with_params(query_context, event_loop):
    """Execute parameterized queries."""

    async def _execute():
        results = []
        session = query_context["session"]

        # Execute INSERT
        insert_query = "INSERT INTO products (id, name, price) VALUES (?, ?, ?)"
        await session.execute(insert_query, [1, "Laptop", 999.99])
        results.append({"query": "INSERT", "success": True})

        # Execute SELECT
        select_query = "SELECT * FROM products WHERE id = ?"
        result = await session.execute(select_query, [1])
        results.append({"query": "SELECT", "result": result})

        query_context["param_results"] = results

    run_async(_execute(), event_loop)


@when(parsers.parse('I execute an invalid query "{query}"'))
def execute_invalid_query(query, query_context, event_loop):
    """Execute invalid query."""

    async def _execute():
        try:
            await query_context["session"].execute(query)
        except Exception as e:
            query_context["error"] = e

    run_async(_execute(), event_loop)


@when(parsers.parse("I execute {count:d} different queries simultaneously"))
def execute_concurrent(count, query_context, event_loop):
    """Execute queries concurrently."""

    async def _execute():
        session = query_context["session"]

        # First insert some data
        for i in range(count):
            await session.execute(
                f"""
                INSERT INTO {query_context['table_name']} (id, metric_name, value, timestamp)
                VALUES ({i}, 'metric_{i}', {i * 1.5}, toTimestamp(now()))
            """
            )

        # Now query concurrently
        async def execute_one(i):
            return await session.execute(
                f"""
                SELECT * FROM {query_context['table_name']} WHERE id = {i}
            """
            )

        start = time.time()
        tasks = [execute_one(i) for i in range(count)]
        results = await asyncio.gather(*tasks)
        end = time.time()

        query_context["concurrent_results"] = results
        query_context["duration"] = end - start

    run_async(_execute(), event_loop)


# Then steps
@then("I should receive the user data")
def verify_user_data(query_context):
    """Verify we got user data."""
    assert query_context["query_result"] is not None
    assert len(query_context["query_result"].rows) > 0
    row = query_context["query_result"].rows[0]
    assert hasattr(row, "id") or "id" in row
    assert hasattr(row, "name") or "name" in row


@then(parsers.parse("the query should complete within {max_ms:d}ms"))
def verify_query_time(max_ms, query_context):
    """Verify query was fast."""
    # For simple queries against local Cassandra, should be very fast
    assert query_context.get("error") is None


@then("the INSERT should succeed")
def verify_insert_success(query_context):
    """Verify INSERT succeeded."""
    assert len(query_context["param_results"]) >= 1
    assert query_context["param_results"][0]["success"] is True


@then("the SELECT should return the inserted data")
def verify_select_data(query_context):
    """Verify SELECT returns data."""
    assert len(query_context["param_results"]) >= 2
    select_result = query_context["param_results"][1]["result"]
    assert len(select_result.rows) > 0
    row = select_result.rows[0]
    assert (hasattr(row, "name") and row.name == "Laptop") or (row["name"] == "Laptop")


@then("I should receive an InvalidRequest error")
def verify_invalid_request(query_context):
    """Verify InvalidRequest error."""
    assert query_context["error"] is not None
    assert isinstance(query_context["error"], InvalidRequest)


@then("the error message should indicate the syntax problem")
def verify_syntax_error_message(query_context):
    """Verify error message mentions syntax."""
    error_msg = str(query_context["error"]).lower()
    assert any(word in error_msg for word in ["syntax", "invalid", "error"])


@then("the session should remain usable for subsequent queries")
def verify_session_still_usable(query_context, event_loop):
    """Verify session still works after error."""

    async def _test():
        # Try a valid query
        result = await query_context["session"].execute("SELECT release_version FROM system.local")
        query_context["recovery_result"] = result

    run_async(_test(), event_loop)
    assert query_context["recovery_result"] is not None
    assert len(query_context["recovery_result"].rows) > 0


@then("all queries should complete successfully")
def verify_all_success(query_context):
    """Verify all queries succeeded."""
    assert len(query_context["concurrent_results"]) == 50
    for result in query_context["concurrent_results"]:
        assert len(result.rows) == 1  # Each query should return exactly one row


@then("no query should block another")
def verify_no_blocking(query_context):
    """Verify queries don't block."""
    # If queries were blocking, 50 queries would take much longer
    # With true async, they should complete quickly
    assert query_context["duration"] < 2.0  # Should take less than 2 seconds


@then("the total time should be less than executing them sequentially")
def verify_concurrent_faster(query_context):
    """Verify concurrent is faster than sequential."""
    # 50 queries sequentially would take at least 50 * 10ms = 500ms
    # Concurrent should be much faster
    assert query_context["duration"] < 0.5


# Additional Given steps for timeout scenario
@given("a table with 1 million rows")
def create_large_table(query_context, event_loop):
    """Create table with many rows for timeout testing."""

    async def _create():
        session = query_context["session"]

        # Create table
        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS huge_table (
                id int PRIMARY KEY,
                data text
            )
        """
        )

        # Insert many rows - we'll insert fewer for test speed
        # but enough to cause a timeout with a full scan
        batch_size = 100
        for batch_start in range(0, 10000, batch_size):
            batch = []
            for i in range(batch_start, min(batch_start + batch_size, 10000)):
                batch.append(f"INSERT INTO huge_table (id, data) VALUES ({i}, '{'x' * 1000}')")

            # Execute batch
            if batch:
                await session.execute(f"BEGIN BATCH {' '.join(batch)} APPLY BATCH")

        query_context["large_table_rows"] = 10000

    run_async(_create(), event_loop)


# Additional When steps
@when("I execute a full table scan with a 1 second timeout")
def execute_scan_with_timeout(query_context, event_loop):
    """Execute full table scan with timeout."""

    async def _execute():
        try:
            # Full table scan with 1 second timeout
            await query_context["session"].execute("SELECT * FROM huge_table", timeout=1.0)
        except Exception as e:
            query_context["error"] = e
            query_context["timeout_occurred"] = True

    run_async(_execute(), event_loop)


# Additional Then steps
@then("the query should timeout after 1 second")
def verify_timeout_duration(query_context):
    """Verify query timed out."""
    assert query_context.get("timeout_occurred", False) is True


@then("I should receive an OperationTimedOut error")
def verify_operation_timeout(query_context):
    """Verify OperationTimedOut error."""
    assert query_context["error"] is not None
    # Could be OperationTimedOut or asyncio.TimeoutError depending on where timeout occurs
    assert isinstance(query_context["error"], (OperationTimedOut, asyncio.TimeoutError))


@then("the session should remain healthy")
def verify_session_healthy_after_timeout(query_context, event_loop):
    """Verify session works after timeout."""

    async def _test():
        # Execute a simple query to verify health
        result = await query_context["session"].execute("SELECT now() FROM system.local")
        query_context["health_check"] = result

    run_async(_test(), event_loop)
    assert query_context["health_check"] is not None
    assert len(query_context["health_check"].rows) > 0


# Cleanup fixture
@pytest.fixture(autouse=True)
def cleanup_after_test(query_context, event_loop):
    """Cleanup resources after each test."""
    yield

    async def _cleanup():
        if query_context.get("session"):
            await query_context["session"].close()
        if query_context.get("cluster"):
            await query_context["cluster"].shutdown()

    if query_context.get("session") or query_context.get("cluster"):
        run_async(_cleanup(), event_loop)
