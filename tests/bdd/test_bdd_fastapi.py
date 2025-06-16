"""BDD tests for FastAPI integration scenarios with real Cassandra."""

import asyncio
import concurrent.futures
import time

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from pytest_bdd import given, parsers, scenario, then, when

from async_cassandra import AsyncCluster

# Import the cassandra_container fixture
pytest_plugins = ["tests._fixtures.cassandra"]


@scenario("features/fastapi_integration.feature", "Simple REST API endpoint")
def test_simple_rest_endpoint():
    """Test simple REST API endpoint."""
    pass


@scenario("features/fastapi_integration.feature", "Handle concurrent API requests")
def test_concurrent_requests():
    """Test concurrent API requests."""
    pass


@scenario("features/fastapi_integration.feature", "Application lifecycle management")
def test_lifecycle_management():
    """Test application lifecycle."""
    pass


@pytest.fixture
def fastapi_context(cassandra_container):
    """Context for FastAPI tests."""
    return {
        "app": None,
        "client": None,
        "cluster": None,
        "session": None,
        "container": cassandra_container,
        "response": None,
        "responses": [],
        "start_time": None,
        "duration": None,
        "error": None,
        "metrics": {},
        "startup_complete": False,
        "shutdown_complete": False,
    }


def run_async(coro):
    """Run async code in sync context."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


# Given steps
@given("a FastAPI application with async-cassandra")
def fastapi_app(fastapi_context):
    """Create FastAPI app with async-cassandra."""
    # Use the new lifespan context manager approach
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Startup
        cluster = AsyncCluster(["127.0.0.1"])
        session = await cluster.connect()
        await session.set_keyspace("test_keyspace")

        app.state.cluster = cluster
        app.state.session = session
        fastapi_context["cluster"] = cluster
        fastapi_context["session"] = session
        fastapi_context["startup_complete"] = True

        yield

        # Shutdown
        if app.state.session:
            await app.state.session.close()
        if app.state.cluster:
            await app.state.cluster.shutdown()
        fastapi_context["shutdown_complete"] = True

    app = FastAPI(lifespan=lifespan)
    fastapi_context["app"] = app

    # Initialize state
    app.state.cluster = None
    app.state.session = None


@given("a running Cassandra cluster with test data")
def cassandra_with_data(fastapi_context):
    """Ensure Cassandra has test data."""
    # The container is already running from the fixture
    assert fastapi_context["container"].is_running()

    # Create test tables and data
    async def setup_data():
        cluster = AsyncCluster(["127.0.0.1"])
        session = await cluster.connect()
        await session.set_keyspace("test_keyspace")

        # Create users table
        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id int PRIMARY KEY,
                name text,
                email text,
                age int,
                created_at timestamp,
                updated_at timestamp
            )
        """
        )

        # Insert test users
        await session.execute(
            """
            INSERT INTO users (id, name, email, age, created_at, updated_at)
            VALUES (123, 'Alice', 'alice@example.com', 25, toTimestamp(now()), toTimestamp(now()))
        """
        )

        await session.execute(
            """
            INSERT INTO users (id, name, email, age, created_at, updated_at)
            VALUES (456, 'Bob', 'bob@example.com', 30, toTimestamp(now()), toTimestamp(now()))
        """
        )

        # Create products table
        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS products (
                id int PRIMARY KEY,
                name text,
                price decimal
            )
        """
        )

        # Insert test products
        for i in range(1, 11):
            await session.execute(
                f"""
                INSERT INTO products (id, name, price)
                VALUES ({i}, 'Product {i}', {10.99 * i})
            """
            )

        await session.close()
        await cluster.shutdown()

    run_async(setup_data())


@given("the FastAPI test client is initialized")
def init_test_client(fastapi_context):
    """Initialize test client."""
    app = fastapi_context["app"]

    # Create test client with lifespan management
    # We'll manually handle the lifespan

    # Enter the lifespan context
    test_client = TestClient(app)
    test_client.__enter__()  # This triggers startup

    fastapi_context["client"] = test_client
    fastapi_context["client_entered"] = True


@given("a user endpoint that queries Cassandra")
def user_endpoint(fastapi_context):
    """Create user endpoint."""
    app = fastapi_context["app"]

    @app.get("/users/{user_id}")
    async def get_user(user_id: int):
        """Get user by ID."""
        session = app.state.session

        result = await session.execute("SELECT * FROM users WHERE id = %s", [user_id])

        rows = result.rows
        if not rows:
            raise HTTPException(status_code=404, detail="User not found")

        user = rows[0]
        return {
            "id": user.id,
            "name": user.name,
            "email": user.email,
            "age": user.age,
            "created_at": user.created_at.isoformat() if user.created_at else None,
            "updated_at": user.updated_at.isoformat() if user.updated_at else None,
        }


@given("a product search endpoint")
def product_endpoint(fastapi_context):
    """Create product search endpoint."""
    app = fastapi_context["app"]

    @app.get("/products/search")
    async def search_products(q: str = ""):
        """Search products."""
        session = app.state.session

        # Get all products and filter in memory (for simplicity)
        result = await session.execute("SELECT * FROM products")

        products = []
        for row in result.rows:
            if not q or q.lower() in row.name.lower():
                products.append(
                    {"id": row.id, "name": row.name, "price": float(row.price) if row.price else 0}
                )

        return {"results": products}


# When steps
@when(parsers.parse('I send a GET request to "{path}"'))
def send_get_request(path, fastapi_context):
    """Send GET request."""
    fastapi_context["start_time"] = time.time()
    response = fastapi_context["client"].get(path)
    fastapi_context["response"] = response
    fastapi_context["duration"] = (time.time() - fastapi_context["start_time"]) * 1000


@when(parsers.parse("I send {count:d} concurrent search requests"))
def send_concurrent_requests(count, fastapi_context):
    """Send concurrent requests."""

    def make_request(i):
        return fastapi_context["client"].get("/products/search?q=Product")

    start = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(make_request, i) for i in range(count)]
        responses = [f.result() for f in concurrent.futures.as_completed(futures)]

    fastapi_context["responses"] = responses
    fastapi_context["duration"] = (time.time() - start) * 1000


@when("the FastAPI application starts up")
def app_startup(fastapi_context):
    """Start the application."""
    # The TestClient triggers startup event when first used
    # Make a dummy request to trigger startup
    try:
        fastapi_context["client"].get("/nonexistent")  # This will 404 but triggers startup
    except Exception:
        pass  # Expected 404


@when("the application shuts down")
def app_shutdown(fastapi_context):
    """Shutdown application."""
    # The TestClient handles shutdown when context exits
    # We'll verify it happened in the Then step
    pass


# Then steps
@then(parsers.parse("I should receive a {status_code:d} response"))
def verify_status_code(status_code, fastapi_context):
    """Verify response status code."""
    assert fastapi_context["response"].status_code == status_code


@then("the response should contain user data")
def verify_user_data(fastapi_context):
    """Verify user data in response."""
    data = fastapi_context["response"].json()
    assert "id" in data
    assert "name" in data
    assert "email" in data
    assert data["id"] == 123
    assert data["name"] == "Alice"


@then(parsers.parse("the request should complete within {timeout:d}ms"))
def verify_request_time(timeout, fastapi_context):
    """Verify request completion time."""
    assert fastapi_context["duration"] < timeout


@then("all requests should receive valid responses")
def verify_all_responses(fastapi_context):
    """Verify all responses are valid."""
    assert len(fastapi_context["responses"]) == 100
    for response in fastapi_context["responses"]:
        assert response.status_code == 200
        data = response.json()
        assert "results" in data
        assert len(data["results"]) > 0


@then(parsers.parse("no request should take longer than {timeout:d}ms"))
def verify_no_slow_requests(timeout, fastapi_context):
    """Verify no slow requests."""
    # Overall time for 100 concurrent requests should be reasonable
    # Not 100x single request time
    assert fastapi_context["duration"] < timeout


@then("the Cassandra connection pool should not be exhausted")
def verify_pool_not_exhausted(fastapi_context):
    """Verify connection pool is OK."""
    # All requests succeeded, so pool wasn't exhausted
    assert all(r.status_code == 200 for r in fastapi_context["responses"])


@then("the Cassandra cluster connection should be established")
def verify_cluster_connected(fastapi_context):
    """Verify cluster connection."""
    assert fastapi_context["startup_complete"] is True
    assert fastapi_context["cluster"] is not None
    assert fastapi_context["session"] is not None


@then("the connection pool should be initialized")
def verify_pool_initialized(fastapi_context):
    """Verify connection pool."""
    # Session exists means pool is initialized
    assert fastapi_context["session"] is not None


@then("all active queries should complete or timeout")
def verify_queries_complete(fastapi_context):
    """Verify queries complete."""
    # Check that FastAPI shutdown was clean
    assert fastapi_context["shutdown_complete"] is True
    # Verify session and cluster were available until shutdown
    assert fastapi_context["session"] is not None
    assert fastapi_context["cluster"] is not None


@then("all connections should be properly closed")
def verify_connections_closed(fastapi_context):
    """Verify connections closed."""
    # After shutdown, connections should be closed
    # We need to actually check this after the shutdown event
    with fastapi_context["client"]:
        pass  # This triggers the shutdown

    # Now verify the session and cluster were closed in shutdown
    assert fastapi_context["shutdown_complete"] is True


@then("no resource warnings should be logged")
def verify_no_warnings(fastapi_context):
    """Verify no resource warnings."""
    import warnings

    # Check if any ResourceWarnings were issued
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always", ResourceWarning)
        # Force garbage collection to trigger any pending warnings
        import gc

        gc.collect()

        # Check for resource warnings
        resource_warnings = [
            warning for warning in w if issubclass(warning.category, ResourceWarning)
        ]
        assert len(resource_warnings) == 0, f"Found resource warnings: {resource_warnings}"


# Cleanup
@pytest.fixture(autouse=True)
def cleanup_after_test(fastapi_context):
    """Cleanup resources after each test."""
    yield

    # Cleanup test client if it was entered
    if fastapi_context.get("client_entered") and fastapi_context.get("client"):
        try:
            fastapi_context["client"].__exit__(None, None, None)
        except Exception:
            pass
