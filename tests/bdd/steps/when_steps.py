"""When step definitions for BDD tests."""

import asyncio
import time

from cassandra.cluster import NoHostAvailable
from pytest_bdd import parsers, when

from async_cassandra import AsyncCluster


@when("I create an async session", target_fixture="test_context")
async def create_async_session(connection_parameters, test_context):
    """Create an async session."""
    cluster = AsyncCluster(**connection_parameters)
    session = await cluster.connect()
    test_context["cluster"] = cluster
    test_context["session"] = session
    return test_context


@when(parsers.parse("I create {count:d} async sessions simultaneously"))
async def create_multiple_sessions(count, connection_parameters):
    """Create multiple sessions concurrently."""

    async def create_session():
        cluster = AsyncCluster(**connection_parameters)
        session = await cluster.connect()
        return {"cluster": cluster, "session": session}

    start_time = time.time()
    sessions = await asyncio.gather(*[create_session() for _ in range(count)])
    duration = time.time() - start_time

    return {"sessions": sessions, "count": count, "duration": duration}


@when("I attempt to create a connection")
async def attempt_connection(connection_parameters):
    """Attempt to create a connection that may fail."""
    try:
        cluster = AsyncCluster(**connection_parameters)
        session = await cluster.connect()
        return {"success": True, "cluster": cluster, "session": session}
    except NoHostAvailable as e:
        return {"success": False, "error": e}
    except Exception as e:
        return {"success": False, "error": e}


@when(parsers.parse("I execute {num_queries:d} concurrent queries"))
async def execute_concurrent_queries(num_queries, active_async_session):
    """Execute multiple queries concurrently."""

    async def run_query(i):
        start = time.time()
        result = await active_async_session.execute(
            "SELECT * FROM system.local WHERE key = ?", ["local"]
        )
        return {
            "query_id": i,
            "duration": time.time() - start,
            "success": True,
            "rows": len(result.current_rows) if hasattr(result, "current_rows") else 0,
        }

    start_time = time.time()
    results = await asyncio.gather(
        *[run_query(i) for i in range(num_queries)], return_exceptions=True
    )
    total_duration = time.time() - start_time

    return {"results": results, "total_duration": total_duration, "num_queries": num_queries}


@when("I close the session")
async def close_session(create_async_session):
    """Close the session and cluster."""
    session_info = create_async_session
    await session_info["session"].close()
    await session_info["cluster"].shutdown()
    return session_info


@when(parsers.parse('I execute "{query}"'))
async def execute_query(query, active_async_session):
    """Execute a single query."""
    start_time = time.time()
    try:
        result = await active_async_session.execute(query)
        return {"success": True, "result": result, "duration": time.time() - start_time}
    except Exception as e:
        return {"success": False, "error": e, "duration": time.time() - start_time}


@when("I execute a query with parameters")
async def execute_parameterized_queries(active_async_session, table_exists):
    """Execute parameterized queries from table."""
    results = []

    # INSERT query
    insert_query = "INSERT INTO products (id, name) VALUES (?, ?)"
    insert_params = [1, "Laptop"]

    try:
        await active_async_session.execute(insert_query, insert_params)
        results.append({"query": "INSERT", "success": True})
    except Exception as e:
        results.append({"query": "INSERT", "success": False, "error": e})

    # SELECT query
    select_query = "SELECT * FROM products WHERE id = ?"
    select_params = [1]

    try:
        result = await active_async_session.execute(select_query, select_params)
        results.append(
            {
                "query": "SELECT",
                "success": True,
                "data": result.current_rows[0] if result.current_rows else None,
            }
        )
    except Exception as e:
        results.append({"query": "SELECT", "success": False, "error": e})

    return results


@when(parsers.parse('I send a GET request to "{endpoint}"'))
async def send_get_request(endpoint, fastapi_test_client):
    """Send GET request to FastAPI endpoint."""
    start_time = time.time()
    response = fastapi_test_client.get(endpoint)
    duration = time.time() - start_time

    return {
        "response": response,
        "status_code": response.status_code,
        "duration": duration,
        "json": response.json() if response.status_code == 200 else None,
    }


@when(parsers.parse("I send {count:d} concurrent search requests"))
async def send_concurrent_requests(count, fastapi_test_client):
    """Send multiple concurrent requests to FastAPI."""
    import asyncio

    import httpx

    async def make_request(client, i):
        start = time.time()
        response = await client.get(f"/search?q=test{i}")
        return {
            "request_id": i,
            "status_code": response.status_code,
            "duration": time.time() - start,
        }

    async with httpx.AsyncClient(app=fastapi_test_client.app, base_url="http://test") as client:
        start_time = time.time()
        results = await asyncio.gather(*[make_request(client, i) for i in range(count)])
        total_duration = time.time() - start_time

    return {"results": results, "total_duration": total_duration, "count": count}


@when("one Cassandra node fails")
def simulate_node_failure(production_cassandra_cluster):
    """Simulate a node failure."""
    # In real implementation, this would actually stop a node
    failed_node = production_cassandra_cluster["nodes"][0]
    production_cassandra_cluster["failed_nodes"] = [failed_node]
    return failed_node


@when(parsers.parse("I submit {count:d} concurrent queries"))
async def submit_many_concurrent_queries(count, active_async_session):
    """Submit many concurrent queries to test thread pool."""

    async def long_query(i):
        # Simulate a query that takes some time
        await asyncio.sleep(0.01)
        return await active_async_session.execute(
            "SELECT * FROM system.local WHERE key = ?", ["local"]
        )

    start_time = time.time()
    tasks = [long_query(i) for i in range(count)]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    duration = time.time() - start_time

    return {
        "results": results,
        "duration": duration,
        "count": count,
        "errors": [r for r in results if isinstance(r, Exception)],
    }


@when("the network connection is interrupted for 5 seconds")
async def interrupt_network_connection():
    """Simulate network interruption."""
    # In real implementation, this would use iptables or similar
    await asyncio.sleep(5)
    return {"interruption_duration": 5}


@when("the connection is restored")
def restore_network_connection():
    """Restore network connection."""
    return {"connection_restored": True}


@when("Cassandra nodes are restarted one by one")
async def rolling_restart_nodes(production_cassandra_cluster):
    """Simulate rolling restart of Cassandra nodes."""
    restart_log = []

    for node in production_cassandra_cluster["nodes"]:
        restart_log.append({"node": node, "action": "stopping", "timestamp": time.time()})

        # Simulate restart time
        await asyncio.sleep(2)

        restart_log.append({"node": node, "action": "started", "timestamp": time.time()})

        # Wait for node to rejoin
        await asyncio.sleep(3)

    return restart_log


@when("I try to connect without specifying protocol version", target_fixture="connection_result")
async def connect_without_protocol_version(cassandra_version_cluster_running, context):
    """Try to connect without specifying protocol version."""
    try:
        cluster = AsyncCluster(
            contact_points=["127.0.0.1"], port=cassandra_version_cluster_running["port"]
        )
        session = await cluster.connect()
        context.cluster = cluster
        context.session = session
        return {"success": True, "cluster": cluster, "session": session}
    except Exception as e:
        return {"success": False, "error": e, "error_type": type(e).__name__}


@when(
    parsers.parse("I try to connect with protocol version {version:d}"),
    target_fixture="connection_result",
)
async def connect_with_protocol_version(cassandra_version_cluster_running, version, context):
    """Try to connect with a specific protocol version."""
    try:
        cluster = AsyncCluster(
            contact_points=["127.0.0.1"],
            port=cassandra_version_cluster_running["port"],
            protocol_version=version,
        )
        session = await cluster.connect()
        context.cluster = cluster
        context.session = session
        return {
            "success": True,
            "cluster": cluster,
            "session": session,
            "protocol_version": version,
        }
    except Exception as e:
        return {"success": False, "error": e, "error_type": type(e).__name__}


@when(
    parsers.parse("I try to create a cluster with protocol version {version:d}"),
    target_fixture="cluster_creation_result",
)
def create_cluster_with_protocol_version(cassandra_version_cluster_running, version):
    """Try to create a cluster with a specific protocol version."""
    try:
        cluster = AsyncCluster(
            contact_points=["127.0.0.1"],
            port=cassandra_version_cluster_running["port"],
            protocol_version=version,
        )
        return {"success": True, "cluster": cluster, "protocol_version": version}
    except Exception as e:
        return {"success": False, "error": e, "error_type": type(e).__name__}


@when("I connect without specifying protocol version", target_fixture="connection_result")
async def connect_to_cassandra_5(cassandra_version_cluster_running, context):
    """Connect to Cassandra 5 without specifying protocol version."""
    try:
        cluster = AsyncCluster(
            contact_points=["127.0.0.1"], port=cassandra_version_cluster_running["port"]
        )
        session = await cluster.connect()
        context.cluster = cluster
        context.session = session
        # Get the negotiated protocol version
        protocol_version = cluster._cluster.protocol_version
        return {
            "success": True,
            "cluster": cluster,
            "session": session,
            "protocol_version": protocol_version,
        }
    except Exception as e:
        return {"success": False, "error": e, "error_type": type(e).__name__}


@when(
    parsers.parse("I connect with protocol version {version:d}"), target_fixture="connection_result"
)
async def connect_to_cassandra_5_with_version(cassandra_version_cluster_running, version, context):
    """Connect to Cassandra 5 with a specific protocol version."""
    try:
        cluster = AsyncCluster(
            contact_points=["127.0.0.1"],
            port=cassandra_version_cluster_running["port"],
            protocol_version=version,
        )
        session = await cluster.connect()
        context.cluster = cluster
        context.session = session
        # Get the actual protocol version being used
        protocol_version = cluster._cluster.protocol_version
        return {
            "success": True,
            "cluster": cluster,
            "session": session,
            "protocol_version": protocol_version,
        }
    except Exception as e:
        return {"success": False, "error": e, "error_type": type(e).__name__}
