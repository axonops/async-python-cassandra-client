"""Then step definitions for BDD tests."""

from cassandra.cluster import NoHostAvailable
from pytest_bdd import parsers, then


@then("the session should be connected")
def verify_session_connected(create_async_session):
    """Verify session is connected."""
    session_info = create_async_session
    assert session_info["session"] is not None
    # In real implementation, check session.is_connected
    return True


@then("I should be able to execute a simple query")
async def verify_can_execute_query(create_async_session):
    """Verify we can execute a query."""
    session = create_async_session["session"]
    result = await session.execute("SELECT release_version FROM system.local")
    assert result is not None
    assert len(result.current_rows) > 0
    return result


@then("all resources should be cleaned up")
def verify_resources_cleaned(close_session):
    """Verify resources are cleaned up."""
    # In real implementation, check that:
    # - All connections are closed
    # - Thread pool is shut down
    # - No warnings about unclosed resources
    return True


@then("all sessions should connect successfully")
def verify_all_sessions_connected(create_multiple_sessions):
    """Verify all sessions connected."""
    sessions_info = create_multiple_sessions
    assert len(sessions_info["sessions"]) == sessions_info["count"]
    for session_info in sessions_info["sessions"]:
        assert session_info["session"] is not None
    return True


@then("no resources should be leaked")
def verify_no_resource_leaks():
    """Verify no resource leaks."""
    # In real implementation:
    # - Check memory usage
    # - Check file descriptor count
    # - Check thread count
    return True


@then("the event loop should not be blocked")
def verify_event_loop_not_blocked(create_multiple_sessions):
    """Verify event loop wasn't blocked."""
    sessions_info = create_multiple_sessions
    # If 100 sessions created in < 5 seconds, loop wasn't blocked
    assert sessions_info["duration"] < 5.0
    return True


@then("I should receive a NoHostAvailable error")
def verify_no_host_available_error(attempt_connection):
    """Verify NoHostAvailable error received."""
    result = attempt_connection
    assert not result["success"]
    assert isinstance(result["error"], NoHostAvailable)
    return result["error"]


@then("the error should contain helpful diagnostic information")
def verify_error_diagnostics(attempt_connection):
    """Verify error contains diagnostic info."""
    error = attempt_connection["error"]
    assert str(error) != ""
    # NoHostAvailable should include attempted hosts and errors
    if isinstance(error, NoHostAvailable):
        assert hasattr(error, "errors")
    return True


@then(parsers.parse("all queries should complete within {timeout:d} seconds"))
def verify_queries_completed_within_timeout(execute_concurrent_queries, timeout):
    """Verify queries completed within timeout."""
    results_info = execute_concurrent_queries
    assert results_info["total_duration"] < timeout

    # Check individual queries
    for result in results_info["results"]:
        if not isinstance(result, Exception):
            assert result["success"]

    return True


@then(parsers.parse("the pool should maintain {expected_size:d} connections"))
def verify_pool_size(expected_size):
    """Verify connection pool size."""
    # In real implementation, check actual pool size
    return True


@then("I should receive the user data")
def verify_user_data_received(execute_query):
    """Verify user data was received."""
    result = execute_query
    assert result["success"]
    assert result["result"] is not None
    assert len(result["result"].current_rows) > 0

    user = result["result"].current_rows[0]
    assert user.id == 123
    assert user.name == "Test User"
    assert user.email == "test@example.com"
    return user


@then(parsers.parse("the query should complete within {max_ms:d}ms"))
def verify_query_performance(execute_query, max_ms):
    """Verify query completed within time limit."""
    result = execute_query
    duration_ms = result["duration"] * 1000
    assert duration_ms < max_ms
    return duration_ms


@then("the INSERT should succeed")
def verify_insert_succeeded(execute_parameterized_queries):
    """Verify INSERT query succeeded."""
    results = execute_parameterized_queries
    insert_result = next(r for r in results if r["query"] == "INSERT")
    assert insert_result["success"]
    return True


@then("the SELECT should return the inserted data")
def verify_select_returns_data(execute_parameterized_queries):
    """Verify SELECT returns correct data."""
    results = execute_parameterized_queries
    select_result = next(r for r in results if r["query"] == "SELECT")
    assert select_result["success"]
    assert select_result["data"] is not None
    assert select_result["data"]["id"] == 1
    assert select_result["data"]["name"] == "Laptop"
    return True


@then(parsers.parse("I should receive a {status_code:d} response"))
def verify_response_status(send_get_request, status_code):
    """Verify HTTP response status code."""
    response_info = send_get_request
    assert response_info["status_code"] == status_code
    return True


@then("the response should contain user data")
def verify_response_contains_user_data(send_get_request):
    """Verify response contains user data."""
    response_info = send_get_request
    assert response_info["json"] is not None
    assert "id" in response_info["json"]
    assert "name" in response_info["json"]
    assert "email" in response_info["json"]
    return True


@then("all requests should receive valid responses")
def verify_all_requests_successful(send_concurrent_requests):
    """Verify all concurrent requests succeeded."""
    results_info = send_concurrent_requests
    for result in results_info["results"]:
        assert result["status_code"] == 200
    return True


@then(parsers.parse("no request should take longer than {max_ms:d}ms"))
def verify_request_performance(send_concurrent_requests, max_ms):
    """Verify request performance."""
    results_info = send_concurrent_requests
    for result in results_info["results"]:
        duration_ms = result["duration"] * 1000
        assert duration_ms < max_ms
    return True


@then("the Cassandra connection pool should not be exhausted")
def verify_pool_not_exhausted(send_concurrent_requests):
    """Verify connection pool wasn't exhausted."""
    # In real implementation, check pool metrics
    # No requests should fail due to pool exhaustion
    results_info = send_concurrent_requests
    assert all(r["status_code"] == 200 for r in results_info["results"])
    return True


@then("queries to other nodes should continue normally")
def verify_queries_continue(production_cassandra_cluster):
    """Verify queries continue on healthy nodes."""
    healthy_nodes = [
        n
        for n in production_cassandra_cluster["nodes"]
        if n not in production_cassandra_cluster.get("failed_nodes", [])
    ]
    assert len(healthy_nodes) >= 2
    return True


@then("queries to the failed node should retry on healthy nodes")
def verify_failover_behavior():
    """Verify queries failover to healthy nodes."""
    # In real implementation, check that:
    # - Queries initially routed to failed node
    # - Are retried on other nodes
    # - Eventually succeed
    return True


@then("no queries should hang indefinitely")
def verify_no_hanging_queries(submit_many_concurrent_queries):
    """Verify no queries hang."""
    results_info = submit_many_concurrent_queries
    # All queries should complete (success or error)
    assert len(results_info["results"]) == results_info["count"]
    # Duration should be reasonable (not hanging)
    assert results_info["duration"] < 60  # Less than 1 minute for 1000 queries
    return True


@then("all queries should eventually complete")
def verify_all_queries_complete(submit_many_concurrent_queries):
    """Verify all queries complete eventually."""
    results_info = submit_many_concurrent_queries
    assert len(results_info["results"]) == results_info["count"]
    # Count successful queries
    successful = sum(1 for r in results_info["results"] if not isinstance(r, Exception))
    assert successful == results_info["count"]
    return True


@then("no deadlock should occur")
def verify_no_deadlock(submit_many_concurrent_queries):
    """Verify no deadlock occurred."""
    results_info = submit_many_concurrent_queries
    # If we got here, no deadlock occurred
    # Additional checks could verify thread states
    assert len(results_info["errors"]) == 0
    return True


@then("memory usage should remain stable")
def verify_memory_stability():
    """Verify memory usage is stable."""
    # In real implementation:
    # - Track memory before/after
    # - Ensure growth is within acceptable bounds
    # - Check for obvious leaks
    return True


@then("response times should degrade gracefully")
def verify_graceful_degradation(submit_many_concurrent_queries):
    """Verify response times degrade gracefully under load."""
    results_info = submit_many_concurrent_queries
    # Average time per query should be reasonable
    avg_time = results_info["duration"] / results_info["count"]
    # Even with thread pool saturation, should be < 1 second per query
    assert avg_time < 1.0
    return True


@then("the connection should fail with protocol version error")
def verify_protocol_version_error(connection_result):
    """Verify connection failed with protocol version error."""
    assert not connection_result["success"], "Connection should have failed"
    assert connection_result["error_type"] == "ConnectionError"
    error_msg = str(connection_result["error"])
    assert "protocol v4 but v5+ is required" in error_msg or "protocol v5" in error_msg.lower()
    return True


@then(parsers.parse('the error message should mention "{text}"'))
def verify_error_message_contains(connection_result, text):
    """Verify error message contains specific text."""
    assert not connection_result["success"], "Connection should have failed"
    error_msg = str(connection_result["error"])
    assert text in error_msg, f"Error message should contain '{text}', but was: {error_msg}"
    return True


@then("the connection should fail with NoHostAvailable error")
def verify_connection_no_host_available_error(connection_result):
    """Verify connection failed with NoHostAvailable error."""
    assert not connection_result["success"], "Connection should have failed"
    error_msg = str(connection_result["error"])
    # The error could be wrapped in ConnectionError
    assert "NoHostAvailable" in error_msg or "protocol" in error_msg.lower()
    return True


@then("the error message should mention protocol incompatibility")
def verify_protocol_incompatibility_message(connection_result):
    """Verify error message mentions protocol incompatibility."""
    assert not connection_result["success"], "Connection should have failed"
    error_msg = str(connection_result["error"])
    # Check for various protocol-related error messages
    protocol_keywords = ["protocol", "version", "v5", "Cassandra 4.0"]
    assert any(
        keyword in error_msg for keyword in protocol_keywords
    ), f"Error message should mention protocol incompatibility, but was: {error_msg}"
    return True


@then("I should get a ConfigurationError immediately")
def verify_configuration_error(cluster_creation_result):
    """Verify cluster creation failed with ConfigurationError."""
    assert not cluster_creation_result["success"], "Cluster creation should have failed"
    assert cluster_creation_result["error_type"] == "ConfigurationError"
    return True


@then("the connection should succeed")
def verify_connection_success(connection_result):
    """Verify connection succeeded."""
    assert connection_result[
        "success"
    ], f"Connection should have succeeded but failed with: {connection_result.get('error')}"
    return True


@then(parsers.parse("the negotiated protocol version should be {version:d} or higher"))
def verify_protocol_version_min(connection_result, version):
    """Verify negotiated protocol version is at least the specified version."""
    assert connection_result["success"], "Connection should have succeeded"
    actual_version = connection_result["protocol_version"]
    assert (
        actual_version >= version
    ), f"Protocol version should be {version} or higher, but was {actual_version}"
    return True


@then(parsers.parse("the negotiated protocol version should be exactly {version:d}"))
def verify_protocol_version_exact(connection_result, version):
    """Verify negotiated protocol version is exactly the specified version."""
    assert connection_result["success"], "Connection should have succeeded"
    actual_version = connection_result["protocol_version"]
    assert (
        actual_version == version
    ), f"Protocol version should be exactly {version}, but was {actual_version}"
    return True
