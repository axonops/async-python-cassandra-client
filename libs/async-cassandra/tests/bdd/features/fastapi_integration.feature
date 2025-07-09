Feature: FastAPI Integration
  As a FastAPI developer
  I want to use async-cassandra in my web application
  So that I can build responsive APIs with Cassandra backend

  Background:
    Given a FastAPI application with async-cassandra
    And a running Cassandra cluster with test data
    And the FastAPI test client is initialized

  @critical @fastapi
  Scenario: Simple REST API endpoint
    Given a user endpoint that queries Cassandra
    When I send a GET request to "/users/123"
    Then I should receive a 200 response
    And the response should contain user data
    And the request should complete within 100ms

  @critical @fastapi @concurrency
  Scenario: Handle concurrent API requests
    Given a product search endpoint
    When I send 100 concurrent search requests
    Then all requests should receive valid responses
    And no request should take longer than 500ms
    And the Cassandra connection pool should not be exhausted

  @fastapi @error_handling
  Scenario: API error handling for database issues
    Given a FastAPI application with async-cassandra
    And a running Cassandra cluster with test data
    And the FastAPI test client is initialized
    And a Cassandra query that will fail
    When I send a request that triggers the failing query
    Then I should receive a 500 error response
    And the error should not expose internal details
    And the connection should be returned to the pool

  @fastapi @startup_shutdown
  Scenario: Application lifecycle management
    Given a FastAPI application with async-cassandra
    And a running Cassandra cluster with test data
    And the FastAPI test client is initialized
    When the FastAPI application starts up
    Then the Cassandra cluster connection should be established
    And the connection pool should be initialized
    When the application shuts down
    Then all active queries should complete or timeout
    And all connections should be properly closed
    And no resource warnings should be logged

  @fastapi @dependency_injection
  Scenario: Use async-cassandra with FastAPI dependencies
    Given a FastAPI application with async-cassandra
    And a running Cassandra cluster with test data
    And the FastAPI test client is initialized
    And a FastAPI dependency that provides a Cassandra session
    When I use this dependency in multiple endpoints
    Then each request should get a working session
    And sessions should be properly managed per request
    And no session leaks should occur between requests

  @fastapi @streaming
  Scenario: Stream large datasets through API
    Given a FastAPI application with async-cassandra
    And a running Cassandra cluster with test data
    And the FastAPI test client is initialized
    And an endpoint that returns 10,000 records
    When I request the data with streaming enabled
    Then the response should start immediately
    And data should be streamed in chunks
    And memory usage should remain constant
    And the client should be able to cancel mid-stream

  @fastapi @pagination
  Scenario: Implement cursor-based pagination
    Given a FastAPI application with async-cassandra
    And a running Cassandra cluster with test data
    And the FastAPI test client is initialized
    And a paginated endpoint for listing items
    When I request the first page with limit 20
    Then I should receive 20 items and a next cursor
    When I request the next page using the cursor
    Then I should receive the next 20 items
    And pagination should work correctly under concurrent access

  @fastapi @caching
  Scenario: Implement query result caching
    Given a FastAPI application with async-cassandra
    And a running Cassandra cluster with test data
    And the FastAPI test client is initialized
    And an endpoint with query result caching enabled
    When I make the same request multiple times
    Then the first request should query Cassandra
    And subsequent requests should use cached data
    And cache should expire after the configured TTL
    And cache should be invalidated on data updates

  @fastapi @prepared_statements
  Scenario: Use prepared statements in API endpoints
    Given a FastAPI application with async-cassandra
    And a running Cassandra cluster with test data
    And the FastAPI test client is initialized
    And an endpoint that uses prepared statements
    When I make 1000 requests to this endpoint
    Then statement preparation should happen only once
    And query performance should be optimized
    And the prepared statement cache should be shared across requests

  @fastapi @monitoring
  Scenario: Monitor API and database performance
    Given monitoring is enabled for the FastAPI app
    And a FastAPI application with async-cassandra
    And a running Cassandra cluster with test data
    And the FastAPI test client is initialized
    And a user endpoint that queries Cassandra
    When I make various API requests
    Then metrics should track:
      | metric_type              | description                    |
      | request_count            | Total API requests             |
      | request_duration         | API response times             |
      | cassandra_query_count    | Database queries per endpoint  |
      | cassandra_query_duration | Database query times           |
      | connection_pool_size     | Active connections             |
      | error_rate               | Failed requests percentage     |
    And metrics should be accessible via "/metrics" endpoint

  @critical @fastapi @connection_reuse
  Scenario: Connection reuse across requests
    Given a FastAPI application with async-cassandra
    And a running Cassandra cluster with test data
    And the FastAPI test client is initialized
    And an endpoint that performs multiple queries
    When I make 50 sequential requests
    Then the same Cassandra session should be reused
    And no new connections should be created after warmup
    And each request should complete faster than connection setup time

  @fastapi @background_tasks
  Scenario: Background tasks with Cassandra operations
    Given a FastAPI application with async-cassandra
    And a running Cassandra cluster with test data
    And the FastAPI test client is initialized
    And an endpoint that triggers background Cassandra operations
    When I submit 10 tasks that write to Cassandra
    Then the API should return immediately with 202 status
    And all background writes should complete successfully
    And no resources should leak from background tasks

  @critical @fastapi @graceful_shutdown
  Scenario: Graceful shutdown under load
    Given a FastAPI application with async-cassandra
    And a running Cassandra cluster with test data
    And the FastAPI test client is initialized
    And heavy concurrent load on the API
    When the application receives a shutdown signal
    Then in-flight requests should complete successfully
    And new requests should be rejected with 503
    And all Cassandra operations should finish cleanly
    And shutdown should complete within 30 seconds

  @fastapi @middleware
  Scenario: Track Cassandra query metrics in middleware
    Given a middleware that tracks Cassandra query execution
    And a FastAPI application with async-cassandra
    And a running Cassandra cluster with test data
    And the FastAPI test client is initialized
    And endpoints that perform different numbers of queries
    When I make requests to endpoints with varying query counts
    Then the middleware should accurately count queries per request
    And query execution time should be measured
    And async operations should not be blocked by tracking

  @critical @fastapi @connection_failure
  Scenario: Handle Cassandra connection failures gracefully
    Given a FastAPI application with async-cassandra
    And a running Cassandra cluster with test data
    And the FastAPI test client is initialized
    And a healthy API with established connections
    When Cassandra becomes temporarily unavailable
    Then API should return 503 Service Unavailable
    And error messages should be user-friendly
    When Cassandra becomes available again
    Then API should automatically recover
    And no manual intervention should be required

  @fastapi @websocket
  Scenario: WebSocket endpoint with Cassandra streaming
    Given a FastAPI application with async-cassandra
    And a running Cassandra cluster with test data
    And the FastAPI test client is initialized
    And a WebSocket endpoint that streams Cassandra data
    When a client connects and requests real-time updates
    Then the WebSocket should stream query results
    And updates should be pushed as data changes
    And connection cleanup should occur on disconnect

  @critical @fastapi @memory_pressure
  Scenario: Handle memory pressure gracefully
    Given a FastAPI application with async-cassandra
    And a running Cassandra cluster with test data
    And the FastAPI test client is initialized
    And an endpoint that fetches large datasets
    When multiple clients request large amounts of data
    Then memory usage should stay within limits
    And requests should be throttled if necessary
    And the application should not crash from OOM

  @fastapi @auth
  Scenario: Authentication and session isolation
    Given a FastAPI application with async-cassandra
    And a running Cassandra cluster with test data
    And the FastAPI test client is initialized
    And endpoints with per-user Cassandra keyspaces
    When different users make concurrent requests
    Then each user should only access their keyspace
    And sessions should be isolated between users
    And no data should leak between user contexts
