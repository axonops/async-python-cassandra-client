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
    Given a Cassandra query that will fail
    When I send a request that triggers the failing query
    Then I should receive a 500 error response
    And the error should not expose internal details
    And the connection should be returned to the pool

  @fastapi @startup_shutdown
  Scenario: Application lifecycle management
    When the FastAPI application starts up
    Then the Cassandra cluster connection should be established
    And the connection pool should be initialized
    When the application shuts down
    Then all active queries should complete or timeout
    And all connections should be properly closed
    And no resource warnings should be logged

  @fastapi @dependency_injection
  Scenario: Use async-cassandra with FastAPI dependencies
    Given a FastAPI dependency that provides a Cassandra session
    When I use this dependency in multiple endpoints
    Then each request should get a working session
    And sessions should be properly managed per request
    And no session leaks should occur between requests

  @fastapi @streaming
  Scenario: Stream large datasets through API
    Given an endpoint that returns 10,000 records
    When I request the data with streaming enabled
    Then the response should start immediately
    And data should be streamed in chunks
    And memory usage should remain constant
    And the client should be able to cancel mid-stream

  @fastapi @pagination
  Scenario: Implement cursor-based pagination
    Given a paginated endpoint for listing items
    When I request the first page with limit 20
    Then I should receive 20 items and a next cursor
    When I request the next page using the cursor
    Then I should receive the next 20 items
    And pagination should work correctly under concurrent access

  @fastapi @caching
  Scenario: Implement query result caching
    Given an endpoint with query result caching enabled
    When I make the same request multiple times
    Then the first request should query Cassandra
    And subsequent requests should use cached data
    And cache should expire after the configured TTL
    And cache should be invalidated on data updates

  @fastapi @prepared_statements
  Scenario: Use prepared statements in API endpoints
    Given an endpoint that uses prepared statements
    When I make 1000 requests to this endpoint
    Then statement preparation should happen only once
    And query performance should be optimized
    And the prepared statement cache should be shared across requests

  @fastapi @monitoring
  Scenario: Monitor API and database performance
    Given monitoring is enabled for the FastAPI app
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