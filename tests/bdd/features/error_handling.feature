Feature: Error Handling and Recovery
  As a developer using async-cassandra
  I want robust error handling and recovery mechanisms
  So that my application can handle failures gracefully

  Background:
    Given a Cassandra cluster is running
    And I have an active async session

  @critical @resilience
  Scenario: Handle NoHostAvailable errors
    Given all Cassandra nodes are temporarily unavailable
    When I attempt to execute a query
    Then I should receive a NoHostAvailable error
    And the error should list all attempted hosts
    And the error should include the specific failure reason for each host
    When the Cassandra nodes become available again
    Then subsequent queries should succeed without manual intervention

  @critical @resilience
  Scenario: Retry idempotent queries on timeout
    Given a query "SELECT * FROM users WHERE id = ?" marked as idempotent
    And the first execution will timeout
    When I execute the query with retry policy
    Then the query should be automatically retried
    And the retry should succeed
    And the total execution time should include retry overhead

  @resilience
  Scenario: Handle write timeout appropriately
    Given a non-idempotent INSERT query
    When the query experiences a write timeout
    Then the query should not be automatically retried
    And I should receive a WriteTimeout error
    And the error should indicate the consistency level and replica status

  @resilience
  Scenario: Recover from coordinator node failure
    Given a query is being executed
    When the coordinator node fails mid-execution
    Then the driver should detect the failure
    And the query should be retried on another node
    And the failure should be transparent to the application

  @error_propagation
  Scenario: Preserve error context through async layers
    Given a complex query that will fail
    When the query fails deep in the driver
    Then the error should bubble up through the async wrapper
    And the stack trace should show the original error location
    And the error message should not be altered or wrapped

  @connection_pool
  Scenario: Handle connection pool exhaustion
    Given a connection pool with maximum 10 connections
    When I execute 50 concurrent long-running queries
    Then new queries should queue for available connections
    And no "pool exhausted" errors should occur
    And all queries should eventually complete

  @critical
  Scenario: Clean up resources on error
    Given a streaming query processing large results
    When an error occurs during result iteration
    Then all allocated resources should be freed
    And no memory leaks should occur
    And the session should remain usable

  @monitoring
  Scenario: Track and report errors appropriately
    Given error monitoring is enabled
    When various types of errors occur:
      | error_type        | count |
      | NoHostAvailable   | 3     |
      | WriteTimeout      | 2     |
      | ReadTimeout       | 5     |
      | InvalidRequest    | 1     |
    Then error metrics should accurately reflect all occurrences
    And errors should be categorized by type
    And error details should be available for debugging

  @circuit_breaker
  Scenario: Circuit breaker for consistently failing nodes
    Given a 3-node cluster where one node is consistently failing
    When I execute 100 queries
    Then the failing node should be marked as down after 3 failures
    And subsequent queries should not attempt to use the failing node
    And the circuit breaker should retry the node after 30 seconds