Feature: Cassandra Connection Management
  As a developer using async-cassandra
  I want to manage database connections efficiently
  So that my application remains responsive and doesn't leak resources

  Background:
    Given a Cassandra cluster is running
    And the async-cassandra driver is configured

  @critical @smoke
  Scenario: Create and close a simple connection
    Given I have connection parameters for the local cluster
    When I create an async session
    Then the session should be connected
    And I should be able to execute a simple query
    When I close the session
    Then all resources should be cleaned up

  @critical @concurrency
  Scenario: Handle multiple concurrent connections
    Given I need to handle 100 concurrent requests
    When I create 100 async sessions simultaneously
    Then all sessions should connect successfully
    And no resources should be leaked
    And the event loop should not be blocked

  @error_handling
  Scenario: Gracefully handle connection failures
    Given the Cassandra cluster is unavailable
    When I attempt to create a connection
    Then I should receive a NoHostAvailable error
    And the error should contain helpful diagnostic information
    And no resources should be leaked

  @performance
  Scenario Outline: Connection pool sizing
    Given I configure a connection pool with <pool_size> connections
    When I execute <num_queries> concurrent queries
    Then all queries should complete within <timeout> seconds
    And the pool should maintain <pool_size> connections

    Examples:
      | pool_size | num_queries | timeout |
      | 5         | 50          | 10      |
      | 10        | 100         | 10      |
      | 20        | 200         | 15      |

  @resilience
  Scenario: Recover from temporary connection loss
    Given an established connection to Cassandra
    When the network connection is interrupted for 5 seconds
    And the connection is restored
    Then subsequent queries should succeed
    And the connection pool should recover automatically

  @critical
  Scenario: Connection cleanup on application shutdown
    Given 10 active connections in the pool
    And 5 queries are currently executing
    When the application initiates shutdown
    Then all executing queries should complete or timeout gracefully
    And all connections should be properly closed
    And no warnings should be logged about unclosed connections