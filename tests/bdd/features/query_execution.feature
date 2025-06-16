Feature: Query Execution
  As a developer using async-cassandra
  I want to execute queries asynchronously
  So that my application can handle multiple operations efficiently

  Background:
    Given a Cassandra cluster is running
    And I have an active async session
    And a test keyspace "test_keyspace" exists

  @critical @smoke
  Scenario: Execute a simple SELECT query
    Given a table "users" with sample data
    When I execute "SELECT * FROM users WHERE id = 123"
    Then I should receive the user data
    And the query should complete within 100ms

  @critical
  Scenario: Execute parameterized queries
    Given a table "products" exists
    When I execute a query with parameters:
      | query                                    | parameters       |
      | INSERT INTO products (id, name) VALUES (?, ?) | [1, "Laptop"]    |
      | SELECT * FROM products WHERE id = ?     | [1]              |
    Then the INSERT should succeed
    And the SELECT should return the inserted data

  @prepared_statements
  Scenario: Use prepared statements for better performance
    Given a table "orders" exists
    When I prepare the statement "INSERT INTO orders (id, total) VALUES (?, ?)"
    And I execute the prepared statement 1000 times with different values
    Then all executions should succeed
    And the average execution time should be less than 5ms

  @batch_operations
  Scenario: Execute batch operations
    Given a table "events" exists
    When I create a batch with 100 INSERT statements
    And I execute the batch
    Then all inserts should be applied atomically
    And I should be able to query all 100 events

  @error_handling
  Scenario: Handle query syntax errors
    When I execute an invalid query "SELCT * FORM users"
    Then I should receive an InvalidRequest error
    And the error message should indicate the syntax problem
    And the session should remain usable for subsequent queries

  @error_handling
  Scenario: Respect query timeouts
    Given a table with 1 million rows
    When I execute a full table scan with a 1 second timeout
    Then the query should timeout after 1 second
    And I should receive an OperationTimedOut error
    And the session should remain healthy

  @streaming
  Scenario: Stream large result sets
    Given a table "logs" with 100,000 rows
    When I execute a streaming query with fetch size 1000
    Then I should receive results in pages of 1000 rows
    And memory usage should remain constant
    And I should be able to cancel streaming at any time

  @concurrency
  Scenario: Execute queries concurrently
    Given a table "metrics" exists
    When I execute 50 different queries simultaneously
    Then all queries should complete successfully
    And no query should block another
    And the total time should be less than executing them sequentially

  @fire_and_forget
  Scenario: Execute fire-and-forget queries
    Given a table "audit_log" exists
    When I execute 1000 INSERT queries in fire-and-forget mode
    Then the queries should return immediately
    And the inserts should eventually be applied
    And the application should not wait for confirmations