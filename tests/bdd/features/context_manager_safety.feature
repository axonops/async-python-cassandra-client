Feature: Context Manager Safety
  As a developer using async-cassandra
  I want context managers to only close their own resources
  So that shared resources remain available for other operations

  Background:
    Given a running Cassandra cluster
    And a test keyspace "test_context_safety"

  Scenario: Query error doesn't close session
    Given an open session connected to the test keyspace
    When I execute a query that causes an error
    Then the session should remain open and usable
    And I should be able to execute subsequent queries successfully

  Scenario: Streaming error doesn't close session
    Given an open session with test data
    When a streaming operation encounters an error
    Then the streaming result should be closed
    But the session should remain open
    And I should be able to start new streaming operations

  Scenario: Session context manager doesn't close cluster
    Given an open cluster connection
    When I use a session in a context manager that exits with an error
    Then the session should be closed
    But the cluster should remain open
    And I should be able to create new sessions from the cluster

  Scenario: Multiple concurrent streams don't interfere
    Given multiple sessions from the same cluster
    When I stream data concurrently from each session
    Then each stream should complete independently
    And closing one stream should not affect others
    And all sessions should remain usable

  Scenario: Nested context managers close in correct order
    Given a cluster, session, and streaming result in nested context managers
    When the innermost context (streaming) exits
    Then only the streaming result should be closed
    When the middle context (session) exits
    Then only the session should be closed
    When the outer context (cluster) exits
    Then the cluster should be shut down

  Scenario: Thread safety during context exit
    Given a session being used by multiple threads
    When one thread exits a streaming context manager
    Then other threads should still be able to use the session
    And no operations should be interrupted

  Scenario: Context manager handles cancellation correctly
    Given an active streaming operation in a context manager
    When the operation is cancelled
    Then the streaming result should be properly cleaned up
    But the session should remain open and usable
