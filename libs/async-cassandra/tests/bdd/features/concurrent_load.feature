Feature: Concurrent Load Handling
  As a developer using async-cassandra
  I need the driver to handle concurrent requests properly
  So that my application doesn't deadlock or leak memory under load

  Background:
    Given a running Cassandra cluster
    And async-cassandra configured with default settings

  @critical @performance
  Scenario: Thread pool exhaustion prevention
    Given a configured thread pool of 10 threads
    When I submit 1000 concurrent queries
    Then all queries should eventually complete
    And no deadlock should occur
    And memory usage should remain stable
    And response times should degrade gracefully

  @critical @memory
  Scenario: Memory leak prevention under load
    Given a baseline memory measurement
    When I execute 10,000 queries
    Then memory usage should not grow continuously
    And garbage collection should work effectively
    And no resource warnings should be logged
    And performance should remain consistent
