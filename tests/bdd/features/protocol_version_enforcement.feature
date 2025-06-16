Feature: Protocol Version Enforcement
    As a developer using async-cassandra
    I want the library to enforce CQL protocol v5 or higher
    So that I can use modern async features safely

    @protocol_version @cassandra_3_11
    Scenario: Connection to Cassandra 3.11 should fail with clear error
        Given a Cassandra 3.11 cluster is running on port 9142
        When I try to connect without specifying protocol version
        Then the connection should fail with protocol version error
        And the error message should mention "protocol v4"
        And the error message should mention "Cassandra 4.0+"
        And the error message should mention "Please upgrade your Cassandra cluster"

    @protocol_version @cassandra_3_11
    Scenario: Explicit protocol v5 with Cassandra 3.11 should fail immediately
        Given a Cassandra 3.11 cluster is running on port 9142
        When I try to connect with protocol version 5
        Then the connection should fail with NoHostAvailable error
        And the error message should mention protocol incompatibility

    @protocol_version @cassandra_3_11
    Scenario: Explicit protocol v4 should be rejected at configuration time
        Given a Cassandra 3.11 cluster is running on port 9142
        When I try to create a cluster with protocol version 4
        Then I should get a ConfigurationError immediately
        And the error message should mention "Protocol version 4 is not supported"

    @protocol_version @cassandra_5
    Scenario: Connection to Cassandra 5 should succeed with protocol v5 or higher
        Given a Cassandra 5.0 cluster is running on port 9042
        When I connect without specifying protocol version
        Then the connection should succeed
        And the negotiated protocol version should be 5 or higher

    @protocol_version @cassandra_5
    Scenario: Explicit protocol v5 with Cassandra 5 should succeed
        Given a Cassandra 5.0 cluster is running on port 9042
        When I connect with protocol version 5
        Then the connection should succeed
        And the negotiated protocol version should be exactly 5

    @protocol_version @cassandra_5
    Scenario: Explicit protocol v6 with Cassandra 5 should fail due to beta status
        Given a Cassandra 5.0 cluster is running on port 9042
        When I try to connect with protocol version 6
        Then the connection should fail with NoHostAvailable error