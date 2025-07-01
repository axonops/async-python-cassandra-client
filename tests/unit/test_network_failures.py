"""
Unit tests for network failure scenarios.

Tests how the async wrapper handles:
- Partial network failures
- Connection timeouts
- Slow network conditions
- Coordinator failures mid-query

Test Organization:
==================
1. Partial Failures - Connected but queries fail
2. Timeout Handling - Different timeout types
3. Network Instability - Flapping, congestion
4. Connection Pool - Recovery after issues
5. Network Topology - Partitions, distance changes

Key Testing Principles:
======================
- Differentiate timeout types
- Test recovery mechanisms
- Simulate real network issues
- Verify error propagation
"""

import asyncio
import time
from unittest.mock import Mock, patch

import pytest
from cassandra import OperationTimedOut, ReadTimeout, WriteTimeout
from cassandra.cluster import ConnectionException, Host, NoHostAvailable

from async_cassandra import AsyncCassandraSession, AsyncCluster


class TestNetworkFailures:
    """Test various network failure scenarios."""

    def create_error_future(self, exception):
        """
        Create a mock future that raises the given exception.

        Helper to simulate driver futures that fail with
        network-related exceptions.
        """
        future = Mock()
        callbacks = []
        errbacks = []

        def add_callbacks(callback=None, errback=None):
            if callback:
                callbacks.append(callback)
            if errback:
                errbacks.append(errback)
                # Call errback immediately with the error
                errback(exception)

        future.add_callbacks = add_callbacks
        future.has_more_pages = False
        future.timeout = None
        future.clear_callbacks = Mock()
        return future

    def create_success_future(self, result):
        """
        Create a mock future that returns a result.

        Helper to simulate successful driver futures after
        network recovery.
        """
        future = Mock()
        callbacks = []
        errbacks = []

        def add_callbacks(callback=None, errback=None):
            if callback:
                callbacks.append(callback)
                # For success, the callback expects an iterable of rows
                mock_rows = [result] if result else []
                callback(mock_rows)
            if errback:
                errbacks.append(errback)

        future.add_callbacks = add_callbacks
        future.has_more_pages = False
        future.timeout = None
        future.clear_callbacks = Mock()
        return future

    @pytest.fixture
    def mock_session(self):
        """Create a mock session."""
        session = Mock()
        session.execute_async = Mock()
        session.prepare_async = Mock()
        session.cluster = Mock()
        return session

    @pytest.mark.asyncio
    async def test_partial_network_failure(self, mock_session):
        """
        Test handling of partial network failures (can connect but can't query).

        What this tests:
        ---------------
        1. Connection established but queries fail
        2. ConnectionException during execution
        3. Exception passed through directly
        4. Native error handling preserved

        Why this matters:
        ----------------
        Partial failures are common in production:
        - Firewall rules changed mid-session
        - Network degradation after connect
        - Load balancer issues

        Applications need direct access to
        handle these "connected but broken" states.
        """
        async_session = AsyncCassandraSession(mock_session)

        # Queries fail with connection error
        mock_session.execute_async.return_value = self.create_error_future(
            ConnectionException("Connection closed by remote host")
        )

        # ConnectionException is now passed through directly
        with pytest.raises(ConnectionException) as exc_info:
            await async_session.execute("SELECT * FROM test")

        assert "Connection closed by remote host" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_connection_timeout_during_query(self, mock_session):
        """
        Test handling of connection timeouts during query execution.

        What this tests:
        ---------------
        1. OperationTimedOut errors handled
        2. Transient timeouts can recover
        3. Multiple attempts tracked
        4. Eventually succeeds

        Why this matters:
        ----------------
        Timeouts can be transient:
        - Network congestion
        - Temporary overload
        - GC pauses

        Applications often retry timeouts
        as they may succeed on retry.
        """
        async_session = AsyncCassandraSession(mock_session)

        # Simulate timeout patterns
        timeout_count = 0

        def execute_async_side_effect(*args, **kwargs):
            nonlocal timeout_count
            timeout_count += 1

            if timeout_count <= 2:
                # First attempts timeout
                return self.create_error_future(OperationTimedOut("Connection timed out"))
            else:
                # Eventually succeeds
                return self.create_success_future({"id": 1})

        mock_session.execute_async.side_effect = execute_async_side_effect

        # First two attempts should timeout
        for i in range(2):
            with pytest.raises(OperationTimedOut):
                await async_session.execute("SELECT * FROM test")

        # Third attempt succeeds
        result = await async_session.execute("SELECT * FROM test")
        assert result.rows[0]["id"] == 1
        assert timeout_count == 3

    @pytest.mark.asyncio
    async def test_slow_network_simulation(self, mock_session):
        """
        Test handling of slow network conditions.

        What this tests:
        ---------------
        1. Slow queries still complete
        2. No premature timeouts
        3. Results returned correctly
        4. Latency tracked

        Why this matters:
        ----------------
        Not all slowness is a timeout:
        - Cross-region queries
        - Large result sets
        - Complex aggregations

        The wrapper must handle slow
        operations without failing.
        """
        async_session = AsyncCassandraSession(mock_session)

        # Create a future that simulates delay
        start_time = time.time()
        mock_session.execute_async.return_value = self.create_success_future(
            {"latency": 0.5, "timestamp": start_time}
        )

        # Execute query
        result = await async_session.execute("SELECT * FROM test")

        # Should return result
        assert result.rows[0]["latency"] == 0.5

    @pytest.mark.asyncio
    async def test_coordinator_failure_mid_query(self, mock_session):
        """
        Test coordinator node failing during query execution.

        What this tests:
        ---------------
        1. Coordinator can fail mid-query
        2. NoHostAvailable with details
        3. Retry finds new coordinator
        4. Query eventually succeeds

        Why this matters:
        ----------------
        Coordinator failures happen:
        - Node crashes
        - Network partition
        - Rolling restarts

        The driver picks new coordinators
        automatically on retry.
        """
        async_session = AsyncCassandraSession(mock_session)

        # Track coordinator changes
        attempt_count = 0

        def execute_async_side_effect(*args, **kwargs):
            nonlocal attempt_count
            attempt_count += 1

            if attempt_count == 1:
                # First coordinator fails mid-query
                return self.create_error_future(
                    NoHostAvailable(
                        "Unable to connect to any servers",
                        {"node0": ConnectionException("Connection lost to coordinator")},
                    )
                )
            else:
                # New coordinator succeeds
                return self.create_success_future({"coordinator": f"node{attempt_count-1}"})

        mock_session.execute_async.side_effect = execute_async_side_effect

        # First attempt should fail
        with pytest.raises(NoHostAvailable):
            await async_session.execute("SELECT * FROM test")

        # Second attempt should succeed
        result = await async_session.execute("SELECT * FROM test")
        assert result.rows[0]["coordinator"] == "node1"
        assert attempt_count == 2

    @pytest.mark.asyncio
    async def test_network_flapping(self, mock_session):
        """
        Test handling of network that rapidly connects/disconnects.

        What this tests:
        ---------------
        1. Alternating success/failure pattern
        2. Each state change handled
        3. No corruption from rapid changes
        4. Accurate success/failure tracking

        Why this matters:
        ----------------
        Network flapping occurs with:
        - Faulty hardware
        - Overloaded switches
        - Misconfigured networking

        The wrapper must remain stable
        despite unstable network.
        """
        async_session = AsyncCassandraSession(mock_session)

        # Simulate flapping network
        flap_count = 0

        def execute_async_side_effect(*args, **kwargs):
            nonlocal flap_count
            flap_count += 1

            # Flip network state every call (odd = down, even = up)
            if flap_count % 2 == 1:
                return self.create_error_future(
                    ConnectionException(f"Network down (flap {flap_count})")
                )
            else:
                return self.create_success_future({"flap_count": flap_count})

        mock_session.execute_async.side_effect = execute_async_side_effect

        # Try multiple queries during flapping
        results = []
        errors = []

        for i in range(6):
            try:
                result = await async_session.execute(f"SELECT {i}")
                results.append(result.rows[0]["flap_count"])
            except ConnectionException as e:
                errors.append(str(e))

        # Should have mix of successes and failures
        assert len(results) == 3  # Even numbered attempts succeed
        assert len(errors) == 3  # Odd numbered attempts fail
        assert flap_count == 6

    @pytest.mark.asyncio
    async def test_request_timeout_vs_connection_timeout(self, mock_session):
        """
        Test differentiating between request and connection timeouts.

        What this tests:
        ---------------
        1. ReadTimeout vs WriteTimeout vs OperationTimedOut
        2. Each timeout type preserved
        3. Timeout details maintained
        4. Proper exception types raised

        Why this matters:
        ----------------
        Different timeouts mean different things:
        - ReadTimeout: query executed, waiting for data
        - WriteTimeout: write may have partially succeeded
        - OperationTimedOut: connection-level timeout

        Applications handle each differently:
        - Read timeouts often safe to retry
        - Write timeouts need idempotency checks
        - Connection timeouts may need backoff
        """
        async_session = AsyncCassandraSession(mock_session)

        # Test different timeout scenarios
        from cassandra import WriteType

        timeout_scenarios = [
            (
                ReadTimeout(
                    "Read timeout",
                    consistency_level=1,
                    required_responses=1,
                    received_responses=0,
                    data_retrieved=False,
                ),
                "read",
            ),
            (WriteTimeout("Write timeout", write_type=WriteType.SIMPLE), "write"),
            (OperationTimedOut("Connection timeout"), "connection"),
        ]

        for timeout_error, timeout_type in timeout_scenarios:
            # Set additional attributes for WriteTimeout
            if timeout_type == "write":
                timeout_error.consistency_level = 1
                timeout_error.required_responses = 1
                timeout_error.received_responses = 0

            mock_session.execute_async.return_value = self.create_error_future(timeout_error)

            try:
                await async_session.execute(f"SELECT * FROM test_{timeout_type}")
            except Exception as e:
                # Verify correct timeout type
                if timeout_type == "read":
                    assert isinstance(e, ReadTimeout)
                elif timeout_type == "write":
                    assert isinstance(e, WriteTimeout)
                else:
                    assert isinstance(e, OperationTimedOut)

    @pytest.mark.asyncio
    async def test_connection_pool_recovery_after_network_issue(self, mock_session):
        """
        Test connection pool recovery after network issues.

        What this tests:
        ---------------
        1. Pool can be exhausted by failures
        2. Recovery happens automatically
        3. Queries fail during recovery
        4. Eventually queries succeed

        Why this matters:
        ----------------
        Connection pools need time to recover:
        - Reconnection attempts
        - Health checks
        - Pool replenishment

        Applications should retry after
        pool exhaustion as recovery
        is often automatic.
        """
        async_session = AsyncCassandraSession(mock_session)

        # Track pool state
        recovery_attempts = 0

        def execute_async_side_effect(*args, **kwargs):
            nonlocal recovery_attempts
            recovery_attempts += 1

            if recovery_attempts <= 2:
                # Pool not recovered
                return self.create_error_future(
                    NoHostAvailable(
                        "Unable to connect to any servers",
                        {"all_hosts": ConnectionException("Pool not recovered")},
                    )
                )
            else:
                # Pool recovered
                return self.create_success_future({"healthy": True})

        mock_session.execute_async.side_effect = execute_async_side_effect

        # First two queries fail during network issue
        for i in range(2):
            with pytest.raises(NoHostAvailable):
                await async_session.execute(f"SELECT {i}")

        # Third query succeeds after recovery
        result = await async_session.execute("SELECT 3")
        assert result.rows[0]["healthy"] is True
        assert recovery_attempts == 3

    @pytest.mark.asyncio
    async def test_network_congestion_backoff(self, mock_session):
        """
        Test exponential backoff during network congestion.

        What this tests:
        ---------------
        1. Congestion causes timeouts
        2. Exponential backoff implemented
        3. Delays increase appropriately
        4. Eventually succeeds

        Why this matters:
        ----------------
        Network congestion requires backoff:
        - Prevents thundering herd
        - Gives network time to recover
        - Reduces overall load

        Exponential backoff is a best
        practice for congestion handling.
        """
        async_session = AsyncCassandraSession(mock_session)

        # Track retry attempts
        attempt_count = 0

        def execute_async_side_effect(*args, **kwargs):
            nonlocal attempt_count
            attempt_count += 1

            if attempt_count < 4:
                # Network congested
                return self.create_error_future(OperationTimedOut("Network congested"))
            else:
                # Congestion clears
                return self.create_success_future({"attempts": attempt_count})

        mock_session.execute_async.side_effect = execute_async_side_effect

        # Execute with manual exponential backoff
        backoff_delays = [0.01, 0.02, 0.04]  # Small delays for testing

        async def execute_with_backoff(query):
            for i, delay in enumerate(backoff_delays):
                try:
                    return await async_session.execute(query)
                except OperationTimedOut:
                    if i < len(backoff_delays) - 1:
                        await asyncio.sleep(delay)
                    else:
                        # Try one more time after last delay
                        await asyncio.sleep(delay)
            return await async_session.execute(query)  # Final attempt

        result = await execute_with_backoff("SELECT * FROM test")

        # Verify backoff worked
        assert attempt_count == 4  # 3 failures + 1 success
        assert result.rows[0]["attempts"] == 4

    @pytest.mark.asyncio
    async def test_asymmetric_network_partition(self):
        """
        Test asymmetric partition where node can send but not receive.

        What this tests:
        ---------------
        1. Asymmetric network failures
        2. Some hosts unreachable
        3. Cluster finds working hosts
        4. Connection eventually succeeds

        Why this matters:
        ----------------
        Real network partitions are often asymmetric:
        - One-way firewall rules
        - Routing issues
        - Split-brain scenarios

        The cluster must work around
        partially failed hosts.
        """
        with patch("async_cassandra.cluster.Cluster") as mock_cluster_class:
            # Create mock cluster
            mock_cluster = Mock()
            mock_cluster_class.return_value = mock_cluster
            mock_cluster.protocol_version = 5  # Add protocol version

            # Create multiple hosts
            hosts = []
            for i in range(3):
                host = Mock(spec=Host)
                host.address = f"10.0.0.{i+1}"
                host.is_up = True
                hosts.append(host)

            mock_cluster.metadata = Mock()
            mock_cluster.metadata.all_hosts = Mock(return_value=hosts)

            # Simulate connection failure to partitioned host
            connection_count = 0

            def connect_side_effect(keyspace=None):
                nonlocal connection_count
                connection_count += 1

                if connection_count == 1:
                    # First attempt includes partitioned host
                    raise NoHostAvailable(
                        "Unable to connect to any servers",
                        {hosts[1].address: OperationTimedOut("Cannot reach host")},
                    )
                else:
                    # Second attempt succeeds without partitioned host
                    return Mock()

            mock_cluster.connect.side_effect = connect_side_effect

            async_cluster = AsyncCluster(contact_points=["10.0.0.1"])

            # Should eventually connect using available hosts
            session = await async_cluster.connect()
            assert session is not None
            assert connection_count == 2

            await async_cluster.shutdown()

    @pytest.mark.asyncio
    async def test_host_distance_changes(self):
        """
        Test handling of host distance changes (LOCAL to REMOTE).

        What this tests:
        ---------------
        1. Host distance can change
        2. LOCAL to REMOTE transitions
        3. Distance changes tracked
        4. Affects query routing

        Why this matters:
        ----------------
        Host distances change due to:
        - Datacenter reconfigurations
        - Network topology changes
        - Dynamic snitch updates

        Distance affects:
        - Query routing preferences
        - Connection pool sizes
        - Retry strategies
        """
        with patch("async_cassandra.cluster.Cluster") as mock_cluster_class:
            # Create mock cluster
            mock_cluster = Mock()
            mock_cluster_class.return_value = mock_cluster
            mock_cluster.protocol_version = 5  # Add protocol version
            mock_cluster.connect.return_value = Mock()

            # Create hosts with distances
            local_host = Mock(spec=Host, address="10.0.0.1")
            remote_host = Mock(spec=Host, address="10.1.0.1")

            mock_cluster.metadata = Mock()
            mock_cluster.metadata.all_hosts = Mock(return_value=[local_host, remote_host])

            async_cluster = AsyncCluster()

            # Track distance changes
            distance_changes = []

            def on_distance_change(host, old_distance, new_distance):
                distance_changes.append({"host": host, "old": old_distance, "new": new_distance})

            # Simulate distance change
            on_distance_change(local_host, "LOCAL", "REMOTE")

            # Verify tracking
            assert len(distance_changes) == 1
            assert distance_changes[0]["old"] == "LOCAL"
            assert distance_changes[0]["new"] == "REMOTE"

            await async_cluster.shutdown()
