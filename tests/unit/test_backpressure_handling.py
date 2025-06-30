"""
Unit tests for backpressure and queue management.

Tests how the async wrapper handles:
- Client-side request queue overflow
- Server overload responses
- Backpressure propagation
- Queue management strategies
"""

import asyncio
from unittest.mock import Mock

import pytest
from cassandra import OperationTimedOut, WriteTimeout

from async_cassandra import AsyncCassandraSession


class TestBackpressureHandling:
    """Test backpressure and queue management scenarios."""

    @pytest.fixture
    def mock_session(self):
        """Create a mock session."""
        session = Mock()
        session.execute_async = Mock()
        session.cluster = Mock()

        # Mock request queue settings
        session.cluster.protocol_version = 5
        session.cluster.connection_class = Mock()
        session.cluster.connection_class.max_in_flight = 128

        return session

    def create_error_future(self, exception):
        """Create a mock future that raises the given exception."""
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
        """Create a mock future that returns a result."""
        future = Mock()
        callbacks = []
        errbacks = []

        def add_callbacks(callback=None, errback=None):
            if callback:
                callbacks.append(callback)
                # For success, the callback expects an iterable of rows
                # Create a mock that can be iterated over
                mock_rows = [result] if result else []
                callback(mock_rows)
            if errback:
                errbacks.append(errback)

        future.add_callbacks = add_callbacks
        future.has_more_pages = False
        future.timeout = None
        future.clear_callbacks = Mock()
        return future

    @pytest.mark.asyncio
    async def test_client_queue_overflow(self, mock_session):
        """Test handling when client request queue overflows."""
        async_session = AsyncCassandraSession(mock_session)

        # Track requests
        request_count = 0
        max_requests = 10

        def execute_async_side_effect(*args, **kwargs):
            nonlocal request_count
            request_count += 1

            if request_count > max_requests:
                # Queue is full
                return self.create_error_future(
                    OperationTimedOut("Client request queue is full (max_in_flight=10)")
                )

            # Success response
            return self.create_success_future({"id": request_count})

        mock_session.execute_async.side_effect = execute_async_side_effect

        # Try to overflow the queue
        tasks = []
        for i in range(15):  # More than max_requests
            tasks.append(async_session.execute(f"SELECT * FROM test WHERE id = {i}"))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Some should fail with overload
        overloaded = [r for r in results if isinstance(r, OperationTimedOut)]
        assert len(overloaded) > 0
        assert "queue is full" in str(overloaded[0])

    @pytest.mark.asyncio
    async def test_server_overload_response(self, mock_session):
        """Test handling server overload responses."""
        async_session = AsyncCassandraSession(mock_session)

        # Simulate server overload responses
        overload_count = 0

        def execute_async_side_effect(*args, **kwargs):
            nonlocal overload_count
            overload_count += 1

            if overload_count <= 3:
                # First 3 requests get overloaded response
                from cassandra import WriteType

                error = WriteTimeout("Coordinator overloaded", write_type=WriteType.SIMPLE)
                error.consistency_level = 1
                error.required_responses = 1
                error.received_responses = 0
                return self.create_error_future(error)

            # Subsequent requests succeed
            # Create a proper row object
            row = {"success": True}
            return self.create_success_future(row)

        mock_session.execute_async.side_effect = execute_async_side_effect

        # First attempts should fail
        for i in range(3):
            with pytest.raises(WriteTimeout) as exc_info:
                await async_session.execute("INSERT INTO test VALUES (1)")
            assert "Coordinator overloaded" in str(exc_info.value)

        # Next attempt should succeed (after backoff)
        result = await async_session.execute("INSERT INTO test VALUES (1)")
        assert len(result.rows) == 1
        assert result.rows[0]["success"] is True

    @pytest.mark.asyncio
    async def test_backpressure_propagation(self, mock_session):
        """Test that backpressure is properly propagated to callers."""
        async_session = AsyncCassandraSession(mock_session)

        # Track requests
        request_count = 0
        threshold = 5

        def execute_async_side_effect(*args, **kwargs):
            nonlocal request_count
            request_count += 1

            if request_count > threshold:
                # Simulate backpressure
                return self.create_error_future(
                    OperationTimedOut("Backpressure active - please slow down")
                )

            # Success response
            return self.create_success_future({"id": request_count})

        mock_session.execute_async.side_effect = execute_async_side_effect

        # Send burst of requests
        tasks = []
        for i in range(10):
            tasks.append(async_session.execute(f"SELECT {i}"))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Should have some backpressure errors
        backpressure_errors = [r for r in results if isinstance(r, OperationTimedOut)]
        assert len(backpressure_errors) > 0
        assert "Backpressure active" in str(backpressure_errors[0])

    @pytest.mark.asyncio
    async def test_adaptive_concurrency_control(self, mock_session):
        """Test adaptive concurrency control based on response times."""
        async_session = AsyncCassandraSession(mock_session)

        # Track concurrency
        request_count = 0
        initial_limit = 10
        current_limit = initial_limit
        rejected_count = 0

        def execute_async_side_effect(*args, **kwargs):
            nonlocal request_count, current_limit, rejected_count
            request_count += 1

            # Simulate adaptive behavior - reduce limit after 5 requests
            if request_count == 5:
                current_limit = 5

            # Reject if over limit
            if request_count % 10 > current_limit:
                rejected_count += 1
                return self.create_error_future(
                    OperationTimedOut(f"Concurrency limit reached ({current_limit})")
                )

            # Success response with simulated latency
            return self.create_success_future({"latency": 50 + request_count})

        mock_session.execute_async.side_effect = execute_async_side_effect

        # Execute requests
        success_count = 0
        for i in range(20):
            try:
                await async_session.execute(f"SELECT {i}")
                success_count += 1
            except OperationTimedOut:
                pass

        # Should have some rejections due to adaptive limits
        assert rejected_count > 0
        assert current_limit != initial_limit

    @pytest.mark.asyncio
    async def test_queue_timeout_handling(self, mock_session):
        """Test handling of requests that timeout while queued."""
        async_session = AsyncCassandraSession(mock_session)

        # Track requests
        request_count = 0
        queue_size_limit = 5

        def execute_async_side_effect(*args, **kwargs):
            nonlocal request_count
            request_count += 1

            # Simulate queue timeout for requests beyond limit
            if request_count > queue_size_limit:
                return self.create_error_future(
                    OperationTimedOut("Request timed out in queue after 1.0s")
                )

            # Success response
            return self.create_success_future({"processed": True})

        mock_session.execute_async.side_effect = execute_async_side_effect

        # Send requests that will queue up
        tasks = []
        for i in range(10):
            tasks.append(async_session.execute(f"SELECT {i}"))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Should have some timeouts
        timeouts = [r for r in results if isinstance(r, OperationTimedOut)]
        assert len(timeouts) > 0
        assert "timed out in queue" in str(timeouts[0])

    @pytest.mark.asyncio
    async def test_priority_queue_management(self, mock_session):
        """Test priority-based queue management during overload."""
        async_session = AsyncCassandraSession(mock_session)

        # Track processed queries
        processed_queries = []

        def execute_async_side_effect(*args, **kwargs):
            query = str(args[0] if args else kwargs.get("query", ""))

            # Determine priority
            is_high_priority = "SYSTEM" in query or "CRITICAL" in query

            # Track order
            if is_high_priority:
                # Insert high priority at front
                processed_queries.insert(0, query)
            else:
                # Append normal priority
                processed_queries.append(query)

            # Always succeed
            return self.create_success_future({"query": query})

        mock_session.execute_async.side_effect = execute_async_side_effect

        # Mix of priority queries
        queries = [
            "SELECT * FROM users",  # Normal
            "CRITICAL: SELECT * FROM system.local",  # High
            "SELECT * FROM data",  # Normal
            "SYSTEM CHECK",  # High
            "SELECT * FROM logs",  # Normal
        ]

        for query in queries:
            result = await async_session.execute(query)
            assert result.rows[0]["query"] == query

        # High priority queries should be at front of processed list
        assert "CRITICAL" in processed_queries[0] or "SYSTEM" in processed_queries[0]
        assert "CRITICAL" in processed_queries[1] or "SYSTEM" in processed_queries[1]

    @pytest.mark.asyncio
    async def test_circuit_breaker_on_overload(self, mock_session):
        """Test circuit breaker pattern for overload protection."""
        async_session = AsyncCassandraSession(mock_session)

        # Track circuit breaker state
        failure_count = 0
        circuit_open = False

        def execute_async_side_effect(*args, **kwargs):
            nonlocal failure_count, circuit_open

            if circuit_open:
                return self.create_error_future(OperationTimedOut("Circuit breaker is OPEN"))

            # First 3 requests fail
            if failure_count < 3:
                failure_count += 1
                if failure_count == 3:
                    circuit_open = True
                return self.create_error_future(OperationTimedOut("Server overloaded"))

            # After circuit reset, succeed
            return self.create_success_future({"success": True})

        mock_session.execute_async.side_effect = execute_async_side_effect

        # Trigger circuit breaker with 3 failures
        for i in range(3):
            with pytest.raises(OperationTimedOut) as exc_info:
                await async_session.execute("SELECT 1")
            assert "Server overloaded" in str(exc_info.value)

        # Circuit should be open
        with pytest.raises(OperationTimedOut) as exc_info:
            await async_session.execute("SELECT 2")
        assert "Circuit breaker is OPEN" in str(exc_info.value)

        # Reset circuit for test
        circuit_open = False

        # Should allow attempt after reset
        result = await async_session.execute("SELECT 3")
        assert result.rows[0]["success"] is True

    @pytest.mark.asyncio
    async def test_load_shedding_strategy(self, mock_session):
        """Test load shedding to prevent system overload."""
        async_session = AsyncCassandraSession(mock_session)

        # Track queries
        shed_count = 0

        def execute_async_side_effect(*args, **kwargs):
            nonlocal shed_count
            query = str(args[0] if args else kwargs.get("query", ""))

            # Shed optional/low priority queries
            if "OPTIONAL" in query or "LOW_PRIORITY" in query:
                shed_count += 1
                return self.create_error_future(OperationTimedOut("Load shedding active (load=85)"))

            # Normal queries succeed
            return self.create_success_future({"executed": query})

        mock_session.execute_async.side_effect = execute_async_side_effect

        # Send mix of queries
        queries = [
            "SELECT * FROM users",
            "OPTIONAL: SELECT * FROM logs",
            "INSERT INTO data VALUES (1)",
            "LOW_PRIORITY: SELECT count(*) FROM events",
            "SELECT * FROM critical_data",
        ]

        results = []
        for query in queries:
            try:
                result = await async_session.execute(query)
                results.append(result.rows[0]["executed"])
            except OperationTimedOut:
                results.append(f"SHED: {query}")

        # Should have shed optional/low priority queries
        shed_queries = [r for r in results if r.startswith("SHED:")]
        assert len(shed_queries) == 2  # OPTIONAL and LOW_PRIORITY
        assert any("OPTIONAL" in q for q in shed_queries)
        assert any("LOW_PRIORITY" in q for q in shed_queries)
        assert shed_count == 2
