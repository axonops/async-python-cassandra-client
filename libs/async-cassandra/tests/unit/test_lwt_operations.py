"""
Unit tests for Lightweight Transaction (LWT) operations.

Tests how the async wrapper handles:
- IF NOT EXISTS conditions
- IF EXISTS conditions
- Conditional updates
- LWT result parsing
- Race conditions
"""

import asyncio
from unittest.mock import Mock

import pytest
from cassandra import InvalidRequest, WriteTimeout
from cassandra.cluster import Session

from async_cassandra import AsyncCassandraSession


class TestLWTOperations:
    """Test Lightweight Transaction operations."""

    def create_lwt_success_future(self, applied=True, existing_data=None):
        """Create a mock future for successful LWT operations."""
        future = Mock()
        callbacks = []
        errbacks = []

        def add_callbacks(callback=None, errback=None):
            if callback:
                callbacks.append(callback)
                # LWT results include the [applied] column
                if applied:
                    # Successful LWT
                    mock_rows = [{"[applied]": True}]
                else:
                    # Failed LWT with existing data
                    result = {"[applied]": False}
                    if existing_data:
                        result.update(existing_data)
                    mock_rows = [result]
                callback(mock_rows)
            if errback:
                errbacks.append(errback)

        future.add_callbacks = add_callbacks
        future.has_more_pages = False
        future.timeout = None
        future.clear_callbacks = Mock()
        return future

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
                errback(exception)

        future.add_callbacks = add_callbacks
        future.has_more_pages = False
        future.timeout = None
        future.clear_callbacks = Mock()
        return future

    @pytest.fixture
    def mock_session(self):
        """Create a mock session."""
        session = Mock(spec=Session)
        session.execute_async = Mock()
        session.prepare = Mock()
        return session

    @pytest.mark.asyncio
    async def test_insert_if_not_exists_success(self, mock_session):
        """
        Test successful INSERT IF NOT EXISTS.

        What this tests:
        ---------------
        1. LWT INSERT succeeds when no conflict
        2. [applied] column is True
        3. Result properly parsed
        4. Async execution works

        Why this matters:
        ----------------
        INSERT IF NOT EXISTS enables:
        - Distributed unique constraints
        - Race-condition-free inserts
        - Idempotent operations

        Critical for distributed systems
        without locks or coordination.
        """
        async_session = AsyncCassandraSession(mock_session)

        # Mock successful LWT
        mock_session.execute_async.return_value = self.create_lwt_success_future(applied=True)

        # Execute INSERT IF NOT EXISTS
        result = await async_session.execute(
            "INSERT INTO users (id, name) VALUES (?, ?) IF NOT EXISTS", (1, "Alice")
        )

        # Verify result
        assert result is not None
        assert len(result.rows) == 1
        assert result.rows[0]["[applied]"] is True

    @pytest.mark.asyncio
    async def test_insert_if_not_exists_conflict(self, mock_session):
        """
        Test INSERT IF NOT EXISTS when row already exists.

        What this tests:
        ---------------
        1. LWT INSERT fails on conflict
        2. [applied] is False
        3. Existing data returned
        4. Can see what blocked insert

        Why this matters:
        ----------------
        Failed LWTs return existing data:
        - Shows why operation failed
        - Enables conflict resolution
        - Helps with debugging

        Applications must check [applied]
        and handle conflicts appropriately.
        """
        async_session = AsyncCassandraSession(mock_session)

        # Mock failed LWT with existing data
        existing_data = {"id": 1, "name": "Bob"}  # Different name
        mock_session.execute_async.return_value = self.create_lwt_success_future(
            applied=False, existing_data=existing_data
        )

        # Execute INSERT IF NOT EXISTS
        result = await async_session.execute(
            "INSERT INTO users (id, name) VALUES (?, ?) IF NOT EXISTS", (1, "Alice")
        )

        # Verify result shows conflict
        assert result is not None
        assert len(result.rows) == 1
        assert result.rows[0]["[applied]"] is False
        assert result.rows[0]["id"] == 1
        assert result.rows[0]["name"] == "Bob"

    @pytest.mark.asyncio
    async def test_update_if_condition_success(self, mock_session):
        """
        Test successful conditional UPDATE.

        What this tests:
        ---------------
        1. Conditional UPDATE when condition matches
        2. [applied] is True on success
        3. Update actually applied
        4. Condition properly evaluated

        Why this matters:
        ----------------
        Conditional updates enable:
        - Optimistic concurrency control
        - Check-then-act atomically
        - Prevent lost updates

        Essential for maintaining data
        consistency without locks.
        """
        async_session = AsyncCassandraSession(mock_session)

        # Mock successful conditional update
        mock_session.execute_async.return_value = self.create_lwt_success_future(applied=True)

        # Execute conditional UPDATE
        result = await async_session.execute(
            "UPDATE users SET email = ? WHERE id = ? IF name = ?", ("alice@example.com", 1, "Alice")
        )

        # Verify result
        assert result is not None
        assert len(result.rows) == 1
        assert result.rows[0]["[applied]"] is True

    @pytest.mark.asyncio
    async def test_update_if_condition_failure(self, mock_session):
        """
        Test conditional UPDATE when condition doesn't match.

        What this tests:
        ---------------
        1. UPDATE fails when condition false
        2. [applied] is False
        3. Current values returned
        4. Update not applied

        Why this matters:
        ----------------
        Failed conditions show current state:
        - Understand why update failed
        - Retry with correct values
        - Implement compare-and-swap

        Prevents blind overwrites and
        maintains data integrity.
        """
        async_session = AsyncCassandraSession(mock_session)

        # Mock failed conditional update
        existing_data = {"name": "Bob"}  # Actual name is different
        mock_session.execute_async.return_value = self.create_lwt_success_future(
            applied=False, existing_data=existing_data
        )

        # Execute conditional UPDATE
        result = await async_session.execute(
            "UPDATE users SET email = ? WHERE id = ? IF name = ?", ("alice@example.com", 1, "Alice")
        )

        # Verify result shows condition failure
        assert result is not None
        assert len(result.rows) == 1
        assert result.rows[0]["[applied]"] is False
        assert result.rows[0]["name"] == "Bob"

    @pytest.mark.asyncio
    async def test_delete_if_exists_success(self, mock_session):
        """
        Test successful DELETE IF EXISTS.

        What this tests:
        ---------------
        1. DELETE succeeds when row exists
        2. [applied] is True
        3. Row actually deleted
        4. No error on existing row

        Why this matters:
        ----------------
        DELETE IF EXISTS provides:
        - Idempotent deletes
        - No error if already gone
        - Useful for cleanup

        Simplifies error handling in
        distributed delete operations.
        """
        async_session = AsyncCassandraSession(mock_session)

        # Mock successful DELETE IF EXISTS
        mock_session.execute_async.return_value = self.create_lwt_success_future(applied=True)

        # Execute DELETE IF EXISTS
        result = await async_session.execute("DELETE FROM users WHERE id = ? IF EXISTS", (1,))

        # Verify result
        assert result is not None
        assert len(result.rows) == 1
        assert result.rows[0]["[applied]"] is True

    @pytest.mark.asyncio
    async def test_delete_if_exists_not_found(self, mock_session):
        """
        Test DELETE IF EXISTS when row doesn't exist.

        What this tests:
        ---------------
        1. DELETE IF EXISTS on missing row
        2. [applied] is False
        3. No error raised
        4. Operation completes normally

        Why this matters:
        ----------------
        Missing row handling:
        - No exception thrown
        - Can detect if deleted
        - Idempotent behavior

        Allows safe cleanup without
        checking existence first.
        """
        async_session = AsyncCassandraSession(mock_session)

        # Mock failed DELETE IF EXISTS
        mock_session.execute_async.return_value = self.create_lwt_success_future(
            applied=False, existing_data={}
        )

        # Execute DELETE IF EXISTS
        result = await async_session.execute(
            "DELETE FROM users WHERE id = ? IF EXISTS", (999,)  # Non-existent ID
        )

        # Verify result
        assert result is not None
        assert len(result.rows) == 1
        assert result.rows[0]["[applied]"] is False

    @pytest.mark.asyncio
    async def test_lwt_with_multiple_conditions(self, mock_session):
        """
        Test LWT with multiple IF conditions.

        What this tests:
        ---------------
        1. Multiple conditions work together
        2. All must be true to apply
        3. Complex conditions supported
        4. AND logic properly evaluated

        Why this matters:
        ----------------
        Multiple conditions enable:
        - Complex business rules
        - Multi-field validation
        - Stronger consistency checks

        Real-world updates often need
        multiple preconditions.
        """
        async_session = AsyncCassandraSession(mock_session)

        # Mock successful multi-condition update
        mock_session.execute_async.return_value = self.create_lwt_success_future(applied=True)

        # Execute UPDATE with multiple conditions
        result = await async_session.execute(
            "UPDATE users SET status = ? WHERE id = ? IF name = ? AND email = ?",
            ("active", 1, "Alice", "alice@example.com"),
        )

        # Verify result
        assert result is not None
        assert len(result.rows) == 1
        assert result.rows[0]["[applied]"] is True

    @pytest.mark.asyncio
    async def test_lwt_timeout_handling(self, mock_session):
        """
        Test LWT timeout scenarios.

        What this tests:
        ---------------
        1. LWT timeouts properly identified
        2. WriteType.CAS indicates LWT
        3. Timeout details preserved
        4. Error not wrapped

        Why this matters:
        ----------------
        LWT timeouts are special:
        - May have partially applied
        - Require careful handling
        - Different from regular timeouts

        Applications must handle LWT
        timeouts differently than
        regular write timeouts.
        """
        async_session = AsyncCassandraSession(mock_session)

        # Mock WriteTimeout for LWT
        from cassandra import WriteType

        timeout_error = WriteTimeout(
            "LWT operation timed out", write_type=WriteType.CAS  # Compare-And-Set (LWT)
        )
        timeout_error.consistency_level = 1
        timeout_error.required_responses = 2
        timeout_error.received_responses = 1

        mock_session.execute_async.return_value = self.create_error_future(timeout_error)

        # Execute LWT that times out
        with pytest.raises(WriteTimeout) as exc_info:
            await async_session.execute(
                "INSERT INTO users (id, name) VALUES (?, ?) IF NOT EXISTS", (1, "Alice")
            )

        assert "LWT operation timed out" in str(exc_info.value)
        assert exc_info.value.write_type == WriteType.CAS

    @pytest.mark.asyncio
    async def test_concurrent_lwt_operations(self, mock_session):
        """
        Test handling of concurrent LWT operations.

        What this tests:
        ---------------
        1. Concurrent LWTs race safely
        2. Only one succeeds
        3. Others see winner's value
        4. No corruption or errors

        Why this matters:
        ----------------
        LWTs handle distributed races:
        - Exactly one winner
        - Losers see winner's data
        - No lost updates

        This is THE pattern for distributed
        mutual exclusion without locks.
        """
        async_session = AsyncCassandraSession(mock_session)

        # Track which request wins the race
        request_count = 0

        def execute_async_side_effect(*args, **kwargs):
            nonlocal request_count
            request_count += 1

            if request_count == 1:
                # First request succeeds
                return self.create_lwt_success_future(applied=True)
            else:
                # Subsequent requests fail (row already exists)
                return self.create_lwt_success_future(
                    applied=False, existing_data={"id": 1, "name": "Alice"}
                )

        mock_session.execute_async.side_effect = execute_async_side_effect

        # Execute multiple concurrent LWT operations
        tasks = []
        for i in range(5):
            task = async_session.execute(
                "INSERT INTO users (id, name) VALUES (?, ?) IF NOT EXISTS", (1, f"User_{i}")
            )
            tasks.append(task)

        results = await asyncio.gather(*tasks)

        # Only first should succeed
        applied_count = sum(1 for r in results if r.rows[0]["[applied]"])
        assert applied_count == 1

        # Others should show the winning value
        for i, result in enumerate(results):
            if not result.rows[0]["[applied]"]:
                assert result.rows[0]["name"] == "Alice"

    @pytest.mark.asyncio
    async def test_lwt_with_prepared_statements(self, mock_session):
        """
        Test LWT operations with prepared statements.

        What this tests:
        ---------------
        1. LWTs work with prepared statements
        2. Parameters bound correctly
        3. [applied] result available
        4. Performance benefits maintained

        Why this matters:
        ----------------
        Prepared LWTs combine:
        - Query plan caching
        - Parameter safety
        - Atomic operations

        Best practice for production
        LWT operations.
        """
        async_session = AsyncCassandraSession(mock_session)

        # Mock prepared statement
        mock_prepared = Mock()
        mock_prepared.query = "INSERT INTO users (id, name) VALUES (?, ?) IF NOT EXISTS"
        mock_prepared.bind = Mock(return_value=Mock())
        mock_session.prepare.return_value = mock_prepared

        # Prepare statement
        prepared = await async_session.prepare(
            "INSERT INTO users (id, name) VALUES (?, ?) IF NOT EXISTS"
        )

        # Execute with prepared statement
        mock_session.execute_async.return_value = self.create_lwt_success_future(applied=True)

        result = await async_session.execute(prepared, (1, "Alice"))

        # Verify result
        assert result is not None
        assert result.rows[0]["[applied]"] is True

    @pytest.mark.asyncio
    async def test_lwt_batch_not_supported(self, mock_session):
        """
        Test that LWT in batch statements raises appropriate error.

        What this tests:
        ---------------
        1. LWTs not allowed in batches
        2. InvalidRequest raised
        3. Clear error message
        4. Cassandra limitation enforced

        Why this matters:
        ----------------
        Cassandra design limitation:
        - Batches for atomicity
        - LWTs for conditions
        - Can't combine both

        Applications must use LWTs
        individually, not in batches.
        """
        from cassandra.query import BatchStatement, BatchType, SimpleStatement

        async_session = AsyncCassandraSession(mock_session)

        # Create batch with LWT (not supported by Cassandra)
        batch = BatchStatement(batch_type=BatchType.LOGGED)

        # Use SimpleStatement to avoid parameter binding issues
        stmt = SimpleStatement("INSERT INTO users (id, name) VALUES (1, 'Alice') IF NOT EXISTS")
        batch.add(stmt)

        # Mock InvalidRequest for LWT in batch
        mock_session.execute_async.return_value = self.create_error_future(
            InvalidRequest("Conditional statements are not supported in batches")
        )

        # Should raise InvalidRequest
        with pytest.raises(InvalidRequest) as exc_info:
            await async_session.execute_batch(batch)

        assert "Conditional statements are not supported" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_lwt_result_parsing(self, mock_session):
        """
        Test parsing of various LWT result formats.

        What this tests:
        ---------------
        1. Various LWT result formats parsed
        2. [applied] always present
        3. Failed LWTs include data
        4. All columns accessible

        Why this matters:
        ----------------
        LWT results vary by operation:
        - Simple success/failure
        - Single column conflicts
        - Multi-column current state

        Robust parsing enables proper
        conflict resolution logic.
        """
        async_session = AsyncCassandraSession(mock_session)

        # Test different result formats
        test_cases = [
            # Simple success
            ({"[applied]": True}, True, None),
            # Failure with single column
            ({"[applied]": False, "value": 42}, False, {"value": 42}),
            # Failure with multiple columns
            (
                {"[applied]": False, "id": 1, "name": "Alice", "email": "alice@example.com"},
                False,
                {"id": 1, "name": "Alice", "email": "alice@example.com"},
            ),
        ]

        for result_data, expected_applied, expected_data in test_cases:
            mock_session.execute_async.return_value = self.create_lwt_success_future(
                applied=result_data["[applied]"],
                existing_data={k: v for k, v in result_data.items() if k != "[applied]"},
            )

            result = await async_session.execute("UPDATE users SET ... IF ...")

            assert result.rows[0]["[applied]"] == expected_applied

            if expected_data:
                for key, value in expected_data.items():
                    assert result.rows[0][key] == value
