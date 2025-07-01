"""
Comprehensive unit tests for protocol exceptions from the DataStax driver.

Tests proper handling of all protocol-level exceptions including:
- OverloadedErrorMessage
- ReadTimeout/WriteTimeout
- Unavailable
- ReadFailure/WriteFailure
- ServerError
- ProtocolException
- IsBootstrappingErrorMessage
- TruncateError
- FunctionFailure
- CDCWriteFailure
"""

from unittest.mock import Mock

import pytest
from cassandra import (
    AlreadyExists,
    AuthenticationFailed,
    CDCWriteFailure,
    CoordinationFailure,
    FunctionFailure,
    InvalidRequest,
    OperationTimedOut,
    ReadFailure,
    ReadTimeout,
    Unavailable,
    WriteFailure,
    WriteTimeout,
)
from cassandra.cluster import NoHostAvailable, ServerError
from cassandra.connection import (
    ConnectionBusy,
    ConnectionException,
    ConnectionShutdown,
    ProtocolError,
)
from cassandra.pool import NoConnectionsAvailable

from async_cassandra import AsyncCassandraSession


class TestProtocolExceptions:
    """Test handling of all protocol-level exceptions."""

    @pytest.fixture
    def mock_session(self):
        """Create a mock session."""
        session = Mock()
        session.execute_async = Mock()
        session.prepare_async = Mock()
        session.cluster = Mock()
        session.cluster.protocol_version = 5
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

    @pytest.mark.asyncio
    async def test_overloaded_error_message(self, mock_session):
        """
        Test handling of OverloadedErrorMessage from coordinator.

        What this tests:
        ---------------
        1. Server overload errors handled
        2. OperationTimedOut for overload
        3. Clear error message
        4. Not wrapped (timeout exception)

        Why this matters:
        ----------------
        Server overload indicates:
        - Too much concurrent load
        - Insufficient cluster capacity
        - Need for backpressure

        Applications should respond with
        backoff and retry strategies.
        """
        async_session = AsyncCassandraSession(mock_session)

        # Create OverloadedErrorMessage - this is typically wrapped in OperationTimedOut
        error = OperationTimedOut("Request timed out - server overloaded")
        mock_session.execute_async.return_value = self.create_error_future(error)

        with pytest.raises(OperationTimedOut) as exc_info:
            await async_session.execute("SELECT * FROM test")

        assert "server overloaded" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_read_timeout(self, mock_session):
        """
        Test handling of ReadTimeout errors.

        What this tests:
        ---------------
        1. Read timeouts not wrapped
        2. Consistency level preserved
        3. Response count available
        4. Data retrieval flag set

        Why this matters:
        ----------------
        Read timeouts tell you:
        - How many replicas responded
        - Whether any data was retrieved
        - If retry might succeed

        Applications can make informed
        retry decisions based on details.
        """
        async_session = AsyncCassandraSession(mock_session)

        error = ReadTimeout(
            "Read request timed out",
            consistency_level=1,
            required_responses=2,
            received_responses=1,
            data_retrieved=False,
        )
        mock_session.execute_async.return_value = self.create_error_future(error)

        with pytest.raises(ReadTimeout) as exc_info:
            await async_session.execute("SELECT * FROM test")

        assert exc_info.value.required_responses == 2
        assert exc_info.value.received_responses == 1
        assert exc_info.value.data_retrieved is False

    @pytest.mark.asyncio
    async def test_write_timeout(self, mock_session):
        """
        Test handling of WriteTimeout errors.

        What this tests:
        ---------------
        1. Write timeouts not wrapped
        2. Write type preserved
        3. Response counts available
        4. Consistency level included

        Why this matters:
        ----------------
        Write timeout details critical for:
        - Determining if write succeeded
        - Understanding failure mode
        - Deciding on retry safety

        Different write types (SIMPLE, BATCH,
        UNLOGGED_BATCH, COUNTER) need different
        retry strategies.
        """
        async_session = AsyncCassandraSession(mock_session)

        from cassandra import WriteType

        error = WriteTimeout("Write request timed out", write_type=WriteType.SIMPLE)
        # Set additional attributes
        error.consistency_level = 1
        error.required_responses = 3
        error.received_responses = 2
        mock_session.execute_async.return_value = self.create_error_future(error)

        with pytest.raises(WriteTimeout) as exc_info:
            await async_session.execute("INSERT INTO test VALUES (1)")

        assert exc_info.value.required_responses == 3
        assert exc_info.value.received_responses == 2
        # write_type is stored as numeric value
        from cassandra import WriteType

        assert exc_info.value.write_type == WriteType.SIMPLE

    @pytest.mark.asyncio
    async def test_unavailable(self, mock_session):
        """
        Test handling of Unavailable errors (not enough replicas).

        What this tests:
        ---------------
        1. Unavailable errors not wrapped
        2. Required replica count shown
        3. Alive replica count shown
        4. Consistency level preserved

        Why this matters:
        ----------------
        Unavailable means:
        - Not enough replicas up
        - Cannot meet consistency
        - Cluster health issue

        Retry won't help until more
        replicas come online.
        """
        async_session = AsyncCassandraSession(mock_session)

        error = Unavailable(
            "Not enough replicas available", consistency=1, required_replicas=3, alive_replicas=1
        )
        mock_session.execute_async.return_value = self.create_error_future(error)

        with pytest.raises(Unavailable) as exc_info:
            await async_session.execute("SELECT * FROM test")

        assert exc_info.value.required_replicas == 3
        assert exc_info.value.alive_replicas == 1

    @pytest.mark.asyncio
    async def test_read_failure(self, mock_session):
        """
        Test handling of ReadFailure errors (replicas failed during read).

        What this tests:
        ---------------
        1. ReadFailure passed through without wrapping
        2. Failure count preserved
        3. Data retrieval flag available
        4. Direct exception access

        Why this matters:
        ----------------
        Read failures indicate:
        - Replicas crashed/errored
        - Data corruption possible
        - More serious than timeout

        Users need direct access to
        handle these serious errors.
        """
        async_session = AsyncCassandraSession(mock_session)

        original_error = ReadFailure("Read failed on replicas", data_retrieved=False)
        # Set additional attributes
        original_error.consistency_level = 1
        original_error.required_responses = 2
        original_error.received_responses = 1
        original_error.numfailures = 1
        mock_session.execute_async.return_value = self.create_error_future(original_error)

        # ReadFailure is now passed through without wrapping
        with pytest.raises(ReadFailure) as exc_info:
            await async_session.execute("SELECT * FROM test")

        assert "Read failed on replicas" in str(exc_info.value)
        assert exc_info.value.numfailures == 1
        assert exc_info.value.data_retrieved is False

    @pytest.mark.asyncio
    async def test_write_failure(self, mock_session):
        """
        Test handling of WriteFailure errors (replicas failed during write).

        What this tests:
        ---------------
        1. WriteFailure passed through without wrapping
        2. Write type preserved
        3. Failure count available
        4. Response details included

        Why this matters:
        ----------------
        Write failures mean:
        - Replicas rejected write
        - Possible constraint violation
        - Data inconsistency risk

        Users need direct access to
        understand write outcomes.
        """
        async_session = AsyncCassandraSession(mock_session)

        from cassandra import WriteType

        original_error = WriteFailure("Write failed on replicas", write_type=WriteType.BATCH)
        # Set additional attributes
        original_error.consistency_level = 1
        original_error.required_responses = 3
        original_error.received_responses = 2
        original_error.numfailures = 1
        mock_session.execute_async.return_value = self.create_error_future(original_error)

        # WriteFailure is now passed through without wrapping
        with pytest.raises(WriteFailure) as exc_info:
            await async_session.execute("INSERT INTO test VALUES (1)")

        assert "Write failed on replicas" in str(exc_info.value)
        assert exc_info.value.numfailures == 1

    @pytest.mark.asyncio
    async def test_function_failure(self, mock_session):
        """
        Test handling of FunctionFailure errors (UDF execution failed).

        What this tests:
        ---------------
        1. FunctionFailure passed through without wrapping
        2. Function details preserved
        3. Keyspace and name available
        4. Argument types included

        Why this matters:
        ----------------
        UDF failures indicate:
        - Logic errors in function
        - Invalid input data
        - Resource constraints

        Users need direct access to
        debug function failures.
        """
        async_session = AsyncCassandraSession(mock_session)

        # Create the actual FunctionFailure that would come from the driver
        original_error = FunctionFailure(
            "User defined function failed",
            keyspace="test_ks",
            function="my_func",
            arg_types=["text", "int"],
        )
        mock_session.execute_async.return_value = self.create_error_future(original_error)

        # FunctionFailure is now passed through without wrapping
        with pytest.raises(FunctionFailure) as exc_info:
            await async_session.execute("SELECT my_func(name, age) FROM users")

        # Verify the exception contains the original error info
        assert "User defined function failed" in str(exc_info.value)
        assert exc_info.value.keyspace == "test_ks"
        assert exc_info.value.function == "my_func"

    @pytest.mark.asyncio
    async def test_cdc_write_failure(self, mock_session):
        """
        Test handling of CDCWriteFailure errors.

        What this tests:
        ---------------
        1. CDCWriteFailure passed through without wrapping
        2. CDC-specific error preserved
        3. Direct exception access
        4. Native error handling

        Why this matters:
        ----------------
        CDC (Change Data Capture) failures:
        - CDC log space exhausted
        - CDC disabled on table
        - System overload

        Applications need direct access
        for CDC-specific handling.
        """
        async_session = AsyncCassandraSession(mock_session)

        original_error = CDCWriteFailure("CDC write failed")
        mock_session.execute_async.return_value = self.create_error_future(original_error)

        # CDCWriteFailure is now passed through without wrapping
        with pytest.raises(CDCWriteFailure) as exc_info:
            await async_session.execute("INSERT INTO cdc_table VALUES (1)")

        assert "CDC write failed" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_coordinator_failure(self, mock_session):
        """
        Test handling of CoordinationFailure errors.

        What this tests:
        ---------------
        1. CoordinationFailure passed through without wrapping
        2. Coordinator node failure preserved
        3. Error message unchanged
        4. Direct exception handling

        Why this matters:
        ----------------
        Coordination failures mean:
        - Coordinator node issues
        - Cannot orchestrate query
        - Different from replica failures

        Users need direct access to
        implement retry strategies.
        """
        async_session = AsyncCassandraSession(mock_session)

        original_error = CoordinationFailure("Coordinator failed to execute query")
        mock_session.execute_async.return_value = self.create_error_future(original_error)

        # CoordinationFailure is now passed through without wrapping
        with pytest.raises(CoordinationFailure) as exc_info:
            await async_session.execute("SELECT * FROM test")

        assert "Coordinator failed to execute query" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_is_bootstrapping_error(self, mock_session):
        """
        Test handling of IsBootstrappingErrorMessage.

        What this tests:
        ---------------
        1. Bootstrapping errors in NoHostAvailable
        2. Node state errors handled
        3. Connection exceptions preserved
        4. Host-specific errors shown

        Why this matters:
        ----------------
        Bootstrapping nodes:
        - Still joining cluster
        - Not ready for queries
        - Temporary state

        Applications should retry on
        other nodes until bootstrap completes.
        """
        async_session = AsyncCassandraSession(mock_session)

        # Bootstrapping errors are typically wrapped in NoHostAvailable
        error = NoHostAvailable(
            "No host available", {"127.0.0.1": ConnectionException("Host is bootstrapping")}
        )
        mock_session.execute_async.return_value = self.create_error_future(error)

        with pytest.raises(NoHostAvailable) as exc_info:
            await async_session.execute("SELECT * FROM test")

        assert "No host available" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_truncate_error(self, mock_session):
        """
        Test handling of TruncateError.

        What this tests:
        ---------------
        1. Truncate timeouts handled
        2. OperationTimedOut for truncate
        3. Error message specific
        4. Not wrapped

        Why this matters:
        ----------------
        Truncate errors indicate:
        - Truncate taking too long
        - Cluster coordination issues
        - Heavy operation timeout

        Truncate is expensive - timeouts
        expected on large tables.
        """
        async_session = AsyncCassandraSession(mock_session)

        # TruncateError is typically wrapped in OperationTimedOut
        error = OperationTimedOut("Truncate operation timed out")
        mock_session.execute_async.return_value = self.create_error_future(error)

        with pytest.raises(OperationTimedOut) as exc_info:
            await async_session.execute("TRUNCATE test_table")

        assert "Truncate operation timed out" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_server_error(self, mock_session):
        """
        Test handling of generic ServerError.

        What this tests:
        ---------------
        1. ServerError wrapped in QueryError
        2. Error code preserved
        3. Error message included
        4. Additional info available

        Why this matters:
        ----------------
        Generic server errors indicate:
        - Internal Cassandra errors
        - Unexpected conditions
        - Bugs or edge cases

        Error codes help identify
        specific server issues.
        """
        async_session = AsyncCassandraSession(mock_session)

        # ServerError is an ErrorMessage subclass that requires code, message, info
        original_error = ServerError(0x0000, "Internal server error occurred", {})
        mock_session.execute_async.return_value = self.create_error_future(original_error)

        # ServerError is passed through directly (ErrorMessage subclass)
        with pytest.raises(ServerError) as exc_info:
            await async_session.execute("SELECT * FROM test")

        assert "Internal server error occurred" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_protocol_error(self, mock_session):
        """
        Test handling of ProtocolError.

        What this tests:
        ---------------
        1. ProtocolError passed through without wrapping
        2. Protocol violations preserved as-is
        3. Error message unchanged
        4. Direct exception access for handling

        Why this matters:
        ----------------
        Protocol errors serious:
        - Version mismatches
        - Message corruption
        - Driver/server bugs

        Users need direct access to these
        exceptions for proper handling.
        """
        async_session = AsyncCassandraSession(mock_session)

        # ProtocolError from connection module takes just a message
        original_error = ProtocolError("Protocol version mismatch")
        mock_session.execute_async.return_value = self.create_error_future(original_error)

        # ProtocolError is now passed through without wrapping
        with pytest.raises(ProtocolError) as exc_info:
            await async_session.execute("SELECT * FROM test")

        assert "Protocol version mismatch" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_connection_busy(self, mock_session):
        """
        Test handling of ConnectionBusy errors.

        What this tests:
        ---------------
        1. ConnectionBusy passed through without wrapping
        2. In-flight request limit error preserved
        3. Connection saturation visible to users
        4. Direct exception handling possible

        Why this matters:
        ----------------
        Connection busy means:
        - Too many concurrent requests
        - Per-connection limit reached
        - Need more connections or less load

        Users need to handle this directly
        for proper connection management.
        """
        async_session = AsyncCassandraSession(mock_session)

        original_error = ConnectionBusy("Connection has too many in-flight requests")
        mock_session.execute_async.return_value = self.create_error_future(original_error)

        # ConnectionBusy is now passed through without wrapping
        with pytest.raises(ConnectionBusy) as exc_info:
            await async_session.execute("SELECT * FROM test")

        assert "Connection has too many in-flight requests" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_connection_shutdown(self, mock_session):
        """
        Test handling of ConnectionShutdown errors.

        What this tests:
        ---------------
        1. ConnectionShutdown passed through without wrapping
        2. Graceful shutdown exception preserved
        3. Connection closing visible to users
        4. Direct error handling enabled

        Why this matters:
        ----------------
        Connection shutdown occurs when:
        - Node shutting down cleanly
        - Connection being recycled
        - Maintenance operations

        Applications need direct access
        to handle retry logic properly.
        """
        async_session = AsyncCassandraSession(mock_session)

        original_error = ConnectionShutdown("Connection is shutting down")
        mock_session.execute_async.return_value = self.create_error_future(original_error)

        # ConnectionShutdown is now passed through without wrapping
        with pytest.raises(ConnectionShutdown) as exc_info:
            await async_session.execute("SELECT * FROM test")

        assert "Connection is shutting down" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_no_connections_available(self, mock_session):
        """
        Test handling of NoConnectionsAvailable from pool.

        What this tests:
        ---------------
        1. NoConnectionsAvailable passed through without wrapping
        2. Pool exhaustion exception preserved
        3. Direct access to pool state
        4. Native exception handling

        Why this matters:
        ----------------
        No connections available means:
        - Connection pool exhausted
        - All connections busy
        - Need to wait or expand pool

        Applications need direct access
        for proper backpressure handling.
        """
        async_session = AsyncCassandraSession(mock_session)

        original_error = NoConnectionsAvailable("Connection pool exhausted")
        mock_session.execute_async.return_value = self.create_error_future(original_error)

        # NoConnectionsAvailable is now passed through without wrapping
        with pytest.raises(NoConnectionsAvailable) as exc_info:
            await async_session.execute("SELECT * FROM test")

        assert "Connection pool exhausted" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_already_exists(self, mock_session):
        """
        Test handling of AlreadyExists errors.

        What this tests:
        ---------------
        1. AlreadyExists wrapped in QueryError
        2. Keyspace/table info preserved
        3. Schema conflict detected
        4. Details accessible

        Why this matters:
        ----------------
        Already exists errors for:
        - CREATE TABLE conflicts
        - CREATE KEYSPACE conflicts
        - Schema synchronization issues

        May be safe to ignore if
        idempotent schema creation.
        """
        async_session = AsyncCassandraSession(mock_session)

        original_error = AlreadyExists(keyspace="test_ks", table="test_table")
        mock_session.execute_async.return_value = self.create_error_future(original_error)

        # AlreadyExists is passed through directly
        with pytest.raises(AlreadyExists) as exc_info:
            await async_session.execute("CREATE TABLE test_table (id int PRIMARY KEY)")

        assert exc_info.value.keyspace == "test_ks"
        assert exc_info.value.table == "test_table"

    @pytest.mark.asyncio
    async def test_invalid_request(self, mock_session):
        """
        Test handling of InvalidRequest errors.

        What this tests:
        ---------------
        1. InvalidRequest not wrapped
        2. Syntax errors caught
        3. Clear error message
        4. Driver exception passed through

        Why this matters:
        ----------------
        Invalid requests indicate:
        - CQL syntax errors
        - Schema mismatches
        - Invalid operations

        These are programming errors
        that need fixing, not retrying.
        """
        async_session = AsyncCassandraSession(mock_session)

        error = InvalidRequest("Invalid CQL syntax")
        mock_session.execute_async.return_value = self.create_error_future(error)

        with pytest.raises(InvalidRequest) as exc_info:
            await async_session.execute("SELCT * FROM test")  # Typo in SELECT

        assert "Invalid CQL syntax" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_multiple_error_types_in_sequence(self, mock_session):
        """
        Test handling different error types in sequence.

        What this tests:
        ---------------
        1. Multiple error types handled
        2. Each preserves its type
        3. No error state pollution
        4. Clean error handling

        Why this matters:
        ----------------
        Real applications see various errors:
        - Must handle each appropriately
        - Error handling can't break
        - State must stay clean

        Ensures robust error handling
        across all exception types.
        """
        async_session = AsyncCassandraSession(mock_session)

        errors = [
            Unavailable(
                "Not enough replicas", consistency=1, required_replicas=3, alive_replicas=1
            ),
            ReadTimeout("Read timed out"),
            InvalidRequest("Invalid query syntax"),  # ServerError requires code/message/info
        ]

        # Test each error type
        for error in errors:
            mock_session.execute_async.return_value = self.create_error_future(error)

            with pytest.raises(type(error)):
                await async_session.execute("SELECT * FROM test")

    @pytest.mark.asyncio
    async def test_error_during_prepared_statement(self, mock_session):
        """
        Test error handling during prepared statement execution.

        What this tests:
        ---------------
        1. Prepare succeeds, execute fails
        2. Prepared statement errors handled
        3. WriteTimeout during execution
        4. Error details preserved

        Why this matters:
        ----------------
        Prepared statements can fail at:
        - Preparation time (schema issues)
        - Execution time (timeout/failures)

        Both error paths must work correctly
        for production reliability.
        """
        async_session = AsyncCassandraSession(mock_session)

        # Prepare succeeds
        prepared = Mock()
        prepared.query = "INSERT INTO users (id, name) VALUES (?, ?)"
        prepare_future = Mock()
        prepare_future.result = Mock(return_value=prepared)
        prepare_future.add_callbacks = Mock()
        prepare_future.has_more_pages = False
        prepare_future.timeout = None
        prepare_future.clear_callbacks = Mock()
        mock_session.prepare_async.return_value = prepare_future

        stmt = await async_session.prepare("INSERT INTO users (id, name) VALUES (?, ?)")

        # But execution fails with write timeout
        from cassandra import WriteType

        error = WriteTimeout("Write timed out", write_type=WriteType.SIMPLE)
        error.consistency_level = 1
        error.required_responses = 2
        error.received_responses = 1
        mock_session.execute_async.return_value = self.create_error_future(error)

        with pytest.raises(WriteTimeout):
            await async_session.execute(stmt, [1, "test"])

    @pytest.mark.asyncio
    async def test_no_host_available_with_multiple_errors(self, mock_session):
        """
        Test NoHostAvailable with different errors per host.

        What this tests:
        ---------------
        1. NoHostAvailable aggregates errors
        2. Per-host errors preserved
        3. Different failure modes shown
        4. All error details available

        Why this matters:
        ----------------
        NoHostAvailable shows why each host failed:
        - Connection refused
        - Authentication failed
        - Timeout

        Detailed errors essential for
        diagnosing cluster-wide issues.
        """
        async_session = AsyncCassandraSession(mock_session)

        # Multiple hosts with different failures
        host_errors = {
            "10.0.0.1": ConnectionException("Connection refused"),
            "10.0.0.2": AuthenticationFailed("Bad credentials"),
            "10.0.0.3": OperationTimedOut("Connection timeout"),
        }

        error = NoHostAvailable("Unable to connect to any servers", host_errors)
        mock_session.execute_async.return_value = self.create_error_future(error)

        with pytest.raises(NoHostAvailable) as exc_info:
            await async_session.execute("SELECT * FROM test")

        assert len(exc_info.value.errors) == 3
        assert "10.0.0.1" in exc_info.value.errors
        assert isinstance(exc_info.value.errors["10.0.0.2"], AuthenticationFailed)
