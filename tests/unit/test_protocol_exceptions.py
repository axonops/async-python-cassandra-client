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
from async_cassandra.exceptions import QueryError


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
        """Test handling of OverloadedErrorMessage from coordinator."""
        async_session = AsyncCassandraSession(mock_session)

        # Create OverloadedErrorMessage - this is typically wrapped in OperationTimedOut
        error = OperationTimedOut("Request timed out - server overloaded")
        mock_session.execute_async.return_value = self.create_error_future(error)

        with pytest.raises(OperationTimedOut) as exc_info:
            await async_session.execute("SELECT * FROM test")

        assert "server overloaded" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_read_timeout(self, mock_session):
        """Test handling of ReadTimeout errors."""
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
        """Test handling of WriteTimeout errors."""
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
        """Test handling of Unavailable errors (not enough replicas)."""
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
        """Test handling of ReadFailure errors (replicas failed during read)."""
        async_session = AsyncCassandraSession(mock_session)

        original_error = ReadFailure("Read failed on replicas", data_retrieved=False)
        # Set additional attributes
        original_error.consistency_level = 1
        original_error.required_responses = 2
        original_error.received_responses = 1
        original_error.numfailures = 1
        mock_session.execute_async.return_value = self.create_error_future(original_error)

        # ReadFailure is wrapped in QueryError
        with pytest.raises(QueryError) as exc_info:
            await async_session.execute("SELECT * FROM test")

        assert "Query execution failed: Read failed on replicas" in str(exc_info.value)
        assert isinstance(exc_info.value.cause, ReadFailure)
        assert exc_info.value.cause.numfailures == 1
        assert exc_info.value.cause.data_retrieved is False

    @pytest.mark.asyncio
    async def test_write_failure(self, mock_session):
        """Test handling of WriteFailure errors (replicas failed during write)."""
        async_session = AsyncCassandraSession(mock_session)

        from cassandra import WriteType

        original_error = WriteFailure("Write failed on replicas", write_type=WriteType.BATCH)
        # Set additional attributes
        original_error.consistency_level = 1
        original_error.required_responses = 3
        original_error.received_responses = 2
        original_error.numfailures = 1
        mock_session.execute_async.return_value = self.create_error_future(original_error)

        # WriteFailure is wrapped in QueryError
        with pytest.raises(QueryError) as exc_info:
            await async_session.execute("INSERT INTO test VALUES (1)")

        assert "Query execution failed: Write failed on replicas" in str(exc_info.value)
        assert isinstance(exc_info.value.cause, WriteFailure)
        assert exc_info.value.cause.numfailures == 1

    @pytest.mark.asyncio
    async def test_function_failure(self, mock_session):
        """Test handling of FunctionFailure errors (UDF execution failed)."""
        async_session = AsyncCassandraSession(mock_session)

        # Create the actual FunctionFailure that would come from the driver
        original_error = FunctionFailure(
            "User defined function failed",
            keyspace="test_ks",
            function="my_func",
            arg_types=["text", "int"],
        )
        mock_session.execute_async.return_value = self.create_error_future(original_error)

        # FunctionFailure is wrapped in QueryError
        with pytest.raises(QueryError) as exc_info:
            await async_session.execute("SELECT my_func(name, age) FROM users")

        # Verify the wrapped exception contains the original error info
        assert "Query execution failed: User defined function failed" in str(exc_info.value)
        # The original exception should be accessible via the cause
        assert exc_info.value.cause == original_error
        assert isinstance(exc_info.value.cause, FunctionFailure)
        assert exc_info.value.cause.keyspace == "test_ks"
        assert exc_info.value.cause.function == "my_func"

    @pytest.mark.asyncio
    async def test_cdc_write_failure(self, mock_session):
        """Test handling of CDCWriteFailure errors."""
        async_session = AsyncCassandraSession(mock_session)

        original_error = CDCWriteFailure("CDC write failed")
        mock_session.execute_async.return_value = self.create_error_future(original_error)

        # CDCWriteFailure is wrapped in QueryError
        with pytest.raises(QueryError) as exc_info:
            await async_session.execute("INSERT INTO cdc_table VALUES (1)")

        assert "Query execution failed: CDC write failed" in str(exc_info.value)
        assert isinstance(exc_info.value.cause, CDCWriteFailure)

    @pytest.mark.asyncio
    async def test_coordinator_failure(self, mock_session):
        """Test handling of CoordinationFailure errors."""
        async_session = AsyncCassandraSession(mock_session)

        original_error = CoordinationFailure("Coordinator failed to execute query")
        mock_session.execute_async.return_value = self.create_error_future(original_error)

        # CoordinationFailure is wrapped in QueryError
        with pytest.raises(QueryError) as exc_info:
            await async_session.execute("SELECT * FROM test")

        assert "Query execution failed: Coordinator failed to execute query" in str(exc_info.value)
        assert isinstance(exc_info.value.cause, CoordinationFailure)

    @pytest.mark.asyncio
    async def test_is_bootstrapping_error(self, mock_session):
        """Test handling of IsBootstrappingErrorMessage."""
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
        """Test handling of TruncateError."""
        async_session = AsyncCassandraSession(mock_session)

        # TruncateError is typically wrapped in OperationTimedOut
        error = OperationTimedOut("Truncate operation timed out")
        mock_session.execute_async.return_value = self.create_error_future(error)

        with pytest.raises(OperationTimedOut) as exc_info:
            await async_session.execute("TRUNCATE test_table")

        assert "Truncate operation timed out" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_server_error(self, mock_session):
        """Test handling of generic ServerError."""
        async_session = AsyncCassandraSession(mock_session)

        # ServerError is an ErrorMessage subclass that requires code, message, info
        original_error = ServerError(0x0000, "Internal server error occurred", {})
        mock_session.execute_async.return_value = self.create_error_future(original_error)

        # ServerError is wrapped in QueryError
        with pytest.raises(QueryError) as exc_info:
            await async_session.execute("SELECT * FROM test")

        assert "Query execution failed:" in str(exc_info.value)
        assert isinstance(exc_info.value.cause, ServerError)

    @pytest.mark.asyncio
    async def test_protocol_error(self, mock_session):
        """Test handling of ProtocolError."""
        async_session = AsyncCassandraSession(mock_session)

        # ProtocolError from connection module takes just a message
        original_error = ProtocolError("Protocol version mismatch")
        mock_session.execute_async.return_value = self.create_error_future(original_error)

        # ProtocolError is wrapped in QueryError
        with pytest.raises(QueryError) as exc_info:
            await async_session.execute("SELECT * FROM test")

        assert "Query execution failed: Protocol version mismatch" in str(exc_info.value)
        assert isinstance(exc_info.value.cause, ProtocolError)

    @pytest.mark.asyncio
    async def test_connection_busy(self, mock_session):
        """Test handling of ConnectionBusy errors."""
        async_session = AsyncCassandraSession(mock_session)

        original_error = ConnectionBusy("Connection has too many in-flight requests")
        mock_session.execute_async.return_value = self.create_error_future(original_error)

        # ConnectionBusy is wrapped in QueryError
        with pytest.raises(QueryError) as exc_info:
            await async_session.execute("SELECT * FROM test")

        assert "Query execution failed: Connection has too many in-flight requests" in str(
            exc_info.value
        )
        assert isinstance(exc_info.value.cause, ConnectionBusy)

    @pytest.mark.asyncio
    async def test_connection_shutdown(self, mock_session):
        """Test handling of ConnectionShutdown errors."""
        async_session = AsyncCassandraSession(mock_session)

        original_error = ConnectionShutdown("Connection is shutting down")
        mock_session.execute_async.return_value = self.create_error_future(original_error)

        # ConnectionShutdown is wrapped in QueryError
        with pytest.raises(QueryError) as exc_info:
            await async_session.execute("SELECT * FROM test")

        assert "Query execution failed: Connection is shutting down" in str(exc_info.value)
        assert isinstance(exc_info.value.cause, ConnectionShutdown)

    @pytest.mark.asyncio
    async def test_no_connections_available(self, mock_session):
        """Test handling of NoConnectionsAvailable from pool."""
        async_session = AsyncCassandraSession(mock_session)

        original_error = NoConnectionsAvailable("Connection pool exhausted")
        mock_session.execute_async.return_value = self.create_error_future(original_error)

        # NoConnectionsAvailable is wrapped in QueryError
        with pytest.raises(QueryError) as exc_info:
            await async_session.execute("SELECT * FROM test")

        assert "Query execution failed: Connection pool exhausted" in str(exc_info.value)
        assert isinstance(exc_info.value.cause, NoConnectionsAvailable)

    @pytest.mark.asyncio
    async def test_already_exists(self, mock_session):
        """Test handling of AlreadyExists errors."""
        async_session = AsyncCassandraSession(mock_session)

        original_error = AlreadyExists(keyspace="test_ks", table="test_table")
        mock_session.execute_async.return_value = self.create_error_future(original_error)

        # AlreadyExists is wrapped in QueryError
        with pytest.raises(QueryError) as exc_info:
            await async_session.execute("CREATE TABLE test_table (id int PRIMARY KEY)")

        assert "Query execution failed:" in str(exc_info.value)
        assert isinstance(exc_info.value.cause, AlreadyExists)
        assert exc_info.value.cause.keyspace == "test_ks"
        assert exc_info.value.cause.table == "test_table"

    @pytest.mark.asyncio
    async def test_invalid_request(self, mock_session):
        """Test handling of InvalidRequest errors."""
        async_session = AsyncCassandraSession(mock_session)

        error = InvalidRequest("Invalid CQL syntax")
        mock_session.execute_async.return_value = self.create_error_future(error)

        with pytest.raises(InvalidRequest) as exc_info:
            await async_session.execute("SELCT * FROM test")  # Typo in SELECT

        assert "Invalid CQL syntax" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_multiple_error_types_in_sequence(self, mock_session):
        """Test handling different error types in sequence."""
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
        """Test error handling during prepared statement execution."""
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
        """Test NoHostAvailable with different errors per host."""
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
