"""
Simplified async session management for Cassandra connections.

This implementation focuses on being a thin wrapper around the driver,
avoiding complex locking and state management.
"""

import asyncio
import logging
import time
from typing import Any, Dict, Optional

from cassandra.cluster import _NOT_SET, EXEC_PROFILE_DEFAULT, Cluster, Session
from cassandra.query import BatchStatement, PreparedStatement, SimpleStatement

from .base import AsyncContextManageable
from .exceptions import ConnectionError, QueryError
from .metrics import MetricsMiddleware
from .result import AsyncResultHandler, AsyncResultSet
from .streaming import AsyncStreamingResultSet, StreamingResultHandler

logger = logging.getLogger(__name__)


class AsyncCassandraSession(AsyncContextManageable):
    """
    Simplified async wrapper for Cassandra Session.

    This implementation:
    - Uses a single lock only for close operations
    - Accepts that operations might fail if close() is called concurrently
    - Focuses on being a thin wrapper without complex state management
    """

    def __init__(self, session: Session, metrics: Optional[MetricsMiddleware] = None):
        """
        Initialize async session wrapper.

        Args:
            session: The underlying Cassandra session.
            metrics: Optional metrics middleware for observability.
        """
        self._session = session
        self._metrics = metrics
        self._closed = False
        self._close_lock = asyncio.Lock()

    def _record_metrics_async(
        self,
        query_str: str,
        duration: float,
        success: bool,
        error_type: Optional[str],
        parameters_count: int,
        result_size: int,
    ) -> None:
        """
        Record metrics in a fire-and-forget manner.

        This method creates a background task to record metrics without blocking
        the main execution flow or preventing exception propagation.
        """
        if not self._metrics:
            return

        async def _record() -> None:
            try:
                assert self._metrics is not None  # Type guard for mypy
                await self._metrics.record_query_metrics(
                    query=query_str,
                    duration=duration,
                    success=success,
                    error_type=error_type,
                    parameters_count=parameters_count,
                    result_size=result_size,
                )
            except Exception as e:
                # Log error but don't propagate - metrics should not break queries
                logger.warning(f"Failed to record metrics: {e}")

        # Create task without awaiting it
        try:
            asyncio.create_task(_record())
        except RuntimeError:
            # No event loop running, skip metrics
            pass

    @classmethod
    async def create(
        cls, cluster: Cluster, keyspace: Optional[str] = None
    ) -> "AsyncCassandraSession":
        """
        Create a new async session.

        Args:
            cluster: The Cassandra cluster to connect to.
            keyspace: Optional keyspace to use.

        Returns:
            New AsyncCassandraSession instance.
        """
        loop = asyncio.get_event_loop()

        # Connect in executor to avoid blocking
        session = await loop.run_in_executor(
            None, lambda: cluster.connect(keyspace) if keyspace else cluster.connect()
        )

        return cls(session)

    async def execute(
        self,
        query: Any,
        parameters: Any = None,
        trace: bool = False,
        custom_payload: Any = None,
        timeout: Any = None,
        execution_profile: Any = EXEC_PROFILE_DEFAULT,
        paging_state: Any = None,
        host: Any = None,
        execute_as: Any = None,
    ) -> AsyncResultSet:
        """
        Execute a CQL query asynchronously.

        Args:
            query: The query to execute.
            parameters: Query parameters.
            trace: Whether to enable query tracing.
            custom_payload: Custom payload to send with the request.
            timeout: Query timeout in seconds or _NOT_SET.
            execution_profile: Execution profile name or object to use.
            paging_state: Paging state for resuming paged queries.
            host: Specific host to execute query on.
            execute_as: User to execute the query as.

        Returns:
            AsyncResultSet containing query results.

        Raises:
            QueryError: If query execution fails.
            ConnectionError: If session is closed.
        """
        # Simple closed check - no lock needed for read
        if self._closed:
            raise ConnectionError("Session is closed")

        # Start metrics timing
        start_time = time.perf_counter()
        success = False
        error_type = None
        result_size = 0

        try:
            # Fix timeout handling - use _NOT_SET if timeout is None
            response_future = self._session.execute_async(
                query,
                parameters,
                trace,
                custom_payload,
                timeout if timeout is not None else _NOT_SET,
                execution_profile,
                paging_state,
                host,
                execute_as,
            )

            handler = AsyncResultHandler(response_future)
            # Pass timeout to get_result if specified
            query_timeout = timeout if timeout is not None and timeout != _NOT_SET else None
            result = await handler.get_result(timeout=query_timeout)

            success = True
            result_size = len(result.rows) if hasattr(result, "rows") else 0
            return result

        except Exception as e:
            error_type = type(e).__name__
            # Check if this is a Cassandra driver exception by looking at its module
            if (
                hasattr(e, "__module__")
                and (e.__module__ == "cassandra" or e.__module__.startswith("cassandra."))
                or isinstance(e, asyncio.TimeoutError)
            ):
                # Pass through all Cassandra driver exceptions and asyncio.TimeoutError
                raise
            else:
                # Only wrap unexpected exceptions
                raise QueryError(f"Query execution failed: {str(e)}", cause=e) from e
        finally:
            # Record metrics in a fire-and-forget manner
            duration = time.perf_counter() - start_time
            query_str = (
                str(query) if isinstance(query, (SimpleStatement, PreparedStatement)) else query
            )
            params_count = len(parameters) if parameters else 0

            self._record_metrics_async(
                query_str=query_str,
                duration=duration,
                success=success,
                error_type=error_type,
                parameters_count=params_count,
                result_size=result_size,
            )

    async def execute_stream(
        self,
        query: Any,
        parameters: Any = None,
        stream_config: Any = None,
        trace: bool = False,
        custom_payload: Any = None,
        timeout: Any = None,
        execution_profile: Any = EXEC_PROFILE_DEFAULT,
        paging_state: Any = None,
        host: Any = None,
        execute_as: Any = None,
    ) -> AsyncStreamingResultSet:
        """
        Execute a CQL query with streaming support for large result sets.

        This method is memory-efficient for queries that return many rows,
        as it fetches results page by page instead of loading everything
        into memory at once.

        Args:
            query: The query to execute.
            parameters: Query parameters.
            stream_config: Configuration for streaming (fetch size, callbacks, etc.)
            trace: Whether to enable query tracing.
            custom_payload: Custom payload to send with the request.
            timeout: Query timeout in seconds or _NOT_SET.
            execution_profile: Execution profile name or object to use.
            paging_state: Paging state for resuming paged queries.
            host: Specific host to execute query on.
            execute_as: User to execute the query as.

        Returns:
            AsyncStreamingResultSet for memory-efficient iteration.

        Raises:
            QueryError: If query execution fails.
            ConnectionError: If session is closed.
        """
        # Simple closed check - no lock needed for read
        if self._closed:
            raise ConnectionError("Session is closed")

        # Start metrics timing for consistency with execute()
        start_time = time.perf_counter()
        success = False
        error_type = None

        try:
            # Apply fetch_size from stream_config if provided
            query_to_execute = query
            if stream_config and hasattr(stream_config, "fetch_size"):
                # If query is a string, create a SimpleStatement with fetch_size
                if isinstance(query_to_execute, str):
                    from cassandra.query import SimpleStatement

                    query_to_execute = SimpleStatement(
                        query_to_execute, fetch_size=stream_config.fetch_size
                    )
                # If it's already a statement, try to set fetch_size
                elif hasattr(query_to_execute, "fetch_size"):
                    query_to_execute.fetch_size = stream_config.fetch_size

            response_future = self._session.execute_async(
                query_to_execute,
                parameters,
                trace,
                custom_payload,
                timeout if timeout is not None else _NOT_SET,
                execution_profile,
                paging_state,
                host,
                execute_as,
            )

            handler = StreamingResultHandler(response_future, stream_config)
            result = await handler.get_streaming_result()
            success = True
            return result

        except Exception as e:
            error_type = type(e).__name__
            # Check if this is a Cassandra driver exception by looking at its module
            if (
                hasattr(e, "__module__")
                and (e.__module__ == "cassandra" or e.__module__.startswith("cassandra."))
                or isinstance(e, asyncio.TimeoutError)
            ):
                # Pass through all Cassandra driver exceptions and asyncio.TimeoutError
                raise
            else:
                # Only wrap unexpected exceptions
                raise QueryError(f"Streaming query execution failed: {str(e)}", cause=e) from e
        finally:
            # Record metrics in a fire-and-forget manner
            duration = time.perf_counter() - start_time
            # Import here to avoid circular imports
            from cassandra.query import PreparedStatement, SimpleStatement

            query_str = (
                str(query) if isinstance(query, (SimpleStatement, PreparedStatement)) else query
            )
            params_count = len(parameters) if parameters else 0

            self._record_metrics_async(
                query_str=query_str,
                duration=duration,
                success=success,
                error_type=error_type,
                parameters_count=params_count,
                result_size=0,  # Streaming doesn't know size upfront
            )

    async def execute_batch(
        self,
        batch_statement: BatchStatement,
        trace: bool = False,
        custom_payload: Optional[Dict[str, bytes]] = None,
        timeout: Any = None,
        execution_profile: Any = EXEC_PROFILE_DEFAULT,
    ) -> AsyncResultSet:
        """
        Execute a batch statement asynchronously.

        Args:
            batch_statement: The batch statement to execute.
            trace: Whether to enable query tracing.
            custom_payload: Custom payload to send with the request.
            timeout: Query timeout in seconds.
            execution_profile: Execution profile to use.

        Returns:
            AsyncResultSet (usually empty for batch operations).

        Raises:
            QueryError: If batch execution fails.
            ConnectionError: If session is closed.
        """
        return await self.execute(
            batch_statement,
            trace=trace,
            custom_payload=custom_payload,
            timeout=timeout if timeout is not None else _NOT_SET,
            execution_profile=execution_profile,
        )

    async def prepare(
        self, query: str, custom_payload: Any = None, timeout: Optional[float] = None
    ) -> PreparedStatement:
        """
        Prepare a CQL statement asynchronously.

        Args:
            query: The query to prepare.
            custom_payload: Custom payload to send with the request.
            timeout: Timeout in seconds. Defaults to DEFAULT_REQUEST_TIMEOUT.

        Returns:
            PreparedStatement that can be executed multiple times.

        Raises:
            QueryError: If statement preparation fails.
            asyncio.TimeoutError: If preparation times out.
            ConnectionError: If session is closed.
        """
        # Simple closed check - no lock needed for read
        if self._closed:
            raise ConnectionError("Session is closed")

        # Import here to avoid circular import
        from .constants import DEFAULT_REQUEST_TIMEOUT

        if timeout is None:
            timeout = DEFAULT_REQUEST_TIMEOUT

        try:
            loop = asyncio.get_event_loop()

            # Prepare in executor to avoid blocking with timeout
            prepared = await asyncio.wait_for(
                loop.run_in_executor(None, lambda: self._session.prepare(query, custom_payload)),
                timeout=timeout,
            )

            return prepared
        except Exception as e:
            # Check if this is a Cassandra driver exception by looking at its module
            if (
                hasattr(e, "__module__")
                and (e.__module__ == "cassandra" or e.__module__.startswith("cassandra."))
                or isinstance(e, asyncio.TimeoutError)
            ):
                # Pass through all Cassandra driver exceptions and asyncio.TimeoutError
                raise
            else:
                # Only wrap unexpected exceptions
                raise QueryError(f"Statement preparation failed: {str(e)}", cause=e) from e

    async def close(self) -> None:
        """
        Close the session and release resources.

        This method is idempotent and can be called multiple times safely.
        Uses a single lock to ensure shutdown is called only once.
        """
        async with self._close_lock:
            if not self._closed:
                self._closed = True
                loop = asyncio.get_event_loop()
                # Use a reasonable timeout for shutdown operations
                await asyncio.wait_for(
                    loop.run_in_executor(None, self._session.shutdown), timeout=30.0
                )
                # Give the driver's internal threads time to finish
                # This helps prevent "cannot schedule new futures after shutdown" errors
                await asyncio.sleep(5.0)

    @property
    def is_closed(self) -> bool:
        """Check if the session is closed."""
        return self._closed

    @property
    def keyspace(self) -> Optional[str]:
        """Get current keyspace."""
        keyspace = self._session.keyspace
        return keyspace if isinstance(keyspace, str) else None

    async def set_keyspace(self, keyspace: str) -> None:
        """
        Set the current keyspace.

        Args:
            keyspace: The keyspace to use.

        Raises:
            QueryError: If setting keyspace fails.
            ValueError: If keyspace name is invalid.
            ConnectionError: If session is closed.
        """
        # Validate keyspace name to prevent injection attacks
        if not keyspace or not all(c.isalnum() or c == "_" for c in keyspace):
            raise ValueError(
                f"Invalid keyspace name: '{keyspace}'. "
                "Keyspace names must contain only alphanumeric characters and underscores."
            )

        await self.execute(f"USE {keyspace}")
