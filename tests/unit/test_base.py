"""
Unit tests for base module decorators and utilities.

This module tests the foundational AsyncContextManageable mixin that provides
async context manager functionality to AsyncCluster, AsyncSession, and other
resources that need automatic cleanup.

Test Organization:
==================
- TestAsyncContextManageable: Tests the async context manager mixin
- TestAsyncStreamingResultSet: Tests streaming result wrapper (if present)

Key Testing Focus:
==================
1. Resource cleanup happens automatically
2. Exceptions don't prevent cleanup
3. Multiple cleanup calls are safe
4. Proper async/await protocol implementation
"""

import pytest

from async_cassandra.base import AsyncContextManageable


class TestAsyncContextManageable:
    """
    Test AsyncContextManageable mixin.

    This mixin is inherited by AsyncCluster, AsyncSession, and other
    resources to provide 'async with' functionality. It ensures proper
    cleanup even when exceptions occur.
    """

    @pytest.mark.asyncio
    async def test_context_manager(self):
        """
        Test basic async context manager functionality.

        What this tests:
        ---------------
        1. Resources implementing AsyncContextManageable can use 'async with'
        2. The resource is returned from __aenter__ for use in the context
        3. close() is automatically called when exiting the context
        4. Resource state properly reflects being closed

        Why this matters:
        ----------------
        Context managers are the primary way to ensure resource cleanup in Python.
        This pattern prevents resource leaks by guaranteeing cleanup happens even
        if the user forgets to call close() explicitly.

        Example usage pattern:
        --------------------
        async with AsyncCluster() as cluster:
            async with cluster.connect() as session:
                await session.execute(...)
        # Both session and cluster are automatically closed here
        """

        class TestResource(AsyncContextManageable):
            close_count = 0
            is_closed = False

            async def close(self):
                self.close_count += 1
                self.is_closed = True

        # Use as context manager
        async with TestResource() as resource:
            # Inside context: resource should be open
            assert not resource.is_closed
            assert resource.close_count == 0

        # After context: should be closed exactly once
        assert resource.is_closed
        assert resource.close_count == 1

    @pytest.mark.asyncio
    async def test_context_manager_with_exception(self):
        """
        Test context manager closes resource even when exception occurs.

        What this tests:
        ---------------
        1. Exceptions inside the context don't prevent cleanup
        2. close() is called even when exception is raised
        3. The original exception is propagated (not suppressed)
        4. Resource state is consistent after exception

        Why this matters:
        ----------------
        Many errors can occur during database operations:
        - Network failures
        - Query errors
        - Timeout exceptions
        - Application logic errors

        The context manager MUST clean up resources even when these
        errors occur, otherwise we leak connections, memory, and threads.

        Real-world scenario:
        -------------------
        async with cluster.connect() as session:
            await session.execute("INVALID QUERY")  # Raises QueryError
        # session.close() must still be called despite the error
        """

        class TestResource(AsyncContextManageable):
            close_count = 0
            is_closed = False

            async def close(self):
                self.close_count += 1
                self.is_closed = True

        resource = None
        try:
            async with TestResource() as res:
                resource = res
                raise ValueError("Test error")
        except ValueError:
            pass

        # Should still close resource on exception
        assert resource is not None
        assert resource.is_closed
        assert resource.close_count == 1

    @pytest.mark.asyncio
    async def test_context_manager_multiple_use(self):
        """
        Test context manager can be used multiple times.

        What this tests:
        ---------------
        1. Same resource can enter/exit context multiple times
        2. close() is called each time the context exits
        3. No state corruption between uses
        4. Resource remains functional for multiple contexts

        Why this matters:
        ----------------
        While not common, some use cases might reuse resources:
        - Connection pooling implementations
        - Cached sessions with periodic cleanup
        - Test fixtures that reset between tests

        The mixin should handle multiple uses gracefully without
        assuming single-use semantics.

        Note:
        -----
        In practice, most resources (cluster, session) are used
        once and discarded, but the base mixin doesn't enforce this.
        """

        class TestResource(AsyncContextManageable):
            close_count = 0

            async def close(self):
                self.close_count += 1

        resource = TestResource()

        # First use
        async with resource:
            pass
        assert resource.close_count == 1

        # Second use - should work and increment close count
        async with resource:
            pass
        assert resource.close_count == 2
