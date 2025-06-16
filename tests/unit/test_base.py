"""
Unit tests for base module decorators and utilities.
"""

import pytest

from async_cassandra.base import AsyncContextManageable


class TestAsyncContextManageable:
    """Test AsyncContextManageable mixin."""

    @pytest.mark.asyncio
    async def test_context_manager(self):
        """Test async context manager functionality."""

        class TestResource(AsyncContextManageable):
            close_count = 0
            is_closed = False

            async def close(self):
                self.close_count += 1
                self.is_closed = True

        # Use as context manager
        async with TestResource() as resource:
            assert not resource.is_closed
            assert resource.close_count == 0

        # Should be closed after exiting context
        assert resource.is_closed
        assert resource.close_count == 1

    @pytest.mark.asyncio
    async def test_context_manager_with_exception(self):
        """Test context manager closes resource on exception."""

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
        """Test context manager can be used multiple times."""

        class TestResource(AsyncContextManageable):
            close_count = 0

            async def close(self):
                self.close_count += 1

        resource = TestResource()

        # First use
        async with resource:
            pass
        assert resource.close_count == 1

        # Second use
        async with resource:
            pass
        assert resource.close_count == 2
