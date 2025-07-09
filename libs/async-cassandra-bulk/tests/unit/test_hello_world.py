"""
Test hello world functionality for Phase 1 package setup.

What this tests:
---------------
1. Package can be imported
2. hello() function works

Why this matters:
----------------
- Verifies package structure is correct
- Confirms package can be distributed via PyPI
"""

import pytest


class TestHelloWorld:
    """Test basic package functionality."""

    def test_package_imports(self):
        """
        Test that the package can be imported.

        What this tests:
        ---------------
        1. Package import doesn't raise exceptions
        2. __version__ attribute exists
        3. hello function is exported

        Why this matters:
        ----------------
        - Users must be able to import the package
        - Version info is required for PyPI
        - Validates pyproject.toml configuration
        """
        import async_cassandra_bulk

        assert hasattr(async_cassandra_bulk, "__version__")
        assert hasattr(async_cassandra_bulk, "hello")

    @pytest.mark.asyncio
    async def test_hello_function(self):
        """
        Test the hello function returns expected message.

        What this tests:
        ---------------
        1. hello() function exists
        2. Function is async
        3. Returns correct message

        Why this matters:
        ----------------
        - Validates basic async functionality
        - Tests package is properly configured
        - Simple smoke test for deployment
        """
        from async_cassandra_bulk import hello

        result = await hello()
        assert result == "Hello from async-cassandra-bulk!"
