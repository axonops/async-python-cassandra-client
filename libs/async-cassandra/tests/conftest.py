"""
Pytest configuration and shared fixtures for all tests.
"""

import asyncio
from unittest.mock import patch

import pytest


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(autouse=True)
def fast_shutdown_for_unit_tests(request):
    """Mock the 5-second sleep in cluster shutdown for unit tests only."""
    # Skip for tests that need real timing
    skip_tests = [
        "test_simplified_threading",
        "test_timeout_implementation",
        "test_protocol_version_bdd",
    ]

    # Check if this test should be skipped
    should_skip = any(skip_test in request.node.nodeid for skip_test in skip_tests)

    # Only apply to unit tests and BDD tests, not integration tests
    if not should_skip and (
        "unit" in request.node.nodeid
        or "_core" in request.node.nodeid
        or "_features" in request.node.nodeid
        or "_resilience" in request.node.nodeid
        or "bdd" in request.node.nodeid
    ):
        # Store the original sleep function
        original_sleep = asyncio.sleep

        async def mock_sleep(seconds):
            # For the 5-second shutdown sleep, make it instant
            if seconds == 5.0:
                return
            # For other sleeps, use a much shorter delay but use the original function
            await original_sleep(min(seconds, 0.01))

        with patch("asyncio.sleep", side_effect=mock_sleep):
            yield
    else:
        # For integration tests or skipped tests, don't mock
        yield
