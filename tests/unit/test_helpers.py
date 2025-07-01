"""
Test helpers for advanced features tests.

This module provides utility functions for creating mock objects that simulate
Cassandra driver behavior in unit tests. These helpers ensure consistent test
behavior and reduce boilerplate across test files.
"""

import asyncio
from unittest.mock import Mock


def create_mock_response_future(rows=None, has_more_pages=False):
    """
    Helper to create a properly configured mock ResponseFuture.

    What this does:
    --------------
    1. Creates mock ResponseFuture
    2. Configures callback behavior
    3. Simulates async execution
    4. Handles event loop scheduling

    Why this matters:
    ----------------
    Consistent mock behavior:
    - Accurate driver simulation
    - Reliable test results
    - Less test flakiness

    Proper async simulation prevents
    race conditions in tests.

    Parameters:
    -----------
    rows : list, optional
        The rows to return when callback is executed
    has_more_pages : bool, default False
        Whether to indicate more pages are available

    Returns:
    --------
    Mock
        A configured mock ResponseFuture object
    """
    mock_future = Mock()
    mock_future.has_more_pages = has_more_pages
    mock_future.timeout = None
    mock_future.add_callbacks = Mock()

    def handle_callbacks(callback=None, errback=None):
        if callback:
            # Schedule callback on the event loop to simulate async behavior
            loop = asyncio.get_event_loop()
            loop.call_soon(callback, rows if rows is not None else [])

    mock_future.add_callbacks.side_effect = handle_callbacks
    return mock_future
