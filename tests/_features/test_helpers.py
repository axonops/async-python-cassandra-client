"""Test helpers for advanced features tests."""

import asyncio
from unittest.mock import Mock


def create_mock_response_future(rows=None, has_more_pages=False):
    """Helper to create a properly configured mock ResponseFuture."""
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
