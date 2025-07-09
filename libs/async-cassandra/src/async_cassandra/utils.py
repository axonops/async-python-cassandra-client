"""
Utility functions and helpers for async-cassandra.
"""

import asyncio
import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)


def get_or_create_event_loop() -> asyncio.AbstractEventLoop:
    """
    Get the current event loop or create a new one if necessary.

    Returns:
        The current or newly created event loop.
    """
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        # No event loop running, create a new one
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop


def safe_call_soon_threadsafe(
    loop: Optional[asyncio.AbstractEventLoop], callback: Any, *args: Any
) -> None:
    """
    Safely schedule a callback in the event loop from another thread.

    Args:
        loop: The event loop to schedule in (may be None).
        callback: The callback function to schedule.
        *args: Arguments to pass to the callback.
    """
    if loop is not None:
        try:
            loop.call_soon_threadsafe(callback, *args)
        except RuntimeError as e:
            # Event loop might be closed
            logger.warning(f"Failed to schedule callback: {e}")
        except Exception:
            # Ignore other exceptions - we don't want to crash the caller
            pass
