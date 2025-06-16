"""
Unit tests for utils module.
"""

import asyncio
import threading
from unittest.mock import Mock, patch

import pytest

from async_cassandra.utils import get_or_create_event_loop, safe_call_soon_threadsafe


class TestGetOrCreateEventLoop:
    """Test get_or_create_event_loop function."""

    @pytest.mark.asyncio
    async def test_get_existing_loop(self):
        """Test getting existing event loop."""
        # Inside an async function, there's already a loop
        loop = get_or_create_event_loop()
        assert loop is asyncio.get_running_loop()
        assert isinstance(loop, asyncio.AbstractEventLoop)

    def test_create_new_loop_when_none_exists(self):
        """Test creating new loop when none exists."""
        # Run in a thread without event loop
        result = {"loop": None, "created": False}

        def run_in_thread():
            # Ensure no event loop exists
            try:
                asyncio.get_running_loop()
                result["created"] = False
            except RuntimeError:
                # Good, no loop exists
                result["created"] = True

            # Get or create loop
            loop = get_or_create_event_loop()
            result["loop"] = loop

        thread = threading.Thread(target=run_in_thread)
        thread.start()
        thread.join()

        assert result["created"] is True
        assert result["loop"] is not None
        assert isinstance(result["loop"], asyncio.AbstractEventLoop)

    def test_creates_and_sets_event_loop(self):
        """Test that function sets the created loop as current."""
        # Mock to control behavior
        mock_loop = Mock(spec=asyncio.AbstractEventLoop)

        with patch("asyncio.get_running_loop", side_effect=RuntimeError):
            with patch("asyncio.new_event_loop", return_value=mock_loop):
                with patch("asyncio.set_event_loop") as mock_set:
                    loop = get_or_create_event_loop()

                    assert loop is mock_loop
                    mock_set.assert_called_once_with(mock_loop)

    @pytest.mark.asyncio
    async def test_concurrent_calls_return_same_loop(self):
        """Test concurrent calls return the same loop in async context."""
        # In async context, they should all get the current running loop
        current_loop = asyncio.get_running_loop()

        # Get multiple references
        loop1 = get_or_create_event_loop()
        loop2 = get_or_create_event_loop()
        loop3 = get_or_create_event_loop()

        # All should be the same loop
        assert loop1 is current_loop
        assert loop2 is current_loop
        assert loop3 is current_loop


class TestSafeCallSoonThreadsafe:
    """Test safe_call_soon_threadsafe function."""

    def test_with_valid_loop(self):
        """Test calling with valid event loop."""
        mock_loop = Mock(spec=asyncio.AbstractEventLoop)
        callback = Mock()

        safe_call_soon_threadsafe(mock_loop, callback, "arg1", "arg2")

        mock_loop.call_soon_threadsafe.assert_called_once_with(callback, "arg1", "arg2")

    def test_with_none_loop(self):
        """Test calling with None loop."""
        callback = Mock()

        # Should not raise exception
        safe_call_soon_threadsafe(None, callback, "arg1", "arg2")

        # Callback should not be called
        callback.assert_not_called()

    def test_with_closed_loop(self):
        """Test calling with closed event loop."""
        mock_loop = Mock(spec=asyncio.AbstractEventLoop)
        mock_loop.call_soon_threadsafe.side_effect = RuntimeError("Event loop is closed")
        callback = Mock()

        # Should not raise exception
        with patch("async_cassandra.utils.logger") as mock_logger:
            safe_call_soon_threadsafe(mock_loop, callback, "arg1", "arg2")

            # Should log warning
            mock_logger.warning.assert_called_once()
            assert "Failed to schedule callback" in mock_logger.warning.call_args[0][0]

    def test_with_various_callback_types(self):
        """Test with different callback types."""
        mock_loop = Mock(spec=asyncio.AbstractEventLoop)

        # Regular function
        def regular_func(x, y):
            return x + y

        safe_call_soon_threadsafe(mock_loop, regular_func, 1, 2)
        mock_loop.call_soon_threadsafe.assert_called_with(regular_func, 1, 2)

        # Lambda
        def lambda_func(x):
            return x * 2

        safe_call_soon_threadsafe(mock_loop, lambda_func, 5)
        mock_loop.call_soon_threadsafe.assert_called_with(lambda_func, 5)

        # Method
        class TestClass:
            def method(self, x):
                return x

        obj = TestClass()
        safe_call_soon_threadsafe(mock_loop, obj.method, 10)
        mock_loop.call_soon_threadsafe.assert_called_with(obj.method, 10)

    def test_no_args(self):
        """Test calling with no arguments."""
        mock_loop = Mock(spec=asyncio.AbstractEventLoop)
        callback = Mock()

        safe_call_soon_threadsafe(mock_loop, callback)

        mock_loop.call_soon_threadsafe.assert_called_once_with(callback)

    def test_many_args(self):
        """Test calling with many arguments."""
        mock_loop = Mock(spec=asyncio.AbstractEventLoop)
        callback = Mock()

        args = list(range(10))
        safe_call_soon_threadsafe(mock_loop, callback, *args)

        mock_loop.call_soon_threadsafe.assert_called_once_with(callback, *args)

    @pytest.mark.asyncio
    async def test_real_event_loop_integration(self):
        """Test with real event loop."""
        loop = asyncio.get_running_loop()
        result = {"called": False, "args": None}

        def callback(*args):
            result["called"] = True
            result["args"] = args

        # Call from another thread
        def call_from_thread():
            safe_call_soon_threadsafe(loop, callback, "test", 123)

        thread = threading.Thread(target=call_from_thread)
        thread.start()
        thread.join()

        # Give the loop a chance to process the callback
        await asyncio.sleep(0.1)

        assert result["called"] is True
        assert result["args"] == ("test", 123)

    def test_exception_in_callback_scheduling(self):
        """Test handling of exceptions during scheduling."""
        mock_loop = Mock(spec=asyncio.AbstractEventLoop)
        mock_loop.call_soon_threadsafe.side_effect = Exception("Unexpected error")
        callback = Mock()

        # Should handle any exception type gracefully
        with patch("async_cassandra.utils.logger") as mock_logger:
            # This should not raise
            try:
                safe_call_soon_threadsafe(mock_loop, callback)
            except Exception:
                pytest.fail("safe_call_soon_threadsafe should not raise exceptions")

            # Should still log warning for non-RuntimeError
            mock_logger.warning.assert_not_called()  # Only logs for RuntimeError


class TestUtilsModuleAttributes:
    """Test module-level attributes and imports."""

    def test_logger_configured(self):
        """Test that logger is properly configured."""
        import async_cassandra.utils

        assert hasattr(async_cassandra.utils, "logger")
        assert async_cassandra.utils.logger.name == "async_cassandra.utils"

    def test_public_api(self):
        """Test that public API is as expected."""
        import async_cassandra.utils

        # Expected public functions
        expected_functions = {"get_or_create_event_loop", "safe_call_soon_threadsafe"}

        # Get actual public functions
        actual_functions = {
            name
            for name in dir(async_cassandra.utils)
            if not name.startswith("_") and callable(getattr(async_cassandra.utils, name))
        }

        # Remove imports that aren't our functions
        actual_functions.discard("asyncio")
        actual_functions.discard("logging")
        actual_functions.discard("Any")
        actual_functions.discard("Optional")

        assert actual_functions == expected_functions

    def test_type_annotations(self):
        """Test that functions have proper type annotations."""
        import inspect

        from async_cassandra.utils import get_or_create_event_loop, safe_call_soon_threadsafe

        # Check get_or_create_event_loop
        sig = inspect.signature(get_or_create_event_loop)
        assert sig.return_annotation == asyncio.AbstractEventLoop

        # Check safe_call_soon_threadsafe
        sig = inspect.signature(safe_call_soon_threadsafe)
        params = sig.parameters
        assert "loop" in params
        assert "callback" in params
        assert "args" in params
