"""
Helper utilities for unit tests.
"""


class MockToken:
    """Mock token that supports comparison for sorting."""

    def __init__(self, value):
        self.value = value

    def __lt__(self, other):
        return self.value < other.value

    def __eq__(self, other):
        return self.value == other.value

    def __repr__(self):
        return f"MockToken({self.value})"
