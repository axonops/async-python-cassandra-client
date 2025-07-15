"""
Unit tests for consistency level management.

What this tests:
---------------
1. Consistency level parsing
2. Execution profile creation
3. Error handling
4. Default behavior

Why this matters:
----------------
- Consistency levels affect performance and reliability
- Must validate user input
- Clear error messages needed
"""

import pytest
from cassandra import ConsistencyLevel
from cassandra.cluster import ExecutionProfile

from async_cassandra_dataframe.consistency import create_execution_profile, parse_consistency_level


class TestConsistencyLevel:
    """Test consistency level functionality."""

    def test_parse_consistency_level_valid_names(self):
        """Test parsing valid consistency level names."""
        # Test valid names
        assert parse_consistency_level("ONE") == ConsistencyLevel.ONE
        assert parse_consistency_level("QUORUM") == ConsistencyLevel.QUORUM
        assert parse_consistency_level("ALL") == ConsistencyLevel.ALL
        assert parse_consistency_level("LOCAL_QUORUM") == ConsistencyLevel.LOCAL_QUORUM
        assert parse_consistency_level("LOCAL_ONE") == ConsistencyLevel.LOCAL_ONE

        # Case insensitive
        assert parse_consistency_level("one") == ConsistencyLevel.ONE
        assert parse_consistency_level("Quorum") == ConsistencyLevel.QUORUM

    def test_parse_consistency_level_with_dash(self):
        """Test parsing consistency levels with dashes."""
        # Should handle both dash and underscore
        assert parse_consistency_level("LOCAL-QUORUM") == ConsistencyLevel.LOCAL_QUORUM
        assert parse_consistency_level("local-one") == ConsistencyLevel.LOCAL_ONE

    def test_parse_consistency_level_none_default(self):
        """Test None returns LOCAL_ONE as default."""
        assert parse_consistency_level(None) == ConsistencyLevel.LOCAL_ONE

    def test_parse_consistency_level_invalid(self):
        """Test invalid consistency levels raise ValueError."""
        # Invalid string
        with pytest.raises(ValueError) as exc_info:
            parse_consistency_level("INVALID")
        assert "invalid consistency level" in str(exc_info.value).lower()
        assert "valid options" in str(exc_info.value).lower()

    def test_all_common_consistency_levels(self):
        """Test that all common consistency levels are supported."""
        common_levels = [
            ("ONE", ConsistencyLevel.ONE),
            ("TWO", ConsistencyLevel.TWO),
            ("THREE", ConsistencyLevel.THREE),
            ("QUORUM", ConsistencyLevel.QUORUM),
            ("ALL", ConsistencyLevel.ALL),
            ("LOCAL_QUORUM", ConsistencyLevel.LOCAL_QUORUM),
            ("EACH_QUORUM", ConsistencyLevel.EACH_QUORUM),
            ("SERIAL", ConsistencyLevel.SERIAL),
            ("LOCAL_SERIAL", ConsistencyLevel.LOCAL_SERIAL),
            ("LOCAL_ONE", ConsistencyLevel.LOCAL_ONE),
            ("ANY", ConsistencyLevel.ANY),
        ]

        for level_str, expected in common_levels:
            assert parse_consistency_level(level_str) == expected

    def test_create_execution_profile(self):
        """Test creating execution profile with consistency level."""
        # Create profile with ONE
        profile = create_execution_profile(ConsistencyLevel.ONE)
        assert isinstance(profile, ExecutionProfile)
        assert profile.consistency_level == ConsistencyLevel.ONE

        # Create profile with QUORUM
        profile = create_execution_profile(ConsistencyLevel.QUORUM)
        assert profile.consistency_level == ConsistencyLevel.QUORUM

    def test_execution_profile_independence(self):
        """Test that each profile is independent."""
        profile1 = create_execution_profile(ConsistencyLevel.ONE)
        profile2 = create_execution_profile(ConsistencyLevel.QUORUM)

        # Should be different instances
        assert profile1 is not profile2
        assert profile1.consistency_level != profile2.consistency_level
