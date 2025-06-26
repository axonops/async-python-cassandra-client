"""
Unit tests for protocol version validation.

These tests ensure protocol version validation happens immediately at
configuration time without requiring a real Cassandra connection.
"""

import pytest

from async_cassandra import AsyncCluster
from async_cassandra.exceptions import ConfigurationError


class TestProtocolVersionValidation:
    """Test protocol version validation at configuration time."""

    def test_protocol_v1_rejected(self):
        """Protocol version 1 should be rejected immediately."""
        with pytest.raises(ConfigurationError) as exc_info:
            AsyncCluster(contact_points=["localhost"], protocol_version=1)

        assert "Protocol version 1 is not supported" in str(exc_info.value)

    def test_protocol_v2_rejected(self):
        """Protocol version 2 should be rejected immediately."""
        with pytest.raises(ConfigurationError) as exc_info:
            AsyncCluster(contact_points=["localhost"], protocol_version=2)

        assert "Protocol version 2 is not supported" in str(exc_info.value)

    def test_protocol_v3_rejected(self):
        """Protocol version 3 should be rejected immediately."""
        with pytest.raises(ConfigurationError) as exc_info:
            AsyncCluster(contact_points=["localhost"], protocol_version=3)

        assert "Protocol version 3 is not supported" in str(exc_info.value)

    def test_protocol_v4_rejected_with_guidance(self):
        """Protocol version 4 should be rejected with cloud provider guidance."""
        with pytest.raises(ConfigurationError) as exc_info:
            AsyncCluster(contact_points=["localhost"], protocol_version=4)

        error_msg = str(exc_info.value)
        assert "Protocol version 4 is not supported" in error_msg
        assert "cloud provider" in error_msg
        assert "check their documentation" in error_msg

    def test_protocol_v5_accepted(self):
        """Protocol version 5 should be accepted."""
        # Should not raise an exception
        cluster = AsyncCluster(contact_points=["localhost"], protocol_version=5)
        assert cluster is not None

    def test_protocol_v6_accepted(self):
        """Protocol version 6 should be accepted (even if beta)."""
        # Should not raise an exception at configuration time
        cluster = AsyncCluster(contact_points=["localhost"], protocol_version=6)
        assert cluster is not None

    def test_future_protocol_accepted(self):
        """Future protocol versions should be accepted for forward compatibility."""
        # Should not raise an exception
        cluster = AsyncCluster(contact_points=["localhost"], protocol_version=7)
        assert cluster is not None

    def test_no_protocol_version_accepted(self):
        """No protocol version specified should be accepted (auto-negotiation)."""
        # Should not raise an exception
        cluster = AsyncCluster(contact_points=["localhost"])
        assert cluster is not None

    def test_auth_with_legacy_protocol_rejected(self):
        """Authentication with legacy protocol should fail immediately."""
        with pytest.raises(ConfigurationError) as exc_info:
            AsyncCluster.create_with_auth(
                contact_points=["localhost"], username="user", password="pass", protocol_version=3
            )

        assert "Protocol version 3 is not supported" in str(exc_info.value)

    def test_migration_guidance_for_v4(self):
        """Protocol v4 error should include migration guidance."""
        with pytest.raises(ConfigurationError) as exc_info:
            AsyncCluster(contact_points=["localhost"], protocol_version=4)

        error_msg = str(exc_info.value)
        assert "async-cassandra requires CQL protocol v5" in error_msg
        assert "Cassandra 4.0 (released July 2021)" in error_msg

    def test_error_message_includes_upgrade_path(self):
        """Legacy protocol errors should include upgrade path."""
        with pytest.raises(ConfigurationError) as exc_info:
            AsyncCluster(contact_points=["localhost"], protocol_version=3)

        error_msg = str(exc_info.value)
        assert "upgrade" in error_msg.lower()
        assert "4.0+" in error_msg
