"""
Unit tests for protocol version validation.

These tests ensure protocol version validation happens immediately at
configuration time without requiring a real Cassandra connection.

Test Organization:
==================
1. Legacy Protocol Rejection - v1, v2, v3 not supported
2. Protocol v4 - Rejected with cloud provider guidance
3. Modern Protocols - v5, v6+ accepted
4. Auto-negotiation - No version specified allowed
5. Error Messages - Clear guidance for upgrades

Key Testing Principles:
======================
- Fail fast at configuration time
- Provide clear upgrade guidance
- Support future protocol versions
- Help users migrate from legacy versions
"""

import pytest

from async_cassandra import AsyncCluster
from async_cassandra.exceptions import ConfigurationError


class TestProtocolVersionValidation:
    """Test protocol version validation at configuration time."""

    def test_protocol_v1_rejected(self):
        """
        Protocol version 1 should be rejected immediately.

        What this tests:
        ---------------
        1. Protocol v1 raises ConfigurationError
        2. Error happens at configuration time
        3. No connection attempt made
        4. Clear error message

        Why this matters:
        ----------------
        Protocol v1 is ancient (Cassandra 1.2):
        - Lacks modern features
        - Security vulnerabilities
        - No async support

        Failing fast prevents confusing
        runtime errors later.
        """
        with pytest.raises(ConfigurationError) as exc_info:
            AsyncCluster(contact_points=["localhost"], protocol_version=1)

        assert "Protocol version 1 is not supported" in str(exc_info.value)

    def test_protocol_v2_rejected(self):
        """
        Protocol version 2 should be rejected immediately.

        What this tests:
        ---------------
        1. Protocol v2 raises ConfigurationError
        2. Consistent with v1 rejection
        3. Clear not supported message
        4. No connection attempted

        Why this matters:
        ----------------
        Protocol v2 (Cassandra 2.0) lacks:
        - Necessary async features
        - Modern authentication
        - Performance optimizations

        async-cassandra needs v5+ features.
        """
        with pytest.raises(ConfigurationError) as exc_info:
            AsyncCluster(contact_points=["localhost"], protocol_version=2)

        assert "Protocol version 2 is not supported" in str(exc_info.value)

    def test_protocol_v3_rejected(self):
        """
        Protocol version 3 should be rejected immediately.

        What this tests:
        ---------------
        1. Protocol v3 raises ConfigurationError
        2. Even though v3 is common
        3. Clear rejection message
        4. Fail at configuration

        Why this matters:
        ----------------
        Protocol v3 (Cassandra 2.1) is common but:
        - Missing required async features
        - No continuous paging
        - Limited result metadata

        Many users on v3 need clear
        upgrade guidance.
        """
        with pytest.raises(ConfigurationError) as exc_info:
            AsyncCluster(contact_points=["localhost"], protocol_version=3)

        assert "Protocol version 3 is not supported" in str(exc_info.value)

    def test_protocol_v4_rejected_with_guidance(self):
        """
        Protocol version 4 should be rejected with cloud provider guidance.

        What this tests:
        ---------------
        1. Protocol v4 rejected despite being modern
        2. Special cloud provider guidance
        3. Helps managed service users
        4. Clear next steps

        Why this matters:
        ----------------
        Protocol v4 (Cassandra 3.0) is tricky:
        - Some cloud providers stuck on v4
        - Users need provider-specific help
        - v5 adds critical async features

        Guidance helps users navigate
        cloud provider limitations.
        """
        with pytest.raises(ConfigurationError) as exc_info:
            AsyncCluster(contact_points=["localhost"], protocol_version=4)

        error_msg = str(exc_info.value)
        assert "Protocol version 4 is not supported" in error_msg
        assert "cloud provider" in error_msg
        assert "check their documentation" in error_msg

    def test_protocol_v5_accepted(self):
        """
        Protocol version 5 should be accepted.

        What this tests:
        ---------------
        1. Protocol v5 configuration succeeds
        2. Minimum supported version
        3. No errors at config time
        4. Cluster object created

        Why this matters:
        ----------------
        Protocol v5 (Cassandra 4.0) provides:
        - Required async features
        - Better streaming
        - Improved performance

        This is the minimum version
        for async-cassandra.
        """
        # Should not raise an exception
        cluster = AsyncCluster(contact_points=["localhost"], protocol_version=5)
        assert cluster is not None

    def test_protocol_v6_accepted(self):
        """
        Protocol version 6 should be accepted (even if beta).

        What this tests:
        ---------------
        1. Protocol v6 configuration allowed
        2. Beta protocols accepted
        3. Forward compatibility
        4. No artificial limits

        Why this matters:
        ----------------
        Protocol v6 (Cassandra 5.0) adds:
        - Vector search features
        - Improved metadata
        - Performance enhancements

        Users testing new features
        shouldn't be blocked.
        """
        # Should not raise an exception at configuration time
        cluster = AsyncCluster(contact_points=["localhost"], protocol_version=6)
        assert cluster is not None

    def test_future_protocol_accepted(self):
        """
        Future protocol versions should be accepted for forward compatibility.

        What this tests:
        ---------------
        1. Unknown versions accepted
        2. Forward compatibility maintained
        3. No hardcoded upper limit
        4. Future-proof design

        Why this matters:
        ----------------
        Future protocols will add features:
        - Don't block early adopters
        - Allow testing new versions
        - Avoid forced upgrades

        The driver should work with
        future Cassandra versions.
        """
        # Should not raise an exception
        cluster = AsyncCluster(contact_points=["localhost"], protocol_version=7)
        assert cluster is not None

    def test_no_protocol_version_accepted(self):
        """
        No protocol version specified should be accepted (auto-negotiation).

        What this tests:
        ---------------
        1. Protocol version optional
        2. Auto-negotiation supported
        3. Driver picks best version
        4. Simplifies configuration

        Why this matters:
        ----------------
        Auto-negotiation benefits:
        - Works across versions
        - Picks optimal protocol
        - Reduces configuration errors

        Most users should use
        auto-negotiation.
        """
        # Should not raise an exception
        cluster = AsyncCluster(contact_points=["localhost"])
        assert cluster is not None

    def test_auth_with_legacy_protocol_rejected(self):
        """
        Authentication with legacy protocol should fail immediately.

        What this tests:
        ---------------
        1. Auth + legacy protocol rejected
        2. create_with_auth validates protocol
        3. Consistent validation everywhere
        4. Clear error message

        Why this matters:
        ----------------
        Legacy protocols + auth problematic:
        - Security vulnerabilities
        - Missing auth features
        - Incompatible mechanisms

        Prevent insecure configurations
        at setup time.
        """
        with pytest.raises(ConfigurationError) as exc_info:
            AsyncCluster.create_with_auth(
                contact_points=["localhost"], username="user", password="pass", protocol_version=3
            )

        assert "Protocol version 3 is not supported" in str(exc_info.value)

    def test_migration_guidance_for_v4(self):
        """
        Protocol v4 error should include migration guidance.

        What this tests:
        ---------------
        1. v4 error includes specifics
        2. Mentions Cassandra 4.0
        3. Release date provided
        4. Clear upgrade path

        Why this matters:
        ----------------
        v4 users need specific help:
        - Many on Cassandra 3.x
        - Upgrade path exists
        - Time-based guidance helps

        Actionable errors reduce
        support burden.
        """
        with pytest.raises(ConfigurationError) as exc_info:
            AsyncCluster(contact_points=["localhost"], protocol_version=4)

        error_msg = str(exc_info.value)
        assert "async-cassandra requires CQL protocol v5" in error_msg
        assert "Cassandra 4.0 (released July 2021)" in error_msg

    def test_error_message_includes_upgrade_path(self):
        """
        Legacy protocol errors should include upgrade path.

        What this tests:
        ---------------
        1. Errors mention upgrade
        2. Target version specified (4.0+)
        3. Actionable guidance
        4. Not just "not supported"

        Why this matters:
        ----------------
        Good error messages:
        - Guide users to solution
        - Reduce confusion
        - Speed up migration

        Users need to know both
        problem AND solution.
        """
        with pytest.raises(ConfigurationError) as exc_info:
            AsyncCluster(contact_points=["localhost"], protocol_version=3)

        error_msg = str(exc_info.value)
        assert "upgrade" in error_msg.lower()
        assert "4.0+" in error_msg
