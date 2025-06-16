"""
Integration tests for protocol version validation.
"""

import pytest

from async_cassandra import AsyncCluster
from async_cassandra.exceptions import ConfigurationError


class TestProtocolVersionIntegration:
    """Integration tests for protocol version requirements."""

    @pytest.mark.asyncio
    async def test_protocol_v5_connection(self, cassandra_service):
        """Test successful connection with protocol v5."""
        cluster = AsyncCluster(contact_points=[cassandra_service.contact_point], protocol_version=5)

        try:
            session = await cluster.connect()

            # Verify we can execute queries
            result = await session.execute("SELECT release_version FROM system.local")
            row = result.one()
            assert row is not None

            await session.close()
        finally:
            await cluster.shutdown()

    @pytest.mark.asyncio
    async def test_protocol_v4_rejected(self, cassandra_service):
        """Test that protocol v4 is rejected with clear error message."""
        with pytest.raises(ConfigurationError) as exc_info:
            AsyncCluster(contact_points=[cassandra_service.contact_point], protocol_version=4)

        error_msg = str(exc_info.value)
        assert "Protocol version 4 is not supported" in error_msg
        assert "requires CQL protocol v5 or higher" in error_msg
        assert "Cassandra 4.0" in error_msg
        assert "July 2021" in error_msg

    @pytest.mark.asyncio
    async def test_protocol_v3_rejected(self, cassandra_service):
        """Test that protocol v3 is rejected."""
        with pytest.raises(ConfigurationError) as exc_info:
            AsyncCluster(contact_points=[cassandra_service.contact_point], protocol_version=3)

        assert "Protocol version 3 is not supported" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_protocol_v2_rejected(self, cassandra_service):
        """Test that protocol v2 is rejected."""
        with pytest.raises(ConfigurationError) as exc_info:
            AsyncCluster(contact_points=[cassandra_service.contact_point], protocol_version=2)

        assert "Protocol version 2 is not supported" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_protocol_v1_rejected(self, cassandra_service):
        """Test that protocol v1 is rejected."""
        with pytest.raises(ConfigurationError) as exc_info:
            AsyncCluster(contact_points=[cassandra_service.contact_point], protocol_version=1)

        assert "Protocol version 1 is not supported" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_no_protocol_version_defaults_to_v5(self, cassandra_service):
        """Test that omitting protocol version defaults to v5."""
        cluster = AsyncCluster(
            contact_points=[cassandra_service.contact_point]
            # No protocol_version specified - should default to v5
        )

        try:
            # This will work if test Cassandra supports v5
            # Will fail with ConnectionError if it only supports v4 or lower
            session = await cluster.connect()

            # Should connect successfully with v5
            result = await session.execute("SELECT release_version FROM system.local")
            assert result.one() is not None

            await session.close()
        finally:
            await cluster.shutdown()

    @pytest.mark.asyncio
    async def test_create_with_auth_protocol_validation(self, cassandra_service):
        """Test that protocol validation works with create_with_auth."""
        # This should raise immediately when creating the cluster
        with pytest.raises(ConfigurationError) as exc_info:
            AsyncCluster.create_with_auth(
                contact_points=[cassandra_service.contact_point],
                username="cassandra",
                password="cassandra",
                protocol_version=3,
            )

        assert "Protocol version 3 is not supported" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_error_message_includes_upgrade_guidance(self, cassandra_service):
        """Test that error message provides helpful upgrade guidance."""
        with pytest.raises(ConfigurationError) as exc_info:
            AsyncCluster(contact_points=[cassandra_service.contact_point], protocol_version=4)

        error_msg = str(exc_info.value)
        # Check for helpful guidance
        assert "Please upgrade your Cassandra cluster" in error_msg
        assert "cloud provider" in error_msg
        assert "check their documentation" in error_msg
