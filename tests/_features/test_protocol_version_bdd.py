"""
BDD tests for protocol version requirements.
"""

import pytest

from async_cassandra import AsyncCluster
from async_cassandra.exceptions import ConfigurationError


class TestProtocolVersionBDD:
    """BDD-style tests for protocol version requirements."""

    @pytest.mark.asyncio
    async def test_legacy_protocol_rejection(self):
        """
        Given: A developer trying to use async-cassandra with an old protocol version
        When: They create a cluster with protocol version < 5
        Then: They receive a clear error explaining the requirement and upgrade path
        """
        # Given: Developer has configuration for protocol v3 (common in Cassandra 2.x/3.x)
        legacy_config = {"contact_points": ["localhost"], "protocol_version": 3}

        # When: They try to create a cluster
        with pytest.raises(ConfigurationError) as exc_info:
            AsyncCluster(**legacy_config)

        # Then: They get a helpful error message
        error_message = str(exc_info.value)
        assert "Protocol version 3 is not supported" in error_message
        assert "requires CQL protocol v5 or higher" in error_message
        assert "Cassandra 4.0" in error_message
        assert "Please upgrade" in error_message

    @pytest.mark.asyncio
    async def test_modern_protocol_acceptance(self):
        """
        Given: A developer using a modern Cassandra cluster
        When: They create a cluster with protocol version 5
        Then: The connection works without issues
        """
        # Given: Modern Cassandra 4.x configuration
        modern_config = {"contact_points": ["localhost"], "protocol_version": 5}

        # When: They create a cluster and connect
        cluster = AsyncCluster(**modern_config)

        try:
            session = await cluster.connect()

            # Then: Everything works as expected
            result = await session.execute("SELECT cluster_name FROM system.local")
            assert result.one() is not None

            await session.close()
        finally:
            await cluster.shutdown()

    @pytest.mark.asyncio
    async def test_automatic_protocol_negotiation(self):
        """
        Given: A developer who doesn't specify a protocol version
        When: They create a cluster without protocol_version parameter
        Then: The driver negotiates the best available version automatically
        """
        # Given: No protocol version specified
        auto_config = {
            "contact_points": ["localhost"]
            # protocol_version omitted intentionally
        }

        # When: They create and use the cluster
        cluster = AsyncCluster(**auto_config)

        try:
            session = await cluster.connect()

            # Then: Connection succeeds with negotiated protocol
            result = await session.execute("SELECT cluster_name FROM system.local")
            assert result.one() is not None

            # And: They can check the negotiated version if needed
            # (The actual protocol version is available in cluster.metadata)

            await session.close()
        finally:
            await cluster.shutdown()

    @pytest.mark.asyncio
    async def test_cloud_provider_guidance(self):
        """
        Given: A developer using a cloud-managed Cassandra service
        When: They try to use an old protocol version
        Then: The error message includes cloud-specific guidance
        """
        # Given: Cloud service configuration with old protocol
        cloud_config = {
            "contact_points": ["localhost"],
            "protocol_version": 4,  # Many cloud services defaulted to v4
        }

        # When: They try to connect
        with pytest.raises(ConfigurationError) as exc_info:
            AsyncCluster(**cloud_config)

        # Then: Error includes cloud provider guidance
        error_message = str(exc_info.value)
        assert "cloud provider" in error_message
        assert "check their documentation" in error_message

    @pytest.mark.asyncio
    async def test_authentication_with_protocol_validation(self):
        """
        Given: A developer using authentication with legacy protocol
        When: They use create_with_auth with an old protocol version
        Then: Protocol validation happens before any connection attempt
        """
        # Given: Authentication credentials and old protocol
        auth_config = {
            "contact_points": ["localhost"],
            "username": "cassandra",
            "password": "cassandra",
            "protocol_version": 2,
        }

        # When: They try to create authenticated cluster
        with pytest.raises(ConfigurationError) as exc_info:
            AsyncCluster.create_with_auth(**auth_config)

        # Then: Protocol is validated immediately (no connection attempted)
        assert "Protocol version 2 is not supported" in str(exc_info.value)
        # The error happens during cluster creation, not connection

    @pytest.mark.asyncio
    async def test_future_protocol_support(self):
        """
        Given: A developer using a future protocol version
        When: They specify protocol version 6 or higher
        Then: async-cassandra allows it (future-proofing)
        """
        # Given: Future protocol version (v6 doesn't exist yet but might)
        future_config = {"contact_points": ["localhost"], "protocol_version": 6}

        # When: They create the cluster
        # Then: No ConfigurationError is raised (actual connection might fail)
        try:
            cluster = AsyncCluster(**future_config)
            # We don't try to connect as v6 doesn't exist yet
            # The point is that async-cassandra doesn't block future versions
            await cluster.shutdown()
        except ConfigurationError:
            pytest.fail("Should not reject future protocol versions")

    @pytest.mark.asyncio
    async def test_migration_scenario(self):
        """
        Given: A team migrating from sync to async cassandra driver
        When: Their old code used protocol v4
        Then: They get clear migration instructions
        """
        # Given: Legacy synchronous driver configuration
        sync_driver_config = {
            "contact_points": ["localhost"],
            "protocol_version": 4,  # Common in sync driver usage
            "executor_threads": 8,  # They were optimizing thread pools
        }

        # When: They try to use async-cassandra with same config
        with pytest.raises(ConfigurationError) as exc_info:
            AsyncCluster(**sync_driver_config)

        # Then: Error helps with migration
        error_message = str(exc_info.value)
        assert "Protocol version 4 is not supported" in error_message
        assert "optimal async performance" in error_message
        # The message explains WHY v5+ is needed for async
