"""
Integration tests for protocol version connection.

Only tests actual connection with protocol v5 - validation logic is tested in unit tests.
"""

import pytest

from async_cassandra import AsyncCluster


class TestProtocolVersionIntegration:
    """Integration tests for protocol version connection."""

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
    async def test_no_protocol_version_uses_negotiation(self, cassandra_service):
        """Test that omitting protocol version allows negotiation."""
        cluster = AsyncCluster(
            contact_points=[cassandra_service.contact_point]
            # No protocol_version specified - driver will negotiate
        )

        try:
            session = await cluster.connect()

            # Should connect successfully
            result = await session.execute("SELECT release_version FROM system.local")
            assert result.one() is not None

            await session.close()
        finally:
            await cluster.shutdown()
