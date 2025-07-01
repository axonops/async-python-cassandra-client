"""
Integration tests for protocol version connection.

Only tests actual connection with protocol v5 - validation logic is tested in unit tests.
"""

import pytest

from async_cassandra import AsyncCluster


class TestProtocolVersionIntegration:
    """Integration tests for protocol version connection."""

    @pytest.mark.asyncio
    async def test_protocol_v5_connection(self):
        """
        Test successful connection with protocol v5.

        What this tests:
        ---------------
        1. Protocol v5 connects
        2. Queries execute OK
        3. Results returned
        4. Clean shutdown

        Why this matters:
        ----------------
        Protocol v5 required:
        - Async features
        - Better performance
        - New data types

        Verifies minimum protocol
        version works.
        """
        cluster = AsyncCluster(contact_points=["localhost"], protocol_version=5)

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
    async def test_no_protocol_version_uses_negotiation(self):
        """
        Test that omitting protocol version allows negotiation.

        What this tests:
        ---------------
        1. Auto-negotiation works
        2. Driver picks version
        3. Connection succeeds
        4. Queries work

        Why this matters:
        ----------------
        Flexible configuration:
        - Works with any server
        - Future compatibility
        - Easier deployment

        Default behavior should
        just work.
        """
        cluster = AsyncCluster(
            contact_points=["localhost"]
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
