#!/usr/bin/env python3
"""
Demonstration of context manager safety in async-cassandra.

This example shows how context managers properly isolate resource cleanup,
ensuring that errors in one operation don't close shared resources.

How to run:
-----------
1. Using Make (automatically starts Cassandra if needed):
   make example-context-safety

2. With external Cassandra cluster:
   CASSANDRA_CONTACT_POINTS=10.0.0.1,10.0.0.2 make example-context-safety

3. Direct Python execution:
   python examples/context_manager_safety_demo.py

4. With custom contact points:
   CASSANDRA_CONTACT_POINTS=cassandra.example.com python examples/context_manager_safety_demo.py

Environment variables:
- CASSANDRA_CONTACT_POINTS: Comma-separated list of contact points (default: localhost)
- CASSANDRA_PORT: Port number (default: 9042)
"""

import asyncio
import logging
import os
import uuid

from async_cassandra import AsyncCluster
from cassandra import InvalidRequest

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def demonstrate_query_error_safety(cluster):
    """Show that query errors don't close the session."""
    logger.info("\n" + "=" * 80)
    logger.info("🛡️  QUERY ERROR SAFETY DEMONSTRATION")
    logger.info("=" * 80)

    async with await cluster.connect() as session:
        logger.info("\n🧪 Test 1: Execute a failing query")
        try:
            # This will fail
            await session.execute("SELECT * FROM non_existent_table")
        except InvalidRequest as e:
            logger.info(f"   ✓ Query failed as expected: {type(e).__name__}")

        # Session should still work
        logger.info("\n🧪 Test 2: Verify session still works after error")
        result = await session.execute("SELECT release_version FROM system.local")
        logger.info(f"   ✅ Session is healthy! Cassandra version: {result.one().release_version}")
        logger.info("\n💡 Key insight: Query errors are isolated - they don't affect the session!")


async def demonstrate_streaming_error_safety(cluster):
    """Show that streaming errors don't close the session."""
    logger.info("\n" + "=" * 80)
    logger.info("🌊 STREAMING ERROR SAFETY DEMONSTRATION")
    logger.info("=" * 80)

    async with await cluster.connect() as session:
        logger.info("\n🛠️  Setting up test data...")
        # Create test keyspace and data
        await session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS context_demo
            WITH REPLICATION = {
                'class': 'SimpleStrategy',
                'replication_factor': 1
            }
            """
        )
        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS context_demo.test_data (
                id UUID PRIMARY KEY,
                value TEXT
            )
            """
        )

        # Insert some data using prepared statement
        insert_stmt = await session.prepare(
            "INSERT INTO context_demo.test_data (id, value) VALUES (?, ?)"
        )
        for i in range(10):
            await session.execute(insert_stmt, [uuid.uuid4(), f"value_{i}"])
        logger.info("   ✓ Created 10 test records")

        # Try streaming from non-existent table (will fail)
        logger.info("\n🧪 Test 1: Stream from non-existent table")
        try:
            async with await session.execute_stream("SELECT * FROM non_existent_table") as stream:
                async for row in stream:
                    pass
        except Exception as e:
            logger.info(f"   ✓ Streaming failed as expected: {type(e).__name__}")

        # Session should still work for new streaming
        logger.info("\n🧪 Test 2: Start new streaming operation after error")
        count = 0
        async with await session.execute_stream("SELECT * FROM context_demo.test_data") as stream:
            async for row in stream:
                count += 1

        logger.info(f"   ✅ Successfully streamed {count} rows after error!")
        logger.info("\n💡 Key insight: Streaming errors are isolated - session remains healthy!")

        # Cleanup
        await session.execute("DROP KEYSPACE context_demo")


async def demonstrate_context_manager_isolation(cluster):
    """Show how context managers isolate resource cleanup."""
    logger.info("\n" + "=" * 80)
    logger.info("🔒 CONTEXT MANAGER ISOLATION DEMONSTRATION")
    logger.info("=" * 80)

    # Scenario 1: Session context doesn't affect cluster
    logger.info("\n🧪 Scenario 1: Session error doesn't affect cluster")
    try:
        async with await cluster.connect() as session:
            result = await session.execute("SELECT now() FROM system.local")
            logger.info(f"   ✓ Query succeeded: {result.one()[0]}")
            logger.info("   💥 Simulating error...")
            raise ValueError("Simulated error in session context")
    except ValueError:
        logger.info("   ✓ Error handled, session closed by context manager")

    # Cluster should still work
    logger.info("\n🧪 Creating new session from same cluster:")
    async with await cluster.connect() as session2:
        result = await session2.execute("SELECT now() FROM system.local")
        logger.info(f"   ✅ New session works perfectly: {result.one()[0]}")

    # Scenario 2: Streaming context doesn't affect session
    logger.info("\n🧪 Scenario 2: Early streaming exit doesn't affect session")
    async with await cluster.connect() as session3:
        # Stream with early exit
        count = 0
        logger.info("   🔄 Starting streaming with early exit...")
        async with await session3.execute_stream("SELECT * FROM system.local") as stream:
            async for row in stream:
                count += 1
                logger.info(f"   ✓ Read {count} row, exiting early...")
                break  # Early exit

        # Session should still work
        logger.info("\n   🧪 Testing session after early streaming exit:")
        result = await session3.execute("SELECT now() FROM system.local")
        logger.info(f"   ✅ Session still healthy: {result.one()[0]}")

    logger.info("\n💡 Key insight: Context managers provide proper isolation!")


async def demonstrate_concurrent_safety(cluster):
    """Show that multiple operations can use shared resources safely."""
    logger.info("\n" + "=" * 80)
    logger.info("🚀 CONCURRENT OPERATIONS SAFETY DEMONSTRATION")
    logger.info("=" * 80)

    # Create shared session
    logger.info("\n🔄 Running multiple concurrent operations on shared session...")
    async with await cluster.connect() as session:

        async def worker(worker_id, query_count):
            """Worker that executes queries."""
            for i in range(query_count):
                try:
                    await session.execute("SELECT now() FROM system.local")
                    logger.info(f"   👷 Worker {worker_id} query {i+1}: Success")
                    await asyncio.sleep(0.1)
                except Exception as e:
                    logger.error(f"   ❌ Worker {worker_id} error: {e}")

        async def streamer():
            """Worker that uses streaming."""
            try:
                async with await session.execute_stream(
                    "SELECT * FROM system_schema.keyspaces"
                ) as stream:
                    count = 0
                    async for row in stream:
                        count += 1
                        if count % 5 == 0:
                            logger.info(f"   🌊 Streamer: Processed {count} keyspaces")
                            await asyncio.sleep(0.1)
                    logger.info(f"   ✅ Streamer: Completed ({count} keyspaces)")
            except Exception as e:
                logger.error(f"   ❌ Streamer error: {e}")

        # Run workers concurrently
        await asyncio.gather(worker(1, 3), worker(2, 3), streamer(), return_exceptions=True)

        logger.info("\n✅ All concurrent operations completed successfully!")
        logger.info("\n💡 Key insight: Multiple operations can safely share a session!")


async def main():
    """Run all demonstrations."""
    logger.info("\n" + "=" * 80)
    logger.info("🛡️  CONTEXT MANAGER SAFETY DEMONSTRATION")
    logger.info("=" * 80)

    # Get contact points from environment or use localhost
    contact_points = os.environ.get("CASSANDRA_CONTACT_POINTS", "localhost").split(",")
    port = int(os.environ.get("CASSANDRA_PORT", "9042"))

    logger.info(f"\n📡 Connecting to Cassandra at {contact_points}:{port}")

    # Use cluster in context manager for automatic cleanup
    async with AsyncCluster(contact_points, port=port) as cluster:
        await demonstrate_query_error_safety(cluster)
        await demonstrate_streaming_error_safety(cluster)
        await demonstrate_context_manager_isolation(cluster)
        await demonstrate_concurrent_safety(cluster)

    logger.info("\n" + "=" * 80)
    logger.info("✅ ALL DEMONSTRATIONS COMPLETED SUCCESSFULLY!")
    logger.info("=" * 80)
    logger.info("\n🎯 Key Takeaways:")
    logger.info("   1. Query errors don't close sessions")
    logger.info("   2. Streaming errors don't close sessions")
    logger.info("   3. Context managers only close their own resources")
    logger.info("   4. Multiple operations can safely share sessions and clusters")
    logger.info("\n💡 Best Practice: Always use context managers for proper resource management!")


if __name__ == "__main__":
    asyncio.run(main())
