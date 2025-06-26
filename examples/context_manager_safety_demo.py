#!/usr/bin/env python3
"""
Demonstration of context manager safety in async-cassandra.

This example shows how context managers properly isolate resource cleanup,
ensuring that errors in one operation don't close shared resources.
"""

import asyncio
import logging
import uuid

from async_cassandra import AsyncCluster
from async_cassandra.exceptions import QueryError

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def demonstrate_query_error_safety(cluster):
    """Show that query errors don't close the session."""
    logger.info("\n=== Demonstrating Query Error Safety ===")

    session = await cluster.connect()

    try:
        # This will fail
        await session.execute("SELECT * FROM non_existent_table")
    except QueryError as e:
        logger.info(f"Query failed as expected: {e}")

    # Session should still work
    logger.info("Session still works after error:")
    result = await session.execute("SELECT release_version FROM system.local")
    logger.info(f"Cassandra version: {result.one().release_version}")

    await session.close()


async def demonstrate_streaming_error_safety(cluster):
    """Show that streaming errors don't close the session."""
    logger.info("\n=== Demonstrating Streaming Error Safety ===")

    session = await cluster.connect()

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
    await session.set_keyspace("context_demo")

    await session.execute(
        """
        CREATE TABLE IF NOT EXISTS test_data (
            id UUID PRIMARY KEY,
            value TEXT
        )
        """
    )

    # Insert some data
    for i in range(10):
        await session.execute(
            "INSERT INTO test_data (id, value) VALUES (%s, %s)", [uuid.uuid4(), f"value_{i}"]
        )

    # Try streaming from non-existent table (will fail)
    try:
        async with await session.execute_stream("SELECT * FROM non_existent_table") as stream:
            async for row in stream:
                pass
    except Exception as e:
        logger.info(f"Streaming failed as expected: {e}")

    # Session should still work for new streaming
    logger.info("Starting new streaming operation after error:")
    count = 0
    async with await session.execute_stream("SELECT * FROM test_data") as stream:
        async for row in stream:
            count += 1

    logger.info(f"Successfully streamed {count} rows after error")

    # Cleanup
    await session.execute("DROP KEYSPACE context_demo")
    await session.close()


async def demonstrate_context_manager_isolation(cluster):
    """Show how context managers isolate resource cleanup."""
    logger.info("\n=== Demonstrating Context Manager Isolation ===")

    # Scenario 1: Session context doesn't affect cluster
    logger.info("\nScenario 1: Session context with error")
    try:
        async with await cluster.connect() as session:
            result = await session.execute("SELECT now() FROM system.local")
            logger.info(f"Query succeeded: {result.one()[0]}")
            raise ValueError("Simulated error in session context")
    except ValueError:
        logger.info("Error handled, session was closed by context manager")

    # Cluster should still work
    logger.info("Creating new session from same cluster:")
    session2 = await cluster.connect()
    result = await session2.execute("SELECT now() FROM system.local")
    logger.info(f"New session works: {result.one()[0]}")
    await session2.close()

    # Scenario 2: Streaming context doesn't affect session
    logger.info("\nScenario 2: Streaming context with early exit")
    session3 = await cluster.connect()

    # Stream with early exit
    count = 0
    async with await session3.execute_stream("SELECT * FROM system.local") as stream:
        async for row in stream:
            count += 1
            break  # Early exit

    logger.info(f"Exited streaming early after {count} row")

    # Session should still work
    result = await session3.execute("SELECT now() FROM system.local")
    logger.info(f"Session still works: {result.one()[0]}")

    await session3.close()


async def demonstrate_concurrent_safety(cluster):
    """Show that multiple operations can use shared resources safely."""
    logger.info("\n=== Demonstrating Concurrent Safety ===")

    # Create shared session
    session = await cluster.connect()

    async def worker(worker_id, query_count):
        """Worker that executes queries."""
        for i in range(query_count):
            try:
                result = await session.execute("SELECT now() FROM system.local")
                logger.info(f"Worker {worker_id} query {i+1}: {result.one()[0]}")
                await asyncio.sleep(0.1)
            except Exception as e:
                logger.error(f"Worker {worker_id} error: {e}")

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
                        logger.info(f"Streamer: Processed {count} keyspaces")
                        await asyncio.sleep(0.1)
                logger.info(f"Streamer: Total {count} keyspaces")
        except Exception as e:
            logger.error(f"Streamer error: {e}")

    # Run workers concurrently
    await asyncio.gather(worker(1, 3), worker(2, 3), streamer(), return_exceptions=True)

    logger.info("All concurrent operations completed")
    await session.close()


async def main():
    """Run all demonstrations."""
    logger.info("Starting Context Manager Safety Demonstration")

    # Use cluster in context manager for automatic cleanup
    async with AsyncCluster(["localhost"]) as cluster:
        await demonstrate_query_error_safety(cluster)
        await demonstrate_streaming_error_safety(cluster)
        await demonstrate_context_manager_isolation(cluster)
        await demonstrate_concurrent_safety(cluster)

    logger.info("\nAll demonstrations completed successfully!")
    logger.info("Key takeaways:")
    logger.info("1. Query errors don't close sessions")
    logger.info("2. Streaming errors don't close sessions")
    logger.info("3. Context managers only close their own resources")
    logger.info("4. Multiple operations can safely share sessions and clusters")


if __name__ == "__main__":
    asyncio.run(main())
