#!/usr/bin/env python3
"""
Simple metrics collection example with async-cassandra.

This example shows basic metrics collection and monitoring.
"""

import asyncio
import uuid
from datetime import datetime

from async_cassandra import AsyncCluster
from async_cassandra.metrics import InMemoryMetricsCollector, create_metrics_system
from async_cassandra.monitoring import ConnectionMonitor


async def main():
    """Run basic metrics example."""
    print("🚀 async-cassandra Metrics Example\n")

    # Create cluster
    cluster = AsyncCluster(["localhost"])

    try:
        # Create base session
        base_session = await cluster.connect()

        # Create metrics collector
        collector = InMemoryMetricsCollector(max_queries=100)

        # Wrap session with metrics
        session = create_metrics_system(base_session, [collector])

        # Set up test keyspace
        print("Setting up test database...")
        await session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS metrics_demo
            WITH REPLICATION = {
                'class': 'SimpleStrategy',
                'replication_factor': 1
            }
        """
        )

        await session.set_keyspace("metrics_demo")

        # Create test table
        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id UUID PRIMARY KEY,
                name TEXT,
                email TEXT,
                created_at TIMESTAMP
            )
        """
        )

        print("✅ Database ready\n")

        # Execute some queries
        print("Executing queries...")

        # Prepare statements
        insert_stmt = await session.prepare(
            "INSERT INTO users (id, name, email, created_at) VALUES (?, ?, ?, ?)"
        )
        select_stmt = await session.prepare("SELECT * FROM users WHERE id = ?")

        # Insert some users
        user_ids = []
        for i in range(10):
            user_id = uuid.uuid4()
            user_ids.append(user_id)
            await session.execute(
                insert_stmt, [user_id, f"User {i}", f"user{i}@example.com", datetime.now()]
            )

        # Select users
        for user_id in user_ids[:5]:
            result = await session.execute(select_stmt, [user_id])
            user = result.one()
            print(f"  Found user: {user.name}")

        # Execute a failing query
        try:
            await session.execute("SELECT * FROM non_existent_table")
        except Exception:
            print("  ❌ Expected error recorded")

        print("\n📊 Metrics Summary:")
        print("=" * 50)

        # Get metrics
        stats = collector.get_stats()

        print(f"Total queries: {stats['total_queries']}")
        print(f"Failed queries: {stats['failed_queries']}")
        print(f"Average latency: {stats['avg_latency']*1000:.1f}ms")
        print(f"Success rate: {(1 - stats['failed_queries']/stats['total_queries'])*100:.1f}%")

        print("\n🔥 Query Performance:")
        for query, metrics in list(stats["queries"].items())[:5]:
            print(f"\n{query[:60]}...")
            print(f"  Calls: {metrics['count']}")
            print(f"  Avg duration: {metrics['avg_duration']*1000:.1f}ms")
            print(f"  Total time: {metrics['total_duration']:.3f}s")

        # Monitor connections
        print("\n🔍 Connection Monitoring:")
        monitor = ConnectionMonitor(session, collector)
        results = await monitor.check_all_connections()

        for host, health in results.items():
            status = "✅ UP" if health["is_up"] else "❌ DOWN"
            print(f"{host}: {status} (response time: {health['response_time']*1000:.1f}ms)")

        # Clean up
        await session.execute("DROP KEYSPACE metrics_demo")

    finally:
        await cluster.shutdown()
        print("\n✅ Example complete!")


if __name__ == "__main__":
    asyncio.run(main())
