#!/usr/bin/env python3
"""
Simple metrics collection example with async-cassandra.

This example shows basic metrics collection and monitoring.
"""

import asyncio
import time
import uuid
from datetime import datetime

from async_cassandra import AsyncCluster
from async_cassandra.metrics import InMemoryMetricsCollector, MetricsMiddleware


async def main():
    """Run basic metrics example."""
    print("🚀 async-cassandra Metrics Example\n")

    # Create metrics collector
    collector = InMemoryMetricsCollector(max_entries=1000)

    # Create metrics middleware
    metrics_middleware = MetricsMiddleware([collector])

    # Create cluster using context manager
    async with AsyncCluster(["localhost"]) as cluster:
        # Create session using context manager
        async with await cluster.connect() as session:

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

            # Execute some queries and collect metrics
            print("\n=== Query Metrics Example ===")
            print("Executing queries...")

            # Prepare statements
            insert_stmt = await session.prepare(
                "INSERT INTO users (id, name, email, created_at) VALUES (?, ?, ?, ?)"
            )
            select_stmt = await session.prepare("SELECT * FROM users WHERE id = ?")

            # Insert some users with metrics tracking
            user_ids = []
            for i in range(10):
                user_id = uuid.uuid4()
                user_ids.append(user_id)

                # Track query metrics
                start_time = time.time()
                try:
                    await session.execute(
                        insert_stmt, [user_id, f"User {i}", f"user{i}@example.com", datetime.now()]
                    )
                    duration = time.time() - start_time
                    await metrics_middleware.record_query_metrics(
                        "INSERT INTO users", duration, success=True, parameters_count=4
                    )
                except Exception as e:
                    duration = time.time() - start_time
                    await metrics_middleware.record_query_metrics(
                        "INSERT INTO users", duration, success=False, error_type=type(e).__name__
                    )
                    raise

            print(f"✅ Inserted {len(user_ids)} users")

            # Select users with metrics
            for user_id in user_ids[:5]:
                start_time = time.time()
                try:
                    result = await session.execute(select_stmt, [user_id])
                    user = result.one()
                    duration = time.time() - start_time
                    await metrics_middleware.record_query_metrics(
                        "SELECT * FROM users WHERE id = ?",
                        duration,
                        success=True,
                        parameters_count=1,
                        result_size=1 if user else 0,
                    )
                    if user:
                        print(f"  Found user: {user.name}")
                except Exception as e:
                    duration = time.time() - start_time
                    await metrics_middleware.record_query_metrics(
                        "SELECT * FROM users WHERE id = ?",
                        duration,
                        success=False,
                        error_type=type(e).__name__,
                    )
                    raise

            # Execute a failing query
            print("\n=== Error Tracking Example ===")
            try:
                start_time = time.time()
                await session.execute("SELECT * FROM non_existent_table")
            except Exception as e:
                duration = time.time() - start_time
                await metrics_middleware.record_query_metrics(
                    "SELECT * FROM non_existent_table",
                    duration,
                    success=False,
                    error_type=type(e).__name__,
                )
                print(f"  ❌ Expected error recorded: {type(e).__name__}")

            # Connection health monitoring
            print("\n=== Connection Health Monitoring ===")

            # Record connection health metrics
            hosts = cluster._cluster.metadata.all_hosts()
            for host in hosts:
                # Simulate connection check
                start_time = time.time()
                try:
                    result = await session.execute("SELECT now() FROM system.local")
                    response_time = time.time() - start_time
                    await metrics_middleware.record_connection_metrics(
                        str(host.address),
                        is_healthy=True,
                        response_time=response_time,
                        total_queries=20,  # Example value
                    )
                    print(f"✅ {host.address}: UP (response time: {response_time*1000:.1f}ms)")
                except Exception:
                    response_time = time.time() - start_time
                    await metrics_middleware.record_connection_metrics(
                        str(host.address),
                        is_healthy=False,
                        response_time=response_time,
                        error_count=1,
                    )
                    print(f"❌ {host.address}: DOWN")

            # Get and display metrics summary
            print("\n=== Performance Summary ===")
            stats = await collector.get_stats()

            if "query_performance" in stats:
                perf = stats["query_performance"]
                if "total_queries" in perf:
                    print("\n📊 Query Metrics:")
                    print(f"  Total queries: {perf['total_queries']}")
                    print(f"  Recent queries (5min): {perf.get('recent_queries_5min', 0)}")
                    print(f"  Success rate: {perf.get('success_rate', 0)*100:.1f}%")
                    print(f"  Average latency: {perf.get('avg_duration_ms', 0):.1f}ms")
                    print(f"  Min latency: {perf.get('min_duration_ms', 0):.1f}ms")
                    print(f"  Max latency: {perf.get('max_duration_ms', 0):.1f}ms")
                    print(f"  Queries/second: {perf.get('queries_per_second', 0):.2f}")

            if "error_summary" in stats and stats["error_summary"]:
                print("\n❌ Error Summary:")
                for error_type, count in stats["error_summary"].items():
                    print(f"  {error_type}: {count}")

            if "top_queries" in stats and stats["top_queries"]:
                print("\n🔥 Top Queries:")
                for query_hash, count in list(stats["top_queries"].items())[:5]:
                    print(f"  Query {query_hash}: {count} executions")

            if "connection_health" in stats:
                print("\n🔗 Connection Health:")
                for host, health in stats["connection_health"].items():
                    status = "UP" if health["healthy"] else "DOWN"
                    print(f"  {host}: {status} (response: {health['response_time_ms']:.1f}ms)")

            # Clean up
            print("\nCleaning up...")
            await session.execute("DROP KEYSPACE metrics_demo")

    print("\n✅ Example complete!")


if __name__ == "__main__":
    asyncio.run(main())
