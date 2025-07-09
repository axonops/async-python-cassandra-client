#!/usr/bin/env python3
"""
Simple metrics collection example with async-cassandra.

This example shows basic metrics collection and monitoring.

How to run:
-----------
1. Using Make (automatically starts Cassandra if needed):
   make example-metrics-simple

2. With external Cassandra cluster:
   CASSANDRA_CONTACT_POINTS=10.0.0.1,10.0.0.2 make example-metrics-simple

3. Direct Python execution:
   python examples/metrics_simple.py

4. With custom contact points:
   CASSANDRA_CONTACT_POINTS=cassandra.example.com python examples/metrics_simple.py

Environment variables:
- CASSANDRA_CONTACT_POINTS: Comma-separated list of contact points (default: localhost)
- CASSANDRA_PORT: Port number (default: 9042)
"""

import asyncio
import os
import time
import uuid
from datetime import datetime

from async_cassandra import AsyncCluster
from async_cassandra.metrics import InMemoryMetricsCollector, MetricsMiddleware


async def main():
    """Run basic metrics example."""
    print("\n" + "=" * 80)
    print("🚀 ASYNC-CASSANDRA METRICS COLLECTION EXAMPLE")
    print("=" * 80)

    # Get contact points from environment or use localhost
    contact_points = os.environ.get("CASSANDRA_CONTACT_POINTS", "localhost").split(",")
    port = int(os.environ.get("CASSANDRA_PORT", "9042"))

    print(f"\n📡 Connecting to Cassandra at {contact_points}:{port}")

    # Create metrics collector
    collector = InMemoryMetricsCollector(max_entries=1000)

    # Create metrics middleware
    metrics_middleware = MetricsMiddleware([collector])

    # Create cluster using context manager
    async with AsyncCluster(contact_points, port=port) as cluster:
        # Create session using context manager
        async with await cluster.connect() as session:

            # Set up test keyspace
            print("\n🛠️  Setting up test database...")
            await session.execute(
                """
                CREATE KEYSPACE IF NOT EXISTS metrics_demo
                WITH REPLICATION = {
                    'class': 'SimpleStrategy',
                    'replication_factor': 1
                }
            """
            )

            # Create test table
            await session.execute(
                """
                CREATE TABLE IF NOT EXISTS metrics_demo.users (
                    id UUID PRIMARY KEY,
                    name TEXT,
                    email TEXT,
                    created_at TIMESTAMP
                )
            """
            )

            print("✅ Database ready!")

            # Execute some queries and collect metrics
            print("\n" + "=" * 80)
            print("📊 QUERY METRICS COLLECTION")
            print("=" * 80)
            print("\n🔄 Executing queries with metrics tracking...")

            # Prepare statements
            insert_stmt = await session.prepare(
                "INSERT INTO metrics_demo.users (id, name, email, created_at) VALUES (?, ?, ?, ?)"
            )
            select_stmt = await session.prepare("SELECT * FROM metrics_demo.users WHERE id = ?")

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
                        "INSERT INTO metrics_demo.users", duration, success=True, parameters_count=4
                    )
                except Exception as e:
                    duration = time.time() - start_time
                    await metrics_middleware.record_query_metrics(
                        "INSERT INTO metrics_demo.users",
                        duration,
                        success=False,
                        error_type=type(e).__name__,
                    )
                    raise

            print(f"\n✅ Inserted {len(user_ids)} users successfully")

            # Select users with metrics
            for user_id in user_ids[:5]:
                start_time = time.time()
                try:
                    result = await session.execute(select_stmt, [user_id])
                    user = result.one()
                    duration = time.time() - start_time
                    await metrics_middleware.record_query_metrics(
                        "SELECT * FROM metrics_demo.users WHERE id = ?",
                        duration,
                        success=True,
                        parameters_count=1,
                        result_size=1 if user else 0,
                    )
                    if user:
                        print(f"   • Found: {user.name}")
                except Exception as e:
                    duration = time.time() - start_time
                    await metrics_middleware.record_query_metrics(
                        "SELECT * FROM metrics_demo.users WHERE id = ?",
                        duration,
                        success=False,
                        error_type=type(e).__name__,
                    )
                    raise

            # Execute a failing query
            print("\n" + "=" * 80)
            print("❌ ERROR TRACKING DEMONSTRATION")
            print("=" * 80)
            print("\n🧪 Testing error metrics collection...")
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
                print(f"   ✓ Expected error captured: {type(e).__name__}")

            # Connection health monitoring
            print("\n" + "=" * 80)
            print("🔗 CONNECTION HEALTH MONITORING")
            print("=" * 80)
            print("\n🏥 Checking cluster health...")

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
                    print(f"   ✅ {host.address}: UP (response time: {response_time*1000:.1f}ms)")
                except Exception:
                    response_time = time.time() - start_time
                    await metrics_middleware.record_connection_metrics(
                        str(host.address),
                        is_healthy=False,
                        response_time=response_time,
                        error_count=1,
                    )
                    print(f"   ❌ {host.address}: DOWN")

            # Get and display metrics summary
            print("\n" + "=" * 80)
            print("📊 PERFORMANCE METRICS SUMMARY")
            print("=" * 80)
            stats = await collector.get_stats()

            if "query_performance" in stats:
                perf = stats["query_performance"]
                if "total_queries" in perf:
                    print("\n📈 Query Performance:")
                    print(f"   • Total queries: {perf['total_queries']}")
                    print(f"   • Recent queries (5min): {perf.get('recent_queries_5min', 0)}")
                    print(f"   • Success rate: {perf.get('success_rate', 0)*100:.1f}%")
                    print(f"   • Average latency: {perf.get('avg_duration_ms', 0):.1f}ms")
                    print(f"   • Min latency: {perf.get('min_duration_ms', 0):.1f}ms")
                    print(f"   • Max latency: {perf.get('max_duration_ms', 0):.1f}ms")
                    print(f"   • Queries/second: {perf.get('queries_per_second', 0):.2f}")

            if "error_summary" in stats and stats["error_summary"]:
                print("\n❌ Error Summary:")
                for error_type, count in stats["error_summary"].items():
                    print(f"   • {error_type}: {count} occurrences")

            if "top_queries" in stats and stats["top_queries"]:
                print("\n🔥 Top Queries by Frequency:")
                for i, (query_hash, count) in enumerate(list(stats["top_queries"].items())[:5], 1):
                    print(f"   {i}. Query {query_hash}: {count} executions")

            if "connection_health" in stats:
                print("\n🔗 Connection Health Status:")
                for host, health in stats["connection_health"].items():
                    status = "✅ UP" if health["healthy"] else "❌ DOWN"
                    print(f"   • {host}: {status} (response: {health['response_time_ms']:.1f}ms)")

            # Clean up
            print("\n🧹 Cleaning up...")
            await session.execute("DROP KEYSPACE metrics_demo")
            print("✅ Keyspace dropped")

    print("\n" + "=" * 80)
    print("✅ METRICS EXAMPLE COMPLETE!")
    print("=" * 80)
    print("\n💡 This example demonstrated:")
    print("   • Query performance tracking")
    print("   • Error rate monitoring")
    print("   • Connection health checks")
    print("   • Metrics aggregation and reporting")


if __name__ == "__main__":
    asyncio.run(main())
