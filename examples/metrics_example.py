#!/usr/bin/env python3
"""
Comprehensive example of metrics collection with async-cassandra.

This example demonstrates:
- Setting up multiple metrics collectors
- Monitoring query performance
- Tracking connection health
- Basic metrics analysis
- Performance optimization patterns

How to run:
-----------
1. Using Make (automatically starts Cassandra if needed):
   make example-metrics

2. With external Cassandra cluster:
   CASSANDRA_CONTACT_POINTS=10.0.0.1,10.0.0.2 make example-metrics

3. Direct Python execution:
   python examples/metrics_example.py

4. With custom contact points:
   CASSANDRA_CONTACT_POINTS=cassandra.example.com python examples/metrics_example.py

Environment variables:
- CASSANDRA_CONTACT_POINTS: Comma-separated list of contact points (default: localhost)
- CASSANDRA_PORT: Port number (default: 9042)
"""

import asyncio
import os
import time
import uuid
from datetime import datetime, timezone

from async_cassandra import AsyncCluster
from async_cassandra.metrics import (
    InMemoryMetricsCollector,
    MetricsMiddleware,
    PrometheusMetricsCollector,
)


async def run_workload(session, metrics: MetricsMiddleware):
    """Run a workload and collect metrics."""
    print("\n" + "=" * 80)
    print("📊 RUNNING QUERY WORKLOAD")
    print("=" * 80)

    # Prepare statements for better performance
    insert_stmt = await session.prepare(
        "INSERT INTO metrics_demo.users (id, name, email, created_at) VALUES (?, ?, ?, ?)"
    )
    select_stmt = await session.prepare("SELECT * FROM metrics_demo.users WHERE id = ?")

    # Insert users with metrics tracking
    user_ids = []
    print("\n📝 Inserting users...")
    for i in range(20):
        user_id = uuid.uuid4()
        user_ids.append(user_id)

        start_time = time.time()
        try:
            await session.execute(
                insert_stmt,
                [user_id, f"User {i}", f"user{i}@example.com", datetime.now(timezone.utc)],
            )
            duration = time.time() - start_time
            await metrics.record_query_metrics(
                "INSERT INTO metrics_demo.users", duration, success=True, parameters_count=4
            )
        except Exception as e:
            duration = time.time() - start_time
            await metrics.record_query_metrics(
                "INSERT INTO metrics_demo.users",
                duration,
                success=False,
                error_type=type(e).__name__,
            )
            raise

    print(f"   ✓ Inserted {len(user_ids)} users successfully")

    # Select queries
    print("\n🔍 Reading users...")
    for user_id in user_ids[:10]:
        start_time = time.time()
        try:
            result = await session.execute(select_stmt, [user_id])
            duration = time.time() - start_time
            await metrics.record_query_metrics(
                "SELECT * FROM metrics_demo.users WHERE id = ?",
                duration,
                success=True,
                parameters_count=1,
                result_size=1 if result.one() else 0,
            )
        except Exception as e:
            duration = time.time() - start_time
            await metrics.record_query_metrics(
                "SELECT * FROM metrics_demo.users WHERE id = ?",
                duration,
                success=False,
                error_type=type(e).__name__,
            )
            raise

    print("   ✓ Read 10 users successfully")

    # Batch query simulation
    print("\n📦 Running batch query...")
    start_time = time.time()
    try:
        result = await session.execute("SELECT * FROM metrics_demo.users LIMIT 100")
        rows = list(result)
        duration = time.time() - start_time
        await metrics.record_query_metrics(
            "SELECT * FROM metrics_demo.users LIMIT 100",
            duration,
            success=True,
            result_size=len(rows),
        )
    except Exception as e:
        duration = time.time() - start_time
        await metrics.record_query_metrics(
            "SELECT * FROM metrics_demo.users LIMIT 100",
            duration,
            success=False,
            error_type=type(e).__name__,
        )
        raise

    print(f"   ✓ Batch query returned {len(rows)} rows")

    # Simulate an error
    print("\n🧪 Testing error handling...")
    try:
        start_time = time.time()
        await session.execute("SELECT * FROM non_existent_table")
    except Exception as e:
        duration = time.time() - start_time
        await metrics.record_query_metrics(
            "SELECT * FROM non_existent_table", duration, success=False, error_type=type(e).__name__
        )
        print(f"   ✓ Expected error properly tracked: {type(e).__name__}")


async def monitor_connections(cluster, metrics: MetricsMiddleware):
    """Monitor connection health."""
    print("\n" + "=" * 80)
    print("🏥 CONNECTION HEALTH MONITORING")
    print("=" * 80)
    print("\n🔍 Checking cluster nodes...")

    hosts = cluster._cluster.metadata.all_hosts()
    for host in hosts:
        start_time = time.time()
        try:
            # Quick health check
            async with await cluster.connect() as temp_session:
                await temp_session.execute("SELECT now() FROM system.local")
            response_time = time.time() - start_time

            await metrics.record_connection_metrics(
                str(host.address),
                is_healthy=True,
                response_time=response_time,
                total_queries=30,  # Example value
            )
            print(f"   ✅ {host.address}: Healthy (response: {response_time*1000:.1f}ms)")
        except Exception:
            response_time = time.time() - start_time
            await metrics.record_connection_metrics(
                str(host.address), is_healthy=False, response_time=response_time, error_count=1
            )
            print(f"   ❌ {host.address}: Unhealthy")


async def main():
    """Demonstrate comprehensive metrics collection."""
    print("\n" + "=" * 80)
    print("🚀 ADVANCED ASYNC-CASSANDRA METRICS EXAMPLE")
    print("=" * 80)

    # Get contact points from environment or use localhost
    contact_points = os.environ.get("CASSANDRA_CONTACT_POINTS", "localhost").split(",")
    port = int(os.environ.get("CASSANDRA_PORT", "9042"))

    print(f"\n📡 Connecting to Cassandra at {contact_points}:{port}")

    # 1. Set up metrics collectors
    print("\n🔧 Setting up metrics system...")

    # In-memory collector for development
    memory_collector = InMemoryMetricsCollector(max_entries=1000)
    collectors = [memory_collector]

    # Try to add Prometheus collector if available
    try:
        prometheus_collector = PrometheusMetricsCollector()
        if prometheus_collector._available:
            collectors.append(prometheus_collector)
            print("   ✅ Prometheus metrics enabled")
        else:
            print("   ℹ️  Prometheus client not available (pip install prometheus_client)")
    except Exception:
        print("   ℹ️  Prometheus client not available (pip install prometheus_client)")

    # Create metrics middleware
    metrics = MetricsMiddleware(collectors)

    # 2. Create cluster and run workload
    async with AsyncCluster(contact_points=contact_points, port=port) as cluster:
        async with await cluster.connect() as session:
            # Set up test environment
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

            # Run workload
            await run_workload(session, metrics)

            # Monitor connections
            await monitor_connections(cluster, metrics)

            # 3. Display collected metrics
            print("\n" + "=" * 80)
            print("📈 METRICS ANALYSIS RESULTS")
            print("=" * 80)

            # Get stats from memory collector
            stats = await memory_collector.get_stats()

            if "query_performance" in stats:
                perf = stats["query_performance"]
                if isinstance(perf, dict) and "total_queries" in perf:
                    print("\n📊 Query Performance Metrics:")
                    print(f"   • Total Queries: {perf['total_queries']}")
                    print(f"   • Recent Queries (5min): {perf.get('recent_queries_5min', 0)}")
                    print(f"   • Success Rate: {perf.get('success_rate', 0)*100:.1f}%")
                    print(f"   • Average Duration: {perf.get('avg_duration_ms', 0):.2f}ms")
                    print(f"   • Min Duration: {perf.get('min_duration_ms', 0):.2f}ms")
                    print(f"   • Max Duration: {perf.get('max_duration_ms', 0):.2f}ms")
                    print(f"   • Queries/Second: {perf.get('queries_per_second', 0):.2f}")

            if "error_summary" in stats and stats["error_summary"]:
                print("\n❌ Error Summary:")
                for error_type, count in stats["error_summary"].items():
                    print(f"   • {error_type}: {count} occurrences")

            if "top_queries" in stats and stats["top_queries"]:
                print("\n🔥 Top Queries by Frequency:")
                for i, (query_hash, count) in enumerate(list(stats["top_queries"].items())[:5], 1):
                    print(f"   {i}. Query {query_hash}: {count} executions")

            if "connection_health" in stats and stats["connection_health"]:
                print("\n🔗 Connection Health Details:")
                for host, health in stats["connection_health"].items():
                    status = "✅ UP" if health["healthy"] else "❌ DOWN"
                    print(f"\n   📡 {host}: {status}")
                    print(f"      • Response Time: {health['response_time_ms']:.2f}ms")
                    print(f"      • Total Queries: {health.get('total_queries', 0)}")
                    print(f"      • Error Count: {health.get('error_count', 0)}")

            # 4. Show optimization tips based on metrics
            print("\n" + "=" * 80)
            print("💡 PERFORMANCE INSIGHTS & RECOMMENDATIONS")
            print("=" * 80)

            if "query_performance" in stats and isinstance(stats["query_performance"], dict):
                perf = stats["query_performance"]
                avg_duration = perf.get("avg_duration_ms", 0)

                print("\n🎯 Query Performance Analysis:")
                if avg_duration > 10:
                    print("   ⚠️  Average query duration is high ({:.2f}ms)".format(avg_duration))
                    print("   📌 Recommendations:")
                    print("      • Use prepared statements for repeated queries")
                    print("      • Add appropriate secondary indexes")
                    print("      • Review your data model for optimization")
                    print("      • Consider partitioning strategy")
                elif avg_duration < 1:
                    print(
                        "   ✅ Excellent query performance! ({:.2f}ms average)".format(avg_duration)
                    )
                else:
                    print("   ✅ Good query performance ({:.2f}ms average)".format(avg_duration))

                success_rate = perf.get("success_rate", 1)
                if success_rate < 0.95:
                    print(f"\n   ⚠️  Success rate is {success_rate*100:.1f}%")
                    print("   📌 Action required: Check error logs for failure patterns")

            # Cleanup
            print("\n🧹 Cleaning up...")
            await session.execute("DROP KEYSPACE IF EXISTS metrics_demo")
            print("✅ Keyspace dropped")

    print("\n" + "=" * 80)
    print("✅ EXAMPLE COMPLETE!")
    print("=" * 80)
    print("\n🚀 Next Steps for Production:")
    print("   1. Install prometheus_client for production metrics")
    print("   2. Integrate with your monitoring dashboard (Grafana, etc.)")
    print("   3. Set up alerts based on performance thresholds")
    print("   4. Use metrics to identify and optimize slow queries")
    print("   5. Monitor connection health continuously")
    print("\n💡 Pro Tip: Export metrics to /metrics endpoint for Prometheus scraping!")


if __name__ == "__main__":
    asyncio.run(main())
