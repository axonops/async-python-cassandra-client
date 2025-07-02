#!/usr/bin/env python3
"""
Comprehensive example of metrics collection with async-cassandra.

This example demonstrates:
- Setting up multiple metrics collectors
- Monitoring query performance
- Tracking connection health
- Basic metrics analysis
- Performance optimization patterns
"""

import asyncio
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
    print("\n📊 Running queries with metrics collection...")

    # Prepare statements for better performance
    insert_stmt = await session.prepare(
        "INSERT INTO users (id, name, email, created_at) VALUES (?, ?, ?, ?)"
    )
    select_stmt = await session.prepare("SELECT * FROM users WHERE id = ?")

    # Insert users with metrics tracking
    user_ids = []
    print("Inserting users...")
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
                "INSERT INTO users", duration, success=True, parameters_count=4
            )
        except Exception as e:
            duration = time.time() - start_time
            await metrics.record_query_metrics(
                "INSERT INTO users", duration, success=False, error_type=type(e).__name__
            )
            raise

    # Select queries
    print("Reading users...")
    for user_id in user_ids[:10]:
        start_time = time.time()
        try:
            result = await session.execute(select_stmt, [user_id])
            duration = time.time() - start_time
            await metrics.record_query_metrics(
                "SELECT * FROM users WHERE id = ?",
                duration,
                success=True,
                parameters_count=1,
                result_size=1 if result.one() else 0,
            )
        except Exception as e:
            duration = time.time() - start_time
            await metrics.record_query_metrics(
                "SELECT * FROM users WHERE id = ?",
                duration,
                success=False,
                error_type=type(e).__name__,
            )
            raise

    # Batch query simulation
    print("Running batch query...")
    start_time = time.time()
    try:
        result = await session.execute("SELECT * FROM users LIMIT 100")
        rows = list(result)
        duration = time.time() - start_time
        await metrics.record_query_metrics(
            "SELECT * FROM users LIMIT 100", duration, success=True, result_size=len(rows)
        )
    except Exception as e:
        duration = time.time() - start_time
        await metrics.record_query_metrics(
            "SELECT * FROM users LIMIT 100", duration, success=False, error_type=type(e).__name__
        )
        raise

    # Simulate an error
    print("Testing error handling...")
    try:
        start_time = time.time()
        await session.execute("SELECT * FROM non_existent_table")
    except Exception as e:
        duration = time.time() - start_time
        await metrics.record_query_metrics(
            "SELECT * FROM non_existent_table", duration, success=False, error_type=type(e).__name__
        )
        print(f"  ❌ Expected error caught: {type(e).__name__}")


async def monitor_connections(cluster, metrics: MetricsMiddleware):
    """Monitor connection health."""
    print("\n🏥 Monitoring connection health...")

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
            print(f"  ✅ {host.address}: Healthy (response: {response_time*1000:.1f}ms)")
        except Exception:
            response_time = time.time() - start_time
            await metrics.record_connection_metrics(
                str(host.address), is_healthy=False, response_time=response_time, error_count=1
            )
            print(f"  ❌ {host.address}: Unhealthy")


async def main():
    """Demonstrate comprehensive metrics collection."""
    print("🚀 Async-Cassandra Metrics Example")
    print("=" * 60)

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
            print("  ✅ Prometheus metrics enabled")
        else:
            print("  ℹ️  Prometheus client not available")
    except Exception:
        print("  ℹ️  Prometheus client not available")

    # Create metrics middleware
    metrics = MetricsMiddleware(collectors)

    # 2. Create cluster and run workload
    async with AsyncCluster(contact_points=["localhost"]) as cluster:
        async with await cluster.connect() as session:
            # Set up test environment
            print("\n📦 Setting up test database...")
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

            # Run workload
            await run_workload(session, metrics)

            # Monitor connections
            await monitor_connections(cluster, metrics)

            # 3. Display collected metrics
            print("\n📈 Metrics Summary")
            print("=" * 60)

            # Get stats from memory collector
            stats = await memory_collector.get_stats()

            if "query_performance" in stats:
                perf = stats["query_performance"]
                if isinstance(perf, dict) and "total_queries" in perf:
                    print("\n📊 Query Performance:")
                    print(f"  Total Queries: {perf['total_queries']}")
                    print(f"  Recent Queries (5min): {perf.get('recent_queries_5min', 0)}")
                    print(f"  Success Rate: {perf.get('success_rate', 0)*100:.1f}%")
                    print(f"  Average Duration: {perf.get('avg_duration_ms', 0):.2f}ms")
                    print(f"  Min Duration: {perf.get('min_duration_ms', 0):.2f}ms")
                    print(f"  Max Duration: {perf.get('max_duration_ms', 0):.2f}ms")
                    print(f"  Queries/Second: {perf.get('queries_per_second', 0):.2f}")

            if "error_summary" in stats and stats["error_summary"]:
                print("\n❌ Error Summary:")
                for error_type, count in stats["error_summary"].items():
                    print(f"  {error_type}: {count}")

            if "top_queries" in stats and stats["top_queries"]:
                print("\n🔥 Top Queries by Frequency:")
                for query_hash, count in list(stats["top_queries"].items())[:5]:
                    print(f"  Query {query_hash}: {count} executions")

            if "connection_health" in stats and stats["connection_health"]:
                print("\n🔗 Connection Health:")
                for host, health in stats["connection_health"].items():
                    status = "UP" if health["healthy"] else "DOWN"
                    print(f"  {host}: {status}")
                    print(f"    Response Time: {health['response_time_ms']:.2f}ms")
                    print(f"    Total Queries: {health.get('total_queries', 0)}")
                    print(f"    Error Count: {health.get('error_count', 0)}")

            # 4. Show optimization tips based on metrics
            print("\n💡 Performance Insights:")
            if "query_performance" in stats and isinstance(stats["query_performance"], dict):
                perf = stats["query_performance"]
                avg_duration = perf.get("avg_duration_ms", 0)
                if avg_duration > 10:
                    print("  ⚠️  Average query duration is high. Consider:")
                    print("     - Using prepared statements")
                    print("     - Adding appropriate indexes")
                    print("     - Reviewing data model")
                elif avg_duration < 1:
                    print("  ✅ Excellent query performance!")
                else:
                    print("  ✅ Good query performance")

                success_rate = perf.get("success_rate", 1)
                if success_rate < 0.95:
                    print(f"  ⚠️  Success rate is {success_rate*100:.1f}%. Check error logs.")

            # Cleanup
            print("\n🧹 Cleaning up...")
            await session.execute("DROP KEYSPACE IF EXISTS metrics_demo")

    print("\n✅ Example complete!")
    print("\nNext steps:")
    print("- Install prometheus_client for production metrics")
    print("- Integrate with your monitoring dashboard")
    print("- Set up alerts based on thresholds")
    print("- Use metrics to optimize slow queries")


if __name__ == "__main__":
    asyncio.run(main())
