#!/usr/bin/env python3
"""
Comprehensive example of metrics collection with async-cassandra.

This example demonstrates:
- Setting up multiple metrics collectors
- Monitoring query performance
- Tracking connection health
- Exporting metrics to Prometheus
- Custom metrics analysis
"""

import asyncio
import uuid
from datetime import datetime

from async_cassandra import AsyncCluster
from async_cassandra.metrics import create_metrics_system


async def main():
    """Demonstrate metrics collection capabilities."""

    # 1. Set up metrics system
    print("🔧 Setting up metrics system...")
    metrics = create_metrics_system(
        backend="memory", prometheus_enabled=False  # Set to True if prometheus_client is installed
    )

    # 2. Create cluster and session with metrics
    cluster = AsyncCluster(contact_points=["localhost"])
    session = await cluster.connect()

    # Inject metrics middleware into session
    session._metrics = metrics

    try:
        # 3. Set up test environment
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

        # 4. Demonstrate metrics collection
        print("\n📊 Running queries with metrics collection...")

        # Fast queries
        for i in range(10):
            user_id = uuid.uuid4()
            await session.execute(
                "INSERT INTO users (id, name, email, created_at) VALUES (?, ?, ?, ?)",
                [user_id, f"User {i}", f"user{i}@example.com", datetime.utcnow()],
            )

        # Some SELECT queries
        for i in range(5):
            await session.execute("SELECT * FROM users LIMIT 10")

        # Simulate a slow query
        print("⏱️  Simulating slow query...")
        await asyncio.sleep(0.1)  # Simulate network delay
        await session.execute("SELECT * FROM users")

        # Simulate an error (this will be caught and recorded)
        try:
            await session.execute("SELECT * FROM non_existent_table")
        except Exception:
            print("❌ Expected error caught and recorded in metrics")

        # 5. Display collected metrics
        print("\n📈 Metrics Summary:")
        print("=" * 50)

        # Get stats from all collectors
        for collector in metrics.collectors:
            stats = await collector.get_stats()

            if "query_performance" in stats:
                perf = stats["query_performance"]
                print(f"Total Queries: {perf['total_queries']}")
                print(f"Recent Queries (5min): {perf['recent_queries_5min']}")
                print(f"Average Duration: {perf['avg_duration_ms']:.2f}ms")
                print(f"Success Rate: {perf['success_rate']:.2%}")
                print(f"Queries/Second: {perf['queries_per_second']:.2f}")

                print("\nPerformance Breakdown:")
                print(f"  Min Duration: {perf['min_duration_ms']:.2f}ms")
                print(f"  Max Duration: {perf['max_duration_ms']:.2f}ms")

            if "error_summary" in stats and stats["error_summary"]:
                print("\n❌ Errors:")
                for error_type, count in stats["error_summary"].items():
                    print(f"  {error_type}: {count}")

            if "top_queries" in stats:
                print("\n🔥 Top Queries:")
                for query_hash, count in list(stats["top_queries"].items())[:5]:
                    print(f"  {query_hash}: {count} executions")

        # 6. Demonstrate connection health monitoring
        print("\n🏥 Connection Health Monitoring:")
        print("=" * 50)

        # Record connection health
        await metrics.record_connection_metrics(
            host="localhost:9042", is_healthy=True, response_time=0.005, total_queries=16  # 5ms
        )

        # Get updated stats
        for collector in metrics.collectors:
            stats = await collector.get_stats()
            if "connection_health" in stats:
                for host, health in stats["connection_health"].items():
                    status = "✅ Healthy" if health["healthy"] else "❌ Unhealthy"
                    print(f"Host {host}: {status}")
                    print(f"  Response Time: {health['response_time_ms']:.2f}ms")
                    print(f"  Total Queries: {health['total_queries']}")
                    print(f"  Errors: {health['error_count']}")

    finally:
        await session.close()
        await cluster.shutdown()


async def prometheus_example():
    """Example showing Prometheus integration."""
    try:
        from prometheus_client import generate_latest, start_http_server

        print("🔧 Setting up Prometheus metrics...")
        metrics = create_metrics_system(backend="memory", prometheus_enabled=True)

        # Start Prometheus HTTP server
        start_http_server(8000)
        print("📊 Prometheus metrics server started on http://localhost:8000")

        # Run some queries with metrics
        cluster = AsyncCluster(contact_points=["localhost"])
        session = await cluster.connect()
        session._metrics = metrics

        # Execute some queries
        for i in range(5):
            await session.execute("SELECT release_version FROM system.local")

        print("📈 Metrics available at http://localhost:8000/metrics")
        print("Sample metrics output:")
        print(generate_latest().decode("utf-8")[:500] + "...")

        await session.close()
        await cluster.shutdown()

    except ImportError:
        print("❌ prometheus_client not installed. Install with: pip install prometheus_client")


def integration_with_fastapi():
    """Example showing FastAPI integration with metrics."""
    example_code = '''
# FastAPI integration example
from fastapi import FastAPI, Depends
from async_cassandra import AsyncCluster
from async_cassandra.metrics import create_metrics_system

app = FastAPI()

# Global metrics system
metrics = create_metrics_system(
    backend="memory",
    prometheus_enabled=True
)

# Global cluster and session
cluster: AsyncCluster = None
session = None

@app.on_event("startup")
async def startup():
    global cluster, session
    cluster = AsyncCluster(contact_points=["localhost"])
    session = await cluster.connect()
    session._metrics = metrics  # Enable metrics

@app.get("/metrics/stats")
async def get_metrics():
    """Get current metrics."""
    stats = {}
    for collector in metrics.collectors:
        collector_stats = await collector.get_stats()
        stats.update(collector_stats)
    return stats

@app.get("/users/{user_id}")
async def get_user(user_id: str):
    # This query will automatically be tracked in metrics
    result = await session.execute(
        "SELECT * FROM users WHERE id = ?",
        [uuid.UUID(user_id)]
    )
    return {"user": result.one()}

# Metrics will be automatically collected for all queries!
'''
    print("🚀 FastAPI Integration Example:")
    print("=" * 50)
    print(example_code)


if __name__ == "__main__":
    print("🎯 Async-Cassandra Metrics & Observability Demo")
    print("=" * 60)

    # Run basic metrics demo
    asyncio.run(main())

    print("\n" + "=" * 60)
    print("🔬 Advanced Examples:")

    # Show Prometheus integration
    asyncio.run(prometheus_example())

    # Show FastAPI integration
    integration_with_fastapi()

    print("\n" + "=" * 60)
    print("✅ Demo completed!")
    print("\nKey Benefits of Metrics/Observability:")
    print("• 📊 Real-time performance monitoring")
    print("• 🚨 Automatic error detection and alerting")
    print("• 🔍 Query performance analysis")
    print("• 📈 Connection health tracking")
    print("• 🎯 Production debugging capabilities")
    print("• 📋 Compliance and audit trails")
