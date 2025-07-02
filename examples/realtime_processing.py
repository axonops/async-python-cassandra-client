#!/usr/bin/env python3
"""
Example of real-time data processing with streaming.

This example demonstrates:
- Processing time-series data in real-time
- Aggregating data while streaming
- Handling continuous data ingestion
- Implementing sliding window analytics

How to run:
-----------
1. Using Make (automatically starts Cassandra if needed):
   make example-realtime

2. With external Cassandra cluster:
   CASSANDRA_CONTACT_POINTS=10.0.0.1,10.0.0.2 make example-realtime

3. Direct Python execution:
   python examples/realtime_processing.py

4. With custom contact points:
   CASSANDRA_CONTACT_POINTS=cassandra.example.com python examples/realtime_processing.py

Environment variables:
- CASSANDRA_CONTACT_POINTS: Comma-separated list of contact points (default: localhost)
- CASSANDRA_PORT: Port number (default: 9042)
"""

import asyncio
import logging
import os
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict

from async_cassandra import AsyncCluster, StreamConfig

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class SensorReading:
    """Represents a sensor reading."""

    sensor_id: str
    timestamp: datetime
    temperature: float
    humidity: float
    pressure: float


@dataclass
class SensorStats:
    """Statistics for a sensor."""

    sensor_id: str
    avg_temperature: float
    avg_humidity: float
    avg_pressure: float
    min_temperature: float
    max_temperature: float
    reading_count: int
    last_updated: datetime


class RealTimeProcessor:
    """Process sensor data in real-time with sliding window analytics."""

    def __init__(self, window_minutes: int = 5):
        self.window_minutes = window_minutes
        self.sensor_windows: Dict[str, deque] = defaultdict(
            lambda: deque(maxlen=1000)  # Keep last 1000 readings per sensor
        )
        self.sensor_stats: Dict[str, SensorStats] = {}
        self.alerts_triggered = 0

    def process_reading(self, reading: SensorReading):
        """Process a single sensor reading."""
        # Add to sliding window
        window = self.sensor_windows[reading.sensor_id]
        window.append(reading)

        # Remove old readings outside the window
        cutoff_time = datetime.now() - timedelta(minutes=self.window_minutes)
        while window and window[0].timestamp < cutoff_time:
            window.popleft()

        # Update statistics
        if window:
            temps = [r.temperature for r in window]
            humidities = [r.humidity for r in window]
            pressures = [r.pressure for r in window]

            self.sensor_stats[reading.sensor_id] = SensorStats(
                sensor_id=reading.sensor_id,
                avg_temperature=sum(temps) / len(temps),
                avg_humidity=sum(humidities) / len(humidities),
                avg_pressure=sum(pressures) / len(pressures),
                min_temperature=min(temps),
                max_temperature=max(temps),
                reading_count=len(window),
                last_updated=datetime.now(),
            )

        # Check for alerts
        self._check_alerts(reading)

    def _check_alerts(self, reading: SensorReading):
        """Check if reading triggers any alerts."""
        # Temperature alert
        if reading.temperature > 35.0 or reading.temperature < -10.0:
            self.alerts_triggered += 1
            logger.warning(
                f"🚨 ALERT: Sensor {reading.sensor_id} temperature out of range: "
                f"{reading.temperature}°C"
            )

        # Humidity alert
        if reading.humidity > 90.0:
            self.alerts_triggered += 1
            logger.warning(
                f"🚨 ALERT: Sensor {reading.sensor_id} high humidity: {reading.humidity}%"
            )

    def get_summary(self) -> Dict:
        """Get current processing summary."""
        active_sensors = len(self.sensor_stats)
        total_readings = sum(s.reading_count for s in self.sensor_stats.values())

        if self.sensor_stats:
            avg_temp = sum(s.avg_temperature for s in self.sensor_stats.values()) / active_sensors
            avg_humidity = sum(s.avg_humidity for s in self.sensor_stats.values()) / active_sensors
        else:
            avg_temp = avg_humidity = 0

        return {
            "active_sensors": active_sensors,
            "total_readings": total_readings,
            "alerts_triggered": self.alerts_triggered,
            "avg_temperature": round(avg_temp, 2),
            "avg_humidity": round(avg_humidity, 2),
            "window_minutes": self.window_minutes,
        }


async def setup_sensor_data(session):
    """Create sensor data table and insert sample data."""
    logger.info("\n🛠️  Setting up IoT sensor data simulation...")

    # Create keyspace
    await session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS iot_data
        WITH REPLICATION = {
            'class': 'SimpleStrategy',
            'replication_factor': 1
        }
    """
    )

    # Create time-series table
    await session.execute(
        """
        CREATE TABLE IF NOT EXISTS iot_data.sensor_readings (
            date date,
            sensor_id text,
            timestamp timestamp,
            temperature double,
            humidity double,
            pressure double,
            PRIMARY KEY ((date, sensor_id), timestamp)
        ) WITH CLUSTERING ORDER BY (timestamp DESC)
    """
    )

    # Insert sample data for the last hour
    insert_stmt = await session.prepare(
        """
        INSERT INTO iot_data.sensor_readings (
            date, sensor_id, timestamp, temperature, humidity, pressure
        ) VALUES (?, ?, ?, ?, ?, ?)
    """
    )

    # Generate data for 50 sensors for more realistic volume
    sensors = [f"sensor_{i:03d}" for i in range(50)]
    base_time = datetime.now() - timedelta(hours=6)  # 6 hours of data

    logger.info(f"📡 Generating data for {len(sensors)} sensors over 6 hours...")
    logger.info("🌡️  Data includes temperature, humidity, and pressure readings")
    tasks = []
    total_readings = 0

    # Insert data at 10-second intervals (more realistic for IoT)
    for i in range(0, 21600, 10):  # 6 hours * 3600 seconds / 10 = 2160 time points
        timestamp = base_time + timedelta(seconds=i)
        date = timestamp.date()

        for sensor_id in sensors:
            # Generate realistic sensor data with some variation
            base_temp = 20.0 + (hash(sensor_id) % 10)
            temperature = base_temp + (i % 600) * 0.01 + (hash(f"{sensor_id}{i}") % 100) * 0.01
            humidity = 40.0 + (i % 1200) * 0.02 + (hash(f"{sensor_id}{i}h") % 100) * 0.01
            pressure = 1013.25 + (i % 300) * 0.05 + (hash(f"{sensor_id}{i}p") % 100) * 0.01

            # Add some anomalies
            if i % 5000 == 0 and sensor_id == "sensor_005":
                temperature = 40.0  # High temperature alert
            if i % 7000 == 0 and sensor_id == "sensor_007":
                humidity = 95.0  # High humidity alert
            if i % 9000 == 0 and sensor_id == "sensor_015":
                temperature = -5.0  # Low temperature alert

            tasks.append(
                session.execute(
                    insert_stmt, [date, sensor_id, timestamp, temperature, humidity, pressure]
                )
            )
            total_readings += 1

        # Execute in batches
        if len(tasks) >= 500:
            await asyncio.gather(*tasks)
            tasks = []
            if total_readings % 10000 == 0:
                logger.info(f"   📊 Progress: {total_readings:,} readings inserted...")

    # Execute remaining tasks
    if tasks:
        await asyncio.gather(*tasks)

    logger.info(f"✅ Sample data setup complete: {total_readings:,} sensor readings created")
    logger.info(f"   • {len(sensors)} sensors")
    logger.info("   • 6 hours of historical data")
    logger.info("   • 10-second intervals")
    logger.info("   • Includes simulated anomalies for alert testing")


async def process_historical_data(session, processor: RealTimeProcessor):
    """Process historical data using streaming."""
    logger.info("\n" + "=" * 80)
    logger.info("📈 PROCESSING HISTORICAL DATA WITH STREAMING")
    logger.info("=" * 80)

    # Query last 6 hours of data
    six_hours_ago = datetime.now() - timedelta(hours=6)
    today = datetime.now().date()

    # Prepare query for specific date partition
    stmt = await session.prepare(
        """
        SELECT * FROM iot_data.sensor_readings
        WHERE date = ?
        AND timestamp > ?
        ALLOW FILTERING
    """
    )

    # Configure streaming with appropriate page size for True Async Paging
    config = StreamConfig(
        fetch_size=5000,  # Process 5000 rows per page
        page_callback=lambda p, t: logger.info(f"📄 Processing page {p} ({t:,} readings so far)"),
    )

    # Stream and process data
    start_time = datetime.now()

    # Use context manager for proper resource cleanup
    async with await session.execute_stream(
        stmt, parameters=[today, six_hours_ago], stream_config=config
    ) as result:
        readings_processed = 0
        async for row in result:
            reading = SensorReading(
                sensor_id=row.sensor_id,
                timestamp=row.timestamp,
                temperature=row.temperature,
                humidity=row.humidity,
                pressure=row.pressure,
            )
            processor.process_reading(reading)
            readings_processed += 1

            # Log progress periodically
            if readings_processed % 10000 == 0:
                summary = processor.get_summary()
                logger.info(
                    f"📊 Progress: {readings_processed:,} readings • "
                    f"{summary['active_sensors']} sensors • "
                    f"{summary['alerts_triggered']} alerts triggered"
                )

    elapsed = (datetime.now() - start_time).total_seconds()

    logger.info("\n" + "─" * 80)
    logger.info("✅ HISTORICAL DATA PROCESSING COMPLETE")
    logger.info("─" * 80)
    logger.info("\n📊 Processing Statistics:")
    logger.info(f"   • Total readings: {readings_processed:,}")
    logger.info(f"   • Duration: {elapsed:.2f} seconds")
    logger.info(f"   • Throughput: {readings_processed/elapsed:,.0f} readings/sec")
    logger.info(f"   • Active sensors: {processor.get_summary()['active_sensors']}")
    logger.info(f"   • Alerts triggered: {processor.alerts_triggered}")


async def process_data_in_pages(session):
    """Demonstrate True Async Paging for batch processing."""
    logger.info("\n" + "=" * 80)
    logger.info("📦 TRUE ASYNC PAGING - BATCH PROCESSING DEMONSTRATION")
    logger.info("=" * 80)

    # Query all data for batch processing
    stmt = await session.prepare(
        """
        SELECT * FROM iot_data.sensor_readings
        WHERE date = ?
        ALLOW FILTERING
    """
    )

    today = datetime.now().date()
    config = StreamConfig(fetch_size=10000)  # 10k rows per page

    page_count = 0
    total_readings = 0
    sensor_data = defaultdict(list)

    logger.info("\n🔄 Processing sensor data in pages...")
    logger.info("💡 Note: Each page is fetched ONLY when needed (True Async Paging)")

    # Use True Async Paging to process large dataset efficiently
    async with await session.execute_stream(
        stmt, parameters=[today], stream_config=config
    ) as result:
        async for page in result.pages():
            page_count += 1
            readings_in_page = len(page)
            total_readings += readings_in_page

            logger.info(f"\n📄 Page {page_count}: {readings_in_page:,} readings")

            # Process each page (e.g., aggregate by sensor)
            for row in page:
                sensor_data[row.sensor_id].append(
                    {
                        "timestamp": row.timestamp,
                        "temperature": row.temperature,
                        "humidity": row.humidity,
                        "pressure": row.pressure,
                    }
                )

            # Simulate page processing time (e.g., writing to data warehouse)
            await asyncio.sleep(0.1)

            # Log memory-efficient processing
            if page_count % 5 == 0:
                logger.info(
                    f"   ✓ Progress: {total_readings:,} total readings • {len(sensor_data)} sensors"
                )

    logger.info("\n" + "─" * 80)
    logger.info("✅ PAGE-BASED PROCESSING COMPLETE")
    logger.info("─" * 80)
    logger.info("\n📊 Results:")
    logger.info(f"   • Total pages: {page_count}")
    logger.info(f"   • Total readings: {total_readings:,}")
    logger.info(f"   • Unique sensors: {len(sensor_data)}")
    logger.info("\n💡 Key benefit: Memory usage remains constant regardless of dataset size!")
    logger.info("   Pages are fetched on-demand as you process them.")


async def simulate_realtime_processing(session, processor: RealTimeProcessor):
    """Simulate real-time data processing."""
    logger.info("\n" + "=" * 80)
    logger.info("🔴 SIMULATING REAL-TIME PROCESSING")
    logger.info("=" * 80)

    # Prepare query for recent data
    stmt = await session.prepare(
        """
        SELECT * FROM iot_data.sensor_readings
        WHERE date = ?
        AND sensor_id = ?
        AND timestamp > ?
        LIMIT 10
        ALLOW FILTERING
    """
    )

    sensors = [f"sensor_{i:03d}" for i in range(10)]
    iterations = 10

    for i in range(iterations):
        logger.info(f"\n🔄 Processing cycle {i+1}/{iterations}")

        # Query recent data for each sensor
        cutoff_time = datetime.now() - timedelta(minutes=processor.window_minutes)
        today = datetime.now().date()

        for sensor_id in sensors:
            # Use context manager to ensure proper cleanup
            async with await session.execute_stream(
                stmt,
                parameters=[today, sensor_id, cutoff_time],
                stream_config=StreamConfig(fetch_size=10),
            ) as result:
                async for row in result:
                    reading = SensorReading(
                        sensor_id=row.sensor_id,
                        timestamp=row.timestamp,
                        temperature=row.temperature,
                        humidity=row.humidity,
                        pressure=row.pressure,
                    )
                    processor.process_reading(reading)

        # Show current statistics
        summary = processor.get_summary()
        logger.info(
            f"📊 Current state: {summary['active_sensors']} sensors • "
            f"{summary['total_readings']} readings • "
            f"{summary['alerts_triggered']} alerts"
        )

        # Show sensor details
        for sensor_id, stats in processor.sensor_stats.items():
            if stats.reading_count > 0:
                logger.debug(
                    f"  {sensor_id}: "
                    f"temp={stats.avg_temperature:.1f}°C "
                    f"({stats.min_temperature:.1f}-{stats.max_temperature:.1f}), "
                    f"humidity={stats.avg_humidity:.1f}%, "
                    f"readings={stats.reading_count}"
                )

        # Simulate delay between processing cycles
        await asyncio.sleep(2)


async def main():
    """Run real-time processing example."""
    # Get contact points from environment or use localhost
    contact_points = os.environ.get("CASSANDRA_CONTACT_POINTS", "localhost").split(",")
    port = int(os.environ.get("CASSANDRA_PORT", "9042"))

    logger.info(f"Connecting to Cassandra at {contact_points}:{port}")

    # Connect to Cassandra using context manager
    async with AsyncCluster(contact_points, port=port) as cluster:
        async with await cluster.connect() as session:
            # Setup test data
            await setup_sensor_data(session)

            # Create processor with 5-minute sliding window
            processor = RealTimeProcessor(window_minutes=5)

            # Process historical data
            await process_historical_data(session, processor)

            # Demonstrate True Async Paging
            await process_data_in_pages(session)

            # Show final summary
            summary = processor.get_summary()
            logger.info("\n" + "=" * 80)
            logger.info("📊 FINAL ANALYTICS SUMMARY")
            logger.info("=" * 80)
            logger.info("\n🌡️  Environmental Metrics:")
            logger.info(f"   • Active sensors: {summary['active_sensors']}")
            logger.info(f"   • Total readings processed: {summary['total_readings']:,}")
            logger.info(f"   • Average temperature: {summary['avg_temperature']}°C")
            logger.info(f"   • Average humidity: {summary['avg_humidity']}%")
            logger.info("\n🚨 Alert Summary:")
            logger.info(f"   • Total alerts triggered: {summary['alerts_triggered']}")
            logger.info("   • Alert types: Temperature anomalies, High humidity")

            # Simulate real-time processing
            await simulate_realtime_processing(session, processor)

            # Cleanup
            logger.info("\n🧹 Cleaning up...")
            await session.execute("DROP KEYSPACE iot_data")
            logger.info("✅ Keyspace dropped")


if __name__ == "__main__":
    asyncio.run(main())
