#!/usr/bin/env python3
"""
Example of real-time data processing with streaming.

This example demonstrates:
- Processing time-series data in real-time
- Aggregating data while streaming
- Handling continuous data ingestion
- Implementing sliding window analytics
"""

import asyncio
import logging
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
                f"ALERT: Sensor {reading.sensor_id} temperature out of range: "
                f"{reading.temperature}°C"
            )

        # Humidity alert
        if reading.humidity > 90.0:
            self.alerts_triggered += 1
            logger.warning(
                f"ALERT: Sensor {reading.sensor_id} high humidity: " f"{reading.humidity}%"
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
    logger.info("Setting up sensor data...")

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

    await session.set_keyspace("iot_data")

    # Create time-series table
    await session.execute(
        """
        CREATE TABLE IF NOT EXISTS sensor_readings (
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
        INSERT INTO sensor_readings (
            date, sensor_id, timestamp, temperature, humidity, pressure
        ) VALUES (?, ?, ?, ?, ?, ?)
    """
    )

    # Generate data for 10 sensors
    sensors = [f"sensor_{i:03d}" for i in range(10)]
    base_time = datetime.now() - timedelta(hours=1)

    logger.info("Inserting sample sensor data...")
    tasks = []

    for i in range(3600):  # One reading per second for an hour
        timestamp = base_time + timedelta(seconds=i)
        date = timestamp.date()

        for sensor_id in sensors:
            # Generate realistic sensor data with some variation
            base_temp = 20.0 + (hash(sensor_id) % 10)
            temperature = base_temp + (i % 60) * 0.1
            humidity = 40.0 + (i % 120) * 0.2
            pressure = 1013.25 + (i % 30) * 0.5

            # Add some anomalies
            if i % 500 == 0 and sensor_id == "sensor_005":
                temperature = 40.0  # High temperature alert
            if i % 700 == 0 and sensor_id == "sensor_007":
                humidity = 95.0  # High humidity alert

            tasks.append(
                session.execute(
                    insert_stmt, [date, sensor_id, timestamp, temperature, humidity, pressure]
                )
            )

        # Execute in batches
        if len(tasks) >= 100:
            await asyncio.gather(*tasks)
            tasks = []

    # Execute remaining tasks
    if tasks:
        await asyncio.gather(*tasks)

    logger.info("Sample data inserted")


async def process_historical_data(session, processor: RealTimeProcessor):
    """Process historical data using streaming."""
    logger.info("\n=== Processing Historical Data ===")

    # Query last hour of data
    one_hour_ago = datetime.now() - timedelta(hours=1)
    today = datetime.now().date()

    # Prepare query for specific date partition
    stmt = await session.prepare(
        """
        SELECT * FROM sensor_readings
        WHERE date = ?
        AND timestamp > ?
    """
    )

    # Configure streaming
    config = StreamConfig(
        fetch_size=1000,
        page_callback=lambda p, t: logger.info(f"Processing page {p} ({t} readings)"),
    )

    # Stream and process data
    start_time = datetime.now()

    # Use context manager for proper resource cleanup
    async with await session.execute_stream(
        stmt, parameters=[today, one_hour_ago], stream_config=config
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
            if readings_processed % 5000 == 0:
                summary = processor.get_summary()
                logger.info(
                    f"Progress: {readings_processed} readings - "
                    f"{summary['active_sensors']} sensors - "
                    f"{summary['alerts_triggered']} alerts"
                )

    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info(f"\nProcessing completed in {elapsed:.2f} seconds")
    logger.info(
        f"Processed {readings_processed} readings "
        f"({readings_processed/elapsed:.0f} readings/sec)"
    )


async def simulate_realtime_processing(session, processor: RealTimeProcessor):
    """Simulate real-time data processing."""
    logger.info("\n=== Simulating Real-Time Processing ===")

    # Prepare query for recent data
    stmt = await session.prepare(
        """
        SELECT * FROM sensor_readings
        WHERE date = ?
        AND sensor_id = ?
        AND timestamp > ?
        LIMIT 10
    """
    )

    sensors = [f"sensor_{i:03d}" for i in range(10)]
    iterations = 10

    for i in range(iterations):
        logger.info(f"\nProcessing cycle {i+1}/{iterations}")

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
        logger.info(f"Current state: {summary}")

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
    # Connect to Cassandra
    cluster = AsyncCluster(["localhost"])

    try:
        session = await cluster.connect()

        # Setup test data
        await setup_sensor_data(session)

        # Create processor with 5-minute sliding window
        processor = RealTimeProcessor(window_minutes=5)

        # Process historical data
        await process_historical_data(session, processor)

        # Show final summary
        summary = processor.get_summary()
        logger.info("\nFinal Summary:")
        logger.info(f"- Active sensors: {summary['active_sensors']}")
        logger.info(f"- Total readings: {summary['total_readings']}")
        logger.info(f"- Alerts triggered: {summary['alerts_triggered']}")
        logger.info(f"- Avg temperature: {summary['avg_temperature']}°C")
        logger.info(f"- Avg humidity: {summary['avg_humidity']}%")

        # Simulate real-time processing
        await simulate_realtime_processing(session, processor)

        # Cleanup
        logger.info("\nCleaning up...")
        await session.execute("DROP KEYSPACE iot_data")

    finally:
        await cluster.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
