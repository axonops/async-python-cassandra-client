"""
Serializers for special Cassandra values.

Handles conversion of writetime and TTL values to pandas-compatible formats.
"""

from datetime import UTC, datetime

import pandas as pd


class WritetimeSerializer:
    """
    Serializes writetime values from Cassandra.

    Writetime in Cassandra is microseconds since epoch.
    """

    @staticmethod
    def to_timestamp(writetime: int | None) -> pd.Timestamp | None:
        """
        Convert Cassandra writetime to pandas Timestamp.

        Args:
            writetime: Microseconds since epoch (or None)

        Returns:
            pandas Timestamp with UTC timezone
        """
        if writetime is None:
            return None

        # Convert microseconds to seconds
        seconds = writetime / 1_000_000

        # Create timestamp
        dt = datetime.fromtimestamp(seconds, tz=UTC)
        return pd.Timestamp(dt)

    @staticmethod
    def from_timestamp(timestamp: pd.Timestamp | None) -> int | None:
        """
        Convert pandas Timestamp to Cassandra writetime.

        Args:
            timestamp: pandas Timestamp (or None)

        Returns:
            Microseconds since epoch
        """
        if timestamp is None:
            return None

        # Ensure UTC
        if timestamp.tz is None:
            timestamp = timestamp.tz_localize("UTC")
        else:
            timestamp = timestamp.tz_convert("UTC")

        # Convert to microseconds
        return int(timestamp.timestamp() * 1_000_000)


class TTLSerializer:
    """
    Serializes TTL values from Cassandra.

    TTL in Cassandra is seconds remaining until expiry.
    """

    @staticmethod
    def to_seconds(ttl: int | None) -> int | None:
        """
        Convert Cassandra TTL to seconds.

        Args:
            ttl: TTL value from Cassandra

        Returns:
            TTL in seconds (or None if no TTL)
        """
        # TTL is already in seconds, just pass through
        # None means no TTL set
        return ttl

    @staticmethod
    def to_timedelta(ttl: int | None) -> pd.Timedelta | None:
        """
        Convert Cassandra TTL to pandas Timedelta.

        Args:
            ttl: TTL value from Cassandra

        Returns:
            pandas Timedelta (or None if no TTL)
        """
        if ttl is None:
            return None

        return pd.Timedelta(seconds=ttl)

    @staticmethod
    def from_seconds(seconds: int | None) -> int | None:
        """
        Convert seconds to Cassandra TTL.

        Args:
            seconds: TTL in seconds

        Returns:
            TTL value for Cassandra
        """
        if seconds is None or seconds <= 0:
            return None

        return int(seconds)

    @staticmethod
    def from_timedelta(delta: pd.Timedelta | None) -> int | None:
        """
        Convert pandas Timedelta to Cassandra TTL.

        Args:
            delta: pandas Timedelta

        Returns:
            TTL in seconds for Cassandra
        """
        if delta is None:
            return None

        # Convert to seconds
        seconds = int(delta.total_seconds())

        # TTL must be positive
        if seconds <= 0:
            return None

        return seconds
