"""
Statistics tracking for bulk operations.

Provides comprehensive metrics and progress tracking for bulk operations
including throughput, completion status, and error tracking.
"""

import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class BulkOperationStats:
    """
    Statistics tracker for bulk operations.

    Tracks progress, performance metrics, and errors during bulk operations
    on Cassandra tables. Supports checkpointing and resumption.
    """

    rows_processed: int = 0
    ranges_completed: int = 0
    total_ranges: int = 0
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    errors: List[Exception] = field(default_factory=list)

    @property
    def duration_seconds(self) -> float:
        """
        Calculate operation duration in seconds.

        Uses end_time if operation is complete, otherwise current time.
        """
        if self.end_time:
            return self.end_time - self.start_time
        return time.time() - self.start_time

    @property
    def rows_per_second(self) -> float:
        """
        Calculate processing throughput.

        Returns 0 if duration is zero to avoid division errors.
        """
        duration = self.duration_seconds
        if duration > 0:
            return self.rows_processed / duration
        return 0

    @property
    def progress_percentage(self) -> float:
        """
        Calculate completion percentage.

        Based on ranges completed vs total ranges.
        """
        if self.total_ranges > 0:
            return (self.ranges_completed / self.total_ranges) * 100
        return 0.0

    @property
    def is_complete(self) -> bool:
        """Check if operation has completed all ranges."""
        return self.ranges_completed == self.total_ranges

    @property
    def error_count(self) -> int:
        """Get total number of errors encountered."""
        return len(self.errors)

    def summary(self) -> str:
        """
        Generate human-readable summary of statistics.

        Returns:
            Formatted string with key metrics
        """
        parts = [
            f"Processed {self.rows_processed} rows",
            f"Progress: {self.progress_percentage:.1f}% ({self.ranges_completed}/{self.total_ranges} ranges)",
            f"Rate: {self.rows_per_second:.1f} rows/sec",
            f"Duration: {self.duration_seconds:.1f} seconds",
        ]

        if self.error_count > 0:
            parts.append(f"Errors: {self.error_count}")

        return " | ".join(parts)

    def as_dict(self) -> Dict[str, Any]:
        """
        Export statistics as dictionary.

        Useful for JSON serialization, logging, or checkpointing.

        Returns:
            Dictionary containing all statistics
        """
        return {
            "rows_processed": self.rows_processed,
            "ranges_completed": self.ranges_completed,
            "total_ranges": self.total_ranges,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_seconds": self.duration_seconds,
            "rows_per_second": self.rows_per_second,
            "progress_percentage": self.progress_percentage,
            "error_count": self.error_count,
            "is_complete": self.is_complete,
        }
