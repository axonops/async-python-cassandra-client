"""Statistics tracking for bulk operations."""

import time
from dataclasses import dataclass, field


@dataclass
class BulkOperationStats:
    """Statistics for bulk operations."""

    rows_processed: int = 0
    ranges_completed: int = 0
    total_ranges: int = 0
    start_time: float = field(default_factory=time.time)
    end_time: float | None = None
    errors: list[Exception] = field(default_factory=list)

    @property
    def duration_seconds(self) -> float:
        """Calculate operation duration."""
        if self.end_time:
            return self.end_time - self.start_time
        return time.time() - self.start_time

    @property
    def rows_per_second(self) -> float:
        """Calculate processing rate."""
        duration = self.duration_seconds
        if duration > 0:
            return self.rows_processed / duration
        return 0

    @property
    def progress_percentage(self) -> float:
        """Calculate progress as percentage."""
        if self.total_ranges > 0:
            return (self.ranges_completed / self.total_ranges) * 100
        return 0

    @property
    def is_complete(self) -> bool:
        """Check if operation is complete."""
        return self.ranges_completed == self.total_ranges
