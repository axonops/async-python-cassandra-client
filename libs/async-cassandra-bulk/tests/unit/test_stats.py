"""
Test statistics tracking for bulk operations.

What this tests:
---------------
1. BulkOperationStats initialization
2. Progress tracking calculations
3. Performance metrics
4. Error tracking

Why this matters:
----------------
- Users need visibility into operation progress
- Performance metrics guide optimization
- Error tracking enables recovery
"""

import time
from unittest.mock import patch

from async_cassandra_bulk.utils.stats import BulkOperationStats


class TestBulkOperationStatsInitialization:
    """Test BulkOperationStats initialization."""

    def test_stats_default_initialization(self):
        """
        Test default initialization values for BulkOperationStats.

        What this tests:
        ---------------
        1. All counters (rows_processed, ranges_completed) start at zero
        2. Start time is set automatically to current time
        3. End time is None (operation not complete)
        4. Error list is initialized as empty list

        Why this matters:
        ----------------
        - Consistent initial state for all operations
        - Accurate duration tracking from instantiation
        - No null pointer errors on error list access
        - Production monitoring depends on accurate timing

        Additional context:
        ---------------------------------
        - Start time uses time.time() for simplicity
        - All fields have dataclass defaults
        - Mutable default (errors list) handled properly
        """
        # Check that start_time is automatically set
        before = time.time()
        stats = BulkOperationStats()
        after = time.time()

        assert stats.rows_processed == 0
        assert stats.ranges_completed == 0
        assert stats.total_ranges == 0
        assert before <= stats.start_time <= after
        assert stats.end_time is None
        assert stats.errors == []

    def test_stats_custom_initialization(self):
        """
        Test BulkOperationStats initialization with custom values.

        What this tests:
        ---------------
        1. Can set initial counter values (rows, ranges)
        2. Custom start time overrides default
        3. All provided values stored correctly
        4. Supports resuming from checkpoint state

        Why this matters:
        ----------------
        - Resume interrupted operations from saved state
        - Testing scenarios with specific conditions
        - Checkpoint restoration requires exact values
        - Production exports may run for hours and need resumption

        Additional context:
        ---------------------------------
        - Used when loading from checkpoint file
        - Start time preserved to calculate total duration
        - Row count accumulates across resumed runs
        """
        stats = BulkOperationStats(
            rows_processed=1000, ranges_completed=5, total_ranges=10, start_time=1234567800.0
        )

        assert stats.rows_processed == 1000
        assert stats.ranges_completed == 5
        assert stats.total_ranges == 10
        assert stats.start_time == 1234567800.0


class TestBulkOperationStatsDuration:
    """Test duration calculation."""

    def test_duration_while_running(self):
        """
        Test duration calculation during active operation.

        What this tests:
        ---------------
        1. Duration uses current time when end_time is None
        2. Calculation updates dynamically as time passes
        3. Returns time.time() - start_time
        4. Accurate to the second

        Why this matters:
        ----------------
        - Real-time progress monitoring in dashboards
        - Accurate ETA calculations for users
        - Live performance metrics during export
        - Production operations need real-time visibility

        Additional context:
        ---------------------------------
        - Uses mock to control time.time() in tests
        - Real implementation calls time.time() each access
        - Property recalculates on every access
        """
        # Create stats with explicit start time
        stats = BulkOperationStats(start_time=100.0)

        # Mock time.time for duration calculation
        with patch("async_cassandra_bulk.utils.stats.time.time") as mock_time:
            # Check duration at t=110
            mock_time.return_value = 110.0
            assert stats.duration_seconds == 10.0

            # Check duration at t=150
            mock_time.return_value = 150.0
            assert stats.duration_seconds == 50.0

    def test_duration_when_complete(self):
        """
        Test duration calculation after operation completes.

        What this tests:
        ---------------
        1. Duration fixed once end_time is set
        2. Uses end_time - start_time calculation
        3. No longer calls time.time()
        4. Value remains constant after completion

        Why this matters:
        ----------------
        - Final statistics must be immutable
        - Historical reporting needs fixed values
        - Performance reports require accurate totals
        - Production metrics stored in monitoring systems

        Additional context:
        ---------------------------------
        - End time set when export finishes or fails
        - Duration used for rows/second calculations
        - Important for billing and capacity planning
        """
        # Create stats with explicit times
        stats = BulkOperationStats(start_time=100.0)
        stats.end_time = 150.0

        # Duration should be fixed even if current time changes
        with patch("async_cassandra_bulk.utils.stats.time.time", return_value=200.0):
            assert stats.duration_seconds == 50.0


class TestBulkOperationStatsMetrics:
    """Test performance metrics calculations."""

    def test_rows_per_second_calculation(self):
        """
        Test throughput calculation in rows per second.

        What this tests:
        ---------------
        1. Calculates rows_processed / duration_seconds
        2. Returns float value for rate
        3. Updates dynamically during operation
        4. Accurate to one decimal place

        Why this matters:
        ----------------
        - Key performance indicator for exports
        - Identifies bottlenecks in processing
        - Guides optimization decisions
        - Production SLAs based on throughput

        Additional context:
        ---------------------------------
        - Typical rates: 10K-100K rows/sec
        - Network and cluster size affect rate
        - Used for capacity planning
        """
        # Create stats with explicit start time
        stats = BulkOperationStats(start_time=100.0)
        stats.rows_processed = 1000

        # Mock current time to be 10 seconds later
        with patch("async_cassandra_bulk.utils.stats.time.time", return_value=110.0):
            assert stats.rows_per_second == 100.0

    def test_rows_per_second_zero_duration(self):
        """
        Test throughput calculation with zero duration edge case.

        What this tests:
        ---------------
        1. No division by zero error when duration is 0
        2. Returns 0 as sensible default
        3. Handles operation start gracefully
        4. Works when start_time equals end_time

        Why this matters:
        ----------------
        - Prevents crashes at operation start
        - UI/monitoring can handle zero values
        - Edge case for very fast operations
        - Production robustness for all scenarios

        Additional context:
        ---------------------------------
        - Can happen in tests or tiny datasets
        - First progress callback may see zero duration
        - Better than returning infinity or NaN
        """
        stats = BulkOperationStats()
        stats.rows_processed = 1000

        # With same start/end time
        stats.end_time = stats.start_time

        assert stats.rows_per_second == 0

    def test_progress_percentage(self):
        """
        Test progress percentage calculation for monitoring.

        What this tests:
        ---------------
        1. Calculates (ranges_completed / total_ranges) * 100
        2. Returns 0.0 to 100.0 range
        3. Updates as ranges complete
        4. Accurate to one decimal place

        Why this matters:
        ----------------
        - User feedback via progress bars
        - Monitoring dashboards show completion
        - ETA calculations based on progress
        - Production visibility for long operations

        Additional context:
        ---------------------------------
        - Based on ranges not rows for accuracy
        - Ranges have similar sizes after splitting
        - More reliable than row-based progress
        """
        stats = BulkOperationStats(total_ranges=10)

        # 0% complete
        assert stats.progress_percentage == 0.0

        # 50% complete
        stats.ranges_completed = 5
        assert stats.progress_percentage == 50.0

        # 100% complete
        stats.ranges_completed = 10
        assert stats.progress_percentage == 100.0

    def test_progress_percentage_zero_ranges(self):
        """
        Test progress percentage with zero total ranges edge case.

        What this tests:
        ---------------
        1. No division by zero when total_ranges is 0
        2. Returns 0.0 as default percentage
        3. Handles empty table scenario
        4. Safe for progress bar rendering

        Why this matters:
        ----------------
        - Empty tables are valid edge case
        - UI components expect valid percentage
        - Prevents crashes in monitoring
        - Production robustness for all data sizes

        Additional context:
        ---------------------------------
        - Empty keyspaces during development
        - Tables cleared between test runs
        - Better than special casing in UI
        """
        stats = BulkOperationStats(total_ranges=0)
        assert stats.progress_percentage == 0.0


class TestBulkOperationStatsCompletion:
    """Test completion tracking."""

    def test_is_complete_check(self):
        """
        Test completion detection based on range progress.

        What this tests:
        ---------------
        1. Returns False when ranges_completed < total_ranges
        2. Returns True when ranges_completed == total_ranges
        3. Updates correctly during operation progress
        4. Works for any number of ranges

        Why this matters:
        ----------------
        - Triggers operation termination
        - Initiates final reporting and cleanup
        - Checkpoint saving on completion
        - Production workflows depend on completion signal

        Additional context:
        ---------------------------------
        - More reliable than row-based completion
        - Ranges are atomic units of work
        - Used by parallel exporter main loop
        """
        stats = BulkOperationStats(total_ranges=3)

        # Not complete
        assert not stats.is_complete

        stats.ranges_completed = 1
        assert not stats.is_complete

        stats.ranges_completed = 2
        assert not stats.is_complete

        # Complete
        stats.ranges_completed = 3
        assert stats.is_complete

    def test_is_complete_with_zero_ranges(self):
        """
        Test completion detection for empty operation.

        What this tests:
        ---------------
        1. Returns True when total_ranges is 0
        2. Logically consistent (0 of 0 is complete)
        3. Handles empty table export scenario
        4. No special casing needed in caller

        Why this matters:
        ----------------
        - Empty tables export successfully
        - No-op operations complete immediately
        - Consistent behavior for automation
        - Production scripts handle all cases

        Additional context:
        ---------------------------------
        - Common in development environments
        - Test cleanup may leave empty tables
        - Export should succeed with empty output
        """
        stats = BulkOperationStats(total_ranges=0, ranges_completed=0)
        assert stats.is_complete


class TestBulkOperationStatsErrors:
    """Test error tracking."""

    def test_error_collection(self):
        """
        Test error list management for failure tracking.

        What this tests:
        ---------------
        1. Errors can be appended to list
        2. List maintains insertion order
        3. Multiple different error types supported
        4. Original exception objects preserved

        Why this matters:
        ----------------
        - Error analysis for troubleshooting
        - Retry strategies based on error types
        - Debugging with full exception details
        - Production monitoring of failure patterns

        Additional context:
        ---------------------------------
        - Errors typically include range information
        - Common: timeouts, node failures, large partitions
        - List can grow large - consider limits
        """
        stats = BulkOperationStats()

        # Add errors
        error1 = Exception("First error")
        error2 = ValueError("Second error")
        error3 = RuntimeError("Third error")

        stats.errors.append(error1)
        stats.errors.append(error2)
        stats.errors.append(error3)

        assert len(stats.errors) == 3
        assert stats.errors[0] is error1
        assert stats.errors[1] is error2
        assert stats.errors[2] is error3

    def test_error_count_tracking(self):
        """
        Test error count property for monitoring.

        What this tests:
        ---------------
        1. error_count property returns len(errors)
        2. Updates as errors are added
        3. Starts at 0 for new stats
        4. Accurate count for any number of errors

        Why this matters:
        ----------------
        - Quality metrics for SLA monitoring
        - Failure threshold triggers (abort if > N)
        - Error rate calculations (errors per range)
        - Production alerting on high error rates

        Additional context:
        ---------------------------------
        - Consider error rate vs absolute count
        - Some errors recoverable (retry)
        - High error rate may indicate cluster issues
        """
        stats = BulkOperationStats()

        # Add method for error count
        assert hasattr(stats, "error_count")
        assert stats.error_count == 0

        stats.errors.append(Exception("Error"))
        assert stats.error_count == 1


class TestBulkOperationStatsFormatting:
    """Test stats display formatting."""

    def test_stats_summary_string(self):
        """
        Test human-readable summary string generation.

        What this tests:
        ---------------
        1. summary() method returns formatted string
        2. Includes rows processed, progress %, rate, duration
        3. Formats numbers for readability
        4. Uses consistent units (rows/sec, seconds)

        Why this matters:
        ----------------
        - User feedback in CLI output
        - Structured logging for operations
        - Progress reporting to users
        - Production operation summaries

        Additional context:
        ---------------------------------
        - Example: "Processed 1000 rows (50.0%) at 100.0 rows/sec in 10.0 seconds"
        - Used in final export report
        - May be parsed by monitoring tools
        """
        stats = BulkOperationStats(
            rows_processed=1000, ranges_completed=5, total_ranges=10, start_time=100.0
        )

        # Mock current time for duration calculation
        with patch("async_cassandra_bulk.utils.stats.time.time", return_value=110.0):
            summary = stats.summary()

        assert "1000 rows" in summary
        assert "50.0%" in summary
        assert "100.0 rows/sec" in summary
        assert "10.0 seconds" in summary

    def test_stats_as_dict(self):
        """
        Test dictionary representation for serialization.

        What this tests:
        ---------------
        1. as_dict() method returns all stat fields
        2. Includes calculated properties (duration, rate, %)
        3. Dictionary is JSON-serializable
        4. All numeric values included

        Why this matters:
        ----------------
        - JSON export to monitoring systems
        - Checkpoint file serialization
        - API responses with statistics
        - Production metrics collection

        Additional context:
        ---------------------------------
        - Used for checkpoint save/restore
        - Sent to time-series databases
        - May include error count in future
        """
        stats = BulkOperationStats(rows_processed=1000, ranges_completed=5, total_ranges=10)

        data = stats.as_dict()

        assert data["rows_processed"] == 1000
        assert data["ranges_completed"] == 5
        assert data["total_ranges"] == 10
        assert "duration_seconds" in data
        assert "rows_per_second" in data
        assert "progress_percentage" in data
