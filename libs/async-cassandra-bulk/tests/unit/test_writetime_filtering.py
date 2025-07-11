"""
Unit tests for writetime filtering functionality.

What this tests:
---------------
1. Writetime filter parsing and validation
2. Filter application in export options
3. Both before and after timestamp filtering
4. Edge cases and error handling

Why this matters:
----------------
- Users need to export only recently changed data
- Historical data exports for archiving
- Incremental export capabilities
- Production data management
"""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from async_cassandra_bulk.operators.bulk_operator import BulkOperator


class TestWritetimeFiltering:
    """Test writetime filtering functionality."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures."""
        self.mock_session = AsyncMock()
        self.operator = BulkOperator(session=self.mock_session)

    def test_writetime_filter_parsing(self):
        """
        Test parsing of writetime filter options.

        What this tests:
        ---------------
        1. Various timestamp formats accepted
        2. Before/after filter parsing
        3. Validation of filter values
        4. Error handling for invalid formats

        Why this matters:
        ----------------
        - Users provide timestamps in different formats
        - Clear error messages needed
        - Flexibility in input formats
        - Prevent invalid queries
        """
        # Test cases for filter parsing
        test_cases = [
            # ISO format
            {
                "writetime_after": "2024-01-01T00:00:00Z",
                "expected_micros": 1704067200000000,
            },
            # Unix timestamp (seconds)
            {
                "writetime_after": 1704067200,
                "expected_micros": 1704067200000000,
            },
            # Unix timestamp (milliseconds)
            {
                "writetime_after": 1704067200000,
                "expected_micros": 1704067200000000,
            },
            # Datetime object
            {
                "writetime_after": datetime(2024, 1, 1, tzinfo=timezone.utc),
                "expected_micros": 1704067200000000,
            },
            # Both before and after
            {
                "writetime_after": "2024-01-01T00:00:00Z",
                "writetime_before": "2024-12-31T23:59:59Z",
                "expected_after_micros": 1704067200000000,
                "expected_before_micros": 1735689599000000,
            },
        ]

        for case in test_cases:
            # This will fail until we implement the parsing logic
            options = {k: v for k, v in case.items() if k.startswith("writetime_")}
            parsed = self.operator._parse_writetime_filters(options)

            if "expected_micros" in case:
                assert parsed["writetime_after_micros"] == case["expected_micros"]
            if "expected_after_micros" in case:
                assert parsed["writetime_after_micros"] == case["expected_after_micros"]
            if "expected_before_micros" in case:
                assert parsed["writetime_before_micros"] == case["expected_before_micros"]

    def test_invalid_writetime_filter_formats(self):
        """
        Test error handling for invalid writetime filters.

        What this tests:
        ---------------
        1. Invalid timestamp formats rejected
        2. Logical errors (before < after) caught
        3. Clear error messages provided
        4. No silent failures

        Why this matters:
        ----------------
        - User mistakes happen
        - Clear feedback needed
        - Prevent bad queries
        - Data integrity
        """
        invalid_cases = [
            # Invalid format
            {"writetime_after": "not-a-date"},
            # Before is earlier than after
            {
                "writetime_after": "2024-12-31T00:00:00Z",
                "writetime_before": "2024-01-01T00:00:00Z",
            },
            # Negative timestamp
            {"writetime_after": -1},
        ]

        for case in invalid_cases:
            with pytest.raises((ValueError, TypeError)):
                self.operator._parse_writetime_filters(case)

    @pytest.mark.asyncio
    async def test_export_with_writetime_after_filter(self):
        """
        Test export with writetime_after filter.

        What this tests:
        ---------------
        1. Filter passed to parallel exporter
        2. Correct microsecond conversion
        3. Integration with existing options
        4. No interference with other features

        Why this matters:
        ----------------
        - Common use case for incremental exports
        - Must work with other export options
        - Performance optimization
        - Production reliability
        """
        # Mock the parallel exporter
        with patch(
            "async_cassandra_bulk.operators.bulk_operator.ParallelExporter"
        ) as mock_exporter_class:
            mock_exporter = AsyncMock()
            mock_exporter.export.return_value = MagicMock(
                rows_processed=100,
                duration_seconds=1.0,
                errors=[],
            )
            mock_exporter_class.return_value = mock_exporter

            # Export with writetime_after filter
            await self.operator.export(
                table="test_table",
                output_path="output.csv",
                format="csv",
                options={
                    "writetime_after": "2024-01-01T00:00:00Z",
                    "writetime_columns": ["*"],
                },
            )

            # Verify filter was passed correctly
            mock_exporter_class.assert_called_once()
            call_kwargs = mock_exporter_class.call_args.kwargs
            assert call_kwargs["writetime_after_micros"] == 1704067200000000
            assert call_kwargs["writetime_columns"] == ["*"]

    @pytest.mark.asyncio
    async def test_export_with_writetime_before_filter(self):
        """
        Test export with writetime_before filter.

        What this tests:
        ---------------
        1. Before filter for historical data
        2. Correct filtering logic
        3. Use case for archiving
        4. Boundary conditions

        Why this matters:
        ----------------
        - Archive old data before deletion
        - Historical data analysis
        - Compliance requirements
        - Data lifecycle management
        """
        with patch(
            "async_cassandra_bulk.operators.bulk_operator.ParallelExporter"
        ) as mock_exporter_class:
            mock_exporter = AsyncMock()
            mock_exporter.export.return_value = MagicMock(
                rows_processed=500,
                duration_seconds=2.0,
                errors=[],
            )
            mock_exporter_class.return_value = mock_exporter

            # Export data written before a specific date
            await self.operator.export(
                table="test_table",
                output_path="archive.csv",
                format="csv",
                options={
                    "writetime_before": "2023-01-01T00:00:00Z",
                    "writetime_columns": ["*"],
                },
            )

            # Verify filter was passed
            call_kwargs = mock_exporter_class.call_args.kwargs
            assert call_kwargs["writetime_before_micros"] == 1672531200000000

    @pytest.mark.asyncio
    async def test_export_with_writetime_range_filter(self):
        """
        Test export with both before and after filters.

        What this tests:
        ---------------
        1. Range-based filtering
        2. Both filters work together
        3. Specific time window exports
        4. Complex filtering scenarios

        Why this matters:
        ----------------
        - Export specific time periods
        - Monthly/yearly archives
        - Debugging time-specific issues
        - Compliance reporting
        """
        with patch(
            "async_cassandra_bulk.operators.bulk_operator.ParallelExporter"
        ) as mock_exporter_class:
            mock_exporter = AsyncMock()
            mock_exporter.export.return_value = MagicMock(
                rows_processed=250,
                duration_seconds=1.5,
                errors=[],
            )
            mock_exporter_class.return_value = mock_exporter

            # Export data from a specific month
            await self.operator.export(
                table="test_table",
                output_path="january_2024.csv",
                format="csv",
                options={
                    "writetime_after": "2024-01-01T00:00:00Z",
                    "writetime_before": "2024-01-31T23:59:59Z",
                    "writetime_columns": ["status", "updated_at"],
                },
            )

            # Verify both filters passed
            call_kwargs = mock_exporter_class.call_args.kwargs
            assert call_kwargs["writetime_after_micros"] == 1704067200000000
            assert call_kwargs["writetime_before_micros"] == 1706745599000000

    def test_writetime_filter_with_no_writetime_columns(self):
        """
        Test behavior when filtering without writetime columns.

        What this tests:
        ---------------
        1. Filter requires writetime columns
        2. Clear error message
        3. Validation logic
        4. User guidance

        Why this matters:
        ----------------
        - Prevent confusing behavior
        - Filter needs writetime data
        - Clear requirements
        - Better UX
        """
        with pytest.raises(ValueError) as excinfo:
            self.operator._validate_writetime_options(
                {
                    "writetime_after": "2024-01-01T00:00:00Z",
                    # No writetime_columns specified
                }
            )

        assert "writetime_columns" in str(excinfo.value)
        assert "filter" in str(excinfo.value)
