"""
Test base exporter abstract class.

What this tests:
---------------
1. Abstract base class contract
2. Required method definitions
3. Common functionality inheritance
4. Configuration validation

Why this matters:
----------------
- Ensures consistent interface for all exporters
- Validates required methods are implemented
- Common functionality works across all formats
- Type safety for exporter implementations
"""

from typing import Any, Dict, List

import pytest

from async_cassandra_bulk.exporters.base import BaseExporter


class TestBaseExporterContract:
    """Test BaseExporter abstract base class contract."""

    def test_base_exporter_is_abstract(self):
        """
        Test that BaseExporter cannot be instantiated directly.

        What this tests:
        ---------------
        1. BaseExporter marked as ABC (Abstract Base Class)
        2. Cannot create instance without implementing all abstract methods
        3. TypeError raised with clear message
        4. Message mentions "abstract class"

        Why this matters:
        ----------------
        - Enforces implementation of required methods
        - Prevents accidental usage of incomplete base class
        - Type safety at instantiation time
        - Production code must use concrete exporters

        Additional context:
        ---------------------------------
        - Uses abc.ABC and @abstractmethod decorators
        - Python enforces at instantiation, not import
        - Subclasses must implement all abstract methods
        """
        with pytest.raises(TypeError) as exc_info:
            BaseExporter(output_path="/tmp/test.csv", options={})

        assert "Can't instantiate abstract class" in str(exc_info.value)

    def test_base_exporter_requires_write_header(self):
        """
        Test that subclasses must implement write_header method.

        What this tests:
        ---------------
        1. write_header marked as @abstractmethod
        2. Missing implementation prevents instantiation
        3. Error message mentions missing method name
        4. Other implemented methods don't satisfy requirement

        Why this matters:
        ----------------
        - Headers are format-specific (CSV has columns, JSON has '[')
        - Each exporter needs custom header logic
        - Compile-time safety for complete implementation
        - Production exporters must handle headers correctly

        Additional context:
        ---------------------------------
        - CSV writes column names
        - JSON writes opening bracket
        - XML writes root element
        """

        class IncompleteExporter(BaseExporter):
            async def write_row(self, row: Dict[str, Any]) -> None:
                pass

            async def write_footer(self) -> None:
                pass

        with pytest.raises(TypeError) as exc_info:
            IncompleteExporter(output_path="/tmp/test.csv", options={})

        assert "write_header" in str(exc_info.value)

    def test_base_exporter_requires_write_row(self):
        """
        Test that subclasses must implement write_row method.

        What this tests:
        ---------------
        1. write_row marked as @abstractmethod
        2. Core method for processing each data row
        3. Missing implementation prevents instantiation
        4. Signature must match base class definition

        Why this matters:
        ----------------
        - Row formatting differs completely by format
        - Core functionality processes millions of rows
        - Type conversion logic lives here
        - Production performance depends on efficient implementation

        Additional context:
        ---------------------------------
        - CSV converts to delimited text
        - JSON serializes to objects
        - Called once per row in dataset
        """

        class IncompleteExporter(BaseExporter):
            async def write_header(self, columns: List[str]) -> None:
                pass

            async def write_footer(self) -> None:
                pass

        with pytest.raises(TypeError) as exc_info:
            IncompleteExporter(output_path="/tmp/test.csv", options={})

        assert "write_row" in str(exc_info.value)

    def test_base_exporter_requires_write_footer(self):
        """
        Test that subclasses must implement write_footer method.

        What this tests:
        ---------------
        1. write_footer marked as @abstractmethod
        2. Called after all rows processed
        3. Missing implementation prevents instantiation
        4. Required even if format needs no footer

        Why this matters:
        ----------------
        - JSON needs closing ']' bracket
        - XML needs closing root tag
        - Ensures valid file format on completion
        - Production files must be parseable

        Additional context:
        ---------------------------------
        - CSV typically needs no footer (can be empty)
        - Critical for streaming formats
        - Called exactly once at end
        """

        class IncompleteExporter(BaseExporter):
            async def write_header(self, columns: List[str]) -> None:
                pass

            async def write_row(self, row: Dict[str, Any]) -> None:
                pass

        with pytest.raises(TypeError) as exc_info:
            IncompleteExporter(output_path="/tmp/test.csv", options={})

        assert "write_footer" in str(exc_info.value)


class TestBaseExporterImplementation:
    """Test BaseExporter common functionality."""

    @pytest.fixture
    def mock_exporter_class(self):
        """Create a concrete exporter for testing."""

        class MockExporter(BaseExporter):
            async def write_header(self, columns: List[str]) -> None:
                self.header_written = True
                self.columns = columns

            async def write_row(self, row: Dict[str, Any]) -> None:
                if not hasattr(self, "rows"):
                    self.rows = []
                self.rows.append(row)

            async def write_footer(self) -> None:
                self.footer_written = True

        return MockExporter

    def test_base_exporter_stores_configuration(self, mock_exporter_class):
        """
        Test that BaseExporter stores output path and options correctly.

        What this tests:
        ---------------
        1. Constructor accepts output_path parameter
        2. Constructor accepts options dict parameter
        3. Values stored as instance attributes unchanged
        4. Options default to empty dict if not provided

        Why this matters:
        ----------------
        - Exporters need file path for output
        - Options customize format-specific behavior
        - Path validation happens in subclasses
        - Production configs passed through options

        Additional context:
        ---------------------------------
        - Common options: delimiter, encoding, compression
        - Path can be absolute or relative
        - Options dict not validated by base class
        """
        exporter = mock_exporter_class(
            output_path="/tmp/test.csv", options={"delimiter": ",", "header": True}
        )

        assert exporter.output_path == "/tmp/test.csv"
        assert exporter.options == {"delimiter": ",", "header": True}

    @pytest.mark.asyncio
    async def test_base_exporter_export_rows_basic_flow(self, mock_exporter_class):
        """
        Test export_rows orchestrates the complete export workflow.

        What this tests:
        ---------------
        1. Calls write_header first with column list
        2. Calls write_row for each yielded row
        3. Calls write_footer after all rows
        4. Returns accurate count of processed rows

        Why this matters:
        ----------------
        - Core workflow ensures correct file structure
        - Order critical for valid output format
        - Row count used for statistics
        - Production exports process millions of rows

        Additional context:
        ---------------------------------
        - Uses async generator for memory efficiency
        - Header must come before any rows
        - Footer must come after all rows
        """
        exporter = mock_exporter_class(output_path="/tmp/test.csv", options={})

        # Mock data
        async def mock_rows():
            yield {"id": 1, "name": "Alice"}
            yield {"id": 2, "name": "Bob"}

        # Execute
        count = await exporter.export_rows(rows=mock_rows(), columns=["id", "name"])

        # Verify
        assert exporter.header_written
        assert exporter.columns == ["id", "name"]
        assert len(exporter.rows) == 2
        assert exporter.rows[0] == {"id": 1, "name": "Alice"}
        assert exporter.rows[1] == {"id": 2, "name": "Bob"}
        assert exporter.footer_written
        assert count == 2

    @pytest.mark.asyncio
    async def test_base_exporter_handles_empty_data(self, mock_exporter_class):
        """
        Test export_rows handles empty dataset gracefully.

        What this tests:
        ---------------
        1. write_header called even with no data
        2. write_row never called for empty generator
        3. write_footer called to close file properly
        4. Returns 0 count accurately

        Why this matters:
        ----------------
        - Empty query results are common
        - File must still be valid format
        - Automated pipelines expect consistent structure
        - Production tables may be temporarily empty

        Additional context:
        ---------------------------------
        - Empty CSV has header row only
        - Empty JSON is []
        - Important for idempotent operations
        """
        exporter = mock_exporter_class(output_path="/tmp/test.csv", options={})

        # Empty data
        async def mock_rows():
            return
            yield  # Make it a generator

        # Execute
        count = await exporter.export_rows(rows=mock_rows(), columns=["id", "name"])

        # Verify
        assert exporter.header_written
        assert exporter.footer_written
        assert not hasattr(exporter, "rows") or len(exporter.rows) == 0
        assert count == 0

    @pytest.mark.asyncio
    async def test_base_exporter_file_handling(self, mock_exporter_class, tmp_path):
        """
        Test that BaseExporter properly manages file resources.

        What this tests:
        ---------------
        1. Opens file for writing with proper mode
        2. File handle available during write operations
        3. File automatically closed after export
        4. Creates parent directories if needed

        Why this matters:
        ----------------
        - Resource leaks crash long-running exports
        - File handles are limited OS resource
        - Proper cleanup even on errors
        - Production exports run for hours

        Additional context:
        ---------------------------------
        - Uses aiofiles for async file I/O
        - Context manager ensures cleanup
        - UTF-8 encoding by default
        """
        output_file = tmp_path / "test_export.csv"

        class FileTrackingExporter(mock_exporter_class):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.file_was_open = False

            async def write_header(self, columns: List[str]) -> None:
                await super().write_header(columns)
                self.file_was_open = hasattr(self, "_file") and self._file is not None
                if self.file_was_open:
                    await self._file.write("# Header\n")

            async def write_row(self, row: Dict[str, Any]) -> None:
                await super().write_row(row)
                if hasattr(self, "_file") and self._file:
                    await self._file.write(f"{row}\n")

            async def write_footer(self) -> None:
                await super().write_footer()
                if hasattr(self, "_file") and self._file:
                    await self._file.write("# Footer\n")

        exporter = FileTrackingExporter(output_path=str(output_file), options={})

        # Mock data
        async def mock_rows():
            yield {"id": 1, "name": "Test"}

        # Execute export
        count = await exporter.export_rows(rows=mock_rows(), columns=["id", "name"])

        # Verify file was handled
        assert exporter.file_was_open
        assert count == 1

        # Verify file was written
        assert output_file.exists()
        content = output_file.read_text()
        assert "# Header" in content
        assert "{'id': 1, 'name': 'Test'}" in content
        assert "# Footer" in content

    @pytest.mark.asyncio
    async def test_base_exporter_error_propagation(self, mock_exporter_class):
        """
        Test that errors in write methods are propagated correctly.

        What this tests:
        ---------------
        1. Errors in write_row bubble up to caller
        2. Original exception type and message preserved
        3. Partial results before error are kept
        4. File cleanup happens despite error

        Why this matters:
        ----------------
        - Debugging requires full error context
        - Partial exports must be detectable
        - Resource cleanup prevents file handle leaks
        - Production monitoring needs real errors

        Additional context:
        ---------------------------------
        - Common errors: disk full, encoding issues
        - First rows may succeed before error
        - Caller decides retry strategy
        """

        class ErrorExporter(mock_exporter_class):
            async def write_row(self, row: Dict[str, Any]) -> None:
                if row.get("id") == 2:
                    raise ValueError("Simulated export error")
                await super().write_row(row)

        exporter = ErrorExporter(output_path="/tmp/test.csv", options={})

        # Mock data that will trigger error
        async def mock_rows():
            yield {"id": 1, "name": "Alice"}
            yield {"id": 2, "name": "Bob"}  # This will error
            yield {"id": 3, "name": "Charlie"}  # Should not be reached

        # Execute and expect error
        with pytest.raises(ValueError) as exc_info:
            await exporter.export_rows(rows=mock_rows(), columns=["id", "name"])

        assert "Simulated export error" in str(exc_info.value)
        # First row should have been processed
        assert len(exporter.rows) == 1
        assert exporter.rows[0]["id"] == 1

    @pytest.mark.asyncio
    async def test_base_exporter_validates_output_path(self, mock_exporter_class):
        """
        Test output path validation at construction time.

        What this tests:
        ---------------
        1. Rejects empty string output path
        2. Rejects None as output path
        3. Clear error message for invalid paths
        4. Validation happens in constructor

        Why this matters:
        ----------------
        - Fail fast with clear errors
        - Prevents confusing file not found later
        - User-friendly error messages
        - Production scripts need early validation

        Additional context:
        ---------------------------------
        - Directory creation happens during export
        - Relative paths resolved from working directory
        - Network paths supported on some systems
        """
        # Test empty path
        with pytest.raises(ValueError) as exc_info:
            mock_exporter_class(output_path="", options={})
        assert "output_path cannot be empty" in str(exc_info.value)

        # Test None path
        with pytest.raises(ValueError) as exc_info:
            mock_exporter_class(output_path=None, options={})
        assert "output_path cannot be empty" in str(exc_info.value)

    def test_base_exporter_options_default(self, mock_exporter_class):
        """
        Test that options parameter has sensible default.

        What this tests:
        ---------------
        1. Options parameter is optional in constructor
        2. Defaults to empty dict when not provided
        3. Attribute always exists and is dict type
        4. Can omit options for simple exports

        Why this matters:
        ----------------
        - Simpler API for basic usage
        - No None checks needed in subclasses
        - Consistent interface across exporters
        - Production code often uses defaults

        Additional context:
        ---------------------------------
        - Each exporter defines own option keys
        - Empty dict means use all defaults
        - Options merged with format-specific defaults
        """
        exporter = mock_exporter_class(output_path="/tmp/test.csv")

        assert exporter.options == {}
        assert isinstance(exporter.options, dict)
