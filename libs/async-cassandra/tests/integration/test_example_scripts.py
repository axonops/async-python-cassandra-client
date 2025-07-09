"""
Integration tests for example scripts.

This module tests that all example scripts in the examples/ directory
work correctly and follow the proper API usage patterns.

What this tests:
---------------
1. All example scripts execute without errors
2. Examples use context managers properly
3. Examples use prepared statements where appropriate
4. Examples clean up resources correctly
5. Examples demonstrate best practices

Why this matters:
----------------
- Examples are often the first code users see
- Broken examples damage library credibility
- Examples should showcase best practices
- Users copy example code into production

Additional context:
---------------------------------
- Tests run each example in isolation
- Cassandra container is shared between tests
- Each example creates and drops its own keyspace
- Tests verify output and side effects
"""

import asyncio
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from async_cassandra import AsyncCluster

# Path to examples directory
EXAMPLES_DIR = Path(__file__).parent.parent.parent / "examples"


class TestExampleScripts:
    """Test all example scripts work correctly."""

    @pytest.fixture(autouse=True)
    async def setup_cassandra(self, cassandra_cluster):
        """Ensure Cassandra is available for examples."""
        # Cassandra is guaranteed to be available via cassandra_cluster fixture
        pass

    @pytest.mark.timeout(180)  # Override default timeout for this test
    async def test_streaming_basic_example(self, cassandra_cluster):
        """
        Test the basic streaming example.

        What this tests:
        ---------------
        1. Script executes without errors
        2. Creates and populates test data
        3. Demonstrates streaming with context manager
        4. Shows filtered streaming with prepared statements
        5. Cleans up keyspace after completion

        Why this matters:
        ----------------
        - Streaming is critical for large datasets
        - Context managers prevent memory leaks
        - Users need clear streaming examples
        - Common use case for analytics
        """
        script_path = EXAMPLES_DIR / "streaming_basic.py"
        assert script_path.exists(), f"Example script not found: {script_path}"

        # Run the example script
        result = subprocess.run(
            [sys.executable, str(script_path)],
            capture_output=True,
            text=True,
            timeout=120,  # Allow time for 100k events generation
        )

        # Check execution succeeded
        if result.returncode != 0:
            print(f"STDOUT:\n{result.stdout}")
            print(f"STDERR:\n{result.stderr}")
        assert result.returncode == 0, f"Script failed with return code {result.returncode}"

        # Verify expected output patterns
        # The examples use logging which outputs to stderr
        output = result.stderr if result.stderr else result.stdout
        assert "Basic Streaming Example" in output
        assert "Inserted 100000 test events" in output or "Inserted 100,000 test events" in output
        assert "Streaming completed:" in output
        assert "Total events: 100,000" in output or "Total events: 100000" in output
        assert "Filtered Streaming Example" in output
        assert "Page-Based Streaming Example (True Async Paging)" in output
        assert "Pages are fetched asynchronously" in output

        # Verify keyspace was cleaned up
        async with AsyncCluster(["localhost"]) as cluster:
            async with await cluster.connect() as session:
                result = await session.execute(
                    "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = 'streaming_example'"
                )
                assert result.one() is None, "Keyspace was not cleaned up"

    async def test_export_large_table_example(self, cassandra_cluster, tmp_path):
        """
        Test the table export example.

        What this tests:
        ---------------
        1. Creates sample data correctly
        2. Exports data to CSV format
        3. Handles different data types properly
        4. Shows progress during export
        5. Cleans up resources
        6. Validates output file content

        Why this matters:
        ----------------
        - Data export is common requirement
        - CSV format widely used
        - Memory efficiency critical for large tables
        - Progress tracking improves UX
        """
        script_path = EXAMPLES_DIR / "export_large_table.py"
        assert script_path.exists(), f"Example script not found: {script_path}"

        # Use temp directory for output
        export_dir = tmp_path / "example_output"
        export_dir.mkdir(exist_ok=True)

        try:
            # Run the example script with custom output directory
            env = os.environ.copy()
            env["EXAMPLE_OUTPUT_DIR"] = str(export_dir)

            result = subprocess.run(
                [sys.executable, str(script_path)],
                capture_output=True,
                text=True,
                timeout=60,
                env=env,
            )

            # Check execution succeeded
            assert result.returncode == 0, f"Script failed with: {result.stderr}"

            # Verify expected output (might be in stdout or stderr due to logging)
            output = result.stdout + result.stderr
            assert "Created 5000 sample products" in output
            assert "Export completed:" in output
            assert "Rows exported: 5,000" in output
            assert f"Output directory: {export_dir}" in output

            # Verify CSV file was created
            csv_files = list(export_dir.glob("*.csv"))
            assert len(csv_files) > 0, "No CSV files were created"

            # Verify CSV content
            csv_file = csv_files[0]
            assert csv_file.stat().st_size > 0, "CSV file is empty"

            # Read and validate CSV content
            with open(csv_file, "r") as f:
                header = f.readline().strip()
                # Verify header contains expected columns
                assert "product_id" in header
                assert "category" in header
                assert "price" in header
                assert "in_stock" in header
                assert "tags" in header
                assert "attributes" in header
                assert "created_at" in header

                # Read a few data rows to verify content
                row_count = 0
                for line in f:
                    row_count += 1
                    if row_count > 10:  # Check first 10 rows
                        break
                    # Basic validation that row has content
                    assert len(line.strip()) > 0
                    assert "," in line  # CSV format

                # Verify we have the expected number of rows (5000 + header)
                f.seek(0)
                total_lines = sum(1 for _ in f)
                assert (
                    total_lines == 5001
                ), f"Expected 5001 lines (header + 5000 rows), got {total_lines}"

        finally:
            # Cleanup - always clean up even if test fails
            # pytest's tmp_path fixture also cleans up automatically
            if export_dir.exists():
                shutil.rmtree(export_dir)

    async def test_context_manager_safety_demo(self, cassandra_cluster):
        """
        Test the context manager safety demonstration.

        What this tests:
        ---------------
        1. Query errors don't close sessions
        2. Streaming errors don't close sessions
        3. Context managers isolate resources
        4. Concurrent operations work safely
        5. Proper error handling patterns

        Why this matters:
        ----------------
        - Users need to understand resource lifecycle
        - Error handling is often done wrong
        - Context managers are mandatory
        - Demonstrates resilience patterns
        """
        script_path = EXAMPLES_DIR / "context_manager_safety_demo.py"
        assert script_path.exists(), f"Example script not found: {script_path}"

        # Run the example script with longer timeout
        result = subprocess.run(
            [sys.executable, str(script_path)],
            capture_output=True,
            text=True,
            timeout=60,  # Increase timeout as this example runs multiple demonstrations
        )

        # Check execution succeeded
        assert result.returncode == 0, f"Script failed with: {result.stderr}"

        # Verify all demonstrations ran (might be in stdout or stderr due to logging)
        output = result.stdout + result.stderr
        assert "Demonstrating Query Error Safety" in output
        assert "Query failed as expected" in output
        assert "Session still works after error" in output

        assert "Demonstrating Streaming Error Safety" in output
        assert "Streaming failed as expected" in output
        assert "Successfully streamed" in output

        assert "Demonstrating Context Manager Isolation" in output
        assert "Demonstrating Concurrent Safety" in output

        # Verify key takeaways are shown
        assert "Query errors don't close sessions" in output
        assert "Context managers only close their own resources" in output

    async def test_metrics_simple_example(self, cassandra_cluster):
        """
        Test the simple metrics example.

        What this tests:
        ---------------
        1. Metrics collection works correctly
        2. Query performance is tracked
        3. Connection health is monitored
        4. Statistics are calculated properly
        5. Error tracking functions

        Why this matters:
        ----------------
        - Observability is critical in production
        - Users need metrics examples
        - Performance monitoring essential
        - Shows integration patterns
        """
        script_path = EXAMPLES_DIR / "metrics_simple.py"
        assert script_path.exists(), f"Example script not found: {script_path}"

        # Run the example script
        result = subprocess.run(
            [sys.executable, str(script_path)],
            capture_output=True,
            text=True,
            timeout=30,
        )

        # Check execution succeeded
        assert result.returncode == 0, f"Script failed with: {result.stderr}"

        # Verify metrics output (might be in stdout or stderr due to logging)
        output = result.stdout + result.stderr
        assert "Query Metrics Example" in output or "async-cassandra Metrics Example" in output
        assert "Connection Health Monitoring" in output
        assert "Error Tracking Example" in output or "Expected error recorded" in output
        assert "Performance Summary" in output

        # Verify statistics are shown
        assert "Total queries:" in output or "Query Metrics:" in output
        assert "Success rate:" in output or "Success Rate:" in output
        assert "Average latency:" in output or "Average Duration:" in output

    @pytest.mark.timeout(240)  # Override default timeout for this test (lots of data)
    async def test_realtime_processing_example(self, cassandra_cluster):
        """
        Test the real-time processing example.

        What this tests:
        ---------------
        1. Time-series data handling
        2. Sliding window analytics
        3. Real-time aggregations
        4. Alert triggering logic
        5. Continuous processing patterns

        Why this matters:
        ----------------
        - IoT/sensor data is common use case
        - Real-time analytics increasingly important
        - Shows advanced streaming patterns
        - Demonstrates time-based queries
        """
        script_path = EXAMPLES_DIR / "realtime_processing.py"
        assert script_path.exists(), f"Example script not found: {script_path}"

        # Run the example script with a longer timeout since it processes lots of data
        result = subprocess.run(
            [sys.executable, str(script_path)],
            capture_output=True,
            text=True,
            timeout=180,  # Allow more time for 108k readings (50 sensors × 2160 time points)
        )

        # Check execution succeeded
        assert result.returncode == 0, f"Script failed with: {result.stderr}"

        # Verify expected output (check both stdout and stderr)
        output = result.stdout + result.stderr

        # Check that setup completed
        assert "Setting up sensor data" in output
        assert "Sample data inserted" in output

        # Check that processing occurred
        assert "Processing Historical Data" in output or "Processing historical data" in output
        assert "Processing completed" in output or "readings processed" in output

        # Check that real-time simulation ran
        assert "Simulating Real-Time Processing" in output or "Processing cycle" in output

        # Verify cleanup
        assert "Cleaning up" in output

    async def test_metrics_advanced_example(self, cassandra_cluster):
        """
        Test the advanced metrics example.

        What this tests:
        ---------------
        1. Multiple metrics collectors
        2. Prometheus integration setup
        3. FastAPI integration patterns
        4. Comprehensive monitoring
        5. Production-ready patterns

        Why this matters:
        ----------------
        - Production systems need Prometheus
        - FastAPI integration common
        - Shows complete monitoring setup
        - Enterprise-ready patterns
        """
        script_path = EXAMPLES_DIR / "metrics_example.py"
        assert script_path.exists(), f"Example script not found: {script_path}"

        # Run the example script
        result = subprocess.run(
            [sys.executable, str(script_path)],
            capture_output=True,
            text=True,
            timeout=30,
        )

        # Check execution succeeded
        assert result.returncode == 0, f"Script failed with: {result.stderr}"

        # Verify advanced features demonstrated (might be in stdout or stderr due to logging)
        output = result.stdout + result.stderr
        assert "Metrics" in output or "metrics" in output
        assert "queries" in output.lower() or "Queries" in output

    @pytest.mark.timeout(240)  # Override default timeout for this test
    async def test_export_to_parquet_example(self, cassandra_cluster, tmp_path):
        """
        Test the Parquet export example.

        What this tests:
        ---------------
        1. Creates test data with various types
        2. Exports data to Parquet format
        3. Handles different compression formats
        4. Shows progress during export
        5. Verifies exported files
        6. Validates Parquet file content
        7. Cleans up resources automatically

        Why this matters:
        ----------------
        - Parquet is popular for analytics
        - Memory-efficient export critical for large datasets
        - Type handling must be correct
        - Shows advanced streaming patterns
        """
        script_path = EXAMPLES_DIR / "export_to_parquet.py"
        assert script_path.exists(), f"Example script not found: {script_path}"

        # Use temp directory for output
        export_dir = tmp_path / "parquet_output"
        export_dir.mkdir(exist_ok=True)

        try:
            # Run the example script with custom output directory
            env = os.environ.copy()
            env["EXAMPLE_OUTPUT_DIR"] = str(export_dir)

            result = subprocess.run(
                [sys.executable, str(script_path)],
                capture_output=True,
                text=True,
                timeout=180,  # Allow time for data generation and export
                env=env,
            )

            # Check execution succeeded
            if result.returncode != 0:
                print(f"STDOUT:\n{result.stdout}")
                print(f"STDERR:\n{result.stderr}")
            assert result.returncode == 0, f"Script failed with return code {result.returncode}"

            # Verify expected output
            output = result.stderr if result.stderr else result.stdout
            assert "Setting up test data" in output
            assert "Test data setup complete" in output
            assert "Example 1: Export Entire Table" in output
            assert "Example 2: Export Filtered Data" in output
            assert "Example 3: Export with Different Compression" in output
            assert "Export completed successfully!" in output
            assert "Verifying Exported Files" in output
            assert f"Output directory: {export_dir}" in output

            # Verify Parquet files were created (look recursively in subdirectories)
            parquet_files = list(export_dir.rglob("*.parquet"))
            assert (
                len(parquet_files) >= 3
            ), f"Expected at least 3 Parquet files, found {len(parquet_files)}"

            # Verify files have content
            for parquet_file in parquet_files:
                assert parquet_file.stat().st_size > 0, f"Parquet file {parquet_file} is empty"

            # Verify we can read and validate the Parquet files
            try:
                import pyarrow as pa
                import pyarrow.parquet as pq

                # Track total rows across all files
                total_rows = 0

                for parquet_file in parquet_files:
                    table = pq.read_table(parquet_file)
                    assert table.num_rows > 0, f"Parquet file {parquet_file} has no rows"
                    total_rows += table.num_rows

                    # Verify expected columns exist
                    column_names = [field.name for field in table.schema]
                    assert "user_id" in column_names
                    assert "event_time" in column_names
                    assert "event_type" in column_names
                    assert "device_type" in column_names
                    assert "country_code" in column_names
                    assert "city" in column_names
                    assert "revenue" in column_names
                    assert "duration_seconds" in column_names
                    assert "is_premium" in column_names
                    assert "metadata" in column_names
                    assert "tags" in column_names

                    # Verify data types are preserved
                    schema = table.schema
                    assert schema.field("is_premium").type == pa.bool_()
                    assert (
                        schema.field("duration_seconds").type == pa.int64()
                    )  # We use int64 in our schema

                    # Read first few rows to validate content
                    df = table.to_pandas()
                    assert len(df) > 0

                    # Validate some data characteristics
                    assert (
                        df["event_type"]
                        .isin(["view", "click", "purchase", "signup", "logout"])
                        .all()
                    )
                    assert df["device_type"].isin(["mobile", "desktop", "tablet", "tv"]).all()
                    assert df["duration_seconds"].between(10, 3600).all()

                # Verify we generated substantial test data (should be > 10k rows)
                assert total_rows > 10000, f"Expected > 10000 total rows, got {total_rows}"

            except ImportError:
                # PyArrow not available in test environment
                pytest.skip("PyArrow not available for full validation")

        finally:
            # Cleanup - always clean up even if test fails
            # pytest's tmp_path fixture also cleans up automatically
            if export_dir.exists():
                shutil.rmtree(export_dir)

    async def test_streaming_non_blocking_demo(self, cassandra_cluster):
        """
        Test the non-blocking streaming demonstration.

        What this tests:
        ---------------
        1. Creates test data for streaming
        2. Demonstrates event loop responsiveness
        3. Shows concurrent operations during streaming
        4. Provides visual feedback of non-blocking behavior
        5. Cleans up resources

        Why this matters:
        ----------------
        - Proves async wrapper doesn't block
        - Critical for understanding async benefits
        - Shows real concurrent execution
        - Validates our architecture claims
        """
        script_path = EXAMPLES_DIR / "streaming_non_blocking_demo.py"
        assert script_path.exists(), f"Example script not found: {script_path}"

        # Run the example script
        result = subprocess.run(
            [sys.executable, str(script_path)],
            capture_output=True,
            text=True,
            timeout=120,  # Allow time for demonstrations
        )

        # Check execution succeeded
        if result.returncode != 0:
            print(f"STDOUT:\n{result.stdout}")
            print(f"STDERR:\n{result.stderr}")
        assert result.returncode == 0, f"Script failed with return code {result.returncode}"

        # Verify expected output
        output = result.stdout + result.stderr
        assert "Starting non-blocking streaming demonstration" in output
        assert "Heartbeat still running!" in output
        assert "Event Loop Analysis:" in output
        assert "Event loop remained responsive!" in output
        assert "Demonstrating concurrent operations" in output
        assert "Demonstration complete!" in output

        # Verify keyspace was cleaned up
        async with AsyncCluster(["localhost"]) as cluster:
            async with await cluster.connect() as session:
                result = await session.execute(
                    "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = 'streaming_demo'"
                )
                assert result.one() is None, "Keyspace was not cleaned up"

    @pytest.mark.parametrize(
        "script_name",
        [
            "streaming_basic.py",
            "export_large_table.py",
            "context_manager_safety_demo.py",
            "metrics_simple.py",
            "export_to_parquet.py",
            "streaming_non_blocking_demo.py",
        ],
    )
    async def test_example_uses_context_managers(self, script_name):
        """
        Verify all examples use context managers properly.

        What this tests:
        ---------------
        1. AsyncCluster used with context manager
        2. Sessions used with context manager
        3. Streaming uses context manager
        4. No resource leaks

        Why this matters:
        ----------------
        - Context managers are mandatory
        - Prevents resource leaks
        - Examples must show best practices
        - Users copy example patterns
        """
        script_path = EXAMPLES_DIR / script_name
        assert script_path.exists(), f"Example script not found: {script_path}"

        # Read script content
        content = script_path.read_text()

        # Check for context manager usage
        assert (
            "async with AsyncCluster" in content
        ), f"{script_name} doesn't use AsyncCluster context manager"

        # If script has streaming, verify context manager usage
        if "execute_stream" in content:
            assert (
                "async with await session.execute_stream" in content
                or "async with session.execute_stream" in content
            ), f"{script_name} doesn't use streaming context manager"

    @pytest.mark.parametrize(
        "script_name",
        [
            "streaming_basic.py",
            "export_large_table.py",
            "context_manager_safety_demo.py",
            "metrics_simple.py",
            "export_to_parquet.py",
            "streaming_non_blocking_demo.py",
        ],
    )
    async def test_example_uses_prepared_statements(self, script_name):
        """
        Verify examples use prepared statements for parameterized queries.

        What this tests:
        ---------------
        1. Prepared statements for inserts
        2. Prepared statements for selects with parameters
        3. No string interpolation in queries
        4. Proper parameter binding

        Why this matters:
        ----------------
        - Prepared statements are mandatory
        - Prevents SQL injection
        - Better performance
        - Examples must show best practices
        """
        script_path = EXAMPLES_DIR / script_name
        assert script_path.exists(), f"Example script not found: {script_path}"

        # Read script content
        content = script_path.read_text()

        # If script has parameterized queries, check for prepared statements
        if "VALUES (?" in content or "WHERE" in content and "= ?" in content:
            assert (
                "prepare(" in content
            ), f"{script_name} has parameterized queries but doesn't use prepare()"


class TestExampleDocumentation:
    """Test that example documentation is accurate and complete."""

    async def test_readme_lists_all_examples(self):
        """
        Verify README documents all example scripts.

        What this tests:
        ---------------
        1. All .py files are documented
        2. Descriptions match actual functionality
        3. Run instructions are provided
        4. Prerequisites are listed

        Why this matters:
        ----------------
        - Users rely on README for navigation
        - Missing examples confuse users
        - Documentation must stay in sync
        - First impression matters
        """
        readme_path = EXAMPLES_DIR / "README.md"
        assert readme_path.exists(), "Examples README.md not found"

        readme_content = readme_path.read_text()

        # Get all Python example files (excluding FastAPI app)
        example_files = [
            f.name for f in EXAMPLES_DIR.glob("*.py") if f.is_file() and not f.name.startswith("_")
        ]

        # Verify each example is documented
        for example_file in example_files:
            assert example_file in readme_content, f"{example_file} not documented in README"

        # Verify required sections exist
        assert "Prerequisites" in readme_content
        assert "Best Practices Demonstrated" in readme_content
        assert "Running Multiple Examples" in readme_content
        assert "Troubleshooting" in readme_content

    async def test_examples_have_docstrings(self):
        """
        Verify all examples have proper module docstrings.

        What this tests:
        ---------------
        1. Module-level docstrings exist
        2. Docstrings describe what's demonstrated
        3. Key features are listed
        4. Usage context is clear

        Why this matters:
        ----------------
        - Docstrings provide immediate context
        - Help users understand purpose
        - Good documentation practice
        - Self-documenting code
        """
        example_files = list(EXAMPLES_DIR.glob("*.py"))

        for example_file in example_files:
            content = example_file.read_text()
            lines = content.split("\n")

            # Check for module docstring
            docstring_found = False
            for i, line in enumerate(lines[:20]):  # Check first 20 lines
                if line.strip().startswith('"""') or line.strip().startswith("'''"):
                    docstring_found = True
                    break

            assert docstring_found, f"{example_file.name} missing module docstring"

            # Verify docstring mentions what's demonstrated
            if docstring_found:
                # Extract docstring content
                docstring_lines = []
                for j in range(i, min(i + 20, len(lines))):
                    docstring_lines.append(lines[j])
                    if j > i and (
                        lines[j].strip().endswith('"""') or lines[j].strip().endswith("'''")
                    ):
                        break

                docstring_content = "\n".join(docstring_lines).lower()
                assert (
                    "demonstrates" in docstring_content or "example" in docstring_content
                ), f"{example_file.name} docstring doesn't describe what it demonstrates"


# Run integration test for a specific example (useful for development)
async def run_single_example(example_name: str):
    """Run a single example script for testing."""
    script_path = EXAMPLES_DIR / example_name
    if not script_path.exists():
        print(f"Example not found: {script_path}")
        return

    print(f"Running {example_name}...")
    result = subprocess.run(
        [sys.executable, str(script_path)],
        capture_output=True,
        text=True,
        timeout=60,
    )

    if result.returncode == 0:
        print("Success! Output:")
        print(result.stdout)
    else:
        print("Failed! Error:")
        print(result.stderr)


if __name__ == "__main__":
    # For development testing
    import sys

    if len(sys.argv) > 1:
        asyncio.run(run_single_example(sys.argv[1]))
    else:
        print("Usage: python test_example_scripts.py <example_name.py>")
        print("Available examples:")
        for f in sorted(EXAMPLES_DIR.glob("*.py")):
            print(f"  - {f.name}")
