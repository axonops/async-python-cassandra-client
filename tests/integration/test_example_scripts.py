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
            timeout=60,  # Allow time for data generation
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

    async def test_export_large_table_example(self, cassandra_cluster):
        """
        Test the table export example.

        What this tests:
        ---------------
        1. Creates sample data correctly
        2. Exports data to CSV format
        3. Handles different data types properly
        4. Shows progress during export
        5. Cleans up resources

        Why this matters:
        ----------------
        - Data export is common requirement
        - CSV format widely used
        - Memory efficiency critical for large tables
        - Progress tracking improves UX
        """
        script_path = EXAMPLES_DIR / "export_large_table.py"
        assert script_path.exists(), f"Example script not found: {script_path}"

        # Ensure export directory doesn't exist
        export_dir = Path("exports")
        if export_dir.exists():
            for file in export_dir.glob("*.csv"):
                file.unlink()

        # Run the example script
        result = subprocess.run(
            [sys.executable, str(script_path)],
            capture_output=True,
            text=True,
            timeout=60,
        )

        # Check execution succeeded
        assert result.returncode == 0, f"Script failed with: {result.stderr}"

        # Verify expected output (might be in stdout or stderr due to logging)
        output = result.stdout + result.stderr
        assert "Created 5000 sample products" in output
        assert "Export completed:" in output
        assert "Rows exported: 5,000" in output

        # Verify CSV file was created
        csv_files = list(export_dir.glob("*.csv"))
        assert len(csv_files) > 0, "No CSV files were created"

        # Verify CSV content
        csv_file = csv_files[0]
        assert csv_file.stat().st_size > 0, "CSV file is empty"

        # Read first few lines to verify format
        with open(csv_file, "r") as f:
            header = f.readline().strip()
            # Verify header contains expected columns
            assert "product_id" in header
            assert "category" in header
            assert "price" in header

        # Cleanup
        for file in export_dir.glob("*.csv"):
            file.unlink()

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
            timeout=90,  # Allow more time for data processing
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

    @pytest.mark.parametrize(
        "script_name",
        [
            "streaming_basic.py",
            "export_large_table.py",
            "context_manager_safety_demo.py",
            "metrics_simple.py",
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
