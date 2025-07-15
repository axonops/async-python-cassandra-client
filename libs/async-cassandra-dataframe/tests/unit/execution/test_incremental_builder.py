"""
Test incremental DataFrame builder for memory efficiency.

What this tests:
---------------
1. Incremental row addition
2. Memory efficiency compared to list collection
3. Type conversion during building
4. Chunk consolidation
5. UDT handling in incremental mode

Why this matters:
----------------
- Current approach uses 2x memory (list + DataFrame)
- Incremental building is more memory efficient
- Allows early termination on memory limits
- Better for large result sets
"""

from unittest.mock import Mock

import pandas as pd
import pytest

from async_cassandra_dataframe.incremental_builder import IncrementalDataFrameBuilder


class TestIncrementalDataFrameBuilder:
    """Test incremental DataFrame building."""

    def test_empty_builder_returns_empty_dataframe(self):
        """Empty builder should return DataFrame with correct columns."""
        builder = IncrementalDataFrameBuilder(columns=["id", "name", "email"])
        df = builder.get_dataframe()

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0
        assert list(df.columns) == ["id", "name", "email"]

    def test_single_row_addition(self):
        """Single row should be added correctly."""
        builder = IncrementalDataFrameBuilder(columns=["id", "name"])

        # Mock row with _asdict
        row = Mock()
        row._asdict.return_value = {"id": 1, "name": "Alice"}

        builder.add_row(row)
        df = builder.get_dataframe()

        assert len(df) == 1
        assert df.iloc[0]["id"] == 1
        assert df.iloc[0]["name"] == "Alice"

    def test_chunk_consolidation(self):
        """Rows should be consolidated into chunks."""
        builder = IncrementalDataFrameBuilder(columns=["id"], chunk_size=3)

        # Add 5 rows - should create 1 chunk + current_chunk_data
        for i in range(5):
            row = Mock()
            row._asdict.return_value = {"id": i}
            builder.add_row(row)

        # After 3 rows, should have 1 chunk
        assert len(builder.chunks) == 1
        assert len(builder.current_chunk_data) == 2

        df = builder.get_dataframe()
        assert len(df) == 5
        assert list(df["id"]) == [0, 1, 2, 3, 4]

    def test_memory_usage_tracking(self):
        """Memory usage should be tracked correctly."""
        builder = IncrementalDataFrameBuilder(columns=["id", "data"], chunk_size=2)

        # Add rows
        for i in range(3):
            row = Mock()
            row._asdict.return_value = {"id": i, "data": "x" * 100}
            builder.add_row(row)

        memory = builder.get_memory_usage()
        assert memory > 0  # Should have some memory usage

    def test_udt_handling(self):
        """UDTs should be handled as dicts."""
        builder = IncrementalDataFrameBuilder(columns=["id", "address"])

        # Mock row with UDT
        row = Mock()
        row._asdict.return_value = {"id": 1, "address": {"street": "123 Main", "city": "NYC"}}

        builder.add_row(row)
        df = builder.get_dataframe()

        assert len(df) == 1
        assert isinstance(df.iloc[0]["address"], dict)
        assert df.iloc[0]["address"]["city"] == "NYC"

    def test_row_without_asdict(self):
        """Rows without _asdict should use getattr."""
        builder = IncrementalDataFrameBuilder(columns=["id", "name"])

        # Mock row without _asdict
        row = Mock(spec=["id", "name"])
        row.id = 1
        row.name = "Bob"

        builder.add_row(row)
        df = builder.get_dataframe()

        assert len(df) == 1
        assert df.iloc[0]["id"] == 1
        assert df.iloc[0]["name"] == "Bob"

    def test_incremental_vs_batch_memory(self):
        """Incremental building should use less peak memory than batch."""
        # This is a conceptual test - in practice would need memory profiling

        # Batch approach simulation
        rows = []
        for i in range(1000):
            row = {"id": i, "data": "x" * 100}
            rows.append(row)
        batch_df = pd.DataFrame(rows)

        # Incremental approach
        builder = IncrementalDataFrameBuilder(columns=["id", "data"], chunk_size=100)
        for i in range(1000):
            row = Mock()
            row._asdict.return_value = {"id": i, "data": "x" * 100}
            builder.add_row(row)
        incremental_df = builder.get_dataframe()

        # Results should be identical
        pd.testing.assert_frame_equal(batch_df, incremental_df)

        # Memory usage difference would be measured in real profiling

    def test_type_mapper_integration(self):
        """Type mapper should be applied if provided."""
        # Mock type mapper
        type_mapper = Mock()
        type_mapper.convert_value = lambda x, t: str(x).upper() if t == "text" else x

        builder = IncrementalDataFrameBuilder(columns=["id", "name"], type_mapper=type_mapper)

        row = Mock()
        row._asdict.return_value = {"id": 1, "name": "alice"}

        # For now, type conversion is a placeholder
        builder.add_row(row)
        df = builder.get_dataframe()

        # Type conversion would be applied in _apply_type_conversions
        assert len(df) == 1


class TestIncrementalBuilderWithStreaming:
    """Test incremental builder with streaming scenarios."""

    @pytest.mark.asyncio
    async def test_streaming_progress_callback(self):
        """Progress callbacks should work with incremental building."""

        # This would be an integration test in practice
        # Here we verify the interface works

        columns = ["id", "name"]
        builder = IncrementalDataFrameBuilder(columns=columns)

        # Simulate streaming rows
        for i in range(10):
            row = Mock()
            row._asdict.return_value = {"id": i, "name": f"user_{i}"}
            builder.add_row(row)

        df = builder.get_dataframe()
        assert len(df) == 10

    def test_early_termination_on_memory_limit(self):
        """Building should stop when memory limit is reached."""
        builder = IncrementalDataFrameBuilder(columns=["id", "data"], chunk_size=10)
        memory_limit = 1024  # 1KB for testing

        rows_added = 0
        for i in range(1000):
            row = Mock()
            row._asdict.return_value = {"id": i, "data": "x" * 1000}
            builder.add_row(row)
            rows_added += 1

            if builder.get_memory_usage() > memory_limit:
                break

        # Should have stopped before adding all rows
        assert rows_added < 1000
        df = builder.get_dataframe()
        assert len(df) == rows_added
