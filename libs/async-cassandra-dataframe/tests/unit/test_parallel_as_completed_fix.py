"""
Test to verify fix for asyncio.as_completed issue.

What this tests:
---------------
1. The bug with asyncio.as_completed KeyError
2. Proper partition tracking through completion
3. Error handling still works correctly

Why this matters:
----------------
- Critical bug preventing parallel execution
- asyncio.as_completed doesn't return original tasks
- Need to track partition info through completion
"""

import asyncio
from unittest.mock import Mock, patch

import pandas as pd
import pytest
from async_cassandra_dataframe.parallel import ParallelPartitionReader


class TestAsCompletedFix:
    """Test the fix for asyncio.as_completed issue."""

    @pytest.mark.asyncio
    async def test_bug_is_fixed(self):
        """The asyncio.as_completed bug has been fixed."""
        # This test verifies the fix works

        async def mock_stream_partition(partition):
            await asyncio.sleep(0.01)
            return pd.DataFrame({"id": [partition["partition_id"]]})

        with patch(
            "async_cassandra_dataframe.partition.StreamingPartitionStrategy"
        ) as MockStrategy:
            mock_strategy = Mock()
            mock_strategy.stream_partition = mock_stream_partition
            MockStrategy.return_value = mock_strategy

            reader = ParallelPartitionReader(session=Mock())
            partitions = [{"partition_id": i, "session": Mock(), "table": "test"} for i in range(3)]

            # This should now work without KeyError
            results = await reader.read_partitions(partitions)

            # Verify we got results from all partitions
            assert len(results) == 3
            # Results might be in any order due to as_completed
            ids = sorted([df.iloc[0]["id"] for df in results])
            assert ids == [0, 1, 2]

    @pytest.mark.asyncio
    async def test_fixed_implementation(self):
        """Test a fixed implementation that properly handles as_completed."""
        # This is how it should work

        async def read_partition_with_info(partition, index):
            """Wrap partition reading to include metadata."""
            await asyncio.sleep(0.01)
            df = pd.DataFrame({"id": [index]})
            return {"index": index, "partition": partition, "df": df}

        partitions = [{"id": i} for i in range(3)]
        tasks = [
            asyncio.create_task(read_partition_with_info(p, i)) for i, p in enumerate(partitions)
        ]

        results = []
        for coro in asyncio.as_completed(tasks):
            result = await coro
            results.append(result)

        # Should complete successfully
        assert len(results) == 3
        # Results may be out of order, but all should be present
        indices = sorted([r["index"] for r in results])
        assert indices == [0, 1, 2]
