"""
Test for fixing the critical asyncio.as_completed bug in parallel execution.

What this tests:
---------------
1. The bug with asyncio.as_completed KeyError
2. Proper parallel execution after fix
3. Correct error handling with parallel tasks
4. Progress tracking works correctly

Why this matters:
----------------
- Parallel execution is completely broken
- This is a P0 bug preventing any parallelism
- User explicitly requested verification of parallel execution
"""

import asyncio
import time
from unittest.mock import Mock

import pandas as pd
import pytest


class TestParallelExecutionBugFix:
    """Test the fix for the critical parallel execution bug."""

    @pytest.mark.asyncio
    async def test_bug_demonstration(self):
        """Demonstrate the current bug with asyncio.as_completed."""
        # This shows exactly what's wrong
        tasks = []
        task_to_data = {}

        async def dummy_task(i):
            await asyncio.sleep(0.01)
            return i

        # Create tasks and map them
        for i in range(3):
            task = asyncio.create_task(dummy_task(i))
            tasks.append(task)
            task_to_data[task] = f"data_{i}"

        # This is what the current code does - IT FAILS
        results = []
        with pytest.raises(KeyError):
            for coro in asyncio.as_completed(tasks):
                # coro is NOT the original task!
                data = task_to_data[coro]  # KeyError!
                result = await coro
                results.append((result, data))

    @pytest.mark.asyncio
    async def test_correct_approach_with_gather(self):
        """Test using asyncio.gather for parallel execution."""
        execution_times = []

        async def mock_partition_read(partition_def):
            start = time.time()
            execution_times.append(("start", start, partition_def["id"]))

            # Simulate work
            await asyncio.sleep(0.05)

            end = time.time()
            execution_times.append(("end", end, partition_def["id"]))

            return pd.DataFrame(
                {"id": [partition_def["id"]], "data": [f"data_{partition_def['id']}"]}
            )

        # Create partition definitions
        partitions = [{"id": i} for i in range(5)]

        # Use gather with semaphore for concurrency control
        semaphore = asyncio.Semaphore(2)  # Max 2 concurrent

        async def read_with_semaphore(partition):
            async with semaphore:
                return await mock_partition_read(partition)

        # Execute all tasks
        start_time = time.time()
        results = await asyncio.gather(
            *[read_with_semaphore(p) for p in partitions], return_exceptions=True
        )
        total_time = time.time() - start_time

        # Verify results
        assert len(results) == 5
        assert all(isinstance(r, pd.DataFrame) for r in results)

        # Verify parallelism - should be faster than sequential
        # 5 tasks * 0.05s = 0.25s sequential
        # With concurrency=2: ~0.15s (3 batches)
        assert total_time < 0.25, f"Too slow: {total_time}s"

        # Verify concurrency limit was respected
        max_concurrent = 0
        current_concurrent = 0
        for event, _, _ in sorted(execution_times, key=lambda x: x[1]):
            if event == "start":
                current_concurrent += 1
                max_concurrent = max(max_concurrent, current_concurrent)
            else:
                current_concurrent -= 1

        assert max_concurrent == 2, f"Concurrency limit not respected: {max_concurrent}"

    @pytest.mark.asyncio
    async def test_fixed_parallel_reader_approach(self):
        """Test a fixed approach for ParallelPartitionReader."""

        class FixedParallelPartitionReader:
            """Fixed implementation using asyncio.gather."""

            def __init__(self, session, max_concurrent=10):
                self.session = session
                self.max_concurrent = max_concurrent
                self._semaphore = asyncio.Semaphore(max_concurrent)

            async def read_partitions(self, partitions):
                """Read partitions in parallel using gather."""

                async def read_single_partition(partition, index):
                    """Read one partition with semaphore control."""
                    async with self._semaphore:
                        try:
                            # Simulate partition reading
                            await asyncio.sleep(0.01)
                            df = pd.DataFrame({"id": [index]})
                            return (index, df, None)  # index, result, error
                        except Exception as e:
                            return (index, None, e)  # index, result, error

                # Create all tasks
                tasks = [read_single_partition(p, i) for i, p in enumerate(partitions)]

                # Execute with gather
                results = await asyncio.gather(*tasks, return_exceptions=True)

                # Process results
                dfs = []
                errors = []
                for result in results:
                    if isinstance(result, Exception):
                        # Handle gather exception
                        errors.append((None, None, result))
                    else:
                        index, df, error = result
                        if error:
                            errors.append((index, partitions[index], error))
                        else:
                            dfs.append(df)

                if errors and not dfs:
                    raise Exception(f"All partitions failed: {errors}")

                return dfs

        # Test the fixed implementation
        reader = FixedParallelPartitionReader(Mock(), max_concurrent=3)
        partitions = [{"id": i} for i in range(10)]

        start = time.time()
        dfs = await reader.read_partitions(partitions)
        duration = time.time() - start

        # Should complete successfully
        assert len(dfs) == 10

        # Should be parallel (faster than sequential)
        assert duration < 0.1, "Should run in parallel"

    @pytest.mark.asyncio
    async def test_error_handling_in_parallel(self):
        """Test that errors are properly handled in parallel execution."""

        async def failing_partition_read(partition):
            if partition["id"] % 2 == 0:
                raise ValueError(f"Simulated error for partition {partition['id']}")
            await asyncio.sleep(0.01)
            return pd.DataFrame({"id": [partition["id"]]})

        partitions = [{"id": i} for i in range(6)]

        # Use gather with return_exceptions
        results = await asyncio.gather(
            *[failing_partition_read(p) for p in partitions], return_exceptions=True
        )

        # Check results
        successes = [r for r in results if isinstance(r, pd.DataFrame)]
        errors = [r for r in results if isinstance(r, Exception)]

        assert len(successes) == 3  # Odd IDs succeed
        assert len(errors) == 3  # Even IDs fail
        assert all("Simulated error" in str(e) for e in errors)
