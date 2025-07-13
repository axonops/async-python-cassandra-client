"""
Fix for asyncio.as_completed issue in parallel.py

The problem:
- asyncio.as_completed(tasks) yields coroutines, not the original tasks
- We can't map these back to our task_to_partition dict

The solution:
- Store the result with the partition info
- Use asyncio.gather with return_exceptions=True for better error handling
"""

# Current buggy code:
"""
for task in asyncio.as_completed(tasks):
    partition_idx, partition = task_to_partition[task]  # KeyError!
    try:
        result = await task
"""

# Fixed approach 1 - Use gather with proper mapping:
"""
# Create tasks with partition info embedded
tasks_with_info = []
for i, partition in enumerate(partitions):
    task = asyncio.create_task(self._read_single_partition(partition, i, total))
    tasks_with_info.append((i, partition, task))

# Use gather to maintain order
results = await asyncio.gather(*[task for _, _, task in tasks_with_info], return_exceptions=True)

# Process results with partition info
for (partition_idx, partition, _), result in zip(tasks_with_info, results):
    if isinstance(result, Exception):
        errors.append((partition_idx, partition, result))
    else:
        successful_results.append(result)
"""

# Fixed approach 2 - Embed partition info in task result:
"""
async def _read_single_partition_with_info(self, partition, index, total):
    try:
        df = await self._read_single_partition(partition, index, total)
        return (index, partition, df, None)  # Success
    except Exception as e:
        return (index, partition, None, e)   # Error

# Then use as_completed normally:
tasks = [
    asyncio.create_task(self._read_single_partition_with_info(p, i, total))
    for i, p in enumerate(partitions)
]

for task in asyncio.as_completed(tasks):
    index, partition, df, error = await task
    if error:
        errors.append((index, partition, error))
    else:
        results.append(df)
"""
