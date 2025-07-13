"""
Consistency level management for async-cassandra-dataframe.

Provides utilities for setting and managing Cassandra consistency levels.
"""

from cassandra import ConsistencyLevel
from cassandra.cluster import ExecutionProfile


def create_execution_profile(consistency_level: ConsistencyLevel) -> ExecutionProfile:
    """
    Create an execution profile with the specified consistency level.

    Args:
        consistency_level: Cassandra consistency level

    Returns:
        ExecutionProfile configured with the consistency level
    """
    profile = ExecutionProfile()
    profile.consistency_level = consistency_level
    return profile


def parse_consistency_level(level_str: str | None) -> ConsistencyLevel:
    """
    Parse a consistency level string.

    Args:
        level_str: Consistency level string (e.g., "LOCAL_ONE", "QUORUM")
                  None defaults to LOCAL_ONE

    Returns:
        ConsistencyLevel enum value

    Raises:
        ValueError: If the consistency level string is invalid
    """
    if level_str is None:
        return ConsistencyLevel.LOCAL_ONE

    # Normalize the string
    level_str = level_str.upper().replace("-", "_")

    # Map common variations
    level_map = {
        "ONE": ConsistencyLevel.ONE,
        "TWO": ConsistencyLevel.TWO,
        "THREE": ConsistencyLevel.THREE,
        "QUORUM": ConsistencyLevel.QUORUM,
        "ALL": ConsistencyLevel.ALL,
        "LOCAL_QUORUM": ConsistencyLevel.LOCAL_QUORUM,
        "EACH_QUORUM": ConsistencyLevel.EACH_QUORUM,
        "SERIAL": ConsistencyLevel.SERIAL,
        "LOCAL_SERIAL": ConsistencyLevel.LOCAL_SERIAL,
        "LOCAL_ONE": ConsistencyLevel.LOCAL_ONE,
        "ANY": ConsistencyLevel.ANY,
    }

    if level_str not in level_map:
        raise ValueError(
            f"Invalid consistency level: {level_str}. "
            f"Valid options: {', '.join(level_map.keys())}"
        )

    return level_map[level_str]
