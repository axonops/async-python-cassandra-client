"""
Token range utilities for distributed Cassandra reads.

Handles token range discovery, splitting, and query generation for
efficient parallel processing of Cassandra tables.
"""

from dataclasses import dataclass
from typing import Any

# Murmur3 token range boundaries
MIN_TOKEN = -(2**63)  # -9223372036854775808
MAX_TOKEN = 2**63 - 1  # 9223372036854775807
TOTAL_TOKEN_RANGE = 2**64 - 1  # Total range size


@dataclass
class TokenRange:
    """
    Represents a token range with replica information.

    Token ranges define a portion of the Cassandra ring and track
    which nodes hold replicas for that range.
    """

    start: int
    end: int
    replicas: list[str]

    @property
    def size(self) -> int:
        """
        Calculate the size of this token range.

        Handles wraparound ranges where end < start (e.g., the last
        range that wraps from near MAX_TOKEN to near MIN_TOKEN).
        """
        if self.end >= self.start:
            return self.end - self.start
        else:
            # Handle wraparound
            return (MAX_TOKEN - self.start) + (self.end - MIN_TOKEN) + 1

    @property
    def fraction(self) -> float:
        """
        Calculate what fraction of the total ring this range represents.

        Used for proportional splitting and progress tracking.
        """
        return self.size / TOTAL_TOKEN_RANGE

    @property
    def is_wraparound(self) -> bool:
        """Check if this is a wraparound range."""
        return self.end < self.start

    def contains_token(self, token: int) -> bool:
        """Check if a token falls within this range."""
        if not self.is_wraparound:
            return self.start <= token <= self.end
        else:
            # Wraparound: token is either after start OR before end
            return token >= self.start or token <= self.end


async def discover_token_ranges(session: Any, keyspace: str) -> list[TokenRange]:
    """
    Discover token ranges from cluster metadata.

    Queries the cluster topology to build a complete map of token ranges
    and their replica nodes.

    Args:
        session: AsyncCassandraSession instance
        keyspace: Keyspace to get replica information for

    Returns:
        List of token ranges covering the entire ring

    Raises:
        RuntimeError: If token map is not available
    """
    # Access cluster through the underlying sync session
    cluster = session._session.cluster
    metadata = cluster.metadata
    token_map = metadata.token_map

    if not token_map:
        raise RuntimeError(
            "Token map not available. This may be due to insufficient permissions "
            "or cluster configuration. Ensure the user has DESCRIBE permission."
        )

    # Get all tokens from the ring
    all_tokens = sorted(token_map.ring)
    if not all_tokens:
        raise RuntimeError("No tokens found in ring")

    ranges = []

    # For single-node clusters, we might only have one token
    # In this case, create a range covering the entire ring
    if len(all_tokens) == 1:
        # Single token - create full ring range
        ranges.append(
            TokenRange(
                start=MIN_TOKEN,
                end=MAX_TOKEN,
                replicas=[str(r.address) for r in token_map.get_replicas(keyspace, all_tokens[0])],
            )
        )
    else:
        # Create ranges from consecutive tokens
        for i in range(len(all_tokens)):
            if i == 0:
                # First range: from MIN_TOKEN to first token
                start = MIN_TOKEN
                end = all_tokens[i].value
            else:
                # Other ranges: from previous token to current token
                start = all_tokens[i - 1].value
                end = all_tokens[i].value

            # Get replicas for this token
            replicas = token_map.get_replicas(keyspace, all_tokens[i])
            replica_addresses = [str(r.address) for r in replicas]

            ranges.append(TokenRange(start=start, end=end, replicas=replica_addresses))

        # Add final range from last token to MAX_TOKEN
        if all_tokens:
            last_replicas = token_map.get_replicas(keyspace, all_tokens[-1])
            ranges.append(
                TokenRange(
                    start=all_tokens[-1].value,
                    end=MAX_TOKEN,
                    replicas=[str(r.address) for r in last_replicas],
                )
            )

    return ranges


def split_proportionally(ranges: list[TokenRange], target_splits: int) -> list[TokenRange]:
    """
    Split ranges proportionally based on their size.

    Larger ranges get more splits to ensure even data distribution.

    Args:
        ranges: List of ranges to split
        target_splits: Target total number of splits

    Returns:
        List of split ranges
    """
    if not ranges:
        return []

    # Calculate total size
    total_size = sum(r.size for r in ranges)
    if total_size == 0:
        return ranges

    splitter = TokenRangeSplitter()
    all_splits = []

    for token_range in ranges:
        # Calculate number of splits for this range
        range_fraction = token_range.size / total_size
        range_splits = max(1, round(range_fraction * target_splits))

        # Split the range
        splits = splitter.split_single_range(token_range, range_splits)
        all_splits.extend(splits)

    return all_splits


def handle_wraparound_ranges(ranges: list[TokenRange]) -> list[TokenRange]:
    """
    Handle wraparound ranges by splitting them.

    Wraparound ranges (where end < start) need to be split into
    two separate ranges for proper querying.

    Args:
        ranges: List of ranges that may include wraparound

    Returns:
        List of ranges with wraparound ranges split
    """
    result = []

    for range in ranges:
        if range.is_wraparound:
            # Split into two ranges
            # First part: from start to MAX_TOKEN
            first_part = TokenRange(start=range.start, end=MAX_TOKEN, replicas=range.replicas)

            # Second part: from MIN_TOKEN to end
            second_part = TokenRange(start=MIN_TOKEN, end=range.end, replicas=range.replicas)

            result.extend([first_part, second_part])
        else:
            # Normal range
            result.append(range)

    return result


def generate_token_range_query(
    keyspace: str,
    table: str,
    partition_keys: list[str],
    token_range: TokenRange,
    columns: list[str] | None = None,
    writetime_columns: list[str] | None = None,
    ttl_columns: list[str] | None = None,
) -> str:
    """
    Generate a CQL query for a specific token range.

    Creates a SELECT query that retrieves all rows within the specified
    token range. Handles the special case of the minimum token to ensure
    no data is missed.

    Args:
        keyspace: Keyspace name
        table: Table name
        partition_keys: List of partition key columns
        token_range: Token range to query
        columns: Optional list of columns to select (default: all)
        writetime_columns: Optional list of columns to get writetime for
        ttl_columns: Optional list of columns to get TTL for

    Returns:
        CQL query string

    Note:
        This function assumes non-wraparound ranges. Wraparound ranges
        (where end < start) should be handled by the caller by splitting
        them into two separate queries.
    """
    # Build column selection list
    select_parts = []

    # Add regular columns
    if columns:
        select_parts.extend(columns)
    else:
        select_parts.append("*")

    # Add writetime columns if requested
    if writetime_columns:
        for col in writetime_columns:
            select_parts.append(f"WRITETIME({col}) AS {col}_writetime")

    # Add TTL columns if requested
    if ttl_columns:
        for col in ttl_columns:
            select_parts.append(f"TTL({col}) AS {col}_ttl")

    column_list = ", ".join(select_parts)

    # Partition key list for token function
    pk_list = ", ".join(partition_keys)

    # Generate token condition
    if token_range.start == MIN_TOKEN:
        # First range uses >= to include minimum token
        token_condition = (
            f"token({pk_list}) >= {token_range.start} AND " f"token({pk_list}) <= {token_range.end}"
        )
    else:
        # All other ranges use > to avoid duplicates
        token_condition = (
            f"token({pk_list}) > {token_range.start} AND " f"token({pk_list}) <= {token_range.end}"
        )

    return f"SELECT {column_list} FROM {keyspace}.{table} WHERE {token_condition}"


class TokenRangeSplitter:
    """
    Splits token ranges for parallel processing.

    Provides various strategies for dividing token ranges to enable
    efficient parallel processing while maintaining even workload distribution.
    """

    def split_single_range(self, token_range: TokenRange, split_count: int) -> list[TokenRange]:
        """
        Split a single token range into approximately equal parts.

        Args:
            token_range: The range to split
            split_count: Number of desired splits

        Returns:
            List of split ranges that cover the original range
        """
        if split_count <= 1:
            return [token_range]

        # Don't split wraparound ranges directly
        if token_range.is_wraparound:
            # First split the wraparound
            non_wrap = handle_wraparound_ranges([token_range])
            # Then split each part
            result = []
            for part in non_wrap:
                # Distribute splits proportionally
                part_splits = max(1, split_count // len(non_wrap))
                result.extend(self.split_single_range(part, part_splits))
            return result

        # Calculate split size
        split_size = token_range.size // split_count
        if split_size < 1:
            # Range too small to split further
            return [token_range]

        splits = []
        current_start = token_range.start

        for i in range(split_count):
            if i == split_count - 1:
                # Last split gets any remainder
                current_end = token_range.end
            else:
                current_end = current_start + split_size

            splits.append(
                TokenRange(start=current_start, end=current_end, replicas=token_range.replicas)
            )

            current_start = current_end

        return splits
