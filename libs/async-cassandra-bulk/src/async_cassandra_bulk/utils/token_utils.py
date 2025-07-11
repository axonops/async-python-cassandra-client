"""
Token range utilities for bulk operations.

Handles token range discovery, splitting, and query generation for
efficient parallel processing of Cassandra tables.
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

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
    replicas: List[str]

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


class TokenRangeSplitter:
    """
    Splits token ranges for parallel processing.

    Provides various strategies for dividing token ranges to enable
    efficient parallel processing while maintaining even workload distribution.
    """

    def split_single_range(self, token_range: TokenRange, split_count: int) -> List[TokenRange]:
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
                # Handle potential overflow
                if current_end > MAX_TOKEN:
                    current_end = current_end - TOTAL_TOKEN_RANGE

            splits.append(
                TokenRange(start=current_start, end=current_end, replicas=token_range.replicas)
            )

            current_start = current_end

        return splits

    def split_proportionally(
        self, ranges: List[TokenRange], target_splits: int
    ) -> List[TokenRange]:
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

        all_splits = []
        for token_range in ranges:
            # Calculate number of splits for this range
            range_fraction = token_range.size / total_size
            range_splits = max(1, round(range_fraction * target_splits))

            # Split the range
            splits = self.split_single_range(token_range, range_splits)
            all_splits.extend(splits)

        return all_splits

    def cluster_by_replicas(
        self, ranges: List[TokenRange]
    ) -> Dict[Tuple[str, ...], List[TokenRange]]:
        """
        Group ranges by their replica sets.

        Enables node-aware scheduling to improve data locality.

        Args:
            ranges: List of ranges to cluster

        Returns:
            Dictionary mapping replica sets to their ranges
        """
        clusters: Dict[Tuple[str, ...], List[TokenRange]] = {}

        for token_range in ranges:
            # Use sorted tuple as key for consistency
            replica_key = tuple(sorted(token_range.replicas))
            if replica_key not in clusters:
                clusters[replica_key] = []
            clusters[replica_key].append(token_range)

        return clusters


async def discover_token_ranges(session: Any, keyspace: str) -> List[TokenRange]:
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
        raise RuntimeError("Token map not available")

    # Get all tokens from the ring
    all_tokens = sorted(token_map.ring)
    if not all_tokens:
        raise RuntimeError("No tokens found in ring")

    ranges = []

    # Create ranges from consecutive tokens
    for i in range(len(all_tokens)):
        start_token = all_tokens[i]
        # Wrap around to first token for the last range
        end_token = all_tokens[(i + 1) % len(all_tokens)]

        # Handle wraparound - last range goes from last token to first token
        if i == len(all_tokens) - 1:
            # This is the wraparound range
            start = start_token.value
            end = all_tokens[0].value
        else:
            start = start_token.value
            end = end_token.value

        # Get replicas for this token
        replicas = token_map.get_replicas(keyspace, start_token)
        replica_addresses = [str(r.address) for r in replicas]

        ranges.append(TokenRange(start=start, end=end, replicas=replica_addresses))

    return ranges


def generate_token_range_query(
    keyspace: str,
    table: str,
    partition_keys: List[str],
    token_range: TokenRange,
    columns: Optional[List[str]] = None,
    writetime_columns: Optional[List[str]] = None,
    ttl_columns: Optional[List[str]] = None,
    clustering_keys: Optional[List[str]] = None,
    counter_columns: Optional[List[str]] = None,
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
        clustering_keys: Optional list of clustering key columns
        counter_columns: Optional list of counter columns to exclude from writetime/TTL

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

    # Build excluded columns set (used for both writetime and TTL)
    # Combine all key columns (partition + clustering)
    key_columns = set(partition_keys)
    if clustering_keys:
        key_columns.update(clustering_keys)

    # Also exclude counter columns from writetime/TTL
    excluded_columns = key_columns.copy()
    if counter_columns:
        excluded_columns.update(counter_columns)

    # Add writetime columns if requested
    if writetime_columns:
        # Handle wildcard writetime request
        if writetime_columns == ["*"]:
            if columns and columns != ["*"]:
                # Get all non-key, non-counter columns from explicit column list
                writetime_cols = [col for col in columns if col not in excluded_columns]
            else:
                # Cannot use wildcard writetime with SELECT *
                # We need explicit columns to know what to get writetime for
                writetime_cols = []
        else:
            # Use specific columns, excluding keys and counters
            # This allows getting writetime for specific columns even with SELECT *
            writetime_cols = [col for col in writetime_columns if col not in excluded_columns]

        # Add WRITETIME() functions
        for col in writetime_cols:
            select_parts.append(f"WRITETIME({col}) AS {col}_writetime")

    # Add TTL columns if requested
    if ttl_columns:
        # Handle wildcard TTL request
        if ttl_columns == ["*"]:
            if columns and columns != ["*"]:
                # Get all non-key, non-counter columns from explicit column list
                ttl_cols = [col for col in columns if col not in excluded_columns]
            else:
                # Cannot use wildcard TTL with SELECT *
                # We need explicit columns to know what to get TTL for
                ttl_cols = []
        else:
            # Use specific columns, excluding keys and counters
            # This allows getting TTL for specific columns even with SELECT *
            ttl_cols = [col for col in ttl_columns if col not in excluded_columns]

        # Add TTL() functions
        for col in ttl_cols:
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


def build_query(
    table: str,
    columns: Optional[List[str]] = None,
    writetime_columns: Optional[List[str]] = None,
    ttl_columns: Optional[List[str]] = None,
    token_range: Optional[TokenRange] = None,
    primary_keys: Optional[List[str]] = None,
) -> str:
    """
    Build a simple CQL query for testing and simple exports.

    Args:
        table: Table name (can include keyspace)
        columns: Optional list of columns to select
        writetime_columns: Optional list of columns to get writetime for
        ttl_columns: Optional list of columns to get TTL for
        token_range: Optional token range (not used in simple query)
        primary_keys: Optional list of primary key columns to exclude

    Returns:
        CQL query string
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
        excluded = set(primary_keys) if primary_keys else set()

        if writetime_columns == ["*"]:
            # Cannot use wildcard with SELECT *
            if columns and columns != ["*"]:
                writetime_cols = [col for col in columns if col not in excluded]
            else:
                select_parts.append("WRITETIME(*)")
                writetime_cols = []
        else:
            writetime_cols = [col for col in writetime_columns if col not in excluded]

        for col in writetime_cols:
            select_parts.append(f"WRITETIME({col}) AS {col}_writetime")

    # Add TTL columns if requested
    if ttl_columns:
        excluded = set(primary_keys) if primary_keys else set()

        if ttl_columns == ["*"]:
            # Cannot use wildcard with SELECT *
            if columns and columns != ["*"]:
                ttl_cols = [col for col in columns if col not in excluded]
            else:
                select_parts.append("TTL(*)")
                ttl_cols = []
        else:
            ttl_cols = [col for col in ttl_columns if col not in excluded]

        for col in ttl_cols:
            select_parts.append(f"TTL({col}) AS {col}_ttl")

    column_list = ", ".join(select_parts)
    return f"SELECT {column_list} FROM {table}"
