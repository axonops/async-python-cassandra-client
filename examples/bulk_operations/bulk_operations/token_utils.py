"""
Token range utilities for bulk operations.

Handles token range discovery, splitting, and query generation.
"""

from dataclasses import dataclass

from async_cassandra import AsyncCassandraSession

# Murmur3 token range boundaries
MIN_TOKEN = -(2**63)  # -9223372036854775808
MAX_TOKEN = 2**63 - 1  # 9223372036854775807
TOTAL_TOKEN_RANGE = 2**64 - 1  # Total range size


@dataclass
class TokenRange:
    """Represents a token range with replica information."""

    start: int
    end: int
    replicas: list[str]

    @property
    def size(self) -> int:
        """Calculate the size of this token range."""
        if self.end >= self.start:
            return self.end - self.start
        else:
            # Handle wraparound (e.g., 9223372036854775800 to -9223372036854775800)
            return (MAX_TOKEN - self.start) + (self.end - MIN_TOKEN) + 1

    @property
    def fraction(self) -> float:
        """Calculate what fraction of the total ring this range represents."""
        return self.size / TOTAL_TOKEN_RANGE


class TokenRangeSplitter:
    """Splits token ranges for parallel processing."""

    def split_single_range(self, token_range: TokenRange, split_count: int) -> list[TokenRange]:
        """Split a single token range into approximately equal parts."""
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
        self, ranges: list[TokenRange], target_splits: int
    ) -> list[TokenRange]:
        """Split ranges proportionally based on their size."""
        if not ranges:
            return []

        # Calculate total size
        total_size = sum(r.size for r in ranges)

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
        self, ranges: list[TokenRange]
    ) -> dict[tuple[str, ...], list[TokenRange]]:
        """Group ranges by their replica sets."""
        clusters: dict[tuple[str, ...], list[TokenRange]] = {}

        for token_range in ranges:
            # Use sorted tuple as key for consistency
            replica_key = tuple(sorted(token_range.replicas))
            if replica_key not in clusters:
                clusters[replica_key] = []
            clusters[replica_key].append(token_range)

        return clusters


async def discover_token_ranges(session: AsyncCassandraSession, keyspace: str) -> list[TokenRange]:
    """Discover token ranges from cluster metadata."""
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
    partition_keys: list[str],
    token_range: TokenRange,
    columns: list[str] | None = None,
) -> str:
    """Generate a CQL query for a specific token range.

    Note: This function assumes non-wraparound ranges. Wraparound ranges
    (where end < start) should be handled by the caller by splitting them
    into two separate queries.
    """
    # Column selection
    column_list = ", ".join(columns) if columns else "*"

    # Partition key list for token function
    pk_list = ", ".join(partition_keys)

    # Generate token condition
    if token_range.start == MIN_TOKEN:
        # First range uses >= to include minimum token
        token_condition = (
            f"token({pk_list}) >= {token_range.start} AND token({pk_list}) <= {token_range.end}"
        )
    else:
        # All other ranges use > to avoid duplicates
        token_condition = (
            f"token({pk_list}) > {token_range.start} AND token({pk_list}) <= {token_range.end}"
        )

    return f"SELECT {column_list} FROM {keyspace}.{table} WHERE {token_condition}"
