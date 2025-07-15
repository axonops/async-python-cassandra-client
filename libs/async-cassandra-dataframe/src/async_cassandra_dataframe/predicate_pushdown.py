"""
Predicate pushdown analyzer for Cassandra queries.

Determines which predicates can be efficiently pushed to Cassandra
based on table schema and CQL limitations.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Any


class PredicateType(Enum):
    """Types of predicates that can be pushed down."""

    PARTITION_KEY = "partition_key"
    CLUSTERING_KEY = "clustering_key"
    REGULAR_COLUMN = "regular_column"
    INDEXED_COLUMN = "indexed_column"


@dataclass
class Predicate:
    """Represents a query predicate."""

    column: str
    operator: str  # =, <, >, <=, >=, IN, CONTAINS
    value: Any
    predicate_type: PredicateType | None = None


class PredicatePushdownAnalyzer:
    """
    Analyzes which predicates can be pushed down to Cassandra.

    Cassandra query restrictions:
    1. Partition key columns must use = or IN
    2. Clustering columns can use range operators but must be in order
    3. Regular columns require ALLOW FILTERING or secondary indexes
    4. Token ranges conflict with partition key predicates
    """

    def __init__(self, table_metadata: dict):
        """
        Initialize with table metadata.

        Args:
            table_metadata: Table metadata including keys and indexes
        """
        self.table_metadata = table_metadata
        self.partition_keys = table_metadata.get("partition_key", [])
        self.clustering_keys = table_metadata.get("clustering_key", [])
        self.indexed_columns = self._extract_indexed_columns()

    def _extract_indexed_columns(self) -> set[str]:
        """Extract columns that have secondary indexes."""
        indexed_columns = set()

        # Check for index information in column metadata
        for column in self.table_metadata.get("columns", []):
            # Check if column has an index
            if column.get("index_name") or column.get("has_index"):
                indexed_columns.add(column["name"])

        # Also check for explicit indexes in table metadata
        indexes = self.table_metadata.get("indexes", {})
        for _index_name, index_info in indexes.items():
            if isinstance(index_info, dict) and "column" in index_info:
                indexed_columns.add(index_info["column"])

        return indexed_columns

    def analyze_predicates(
        self, predicates: list[dict[str, Any]], use_token_ranges: bool = True
    ) -> tuple[list[Predicate], list[Predicate], bool]:
        """
        Analyze predicates and determine pushdown strategy.

        Args:
            predicates: List of predicate dictionaries
            use_token_ranges: Whether to use token ranges for partitioning

        Returns:
            Tuple of:
            - Predicates that can be pushed to Cassandra
            - Predicates that must be applied client-side
            - Whether token ranges can be used
        """
        if not predicates:
            return [], [], use_token_ranges

        # Convert to Predicate objects and classify
        classified_predicates = []
        for pred_dict in predicates:
            pred = Predicate(
                column=pred_dict["column"], operator=pred_dict["operator"], value=pred_dict["value"]
            )
            pred.predicate_type = self._classify_predicate(pred)
            classified_predicates.append(pred)

        # Analyze pushdown feasibility
        pushdown = []
        client_side = []
        can_use_tokens = use_token_ranges

        # Check partition key predicates
        pk_predicates = [
            p for p in classified_predicates if p.predicate_type == PredicateType.PARTITION_KEY
        ]

        if pk_predicates:
            # If we have partition key predicates, analyze them
            if self._has_complete_partition_key(pk_predicates):
                # Full partition key specified - most efficient query
                pushdown.extend(pk_predicates)
                can_use_tokens = False  # Don't need token ranges

                # Now we can also push down clustering key predicates
                ck_predicates = [
                    p
                    for p in classified_predicates
                    if p.predicate_type == PredicateType.CLUSTERING_KEY
                ]

                if ck_predicates:
                    # Check if clustering predicates are valid
                    valid_ck, invalid_ck = self._validate_clustering_predicates(ck_predicates)
                    pushdown.extend(valid_ck)
                    client_side.extend(invalid_ck)
            else:
                # Partial partition key - need token ranges
                # These predicates go client-side
                client_side.extend(pk_predicates)

        # Handle other predicates
        for pred in classified_predicates:
            if pred in pushdown or pred in client_side:
                continue

            if pred.predicate_type == PredicateType.INDEXED_COLUMN:
                # Can push down indexed column predicates
                pushdown.append(pred)
            else:
                # Regular columns go client-side
                client_side.append(pred)

        return pushdown, client_side, can_use_tokens

    def _classify_predicate(self, predicate: Predicate) -> PredicateType:
        """Classify predicate based on column type."""
        if predicate.column in self.partition_keys:
            return PredicateType.PARTITION_KEY
        elif predicate.column in self.clustering_keys:
            return PredicateType.CLUSTERING_KEY
        elif predicate.column in self.indexed_columns:
            return PredicateType.INDEXED_COLUMN
        else:
            return PredicateType.REGULAR_COLUMN

    def _has_complete_partition_key(self, pk_predicates: list[Predicate]) -> bool:
        """
        Check if predicates specify complete partition key.

        All partition key columns must have equality predicates or IN.
        """
        pk_columns = {p.column for p in pk_predicates if p.operator in ("=", "IN")}
        return pk_columns == set(self.partition_keys)

    def _validate_clustering_predicates(
        self, ck_predicates: list[Predicate]
    ) -> tuple[list[Predicate], list[Predicate]]:
        """
        Validate clustering key predicates.

        Rules:
        1. Must be in clustering column order
        2. Can't skip columns
        3. Only last column can use range operators

        Returns:
            Tuple of (valid_predicates, invalid_predicates)
        """
        valid = []
        invalid = []

        # Sort by clustering key order
        ck_order = {col: i for i, col in enumerate(self.clustering_keys)}
        sorted_preds = sorted(ck_predicates, key=lambda p: ck_order.get(p.column, 999))

        # Check order and operators
        for i, pred in enumerate(sorted_preds):
            expected_col = self.clustering_keys[i] if i < len(self.clustering_keys) else None

            if pred.column != expected_col:
                # Skipped a clustering column - rest are invalid
                invalid.extend(sorted_preds[i:])
                break

            if i < len(sorted_preds) - 1 and pred.operator != "=":
                # Non-equality on non-last clustering column
                invalid.extend(sorted_preds[i:])
                break

            valid.append(pred)

        return valid, invalid

    def build_where_clause(
        self,
        pushdown_predicates: list[Predicate],
        token_range: tuple[int, int] | None = None,
        allow_filtering: bool = False,
    ) -> tuple[str, list[Any]]:
        """
        Build WHERE clause from predicates.

        Args:
            pushdown_predicates: Predicates to include in WHERE clause
            token_range: Optional token range for partitioning
            allow_filtering: Whether to add ALLOW FILTERING

        Returns:
            Tuple of (where_clause, parameters)
        """
        conditions = []
        params: list[Any] = []

        # Add token range if specified
        if token_range:
            pk_cols = ", ".join(self.partition_keys)
            conditions.append(f"TOKEN({pk_cols}) >= ?")
            conditions.append(f"TOKEN({pk_cols}) <= ?")
            params.extend(token_range)

        # Add predicates
        for pred in pushdown_predicates:
            if pred.operator == "IN":
                placeholders = ", ".join(["?"] * len(pred.value))
                conditions.append(f"{pred.column} IN ({placeholders})")
                params.extend(pred.value)
            else:
                conditions.append(f"{pred.column} {pred.operator} ?")
                params.append(pred.value)

        where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""

        if allow_filtering and where_clause:
            where_clause += " ALLOW FILTERING"

        return where_clause, params
