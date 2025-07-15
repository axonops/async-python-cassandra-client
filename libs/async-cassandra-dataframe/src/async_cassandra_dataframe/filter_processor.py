"""
Filter processing for DataFrame operations.

Handles writetime filtering, client-side predicates, and partition key validation.
"""

from datetime import UTC, datetime
from typing import Any

import dask.dataframe as dd
import pandas as pd


class FilterProcessor:
    """Processes various filters for Cassandra DataFrame operations."""

    def __init__(self, table_metadata: dict[str, Any]):
        """
        Initialize filter processor.

        Args:
            table_metadata: Cassandra table metadata
        """
        self._table_metadata = table_metadata

    def validate_partition_key_predicates(
        self, predicates: list[dict[str, Any]], require_partition_key: bool
    ) -> None:
        """
        Validate that predicates include partition keys if required.

        Args:
            predicates: List of predicates
            require_partition_key: Whether to enforce partition key presence

        Raises:
            ValueError: If partition keys are missing and enforcement is enabled
        """
        if not require_partition_key or not predicates:
            return

        # Get partition key columns
        partition_keys = self._table_metadata["partition_key"]

        # Check which partition keys have predicates
        predicate_columns = {p["column"] for p in predicates}
        missing_keys = set(partition_keys) - predicate_columns

        if missing_keys:
            raise ValueError(
                f"Predicate pushdown requires all partition keys. "
                f"Missing: {', '.join(sorted(missing_keys))}. "
                f"This would cause a full table scan! "
                f"Either add predicates for these columns or set "
                f"require_partition_key_predicate=False to proceed anyway."
            )

    def normalize_writetime_filter(
        self, filter_spec: dict[str, Any], snapshot_time: datetime | None
    ) -> dict[str, Any]:
        """Normalize and validate writetime filter specification."""
        # Required fields
        if "column" not in filter_spec:
            raise ValueError("writetime_filter must have 'column' field")
        if "operator" not in filter_spec:
            raise ValueError("writetime_filter must have 'operator' field")
        if "timestamp" not in filter_spec:
            raise ValueError("writetime_filter must have 'timestamp' field")

        # Validate operator
        valid_operators = [">", ">=", "<", "<=", "==", "!="]
        if filter_spec["operator"] not in valid_operators:
            raise ValueError(f"Invalid operator. Must be one of: {valid_operators}")

        # Process timestamp
        timestamp = filter_spec["timestamp"]
        if timestamp == "now":
            if snapshot_time:
                timestamp = snapshot_time
            else:
                timestamp = datetime.now(UTC)
        elif isinstance(timestamp, str):
            timestamp = pd.Timestamp(timestamp).to_pydatetime()

        # Ensure timezone aware
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=UTC)

        return {
            "column": filter_spec["column"],
            "operator": filter_spec["operator"],
            "timestamp": timestamp,
            "timestamp_micros": int(timestamp.timestamp() * 1_000_000),
        }

    def apply_writetime_filter(
        self, df: dd.DataFrame, writetime_filter: dict[str, Any]
    ) -> dd.DataFrame:
        """Apply writetime filtering to DataFrame."""
        operator = writetime_filter["operator"]
        timestamp = writetime_filter["timestamp"]

        # Build filter expression for each column
        filter_mask = None
        for col in writetime_filter["columns"]:
            col_writetime = f"{col}_writetime"
            if col_writetime not in df.columns:
                continue

            # Create column filter
            if operator == ">":
                col_mask = df[col_writetime] > timestamp
            elif operator == ">=":
                col_mask = df[col_writetime] >= timestamp
            elif operator == "<":
                col_mask = df[col_writetime] < timestamp
            elif operator == "<=":
                col_mask = df[col_writetime] <= timestamp
            elif operator == "==":
                col_mask = df[col_writetime] == timestamp
            elif operator == "!=":
                col_mask = df[col_writetime] != timestamp

            # Combine with OR logic (any column matching is included)
            if filter_mask is None:
                filter_mask = col_mask
            else:
                filter_mask = filter_mask | col_mask

        # Apply filter
        if filter_mask is not None:
            df = df[filter_mask]

        return df

    def apply_client_predicates(self, df: dd.DataFrame, predicates: list[Any]) -> dd.DataFrame:
        """Apply client-side predicates to DataFrame."""
        from decimal import Decimal

        for pred in predicates:
            col = pred.column
            op = pred.operator
            val = pred.value

            # For numeric comparisons with Decimal columns, ensure compatible types
            col_info = next((c for c in self._table_metadata["columns"] if c["name"] == col), None)
            if col_info and str(col_info["type"]) == "decimal" and isinstance(val, int | float):
                # Convert numeric value to Decimal for comparison
                val = Decimal(str(val))

            if op == "=":
                df = df[df[col] == val]
            elif op == "!=":
                df = df[df[col] != val]
            elif op == ">":
                df = df[df[col] > val]
            elif op == ">=":
                df = df[df[col] >= val]
            elif op == "<":
                df = df[df[col] < val]
            elif op == "<=":
                df = df[df[col] <= val]
            elif op == "IN":
                df = df[df[col].isin(val)]
            else:
                raise ValueError(f"Unsupported operator for client-side filtering: {op}")

        return df
