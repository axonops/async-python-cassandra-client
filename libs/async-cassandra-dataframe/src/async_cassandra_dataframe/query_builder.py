"""
Query builder for Cassandra DataFrame operations.

Constructs CQL queries with proper column selection, writetime/TTL support,
and token range filtering.
"""

from typing import Any


class QueryBuilder:
    """
    Builds CQL queries for DataFrame operations.

    CRITICAL:
    - Always use prepared statements
    - Never use SELECT *
    - Handle writetime/TTL columns properly
    """

    def __init__(self, table_metadata: dict[str, Any]):
        """
        Initialize with table metadata.

        Args:
            table_metadata: Processed table metadata
        """
        self.table_metadata = table_metadata
        self.keyspace = table_metadata["keyspace"]
        self.table = table_metadata["table"]
        self.primary_key = table_metadata["primary_key"]

    def build_partition_query(
        self,
        columns: list[str] | None = None,
        token_range: tuple[int, int] | None = None,
        writetime_columns: list[str] | None = None,
        ttl_columns: list[str] | None = None,
        limit: int | None = None,
        predicates: list[dict[str, Any]] | None = None,
        allow_filtering: bool = False,
    ) -> tuple[str, list[Any]]:
        """
        Build query for reading a partition.

        Args:
            columns: Columns to select (None = all)
            token_range: Token range for this partition
            writetime_columns: Columns to get writetime for
            ttl_columns: Columns to get TTL for
            limit: Row limit
            predicates: List of predicates to apply
            allow_filtering: Whether to add ALLOW FILTERING

        Returns:
            Tuple of (query_string, parameters)
        """
        # Build SELECT clause
        select_columns = self._build_select_clause(columns, writetime_columns, ttl_columns)

        # Build FROM clause
        from_clause = f"{self.keyspace}.{self.table}"

        # Build WHERE clause
        where_clause, params = self._build_where_clause(token_range, predicates)

        # Build complete query
        query_parts = [
            "SELECT",
            select_columns,
            "FROM",
            from_clause,
        ]

        if where_clause:
            query_parts.extend(["WHERE", where_clause])

        if allow_filtering and predicates:
            query_parts.append("ALLOW FILTERING")

        if limit:
            query_parts.extend(["LIMIT", str(limit)])

        query = " ".join(query_parts)

        return query, params

    def _build_select_clause(
        self,
        columns: list[str] | None,
        writetime_columns: list[str] | None,
        ttl_columns: list[str] | None,
    ) -> str:
        """
        Build SELECT column list.

        CRITICAL: Never use SELECT *, always explicit columns.
        """
        # Get base columns
        if columns:
            # Use specified columns
            base_columns = columns
        else:
            # Use all columns from metadata
            base_columns = [col["name"] for col in self.table_metadata["columns"]]

        # Start with base columns
        select_parts = list(base_columns)

        # Add writetime columns
        if writetime_columns:
            for col in writetime_columns:
                if col in base_columns and col not in self.primary_key:
                    # Add writetime function
                    select_parts.append(f"WRITETIME({col}) AS {col}_writetime")

        # Add TTL columns
        if ttl_columns:
            for col in ttl_columns:
                if col in base_columns and col not in self.primary_key:
                    # Add TTL function
                    select_parts.append(f"TTL({col}) AS {col}_ttl")

        return ", ".join(select_parts)

    def _build_where_clause(
        self,
        token_range: tuple[int, int] | None,
        predicates: list[dict[str, Any]] | None = None,
    ) -> tuple[str, list[Any]]:
        """
        Build WHERE clause for token range filtering and predicates.

        Args:
            token_range: Token range to filter
            predicates: List of predicates to apply

        Returns:
            Tuple of (where_clause, parameters)
        """
        clauses = []
        params = []

        # Add token range if specified
        if token_range:
            # Get partition key columns
            partition_keys = self.table_metadata["partition_key"]
            if partition_keys:
                # Build token function with partition keys
                token_func = f"TOKEN({', '.join(partition_keys)})"
                clauses.append(f"{token_func} >= ? AND {token_func} <= ?")
                params.extend([token_range[0], token_range[1]])

        # Add predicates
        if predicates:
            for pred in predicates:
                col = pred["column"]
                op = pred["operator"]
                val = pred["value"]

                if op == "IN":
                    placeholders = ", ".join(["?" for _ in val])
                    clauses.append(f"{col} IN ({placeholders})")
                    params.extend(val)
                else:
                    clauses.append(f"{col} {op} ?")
                    params.append(val)

        if not clauses:
            return "", []

        where_clause = " AND ".join(clauses)
        return where_clause, params

    def build_count_query(
        self,
        token_range: tuple[int, int] | None = None,
    ) -> tuple[str, list[Any]]:
        """
        Build query for counting rows in partition.

        Args:
            token_range: Token range to count

        Returns:
            Tuple of (query_string, parameters)
        """
        # Build WHERE clause
        where_clause, params = self._build_where_clause(token_range)

        # Build query
        query_parts = [
            "SELECT COUNT(*) FROM",
            f"{self.keyspace}.{self.table}",
        ]

        if where_clause:
            query_parts.extend(["WHERE", where_clause])

        query = " ".join(query_parts)

        return query, params

    def build_sample_query(
        self,
        columns: list[str] | None = None,
        sample_size: int = 1000,
    ) -> str:
        """
        Build query for sampling data.

        Used for schema inference and type detection.

        Args:
            columns: Columns to sample
            sample_size: Number of rows to sample

        Returns:
            Query string
        """
        # Build SELECT clause
        if columns:
            select_clause = ", ".join(columns)
        else:
            # Get all columns
            all_columns = [col["name"] for col in self.table_metadata["columns"]]
            select_clause = ", ".join(all_columns)

        # Build query with LIMIT
        query = f"""
            SELECT {select_clause}
            FROM {self.keyspace}.{self.table}
            LIMIT {sample_size}
        """

        return query.strip()

    def validate_columns(self, columns: list[str]) -> list[str]:
        """
        Validate that requested columns exist.

        Args:
            columns: Column names to validate

        Returns:
            List of valid column names

        Raises:
            ValueError: If any columns don't exist
        """
        # Get all column names
        valid_columns = {col["name"] for col in self.table_metadata["columns"]}

        # Check each requested column
        invalid = []
        for col in columns:
            if col not in valid_columns:
                invalid.append(col)

        if invalid:
            raise ValueError(
                f"Column(s) not found in table {self.keyspace}.{self.table}: "
                f"{', '.join(invalid)}"
            )

        return columns
