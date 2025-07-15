"""
Table metadata handling for Cassandra DataFrames.

Extracts and processes Cassandra table metadata for DataFrame operations.
"""

from typing import Any

from cassandra.metadata import ColumnMetadata, TableMetadata


class TableMetadataExtractor:
    """
    Extracts and processes Cassandra table metadata.

    Provides information about:
    - Column types and properties
    - Primary key structure
    - Writetime/TTL support
    - Token ranges
    """

    def __init__(self, session):
        """
        Initialize with async-cassandra session.

        Args:
            session: AsyncSession instance
        """
        self.session = session
        # Access underlying sync session for metadata
        self._sync_session = session._session
        self._cluster = self._sync_session.cluster

    async def get_table_metadata(self, keyspace: str, table: str) -> dict[str, Any]:
        """
        Get comprehensive table metadata.

        Args:
            keyspace: Keyspace name
            table: Table name

        Returns:
            Dict with table metadata including columns, keys, etc.
        """
        # Get table metadata from cluster
        keyspace_meta = self._cluster.metadata.keyspaces.get(keyspace)
        if not keyspace_meta:
            raise ValueError(f"Keyspace '{keyspace}' not found")

        table_meta = keyspace_meta.tables.get(table)
        if not table_meta:
            raise ValueError(f"Table '{keyspace}.{table}' not found")

        return self._process_table_metadata(table_meta)

    def _process_table_metadata(self, table_meta: TableMetadata) -> dict[str, Any]:
        """Process raw table metadata into structured format."""
        # Extract column information
        columns = []
        partition_keys = set()
        clustering_keys = set()

        # Process partition keys
        for col in table_meta.partition_key:
            partition_keys.add(col.name)
            columns.append(self._process_column(col, is_partition_key=True))

        # Process clustering keys
        for col in table_meta.clustering_key:
            clustering_keys.add(col.name)
            columns.append(self._process_column(col, is_clustering_key=True))

        # Process regular columns
        for col_name, col_meta in table_meta.columns.items():
            if col_name not in partition_keys and col_name not in clustering_keys:
                columns.append(self._process_column(col_meta))

        return {
            "keyspace": table_meta.keyspace_name,
            "table": table_meta.name,
            "columns": columns,
            "partition_key": [col.name for col in table_meta.partition_key],
            "clustering_key": [col.name for col in table_meta.clustering_key],
            "primary_key": self._get_primary_key(table_meta),
            "options": table_meta.options,
        }

    def _process_column(
        self, col: ColumnMetadata, is_partition_key: bool = False, is_clustering_key: bool = False
    ) -> dict[str, Any]:
        """Process column metadata."""
        return {
            "name": col.name,
            "type": col.cql_type,
            "is_primary_key": is_partition_key or is_clustering_key,
            "is_partition_key": is_partition_key,
            "is_clustering_key": is_clustering_key,
            "is_static": col.is_static,
            "is_reversed": col.is_reversed,
            # Writetime/TTL support
            "supports_writetime": self._supports_writetime(
                col, is_partition_key, is_clustering_key
            ),
            "supports_ttl": self._supports_ttl(col, is_partition_key, is_clustering_key),
        }

    def _supports_writetime(self, col: ColumnMetadata, is_pk: bool, is_ck: bool) -> bool:
        """
        Check if column supports writetime.

        Primary key columns, counters, and UDTs don't support writetime.
        """
        if is_pk or is_ck:
            return False

        col_type_str = str(col.cql_type)

        # Counter columns don't support writetime
        if col_type_str == "counter":
            return False

        # Only direct UDT columns don't support writetime
        # Collections of UDTs do support writetime on the collection itself
        if self._is_direct_udt_type(col_type_str):
            return False

        return True

    def _supports_ttl(self, col: ColumnMetadata, is_pk: bool, is_ck: bool) -> bool:
        """
        Check if column supports TTL.

        Primary key columns and counters don't support TTL.
        """
        if is_pk or is_ck:
            return False

        # Counter columns don't support TTL
        if str(col.cql_type) == "counter":
            return False

        return True

    def _get_primary_key(self, table_meta: TableMetadata) -> list[str]:
        """Get full primary key (partition + clustering)."""
        pk = [col.name for col in table_meta.partition_key]
        pk.extend([col.name for col in table_meta.clustering_key])
        return pk

    def _is_udt_type(self, col_type_str: str) -> bool:
        """
        Check if a column type is a UDT.

        Args:
            col_type_str: String representation of column type

        Returns:
            True if the type is a UDT
        """
        # Remove frozen wrapper if present
        type_str = col_type_str
        if type_str.startswith("frozen<") and type_str.endswith(">"):
            type_str = type_str[7:-1]

        # Check if it's a collection of UDTs
        if any(type_str.startswith(prefix) for prefix in ["list<", "set<", "map<"]):
            # Extract inner types
            inner = type_str[type_str.index("<") + 1 : -1]
            # For maps, check both key and value types
            if type_str.startswith("map<"):
                parts = inner.split(",", 1)
                if len(parts) == 2:
                    return self._is_udt_type(parts[0].strip()) or self._is_udt_type(
                        parts[1].strip()
                    )
            else:
                return self._is_udt_type(inner)

        # Check if it's a vector type (vector<type, dimensions>)
        if type_str.startswith("vector<"):
            return False

        # It's a UDT if it's not a known Cassandra type
        return type_str not in {
            "ascii",
            "bigint",
            "blob",
            "boolean",
            "counter",
            "date",
            "decimal",
            "double",
            "duration",
            "float",
            "inet",
            "int",
            "smallint",
            "text",
            "time",
            "timestamp",
            "timeuuid",
            "tinyint",
            "uuid",
            "varchar",
            "varint",
            "tuple",
        }

    def _is_direct_udt_type(self, col_type_str: str) -> bool:
        """
        Check if a column is directly a UDT (not a collection containing UDTs).

        Args:
            col_type_str: String representation of column type

        Returns:
            True if the column itself is a UDT (not a collection of UDTs)
        """
        # Remove frozen wrapper if present
        type_str = col_type_str
        if type_str.startswith("frozen<") and type_str.endswith(">"):
            type_str = type_str[7:-1]

        # If it's a collection, it's not a direct UDT
        if any(type_str.startswith(prefix) for prefix in ["list<", "set<", "map<"]):
            return False

        # Check if it's a vector type (vector<type, dimensions>)
        if type_str.startswith("vector<"):
            return False

        # It's a UDT if it's not a known Cassandra type
        return type_str not in {
            "ascii",
            "bigint",
            "blob",
            "boolean",
            "counter",
            "date",
            "decimal",
            "double",
            "duration",
            "float",
            "inet",
            "int",
            "smallint",
            "text",
            "time",
            "timestamp",
            "timeuuid",
            "tinyint",
            "uuid",
            "varchar",
            "varint",
            "tuple",
        }

    def get_writetime_capable_columns(self, table_metadata: dict[str, Any]) -> list[str]:
        """
        Get list of columns that support writetime.

        Args:
            table_metadata: Processed table metadata

        Returns:
            List of column names that support writetime
        """
        return [col["name"] for col in table_metadata["columns"] if col["supports_writetime"]]

    def get_ttl_capable_columns(self, table_metadata: dict[str, Any]) -> list[str]:
        """
        Get list of columns that support TTL.

        Args:
            table_metadata: Processed table metadata

        Returns:
            List of column names that support TTL
        """
        return [col["name"] for col in table_metadata["columns"] if col["supports_ttl"]]

    def expand_column_wildcards(
        self,
        columns: list[str] | None,
        table_metadata: dict[str, Any],
        writetime_capable_only: bool = False,
        ttl_capable_only: bool = False,
    ) -> list[str]:
        """
        Expand column wildcards like "*" to actual column names.

        Args:
            columns: List of column names (may include "*")
            table_metadata: Table metadata
            writetime_capable_only: Only return writetime-capable columns
            ttl_capable_only: Only return TTL-capable columns

        Returns:
            Expanded list of column names
        """
        if not columns:
            return []

        # Get all possible columns based on filters
        if writetime_capable_only:
            all_columns = self.get_writetime_capable_columns(table_metadata)
        elif ttl_capable_only:
            all_columns = self.get_ttl_capable_columns(table_metadata)
        else:
            all_columns = [col["name"] for col in table_metadata["columns"]]

        # Handle wildcard
        if "*" in columns:
            return all_columns

        # Filter to requested columns that exist
        all_columns_set = set(all_columns)
        return [col for col in columns if col in all_columns_set]
