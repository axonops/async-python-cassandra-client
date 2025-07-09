"""Apache Iceberg integration for Cassandra bulk operations.

This module provides functionality to export Cassandra data to Apache Iceberg tables,
enabling modern data lakehouse capabilities including:
- ACID transactions
- Schema evolution
- Time travel
- Hidden partitioning
- Efficient analytics
"""

from bulk_operations.iceberg.exporter import IcebergExporter
from bulk_operations.iceberg.schema_mapper import CassandraToIcebergSchemaMapper

__all__ = ["IcebergExporter", "CassandraToIcebergSchemaMapper"]
