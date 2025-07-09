"""Iceberg catalog configuration for filesystem-based tables."""

from pathlib import Path
from typing import Any

from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.catalog.sql import SqlCatalog


def create_filesystem_catalog(
    name: str = "cassandra_export",
    warehouse_path: str | Path | None = None,
) -> Catalog:
    """Create a filesystem-based Iceberg catalog.

    What this does:
    --------------
    1. Creates a local filesystem catalog using SQLite
    2. Stores table metadata in SQLite database
    3. Stores actual data files in warehouse directory
    4. No external dependencies (S3, Hive, etc.)

    Why this matters:
    ----------------
    - Simple setup for development and testing
    - No cloud dependencies
    - Easy to inspect and debug
    - Can be migrated to production catalogs later

    Args:
        name: Catalog name
        warehouse_path: Path to warehouse directory (default: ./iceberg_warehouse)

    Returns:
        Iceberg catalog instance
    """
    if warehouse_path is None:
        warehouse_path = Path.cwd() / "iceberg_warehouse"
    else:
        warehouse_path = Path(warehouse_path)

    # Create warehouse directory if it doesn't exist
    warehouse_path.mkdir(parents=True, exist_ok=True)

    # SQLite catalog configuration
    catalog_config = {
        "type": "sql",
        "uri": f"sqlite:///{warehouse_path / 'catalog.db'}",
        "warehouse": str(warehouse_path),
    }

    # Create catalog
    catalog = SqlCatalog(name, **catalog_config)

    return catalog


def get_or_create_catalog(
    catalog_name: str = "cassandra_export",
    warehouse_path: str | Path | None = None,
    config: dict[str, Any] | None = None,
) -> Catalog:
    """Get existing catalog or create a new one.

    This allows for custom catalog configurations while providing
    sensible defaults for filesystem-based catalogs.

    Args:
        catalog_name: Name of the catalog
        warehouse_path: Path to warehouse (for filesystem catalogs)
        config: Custom catalog configuration (overrides defaults)

    Returns:
        Iceberg catalog instance
    """
    if config is not None:
        # Use custom configuration
        return load_catalog(catalog_name, **config)
    else:
        # Use filesystem catalog
        return create_filesystem_catalog(catalog_name, warehouse_path)
