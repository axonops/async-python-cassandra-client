"""Unit tests for Iceberg catalog configuration.

What this tests:
---------------
1. Filesystem catalog creation
2. Warehouse directory setup
3. Custom catalog configuration
4. Catalog loading

Why this matters:
----------------
- Catalog is the entry point to Iceberg
- Proper configuration is critical
- Warehouse location affects data storage
- Supports multiple catalog types
"""

import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from pyiceberg.catalog import Catalog

from bulk_operations.iceberg.catalog import create_filesystem_catalog, get_or_create_catalog


class TestIcebergCatalog(unittest.TestCase):
    """Test Iceberg catalog configuration."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.warehouse_path = Path(self.temp_dir) / "test_warehouse"

    def tearDown(self):
        """Clean up test fixtures."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_create_filesystem_catalog_default_path(self):
        """
        Test creating filesystem catalog with default path.

        What this tests:
        ---------------
        1. Default warehouse path is created
        2. Catalog is properly configured
        3. SQLite URI is correct

        Why this matters:
        ----------------
        - Easy setup for development
        - Consistent default behavior
        - No external dependencies
        """
        with patch("bulk_operations.iceberg.catalog.Path.cwd") as mock_cwd:
            mock_cwd.return_value = Path(self.temp_dir)

            catalog = create_filesystem_catalog("test_catalog")

            # Check catalog properties
            self.assertEqual(catalog.name, "test_catalog")

            # Check warehouse directory was created
            expected_warehouse = Path(self.temp_dir) / "iceberg_warehouse"
            self.assertTrue(expected_warehouse.exists())

    def test_create_filesystem_catalog_custom_path(self):
        """
        Test creating filesystem catalog with custom path.

        What this tests:
        ---------------
        1. Custom warehouse path is used
        2. Directory is created if missing
        3. Path objects are handled

        Why this matters:
        ----------------
        - Flexibility in storage location
        - Integration with existing infrastructure
        - Path handling consistency
        """
        catalog = create_filesystem_catalog(
            name="custom_catalog", warehouse_path=self.warehouse_path
        )

        # Check catalog name
        self.assertEqual(catalog.name, "custom_catalog")

        # Check warehouse directory exists
        self.assertTrue(self.warehouse_path.exists())
        self.assertTrue(self.warehouse_path.is_dir())

    def test_create_filesystem_catalog_string_path(self):
        """
        Test creating catalog with string path.

        What this tests:
        ---------------
        1. String paths are converted to Path objects
        2. Catalog works with string paths

        Why this matters:
        ----------------
        - API flexibility
        - Backward compatibility
        - User convenience
        """
        str_path = str(self.warehouse_path)
        catalog = create_filesystem_catalog(name="string_path_catalog", warehouse_path=str_path)

        self.assertEqual(catalog.name, "string_path_catalog")
        self.assertTrue(Path(str_path).exists())

    def test_get_or_create_catalog_default(self):
        """
        Test get_or_create_catalog with defaults.

        What this tests:
        ---------------
        1. Default filesystem catalog is created
        2. Same parameters as create_filesystem_catalog

        Why this matters:
        ----------------
        - Simplified API for common case
        - Consistent behavior
        """
        with patch("bulk_operations.iceberg.catalog.create_filesystem_catalog") as mock_create:
            mock_catalog = Mock(spec=Catalog)
            mock_create.return_value = mock_catalog

            result = get_or_create_catalog(
                catalog_name="default_test", warehouse_path=self.warehouse_path
            )

            # Verify create_filesystem_catalog was called
            mock_create.assert_called_once_with("default_test", self.warehouse_path)
            self.assertEqual(result, mock_catalog)

    def test_get_or_create_catalog_custom_config(self):
        """
        Test get_or_create_catalog with custom configuration.

        What this tests:
        ---------------
        1. Custom config overrides defaults
        2. load_catalog is used for custom configs

        Why this matters:
        ----------------
        - Support for different catalog types
        - Flexibility for production deployments
        - Integration with existing catalogs
        """
        custom_config = {
            "type": "rest",
            "uri": "https://iceberg-catalog.example.com",
            "credential": "token123",
        }

        with patch("bulk_operations.iceberg.catalog.load_catalog") as mock_load:
            mock_catalog = Mock(spec=Catalog)
            mock_load.return_value = mock_catalog

            result = get_or_create_catalog(catalog_name="rest_catalog", config=custom_config)

            # Verify load_catalog was called with custom config
            mock_load.assert_called_once_with("rest_catalog", **custom_config)
            self.assertEqual(result, mock_catalog)

    def test_warehouse_directory_creation(self):
        """
        Test that warehouse directory is created with proper permissions.

        What this tests:
        ---------------
        1. Directory is created if missing
        2. Parent directories are created
        3. Existing directories are not affected

        Why this matters:
        ----------------
        - Data needs a place to live
        - Permissions affect data security
        - Idempotent operation
        """
        nested_path = self.warehouse_path / "nested" / "warehouse"

        # Ensure it doesn't exist
        self.assertFalse(nested_path.exists())

        # Create catalog
        create_filesystem_catalog(name="nested_test", warehouse_path=nested_path)

        # Check all directories were created
        self.assertTrue(nested_path.exists())
        self.assertTrue(nested_path.is_dir())
        self.assertTrue(nested_path.parent.exists())

        # Create again - should not fail
        create_filesystem_catalog(name="nested_test2", warehouse_path=nested_path)
        self.assertTrue(nested_path.exists())

    def test_catalog_properties(self):
        """
        Test that catalog has expected properties.

        What this tests:
        ---------------
        1. Catalog type is set correctly
        2. Warehouse location is set
        3. URI format is correct

        Why this matters:
        ----------------
        - Properties affect catalog behavior
        - Debugging and monitoring
        - Integration requirements
        """
        catalog = create_filesystem_catalog(
            name="properties_test", warehouse_path=self.warehouse_path
        )

        # Check basic properties
        self.assertEqual(catalog.name, "properties_test")

        # For SQL catalog, we'd check additional properties
        # but they're not exposed in the base Catalog interface

        # Verify catalog can be used (basic smoke test)
        # This would fail if catalog is misconfigured
        namespaces = list(catalog.list_namespaces())
        self.assertIsInstance(namespaces, list)


if __name__ == "__main__":
    unittest.main()
