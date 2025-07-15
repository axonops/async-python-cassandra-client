"""
Unit tests for CQL query builder.

What this tests:
---------------
1. Basic query construction
2. Column selection
3. WHERE clause generation
4. Token range queries
5. Writetime/TTL queries

Why this matters:
----------------
- Correct CQL generation critical
- Security (no injection)
- Performance optimization
"""

import pytest

from async_cassandra_dataframe.query_builder import QueryBuilder


class TestQueryBuilder:
    """Test CQL query building functionality."""

    @pytest.fixture
    def table_metadata(self):
        """Sample table metadata for testing."""
        return {
            "keyspace": "test_keyspace",
            "table": "test_table",
            "columns": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "text"},
                {"name": "created_at", "type": "timestamp"},
                {"name": "value", "type": "double"},
            ],
            "partition_key": ["id"],
            "clustering_key": ["created_at"],
            "primary_key": ["id", "created_at"],
        }

    def test_build_basic_select(self, table_metadata):
        """Test building basic SELECT query."""
        builder = QueryBuilder(table_metadata)

        query, params = builder.build_partition_query(columns=None)  # Select all

        assert "SELECT" in query
        assert "FROM test_keyspace.test_table" in query
        assert params == []

    def test_build_select_with_columns(self, table_metadata):
        """Test building SELECT with specific columns."""
        builder = QueryBuilder(table_metadata)

        query, params = builder.build_partition_query(columns=["id", "name", "value"])

        assert "SELECT id, name, value" in query
        assert "FROM test_keyspace.test_table" in query
        assert params == []

    def test_build_select_with_where(self, table_metadata):
        """Test building SELECT with WHERE clause."""
        builder = QueryBuilder(table_metadata)

        # Partition key predicate
        query, params = builder.build_partition_query(
            columns=None, predicates=[{"column": "id", "operator": "=", "value": 123}]
        )

        assert "WHERE id = ?" in query
        assert params == [123]

    def test_build_token_range_query(self, table_metadata):
        """Test building token range query."""
        builder = QueryBuilder(table_metadata)

        query, params = builder.build_partition_query(
            columns=None, token_range=(-9223372036854775808, 0)
        )

        assert "TOKEN(id) >= ? AND TOKEN(id) <= ?" in query
        assert params == [-9223372036854775808, 0]

    def test_build_query_with_allow_filtering(self, table_metadata):
        """Test building query with ALLOW FILTERING."""
        builder = QueryBuilder(table_metadata)

        query, params = builder.build_partition_query(
            columns=None,
            predicates=[{"column": "value", "operator": ">", "value": 100}],
            allow_filtering=True,
        )

        assert "WHERE value > ?" in query
        assert "ALLOW FILTERING" in query
        assert params == [100]

    def test_build_writetime_query(self, table_metadata):
        """Test building query with WRITETIME columns."""
        builder = QueryBuilder(table_metadata)

        query, params = builder.build_partition_query(
            columns=["id", "name"], writetime_columns=["name"]
        )

        assert "id, name" in query
        assert "WRITETIME(name) AS name_writetime" in query
        assert params == []

    def test_build_ttl_query(self, table_metadata):
        """Test building query with TTL columns."""
        builder = QueryBuilder(table_metadata)

        query, params = builder.build_partition_query(
            columns=["id", "value"], ttl_columns=["value"]
        )

        assert "id, value" in query
        assert "TTL(value) AS value_ttl" in query
        assert params == []

    def test_build_complex_query(self, table_metadata):
        """Test building complex query with multiple features."""
        builder = QueryBuilder(table_metadata)

        query, params = builder.build_partition_query(
            columns=["id", "name", "value"],
            writetime_columns=["name"],
            ttl_columns=["value"],
            predicates=[{"column": "id", "operator": "=", "value": 123}],
            allow_filtering=False,
        )

        assert "id, name, value" in query
        assert "WRITETIME(name) AS name_writetime" in query
        assert "TTL(value) AS value_ttl" in query
        assert "WHERE id = ?" in query
        assert params == [123]

    def test_validate_columns(self, table_metadata):
        """Test column validation."""
        builder = QueryBuilder(table_metadata)

        # Valid columns should not raise
        validated = builder.validate_columns(["id", "name", "value"])
        assert validated == ["id", "name", "value"]

        # Invalid column should raise
        with pytest.raises(ValueError) as exc_info:
            builder.validate_columns(["id", "invalid_column"])
        assert "invalid_column" in str(exc_info.value)

    def test_writetime_with_primary_key(self, table_metadata):
        """Test that writetime is not added for primary key columns."""
        builder = QueryBuilder(table_metadata)

        # Try to get writetime for primary key column
        query, params = builder.build_partition_query(
            columns=["id", "name"], writetime_columns=["id", "name"]  # id is primary key
        )

        # Should only have writetime for non-primary key column
        assert "WRITETIME(name) AS name_writetime" in query
        assert "WRITETIME(id)" not in query  # Primary key should not have writetime

    def test_build_query_with_empty_columns(self, table_metadata):
        """Test building query with empty column list."""
        builder = QueryBuilder(table_metadata)

        # Empty list should select specific columns
        query, params = builder.build_partition_query(columns=[])

        # Even with empty columns, should still build a valid query
        assert "SELECT" in query
        assert "FROM test_keyspace.test_table" in query

    def test_token_range_with_multiple_partition_keys(self):
        """Test token range query with composite partition key."""
        metadata = {
            "keyspace": "test",
            "table": "events",
            "columns": [
                {"name": "user_id", "type": "int"},
                {"name": "date", "type": "date"},
                {"name": "value", "type": "double"},
            ],
            "partition_key": ["user_id", "date"],
            "clustering_key": [],
            "primary_key": ["user_id", "date"],
        }

        builder = QueryBuilder(metadata)

        query, params = builder.build_partition_query(columns=None, token_range=(0, 1000))

        assert "TOKEN(user_id, date) >= ? AND TOKEN(user_id, date) <= ?" in query
        assert params == [0, 1000]

    def test_build_count_query(self, table_metadata):
        """Test building count query."""
        builder = QueryBuilder(table_metadata)

        # Test count query without token range
        query, params = builder.build_count_query()
        assert "SELECT COUNT(*) FROM test_keyspace.test_table" in query
        assert params == []

        # Test count query with token range
        query, params = builder.build_count_query(token_range=(-1000, 1000))
        assert "SELECT COUNT(*) FROM test_keyspace.test_table" in query
        assert "WHERE TOKEN(id) >= ? AND TOKEN(id) <= ?" in query
        assert params == [-1000, 1000]

    def test_build_sample_query(self, table_metadata):
        """Test building sample query for schema inference."""
        builder = QueryBuilder(table_metadata)

        # Test with no columns specified
        query = builder.build_sample_query(sample_size=100)
        assert "SELECT id, name, created_at, value" in query
        assert "FROM test_keyspace.test_table" in query
        assert "LIMIT 100" in query

        # Test with specific columns
        query = builder.build_sample_query(columns=["id", "name"], sample_size=50)
        assert "SELECT id, name" in query
        assert "LIMIT 50" in query
