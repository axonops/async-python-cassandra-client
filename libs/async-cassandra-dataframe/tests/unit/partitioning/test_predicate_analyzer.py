"""
Unit tests for predicate pushdown analyzer.

Tests the logic for determining which predicates can be pushed to Cassandra.
"""

from async_cassandra_dataframe.predicate_pushdown import (
    Predicate,
    PredicatePushdownAnalyzer,
    PredicateType,
)


class TestPredicateAnalyzer:
    """Test predicate analysis logic."""

    def test_partition_key_predicate_classification(self):
        """Test that partition key columns are correctly identified."""
        metadata = {
            "partition_key": ["user_id", "year"],
            "clustering_key": ["month", "day"],
            "columns": [
                {"name": "user_id", "type": "int"},
                {"name": "year", "type": "int"},
                {"name": "month", "type": "int"},
                {"name": "day", "type": "int"},
                {"name": "value", "type": "float"},
            ],
        }

        analyzer = PredicatePushdownAnalyzer(metadata)

        # Test single partition key predicate
        predicates = [{"column": "user_id", "operator": "=", "value": 123}]

        pushdown, client_side, use_tokens = analyzer.analyze_predicates(predicates)

        # Should NOT push down incomplete partition key
        assert len(client_side) == 1
        assert len(pushdown) == 0
        assert use_tokens is True  # Still use token ranges

    def test_complete_partition_key_pushdown(self):
        """Test complete partition key enables direct access."""
        metadata = {
            "partition_key": ["user_id", "year"],
            "clustering_key": ["month"],
            "columns": [
                {"name": "user_id", "type": "int"},
                {"name": "year", "type": "int"},
                {"name": "month", "type": "int"},
            ],
        }

        analyzer = PredicatePushdownAnalyzer(metadata)

        # Complete partition key
        predicates = [
            {"column": "user_id", "operator": "=", "value": 123},
            {"column": "year", "operator": "=", "value": 2024},
        ]

        pushdown, client_side, use_tokens = analyzer.analyze_predicates(predicates)

        assert len(pushdown) == 2
        assert len(client_side) == 0
        assert use_tokens is False  # Direct partition access

    def test_clustering_key_with_partition_key(self):
        """Test clustering predicates require complete partition key."""
        metadata = {
            "partition_key": ["sensor_id"],
            "clustering_key": ["timestamp"],
            "columns": [
                {"name": "sensor_id", "type": "int"},
                {"name": "timestamp", "type": "timestamp"},
                {"name": "value", "type": "float"},
            ],
        }

        analyzer = PredicatePushdownAnalyzer(metadata)

        # With complete partition key
        predicates = [
            {"column": "sensor_id", "operator": "=", "value": 1},
            {"column": "timestamp", "operator": ">", "value": "2024-01-01"},
        ]

        pushdown, client_side, use_tokens = analyzer.analyze_predicates(predicates)

        assert len(pushdown) == 2  # Both can be pushed
        assert len(client_side) == 0
        assert use_tokens is False

    def test_regular_column_requires_client_filtering(self):
        """Test regular columns can't be pushed without index."""
        metadata = {
            "partition_key": ["id"],
            "clustering_key": [],
            "columns": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "text"},
                {"name": "status", "type": "text"},
            ],
        }

        analyzer = PredicatePushdownAnalyzer(metadata)

        predicates = [{"column": "status", "operator": "=", "value": "active"}]

        pushdown, client_side, use_tokens = analyzer.analyze_predicates(predicates)

        assert len(pushdown) == 0
        assert len(client_side) == 1
        assert use_tokens is True  # Use token ranges for scanning

    def test_in_operator_on_partition_key(self):
        """Test IN operator on partition key."""
        metadata = {
            "partition_key": ["id"],
            "clustering_key": [],
            "columns": [
                {"name": "id", "type": "int"},
                {"name": "data", "type": "text"},
            ],
        }

        analyzer = PredicatePushdownAnalyzer(metadata)

        predicates = [{"column": "id", "operator": "IN", "value": [1, 2, 3, 4, 5]}]

        pushdown, client_side, use_tokens = analyzer.analyze_predicates(predicates)

        # IN on partition key can be pushed down
        assert len(pushdown) == 1
        assert len(client_side) == 0
        assert use_tokens is False  # Direct partition access

    def test_where_clause_building(self):
        """Test WHERE clause construction."""
        metadata = {"partition_key": ["user_id"], "clustering_key": ["timestamp"], "columns": []}

        analyzer = PredicatePushdownAnalyzer(metadata)

        # Test with token range
        predicates = [Predicate("user_id", "=", 123, PredicateType.PARTITION_KEY)]

        where, params = analyzer.build_where_clause(predicates, token_range=(-1000, 1000))

        assert "TOKEN(user_id) >= ?" in where
        assert "TOKEN(user_id) <= ?" in where
        assert "user_id = ?" in where
        assert params == [-1000, 1000, 123]

    def test_invalid_clustering_order(self):
        """Test clustering predicates must be in order."""
        metadata = {"partition_key": ["pk"], "clustering_key": ["ck1", "ck2", "ck3"], "columns": []}

        analyzer = PredicatePushdownAnalyzer(metadata)

        # Skip ck1 - invalid
        ck_predicates = [
            Predicate("ck2", "=", 2, PredicateType.CLUSTERING_KEY),
            Predicate("ck3", ">", 3, PredicateType.CLUSTERING_KEY),
        ]

        valid, invalid = analyzer._validate_clustering_predicates(ck_predicates)

        assert len(valid) == 0
        assert len(invalid) == 2  # Both invalid due to skipping ck1
