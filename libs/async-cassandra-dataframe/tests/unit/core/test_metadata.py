"""
Unit tests for table metadata extraction.

What this tests:
---------------
1. Table metadata extraction
2. Column type processing
3. Primary key identification
4. Writetime/TTL eligibility
5. Error handling for missing tables

Why this matters:
----------------
- Correct metadata drives all operations
- Type information prevents data loss
- Key structure affects query generation
"""

from unittest.mock import Mock

import pytest

from async_cassandra_dataframe.metadata import TableMetadataExtractor


class TestTableMetadataExtractor:
    """Test table metadata extraction functionality."""

    @pytest.fixture
    def mock_session(self):
        """Create a mock async session with metadata."""
        session = Mock()

        # Mock the sync session and cluster
        sync_session = Mock()
        cluster = Mock()

        session._session = sync_session
        sync_session.cluster = cluster

        return session, cluster

    def test_init(self, mock_session):
        """Test metadata extractor initialization."""
        session, cluster = mock_session

        extractor = TableMetadataExtractor(session)

        assert extractor.session == session
        assert extractor._sync_session == session._session
        assert extractor._cluster == cluster

    @pytest.mark.asyncio
    async def test_get_table_metadata_success(self, mock_session):
        """Test successful table metadata retrieval."""
        session, cluster = mock_session

        # Create mock keyspace and table metadata
        keyspace_meta = Mock()
        table_meta = Mock()

        # Set up the metadata hierarchy
        cluster.metadata.keyspaces = {"test_ks": keyspace_meta}
        keyspace_meta.tables = {"test_table": table_meta}

        # Mock table structure
        table_meta.keyspace_name = "test_ks"
        table_meta.name = "test_table"

        # Mock columns
        id_col = Mock()
        id_col.name = "id"
        id_col.cql_type = "int"

        name_col = Mock()
        name_col.name = "name"
        name_col.cql_type = "text"

        table_meta.partition_key = [id_col]
        table_meta.clustering_key = []
        table_meta.columns = {"id": id_col, "name": name_col}

        extractor = TableMetadataExtractor(session)

        # Test getting metadata
        result = await extractor.get_table_metadata("test_ks", "test_table")

        assert result["keyspace"] == "test_ks"
        assert result["table"] == "test_table"
        assert len(result["columns"]) == 2
        assert result["partition_key"] == ["id"]
        assert result["clustering_key"] == []

    @pytest.mark.asyncio
    async def test_get_table_metadata_keyspace_not_found(self, mock_session):
        """Test error when keyspace doesn't exist."""
        session, cluster = mock_session
        cluster.metadata.keyspaces = {}

        extractor = TableMetadataExtractor(session)

        with pytest.raises(ValueError) as exc_info:
            await extractor.get_table_metadata("nonexistent_ks", "test_table")

        assert "Keyspace 'nonexistent_ks' not found" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_table_metadata_table_not_found(self, mock_session):
        """Test error when table doesn't exist."""
        session, cluster = mock_session

        keyspace_meta = Mock()
        keyspace_meta.tables = {}
        cluster.metadata.keyspaces = {"test_ks": keyspace_meta}

        extractor = TableMetadataExtractor(session)

        with pytest.raises(ValueError) as exc_info:
            await extractor.get_table_metadata("test_ks", "nonexistent_table")

        assert "Table 'test_ks.nonexistent_table' not found" in str(exc_info.value)

    def test_process_table_metadata_with_all_key_types(self, mock_session):
        """Test processing table with partition and clustering keys."""
        session, _ = mock_session
        extractor = TableMetadataExtractor(session)

        # Create mock table metadata
        table_meta = Mock()
        table_meta.keyspace_name = "test_ks"
        table_meta.name = "test_table"

        # Mock columns
        # Partition key
        user_id = Mock()
        user_id.name = "user_id"
        user_id.cql_type = Mock()
        user_id.cql_type.__str__ = Mock(return_value="uuid")

        # Clustering key
        created_at = Mock()
        created_at.name = "created_at"
        created_at.cql_type = Mock()
        created_at.cql_type.__str__ = Mock(return_value="timestamp")

        # Regular column
        data = Mock()
        data.name = "data"
        data.cql_type = Mock()
        data.cql_type.__str__ = Mock(return_value="text")

        table_meta.partition_key = [user_id]
        table_meta.clustering_key = [created_at]
        table_meta.columns = {"user_id": user_id, "created_at": created_at, "data": data}

        result = extractor._process_table_metadata(table_meta)

        assert result["keyspace"] == "test_ks"
        assert result["table"] == "test_table"
        assert len(result["columns"]) == 3
        assert result["partition_key"] == ["user_id"]
        assert result["clustering_key"] == ["created_at"]
        assert result["primary_key"] == ["user_id", "created_at"]

        # Check column properties
        columns_by_name = {col["name"]: col for col in result["columns"]}

        assert columns_by_name["user_id"]["is_partition_key"] is True
        assert columns_by_name["user_id"]["is_clustering_key"] is False
        assert columns_by_name["user_id"]["supports_writetime"] is False

        assert columns_by_name["created_at"]["is_partition_key"] is False
        assert columns_by_name["created_at"]["is_clustering_key"] is True
        assert columns_by_name["created_at"]["supports_writetime"] is False

        assert columns_by_name["data"]["is_partition_key"] is False
        assert columns_by_name["data"]["is_clustering_key"] is False
        assert columns_by_name["data"]["supports_writetime"] is True

    def test_process_column_regular(self, mock_session):
        """Test processing a regular column."""
        session, _ = mock_session
        extractor = TableMetadataExtractor(session)

        col = Mock()
        col.name = "email"
        col.cql_type = Mock()
        col.cql_type.__str__ = Mock(return_value="text")

        result = extractor._process_column(col)

        assert result["name"] == "email"
        assert str(result["type"]) == "text"
        assert result["is_partition_key"] is False
        assert result["is_clustering_key"] is False
        assert result["supports_writetime"] is True
        assert result["supports_ttl"] is True

    def test_process_column_partition_key(self, mock_session):
        """Test processing a partition key column."""
        session, _ = mock_session
        extractor = TableMetadataExtractor(session)

        col = Mock()
        col.name = "id"
        col.cql_type = Mock()
        col.cql_type.__str__ = Mock(return_value="int")

        result = extractor._process_column(col, is_partition_key=True)

        assert result["name"] == "id"
        assert str(result["type"]) == "int"
        assert result["is_partition_key"] is True
        assert result["is_clustering_key"] is False
        assert result["supports_writetime"] is False
        assert result["supports_ttl"] is False

    def test_process_column_clustering_key(self, mock_session):
        """Test processing a clustering key column."""
        session, _ = mock_session
        extractor = TableMetadataExtractor(session)

        col = Mock()
        col.name = "timestamp"
        col.cql_type = Mock()
        col.cql_type.__str__ = Mock(return_value="timestamp")

        result = extractor._process_column(col, is_clustering_key=True)

        assert result["name"] == "timestamp"
        assert str(result["type"]) == "timestamp"
        assert result["is_partition_key"] is False
        assert result["is_clustering_key"] is True
        assert result["supports_writetime"] is False
        assert result["supports_ttl"] is False

    def test_process_column_complex_type(self, mock_session):
        """Test processing column with complex type."""
        session, _ = mock_session
        extractor = TableMetadataExtractor(session)

        col = Mock()
        col.name = "tags"
        col.cql_type = Mock()
        col.cql_type.__str__ = Mock(return_value="list<text>")

        result = extractor._process_column(col)

        assert result["name"] == "tags"
        assert str(result["type"]) == "list<text>"
        assert result["supports_writetime"] is True

    def test_get_writetime_capable_columns(self, mock_session):
        """Test getting columns capable of having writetime."""
        session, _ = mock_session
        extractor = TableMetadataExtractor(session)

        metadata = {
            "columns": [
                {"name": "id", "supports_writetime": False},
                {"name": "name", "supports_writetime": True},
                {"name": "email", "supports_writetime": True},
                {"name": "created_at", "supports_writetime": False},
            ]
        }

        result = extractor.get_writetime_capable_columns(metadata)

        assert result == ["name", "email"]

    def test_get_ttl_capable_columns(self, mock_session):
        """Test getting columns capable of having TTL."""
        session, _ = mock_session
        extractor = TableMetadataExtractor(session)

        metadata = {
            "columns": [
                {"name": "id", "supports_ttl": False},
                {"name": "cache_data", "supports_ttl": True},
                {"name": "temp_token", "supports_ttl": True},
            ]
        }

        result = extractor.get_ttl_capable_columns(metadata)

        assert result == ["cache_data", "temp_token"]

    def test_expand_column_wildcards(self, mock_session):
        """Test expanding column wildcards."""
        session, _ = mock_session
        extractor = TableMetadataExtractor(session)

        metadata = {
            "columns": [
                {"name": "id", "supports_writetime": False},
                {"name": "name", "supports_writetime": True},
                {"name": "email", "supports_writetime": True},
                {"name": "data", "supports_writetime": True},
            ]
        }

        # Test wildcard expansion for writetime columns
        result = extractor.expand_column_wildcards(
            columns=["*"], table_metadata=metadata, writetime_capable_only=True
        )

        # Should expand to only writetime-capable columns
        assert set(result) == {"name", "email", "data"}

        # Test specific columns
        result = extractor.expand_column_wildcards(
            columns=["id", "name", "unknown"], table_metadata=metadata
        )

        # Should filter out unknown column
        assert result == ["id", "name"]

    def test_empty_table(self, mock_session):
        """Test processing empty table metadata."""
        session, _ = mock_session
        extractor = TableMetadataExtractor(session)

        table_meta = Mock()
        table_meta.keyspace_name = "test_ks"
        table_meta.name = "empty_table"
        table_meta.partition_key = []
        table_meta.clustering_key = []
        table_meta.columns = {}

        result = extractor._process_table_metadata(table_meta)

        assert result["keyspace"] == "test_ks"
        assert result["table"] == "empty_table"
        assert result["columns"] == []
        assert result["partition_key"] == []
        assert result["clustering_key"] == []
        assert result["primary_key"] == []
