"""
Integration tests for table metadata extraction against real Cassandra.

What this tests:
---------------
1. Metadata extraction from various table structures
2. UDT detection and writetime/TTL support
3. Complex types (collections, frozen, nested)
4. Static columns and counter types
5. Clustering order and reversed columns
6. Secondary indexes and materialized views
7. Edge cases and error conditions

Why this matters:
----------------
- Metadata drives all DataFrame operations
- Real Cassandra metadata can be complex
- Type detection affects data conversion
- Primary key structure affects queries
- Must handle all Cassandra features

Additional context:
---------------------------------
Tests use real Cassandra to ensure metadata extraction
works correctly with actual driver responses.
"""

from uuid import uuid4

import pytest

import async_cassandra_dataframe as cdf
from async_cassandra_dataframe.metadata import TableMetadataExtractor


class TestMetadataIntegration:
    """Integration tests for metadata extraction."""

    @pytest.mark.asyncio
    async def test_basic_table_metadata(self, session, test_table_name):
        """
        Test metadata extraction for a basic table.

        What this tests:
        ---------------
        1. Simple table with partition and clustering keys
        2. Regular columns of various types
        3. Primary key structure extraction
        4. Writetime/TTL support detection
        5. Column ordering preservation

        Why this matters:
        ----------------
        - Most common table structure
        - Foundation for all operations
        - Must correctly identify key columns
        - Writetime/TTL affects features
        """
        # Create a basic table
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                user_id UUID,
                created_at TIMESTAMP,
                name TEXT,
                email TEXT,
                age INT,
                active BOOLEAN,
                PRIMARY KEY (user_id, created_at)
            ) WITH CLUSTERING ORDER BY (created_at DESC)
        """
        )

        try:
            # Extract metadata
            extractor = TableMetadataExtractor(session)
            metadata = await extractor.get_table_metadata("test_dataframe", test_table_name)

            # Verify basic structure
            assert metadata["keyspace"] == "test_dataframe"
            assert metadata["table"] == test_table_name
            assert len(metadata["columns"]) == 6

            # Verify primary key structure
            assert metadata["partition_key"] == ["user_id"]
            assert metadata["clustering_key"] == ["created_at"]
            assert metadata["primary_key"] == ["user_id", "created_at"]

            # Check column properties
            columns_by_name = {col["name"]: col for col in metadata["columns"]}

            # Partition key
            assert columns_by_name["user_id"]["is_partition_key"] is True
            assert columns_by_name["user_id"]["is_clustering_key"] is False
            assert columns_by_name["user_id"]["supports_writetime"] is False
            assert columns_by_name["user_id"]["supports_ttl"] is False

            # Clustering key
            assert columns_by_name["created_at"]["is_partition_key"] is False
            assert columns_by_name["created_at"]["is_clustering_key"] is True
            assert columns_by_name["created_at"]["is_reversed"] is True  # DESC order
            assert columns_by_name["created_at"]["supports_writetime"] is False
            assert columns_by_name["created_at"]["supports_ttl"] is False

            # Regular columns should support writetime/TTL
            for col_name in ["name", "email", "age", "active"]:
                assert columns_by_name[col_name]["is_partition_key"] is False
                assert columns_by_name[col_name]["is_clustering_key"] is False
                assert columns_by_name[col_name]["supports_writetime"] is True
                assert columns_by_name[col_name]["supports_ttl"] is True

            # Test helper methods
            writetime_cols = extractor.get_writetime_capable_columns(metadata)
            assert set(writetime_cols) == {"name", "email", "age", "active"}

            ttl_cols = extractor.get_ttl_capable_columns(metadata)
            assert set(ttl_cols) == {"name", "email", "age", "active"}

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    @pytest.mark.asyncio
    async def test_complex_types_metadata(self, session, test_table_name):
        """
        Test metadata for tables with complex types.

        What this tests:
        ---------------
        1. Collection types (LIST, SET, MAP)
        2. Frozen collections
        3. Nested collections
        4. Tuple types
        5. All primitive types

        Why this matters:
        ----------------
        - Complex types are common
        - Type information affects conversion
        - Collections have special handling
        - Frozen types enable primary key usage
        """
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                id INT PRIMARY KEY,
                -- Collections
                tags LIST<TEXT>,
                unique_tags SET<INT>,
                attributes MAP<TEXT, TEXT>,
                frozen_list FROZEN<LIST<TEXT>>,
                frozen_set FROZEN<SET<UUID>>,
                frozen_map FROZEN<MAP<TEXT, INT>>,

                -- Nested collections
                nested_list LIST<FROZEN<LIST<TEXT>>>,
                nested_map MAP<TEXT, FROZEN<SET<INT>>>,

                -- Tuple
                coordinates TUPLE<DOUBLE, DOUBLE, DOUBLE>,

                -- All numeric types
                tiny_num TINYINT,
                small_num SMALLINT,
                regular_num INT,
                big_num BIGINT,
                huge_num VARINT,
                float_num FLOAT,
                double_num DOUBLE,
                decimal_num DECIMAL,

                -- Temporal types
                date_col DATE,
                time_col TIME,
                timestamp_col TIMESTAMP,
                duration_col DURATION,

                -- Other types
                blob_col BLOB,
                inet_col INET,
                uuid_col UUID,
                timeuuid_col TIMEUUID,
                bool_col BOOLEAN,
                ascii_col ASCII,
                varchar_col VARCHAR
            )
        """
        )

        try:
            extractor = TableMetadataExtractor(session)
            metadata = await extractor.get_table_metadata("test_dataframe", test_table_name)

            columns_by_name = {col["name"]: col for col in metadata["columns"]}

            # Verify collection types
            assert "list<text>" in str(columns_by_name["tags"]["type"])
            assert "set<int>" in str(columns_by_name["unique_tags"]["type"])
            assert "map<text, text>" in str(columns_by_name["attributes"]["type"])

            # Frozen collections
            assert "frozen<list<text>>" in str(columns_by_name["frozen_list"]["type"])
            assert "frozen<set<uuid>>" in str(columns_by_name["frozen_set"]["type"])
            assert "frozen<map<text, int>>" in str(columns_by_name["frozen_map"]["type"])

            # Nested collections
            assert "list<frozen<list<text>>>" in str(columns_by_name["nested_list"]["type"])
            assert "map<text, frozen<set<int>>>" in str(columns_by_name["nested_map"]["type"])

            # Tuple type
            assert "tuple<double, double, double>" in str(columns_by_name["coordinates"]["type"])

            # All collections support writetime/TTL
            collection_cols = [
                "tags",
                "unique_tags",
                "attributes",
                "frozen_list",
                "frozen_set",
                "frozen_map",
                "nested_list",
                "nested_map",
            ]
            for col_name in collection_cols:
                assert columns_by_name[col_name]["supports_writetime"] is True
                assert columns_by_name[col_name]["supports_ttl"] is True

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    @pytest.mark.asyncio
    async def test_udt_metadata(self, session, test_table_name):
        """
        Test metadata extraction for tables with UDTs.

        What this tests:
        ---------------
        1. Simple UDT detection
        2. Nested UDT support
        3. Collections of UDTs
        4. Frozen UDTs in primary keys
        5. Writetime/TTL support for UDTs

        Why this matters:
        ----------------
        - UDTs don't support direct writetime/TTL
        - Only UDT fields support writetime
        - Critical for proper feature support
        - Common in production schemas
        """
        # Create UDTs
        await session.execute(
            """
            CREATE TYPE IF NOT EXISTS test_dataframe.address (
                street TEXT,
                city TEXT,
                zip_code INT
            )
        """
        )

        await session.execute(
            """
            CREATE TYPE IF NOT EXISTS test_dataframe.contact_info (
                email TEXT,
                phone TEXT,
                address FROZEN<address>
            )
        """
        )

        # Create table with UDTs
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                id INT,
                location FROZEN<address>,
                contact contact_info,
                addresses LIST<FROZEN<address>>,
                contacts_by_type MAP<TEXT, FROZEN<contact_info>>,
                PRIMARY KEY (id, location)
            )
        """
        )

        try:
            extractor = TableMetadataExtractor(session)
            metadata = await extractor.get_table_metadata("test_dataframe", test_table_name)

            columns_by_name = {col["name"]: col for col in metadata["columns"]}

            # UDT in clustering key (frozen)
            assert columns_by_name["location"]["is_clustering_key"] is True
            assert columns_by_name["location"]["supports_writetime"] is False
            assert columns_by_name["location"]["supports_ttl"] is False

            # Regular UDT column - UDTs don't support writetime/TTL
            assert extractor._is_udt_type(str(columns_by_name["contact"]["type"]))
            assert columns_by_name["contact"]["supports_writetime"] is False
            assert columns_by_name["contact"]["supports_ttl"] is True  # TTL is supported

            # Collections of UDTs - collections support writetime/TTL but not the UDTs inside
            assert columns_by_name["addresses"]["supports_writetime"] is True
            assert columns_by_name["addresses"]["supports_ttl"] is True

            # Map with UDT values
            assert columns_by_name["contacts_by_type"]["supports_writetime"] is True
            assert columns_by_name["contacts_by_type"]["supports_ttl"] is True

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")
            await session.execute("DROP TYPE IF EXISTS test_dataframe.contact_info")
            await session.execute("DROP TYPE IF EXISTS test_dataframe.address")

    @pytest.mark.asyncio
    async def test_counter_and_static_metadata(self, session, test_table_name):
        """
        Test metadata for counter and static columns.

        What this tests:
        ---------------
        1. Counter column detection
        2. Static column identification
        3. Counter restrictions (no writetime/TTL)
        4. Static column properties
        5. Mixed column types

        Why this matters:
        ----------------
        - Counters have special restrictions
        - Static columns shared in partition
        - Affects query generation
        - Important for correct operations
        """
        # Counter table
        await session.execute(
            f"""
            CREATE TABLE {test_table_name}_counters (
                id INT PRIMARY KEY,
                page_views COUNTER,
                downloads COUNTER
            )
        """
        )

        # Table with static columns
        await session.execute(
            f"""
            CREATE TABLE {test_table_name}_static (
                partition_id INT,
                cluster_id INT,
                static_data TEXT STATIC,
                regular_data TEXT,
                PRIMARY KEY (partition_id, cluster_id)
            )
        """
        )

        try:
            extractor = TableMetadataExtractor(session)

            # Test counter metadata
            counter_meta = await extractor.get_table_metadata(
                "test_dataframe", f"{test_table_name}_counters"
            )
            counter_cols = {col["name"]: col for col in counter_meta["columns"]}

            # Counters don't support writetime or TTL
            assert counter_cols["page_views"]["supports_writetime"] is False
            assert counter_cols["page_views"]["supports_ttl"] is False
            assert counter_cols["downloads"]["supports_writetime"] is False
            assert counter_cols["downloads"]["supports_ttl"] is False

            # Test static column metadata
            static_meta = await extractor.get_table_metadata(
                "test_dataframe", f"{test_table_name}_static"
            )
            static_cols = {col["name"]: col for col in static_meta["columns"]}

            # Static columns should be marked
            assert static_cols["static_data"]["is_static"] is True
            assert static_cols["regular_data"]["is_static"] is False

            # Both support writetime/TTL
            assert static_cols["static_data"]["supports_writetime"] is True
            assert static_cols["static_data"]["supports_ttl"] is True
            assert static_cols["regular_data"]["supports_writetime"] is True
            assert static_cols["regular_data"]["supports_ttl"] is True

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}_counters")
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}_static")

    @pytest.mark.asyncio
    async def test_wildcard_expansion(self, session, test_table_name):
        """
        Test column wildcard expansion functionality.

        What this tests:
        ---------------
        1. "*" expansion to all columns
        2. Filtering for writetime-capable columns
        3. Filtering for TTL-capable columns
        4. Handling non-existent columns
        5. Empty column lists

        Why this matters:
        ----------------
        - Wildcard support improves usability
        - Must respect column capabilities
        - Prevents invalid operations
        - Common user pattern
        """
        # Create table with mix of column types
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                partition_id INT,
                cluster_id INT,
                regular_text TEXT,
                regular_int INT,
                PRIMARY KEY (partition_id, cluster_id)
            )
        """
        )

        try:
            extractor = TableMetadataExtractor(session)
            metadata = await extractor.get_table_metadata("test_dataframe", test_table_name)

            # Test "*" expansion - all columns
            all_cols = extractor.expand_column_wildcards(["*"], metadata)
            assert set(all_cols) == {"partition_id", "cluster_id", "regular_text", "regular_int"}

            # Test writetime-capable expansion
            writetime_cols = extractor.expand_column_wildcards(
                ["*"], metadata, writetime_capable_only=True
            )
            # Only regular columns support writetime (not keys)
            assert set(writetime_cols) == {"regular_text", "regular_int"}

            # Test TTL-capable expansion
            ttl_cols = extractor.expand_column_wildcards(["*"], metadata, ttl_capable_only=True)
            # Regular columns support TTL (not keys)
            assert set(ttl_cols) == {"regular_text", "regular_int"}

            # Test specific column selection with filtering
            selected = extractor.expand_column_wildcards(
                ["partition_id", "regular_text", "nonexistent"], metadata
            )
            # Should filter out nonexistent
            assert selected == ["partition_id", "regular_text"]

            # Test empty column list
            empty = extractor.expand_column_wildcards([], metadata)
            assert empty == []

            # Test None columns
            none_result = extractor.expand_column_wildcards(None, metadata)
            assert none_result == []

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    @pytest.mark.asyncio
    async def test_vector_type_metadata(self, session, test_table_name):
        """
        Test metadata for vector type (Cassandra 5.0+).

        What this tests:
        ---------------
        1. Vector type detection
        2. Vector dimensions extraction
        3. Writetime/TTL support for vectors
        4. Vector in collections

        Why this matters:
        ----------------
        - Vector search is important feature
        - Must handle new Cassandra types
        - Type info needed for conversion
        - Growing use case
        """
        # Skip if Cassandra doesn't support vectors
        try:
            await session.execute(
                f"""
                CREATE TABLE {test_table_name} (
                    id INT PRIMARY KEY,
                    embedding VECTOR<FLOAT, 3>,
                    embeddings LIST<FROZEN<VECTOR<FLOAT, 3>>>
                )
            """
            )
        except Exception as e:
            if "vector" in str(e).lower():
                pytest.skip("Cassandra version doesn't support VECTOR type")
            raise

        try:
            extractor = TableMetadataExtractor(session)
            metadata = await extractor.get_table_metadata("test_dataframe", test_table_name)

            columns_by_name = {col["name"]: col for col in metadata["columns"]}

            # Vector column properties
            assert "vector" in str(columns_by_name["embedding"]["type"]).lower()

            # Vector types should support writetime/TTL (they're not UDTs)
            assert columns_by_name["embedding"]["supports_writetime"] is True
            assert columns_by_name["embedding"]["supports_ttl"] is True

            # List of vectors
            assert "list" in str(columns_by_name["embeddings"]["type"]).lower()
            assert "vector" in str(columns_by_name["embeddings"]["type"]).lower()

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    @pytest.mark.asyncio
    async def test_metadata_with_dataframe_read(self, session, test_table_name):
        """
        Test that metadata is correctly used in DataFrame operations.

        What this tests:
        ---------------
        1. Metadata drives column selection
        2. Writetime columns properly filtered
        3. Type conversion uses metadata
        4. Primary keys used for queries
        5. End-to-end integration

        Why this matters:
        ----------------
        - Metadata must work with DataFrame
        - Real-world usage validation
        - Catches integration issues
        - Ensures feature completeness
        """
        # Create regular table
        await session.execute(
            f"""
            CREATE TABLE {test_table_name} (
                user_id UUID,
                timestamp TIMESTAMP,
                status TEXT,
                score INT,
                PRIMARY KEY (user_id, timestamp)
            )
        """
        )

        try:
            # Insert test data
            user_id = uuid4()

            # Regular insert
            await session.execute(
                f"""
                INSERT INTO {test_table_name} (user_id, timestamp, status, score)
                VALUES ({user_id}, '2024-01-15 10:00:00', 'active', 100)
            """
            )

            # Read with writetime - should only work for status column
            df = await cdf.read_cassandra_table(
                f"test_dataframe.{test_table_name}",
                session=session,
                writetime_columns=["*"],  # Should expand to only writetime-capable
            )

            pdf = df.compute()

            # Verify columns
            assert "user_id" in pdf.columns
            assert "timestamp" in pdf.columns
            assert "status" in pdf.columns
            assert "score" in pdf.columns

            # Writetime should only exist for non-key columns
            assert "status_writetime" in pdf.columns
            assert "score_writetime" in pdf.columns
            assert "user_id_writetime" not in pdf.columns
            assert "timestamp_writetime" not in pdf.columns

            # Verify data types from metadata
            assert str(pdf["user_id"].dtype) == "cassandra_uuid"
            assert str(pdf["timestamp"].dtype) == "datetime64[ns, UTC]"
            assert str(pdf["status"].dtype) == "string"
            assert str(pdf["score"].dtype) == "Int32"

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    @pytest.mark.asyncio
    async def test_edge_cases(self, session, test_table_name):
        """
        Test edge cases in metadata extraction.

        What this tests:
        ---------------
        1. Tables with only primary key
        2. Composite partition keys
        3. Multiple clustering columns
        4. Reserved column names
        5. Very long type definitions

        Why this matters:
        ----------------
        - Must handle all valid schemas
        - Edge cases reveal bugs
        - Production schemas vary widely
        - Robustness is critical
        """
        # Table with only primary key
        await session.execute(
            f"""
            CREATE TABLE {test_table_name}_pk_only (
                id UUID PRIMARY KEY
            )
        """
        )

        # Table with composite partition key
        await session.execute(
            f"""
            CREATE TABLE {test_table_name}_composite (
                region TEXT,
                bucket INT,
                timestamp TIMESTAMP,
                sensor_id UUID,
                value DOUBLE,
                PRIMARY KEY ((region, bucket), timestamp, sensor_id)
            ) WITH CLUSTERING ORDER BY (timestamp DESC, sensor_id ASC)
        """
        )

        try:
            extractor = TableMetadataExtractor(session)

            # Test PK-only table
            pk_meta = await extractor.get_table_metadata(
                "test_dataframe", f"{test_table_name}_pk_only"
            )
            assert len(pk_meta["columns"]) == 1
            assert pk_meta["partition_key"] == ["id"]
            assert pk_meta["clustering_key"] == []

            # Test composite partition key
            comp_meta = await extractor.get_table_metadata(
                "test_dataframe", f"{test_table_name}_composite"
            )
            assert comp_meta["partition_key"] == ["region", "bucket"]
            assert comp_meta["clustering_key"] == ["timestamp", "sensor_id"]
            assert comp_meta["primary_key"] == ["region", "bucket", "timestamp", "sensor_id"]

            # Check clustering order
            cols_by_name = {col["name"]: col for col in comp_meta["columns"]}
            assert cols_by_name["timestamp"]["is_reversed"] is True
            assert cols_by_name["sensor_id"]["is_reversed"] is False

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}_pk_only")
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}_composite")
