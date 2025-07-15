"""
Integration tests for data types that don't support writetime.

What this tests:
---------------
1. Counter columns - cannot have writetime
2. Primary key columns - cannot have writetime
3. Error handling when trying to export writetime for unsupported types
4. Mixed tables with supported and unsupported writetime columns
5. Proper behavior when export_writetime=True with unsupported types

Why this matters:
----------------
- Attempting to get writetime on counters causes errors
- Primary keys don't have writetime
- Export must handle these gracefully
- Users need clear behavior when mixing types

Additional context:
---------------------------------
- WRITETIME() function in CQL throws error on counters
- Primary key columns are special and don't store writetime
- We must handle these cases without failing the entire export
"""

import asyncio
import json
import os
import tempfile
import uuid
from datetime import datetime, timezone

import pytest

from async_cassandra_bulk import BulkOperator


class TestWritetimeUnsupportedTypes:
    """Test writetime behavior with unsupported data types."""

    @pytest.mark.asyncio
    async def test_counter_columns_no_writetime(self, session):
        """Test that counter columns don't support writetime."""
        table = f"test_counter_{uuid.uuid4().hex[:8]}"

        # Create table with counter
        await session.execute(
            f"""
            CREATE TABLE {table} (
                id int PRIMARY KEY,
                page_views counter,
                downloads counter
            )
        """
        )

        # Update counters
        await session.execute(f"UPDATE {table} SET page_views = page_views + 100 WHERE id = 1")
        await session.execute(f"UPDATE {table} SET downloads = downloads + 50 WHERE id = 1")
        await session.execute(f"UPDATE {table} SET page_views = page_views + 200 WHERE id = 2")

        # Export without writetime - should work
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            output_file = f.name

        try:
            operator = BulkOperator(session=session)
            await operator.export(
                table=f"test_bulk.{table}",
                output_path=output_file,
                format="json",
                options={"include_writetime": False},
            )

            with open(output_file, "r") as f:
                data = json.load(f)  # Load the entire JSON array

                # Verify counter values exported correctly
                row_by_id = {row["id"]: row for row in data}
                assert row_by_id[1]["page_views"] == 100
                assert row_by_id[1]["downloads"] == 50
                assert row_by_id[2]["page_views"] == 200
                assert row_by_id[2]["downloads"] is None  # Non-updated counter is NULL

        finally:
            os.unlink(output_file)

        # Export with writetime - should handle gracefully
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            output_file2 = f.name

        try:
            await operator.export(
                table=f"test_bulk.{table}",
                output_path=output_file2,
                format="json",
                options={"include_writetime": True},  # This should not cause errors
            )

            with open(output_file2, "r") as f:
                rows = json.load(f)

                # Counters should be exported but no writetime columns
                for row in rows:
                    assert "page_views" in row
                    assert "downloads" in row
                    # No writetime columns for counters
                    assert "page_views_writetime" not in row
                    assert "downloads_writetime" not in row
                    assert "id_writetime" not in row  # PK also has no writetime

        finally:
            os.unlink(output_file2)

    @pytest.mark.asyncio
    async def test_primary_key_no_writetime(self, session):
        """Test that primary key columns don't have writetime."""
        table = f"test_pk_writetime_{uuid.uuid4().hex[:8]}"

        # Create table with composite primary key
        await session.execute(
            f"""
            CREATE TABLE {table} (
                partition_id int,
                cluster_id int,
                name text,
                value text,
                PRIMARY KEY (partition_id, cluster_id)
            )
        """
        )

        # Insert data
        stmt = await session.prepare(
            f"INSERT INTO {table} (partition_id, cluster_id, name, value) VALUES (?, ?, ?, ?)"
        )
        await session.execute(stmt, (1, 1, "Alice", "value1"))
        await session.execute(stmt, (1, 2, "Bob", "value2"))
        await session.execute(stmt, (2, 1, "Charlie", "value3"))

        # Export with writetime
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            output_file = f.name

        try:
            operator = BulkOperator(session=session)
            await operator.export(
                table=f"test_bulk.{table}",
                output_path=output_file,
                format="json",
                options={"include_writetime": True},
            )

            with open(output_file, "r") as f:
                rows = json.load(f)

                # Verify writetime only for non-PK columns
                for row in rows:
                    # Primary key columns - no writetime
                    assert "partition_id_writetime" not in row
                    assert "cluster_id_writetime" not in row

                    # Regular columns - should have writetime
                    assert "name_writetime" in row
                    assert "value_writetime" in row
                    assert row["name_writetime"] is not None
                    assert row["value_writetime"] is not None

        finally:
            os.unlink(output_file)

    @pytest.mark.asyncio
    @pytest.mark.skip(reason="Cassandra doesn't allow mixing counter and non-counter columns")
    async def test_mixed_table_supported_unsupported(self, session):
        """Test table with mix of supported and unsupported writetime columns."""
        table = f"test_mixed_writetime_{uuid.uuid4().hex[:8]}"

        # Create complex table
        await session.execute(
            f"""
            CREATE TABLE {table} (
                user_id uuid PRIMARY KEY,
                username text,
                email text,
                login_count counter,
                last_login timestamp,
                preferences map<text, text>
            )
        """
        )

        # Insert regular data
        user_id = uuid.uuid4()
        stmt = await session.prepare(
            f"INSERT INTO {table} (user_id, username, email, last_login, preferences) VALUES (?, ?, ?, ?, ?)"
        )
        await session.execute(
            stmt,
            (
                user_id,
                "testuser",
                "test@example.com",
                datetime.now(timezone.utc),
                {"theme": "dark", "language": "en"},
            ),
        )

        # Update counter
        await session.execute(
            f"UPDATE {table} SET login_count = login_count + 5 WHERE user_id = {user_id}"
        )

        # Export with writetime
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            output_file = f.name

        try:
            operator = BulkOperator(session=session)
            await operator.export(
                table=f"test_bulk.{table}",
                output_path=output_file,
                format="json",
                options={"include_writetime": True},
            )

            with open(output_file, "r") as f:
                data = json.load(f)
                assert len(data) == 1
                row = data[0]

                # Primary key - no writetime
                assert "user_id_writetime" not in row

                # Counter - no writetime
                assert "login_count_writetime" not in row
                assert row["login_count"] == 5

                # Regular columns - should have writetime
                assert "username_writetime" in row
                assert "email_writetime" in row
                assert "last_login_writetime" in row
                assert "preferences_writetime" in row

                # Verify values
                assert row["username"] == "testuser"
                assert row["email"] == "test@example.com"
                assert row["preferences"] == {"theme": "dark", "language": "en"}

        finally:
            os.unlink(output_file)

    @pytest.mark.asyncio
    async def test_static_columns_writetime(self, session):
        """Test writetime behavior with static columns."""
        table = f"test_static_writetime_{uuid.uuid4().hex[:8]}"

        await session.execute(
            f"""
            CREATE TABLE {table} (
                partition_id int,
                cluster_id int,
                static_data text STATIC,
                regular_data text,
                PRIMARY KEY (partition_id, cluster_id)
            )
        """
        )

        # Insert data with static column
        await session.execute(
            f"INSERT INTO {table} (partition_id, cluster_id, static_data, regular_data) VALUES (1, 1, 'static1', 'regular1')"
        )
        await session.execute(
            f"INSERT INTO {table} (partition_id, cluster_id, regular_data) VALUES (1, 2, 'regular2')"
        )

        # Export with writetime
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            output_file = f.name

        try:
            operator = BulkOperator(session=session)
            await operator.export(
                table=f"test_bulk.{table}",
                output_path=output_file,
                format="json",
                options={"include_writetime": True},
            )

            with open(output_file, "r") as f:
                rows = json.load(f)

                # Both rows should have the same static column writetime
                static_writetimes = [
                    row.get("static_data_writetime")
                    for row in rows
                    if "static_data_writetime" in row
                ]
                if static_writetimes:
                    assert all(wt == static_writetimes[0] for wt in static_writetimes)

                # Regular columns should have different writetimes
                for row in rows:
                    assert "regular_data_writetime" in row
                    assert row["regular_data_writetime"] is not None

        finally:
            os.unlink(output_file)

    @pytest.mark.asyncio
    @pytest.mark.skip(reason="Materialized views are disabled by default in Cassandra")
    async def test_materialized_view_writetime(self, session):
        """Test writetime export from materialized views."""
        base_table = f"test_base_table_{uuid.uuid4().hex[:8]}"
        view_name = f"test_view_{uuid.uuid4().hex[:8]}"

        # Create base table
        await session.execute(
            f"""
            CREATE TABLE {base_table} (
                id int,
                category text,
                name text,
                value int,
                PRIMARY KEY (id, category)
            )
        """
        )

        # Create materialized view
        await session.execute(
            f"""
            CREATE MATERIALIZED VIEW {view_name} AS
            SELECT * FROM {base_table}
            WHERE category IS NOT NULL AND id IS NOT NULL
            PRIMARY KEY (category, id)
        """
        )

        # Insert data
        stmt = await session.prepare(
            f"INSERT INTO {base_table} (id, category, name, value) VALUES (?, ?, ?, ?)"
        )
        await session.execute(stmt, (1, "electronics", "laptop", 1000))
        await session.execute(stmt, (2, "electronics", "phone", 500))
        await session.execute(stmt, (3, "books", "novel", 20))

        # Wait for view to be updated
        await asyncio.sleep(1)

        # Export from materialized view with writetime
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            output_file = f.name

        try:
            operator = BulkOperator(session=session)
            await operator.export(
                table=view_name,
                output_path=output_file,
                format="json",
                options={"include_writetime": True},
            )

            with open(output_file, "r") as f:
                rows = json.load(f)
                assert len(rows) == 3

                # Materialized views should have writetime for non-PK columns
                for row in rows:
                    # New primary key columns - no writetime
                    assert "category_writetime" not in row
                    assert "id_writetime" not in row

                    # Regular columns - should have writetime from base table
                    assert "name_writetime" in row
                    assert "value_writetime" in row

        finally:
            os.unlink(output_file)

    @pytest.mark.asyncio
    async def test_collection_writetime_behavior(self, session):
        """Test writetime behavior with collection columns."""
        table = f"test_collection_writetime_{uuid.uuid4().hex[:8]}"

        await session.execute(
            f"""
            CREATE TABLE {table} (
                id int PRIMARY KEY,
                tags set<text>,
                scores list<int>,
                metadata map<text, text>
            )
        """
        )

        # Insert data
        stmt = await session.prepare(
            f"INSERT INTO {table} (id, tags, scores, metadata) VALUES (?, ?, ?, ?)"
        )
        await session.execute(
            stmt, (1, {"tag1", "tag2", "tag3"}, [10, 20, 30], {"key1": "value1", "key2": "value2"})
        )

        # Update individual collection elements
        await session.execute(f"UPDATE {table} SET tags = tags + {{'tag4'}} WHERE id = 1")
        await session.execute(f"UPDATE {table} SET scores = scores + [40] WHERE id = 1")
        await session.execute(f"UPDATE {table} SET metadata['key3'] = 'value3' WHERE id = 1")

        # Export with writetime
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            output_file = f.name

        try:
            operator = BulkOperator(session=session)
            await operator.export(
                table=f"test_bulk.{table}",
                output_path=output_file,
                format="json",
                options={"include_writetime": True},
            )

            with open(output_file, "r") as f:
                data = json.load(f)
                row = data[0]

                # Collections should have writetime
                assert "tags_writetime" in row
                assert "scores_writetime" in row
                assert "metadata_writetime" in row

                # Note: Collection writetime is complex - it's the max writetime
                # of all elements in the collection
                assert row["tags_writetime"] is not None
                assert row["scores_writetime"] is not None
                assert row["metadata_writetime"] is not None

        finally:
            os.unlink(output_file)

    @pytest.mark.asyncio
    @pytest.mark.skip(reason="Cassandra doesn't allow mixing counter and non-counter columns")
    async def test_error_handling_counter_writetime_query(self, session):
        """Test that we handle errors gracefully when querying writetime on counters."""
        table = f"test_counter_error_{uuid.uuid4().hex[:8]}"

        await session.execute(
            f"""
            CREATE TABLE {table} (
                id int PRIMARY KEY,
                regular_col text,
                counter_col counter
            )
        """
        )

        # Insert regular data and update counter
        await session.execute(f"INSERT INTO {table} (id, regular_col) VALUES (1, 'test')")
        await session.execute(f"UPDATE {table} SET counter_col = counter_col + 10 WHERE id = 1")

        # Verify that direct WRITETIME query on counter fails
        with pytest.raises(Exception):
            # This should fail - WRITETIME not supported on counters
            await session.execute(
                f"SELECT id, regular_col, counter_col, WRITETIME(counter_col) FROM {table}"
            )

        # But our export should handle it gracefully
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            output_file = f.name

        try:
            operator = BulkOperator(session=session)
            await operator.export(
                table=f"test_bulk.{table}",
                output_path=output_file,
                format="json",
                options={"include_writetime": True},
            )

            with open(output_file, "r") as f:
                data = json.load(f)
                row = data[0]

                # Should export the data
                assert row["id"] == 1
                assert row["regular_col"] == "test"
                assert row["counter_col"] == 10

                # Writetime only for regular column
                assert "regular_col_writetime" in row
                assert "counter_col_writetime" not in row

        finally:
            os.unlink(output_file)
