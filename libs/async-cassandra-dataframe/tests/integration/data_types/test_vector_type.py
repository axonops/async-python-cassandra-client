"""
Test support for Cassandra vector datatype.

Cassandra 5.0+ introduces vector types for similarity search and AI workloads.
This test ensures we properly handle vector data types.
"""

import numpy as np
import pandas as pd
import pytest

import async_cassandra_dataframe as cdf


class TestVectorType:
    """Test Cassandra vector datatype support."""

    @pytest.mark.asyncio
    async def test_vector_type_basic(self, session, test_table_name):
        """
        Test basic vector type operations.

        What this tests:
        ---------------
        1. Creating tables with vector columns
        2. Inserting vector data
        3. Reading vector data back
        4. Preserving vector dimensions and values

        Why this matters:
        ----------------
        - Vector search is critical for AI/ML workloads
        - Embeddings must maintain precision
        - Dimension integrity is crucial
        """
        # Check if Cassandra supports vector types (5.0+)
        try:
            # Create table with vector column
            await session.execute(
                f"""
                CREATE TABLE {test_table_name} (
                    id INT PRIMARY KEY,
                    embedding VECTOR<FLOAT, 3>,
                    description TEXT
                )
            """
            )
        except Exception as e:
            if "Unknown type" in str(e) or "Invalid type" in str(e):
                pytest.skip("Cassandra version does not support VECTOR type")
            raise

        try:
            # Test data
            test_vectors = [
                (1, [0.1, 0.2, 0.3], "first vector"),
                (2, [1.0, 0.0, -1.0], "unit vector"),
                (3, [-0.5, 0.5, 0.0], "mixed vector"),
                (4, [float("nan"), float("inf"), float("-inf")], "special values"),
            ]

            # Insert vectors
            insert_stmt = await session.prepare(
                f"""
                INSERT INTO {test_table_name} (id, embedding, description)
                VALUES (?, ?, ?)
            """
            )

            for id_val, vector, desc in test_vectors:
                await session.execute(insert_stmt, (id_val, vector, desc))

            # Read back
            df = await cdf.read_cassandra_table(
                f"test_dataframe.{test_table_name}", session=session
            )
            pdf = df.compute()
            pdf = pdf.sort_values("id").reset_index(drop=True)

            # Verify vector data
            # Vector 1: Basic floats
            vec1 = pdf.iloc[0]["embedding"]
            assert isinstance(vec1, list | np.ndarray), f"Vector type wrong: {type(vec1)}"
            # Cassandra VECTOR<FLOAT> uses 32-bit precision
            expected = np.array([0.1, 0.2, 0.3], dtype=np.float32)
            if isinstance(vec1, list):
                vec1_arr = np.array(vec1, dtype=np.float32)
            else:
                vec1_arr = vec1
            np.testing.assert_array_almost_equal(vec1_arr, expected, decimal=6)

            # Vector 2: Unit vector
            vec2 = pdf.iloc[1]["embedding"]
            expected2 = np.array([1.0, 0.0, -1.0], dtype=np.float32)
            if isinstance(vec2, list):
                vec2_arr = np.array(vec2, dtype=np.float32)
            else:
                vec2_arr = vec2
            np.testing.assert_array_almost_equal(vec2_arr, expected2, decimal=6)

            # Vector 3: Mixed values
            vec3 = pdf.iloc[2]["embedding"]
            expected3 = np.array([-0.5, 0.5, 0.0], dtype=np.float32)
            if isinstance(vec3, list):
                vec3_arr = np.array(vec3, dtype=np.float32)
            else:
                vec3_arr = vec3
            np.testing.assert_array_almost_equal(vec3_arr, expected3, decimal=6)

            # Vector 4: Special values
            vec4 = pdf.iloc[3]["embedding"]
            if isinstance(vec4, list):
                assert np.isnan(vec4[0]), "NaN not preserved"
                assert np.isinf(vec4[1]) and vec4[1] > 0, "Positive infinity not preserved"
                assert np.isinf(vec4[2]) and vec4[2] < 0, "Negative infinity not preserved"
            else:
                assert np.isnan(vec4[0]), "NaN not preserved"
                assert np.isposinf(vec4[1]), "Positive infinity not preserved"
                assert np.isneginf(vec4[2]), "Negative infinity not preserved"

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    @pytest.mark.asyncio
    async def test_vector_type_dimensions(self, session, test_table_name):
        """
        Test vector types with different dimensions.

        What this tests:
        ---------------
        1. Vectors of different dimensions (1D to high-D)
        2. Large vectors (1024D, 1536D for embeddings)
        3. Dimension consistency

        Why this matters:
        ----------------
        - Different embedding models use different dimensions
        - OpenAI embeddings: 1536D
        - Many models: 384D, 768D, 1024D
        """
        # Skip if vector not supported
        try:
            await session.execute(
                f"""
                CREATE TABLE {test_table_name} (
                    id INT PRIMARY KEY,
                    small_vec VECTOR<FLOAT, 3>,
                    medium_vec VECTOR<FLOAT, 128>,
                    large_vec VECTOR<FLOAT, 1536>
                )
            """
            )
        except Exception as e:
            if "Unknown type" in str(e) or "Invalid type" in str(e):
                pytest.skip("Cassandra version does not support VECTOR type")
            raise

        try:
            # Create vectors of different sizes
            small = [1.0, 2.0, 3.0]
            medium = [float(i) / 128 for i in range(128)]
            large = [float(i) / 1536 for i in range(1536)]

            # Insert
            insert_stmt = await session.prepare(
                f"INSERT INTO {test_table_name} (id, small_vec, medium_vec, large_vec) VALUES (?, ?, ?, ?)"
            )
            await session.execute(insert_stmt, (1, small, medium, large))

            # Read back
            df = await cdf.read_cassandra_table(
                f"test_dataframe.{test_table_name}", session=session
            )
            pdf = df.compute()

            # Verify dimensions preserved
            assert len(pdf.iloc[0]["small_vec"]) == 3, "Small vector dimension wrong"
            assert len(pdf.iloc[0]["medium_vec"]) == 128, "Medium vector dimension wrong"
            assert len(pdf.iloc[0]["large_vec"]) == 1536, "Large vector dimension wrong"

            # Verify values preserved
            if isinstance(pdf.iloc[0]["small_vec"], list):
                assert pdf.iloc[0]["small_vec"] == small
            else:
                np.testing.assert_array_almost_equal(pdf.iloc[0]["small_vec"], small)

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    @pytest.mark.asyncio
    async def test_vector_null_handling(self, session, test_table_name):
        """
        Test NULL handling for vector types.

        What this tests:
        ---------------
        1. NULL vectors
        2. Partial NULL in collections of vectors
        3. Empty vector handling

        Why this matters:
        ----------------
        - Not all records may have embeddings
        - Proper NULL handling prevents errors
        """
        try:
            await session.execute(
                f"""
                CREATE TABLE {test_table_name} (
                    id INT PRIMARY KEY,
                    embedding VECTOR<FLOAT, 3>,
                    vector_list LIST<FROZEN<VECTOR<FLOAT, 2>>>
                )
            """
            )
        except Exception as e:
            if "Unknown type" in str(e) or "Invalid type" in str(e):
                pytest.skip("Cassandra version does not support VECTOR type")
            raise

        try:
            # Insert NULL and non-NULL vectors
            test_data = [
                (1, [1.0, 2.0, 3.0], [[0.1, 0.2], [0.3, 0.4]]),
                (2, None, None),
                (3, [4.0, 5.0, 6.0], []),
            ]

            insert_stmt = await session.prepare(
                f"INSERT INTO {test_table_name} (id, embedding, vector_list) VALUES (?, ?, ?)"
            )
            for row in test_data:
                await session.execute(insert_stmt, row)

            # Read back
            df = await cdf.read_cassandra_table(
                f"test_dataframe.{test_table_name}", session=session
            )
            pdf = df.compute()
            pdf = pdf.sort_values("id").reset_index(drop=True)

            # Verify NULL handling
            assert pdf.iloc[0]["embedding"] is not None, "Non-NULL vector became NULL"
            assert (
                pd.isna(pdf.iloc[1]["embedding"]) or pdf.iloc[1]["embedding"] is None
            ), "NULL vector not preserved"
            assert pdf.iloc[2]["embedding"] is not None, "Non-NULL vector became NULL"

            # Empty collection should be None in Cassandra
            assert (
                pd.isna(pdf.iloc[2]["vector_list"]) or pdf.iloc[2]["vector_list"] is None
            ), "Empty vector list not NULL"

        finally:
            await session.execute(f"DROP TABLE IF EXISTS {test_table_name}")
