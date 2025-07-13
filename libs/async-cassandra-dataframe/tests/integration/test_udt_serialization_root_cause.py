"""
Test to identify root cause of UDT string serialization.

This test compares UDT handling between:
1. Raw cassandra-driver
2. async-cassandra wrapper
3. async-cassandra-dataframe with and without Dask

What this tests:
---------------
1. UDT serialization at each layer of the stack
2. Identifies where UDTs get converted to strings
3. Tests nested UDTs and collections containing UDTs
4. Verifies if this is a cassandra-driver limitation or our bug

Why this matters:
----------------
- UDTs should remain as dict/namedtuple objects
- String serialization breaks type safety
- Users expect to access UDT fields directly
- This affects production data processing

Expected outcomes:
-----------------
- cassandra-driver: Returns namedtuple or dict-like objects
- async-cassandra: Should preserve the same behavior
- async-cassandra-dataframe: Should preserve UDT objects
- Dask serialization: May convert to strings (known limitation)
"""

import asyncio

# Import dataframe reader
import async_cassandra_dataframe as cdf
import dask.dataframe as dd

# Import async wrappers
from async_cassandra import AsyncCluster
from cassandra.cluster import Cluster


class TestUDTSerializationRootCause:
    """Test UDT serialization to find root cause."""

    @classmethod
    def setup_class(cls):
        """Set up test environment."""
        cls.keyspace = "test_udt_root_cause"

    def setup_method(self):
        """Create test keyspace and types."""
        # Use sync driver for setup
        cluster = Cluster(["localhost"])
        session = cluster.connect()

        # Create keyspace
        session.execute(
            f"""
            CREATE KEYSPACE IF NOT EXISTS {self.keyspace}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        """
        )
        session.set_keyspace(self.keyspace)

        # Create UDTs
        session.execute(
            """
            CREATE TYPE IF NOT EXISTS address (
                street text,
                city text,
                state text,
                zip_code int
            )
        """
        )

        session.execute(
            """
            CREATE TYPE IF NOT EXISTS contact_info (
                email text,
                phone text,
                address frozen<address>
            )
        """
        )

        # Create table with various UDT scenarios
        session.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id int PRIMARY KEY,
                name text,
                home_address frozen<address>,
                work_address frozen<address>,
                contact frozen<contact_info>,
                addresses list<frozen<address>>,
                contacts_by_type map<text, frozen<contact_info>>
            )
        """
        )

        # Insert test data
        session.execute(
            """
            INSERT INTO users (id, name, home_address, work_address, contact, addresses, contacts_by_type)
            VALUES (
                1,
                'Test User',
                {street: '123 Home St', city: 'HomeCity', state: 'HS', zip_code: 12345},
                {street: '456 Work Ave', city: 'WorkCity', state: 'WS', zip_code: 67890},
                {
                    email: 'test@example.com',
                    phone: '555-1234',
                    address: {street: '789 Contact Ln', city: 'ContactCity', state: 'CS', zip_code: 11111}
                },
                [
                    {street: '111 First St', city: 'FirstCity', state: 'FS', zip_code: 11111},
                    {street: '222 Second St', city: 'SecondCity', state: 'SS', zip_code: 22222}
                ],
                {
                    'personal': {
                        email: 'personal@example.com',
                        phone: '555-5555',
                        address: {street: '333 Personal St', city: 'PersonalCity', state: 'PS', zip_code: 33333}
                    },
                    'work': {
                        email: 'work@example.com',
                        phone: '555-9999',
                        address: {street: '444 Work St', city: 'WorkCity', state: 'WS', zip_code: 44444}
                    }
                }
            )
        """
        )

        session.shutdown()
        cluster.shutdown()

    def teardown_method(self):
        """Clean up test keyspace."""
        cluster = Cluster(["localhost"])
        session = cluster.connect()
        session.execute(f"DROP KEYSPACE IF EXISTS {self.keyspace}")
        session.shutdown()
        cluster.shutdown()

    def test_1_raw_cassandra_driver(self):
        """Test 1: Raw cassandra-driver UDT handling."""
        print("\n=== TEST 1: Raw cassandra-driver ===")

        cluster = Cluster(["localhost"])
        session = cluster.connect(self.keyspace)

        # Query data
        result = session.execute("SELECT * FROM users WHERE id = 1")
        row = result.one()

        print(f"Row type: {type(row)}")
        print(f"home_address type: {type(row.home_address)}")
        print(f"home_address value: {row.home_address}")
        print(f"home_address.city: {row.home_address.city}")

        print(f"\ncontact type: {type(row.contact)}")
        print(f"contact value: {row.contact}")
        print(f"contact.address type: {type(row.contact.address)}")
        print(f"contact.address.city: {row.contact.address.city}")

        print(f"\naddresses type: {type(row.addresses)}")
        print(f"addresses[0] type: {type(row.addresses[0])}")
        print(f"addresses[0].city: {row.addresses[0].city}")

        print(f"\ncontacts_by_type type: {type(row.contacts_by_type)}")
        print(f"contacts_by_type['personal'] type: {type(row.contacts_by_type['personal'])}")
        print(f"contacts_by_type['personal'].email: {row.contacts_by_type['personal'].email}")

        # Verify UDTs are NOT strings
        assert hasattr(row.home_address, "city"), "UDT should have city attribute"
        assert row.home_address.city == "HomeCity"
        assert hasattr(row.contact.address, "city"), "Nested UDT should have city attribute"
        assert row.contact.address.city == "ContactCity"

        session.shutdown()
        cluster.shutdown()

    async def test_2_async_cassandra_wrapper(self):
        """Test 2: async-cassandra wrapper UDT handling."""
        print("\n=== TEST 2: async-cassandra wrapper ===")

        async with AsyncCluster(["localhost"]) as cluster:
            session = await cluster.connect(self.keyspace)
            try:
                # Query data
                result = await session.execute("SELECT * FROM users WHERE id = 1")
                row = result.one()

                print(f"Row type: {type(row)}")
                print(f"home_address type: {type(row.home_address)}")
                print(f"home_address value: {row.home_address}")
                print(f"home_address.city: {row.home_address.city}")

                print(f"\ncontact type: {type(row.contact)}")
                print(f"contact value: {row.contact}")
                print(f"contact.address type: {type(row.contact.address)}")
                print(f"contact.address.city: {row.contact.address.city}")

                # Verify UDTs are still NOT strings
                assert hasattr(row.home_address, "city"), "UDT should have city attribute"
                assert row.home_address.city == "HomeCity"
                assert hasattr(row.contact.address, "city"), "Nested UDT should have city attribute"
                assert row.contact.address.city == "ContactCity"
            finally:
                await session.close()

    async def test_3_dataframe_no_dask(self):
        """Test 3: async-cassandra-dataframe without Dask (single partition)."""
        print("\n=== TEST 3: async-cassandra-dataframe (no Dask) ===")

        async with AsyncCluster(["localhost"]) as cluster:
            session = await cluster.connect(self.keyspace)
            try:
                # Read with single partition to avoid Dask serialization
                df = await cdf.read_cassandra_table(
                    "users",
                    session=session,
                    partition_count=1,  # Single partition
                    use_parallel_execution=False,  # No parallel execution
                )

                # Compute immediately
                pdf = df.compute()

                print(f"DataFrame shape: {pdf.shape}")
                print(f"Columns: {list(pdf.columns)}")

                # Check first row
                if len(pdf) > 0:
                    row = pdf.iloc[0]
                    print(f"\nhome_address type: {type(row['home_address'])}")
                    print(f"home_address value: {row['home_address']}")

                    # Try to access as dict
                    if isinstance(row["home_address"], dict):
                        print(f"home_address['city']: {row['home_address']['city']}")
                    elif isinstance(row["home_address"], str):
                        print("WARNING: home_address is a string!")
                        # Try to parse
                        try:
                            import ast

                            parsed = ast.literal_eval(row["home_address"])
                            print(f"Parsed city: {parsed['city']}")
                        except (ValueError, SyntaxError):
                            print("Failed to parse string")
                    else:
                        print(
                            f"home_address has attributes: {hasattr(row['home_address'], 'city')}"
                        )
                        if hasattr(row["home_address"], "city"):
                            print(f"home_address.city: {row['home_address'].city}")
            finally:
                await session.close()

    async def test_4_dataframe_with_dask(self):
        """Test 4: async-cassandra-dataframe with Dask (multiple partitions)."""
        print("\n=== TEST 4: async-cassandra-dataframe (with Dask) ===")

        async with AsyncCluster(["localhost"]) as cluster:
            session = await cluster.connect()
            try:
                # Read with multiple partitions to trigger Dask serialization
                df = await cdf.read_cassandra_table(
                    f"{self.keyspace}.users",
                    session=session,
                    partition_count=3,  # Multiple partitions
                    use_parallel_execution=False,  # Use Dask delayed
                )

                print(f"Dask DataFrame partitions: {df.npartitions}")

                # Check meta
                print("\nDask meta dtypes:")
                print(df.dtypes)

                # Compute
                pdf = df.compute()

                print(f"\nComputed DataFrame shape: {pdf.shape}")

                # Check first row
                if len(pdf) > 0:
                    row = pdf.iloc[0]
                    print(f"\nhome_address type: {type(row['home_address'])}")
                    print(f"home_address value: {row['home_address']}")

                    if isinstance(row["home_address"], str):
                        print("CONFIRMED: Dask serialization converts UDT to string!")
            finally:
                await session.close()

    async def test_5_dataframe_parallel_execution(self):
        """Test 5: async-cassandra-dataframe with parallel execution."""
        print("\n=== TEST 5: async-cassandra-dataframe (parallel execution) ===")

        async with AsyncCluster(["localhost"]) as cluster:
            session = await cluster.connect()
            try:
                # Read with parallel execution
                df = await cdf.read_cassandra_table(
                    f"{self.keyspace}.users",
                    session=session,
                    partition_count=3,
                    use_parallel_execution=True,  # Use async parallel
                )

                # This should return already computed data
                print(f"DataFrame type: {type(df)}")

                if isinstance(df, dd.DataFrame):
                    pdf = df.compute()
                else:
                    pdf = df

                print(f"DataFrame shape: {pdf.shape}")

                # Check first row
                if len(pdf) > 0:
                    row = pdf.iloc[0]
                    print(f"\nhome_address type: {type(row['home_address'])}")
                    print(f"home_address value: {row['home_address']}")

                    if isinstance(row["home_address"], dict):
                        print("SUCCESS: Parallel execution preserves UDT as dict!")
                        print(f"home_address['city']: {row['home_address']['city']}")
                    elif isinstance(row["home_address"], str):
                        print("ISSUE: Parallel execution also converts to string")
            finally:
                await session.close()

    async def test_6_direct_partition_read(self):
        """Test 6: Direct partition read to isolate the issue."""
        print("\n=== TEST 6: Direct partition read ===")

        async with AsyncCluster(["localhost"]) as cluster:
            session = await cluster.connect()
            try:
                from async_cassandra_dataframe.partition import StreamingPartitionStrategy

                # Create partition strategy
                strategy = StreamingPartitionStrategy(session=session, memory_per_partition_mb=128)

                # Create simple partition definition
                partition = {
                    "query": f"SELECT * FROM {self.keyspace}.users",
                    "table": f"{self.keyspace}.users",
                    "columns": ["id", "name", "home_address", "contact"],
                    "session": session,
                }

                # Stream the partition directly
                df = await strategy.stream_partition(partition)

                print(f"Direct read shape: {df.shape}")

                if len(df) > 0:
                    row = df.iloc[0]
                    print(f"\nhome_address type: {type(row['home_address'])}")
                    print(f"home_address value: {row['home_address']}")

                    # This should tell us if the issue is in partition reading
                    if isinstance(row["home_address"], dict):
                        print("Partition strategy preserves dict")
                    elif hasattr(row["home_address"], "city"):
                        print("Partition strategy preserves namedtuple")
                    else:
                        print("Issue is in partition reading!")
            finally:
                await session.close()


def run_tests():
    """Run all tests in sequence."""
    test = TestUDTSerializationRootCause()
    test.setup_class()

    try:
        # Test 1: Raw driver
        test.setup_method()
        try:
            test.test_1_raw_cassandra_driver()
        finally:
            test.teardown_method()

        # Test 2: Async wrapper
        test.setup_method()
        try:
            asyncio.run(test.test_2_async_cassandra_wrapper())
        finally:
            test.teardown_method()

        # Test 3: DataFrame no Dask
        test.setup_method()
        try:
            asyncio.run(test.test_3_dataframe_no_dask())
        finally:
            test.teardown_method()

        # Test 4: DataFrame with Dask
        test.setup_method()
        try:
            asyncio.run(test.test_4_dataframe_with_dask())
        finally:
            test.teardown_method()

        # Test 5: Parallel execution
        test.setup_method()
        try:
            asyncio.run(test.test_5_dataframe_parallel_execution())
        finally:
            test.teardown_method()

        # Test 6: Direct partition
        test.setup_method()
        try:
            asyncio.run(test.test_6_direct_partition_read())
        finally:
            test.teardown_method()

    except Exception as e:
        print(f"\nTest failed with error: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    run_tests()
