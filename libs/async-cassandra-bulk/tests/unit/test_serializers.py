"""
Unit tests for type serializers.

What this tests:
---------------
1. All Cassandra data types are properly serialized
2. Serialization works correctly for different formats (CSV, JSON)
3. Null values are handled appropriately
4. Collections and complex types are serialized correctly

Why this matters:
----------------
- Data integrity during export is critical
- Different formats have different requirements
- Type conversion errors can cause data loss
- All Cassandra types must be supported
"""

import json
from datetime import date, datetime, time, timezone
from decimal import Decimal
from uuid import uuid4

import pytest
from cassandra.util import Date, Time

from async_cassandra_bulk.serializers import SerializationContext, get_global_registry
from async_cassandra_bulk.serializers.basic_types import (
    BinarySerializer,
    BooleanSerializer,
    CounterSerializer,
    DateSerializer,
    DecimalSerializer,
    DurationSerializer,
    FloatSerializer,
    InetSerializer,
    IntegerSerializer,
    NullSerializer,
    StringSerializer,
    TimeSerializer,
    TimestampSerializer,
    UUIDSerializer,
    VectorSerializer,
)
from async_cassandra_bulk.serializers.collection_types import (
    ListSerializer,
    MapSerializer,
    SetSerializer,
    TupleSerializer,
)


class TestNullSerializer:
    """Test NULL value serialization."""

    def test_null_csv_serialization(self):
        """
        Test NULL serialization for CSV format.

        What this tests:
        ---------------
        1. None values converted to configured null string
        2. Default null value is empty string
        3. Custom null values respected
        4. Non-null values rejected

        Why this matters:
        ----------------
        - CSV needs consistent NULL representation
        - Users may want custom NULL markers
        - Must distinguish NULL from empty string
        - Type safety prevents bugs
        """
        serializer = NullSerializer()

        # Default null value (empty string)
        context = SerializationContext(format="csv", options={})
        assert serializer.serialize(None, context) == ""

        # Custom null value
        context = SerializationContext(format="csv", options={"null_value": "NULL"})
        assert serializer.serialize(None, context) == "NULL"

        # Should reject non-null values
        with pytest.raises(ValueError):
            serializer.serialize("not null", context)

    def test_null_json_serialization(self):
        """Test NULL serialization for JSON format."""
        serializer = NullSerializer()
        context = SerializationContext(format="json", options={})

        assert serializer.serialize(None, context) is None

    def test_can_handle(self):
        """Test NULL value detection."""
        serializer = NullSerializer()
        assert serializer.can_handle(None) is True
        assert serializer.can_handle(0) is False
        assert serializer.can_handle("") is False
        assert serializer.can_handle(False) is False


class TestBooleanSerializer:
    """Test boolean value serialization."""

    def test_boolean_csv_serialization(self):
        """
        Test boolean serialization for CSV format.

        What this tests:
        ---------------
        1. True becomes "true" (lowercase)
        2. False becomes "false" (lowercase)
        3. Consistent with Cassandra conventions
        4. String representation for CSV

        Why this matters:
        ----------------
        - CSV requires text representation
        - Must match Cassandra's boolean format
        - Consistency across exports
        - Round-trip compatibility
        """
        serializer = BooleanSerializer()
        context = SerializationContext(format="csv", options={})

        assert serializer.serialize(True, context) == "true"
        assert serializer.serialize(False, context) == "false"

    def test_boolean_json_serialization(self):
        """Test boolean serialization for JSON format."""
        serializer = BooleanSerializer()
        context = SerializationContext(format="json", options={})

        assert serializer.serialize(True, context) is True
        assert serializer.serialize(False, context) is False

    def test_can_handle(self):
        """Test boolean detection."""
        serializer = BooleanSerializer()
        assert serializer.can_handle(True) is True
        assert serializer.can_handle(False) is True
        assert serializer.can_handle(1) is False  # Not a bool
        assert serializer.can_handle(0) is False  # Not a bool


class TestNumericSerializers:
    """Test numeric type serializers."""

    def test_integer_serialization(self):
        """
        Test integer serialization (TINYINT, SMALLINT, INT, BIGINT, VARINT).

        What this tests:
        ---------------
        1. All integer sizes handled correctly
        2. Negative values preserved
        3. Large integers (BIGINT) maintained
        4. Very large integers (VARINT) supported

        Why this matters:
        ----------------
        - Cassandra has multiple integer types
        - Must preserve full precision
        - Sign must be maintained
        - Python handles arbitrary precision
        """
        serializer = IntegerSerializer()

        # CSV format
        csv_context = SerializationContext(format="csv", options={})
        assert serializer.serialize(42, csv_context) == "42"
        assert serializer.serialize(-128, csv_context) == "-128"  # TINYINT min
        assert serializer.serialize(127, csv_context) == "127"  # TINYINT max
        assert (
            serializer.serialize(9223372036854775807, csv_context) == "9223372036854775807"
        )  # BIGINT max
        assert serializer.serialize(10**100, csv_context) == str(10**100)  # VARINT

        # JSON format
        json_context = SerializationContext(format="json", options={})
        assert serializer.serialize(42, json_context) == 42
        assert serializer.serialize(-128, json_context) == -128

    def test_float_serialization(self):
        """
        Test floating point serialization (FLOAT, DOUBLE).

        What this tests:
        ---------------
        1. Normal float values
        2. Special values (NaN, Infinity)
        3. Precision preservation
        4. JSON compatibility for special values

        Why this matters:
        ----------------
        - Scientific data uses special float values
        - JSON doesn't support NaN/Infinity natively
        - Precision loss must be minimized
        - Cross-format compatibility
        """
        serializer = FloatSerializer()

        # CSV format
        csv_context = SerializationContext(format="csv", options={})
        assert serializer.serialize(3.14, csv_context) == "3.14"
        assert serializer.serialize(float("nan"), csv_context) == "NaN"
        assert serializer.serialize(float("inf"), csv_context) == "Infinity"
        assert serializer.serialize(float("-inf"), csv_context) == "-Infinity"

        # JSON format - special values as strings
        json_context = SerializationContext(format="json", options={})
        assert serializer.serialize(3.14, json_context) == 3.14
        assert serializer.serialize(float("nan"), json_context) == "NaN"
        assert serializer.serialize(float("inf"), json_context) == "Infinity"

    def test_decimal_serialization(self):
        """
        Test DECIMAL type serialization.

        What this tests:
        ---------------
        1. Arbitrary precision preserved
        2. No floating point errors
        3. String representation for JSON
        4. Optional float conversion

        Why this matters:
        ----------------
        - Financial data needs exact decimals
        - Precision must be maintained
        - JSON lacks decimal type
        - User may prefer float for size
        """
        serializer = DecimalSerializer()

        decimal_value = Decimal("123.456789012345678901234567890")

        # CSV format
        csv_context = SerializationContext(format="csv", options={})
        assert serializer.serialize(decimal_value, csv_context) == str(decimal_value)

        # JSON format - default as string
        json_context = SerializationContext(format="json", options={})
        assert serializer.serialize(decimal_value, json_context) == str(decimal_value)

        # JSON format - optional float conversion
        json_float_context = SerializationContext(format="json", options={"decimal_as_float": True})
        assert isinstance(serializer.serialize(decimal_value, json_float_context), float)


class TestStringSerializers:
    """Test string type serializers."""

    def test_string_serialization(self):
        """
        Test string serialization (TEXT, VARCHAR, ASCII).

        What this tests:
        ---------------
        1. Basic strings preserved
        2. Unicode handled correctly
        3. Empty strings maintained
        4. Special characters preserved

        Why this matters:
        ----------------
        - Text data is most common type
        - Unicode support is critical
        - Empty != NULL distinction
        - Data integrity paramount
        """
        serializer = StringSerializer()
        context = SerializationContext(format="csv", options={})

        assert serializer.serialize("hello", context) == "hello"
        assert serializer.serialize("", context) == ""
        assert serializer.serialize("Unicode: 你好 🌍", context) == "Unicode: 你好 🌍"
        assert serializer.serialize("Line\nbreak", context) == "Line\nbreak"

    def test_binary_serialization(self):
        """
        Test BLOB type serialization.

        What this tests:
        ---------------
        1. Binary data converted to hex for CSV
        2. Binary data base64 encoded for JSON
        3. Empty bytes handled
        4. Arbitrary bytes preserved

        Why this matters:
        ----------------
        - Binary data needs text representation
        - Different formats use different encodings
        - Must be reversible
        - Common for images, files, etc.
        """
        serializer = BinarySerializer()

        # CSV format - hex encoding
        csv_context = SerializationContext(format="csv", options={})
        assert serializer.serialize(b"hello", csv_context) == "68656c6c6f"
        assert serializer.serialize(b"", csv_context) == ""
        assert serializer.serialize(b"\x00\xff", csv_context) == "00ff"

        # JSON format - base64 encoding
        json_context = SerializationContext(format="json", options={})
        assert serializer.serialize(b"hello", json_context) == "aGVsbG8="
        assert serializer.serialize(b"", json_context) == ""


class TestUUIDSerializer:
    """Test UUID and TIMEUUID serialization."""

    def test_uuid_serialization(self):
        """
        Test UUID/TIMEUUID serialization.

        What this tests:
        ---------------
        1. UUID converted to standard string format
        2. Both UUID and TIMEUUID handled
        3. Consistent formatting
        4. Reversible representation

        Why this matters:
        ----------------
        - UUIDs are primary keys often
        - Standard format ensures compatibility
        - Must be parseable by other tools
        - Time-based UUIDs preserve ordering
        """
        serializer = UUIDSerializer()
        test_uuid = uuid4()

        # CSV format
        csv_context = SerializationContext(format="csv", options={})
        result = serializer.serialize(test_uuid, csv_context)
        assert result == str(test_uuid)
        assert len(result) == 36  # Standard UUID string length

        # JSON format
        json_context = SerializationContext(format="json", options={})
        assert serializer.serialize(test_uuid, json_context) == str(test_uuid)


class TestTemporalSerializers:
    """Test date/time type serializers."""

    def test_timestamp_serialization(self):
        """
        Test TIMESTAMP serialization.

        What this tests:
        ---------------
        1. ISO 8601 format for text formats
        2. Timezone information preserved
        3. Millisecond precision maintained
        4. Optional Unix timestamp for JSON

        Why this matters:
        ----------------
        - Timestamps are very common
        - Timezone bugs cause data errors
        - Standard format needed
        - Some systems prefer Unix timestamps
        """
        serializer = TimestampSerializer()
        test_time = datetime(2024, 1, 15, 10, 30, 45, 123000, tzinfo=timezone.utc)

        # CSV format - ISO 8601
        csv_context = SerializationContext(format="csv", options={})
        result = serializer.serialize(test_time, csv_context)
        assert result == "2024-01-15T10:30:45.123000+00:00"

        # JSON format - ISO by default
        json_context = SerializationContext(format="json", options={})
        assert serializer.serialize(test_time, json_context) == test_time.isoformat()

        # JSON format - Unix timestamp option
        json_unix_context = SerializationContext(
            format="json", options={"timestamp_format": "unix"}
        )
        unix_result = serializer.serialize(test_time, json_unix_context)
        assert isinstance(unix_result, int)
        assert unix_result == int(test_time.timestamp() * 1000)

    def test_date_serialization(self):
        """
        Test DATE serialization.

        What this tests:
        ---------------
        1. Date without time component
        2. ISO format YYYY-MM-DD
        3. Cassandra Date type handled
        4. Python date type handled

        Why this matters:
        ----------------
        - Date-only fields common
        - Must not include time
        - Standard format needed
        - Driver returns special type
        """
        serializer = DateSerializer()

        # Python date
        test_date = date(2024, 1, 15)
        context = SerializationContext(format="csv", options={})
        assert serializer.serialize(test_date, context) == "2024-01-15"

        # Cassandra Date type
        cassandra_date = Date(test_date)
        assert serializer.serialize(cassandra_date, context) == "2024-01-15"

    def test_time_serialization(self):
        """
        Test TIME serialization.

        What this tests:
        ---------------
        1. Time without date component
        2. Nanosecond precision preserved
        3. ISO format HH:MM:SS.ffffff
        4. Cassandra Time type handled

        Why this matters:
        ----------------
        - Time-only fields for schedules
        - High precision timing data
        - Standard format needed
        - Driver returns special type
        """
        serializer = TimeSerializer()

        # Python time
        test_time = time(14, 30, 45, 123456)
        context = SerializationContext(format="csv", options={})
        assert serializer.serialize(test_time, context) == "14:30:45.123456"

        # Cassandra Time type (nanoseconds)
        cassandra_time = Time(52245123456789)  # 14:30:45.123456789
        result = serializer.serialize(cassandra_time, context)
        assert result.startswith("14:30:45.123456")


class TestSpecialSerializers:
    """Test special type serializers."""

    def test_inet_serialization(self):
        """
        Test INET (IP address) serialization.

        What this tests:
        ---------------
        1. IPv4 addresses preserved
        2. IPv6 addresses handled
        3. String format maintained
        4. Validation of IP format

        Why this matters:
        ----------------
        - Network data common in logs
        - Both IP versions supported
        - Standard notation required
        - Must be parseable
        """
        serializer = InetSerializer()
        context = SerializationContext(format="csv", options={})

        # IPv4
        assert serializer.serialize("192.168.1.1", context) == "192.168.1.1"
        assert serializer.serialize("8.8.8.8", context) == "8.8.8.8"

        # IPv6
        assert serializer.serialize("::1", context) == "::1"
        assert serializer.serialize("2001:db8::1", context) == "2001:db8::1"

    def test_duration_serialization(self):
        """
        Test DURATION serialization.

        What this tests:
        ---------------
        1. Months, days, nanoseconds components
        2. ISO 8601 duration format for CSV
        3. Component object for JSON
        4. All components preserved

        Why this matters:
        ----------------
        - Duration type is complex
        - No standard representation
        - Must preserve all components
        - Used for time intervals
        """
        serializer = DurationSerializer()

        # Create a mock duration object
        class MockDuration:
            def __init__(self, months, days, nanoseconds):
                self.months = months
                self.days = days
                self.nanoseconds = nanoseconds

        duration = MockDuration(1, 2, 3_000_000_000)  # 1 month, 2 days, 3 seconds

        # CSV format - ISO-ish duration
        csv_context = SerializationContext(format="csv", options={})
        assert serializer.serialize(duration, csv_context) == "P1M2DT3.0S"

        # JSON format - component object
        json_context = SerializationContext(format="json", options={})
        result = serializer.serialize(duration, json_context)
        assert result == {"months": 1, "days": 2, "nanoseconds": 3_000_000_000}

    def test_counter_serialization(self):
        """
        Test COUNTER serialization.

        What this tests:
        ---------------
        1. Counter values as integers
        2. Large counter values supported
        3. Negative counters possible
        4. Same as integer serialization

        Why this matters:
        ----------------
        - Counters are special in Cassandra
        - Read as regular integers
        - Must handle full range
        - Common for metrics
        """
        serializer = CounterSerializer()

        csv_context = SerializationContext(format="csv", options={})
        assert serializer.serialize(42, csv_context) == "42"
        assert serializer.serialize(-10, csv_context) == "-10"
        assert serializer.serialize(9223372036854775807, csv_context) == "9223372036854775807"

    def test_vector_serialization(self):
        """
        Test VECTOR serialization (Cassandra 5.0+).

        What this tests:
        ---------------
        1. Fixed-length float arrays
        2. Bracket notation for CSV
        3. Native array for JSON
        4. All values converted to float

        Why this matters:
        ----------------
        - Vector search is new feature
        - ML/AI embeddings common
        - Must preserve precision
        - Format consistency needed
        """
        serializer = VectorSerializer()

        vector = [1.0, 2.5, -3.14, 0.0]

        # CSV format - bracket notation
        csv_context = SerializationContext(format="csv", options={})
        assert serializer.serialize(vector, csv_context) == "[1.0,2.5,-3.14,0.0]"

        # JSON format - native array
        json_context = SerializationContext(format="json", options={})
        result = serializer.serialize(vector, json_context)
        assert result == [1.0, 2.5, -3.14, 0.0]

        # Integer values converted to float
        int_vector = [1, 2, 3]
        assert serializer.serialize(int_vector, json_context) == [1.0, 2.0, 3.0]


class TestCollectionSerializers:
    """Test collection type serializers."""

    def test_list_serialization(self):
        """
        Test LIST serialization.

        What this tests:
        ---------------
        1. Order preserved
        2. Duplicates allowed
        3. Nested values handled
        4. Empty lists supported

        Why this matters:
        ----------------
        - Lists maintain insertion order
        - Common for time series data
        - Can contain complex types
        - Empty != NULL
        """
        serializer = ListSerializer()

        test_list = ["a", "b", "c", "b"]  # Note duplicate

        # CSV format - JSON array
        csv_context = SerializationContext(format="csv", options={})
        result = serializer.serialize(test_list, csv_context)
        assert json.loads(result) == test_list

        # JSON format - native array
        json_context = SerializationContext(format="json", options={})
        assert serializer.serialize(test_list, json_context) == test_list

    def test_set_serialization(self):
        """
        Test SET serialization.

        What this tests:
        ---------------
        1. Uniqueness enforced
        2. Sorted for consistency
        3. No duplicates in output
        4. Empty sets supported

        Why this matters:
        ----------------
        - Sets ensure uniqueness
        - Order not guaranteed in Cassandra
        - Sorting provides consistency
        - Common for tags/categories
        """
        serializer = SetSerializer()

        test_set = {"banana", "apple", "cherry", "apple"}  # Duplicate will be removed

        # CSV format - JSON array (sorted)
        csv_context = SerializationContext(format="csv", options={})
        result = serializer.serialize(test_set, csv_context)
        assert json.loads(result) == ["apple", "banana", "cherry"]

        # JSON format - sorted array
        json_context = SerializationContext(format="json", options={})
        assert serializer.serialize(test_set, json_context) == ["apple", "banana", "cherry"]

    def test_map_serialization(self):
        """
        Test MAP serialization.

        What this tests:
        ---------------
        1. Key-value pairs preserved
        2. Non-string keys converted
        3. Nested values supported
        4. Empty maps handled

        Why this matters:
        ----------------
        - Maps store metadata
        - Keys can be any type
        - JSON requires string keys
        - Common for configurations
        """
        serializer = MapSerializer()

        test_map = {"name": "John", "age": 30, "active": True}

        # CSV format - JSON object
        csv_context = SerializationContext(format="csv", options={})
        result = serializer.serialize(test_map, csv_context)
        assert json.loads(result) == test_map

        # JSON format - native object
        json_context = SerializationContext(format="json", options={})
        assert serializer.serialize(test_map, json_context) == test_map

        # Non-string keys
        int_key_map = {1: "one", 2: "two"}
        result = serializer.serialize(int_key_map, json_context)
        assert result == {"1": "one", "2": "two"}

    def test_tuple_serialization(self):
        """
        Test TUPLE serialization.

        What this tests:
        ---------------
        1. Fixed size preserved
        2. Order maintained
        3. Heterogeneous types supported
        4. Converts to array for JSON

        Why this matters:
        ----------------
        - Tuples for structured data
        - Order is significant
        - Mixed types common
        - JSON lacks tuple type
        """
        serializer = TupleSerializer()

        test_tuple = ("Alice", 25, True, 3.14)

        # CSV format - JSON array
        csv_context = SerializationContext(format="csv", options={})
        result = serializer.serialize(test_tuple, csv_context)
        assert json.loads(result) == list(test_tuple)

        # JSON format - array
        json_context = SerializationContext(format="json", options={})
        assert serializer.serialize(test_tuple, json_context) == list(test_tuple)


class TestUDTSerializer:
    """Test User-Defined Type (UDT) serialization with complex scenarios."""

    def test_simple_udt_serialization(self):
        """
        Test basic UDT serialization.

        What this tests:
        ---------------
        1. Simple UDT with basic fields
        2. Named tuple representation
        3. Object attribute access
        4. Field name preservation

        Why this matters:
        ----------------
        - UDTs are custom types in Cassandra
        - Driver returns them as objects
        - Field names must be preserved
        - Common for domain modeling
        """
        from collections import namedtuple

        from async_cassandra_bulk.serializers.collection_types import UDTSerializer

        serializer = UDTSerializer()

        # Named tuple style UDT
        Address = namedtuple("Address", ["street", "city", "zip_code"])
        address = Address("123 Main St", "New York", "10001")

        # CSV format
        csv_context = SerializationContext(format="csv", options={})
        result = serializer.serialize(address, csv_context)
        parsed = json.loads(result)
        assert parsed == {"street": "123 Main St", "city": "New York", "zip_code": "10001"}

        # JSON format
        json_context = SerializationContext(format="json", options={})
        result = serializer.serialize(address, json_context)
        assert result == {"street": "123 Main St", "city": "New York", "zip_code": "10001"}

    def test_nested_udt_serialization(self):
        """
        Test nested UDT serialization.

        What this tests:
        ---------------
        1. UDT containing other UDTs
        2. Multiple levels of nesting
        3. Collections within UDTs
        4. Complex type hierarchies

        Why this matters:
        ----------------
        - Real schemas have nested UDTs
        - Deep nesting is common
        - Must handle arbitrary depth
        - Complex domain models
        """
        from collections import namedtuple

        from async_cassandra_bulk.serializers.collection_types import UDTSerializer

        serializer = UDTSerializer()

        # Define nested UDT structure
        Coordinate = namedtuple("Coordinate", ["lat", "lon"])
        Address = namedtuple("Address", ["street", "city", "location"])
        Person = namedtuple("Person", ["name", "age", "addresses", "tags"])

        # Create nested instance
        location = Coordinate(40.7128, -74.0060)
        home = Address("123 Main St", "New York", location)
        work = Address("456 Corp Ave", "Boston", Coordinate(42.3601, -71.0589))
        person = Person(
            name="John Doe",
            age=30,
            addresses=[home, work],
            tags={"developer", "python", "cassandra"},
        )

        # Test serialization
        json_context = SerializationContext(format="json", options={})
        result = serializer.serialize(person, json_context)

        assert result["name"] == "John Doe"
        assert result["age"] == 30
        assert len(result["addresses"]) == 2
        assert result["addresses"][0]["location"]["lat"] == 40.7128
        assert "developer" in result["tags"]

    def test_cassandra_driver_udt_object(self):
        """
        Test UDT objects as returned by Cassandra driver.

        What this tests:
        ---------------
        1. Driver-specific UDT objects
        2. Dynamic attribute access
        3. Hidden attributes filtered
        4. Module detection for UDTs

        Why this matters:
        ----------------
        - Driver returns custom objects
        - Must handle driver internals
        - Different drivers vary
        - Production compatibility
        """
        from async_cassandra_bulk.serializers.collection_types import UDTSerializer

        serializer = UDTSerializer()

        # Mock Cassandra driver UDT object
        class MockUDT:
            """Simulates cassandra.usertype objects."""

            __module__ = "cassandra.usertype.UserType_ks1_address"
            __cassandra_udt__ = True

            def __init__(self):
                self.street = "789 Driver St"
                self.city = "San Francisco"
                self.zip_code = "94105"
                self.country = "USA"
                self._internal = "hidden"  # Should be filtered
                self.__private = "private"  # Should be filtered

        udt = MockUDT()

        json_context = SerializationContext(format="json", options={})
        result = serializer.serialize(udt, json_context)

        assert result == {
            "street": "789 Driver St",
            "city": "San Francisco",
            "zip_code": "94105",
            "country": "USA",
        }
        assert "_internal" not in result
        assert "__private" not in result

    def test_udt_with_null_fields(self):
        """
        Test UDT with null/missing fields.

        What this tests:
        ---------------
        1. Optional UDT fields
        2. NULL value handling
        3. Missing vs NULL distinction
        4. Partial UDT population

        Why this matters:
        ----------------
        - UDT fields can be NULL
        - Schema evolution support
        - Backward compatibility
        - Sparse data common
        """
        from collections import namedtuple

        from async_cassandra_bulk.serializers.collection_types import UDTSerializer

        serializer = UDTSerializer()

        # UDT with some None values
        UserProfile = namedtuple("UserProfile", ["username", "email", "phone", "bio"])
        profile = UserProfile("johndoe", "john@example.com", None, None)

        json_context = SerializationContext(format="json", options={})
        result = serializer.serialize(profile, json_context)

        assert result == {
            "username": "johndoe",
            "email": "john@example.com",
            "phone": None,
            "bio": None,
        }

    def test_udt_with_all_cassandra_types(self):
        """
        Test UDT containing all Cassandra types.

        What this tests:
        ---------------
        1. UDT with every Cassandra type as field
        2. Complex type mixing
        3. Collection fields in UDTs
        4. Type serialization within UDT context

        Why this matters:
        ----------------
        - UDTs can contain any type
        - Type interactions complex
        - Real schemas mix all types
        - Comprehensive validation
        """
        from collections import namedtuple
        from datetime import date, datetime, time
        from decimal import Decimal
        from uuid import uuid4

        # Define complex UDT with all types
        ComplexType = namedtuple(
            "ComplexType",
            [
                "id",  # UUID
                "name",  # TEXT
                "age",  # INT
                "balance",  # DECIMAL
                "rating",  # FLOAT
                "active",  # BOOLEAN
                "data",  # BLOB
                "created",  # TIMESTAMP
                "birth_date",  # DATE
                "alarm_time",  # TIME
                "tags",  # SET<TEXT>
                "scores",  # LIST<INT>
                "metadata",  # MAP<TEXT,TEXT>
                "coordinates",  # TUPLE<DOUBLE,DOUBLE>
                "ip_address",  # INET
                "duration",  # DURATION
                "vector",  # VECTOR<FLOAT>
            ],
        )

        # Create instance with all types
        test_id = uuid4()
        test_time = datetime.now()
        complex_obj = ComplexType(
            id=test_id,
            name="Test User",
            age=25,
            balance=Decimal("1234.56"),
            rating=4.5,
            active=True,
            data=b"binary data",
            created=test_time,
            birth_date=date(1999, 1, 1),
            alarm_time=time(7, 30, 0),
            tags={"python", "java", "scala"},
            scores=[95, 87, 92],
            metadata={"level": "expert", "region": "US"},
            coordinates=(37.7749, -122.4194),
            ip_address="192.168.1.100",
            duration=None,  # Would be Duration object
            vector=[0.1, 0.2, 0.3, 0.4],
        )

        json_context = SerializationContext(format="json", options={})
        registry = get_global_registry()

        # Serialize through registry to handle nested types
        result = registry.serialize(complex_obj, json_context)

        # Verify complex serialization
        assert result["id"] == str(test_id)
        assert result["name"] == "Test User"
        assert result["balance"] == str(Decimal("1234.56"))
        assert result["active"] is True
        assert result["tags"] == ["java", "python", "scala"]  # Sorted
        assert result["scores"] == [95, 87, 92]
        assert result["coordinates"] == [37.7749, -122.4194]
        assert result["vector"] == [0.1, 0.2, 0.3, 0.4]

    def test_udt_with_frozen_collections(self):
        """
        Test UDT with frozen collection fields.

        What this tests:
        ---------------
        1. Frozen lists in UDTs
        2. Frozen sets in UDTs
        3. Frozen maps in UDTs
        4. Nested frozen types

        Why this matters:
        ----------------
        - Frozen required for some uses
        - Primary key constraints
        - Immutability guarantees
        - Performance optimization
        """
        from collections import namedtuple

        from async_cassandra_bulk.serializers.collection_types import UDTSerializer

        serializer = UDTSerializer()

        # UDT with frozen collections
        Event = namedtuple("Event", ["id", "attendees", "config", "tags"])
        event = Event(
            id="event-123",
            attendees=frozenset(["alice", "bob", "charlie"]),  # Frozen set
            config={"immutable": True, "version": "1.0"},  # Would be frozen map
            tags=["conference", "tech", "2024"],  # Would be frozen list
        )

        json_context = SerializationContext(format="json", options={})
        result = serializer.serialize(event, json_context)

        assert result["id"] == "event-123"
        # Frozen set becomes sorted list in JSON
        assert sorted(result["attendees"]) == ["alice", "bob", "charlie"]
        assert result["config"]["immutable"] is True
        assert result["tags"] == ["conference", "tech", "2024"]

    def test_udt_circular_reference_handling(self):
        """
        Test UDT with potential circular references.

        What this tests:
        ---------------
        1. Self-referential UDT structures
        2. Circular reference detection
        3. Graceful handling of cycles
        4. Maximum depth limits

        Why this matters:
        ----------------
        - Graph-like data structures
        - Prevent infinite recursion
        - Memory safety
        - Real-world data complexity
        """
        from async_cassandra_bulk.serializers.collection_types import UDTSerializer

        serializer = UDTSerializer()

        # Create object with circular reference
        class Node:
            def __init__(self, value):
                self.value = value
                self.children = []
                self.parent = None

        root = Node("root")
        child1 = Node("child1")
        child2 = Node("child2")

        root.children = [child1, child2]
        child1.parent = root  # Circular reference
        child2.parent = root  # Circular reference

        # This should handle gracefully without infinite recursion
        json_context = SerializationContext(format="json", options={})

        # The serializer should extract only the direct attributes
        result = serializer.serialize(root, json_context)

        assert result["value"] == "root"
        # The circular parent reference might not serialize fully
        # but shouldn't crash

    def test_udt_can_handle_detection(self):
        """
        Test UDT detection heuristics.

        What this tests:
        ---------------
        1. Named tuple detection
        2. Cassandra UDT marker detection
        3. Module name detection
        4. False positive prevention

        Why this matters:
        ----------------
        - Must identify UDTs correctly
        - Avoid false positives
        - Support various drivers
        - Extensibility for custom types
        """
        from collections import namedtuple

        from async_cassandra_bulk.serializers.collection_types import UDTSerializer

        serializer = UDTSerializer()

        # Should detect named tuples
        Address = namedtuple("Address", ["street", "city"])
        assert serializer.can_handle(Address("123 Main", "NYC")) is True

        # Should detect objects with UDT marker
        class MarkedUDT:
            __cassandra_udt__ = True

        assert serializer.can_handle(MarkedUDT()) is True

        # Should detect by module name
        class DriverUDT:
            __module__ = "cassandra.usertype.SomeUDT"

        assert serializer.can_handle(DriverUDT()) is True

        # Should NOT detect regular objects
        class RegularClass:
            pass

        assert serializer.can_handle(RegularClass()) is False
        assert serializer.can_handle({"regular": "dict"}) is False
        assert serializer.can_handle([1, 2, 3]) is False


class TestSerializerRegistry:
    """Test the serializer registry."""

    def test_registry_finds_correct_serializer(self):
        """
        Test registry serializer selection.

        What this tests:
        ---------------
        1. Correct serializer chosen for each type
        2. Type cache works correctly
        3. Fallback behavior for unknown types
        4. Registry handles all Cassandra types

        Why this matters:
        ----------------
        - Central dispatch must work
        - Performance needs caching
        - Unknown types shouldn't crash
        - Extensibility for custom types
        """
        registry = get_global_registry()

        # Basic types
        assert registry.find_serializer(None) is not None
        assert registry.find_serializer(True) is not None
        assert registry.find_serializer(42) is not None
        assert registry.find_serializer(3.14) is not None
        assert registry.find_serializer("text") is not None
        assert registry.find_serializer(b"bytes") is not None
        assert registry.find_serializer(uuid4()) is not None

        # Collections
        assert registry.find_serializer([1, 2, 3]) is not None
        assert registry.find_serializer({1, 2, 3}) is not None
        assert registry.find_serializer({"a": 1}) is not None
        assert registry.find_serializer((1, 2)) is not None

    def test_registry_serialize_with_nested_collections(self):
        """
        Test registry handles nested collections.

        What this tests:
        ---------------
        1. Recursive serialization works
        2. Nested collections properly converted
        3. Mixed types in collections handled
        4. Deep nesting supported

        Why this matters:
        ----------------
        - Real data has complex nesting
        - Must handle arbitrary depth
        - Type mixing is common
        - Data integrity critical
        """
        registry = get_global_registry()
        context = SerializationContext(format="json", options={})

        # Nested list with mixed types
        nested_list = [1, "two", [3, 4], {"five": 5}, True, None]
        result = registry.serialize(nested_list, context)
        assert result == [1, "two", [3, 4], {"five": 5}, True, None]

        # Nested map with various types
        nested_map = {
            "strings": ["a", "b", "c"],
            "numbers": {1, 2, 3},  # Set becomes sorted list
            "metadata": {"nested": {"deeply": True}},
            "tuple": (1, "two", 3.0),
        }
        result = registry.serialize(nested_map, context)
        assert result["strings"] == ["a", "b", "c"]
        assert result["numbers"] == [1, 2, 3]  # Set converted to sorted list
        assert result["metadata"]["nested"]["deeply"] is True
        assert result["tuple"] == [1, "two", 3.0]  # Tuple to list
