"""
Custom pandas extension type for Cassandra writetime values.

Writetime in Cassandra is stored as microseconds since epoch and represents
when a value was written. This custom dtype preserves that semantic meaning
and provides utilities for working with writetimes.
"""

# mypy: ignore-errors

from __future__ import annotations

from collections.abc import Sequence

import numpy as np
import pandas as pd
from pandas.api.extensions import ExtensionArray, ExtensionDtype


class CassandraWritetimeDtype(ExtensionDtype):
    """Custom dtype for Cassandra writetime values."""

    name = "cassandra_writetime"
    type = np.int64
    kind = "i"
    _is_numeric_dtype = True

    @classmethod
    def construct_from_string(cls, string: str) -> CassandraWritetimeDtype:
        """Construct from string representation."""
        if string == cls.name:
            return cls()
        raise TypeError(f"Cannot construct a '{cls.name}' from '{string}'")

    def __str__(self) -> str:
        """String representation."""
        return self.name

    def __repr__(self) -> str:
        """String representation."""
        return f"{self.__class__.__name__}()"

    @classmethod
    def construct_array_type(cls) -> type[CassandraWritetimeArray]:
        """Return the array type associated with this dtype."""
        return CassandraWritetimeArray


class CassandraWritetimeArray(ExtensionArray):
    """Array of Cassandra writetime values (microseconds since epoch)."""

    def __init__(self, values: Sequence, dtype: CassandraWritetimeDtype = None):
        """
        Initialize writetime array.

        Args:
            values: Sequence of writetime values (microseconds since epoch) or None
            dtype: CassandraWritetimeDtype instance
        """
        # Convert to int64 array, preserving None as pd.NA
        if isinstance(values, list | tuple):
            arr = np.empty(len(values), dtype=np.int64)
            mask = np.zeros(len(values), dtype=bool)
            for i, val in enumerate(values):
                if val is None or pd.isna(val):
                    mask[i] = True
                    arr[i] = 0  # Placeholder value
                else:
                    arr[i] = int(val)
            self._values = pd.arrays.IntegerArray(arr, mask)
        else:
            # Assume it's already an appropriate array
            self._values = pd.array(values, dtype="Int64")

        self._dtype = dtype or CassandraWritetimeDtype()

    @classmethod
    def _from_sequence(cls, scalars, dtype=None, copy=False):
        """Construct from sequence of scalars."""
        return cls(scalars, dtype=dtype)

    @classmethod
    def _from_factorized(cls, values, original):
        """Reconstruct from factorized values."""
        return cls(values, dtype=original.dtype)

    def __getitem__(self, key):
        """Get item by index."""
        result = self._values[key]
        if isinstance(key, int):
            return result
        return type(self)(result, dtype=self._dtype)

    def __setitem__(self, key, value):
        """Set item by index."""
        self._values[key] = value

    def __len__(self) -> int:
        """Length of array."""
        return len(self._values)

    def __eq__(self, other):
        """Equality comparison."""
        if isinstance(other, CassandraWritetimeArray):
            return self._values == other._values
        return self._values == self._convert_comparison_value(other)

    def __ne__(self, other):
        """Not equal comparison."""
        if isinstance(other, CassandraWritetimeArray):
            return self._values != other._values
        return self._values != self._convert_comparison_value(other)

    def __lt__(self, other):
        """Less than comparison."""
        if isinstance(other, CassandraWritetimeArray):
            return self._values < other._values
        return self._values < self._convert_comparison_value(other)

    def __le__(self, other):
        """Less than or equal comparison."""
        if isinstance(other, CassandraWritetimeArray):
            return self._values <= other._values
        return self._values <= self._convert_comparison_value(other)

    def __gt__(self, other):
        """Greater than comparison."""
        if isinstance(other, CassandraWritetimeArray):
            return self._values > other._values
        return self._values > self._convert_comparison_value(other)

    def __ge__(self, other):
        """Greater than or equal comparison."""
        if isinstance(other, CassandraWritetimeArray):
            return self._values >= other._values
        return self._values >= self._convert_comparison_value(other)

    def _convert_comparison_value(self, other):
        """Convert comparison value to microseconds since epoch."""
        if isinstance(other, pd.Timestamp | pd.DatetimeIndex):
            # Convert to microseconds since epoch
            return int(other.value / 1000)  # pandas stores nanoseconds
        elif hasattr(other, "timestamp"):
            # datetime.datetime
            return int(other.timestamp() * 1_000_000)
        else:
            # Assume it's already microseconds or a numeric value
            return other

    @property
    def dtype(self):
        """The dtype of this array."""
        return self._dtype

    @property
    def nbytes(self) -> int:
        """Number of bytes consumed by the array."""
        return self._values.nbytes

    def isna(self):
        """Return boolean array indicating missing values."""
        return self._values.isna()

    def take(self, indices, allow_fill=False, fill_value=None):
        """Take elements from array."""
        result = self._values.take(indices, allow_fill=allow_fill, fill_value=fill_value)
        return type(self)(result, dtype=self._dtype)

    def copy(self):
        """Return a copy of the array."""
        return type(self)(self._values.copy(), dtype=self._dtype)

    @classmethod
    def _concat_same_type(cls, to_concat):
        """Concatenate multiple arrays."""
        if len(to_concat) == 0:
            return cls([], dtype=CassandraWritetimeDtype())

        # Extract all underlying IntegerArrays
        int_arrays = [arr._values for arr in to_concat]

        # Use pandas concat on the IntegerArrays
        concatenated = pd.concat([pd.Series(arr) for arr in int_arrays]).array

        return cls(concatenated, dtype=to_concat[0].dtype)

    def to_timestamp(self) -> pd.Series:
        """
        Convert writetime values to pandas timestamps.

        Returns:
            Series of timestamps with timezone UTC
        """
        # Convert microseconds to nanoseconds
        nanos = self._values * 1000
        # Create timestamps
        return pd.to_datetime(nanos, unit="ns", utc=True)

    def age(self, reference_time=None) -> pd.Series:
        """
        Calculate age of values from writetime.

        Args:
            reference_time: Reference time (default: now)

        Returns:
            Series of timedeltas representing age
        """
        if reference_time is None:
            reference_time = pd.Timestamp.now("UTC")
        elif not isinstance(reference_time, pd.Timestamp):
            reference_time = pd.Timestamp(reference_time, tz="UTC")

        timestamps = self.to_timestamp()
        return reference_time - timestamps

    def to_microseconds(self) -> pd.Series:
        """
        Get raw microseconds values.

        Returns:
            Series of int64 microseconds since epoch
        """
        return pd.Series(self._values)

    @property
    def na_value(self):
        """The missing value for this dtype."""
        return pd.NA
