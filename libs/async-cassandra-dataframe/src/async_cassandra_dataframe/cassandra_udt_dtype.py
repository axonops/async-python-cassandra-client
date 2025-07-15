"""
Custom pandas extension type for Cassandra User Defined Types (UDTs).

This preserves the full type information and structure of UDTs without
converting to dicts or strings, maintaining type safety and precision.
"""

# mypy: ignore-errors

from __future__ import annotations

from collections.abc import Sequence

import numpy as np
import pandas as pd
from pandas.api.extensions import ExtensionArray, ExtensionDtype


class CassandraUDTDtype(ExtensionDtype):
    """Custom dtype for Cassandra UDTs."""

    name = "cassandra_udt"
    type = object
    kind = "O"
    _is_numeric_dtype = False

    def __init__(self, keyspace: str = None, udt_name: str = None):
        """
        Initialize UDT dtype.

        Args:
            keyspace: Keyspace containing the UDT
            udt_name: Name of the UDT
        """
        self.keyspace = keyspace
        self.udt_name = udt_name

    @classmethod
    def construct_from_string(cls, string: str) -> CassandraUDTDtype:
        """Construct from string representation."""
        if string == cls.name:
            return cls()
        # Support format: cassandra_udt[keyspace.typename]
        if string.startswith("cassandra_udt[") and string.endswith("]"):
            content = string[14:-1]
            if "." in content:
                keyspace, udt_name = content.split(".", 1)
                return cls(keyspace=keyspace, udt_name=udt_name)
        return cls()

    def __str__(self) -> str:
        """String representation."""
        if self.keyspace and self.udt_name:
            return f"cassandra_udt[{self.keyspace}.{self.udt_name}]"
        return self.name

    def __repr__(self) -> str:
        """String representation."""
        return str(self)

    @classmethod
    def construct_array_type(cls) -> type[CassandraUDTArray]:
        """Return the array type associated with this dtype."""
        return CassandraUDTArray


class CassandraUDTArray(ExtensionArray):
    """Array of Cassandra UDT values."""

    def __init__(self, values: Sequence, dtype: CassandraUDTDtype = None):
        """
        Initialize UDT array.

        Args:
            values: Sequence of UDT values (namedtuples or None)
            dtype: CassandraUDTDtype instance
        """
        self._values = np.asarray(values, dtype=object)
        self._dtype = dtype or CassandraUDTDtype()

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
        if isinstance(key, int):
            return self._values[key]
        return type(self)(self._values[key], dtype=self._dtype)

    def __setitem__(self, key, value):
        """Set item by index."""
        self._values[key] = value

    def __len__(self) -> int:
        """Length of array."""
        return len(self._values)

    def __eq__(self, other):
        """Equality comparison."""
        if isinstance(other, CassandraUDTArray):
            return np.array_equal(self._values, other._values)
        return NotImplemented

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
        return pd.isna(self._values)

    def take(self, indices, allow_fill=False, fill_value=None):
        """Take elements from array."""
        if allow_fill:
            mask = indices == -1
            if mask.any():
                if fill_value is None:
                    fill_value = self.dtype.na_value
                result = np.empty(len(indices), dtype=object)
                result[mask] = fill_value
                result[~mask] = self._values[indices[~mask]]
                return type(self)(result, dtype=self._dtype)

        return type(self)(self._values[indices], dtype=self._dtype)

    def copy(self):
        """Return a copy of the array."""
        return type(self)(self._values.copy(), dtype=self._dtype)

    def _concat_same_type(cls, to_concat):
        """Concatenate multiple arrays."""
        values = np.concatenate([arr._values for arr in to_concat])
        return cls(values, dtype=to_concat[0].dtype)

    def to_dict(self) -> pd.Series:
        """
        Convert UDT values to dictionaries.

        Returns:
            Series of dictionaries
        """

        def convert_value(val):
            if val is None or pd.isna(val):
                return None
            if hasattr(val, "_asdict"):
                # Recursively convert nested UDTs
                d = val._asdict()
                for k, v in d.items():
                    if hasattr(v, "_asdict"):
                        d[k] = convert_value(v)
                return d
            return val

        return pd.Series([convert_value(val) for val in self._values])

    def to_string(self) -> pd.Series:
        """
        Convert to string representation.

        Returns:
            Series of strings
        """

        def format_value(val):
            if val is None or pd.isna(val):
                return None
            return str(val)

        return pd.Series([format_value(val) for val in self._values])

    @property
    def na_value(self):
        """The missing value for this dtype."""
        return None
