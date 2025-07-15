"""
Custom pandas extension types for Cassandra data types.

This module provides extension types for Cassandra data types that don't have
proper pandas nullable dtype equivalents, ensuring:
- Full precision preservation
- Type safety
- Consistent NULL handling
- Seamless pandas integration
"""

# mypy: ignore-errors

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from decimal import Decimal
from ipaddress import IPv4Address, IPv6Address, ip_address
from typing import TYPE_CHECKING, Any
from uuid import UUID

import numpy as np
import pandas as pd
from cassandra.util import Duration
from pandas.api.extensions import ExtensionDtype, register_extension_dtype
from pandas.core.arrays import ExtensionArray as BaseExtensionArray
from pandas.core.dtypes.base import ExtensionDtype as BaseExtensionDtype

if TYPE_CHECKING:
    from pandas._typing import Dtype


# Base class for Cassandra extension arrays
class CassandraExtensionArray(BaseExtensionArray):
    """Base class for Cassandra extension arrays."""

    def __init__(
        self, values: Sequence[Any] | np.ndarray, dtype: ExtensionDtype, copy: bool = False
    ):
        """Initialize the array."""
        if isinstance(values, np.ndarray):
            if copy:
                values = values.copy()
            self._ndarray = values
        else:
            # Convert to object array
            arr = np.empty(len(values), dtype=object)
            for i, val in enumerate(values):
                if val is None or pd.isna(val):
                    arr[i] = pd.NA
                else:
                    arr[i] = self._validate_scalar(val)
            self._ndarray = arr
        self._dtype = dtype

    def _validate_scalar(self, value: Any) -> Any:
        """Validate and possibly convert a scalar value. Override in subclasses."""
        return value

    @classmethod
    def _from_sequence(
        cls, scalars: Sequence[Any], *, dtype: Dtype | None = None, copy: bool = False
    ) -> CassandraExtensionArray:
        """Construct a new array from a sequence of scalars."""
        if dtype is None:
            dtype = cls._dtype_class()
        return cls(scalars, dtype, copy=copy)

    @classmethod
    def _from_factorized(
        cls, values: np.ndarray, original: CassandraExtensionArray
    ) -> CassandraExtensionArray:
        """Reconstruct an array after factorization."""
        return cls(values, original.dtype, copy=False)

    @classmethod
    def _concat_same_type(
        cls, to_concat: Sequence[CassandraExtensionArray]
    ) -> CassandraExtensionArray:
        """Concatenate multiple arrays."""
        values = np.concatenate([arr._ndarray for arr in to_concat])
        return cls(values, to_concat[0].dtype, copy=False)

    @property
    def dtype(self) -> ExtensionDtype:
        """The dtype for this array."""
        return self._dtype

    @property
    def nbytes(self) -> int:
        """The number of bytes needed to store this object in memory."""
        # Rough estimate: 48 bytes per Python object
        return len(self) * 48

    def __len__(self) -> int:
        """Length of this array."""
        return len(self._ndarray)

    def __getitem__(self, item: int | slice | np.ndarray) -> Any:
        """Select a subset of self."""
        if isinstance(item, int):
            return self._ndarray[item]
        else:
            return type(self)(self._ndarray[item], self.dtype, copy=False)

    def __setitem__(self, key: int | slice | np.ndarray, value: Any) -> None:
        """Set one or more values inplace."""
        if pd.isna(value):
            value = pd.NA
        else:
            value = self._validate_scalar(value)
        self._ndarray[key] = value

    def isna(self) -> np.ndarray:
        """Boolean array indicating if each value is missing."""
        return pd.isna(self._ndarray)

    def take(
        self, indices: Sequence[int], *, allow_fill: bool = False, fill_value: Any = None
    ) -> CassandraExtensionArray:
        """Take elements from an array."""
        if allow_fill:
            if fill_value is None or pd.isna(fill_value):
                fill_value = pd.NA
            result = np.full(len(indices), fill_value, dtype=object)
            mask = (np.asarray(indices) >= 0) & (np.asarray(indices) < len(self))
            result[mask] = self._ndarray[np.asarray(indices)[mask]]
            return type(self)(result, self.dtype, copy=False)
        else:
            return type(self)(self._ndarray.take(indices), self.dtype, copy=False)

    def copy(self) -> CassandraExtensionArray:
        """Return a copy of the array."""
        return type(self)(self._ndarray.copy(), self.dtype, copy=False)

    def unique(self) -> CassandraExtensionArray:
        """Compute the unique values."""
        uniques = pd.unique(self._ndarray)
        return type(self)(uniques, self.dtype, copy=False)

    def __array__(self, dtype: np.dtype | None = None) -> np.ndarray:
        """Convert to numpy array."""
        return self._ndarray

    def __eq__(self, other: Any) -> np.ndarray:
        """Return element-wise equality."""
        if isinstance(other, CassandraExtensionArray | np.ndarray):
            return self._ndarray == other
        else:
            # Scalar comparison
            return self._ndarray == other

    def __ne__(self, other: Any) -> np.ndarray:
        """Return element-wise inequality."""
        return ~self.__eq__(other)

    def __lt__(self, other: Any) -> np.ndarray:
        """Return element-wise less than."""
        if isinstance(other, CassandraExtensionArray):
            return self._ndarray < other._ndarray
        else:
            return self._ndarray < other

    def __le__(self, other: Any) -> np.ndarray:
        """Return element-wise less than or equal."""
        if isinstance(other, CassandraExtensionArray):
            return self._ndarray <= other._ndarray
        else:
            return self._ndarray <= other

    def __gt__(self, other: Any) -> np.ndarray:
        """Return element-wise greater than."""
        if isinstance(other, CassandraExtensionArray):
            return self._ndarray > other._ndarray
        else:
            return self._ndarray > other

    def __ge__(self, other: Any) -> np.ndarray:
        """Return element-wise greater than or equal."""
        if isinstance(other, CassandraExtensionArray):
            return self._ndarray >= other._ndarray
        else:
            return self._ndarray >= other

    def _reduce(self, name: str, *, skipna: bool = True, **kwargs: Any) -> Any:
        """Return a scalar result of performing the reduction operation."""
        raise NotImplementedError(f"Reduction '{name}' not implemented for {type(self).__name__}")


# Date Extension (full Cassandra date range support)
@register_extension_dtype
class CassandraDateDtype(BaseExtensionDtype):
    """Extension dtype for Cassandra DATE type."""

    name = "cassandra_date"
    type = date
    kind = "O"
    _is_numeric = False

    @classmethod
    def construct_array_type(cls) -> type[CassandraDateArray]:
        """Return the array type associated with this dtype."""
        return CassandraDateArray


class CassandraDateArray(CassandraExtensionArray):
    """Array of Cassandra dates with support for missing values."""

    _dtype_class = CassandraDateDtype

    def _validate_scalar(self, value: Any) -> Any:
        """Validate and convert scalar to date."""
        if isinstance(value, date):
            return value
        elif hasattr(value, "date"):
            return value.date()
        else:
            raise TypeError(f"Cannot convert {type(value)} to date")

    def _values_for_argsort(self) -> np.ndarray:
        """Return values for sorting."""
        result = np.empty(len(self), dtype=np.int64)
        for i, val in enumerate(self._ndarray):
            if pd.isna(val):
                result[i] = -(2**63)  # NA sorts first
            else:
                result[i] = val.toordinal()
        return result

    def __ge__(self, other: Any) -> np.ndarray:
        """Return element-wise greater than or equal."""
        if isinstance(other, date) and not isinstance(other, pd.Timestamp):
            # Convert date to comparable format
            result = np.empty(len(self), dtype=bool)
            for i, val in enumerate(self._ndarray):
                if pd.isna(val):
                    result[i] = False
                else:
                    # Compare dates directly
                    if hasattr(val, "date"):
                        result[i] = val.date() >= other
                    else:
                        result[i] = val >= other
            return result
        else:
            return super().__ge__(other)

    def __gt__(self, other: Any) -> np.ndarray:
        """Return element-wise greater than."""
        if isinstance(other, date) and not isinstance(other, pd.Timestamp):
            # Convert date to comparable format
            result = np.empty(len(self), dtype=bool)
            for i, val in enumerate(self._ndarray):
                if pd.isna(val):
                    result[i] = False
                else:
                    # Compare dates directly
                    if hasattr(val, "date"):
                        result[i] = val.date() > other
                    else:
                        result[i] = val > other
            return result
        else:
            return super().__gt__(other)

    def __le__(self, other: Any) -> np.ndarray:
        """Return element-wise less than or equal."""
        if isinstance(other, date) and not isinstance(other, pd.Timestamp):
            # Convert date to comparable format
            result = np.empty(len(self), dtype=bool)
            for i, val in enumerate(self._ndarray):
                if pd.isna(val):
                    result[i] = False
                else:
                    # Compare dates directly
                    if hasattr(val, "date"):
                        result[i] = val.date() <= other
                    else:
                        result[i] = val <= other
            return result
        else:
            return super().__le__(other)

    def __lt__(self, other: Any) -> np.ndarray:
        """Return element-wise less than."""
        if isinstance(other, date) and not isinstance(other, pd.Timestamp):
            # Convert date to comparable format
            result = np.empty(len(self), dtype=bool)
            for i, val in enumerate(self._ndarray):
                if pd.isna(val):
                    result[i] = False
                else:
                    # Compare dates directly
                    if hasattr(val, "date"):
                        result[i] = val.date() < other
                    else:
                        result[i] = val < other
            return result
        else:
            return super().__lt__(other)

    def __eq__(self, other: Any) -> np.ndarray:
        """Return element-wise equality."""
        if isinstance(other, date) and not isinstance(other, pd.Timestamp):
            # Convert date to comparable format
            result = np.empty(len(self), dtype=bool)
            for i, val in enumerate(self._ndarray):
                if pd.isna(val):
                    result[i] = pd.isna(other)
                else:
                    # Compare dates directly
                    if hasattr(val, "date"):
                        result[i] = val.date() == other
                    else:
                        result[i] = val == other
            return result
        else:
            return super().__eq__(other)

    def to_datetime64(self, errors: str = "raise") -> pd.Series:
        """Convert to pandas datetime64[ns] dtype."""
        result = []
        for val in self._ndarray:
            if pd.isna(val):
                result.append(pd.NaT)
            else:
                try:
                    result.append(pd.Timestamp(val))
                except (pd.errors.OutOfBoundsDatetime, OverflowError) as err:
                    if errors == "raise":
                        raise OverflowError(
                            f"Date {val} is outside the range of pandas datetime64[ns]"
                        ) from err
                    elif errors == "coerce":
                        result.append(pd.NaT)
                    else:  # ignore
                        result.append(val)
        return pd.Series(result)

    def _reduce(self, name: str, *, skipna: bool = True, **kwargs: Any) -> Any:
        """Return a scalar result of performing the reduction operation."""
        if name in ["min", "max"]:
            mask = ~self.isna() if skipna else np.ones(len(self), dtype=bool)
            valid = self._ndarray[mask]
            if len(valid) == 0:
                return pd.NA
            return getattr(valid, name)()
        else:
            return super()._reduce(name, skipna=skipna, **kwargs)


# Decimal Extension (full precision preservation)
@register_extension_dtype
class CassandraDecimalDtype(BaseExtensionDtype):
    """Extension dtype for Cassandra DECIMAL type."""

    name = "cassandra_decimal"
    type = Decimal
    kind = "O"
    _is_numeric = True

    @classmethod
    def construct_array_type(cls) -> type[CassandraDecimalArray]:
        """Return the array type associated with this dtype."""
        return CassandraDecimalArray


class CassandraDecimalArray(CassandraExtensionArray):
    """Array of Decimal values with full precision preservation."""

    _dtype_class = CassandraDecimalDtype

    def _validate_scalar(self, value: Any) -> Any:
        """Validate and convert scalar to Decimal."""
        if isinstance(value, Decimal):
            return value
        else:
            return Decimal(str(value))

    def _values_for_argsort(self) -> np.ndarray:
        """Return values for sorting."""
        # Convert to float64 for sorting (may lose precision but preserves order)
        result = np.empty(len(self), dtype=np.float64)
        for i, val in enumerate(self._ndarray):
            if pd.isna(val):
                result[i] = np.nan
            else:
                result[i] = float(val)
        return result

    def to_float64(self) -> pd.Series:
        """Convert to float64 (may lose precision)."""
        result = []
        for val in self._ndarray:
            if pd.isna(val):
                result.append(np.nan)
            else:
                result.append(float(val))
        return pd.Series(result, dtype="float64")

    def _reduce(self, name: str, *, skipna: bool = True, **kwargs: Any) -> Any:
        """Return a scalar result of performing the reduction operation."""
        if name in ["sum", "min", "max", "mean"]:
            mask = ~self.isna() if skipna else np.ones(len(self), dtype=bool)
            valid = self._ndarray[mask]
            if len(valid) == 0:
                return pd.NA
            if name == "mean":
                return sum(valid) / len(valid)
            elif name == "sum":
                return sum(valid)
            else:
                return getattr(valid, name)()
        else:
            return super()._reduce(name, skipna=skipna, **kwargs)


# Varint Extension (unlimited precision integers)
@register_extension_dtype
class CassandraVarintDtype(BaseExtensionDtype):
    """Extension dtype for Cassandra VARINT type."""

    name = "cassandra_varint"
    type = int
    kind = "O"
    _is_numeric = True

    @classmethod
    def construct_array_type(cls) -> type[CassandraVarintArray]:
        """Return the array type associated with this dtype."""
        return CassandraVarintArray


class CassandraVarintArray(CassandraExtensionArray):
    """Array of unlimited precision integers."""

    _dtype_class = CassandraVarintDtype

    def _validate_scalar(self, value: Any) -> Any:
        """Validate and convert scalar to int."""
        return int(value)

    def _values_for_argsort(self) -> np.ndarray:
        """Return values for sorting."""
        # For sorting, we'll need to handle very large integers
        # This is a simplified approach that may not work for extremely large values
        result = []
        for val in self._ndarray:
            if pd.isna(val):
                result.append((0, -1))  # NA sorts first
            else:
                # Store sign and absolute value for comparison
                result.append((1 if val >= 0 else -1, abs(val)))

        # Convert to structured array for sorting
        dt = np.dtype([("sign", np.int8), ("value", object)])
        return np.array(result, dtype=dt)

    def to_int64(self, errors: str = "raise") -> pd.Series:
        """Convert to int64 (may overflow)."""
        result = []
        for val in self._ndarray:
            if pd.isna(val):
                result.append(pd.NA)
            else:
                if -(2**63) <= val <= 2**63 - 1:
                    result.append(val)
                else:
                    if errors == "raise":
                        raise OverflowError(f"Value {val} is outside int64 range")
                    elif errors == "coerce":
                        result.append(pd.NA)
                    else:  # ignore
                        result.append(val)
        return pd.Series(result, dtype="Int64")


# IP Address Extension
@register_extension_dtype
class CassandraInetDtype(BaseExtensionDtype):
    """Extension dtype for Cassandra INET type."""

    name = "cassandra_inet"
    type = (IPv4Address, IPv6Address)
    kind = "O"
    _is_numeric = False

    @classmethod
    def construct_array_type(cls) -> type[CassandraInetArray]:
        """Return the array type associated with this dtype."""
        return CassandraInetArray


class CassandraInetArray(CassandraExtensionArray):
    """Array of IP addresses."""

    _dtype_class = CassandraInetDtype

    def _validate_scalar(self, value: Any) -> Any:
        """Validate and convert scalar to IP address."""
        if isinstance(value, IPv4Address | IPv6Address):
            return value
        else:
            return ip_address(value)

    def _values_for_argsort(self) -> np.ndarray:
        """Return values for sorting."""
        # Convert IP addresses to integers for sorting
        result = np.empty(len(self), dtype=object)
        for i, val in enumerate(self._ndarray):
            if pd.isna(val):
                result[i] = -1  # NA sorts first
            else:
                result[i] = int(val)
        return result

    def to_string(self) -> pd.Series:
        """Convert to string representation."""
        result = []
        for val in self._ndarray:
            if pd.isna(val):
                result.append(pd.NA)
            else:
                result.append(str(val))
        return pd.Series(result, dtype="string")


# UUID Extension
@register_extension_dtype
class CassandraUUIDDtype(BaseExtensionDtype):
    """Extension dtype for Cassandra UUID type."""

    name = "cassandra_uuid"
    type = UUID
    kind = "O"
    _is_numeric = False

    @classmethod
    def construct_array_type(cls) -> type[CassandraUUIDArray]:
        """Return the array type associated with this dtype."""
        return CassandraUUIDArray


class CassandraUUIDArray(CassandraExtensionArray):
    """Array of UUIDs."""

    _dtype_class = CassandraUUIDDtype

    def _validate_scalar(self, value: Any) -> Any:
        """Validate and convert scalar to UUID."""
        if isinstance(value, UUID):
            return value
        else:
            return UUID(value)

    def _values_for_argsort(self) -> np.ndarray:
        """Return values for sorting."""
        # Convert UUIDs to integers for sorting
        result = np.empty(len(self), dtype=object)
        for i, val in enumerate(self._ndarray):
            if pd.isna(val):
                result[i] = -1  # NA sorts first
            else:
                result[i] = val.int
        return result

    def to_string(self) -> pd.Series:
        """Convert to string representation."""
        result = []
        for val in self._ndarray:
            if pd.isna(val):
                result.append(pd.NA)
            else:
                result.append(str(val))
        return pd.Series(result, dtype="string")


# TimeUUID Extension
@register_extension_dtype
class CassandraTimeUUIDDtype(BaseExtensionDtype):
    """Extension dtype for Cassandra TIMEUUID type."""

    name = "cassandra_timeuuid"
    type = UUID
    kind = "O"
    _is_numeric = False

    @classmethod
    def construct_array_type(cls) -> type[CassandraTimeUUIDArray]:
        """Return the array type associated with this dtype."""
        return CassandraTimeUUIDArray


class CassandraTimeUUIDArray(CassandraExtensionArray):
    """Array of TimeUUIDs."""

    _dtype_class = CassandraTimeUUIDDtype

    def _validate_scalar(self, value: Any) -> Any:
        """Validate and convert scalar to UUID."""
        if isinstance(value, UUID):
            # TimeUUIDs should be version 1 UUIDs
            if value.version != 1:
                raise ValueError(f"TimeUUID must be version 1, got version {value.version}")
            return value
        else:
            uuid_val = UUID(value)
            if uuid_val.version != 1:
                raise ValueError(f"TimeUUID must be version 1, got version {uuid_val.version}")
            return uuid_val

    def _values_for_argsort(self) -> np.ndarray:
        """Return values for sorting."""
        # TimeUUIDs should be sorted by timestamp, not by UUID value
        result = np.empty(len(self), dtype=np.int64)
        for i, val in enumerate(self._ndarray):
            if pd.isna(val):
                result[i] = -1  # NA sorts first
            else:
                # Extract timestamp from TimeUUID (first 60 bits)
                result[i] = (val.time - 0x01B21DD213814000) * 100 // 1_000_000_000
        return result

    def to_string(self) -> pd.Series:
        """Convert to string representation."""
        result = []
        for val in self._ndarray:
            if pd.isna(val):
                result.append(pd.NA)
            else:
                result.append(str(val))
        return pd.Series(result, dtype="string")

    def to_timestamp(self) -> pd.Series:
        """Extract timestamp from TimeUUIDs."""
        result = []
        for val in self._ndarray:
            if pd.isna(val):
                result.append(pd.NaT)
            else:
                # Convert UUID timestamp to Unix timestamp
                timestamp = (val.time - 0x01B21DD213814000) * 100 / 1_000_000_000
                result.append(pd.Timestamp(timestamp, unit="s"))
        return pd.Series(result, dtype="datetime64[ns, UTC]")


# Duration Extension
@register_extension_dtype
class CassandraDurationDtype(BaseExtensionDtype):
    """Extension dtype for Cassandra DURATION type."""

    name = "cassandra_duration"
    type = Duration
    kind = "O"
    _is_numeric = False

    @classmethod
    def construct_array_type(cls) -> type[CassandraDurationArray]:
        """Return the array type associated with this dtype."""
        return CassandraDurationArray


class CassandraDurationArray(CassandraExtensionArray):
    """Array of Cassandra Duration values."""

    _dtype_class = CassandraDurationDtype

    def _validate_scalar(self, value: Any) -> Any:
        """Validate and convert scalar to Duration."""
        if isinstance(value, Duration):
            return value
        else:
            raise TypeError(f"Cannot convert {type(value)} to Duration")

    def _values_for_argsort(self) -> np.ndarray:
        """Return values for sorting."""
        # Convert to total nanoseconds for sorting
        result = np.empty(len(self), dtype=np.int64)
        for i, val in enumerate(self._ndarray):
            if pd.isna(val):
                result[i] = -(2**63)  # NA sorts first
            else:
                # Approximate total nanoseconds (months and days are approximate)
                total_ns = val.nanoseconds
                total_ns += val.days * 24 * 60 * 60 * 1_000_000_000
                total_ns += val.months * 30 * 24 * 60 * 60 * 1_000_000_000
                result[i] = total_ns
        return result

    def to_components(self) -> pd.DataFrame:
        """Convert to DataFrame with component columns."""
        months, days, nanoseconds = [], [], []
        for val in self._ndarray:
            if pd.isna(val):
                months.append(pd.NA)
                days.append(pd.NA)
                nanoseconds.append(pd.NA)
            else:
                months.append(val.months)
                days.append(val.days)
                nanoseconds.append(val.nanoseconds)

        return pd.DataFrame(
            {
                "months": pd.Series(months, dtype="Int32"),
                "days": pd.Series(days, dtype="Int32"),
                "nanoseconds": pd.Series(nanoseconds, dtype="Int64"),
            }
        )
