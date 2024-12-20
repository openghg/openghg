from __future__ import annotations

import numpy as np
import xarray as xr
from pathlib import Path
from typing import (
    Any,
    Union,
    Optional,
    TypeVar,
    NamedTuple,
    Protocol,
    runtime_checkable,
)
from collections import defaultdict

pathType = Union[str, Path]
optionalPathType = Optional[pathType]
multiPathType = Union[str, Path, tuple, list]
resultsType = defaultdict[str, dict]

# Create types for ndarray or xr.DataArray inputs
# Using TypeVar means - whichever type is passed in will be the one which is returned.
ArrayLike = Union[np.ndarray, xr.DataArray]
ArrayLikeMatch = TypeVar("ArrayLikeMatch", np.ndarray, xr.DataArray)
XrDataLike = Union[xr.DataArray, xr.Dataset]
XrDataLikeMatch = TypeVar("XrDataLikeMatch", xr.DataArray, xr.Dataset)


class TimePeriod(NamedTuple):
    value: int | float | None = None
    unit: str | None = None


class MetadataAndData(NamedTuple):
    """A very simple implementation of the `HasMetadataAndData` protocol."""

    metadata: dict
    data: xr.Dataset


@runtime_checkable
class HasMetadataAndData(Protocol):
    """Protocol that includes _BaseData and its subclasses, as well as
    other containers of data and metadata.
    """

    metadata: dict
    data: xr.Dataset

    def __init__(self, metadata: dict, data: xr.Dataset) -> None: ...


CT = TypeVar("CT", bound="Comparable")


class Comparable(Protocol):
    """Type for checking if objects can be compared.

    Based on https://github.com/python/typing/issues/59
    """

    def __eq__(self, other: Any) -> bool: ...

    def __lt__(self: CT, other: CT) -> bool: ...

    def __gt__(self: CT, other: CT) -> bool:
        return (not self < other) and self != other

    def __le__(self: CT, other: CT) -> bool:
        return self < other or self == other

    def __ge__(self: CT, other: CT) -> bool:
        return not self < other
