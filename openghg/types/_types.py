from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import (
    Any,
    cast,
    DefaultDict,
    Dict,
    Optional,
    Union,
    Literal,
    Tuple,
    TypeVar,
    NamedTuple,
    Protocol,
    runtime_checkable,
)
from collections import defaultdict

import numpy as np
import pandas as pd
import xarray as xr


pathType = Union[str, Path]
multiPathType = Union[str, Path, tuple, list]
resultsType = defaultdict[str, dict]

# Create types for ndarray or xr.DataArray inputs
# Using TypeVar means - whichever type is passed in will be the one which is returned.
ArrayLike = Union[np.ndarray, xr.DataArray]
ArrayLikeMatch = TypeVar("ArrayLikeMatch", np.ndarray, xr.DataArray)
XrDataLike = Union[xr.DataArray, xr.Dataset]
XrDataLikeMatch = TypeVar("XrDataLikeMatch", xr.DataArray, xr.Dataset)

# Defining literals for different (generally internal) settings
ReindexMethod = Literal["nearest", "pad", "ffill", "backfill", "bfill"]


@dataclass(frozen=True, eq=True)
class TimePeriod:
    value: Union[int, float, None] = None
    unit: Optional[str] = None

    def to_date_offset(self) -> pd.DateOffset:
        """Convert TimePeriod object to pd.DateOffset

        Returns:
            pandas DateOffset of `self.value` many `self.unit`s.

        TODO: what should default behavior be? Empty DateOffset is 1 day.
        """
        if self.value is None or self.unit is None:
            return pd.DateOffset()
        else:
            offset = pd.tseries.frequencies.to_offset(f"{self.value}{self.unit}")
            return cast(pd.DateOffset, offset)

    def __iter__(self) -> Iterator:
        """Unpack TimePeriod objects via `value, unit = TimePeriod(...)`"""
        return iter((self.value, self.unit))


@dataclass
class MetadataAndData:
    """A very simple implementation of the `HasMetadataAndData` protocol."""

    metadata: dict
    data: xr.Dataset


def convert_to_list_of_metadata_and_data(
    parser_output: dict | Iterable[HasMetadataAndData],
) -> list[MetadataAndData]:
    """Convert from old nested dict format to list of MetadataAndData."""
    if isinstance(parser_output, dict):
        return [MetadataAndData(v["metadata"], v["data"]) for v in parser_output.values()]
    else:
        parser_output = list(parser_output)

        if all(isinstance(x, HasMetadataAndData) for x in parser_output):
            return [MetadataAndData(v.metadata, v.data) for v in parser_output]

    raise ValueError(
        "`parser_output` must be dict or Iterable of objects with .metadata and .data attributes."
    )


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
