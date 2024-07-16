import numpy as np
import pandas as pd
import xarray as xr
from dataclasses import dataclass
from pathlib import Path
from typing import cast, DefaultDict, Dict, Iterator, List, Tuple, Union, Optional, TypeVar

pathType = Union[str, Path]
optionalPathType = Optional[pathType]
multiPathType = Union[str, Path, Tuple, List]
resultsType = DefaultDict[str, Dict]

# Create types for ndarray or xr.DataArray inputs
# Using TypeVar means - whichever type is passed in will be the one which is returned.
ArrayLike = Union[np.ndarray, xr.DataArray]
ArrayLikeMatch = TypeVar("ArrayLikeMatch", np.ndarray, xr.DataArray)
XrDataLike = Union[xr.DataArray, xr.Dataset]
XrDataLikeMatch = TypeVar("XrDataLikeMatch", xr.DataArray, xr.Dataset)


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
