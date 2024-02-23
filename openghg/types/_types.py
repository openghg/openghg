import numpy as np
import xarray as xr
from pathlib import Path
from typing import DefaultDict, Dict, List, Tuple, Union, Optional, TypeVar, NamedTuple

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


class TimePeriod(NamedTuple):
    value: Union[int, float, None] = None
    unit: Optional[str] = None
