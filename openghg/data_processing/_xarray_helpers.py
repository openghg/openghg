"""Helper functions related to xarray."""

from typing import cast

import numpy as np
import xarray as xr
from openghg.types import XrDataLikeMatch


class XrTypesHack:
    """Wrap a numpy ufunc to operate on xarray DataArrays and Datasets with correct return types.

    There is a problem with numpy's type hints that causes mypy to think that numpy functions
    applied to xarray DataArrays and Datasets return numpy arrays: https://github.com/pydata/xarray/issues/8388.

    Further, using a class to do this wrapping seems to be necessary. Possibly related to some
    old mypy issue: https://github.com/python/mypy/issues/1551
    """

    def __init__(self, ufunc: np.ufunc) -> None:
        self.ufunc = ufunc

    def __call__(self, data: XrDataLikeMatch) -> XrDataLikeMatch:
        result = self.ufunc(data)
        if isinstance(data, xr.DataArray):
            return cast(xr.DataArray, result)
        if isinstance(data, xr.Dataset):
            return cast(xr.Dataset, result)


xr_sqrt = XrTypesHack(np.sqrt)
