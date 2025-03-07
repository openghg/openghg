"""Functions to help avoid xarray issues."""

import numpy as np
import xarray as xr
from numpy.typing import DTypeLike
from numpy._typing import _ArrayLikeComplex_co


def xr_linspace_with_np_output(
    start: _ArrayLikeComplex_co | xr.DataArray,
    stop: _ArrayLikeComplex_co | xr.DataArray,
    num: int = 50,
    endpoint: bool = True,
    retstep: bool = False,
    dtype: DTypeLike | None = None,
    axis: int = 0,
) -> np.ndarray:
    """Wrapper around np.linspace to convert inputs to numpy"""
    if isinstance(start, xr.DataArray):
        start = start.values

    if isinstance(stop, xr.DataArray):
        stop = stop.values

    return np.linspace(start, stop, num, endpoint, retstep, dtype, axis)
