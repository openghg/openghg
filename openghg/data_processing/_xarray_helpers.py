"""
Helper functions related to xarray.
"""

from collections.abc import Callable
from typing import Any, cast, Concatenate, Literal, ParamSpec, Protocol, runtime_checkable

import numpy as np
import xarray as xr
from openghg.types import XrDataLikeMatch


P = ParamSpec("P")
XrOpType = Callable[Concatenate[XrDataLikeMatch, P], XrDataLikeMatch]


@runtime_checkable
class _SupportsArrayUFunc(Protocol):
    """Protocol for objects that can be used with numpy ufuncs.

    This isn't part of Numpy's public typing API yet.
    """

    def __array_ufunc__(
        self,
        ufunc: np.ufunc,
        method: Literal["__call__", "reduce", "reduceat", "accumulate", "outer", "inner"],
        *inputs: Any,
        **kwargs: Any,
    ) -> Any: ...


UFuncType = Callable[Concatenate[_SupportsArrayUFunc, P], Any]


def xr_types(func: UFuncType) -> XrOpType:
    """Wrap a function that operators on xarray DataArrays and Datasets to get correct return types.

    There is a problem with numpy's type hints that causes mypy to thing that numpy functions
    applied to xarray DataArrays and Datasets return numpy arrays: https://github.com/pydata/xarray/issues/8388.

    This wrapper takes function, applies to a DataArray or Dataset, and then casts the result to the correct type.

    Note: we do not use `functools.wraps` here, because that doesn't like when type hints are changed, even though
    we have narrowed the possible inputs to the numpy ufunc, and checked that the output is the expected type.

    Args:
        func: a function that maps xr.DataArray or xr.Dataset to the same type (but may not have the right type hints).
            This mainly applies to numpy functions applied to xarray objects.

    Returns:
        function with correct type hints, and which will throw a ValueError if its
        output type does not match the input type.
    """

    def xr_typed_func(data: XrDataLikeMatch, *args: P.args, **kwargs: P.kwargs) -> XrDataLikeMatch:
        result = func(data, *args, **kwargs)
        if isinstance(data, xr.DataArray) and isinstance(result, xr.DataArray):
            return cast(xr.DataArray, result)
        if isinstance(data, xr.Dataset) and isinstance(result, xr.Dataset):
            return cast(xr.Dataset, result)
        raise ValueError(
            f"Function {func.__name__} did not return same type as input: {type(data)} != {type(result)}."
        )

    return xr_typed_func


xr_sqrt = xr_types(np.sqrt)
