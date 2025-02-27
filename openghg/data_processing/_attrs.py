"""Functions to manipulating the attributes and variable names of xr.Datasets.

Often the attributes and variable names of a dataset need to be changed
after some computation.

For example, after resampling a mole fraction timeseries called `ch4`
by taking its standard deviation, we would like the resulting
data variable to be called `ch4_variability`, and we would like the
"long_name" attribute to have "variability" added to it.

The `update_attrs` and `rename` functions both take a xr.Dataset
and a variable number of arguments specifying how the attributes
or data variable names should be modified.

There are two cases:
1. if a function is passed, that function is applied to the values of attributes
   or data variable names
2. if a tuple containing a function and a list of strings is passed, then the function
   is applied only to values whose keys are in the list (or only to data variables that
   are in the list)

Examples:
---------
>>> rename(ds, lambda x: x.lower())

would make all data variable names in `ds` lower case.

>>> update_attrs(ds, (lambda x: x + "_variability", ["long_name"]))

would add "_variability" to the end of the value of `ds.attrs["long_name"]`,
and leave the other attributes unchanged.

Multiple functions or tuples can be passed. The functions are applied
in the same order they are passed to `rename` or `update_attrs`.

>>> rename(ds, lambda x: x.lower(), (lambda x: x.upper(), ["important_dv"]))

will make all data variable names lowercase, then will make only "important_dv" uppercase.

If there are multiple functions passed to `rename`, then the keys or data variable names passed
should refer to the original dataset variable names (rather than the names with the previous updates
applied).

So, if `Important_dv` is in `ds`, we should use

>>> rename(ds, lambda x: x.lower(), (lambda x: x.upper(), ["Important_dv"]))

or use two calls to `rename`:

>>> ds = rename(ds, lambda x: x.lower())
>>> rename(ds, (lambda x: x.upper(), ["important_dv"]))

"""

from collections.abc import Callable, Iterable
from functools import partial, reduce
from typing import Any

import xarray as xr

from openghg.types import XrDataLikeMatch


def map_dict(d: dict, func: Callable, keys: list[str] | None = None) -> dict:
    """Apply a function to the values of a dict, optionally filtering by a collection of keys.

    The input dict is not modified by this function; a modified copy is returned.

    Args:
        d: input dict
        func: function applied to the values of the dictionary
        keys: optional list of keys that the function should be applied to;
            values with keys not in this list will be passed through unchanged.

    Returns:
        dict with function applied to values of input dict
    """

    def f(k, v):  # type: ignore
        if keys is None:
            return func(v)
        return func(v) if k in keys else v

    return {k: f(k, v) for k, v in d.items()}


def map_dict_multi(d: dict, funcs: Iterable[Callable | tuple[Callable, list[str]]]) -> dict:
    """Apply a sequence of functions to the values of a dict, optionally filtering by collection of keys.

    The input dict is not modified by this function; a modified copy is returned.

    Args:
        d: input dict
        funcs: functions applied to the values of the dictionary. If a tuple
            containing a function and a list of strings is passed, then the function
            is only applied to values whose keys are in that list; other values are
            unmodified.

    Returns:
        dict with functions applied to values of input dict
    """
    tups = (func if isinstance(func, tuple) else (func, None) for func in funcs)
    maps = (partial(map_dict, func=tup[0], keys=tup[1]) for tup in tups)
    return reduce(lambda x, f: f(x), maps, d)


def update_attrs(
    data: XrDataLikeMatch,
    *funcs: Callable | tuple[Callable, list[str]],
    global_attrs: dict | None = None,
    **kwargs: Any,
) -> XrDataLikeMatch:
    """Update and add to attributes of DataArray or Dataset.

    For example, if `ds` is a Dataset, then

    >>> update_attrs(ds,
    >>>              lambda x: x.lower()
    >>>              global_attrs={"processed_by": "OpenGHG"},
    >>>              updated=datetime.date.today())

    will:

    1. make all existing attributes (on the dataset and all its data variables) lowercase
    2. will add the global attribute `processed_by="OpenGHG"`
    3. added the attribute `updated=datetime.date.today()` globally and on all data variables

    Args:
        data: xr.DataArray or xr.Dataset whose attributes are to be modified
        funcs: optional tranformation(s) to be applied to attribute values. If a tuple
            is passed, the list of keys in the second argument is used to select the
            keys that the function applies to.
        global_attrs: optional global attributes to add; for a Dataset, these are
            added to the Dataset attributes; for a DataArray, they are combined with
            **kwargs.
        **kwargs: attributes to add to DataArray, or Dataset and all data variables.

    Returns:
        (shallow copy of) DataArray or Dataset with updated attributes

    """
    global_attrs = global_attrs or {}

    if not funcs:
        return data.assign_attrs(**kwargs, **global_attrs)

    update_dict = map_dict_multi(data.attrs, funcs)

    data = data.assign_attrs(**update_dict, **kwargs, **global_attrs)

    if isinstance(data, xr.DataArray):
        return data

    # otherwise, data is a Dataset, and we want to update data variable
    # attributes, but not add global attributes
    for dv in data.data_vars:
        data[dv] = update_attrs(data[dv], *funcs, **kwargs)

    return data


def _make_rename_dict(
    to_rename: xr.Dataset,
    *funcs: Callable | tuple[Callable, list[str]],
) -> dict[str, str]:
    """Make dictionary to rename data variables based on update specification.

    Args:
        to_rename: Dataset whose variables are to be renamed
        funcs: function or tuple (function, [dv1, dv2, ...]), where dv1, dv2, ... are
            the data variable names that the function should be applied to

    Returns:
        dict mapping old names to new names
    """
    to_update = {str(dv): str(dv) for dv in to_rename.data_vars}

    return map_dict_multi(to_update, funcs) if funcs else to_update


def rename(data: xr.Dataset, *funcs: Callable | tuple[Callable, list[str]]) -> xr.Dataset:
    """Rename data variables based on specification.

    Args:
        data: xr.Dataset whose data variables are to be renamed
        *funcs: functions or tuples containing a functions and a list of strings.
            The functions are applied to the data variable names. If a tuple is
            passed, then the function from the tuple is only applied to data variables
            that are in the list from the tuple.

    Returns:
        xr.Dataset with renamed data variables
    """
    rename_dict = _make_rename_dict(data, *funcs)
    return data.rename(rename_dict)
