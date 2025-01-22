"""Functions to manipulating the attributes and variable names of xr.Datasets.

Often the attributes and variable names of a dataset need to be changed
after some computation.

For example, after resampling a mole fraction timeseries called `ch4`
by taking its standard deviation, we would like the resulting
data variable to be called `ch4_variability`, and we would like the
"long_name" attribute to have "variability" added to it.

These functions are mainly intended to be used internally, as part of
other data processing functions. There is a small "domain specific language"
that allows transformations to be specified using strings, functions, and
tuples.

For instance, when the list

>>> updaters = ["lower", ("replace", "ch4", "mf")]

is passed to the `rename` function:

>>> ds = rename(ds, updaters)

the data variables of `ds` are renamed by first making them lower case,
then by replacing "ch4" with "mf".

Alternatively, we could use

>>> updaters = [lambda x: x.lower(), lambda x: x.replace("ch4", "mf")]

This module contains a collection of functions that can be applied to
update names or attribute values:
- `add_prefix`
- `add_suffix`

These can be accessed by name from a registry, so that they can be
used as above.

These functions have a positional only argument `value`; the other
arguments to the functions are bound by a class `UpdateSpec` to suit
a particular application.

For example:
>>> add_variability_suffix = UpdateSpec(add_suffix, "variability")
>>> assert add_variability_suffix("ch4") == "ch4_variability"

This can also be achieved by passing a tuple to `UpdateSpec.parse`:
>>> updater_tuple = ("add_suffix", "variabilty")
>>> add_variability_suffix = UpdateSpec.parse(updater_tuple)

Or, we could use
>>> add_variability_suffix = UpdateSpec.parse(lambda x: x + "_variability")

A valid argument for `UpdateSpec` parse is either:
1. a string, which is interpreted as the name for a function; the registry
   will be searched for this function name, and if it isn't found, the name
   is assumed to be a Python string method (like "lower" or "replace")
2. a function, which will be applied to dictionary values or used for renaming
3. a tuple whose first argument is a string or function; the first argument is
   parsed as in cases 1 and 2, and the remaining arguments will be applied to
   the function.

When we passed `("add_suffix", "variability")` to `UpdateSpec.parse`, this
was an example of case 3.
"""

from collections.abc import Iterable
from functools import singledispatch
from inspect import get_annotations, signature
from typing import Any, TypeVar, cast
from typing_extensions import Self
from collections.abc import Callable, Sequence

import xarray as xr

from openghg.util import Registry


registry = Registry()


@registry.register
def add_prefix(value: str, /, prefix: str, sep: str = "_") -> str:
    """Add a prefix to a string.

    Args:
        value: the string to add a prefix to
        prefix: the prefix to add
        sep: the separator between prefix and value

    Returns:
        string with prefix added
    """
    return prefix + sep + value


@registry.register
def add_suffix(value: str, /, suffix: str, sep: str = "_") -> str:
    """Add a suffix to a string.

    Args:
        value: the string to add a suffix to
        suffix: the suffix to add
        sep: the separator between suffix and value

    Returns:
        string with suffix added
    """
    return value + sep + suffix


def str_method(value: str, /, method: str, *args: Any, **kwargs: Any) -> str:
    """Call a string method by name on a given string.

    Args:
        value: string to apply the method to
        method: name of the method (must be a Python string method).
            For instance: "upper", "strip", or "replace".
        *args: positional arguments for the string method.
        **kwargs: keyword arguments for the string method.

    Returns:
        string modified by specified method

    Raises:
        ValueError: if `method` is not a valid Python string method, or does not
            return a string.
    """
    # try getting bound method from value
    try:
        func = getattr(value, method)
    except AttributeError:
        raise ValueError(f"Method {method} is not a valid Python string method.")

    result = func(*args, **kwargs)

    if not isinstance(result, str):
        raise ValueError(f"String method {method} does not return a string.")

    return cast(str, func(*args, **kwargs))


class UpdateSpec:
    """Holds a function and arguments, so they can be applied later to modify a string.

    This is similar to `functools.partial`, except for the order in which arguments are applied during
    the call.

    The functions passed to `UpdateSpec` are assumed to have a positional-only string argument,
    and any args passed are assumed to come after this argument.

    Once created, an `UpdateSpec` object can be used like a function to modify strings.

    An `UpdateSpec` has an additional `keys` attribute, which can be used by functions that accept
    `UpdateSpec` objects to restrict the values that the `UpdateSpec` is applied to.
    """

    def __init__(self, func: Callable, *args: Any, keys: Sequence[str] | None = None, **kwargs: Any) -> None:
        """Create UpdateSpec object.

        Args:
            func: function that will be used to modify strings. Must have a single positional-only
                argument that accepts strings, and must return a string.
            *args: positional arguments (after the first argument) that are needed by `func`
            keys: optional list of strings; when supplied and the `UpdateSpec` is called on a dictionary
                only values with these keys will be modified.
            **kwargs: keyword arguments that are needed by `func`
        """
        self.func = func

        self.args = args
        self.kwargs = kwargs

        self.keys = keys

        self._validate()

    @classmethod
    def parse(cls, x: str | Callable | tuple) -> Self:
        if not isinstance(x, tuple):
            if isinstance(x, str):
                return cls(str_method, method=x)
            return cls(x)

        fn, *args = x

        non_dict_args = []
        kwargs = {}

        for arg in args:
            if isinstance(arg, dict):
                kwargs.update(arg)
            else:
                non_dict_args.append(arg)

        if isinstance(fn, str):
            if fn in registry:
                return cls(registry[fn], *non_dict_args, **kwargs)
            return cls(str_method, fn, *non_dict_args, **kwargs)
        elif callable(fn):
            return cls(fn, *non_dict_args, **kwargs)

        raise ValueError(f"Cannot parse tuple {x} since it does not begin with a string or a function.")

    def _validate(self) -> None:
        """Raise a ValueError if the supplied function and arguments are invalid.

        This error could be raised if:
        1. self.func does have a single positional-only argument
        2. the first argument (i.e. the positional-only argument) of self.func is not `str` type
        3. the return type of self.fucn is not `str`
        4. the supplied arguments are incomplete, assuming a string value is passed to self.func
           as the first argument

        Conditions 1 and 2 are skipped if self.func is a lambda function.

        Raises:
            ValueError: if self.func violates the rules above.
        """
        sig = signature(self.func)
        params = sig.parameters

        is_lambda = self.func.__code__.co_name == "<lambda>"

        if not is_lambda:
            # check if there is exactly one positional only argument
            tf_var_pos = [param.kind == param.POSITIONAL_ONLY for param in params.values()][:2]
            has_single_pos_only = tf_var_pos[0] is True and sum(tf_var_pos) == 1

            if not has_single_pos_only:
                raise ValueError(f"Function {self.func} must have a single positional-only argument.")

            # check that the positional only argument type is str
            try:
                var_pos_name = next(
                    name for name, param in params.items() if param.kind == param.POSITIONAL_ONLY
                )
            except StopIteration:
                first_arg_takes_string = False
            else:
                first_arg_takes_string = get_annotations(self.func).get(var_pos_name, type(None)) is str

            if not first_arg_takes_string:
                raise ValueError(f"The first argument of {self.func} must be `str`.")

        # check that the return type is str, but wait to raise error until next test
        returns_str = sig.return_annotation is str

        # check that if a string is passed as the first value, then all of the function arguments are
        # supplied
        try:
            sig.bind("test", *self.args, **self.kwargs)
        except TypeError as e:
            raise ValueError(f"Missing arguments {e}") from e
        else:
            if not returns_str:
                returns_str = isinstance(self.func("test", *self.args, **self.kwargs), str)

        if not returns_str:
            raise ValueError(f"The function {self.func} must return `str`.")

    def __repr__(self) -> str:
        if self.keys is not None:
            return f"UpdateSpec({self.func.__name__}, {self.args}, {self.kwargs}, keys={self.keys})"
        return f"UpdateSpec({self.func.__name__}, {self.args}, {self.kwargs})"

    def __call__(self, value: str) -> str:
        """Call the underlying function on `value`, adding in args and kwargs stored in UpdateSpec."""
        return cast(str, self.func(value, *self.args, **self.kwargs))


@singledispatch
def _make_update_dict(spec: Any, to_update: dict[str, str]) -> dict[str, str]:
    """Update dictionary based on an update specification.

    Args:
        spec: UpdateSpec to be used to update dictionary values
        to_update: dictionary to update

    Returns:
        dictionary of keys and value
    """
    raise NotImplementedError(
        f"`spec` must be `UpdateSpec` or `Iterable[UpdateSpec]`; received {spec.__class__}."
    )


@_make_update_dict.register
def _(spec: UpdateSpec, to_update: dict[str, str]) -> dict[str, str]:
    """Update dictionary based on an update specification.

    Args:
        spec: UpdateSpec to be used to update dictionary values
        to_update: dictionary to update

    Returns:
        dictionary of keys and value
    """
    if spec.keys is None:
        spec_dict = {k: spec(v) for k, v in to_update.items()}
    else:
        spec_dict = {k: spec(to_update[k]) for k in spec.keys if k in to_update}

    return spec_dict


@_make_update_dict.register(Iterable)
def _(specs: Iterable[UpdateSpec], to_update: dict[str, str]) -> dict[str, str]:
    spec_dict = {}
    to_update = to_update.copy()

    for spec in specs:
        spec_dict.update(_make_update_dict(spec, to_update))
        to_update.update(spec_dict)

    return spec_dict


DataArrayOrSet = TypeVar("DataArrayOrSet", xr.DataArray, xr.Dataset)
Updater = UpdateSpec | str | Callable | tuple


def _parse_updaters_to_specs(specs: Updater | list[Updater]) -> list[UpdateSpec]:
    if not isinstance(specs, list):
        specs = [specs]

    result = []

    for spec in specs:
        if isinstance(spec, UpdateSpec):
            result.append(spec)
        else:
            result.append(UpdateSpec.parse(spec))

    return result


def update_attrs(
    data: DataArrayOrSet,
    specs: list[Updater] | Updater | None = None,
    global_attrs: dict | None = None,
    **kwargs: Any,
) -> DataArrayOrSet:
    """Update and add to DataArray or Dataset attributes.

    For example, if `ds` is a Dataset, then

    >>> update_attrs(ds,
    >>>              UpdateSpec(str_method, "lower"),
    >>>              global_attrs={"processed_by": "OpenGHG"},
    >>>              updated=datetime.date.today())

    will:

    1. make all existing attributes (on the dataset and all its data variables) lowercase
    2. will add the global attribute `processed_by="OpenGHG"`
    3. added the attribute `updated=datetime.date.today()` globally and on all data variables

    Args:
        data: xr.DataArray or xr.Dataset whose attributes are to be modified
        specs: optional tranformation(s) to be applied to attribute values. These
            can be: the name of a string method (as a string), a function that maps strings
            to strings, or an `UpdateSpec` object. If the string method or function need
            additional arguments, then a tuple (str or function, arg1, arg2, ..., kwargs={...})
            should be passed.
        global_attrs: optional global attributes to add; for a Dataset, these are
            added to the Dataset attributes; for a DataArray, they are combined with
            **kwargs.
        **kwargs: attributes to add to DataArray, or Dataset and all data variables.

    Returns:
        (shallow copy of) DataArray or Dataset with updated attributes

    """
    global_attrs = global_attrs or {}

    if specs is None:
        return data.assign_attrs(**kwargs, **global_attrs)

    parsed_specs = _parse_updaters_to_specs(specs)

    spec_dict = _make_update_dict(parsed_specs, data.attrs)
    spec_dict.update(kwargs)
    data = data.assign_attrs(spec_dict, **global_attrs)

    if isinstance(data, xr.DataArray):
        return data

    # otherwise, data is a Dataset, and we want to update data variable
    # attributes, but not add global attributes
    for dv in data.data_vars:
        data[dv] = update_attrs(data[dv], specs, **kwargs)

    return data


def _make_rename_dict(
    specs: UpdateSpec | list[UpdateSpec], to_rename: xr.Dataset | list[str]
) -> dict[str, str]:
    """Make dictionary to rename data variables based on update specification.

    Args:
        specs: (list of) update specification(s) to apply to data variable names
        to_rename: Dataset whose variables are to be renamed, or a list of strings (representing data var names).
            The list options is mainly for testing.

    Returns:
        dict mapping old names to new names
    """
    if isinstance(to_rename, xr.Dataset):
        to_update = {str(dv): str(dv) for dv in to_rename.data_vars}
    else:
        to_update = {x: x for x in to_rename}

    return _make_update_dict(specs, to_update)


def rename(data: xr.Dataset, specs: Updater | list[Updater]) -> xr.Dataset:
    """Rename data variables based on specification.

    Args:
        data: xr.Dataset whose data variables are to be renamed
        specs: (list of) update specification(s) to apply to data variable names

    Returns:
        xr.Dataset with renamed data variables
    """
    parsed_specs = _parse_updaters_to_specs(specs)
    rename_dict = _make_rename_dict(parsed_specs, data)
    return data.rename(rename_dict)
