"""Helper functions for processing search input into form accepted by `Metastore.search`."""

import itertools
from typing import Any, cast, TypeVar
from collections.abc import Callable

import numpy as np

from openghg.util import extract_float
from openghg.types import Comparable


T = TypeVar("T", bound=Comparable)  # types with <=


def _in_interval(x: T, start: T | None, stop: T | None) -> bool:
    """Return True if start <= x <= stop; if either start or stop is None, omit
    the corresponding bound.

    Args:
        x: value to test for inclusion in the interval [start, stop]
        start: start of the interval
        stop: end of the interval

    Returns:
        True if start <= x <= stop, False otherwise
    """
    if start is None and stop is None:
        return True
    elif start is None:
        stop = cast(T, stop)  # to appease mypy
        return x <= stop
    elif stop is None:
        return x >= start
    else:
        return start <= x <= stop


def _convert_slice_to_test(s: slice, key: str | None = None) -> Callable:
    """Convert slice to a function that checks if values are in the interval specified by the slice.

    Args:
        s: slice specifying interval of values
        key: optional metadata key, used to choose value formatting

    Returns:
        function that returns True if values are in the slice interval
    """

    def formatter(x: Any) -> Any:
        """Formatting if key == 'inlet': try to extract float from strings."""
        if key == "inlet":
            if isinstance(x, (int, float)):
                return x
            if isinstance(x, str):
                try:
                    result = extract_float(x)
                except ValueError:
                    return None
                else:
                    return result
            return None
        return x  # key != "inlet"

    def test_func(x: Any) -> bool:
        """Return True if start <= formatter(x) <= stop."""
        return _in_interval(formatter(x), formatter(s.start), formatter(s.stop))  # type: ignore

    return test_func


def _is_neg_lookup_flag(x: Any) -> bool:
    """Check if x matches the `neg_lookup_flag`."""
    try:
        result = bool(np.isnan(x))
    except TypeError:
        return False
    return result


def process_special_queries(search_terms: dict) -> dict:
    """Separate 'function queries' and 'negative lookup keys' from normal search terms.

    Function queries apply a function to the value stored at a given key.
    Negative lookup keys are keys whose value is np.nan (or `float("nan")`).

    Args:
        search_terms: dict of search terms

    Returns:
        dict containing search_terms dict, search_functions dict, and negative_lookup_keys list, which
        are the parameters for TinyDBMetastore.search
    """
    _search_terms = search_terms.copy()  # copy to avoid mutating search_terms while iterating over items
    search_functions = {}
    negative_lookup_keys = []

    for k, v in search_terms.items():
        if isinstance(v, slice):
            search_functions[k] = _convert_slice_to_test(v, key=k)
            del _search_terms[k]
        elif _is_neg_lookup_flag(v):
            negative_lookup_keys.append(k)
            del _search_terms[k]

    return {
        "search_terms": _search_terms,
        "search_functions": search_functions,
        "negative_lookup_keys": negative_lookup_keys,
    }


def flatten_search_kwargs(search_kwargs: dict) -> list[dict]:
    """Process search kwargs into list of flat dictionaries with the correct combinations of search queries.

    To set this up for keywords with multiple options, lists of the (key, value) pair terms are created.

    For instance, if

        species = ["ch4", "methane"]

    and

        time_resolution = {"time_resolved": "true", "high_time_resolution: "true"}

    we expect this to create search options to look for: "species" as "ch4" OR "methane" AND
    either "time_resolved" as "true" OR "high_time_resolution" as "true".

    Args:
        search_kwargs: dictionary of search terms

    Returns:
        list of flat dictionaries containing all combinations of search terms from (nested) input search terms
    """
    single_options = {}

    # multiple_options will contain tuple pairs for the options we wish to search for. e.g. for
    # species = ["ch4", "methane"], time_resolution = {"time_resolved": "true", "high_time_resolution: "true"}
    # multiple_options is: [[("species", "ch4"), ("species", "methane")],
    #                       [("time_resolved": "true"), ("high_time_resolution": "true")]]
    multiple_options = []

    for k, v in search_kwargs.items():
        if isinstance(v, (list, tuple)):
            expand_key_values = [(k, value) for value in v]
            multiple_options.append(expand_key_values)
        elif isinstance(v, dict):
            expand_key_values = list(v.items())
            multiple_options.append(expand_key_values)
        else:
            single_options[k] = v

    expanded_search = []
    if multiple_options:
        # Ensure that all permutations of the search options are created.
        for kv_pair in itertools.product(*multiple_options):
            d = dict(kv_pair)
            if single_options:
                d.update(single_options)
            expanded_search.append(d)
    else:
        expanded_search.append(single_options)

    return expanded_search


def process_search_kwargs(search_kwargs: dict) -> list[dict]:
    """Flatten search kwargs and process species queries."""
    expanded_search = flatten_search_kwargs(search_kwargs)
    return [process_special_queries(x) for x in expanded_search]
