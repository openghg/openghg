"""
Helper functions for parsing the list of dicts returned by standardise
functions.

For example,

results = filt(results, file="some_file_name.nc")

will return a list of dicts containing the key-value pair file="some_file_name.nc".
"""


def filt(results: list[dict], **kwargs) -> list[dict]:
    """Filter list of dicts by kwargs"""
    return [res for res in results if kwargs.items() <= res.items()]


def select(results: list[dict], *args) -> list[dict]:
    """Select key-value pairs from result dicts by keys passed as args"""
    selected_results = []

    for res in results:
        selected_results.append({arg: res.get(arg) for arg in args})

    return selected_results


def make_keys(results, *args) -> list[str]:
    """Make strings combining values of metadata."""
    if not args:
        args = ("species", "inlet")

    keys = []
    for res in select(results, *args):
        values_strings = list(map(str, res.values()))
        keys.append("_".join(values_strings))

    return keys
