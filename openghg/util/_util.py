""" Utility functions that are used by multiple modules

"""
from typing import Dict, Tuple, Iterator
from collections.abc import Iterable


__all__ = [
    "unanimous",
    "valid_site",
    "pairwise",
    "multiple_inlets",
]


def unanimous(seq: Dict) -> bool:
    """Checks that all values in an iterable object
    are the same

    Args:
        seq: Iterable object
    Returns
        bool: True if all values are the same

    """
    it = iter(seq.values())
    try:
        first = next(it)
    except StopIteration:
        return True
    else:
        return all(i == first for i in it)


def pairwise(iterable: Iterable) -> Iterator[Tuple[str, str]]:
    """Return a zip of an iterable where a is the iterable
    and b is the iterable advanced one step.

    Args:
        iterable: Any iterable type
    Returns:
        tuple: Tuple of iterables
    """
    from itertools import tee

    a, b = tee(iterable)
    next(b, None)

    return zip(a, b)


def valid_site(site: str) -> bool:
    """Check if the passed site is a valid one

    Args:
        site: Three letter site code
    Returns:
        bool: True if site is valid
    """
    from openghg.util import load_json

    site_data = load_json("acrg_site_info.json")

    return site.upper() in site_data


def multiple_inlets(site: str) -> bool:
    """Check if the passed site has more than one inlet

    Args:
        site: Three letter site code
    Returns:
        bool: True if multiple inlets
    """
    from openghg.util import load_json

    site_data = load_json("acrg_site_info.json")

    site = site.upper()
    network = next(iter(site_data[site]))

    try:
        heights = set(site_data[network]["height"])
    except KeyError:
        try:
            heights = set(site_data[network]["height_name"])
        except KeyError:
            return True

    return len(heights) > 1
