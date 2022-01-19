""" Utility functions that are used by multiple modules

"""
from typing import Dict, Tuple, Iterator
from collections.abc import Iterable


__all__ = [
    "unanimous",
    "verify_site",
    "pairwise",
    "multiple_inlets",
    "running_in_cloud",
]


def running_in_cloud() -> bool:
    """Are we running in the cloud?

    Checks for the OPENGHG_CLOUD environment variable being set

    Returns:
        bool: True if running in cloud
    """
    from os import environ

    cloud_env = environ.get("OPENGHG_CLOUD")

    return cloud_env is not None


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


def find_matching_site(site_name: str) -> str:
    """Try and find the matching site code for a given site name

    Args:
        site_name: Name of site
    Returns:
        str: Site code
    """
    from rapidfuzz import process
    from openghg.util import load_json, InvalidSiteError

    name_code_lookup: Dict[str, str] = load_json(filename="name_code_lookup.json")

    matches = process.extract(site_name, name_code_lookup.keys())
    scores = [s for m, s, _ in matches]

    # This seems like a decent cutoff score for a decent find
    cutoff_score = 85

    if scores[0] == scores[1] or scores[0] < cutoff_score:
        raise InvalidSiteError(f"No definite match for {site_name}.")

    best_match = matches[0][0]

    return name_code_lookup[best_match]


def verify_site(site: str) -> str:
    """Check if the passed site is a valid one

    Args:
        site: Three letter site code
    Returns:
        bool: True if site is valid
    """
    from openghg.util import load_json

    site_data = load_json("acrg_site_info.json")

    if site.upper() in site_data:
        return site.lower()
    else:
        site_code = find_matching_site(site_name=site)
        return site_code.lower()


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
