""" Utility functions that are used by multiple modules

"""
from collections.abc import Iterable
from typing import Any, Dict, Iterator, Optional, Tuple


def running_in_cloud() -> bool:
    """Are we running in the cloud?

    Checks for the OPENGHG_CLOUD environment variable being set

    Returns:
        bool: True if running in cloud
    """
    from os import environ

    cloud_env = environ.get("OPENGHG_CLOUD", "0")

    return bool(int(cloud_env))


def running_on_hub() -> bool:
    """Are we running on the OpenGHG Hub?

    Checks for the OPENGHG_CLOUD environment variable being set

    Returns:
        bool: True if running in cloud
    """
    from os import environ

    hub_env = environ.get("OPENGHG_HUB", "0")

    return bool(int(hub_env))


def running_locally() -> bool:
    """Are we running OpenGHG locally?

    Returns:
        bool: True if running locally
    """
    return not (running_on_hub() or running_in_cloud())


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


def pairwise(iterable: Iterable) -> Iterator[Tuple[Any, Any]]:
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


def site_code_finder(site_name: str) -> Optional[str]:
    """Find the site code for a given site name.

    Args:
        site_name: Site long name
    Returns:
        str or None: Three letter site code if found
    """
    from openghg.util import load_json
    from rapidfuzz import process  # type: ignore

    sites = load_json("site_lookup.json")

    inverted = {s["short_name"]: c for c, s in sites.items()}

    matches = process.extract(query=site_name, choices=inverted.keys())
    highest_score = matches[0][1]

    if highest_score < 90:
        return None

    matched_site = matches[0][0]
    site_code: str = inverted[matched_site]

    return site_code


def find_matching_site(site_name: str, possible_sites: Dict) -> str:
    """Try and find a similar name to site_name in site_list and return a suggestion or
    error string.

    Args:
        site_name: Name of site
        site_list: List of sites to check
    Returns:
        str: Suggestion / error message
    """
    from rapidfuzz import process

    site_list = possible_sites.keys()

    matches = process.extract(site_name, site_list)

    scores = [s for m, s, _ in matches]

    # This seems like a decent cutoff score for a decent find
    cutoff_score = 85

    if scores[0] < cutoff_score:
        return f"No suggestion for {site_name}."
    elif scores[0] > cutoff_score and scores[0] > scores[1]:
        best_match = matches[0][0]
        return f"Did you mean {best_match.upper()}, code: {possible_sites[best_match]} ?"
    elif scores[0] == scores[1]:
        suggestions = [f"{match.title()}, code: {possible_sites[match]}" for match, _, _ in matches]
        nl_char = "\n"
        return f"Did you mean one of : \n {nl_char.join(suggestions)}"
    else:
        return f"Unknown site: {site_name}"


def verify_site(site: str) -> str:
    """Check if the passed site is a valid one and returns the three
    letter site code if found. Otherwise we use fuzzy text matching to suggest
    sites with similar names.

    Args:
        site: Three letter site code or site name
    Returns:
        str: Verified three letter site code if valid site
    """
    from openghg.types import InvalidSiteError
    from openghg.util import load_json, remove_punctuation

    site_data = load_json("site_lookup.json")

    if site.upper() in site_data:
        return site.lower()
    else:
        site = remove_punctuation(site)
        name_lookup: Dict[str, str] = {value["short_name"]: code for code, value in site_data.items()}

        try:
            return name_lookup[site].lower()
        except KeyError:
            long_names = {value["long_name"]: code for code, value in site_data.items()}
            message = find_matching_site(site_name=site, possible_sites=long_names)
            raise InvalidSiteError(message)


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
