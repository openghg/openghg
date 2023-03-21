""" Utility functions that are used by multiple modules

"""
from collections.abc import Iterable
from typing import Any, Dict, Iterator, Optional, Tuple
import logging

logger = logging.getLogger("openghg.util")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


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
    from openghg.util import remove_punctuation
    from rapidfuzz import process  # type: ignore

    site_name = remove_punctuation(site_name)

    inverted = _create_site_lookup_dict()

    matches = process.extract(query=site_name, choices=inverted.keys())
    highest_score = matches[0][1]

    if highest_score < 90:
        return None

    # If there are multiple >= 90 matches we return None as this is ambiguous
    greater_than_90 = sum(match[1] >= 90 for match in matches)
    if greater_than_90 > 1:
        logger.warning("Please provide more site information, more than one site found.")
        return None

    matched_site = matches[0][0]
    site_code: str = inverted[matched_site]

    return site_code.lower()


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


def _create_site_lookup_dict() -> Dict:
    """Create a dictionary of site name: three letter site code values

    Returns:
        dict: Dictionary of site_name: site_code values
    """
    from openghg_defs import site_info_file
    from openghg.util import load_json, remove_punctuation

    site_info = load_json(path=site_info_file)

    inverted = {}
    for site, site_data in site_info.items():
        for _, network_data in site_data.items():
            try:
                long_name = network_data["long_name"]
            except KeyError:
                pass
            else:
                # Remove the country from the name
                try:
                    no_country = remove_punctuation(long_name.split(",")[0])
                except IndexError:
                    no_country = remove_punctuation(long_name)

                inverted[no_country] = site

            break

    return inverted


def verify_site(site: str) -> Optional[str]:
    """Check if the passed site is a valid one and returns the three
    letter site code if found. Otherwise we use fuzzy text matching to suggest
    sites with similar names.

    Args:
        site: Three letter site code or site name
    Returns:
        str: Verified three letter site code if valid site
    """
    from openghg.util import load_json
    from openghg_defs import site_info_file

    site_data = load_json(path=site_info_file)

    if site.upper() in site_data:
        return site.lower()
    else:
        site_code = site_code_finder(site_name=site)
        if site_code is None:
            logger.warning(f"Unable to find site code for {site}, please provide additional metadata.")
        return site_code


def multiple_inlets(site: str) -> bool:
    """Check if the passed site has more than one inlet

    Args:
        site: Three letter site code
    Returns:
        bool: True if multiple inlets
    """
    from openghg.util import get_site_info

    site_data = get_site_info()

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
