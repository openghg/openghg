from typing import Any
from openghg.util import load_json
from openghg.types import optionalPathType

__all__ = ["get_site_info", "sites_in_network"]


def get_site_info(site_filepath: optionalPathType = None) -> dict[str, Any]:
    """Extract data from site info JSON file as a dictionary.

    This uses the data stored within openghg_defs/data/site_info JSON file by default.

    Args:
        site_filepath: Alternative site info file.
    Returns:
        dict: Data from site JSON file
    """
    from openghg_defs import site_info_file

    if site_filepath is None:
        site_info_json = load_json(path=site_info_file)
    else:
        site_info_json = load_json(path=site_filepath)

    return site_info_json


def sites_in_network(network: str, site_filepath: optionalPathType = None) -> list:
    """Extract details of all the sites within a network.
    Note: this will assume the network is stored in upper case.

    Args:
        network: Name of the network
        site_filepath: Alternative site info file. Defaults to openghg_defs input.
    Returns:
        list: List of site codes.
    """
    # Load in site data
    site_data = get_site_info(site_filepath=site_filepath)

    network = network.upper()

    matching_sites = []
    for site, details in site_data.items():
        networks = details.keys()
        networks = [n.upper() for n in networks]
        if network in networks:
            matching_sites.append(site)

    return matching_sites
