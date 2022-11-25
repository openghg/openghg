from openghg.util import load_json
from typing import Union
from pathlib import Path


def sites_in_network(network: str, site_json: Union[str, Path] = "default") -> list:
    """
    Extract details of all the sites within a network.
    This will use the "acrg_site_info.json" file by default.

    Note: this will assume the network is stored in upper case.

    Args:
        network: Name of the network
        site_json: By default this will use the "acrg_site_info.json" file
            but an alternative file which matches to this format may be specified.
    
    Returns:
        list: List of site codes.
    """

    if site_json == "default":
        site_data = load_json(filename="acrg_site_info.json")
    else:
        site_json_path = Path(site_json)
        path = site_json_path.parent
        filename = site_json_path.name
        site_data = load_json(filename=filename, path=path)

    network = network.upper()

    matching_sites = []
    for site, details in site_data.items():
        networks = details.keys()
        networks = [n.upper() for n in networks]
        if network in networks:
            matching_sites.append(site)

    return matching_sites
