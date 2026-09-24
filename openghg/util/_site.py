from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

from openghg.util._inlet import format_inlet

__all__ = ["get_site_info", "sites_in_network", "_get_site_data"]


def get_site_info(site_filepath: str | Path | None = None) -> dict[str, Any]:
    """Extract data from site info JSON file as a dictionary.

    This uses the data stored within openghg_defs/data/site_info JSON file by default.

    Args:
        site_filepath: Alternative site info file.
    Returns:
        dict: Data from site JSON file
    """
    from openghg_defs import site_info_file

    fpath = site_info_file if site_filepath is None else site_filepath
    return cast(dict[str, Any], json.loads(Path(fpath).read_text()))


def _get_site_data(site: str, network: str) -> tuple[float, float, float, list]:
    """Extract site location data from site attributes file.

    Args:
        site: Site code
    Returns:
        dict: Dictionary of site data
    """

    network = network.upper()
    site = site.upper()

    site_info = get_site_info()

    try:
        site_data = site_info[site][network]
        latitude = float(site_data["latitude"])
        longitute = float(site_data["longitude"])
        site_height = float(site_data["height_station_masl"])
        inlet_heights = site_data["height_name"]
    except KeyError as e:
        raise KeyError(f"Incorrect site or network : {e}")

    inlet_heights = [format_inlet(inlet, units="m") for inlet in inlet_heights]

    return latitude, longitute, site_height, inlet_heights


def sites_in_network(network: str, site_filepath: str | Path | None = None) -> list:
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
