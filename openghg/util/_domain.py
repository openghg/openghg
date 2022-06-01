import numpy as np
from numpy import ndarray
from typing import Tuple
from openghg.util import get_datapath, load_json


def find_domain(domain: str) -> Tuple[ndarray, ndarray]:
    """
    TODO: Add docstring
    """
    domain_info = load_json(filename="domain_info.json")

    # Look for domain in domain_info file
    if domain in domain_info:
        domain_data = domain_info[domain]
    else:
        raise ValueError(f"Pre-defined domain '{domain}' not found")

    # Extract or create latitude and longitude data
    latitude = _get_coord_data("latitude", domain_data, domain)
    longitude = _get_coord_data("longitude", domain_data, domain)

    return latitude, longitude

    
def _get_coord_data(coord: str, data: dict, domain: str):
    """
    TODO: Add docstring
    """
    # Look for explicit file keyword in data e.g. "latitude_file"
    # Extract data from file if found and return
    filename_str = f"{coord}_file"
    if filename_str in data:
        full_filename = get_datapath(data[filename_str])
        coord_data = np.loadtxt(full_filename)
        return coord_data

    # If no explicit file name defined, look within known location to see
    # if data is present by looking for file of form "domain/{domain}_{coord}.csv"
    # e.g. "domain/EUROPE_latitude.csv" (within "openghg/openghg/data" folder)
    try:
        full_filename = get_datapath(f"{domain}_{coord}.csv", "domain")
        coord_data = np.loadtxt(full_filename)
    except OSError:
        pass
    else:
        return coord_data

    # If no data files can be found, look for coordinate range and increment values
    # If present, create the coordinate data. If not raise a ValueError.
    try:
        range = data[f"{coord}_range"]
        increment = data[f"{coord}_increment"]
    except KeyError:
        raise ValueError(f"Unable to get {coord} coordinate data for domain: {domain}")

    coord_min = float(range[0])
    coord_max = float(range[-1])
    increment = float(increment)

    coord_data = np.arange(coord_min, coord_max+increment, increment)

    return coord_data
