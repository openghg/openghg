import numpy as np
from numpy import ndarray
from typing import Tuple, Union, Dict, Any
from openghg.util import get_datapath, load_json


def find_domain(domain: str) -> Tuple[ndarray, ndarray]:
    """
    Finds the latitude and longitude values in degrees associated
    with a given domain name.

    Args:
        domain: Pre-defined domain name (see 'domain_info.json')

    Returns:
        array, array : Latitude and longitude values for the domain in degrees.
    """
    domain_info = load_json(filename="domain_info.json")

    # Look for domain in domain_info file
    if domain in domain_info:
        domain_data = domain_info[domain]
    elif domain.upper() in domain_info:
        domain = domain.upper()
        domain_data = domain_info[domain]
    else:
        raise ValueError(f"Pre-defined domain '{domain}' not found")

    # Extract or create latitude and longitude data
    latitude = _get_coord_data("latitude", domain_data, domain)
    longitude = _get_coord_data("longitude", domain_data, domain)

    return latitude, longitude


def _get_coord_data(coord: str, data: Dict[str, Any], domain: str) -> ndarray:
    """
    Attempts to extract or derive coordinate (typically latitude/longitude)
    values for a domain from provided data dictionary (typically
    this can be derived from 'domain_info.json' file).

    This looks for:
     - File containing coordinate values (in degrees)
         - Looks for "{coord}_file" attribute e.g. "latitude_file"
         - OR for a file within "domain" subfolder called "{domain}_{coord}.dat"
           e.g. "EUROPE_latitude.dat"
     - "{coord}_range" and "{coord}_increment" attributes to use to construct
       the coordinate values e.g. "latitude_range" to include the start and
       end (inclusive) range and "latitude_increment" for the step in degrees.

    Args:
        coord: Name of coordinate (e.g. latitude, longitude)
        data: Data dictionary containing details of domain
              (e.g. derived from 'domain_info.json')
        domain: Name of domain

    Returns:
        array: Extracted or derived coordinate values
    """
    # Look for explicit file keyword in data e.g. "latitude_file"
    # Extract data from file if found and return
    filename_str = f"{coord}_file"
    if filename_str in data:
        full_filename = get_datapath(data[filename_str])
        coord_data: ndarray = np.loadtxt(full_filename)
        return coord_data

    # If no explicit file name defined, look within known location to see
    # if data is present by looking for file of form "domain/{domain}_{coord}.csv"
    # e.g. "domain/EUROPE_latitude.csv" (within "openghg/openghg/data" folder)
    try:
        full_filename = get_datapath(f"{domain}_{coord}.dat", "domain")
        coord_data = np.loadtxt(full_filename)
    except OSError:
        pass
    else:
        return coord_data

    # If no data files can be found, look for coordinate range and increment values
    # If present, create the coordinate data. If not raise a ValueError.
    try:
        coord_range = data[f"{coord}_range"]
        increment = data[f"{coord}_increment"]
    except KeyError:
        raise ValueError(f"Unable to get {coord} coordinate data for domain: {domain}")

    coord_min = float(coord_range[0])
    coord_max = float(coord_range[-1])
    increment = float(increment)

    coord_data = np.arange(coord_min, coord_max + increment, increment)

    return coord_data


def convert_longitude(
    longitude: ndarray, return_index: bool = False
) -> Union[ndarray, Tuple[ndarray, ndarray]]:
    """
    Convert longitude extent to -180 - 180 and reorder.

    Args:
        longitude: Array of valid longitude values in degrees.
        return_index: Return re-ordering index as well as updated longitude

    Returns:
        ndarray(, ndarray) : Updated longitude values and new indices if requested.
    """
    # Check range of longitude values and convert to -180 - +180
    mtohe = longitude > 180
    longitude[mtohe] = longitude[mtohe] - 360
    ordinds = np.argsort(longitude)
    longitude = longitude[ordinds]

    if return_index:
        return longitude, ordinds
    else:
        return longitude
