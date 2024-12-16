from typing import Any

import numpy as np
from numpy import ndarray

from openghg.types import optionalPathType, ArrayLikeMatch, ArrayLike, XrDataLike, XrDataLikeMatch

import logging

logger = logging.getLogger("openghg.util.domain")

__all__ = ["get_domain_info", "find_domain", "convert_lon_to_180", "convert_lon_to_360"]


def get_domain_info(domain_filepath: optionalPathType = None) -> dict[str, Any]:
    """Extract data from domain info JSON file as a dictionary.

    This uses the data stored within openghg_defs/domain_info JSON file by default.

    Args:
        domain_filepath: Alternative domain info file.
    Returns:
        dict: Data from domain JSON file
    """
    from openghg_defs import domain_info_file
    from openghg.util import load_json

    if domain_filepath is None:
        domain_info_json = load_json(path=domain_info_file)
    else:
        domain_info_json = load_json(path=domain_filepath)

    return domain_info_json


def find_domain(domain: str, domain_filepath: optionalPathType = None) -> tuple[ndarray, ndarray]:
    """Finds the latitude and longitude values in degrees associated
    with a given domain name.

    Args:
        domain: Pre-defined domain name
        domain_filepath: Alternative domain info file. Defaults to openghg_defs input.
    Returns:
        array, array : Latitude and longitude values for the domain in degrees.
    """

    domain_info = get_domain_info(domain_filepath)

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


def _get_coord_data(coord: str, data: dict[str, Any], domain: str) -> ndarray:
    """Attempts to extract or derive coordinate (typically latitude/longitude)
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
    from openghg_defs import data_path

    # Look for explicit file keyword in data e.g. "latitude_file"
    # Extract data from file if found and return
    filename_str = f"{coord}_file"
    if filename_str in data:
        full_filename = data_path / data[filename_str]
        coord_data: ndarray = np.loadtxt(full_filename)
        return coord_data

    # If no explicit file name defined, look within known location to see
    # if data is present by looking for file of form "domain/{domain}_{coord}.csv"
    # e.g. "domain/EUROPE_latitude.csv" (within "openghg/openghg/data" folder)
    try:
        full_filename = data_path / "domain" / f"{domain}_{coord}.dat"
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


def find_coord_name(data: XrDataLike, options: list[str]) -> str | None:
    """
    Find the name of a coordinate based on input options.
    Only the first found value will be returned.

    Args:
        data: xarray Data structure
        options: List of options to check. Will be checked in order.

    Returns:
        str / None: Name of coordinate if located within data. None otherwise.
    """
    for option in options:
        if option in data.coords:
            name = option
            break
    else:
        return None

    return name


def convert_lon_to_180(longitude: ArrayLikeMatch) -> ArrayLikeMatch:
    """
    Convert longitude extent from (0 to 360) to (-180 to 180).
    This does *not* reorder the values.

    Args:
        longitude: Valid longitude values in degrees.
    Returns:
        ndarray / DataArray : Updated longitude values in the same order.
    """

    # Check range of longitude values and convert to -180 to 180.
    longitude = ((longitude - 180) % 360) - 180

    return longitude


def convert_lon_to_360(longitude: ArrayLikeMatch) -> ArrayLikeMatch:
    """
    Convert longitude extent to (0 to 360).
    This does *not* reorder the values.

    Args:
        longitude: Valid longitude values in degrees.
    Returns:
        ndarray / DataArray : Updated longitude values in the same order.
    """

    # Check range of longitude values and convert to 0 to 360.
    longitude = longitude % 360

    return longitude


def convert_internal_longitude(
    data: XrDataLikeMatch, lon_name: str | None = None, reorder: bool = True
) -> XrDataLikeMatch:
    """
    Convert longitude coordinate within an xarray data structure (DataArray or Dataset).

    Args:
        data: Data with longitude values to convert.
        lon_name: By default will look a coord called "lon" or "longitude".
            Otherwise must be specified.
        reorder: Whether to reorder the data based on the converted longitude values.
    Returns:
        DataArray / Dataset: Input data with updated longitude values
    """
    if lon_name is None:
        lon_options = ["lon", "longitude"]
        lon_name = find_coord_name(data, lon_options)
        if lon_name is None:
            raise ValueError("Please specify 'lon_name'.")

    longitude = data[lon_name]
    longitude = convert_lon_to_180(longitude)

    data = data.assign_coords({lon_name: longitude})

    if reorder:
        data = data.sortby(lon_name)

    return data


def cut_data_extent(
    data: XrDataLikeMatch,
    lat_out: ArrayLike,
    lon_out: ArrayLike,
    lat_name: str | None = None,
    lon_name: str | None = None,
    copy: bool = False,
) -> XrDataLikeMatch:
    """
    Cut down extent of data within an xarray data structure (DataArray or Dataset)
    against an output latitude and longitude range.

    A buffer based on the maximum difference along the lon_out and lat_out dimensions
    will be added when the data is cut.

    Args:
        data: Data to be cut down
        lat_out: Array containing output latitude values
        lon_out: Array containing output longitude values
        lat_name: Name of latitude dimension. Must be specified if not "lat" or "latitude".
        lon_name: Name of longitude dimension. Must be specified if not "lon" or "longitude".
        copy: Whether to explicitly copy the data.

    Returns:
        xarray.DataArray / xarray.Dataset: data with reduced lat, lon ranges.
    """
    if lat_name is None:
        lat_options = ["lat", "latitude"]
        lat_name = find_coord_name(data, lat_options)
        if lat_name is None:
            raise ValueError("Please specify 'lat_name'.")

    if lon_name is None:
        lon_options = ["lon", "longitude"]
        lon_name = find_coord_name(data, lon_options)
        if lon_name is None:
            raise ValueError("Please specify 'lon_name'.")

    if isinstance(lat_out, np.ndarray):
        lat_out.sort()
    else:
        lat_out = lat_out.sortby(lat_out[lat_name])

    if isinstance(lon_out, np.ndarray):
        lon_out.sort()
    else:
        lon_out = lon_out.sortby(lon_out[lon_name])

    lat_diff = (lat_out[1:] - lat_out[:-1]).max()
    lon_diff = (lon_out[1:] - lon_out[:-1]).max()

    lat_low = np.min(lat_out) - lat_diff
    lat_high = np.max(lat_out) + lat_diff
    lon_low = np.min(lon_out) - lon_diff
    lon_high = np.max(lon_out) + lon_diff

    lat_cut_wide_range = slice(lat_low, lat_high)
    lon_cut_wide_range = slice(lon_low, lon_high)

    if copy:
        data_cut = data.copy()
    else:
        data_cut = data

    data_cut = data_cut.sel({lat_name: lat_cut_wide_range, lon_name: lon_cut_wide_range})

    return data_cut


def check_coord_alignment(data: XrDataLikeMatch, domain: str, coord: str) -> XrDataLikeMatch:
    """
    Check that the values of a given coordinate (lat/lon or latitude/longitude) in
    spatial data matches the openghg_defs values for that domain. If they don't match
    roughly (i.e. within 5%), an exception is raised to alert the user to the fact that
    they may have the wrong domain altogether. An exception is also raised if the length
    of the arrays don't match. If the lat or lon don't match exactly, the old coordinates
    are replaced with the 'correct' values from openghg_defs.

    If the domain is not one of the domains listed in openghg_defs
    (i.e. ARCTIC, EASTASIA, EUROPE, PACIFIC, SOUTHAFRICA, USA) then the checks are not
    carried out and the data is returned unaltered.

    Args:
        data: spatial data to be checked. Must have 'lat' and 'lon' dimensions
        domain: domain in question. Must be a valid domain in openghg_defs. If
                this is a domain from openghg_defs this will be checked and aligned with
                this definition. Otherwise, coordinates will not be changed.
        coord: coordinate to check. Currently 'lat' or 'lon' only.

    Returns:
        xarray.DataArray / xarray.Dataset: data with coordinate ranges aligned to openghg domain.
    """

    known_domains = list(get_domain_info().keys())

    if domain.upper() not in known_domains:
        logger.warning(
            f"Domain input: {domain} is not a standard domain within openghg_defs and so has not been standardised against this."
        )
        return data

    coords_in = data[coord].values

    true_lats, true_lons = find_domain(domain)
    if coord in ["lat", "latitude"]:
        true_coords = true_lats
    elif coord in ["lon", "longitude"]:
        true_coords = true_lons

    if len(coords_in) != len(true_coords):
        raise ValueError(f"length of {coord} coordinates does not match those in openghg {domain} domain")

    if not np.allclose(coords_in, true_coords, atol=0, rtol=0.05):
        raise ValueError(
            f"input {coord} coordinates vary significantly from openghg {domain} domain. Please check correct domain and coordinates selected"
        )

    data = data.assign_coords({coord: true_coords})

    return data


def align_lat_lon(data: XrDataLikeMatch, domain: str) -> XrDataLikeMatch:
    """
    Aligns the lat and lon coordinates of a DataArray or Dataset to the openghg_defs values, according to
    the 'check_coord_alignment' function.

    Args:
        data: spatial data to be checked. Must have 'lat'/'latitude' and 'lon'/'longitude' dimensions
        domain: domain in question. Must be a valid domain in openghg_defs

    Returns:
        xarray.DataArray / xarray.Dataset: data with lat, lon ranges aligned to openghg domain.
    """
    dims = data.dims
    if "longitude" in dims and "latitude" in dims:
        data = check_coord_alignment(data=data, domain=domain, coord="latitude")
        data = check_coord_alignment(data=data, domain=domain, coord="longitude")
    elif "lon" in dims and "lat" in dims:
        data = check_coord_alignment(data=data, domain=domain, coord="lat")
        data = check_coord_alignment(data=data, domain=domain, coord="lon")
    else:
        raise ValueError(
            'Dimensions "lon" and "lat" (or "longitude" and "latitude") could not be found in dataset.'
        )

    return data
