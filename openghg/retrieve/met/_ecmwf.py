from __future__ import annotations

import numpy as np
from typing import List, Optional, Union, Tuple
import os
import pandas as pd
import xarray as xr

from openghg.dataobjects import METData

import logging

logger = logging.getLogger("openghg.store")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler

# import requests
# from xarray import open_dataset, Dataset
# from requests.adapters import HTTPAdapter
# from requests.packages.urllib3.util.retry import Retry
# from openghg.util import timestamp_tzaware


__all__ = ["retrieve_met", "METData"]


def retrieve_met(
    site: str,
    network: str,
    years: Optional[Union[str, List[str]]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    variables: Optional[List[str]] = None,
    save_path: Optional[str] = None,
) -> METData:
    """Retrieve METData data. Downloads monthly met files.

    This function currently on retrieves data from the "reanalysis-era5-pressure-levels"
    dataset but may be modified for other datasets in the future.
    Args:
        site: Three letter sitec code
        network: Network
        years: Year(s) of data required
    Returns:
        METData: METData object holding data and metadata
    """
    import cdsapi

    # check right date
    if years is None and (start_date is None or end_date is None):
        raise AttributeError("You /must pass either the argument years or both start_date and end_date")

    if variables is None:
        variables = ["u_component_of_wind", "v_component_of_wind"]

        valid_variables = [
            "divergence",
            "fraction_of_cloud_cover",
            "geopotential",
            "ozone_mass_mixing_ratio",
            "potential_vorticity",
            "relative_humidity",
            "specific_cloud_ice_water_content",
            "specific_cloud_liquid_water_content",
            "specific_humidity",
            "specific_rain_water_content",
            "specific_snow_water_content",
            "temperature",
            "u_component_of_wind",
            "v_component_of_wind",
            "vertical_velocity",
            "vorticity",
        ]

        print(variables)
        assert np.all(
            [var in valid_variables for var in variables]
        ), f"One of more of the variables passed does not exist in ERA5 data. The problematic variables are {set(variables) - set(valid_variables)}"

    latitude, longitude, site_height, inlet_heights = get_site_data(site=site, network=network)

    # Get the area to retrieve data for
    ecmwf_area = _get_ecmwf_area(site_lat=latitude, site_long=longitude)
    # Calculate the pressure at measurement height(s)
    measure_pressure = get_site_pressure(inlet_heights=inlet_heights, site_height=site_height)
    # Calculate the ERA5 pressure levels required
    ecmwf_pressure_levels = _altitude_to_ecmwf_pressure(measure_pressure=measure_pressure)

    if years is not None:
        if not isinstance(years, list):
            years = [years]
        else:
            years = sorted(years)

        download_list = pd.period_range(start=str(years[0]), end=str(int(years[-1]) + 1), freq="M")[:-1]
        start_d = pd.Timestamp(download_list[0])
        end_d = pd.Timestamp(download_list[-1])

    if start_date is not None and end_date is not None:
        download_list = pd.period_range(start=start_date, end=end_date, freq="M")
        start_d = pd.Timestamp(start_date)
        end_d = pd.Timestamp(end_date)

    cds_client = cdsapi.Client()
    dataset_name = "reanalysis-era5-pressure-levels"
    default_save_path = os.path.join(os.getcwd().split("openghg")[0], "openghg/metdata")
    save_path = default_save_path if save_path is None else save_path
    assert os.path.isdir(
        save_path
    ), f"The save path {save_path} is not a directory. Please create it or pass a different save_path"

    logger.info(f"Retrieving to {save_path}")

    years = list(set([str(download_date).split("-")[0] for download_date in download_list]))
    months = list(set([str(download_date).split("-")[1] for download_date in download_list]))
    request = {
        "product_type": "reanalysis",
        "format": "netcdf",
        "variable": variables,
        "pressure_level": ecmwf_pressure_levels,
        "year": years,
        "month": months,
        "day": [str(x).zfill(2) for x in range(1, 32)],
        "time": [f"{str(x).zfill(2)}:00" for x in range(0, 24)],
        "area": ecmwf_area,
    }

    print(request)
    # retrieve from copernicus and save requested data to a temp file
    try:
        cds_client.retrieve(
            dataset_name,
            request,
            os.path.join(
                save_path,
                f"Met_{site}_{network}_{start_d.year}{start_d.month}_{end_d.year}{end_d.month}_temp.nc",
            ),
        )

    except Exception as e:
        logger.warn(f"Retrieve process failed with error {e}")

    dataset = xr.open_dataset(
        os.path.join(
            save_path,
            f"Met_{site}_{network}_{start_d.year}{start_d.month}_{end_d.year}{end_d.month}_temp.nc",
        )
    )

    metadata = {
        "product_type": request["product_type"],
        "format": request["format"],
        "variable": request["variable"],
        "pressure_level": request["pressure_level"],
        "area": request["area"],
        "site": site,
        "network": network,
        "start_date": str(start_d),
        "end_date": str(end_d),
    }

    # resaving with attributes
    dataset.attrs.update(metadata)
    dataset.to_netcdf(
        os.path.join(
            save_path,
            f"Met_{site}_{network}_{start_d.year}{start_d.month}_{end_d.year}{end_d.month}.nc",
        )
    )
    # remove temp file
    os.remove(
        os.path.join(
            save_path, f"Met_{site}_{network}_{start_d.year}{start_d.month}_{end_d.year}{end_d.month}_temp.nc"
        )
    )

    return METData(data=dataset, metadata=metadata)

    # Retrieve metadata from Copernicus about the dataset, this includes
    # the location of the data netCDF file.
    # result = cds_client.retrieve(dataset_name, request, filepath)

    # raise NotImplementedError("The met retrieval module needs updating and doesn't currently work.")
    # from openghg.dataobjects import METData

    # # TODO - we might need to customise this further in the future to
    # # request other types of weather data
    # request = {
    #     "product_type": "reanalysis",
    #     "format": "netcdf",
    #     "variable": variables,
    #     "pressure_level": ecmwf_pressure_levels,
    #     "year": [str(x) for x in years],
    #     "month": [str(x).zfill(2) for x in range(1, 13)],
    #     "day": [str(x).zfill(2) for x in range(1, 32)],
    #     "time": [f"{str(x).zfill(2)}:00" for x in range(0, 24)],
    #     "area": ecmwf_area,
    # }

    # cds_client = cdsapi.Client()
    # dataset_name = "reanalysis-era5-pressure-levels"

    # # Retrieve metadata from Copernicus about the dataset, this includes
    # # the location of the data netCDF file.
    # result = cds_client.retrieve(name=dataset_name, request=request)

    # # Download the data itself
    # dataset = _download_data(url=result.location)
    # # dataset = xr.open_dataset("/home/gar/Documents/Devel/RSE/openghg/tests/data/request_return.nc")

    # # We replace the date data with a start and end date here
    # start_date = str(timestamp_tzaware(f"{years[0]}-1-1"))
    # end_date = str(timestamp_tzaware(f"{years[-1]}-12-31"))

    # metadata = {
    #     "product_type": request["product_type"],
    #     "format": request["format"],
    #     "variable": request["variable"],
    #     "pressure_level": request["pressure_level"],
    #     "area": request["area"],
    #     "site": site,
    #     "network": network,
    #     "start_date": start_date,
    #     "end_date": end_date,
    # }

    # return METData(data=dataset, metadata=metadata)


# def _download_data(url: str) -> Dataset:
#     """Retrieve data from the passed URL. This is used to retrieve data from
#     the Copernicus data store.

#     Args:
#         url: URL string
#     Returns:
#         xarray.Dataset: NetCDF data retrieved
#     """
#     # If we get any of these codes we'll try again
#     retriable_status_codes = [
#         requests.codes.internal_server_error,
#         requests.codes.bad_gateway,
#         requests.codes.service_unavailable,
#         requests.codes.gateway_timeout,
#         requests.codes.too_many_requests,
#         requests.codes.request_timeout,
#     ]

#     timeout = 20  # seconds

#     retry_strategy = Retry(
#         total=3,
#         status_forcelist=retriable_status_codes,
#         allowed_methods=["HEAD", "GET", "OPTIONS"],
#         backoff_factor=1,
#     )  # type: ignore

#     adapter = HTTPAdapter(max_retries=retry_strategy)

#     http = requests.Session()
#     http.mount("https://", adapter)
#     http.mount("http://", adapter)

#     data = http.get(url, timeout=timeout).content

#     try:
#         dataset: Dataset = open_dataset(data)
#     except ValueError:
#         raise ValueError("Invalid data returned, cannot create dataset.")

#     return dataset


def _two_closest_values(diff: np.ndarray) -> np.ndarray:
    """Get location of two closest values in an array of differences.

    Args:
        diff: Numpy array of values
    Returns:
        np.ndarry: Numpy array of two closes values
    """
    closest_values: np.ndarray = np.argpartition(np.abs(diff), 2)[:2]
    return closest_values


def get_site_data(site: str, network: str) -> Tuple[float, float, float, List]:
    """Extract site location data from site attributes file.

    Args:
        site: Site code
    Returns:
        dict: Dictionary of site data
    """
    from openghg.util import load_json
    from openghg_defs import site_info_file

    network = network.upper()
    site = site.upper()
    site_info = load_json(site_info_file)

    try:
        site_data = site_info[site][network]
        latitude = float(site_data["latitude"])
        longitute = float(site_data["longitude"])
        site_height = float(site_data["height_station_masl"])
        inlet_heights = site_data["height_name"]
    except KeyError as e:
        raise KeyError(f"Incorrect site or network : {e}")

    return latitude, longitute, site_height, inlet_heights


def _get_ecmwf_area(site_lat: float, site_long: float) -> List:
    """Find out the area required from ERA5.

    Args:
        site_lat: Latitude of site
        site_long: Site longitude
    Returns:
        list: List of min/max lat long values
    """
    ecwmf_lat = np.arange(-90, 90.25, 0.25)
    ecwmf_lon = np.arange(-180, 180.25, 0.25)

    ecwmf_lat_indices = _two_closest_values(ecwmf_lat - site_lat)
    ecwmf_lon_indices = _two_closest_values(ecwmf_lon - site_long)

    return [
        np.max(ecwmf_lat[ecwmf_lat_indices]),
        np.min(ecwmf_lon[ecwmf_lon_indices]),
        np.min(ecwmf_lat[ecwmf_lat_indices]),
        np.max(ecwmf_lon[ecwmf_lon_indices]),
    ]


def get_site_pressure(inlet_heights: List, site_height: float) -> List[float]:
    """Calculate the pressure levels required

    Args:
        inlet_height: Height(s) of inlets
        site_height: Height of site
    Returns:
        list: List of pressures
    """
    import re

    if not isinstance(inlet_heights, list):
        inlet_heights = [inlet_heights]

    measured_pressure = []
    for h in inlet_heights:
        try:
            # Extract the number from the inlet height str using regex
            inlet = float(re.findall(r"\d+(?:\.\d+)?", h)[0])
            measurement_height = inlet + float(site_height)
            # Calculate the pressure
            pressure = 1000 * np.exp((-1 * measurement_height) / 7640)
            measured_pressure.append(pressure)
        except IndexError:
            pass

    return measured_pressure


def _altitude_to_ecmwf_pressure(measure_pressure: List[float]) -> List[str]:
    """Find out what pressure levels are required from ERA5.

    Args:
        measure_pressure: List of pressures
    Returns:
        list: List of desired pressures
    """
    from openghg.util import load_internal_json

    ecmwf_metadata = load_internal_json(filename="ecmwf_dataset_info.json")
    dataset_metadata = ecmwf_metadata["datasets"]
    valid_levels = dataset_metadata["reanalysis_era5_pressure_levels"]["valid_levels"]

    # Available ERA5 pressure levels
    era5_pressure_levels = np.array(valid_levels)

    # Match pressure to ERA5 pressure levels
    ecwmf_pressure_indices = np.zeros(len(measure_pressure) * 2)

    for index, m in enumerate(measure_pressure):
        ecwmf_pressure_indices[(index * 2) : (index * 2 + 2)] = _two_closest_values(m - era5_pressure_levels)

    desired_era5_pressure = era5_pressure_levels[np.unique(ecwmf_pressure_indices).astype(int)]

    pressure_levels: List = desired_era5_pressure.astype(str).tolist()

    return pressure_levels
