from __future__ import annotations

import os
import cdsapi  # type: ignore
import numpy as np
from typing import List, Tuple
from openghg.util import get_site_info  # , timestamp_tzaware
import pathlib

import logging

# import requests
# from xarray import open_dataset, Dataset
# from requests.adapters import HTTPAdapter
# from requests.packages.urllib3.util.retry import Retry
# from openghg.util import timestamp_tzaware

# if TYPE_CHECKING:
#     from openghg.dataobjects import METData

logger = logging.getLogger("openghg.store")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


__all__ = ["pull_met", "check_cds_access"]


def check_cds_access() -> None:
    print(
        """To access data through the Copernicus API:
          (instructions: Follow the instructions here https://cds.climate.copernicus.eu/how-to-api)
          1. Register/log-in to Copernicus
          2. from your profile, copy the url and key
          3. copy them into a file on /user/home/ab12345/.cdsapi
          4. ensure the cdsapi library is installed
          """
    )
    try:
        _ = cdsapi.Client()
    except Exception as e:
        print("there was an error retrieving the client. check out the instructions")
        print(f"error: {e}")
    else:
        print("your client loaded successfully!")


def pull_met(
    site: str,
    network: str,
    years: str | list[str],
    variables: list[str] | None = None,
    save_path: str | None = None,
) -> List:
    """Pull METData data and store on disc. Note that this function will only download a
    full year of data which may take some time.

    This function currently on retrieves data from the "reanalysis-era5-pressure-levels"
    dataset but may be modified for other datasets in the future.
    Args:
        site: Three letter sitec code
        network: Network
        years: Year(s) of data required
        variables: List of variables to download
        save_path: path to save the data to. If none, saves to $HOME/metdata
    Returns:
        list of paths of the downloaded files
    """
    # raise NotImplementedError("The met retrieval module needs updating and doesn't currently work.")
    # from openghg.dataobjects import METData

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

        assert np.all(
            [var in valid_variables for var in variables]
        ), f"""One of more of the variables passed does not exist in ERA5 data. \
        The problematic variables are {set(variables) - set(valid_variables)}"""

    latitude, longitute, site_height, inlet_heights = _get_site_data(site, network)

    # Get the area to retrieve data for
    ecmwf_area = _get_ecmwf_area(site_lat=latitude, site_long=longitute)
    # Calculate the pressure at measurement height(s)
    measure_pressure = _get_site_pressure(inlet_heights=inlet_heights, site_height=site_height)
    # Calculate the ERA5 pressure levels required
    ecmwf_pressure_levels = _altitude_to_ecmwf_pressure(measure_pressure=measure_pressure)

    if not isinstance(years, list):
        years = [years]
    else:
        years = sorted(years)

    default_save_path = os.path.join(pathlib.Path.home(), "met_data")
    if save_path is None:
        save_path = default_save_path
        os.makedirs(save_path, exist_ok=True)
    else:
        assert os.path.isdir(
            save_path
        ), f"The save path {save_path} is not a directory. Please create it or pass a different save_path"

    dataset_savepaths = []

    # TODO - we might need to customise this further in the future to
    # request other types of weather data
    for year in years:
        for month_int in range(1, 13):
            month = str(month_int).zfill(2)
            request = {
                "product_type": "reanalysis",
                "format": "netcdf",
                "variable": variables,
                "pressure_level": ecmwf_pressure_levels,
                "year": str(year),
                "month": month,
                "day": [str(x).zfill(2) for x in range(1, 32)],
                "time": [f"{str(x).zfill(2)}:00" for x in range(0, 24)][::3],
                "area": ecmwf_area,
            }

            cds_client = cdsapi.Client()
            dataset_name = "reanalysis-era5-pressure-levels"

            # Retrieve metadata from Copernicus about the dataset, this includes
            # the location of the data netCDF file.

            dataset_savepath = os.path.join(
                save_path,
                f"Met_{site}_{network}_{month}{year}.nc",
            )

            dataset_savepaths.append(dataset_savepath)

            logger.info(f"Retrieving data for {site} and {month}/{year} to {dataset_savepath}")

            _ = cds_client.retrieve(name=dataset_name, request=request, target=dataset_savepath)

    return dataset_savepaths

    # We replace the date data with a start and end date here
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


def _two_closest_values(diff: np.ndarray) -> np.ndarray:
    """Get location of two closest values in an array of differences.

    Args:
        diff: Numpy array of values
    Returns:
        np.ndarry: Numpy array of two closes values
    """
    closest_values: np.ndarray = np.argpartition(np.abs(diff), 2)[:2]
    return closest_values


def _get_site_data(site: str, network: str) -> Tuple[float, float, float, List]:
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


def _get_site_pressure(inlet_heights: List, site_height: float) -> List[float]:
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

    ecwmf_info_file = "ecmwf_dataset_info.json"
    ecmwf_metadata = load_internal_json(ecwmf_info_file)
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
