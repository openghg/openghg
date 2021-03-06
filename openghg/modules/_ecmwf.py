import cdsapi
from dataclasses import dataclass
import numpy as np
from typing import Dict, List, Tuple, Union
import requests
import xarray as xr
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from openghg.util import timestamp_tzaware

__all__ = ["retrieve_met", "METData"]


@dataclass(frozen=True)
class METData:
    data: xr.Dataset
    metadata: Dict


# def retrieve_met(site: str, network: str, year: str) -> METData:
def retrieve_met(site: str, network: str, years: Union[str, List[str]]) -> METData:
    """ Retrieve METData data. Note that this function will only download a
        full year of data which may take some time.

        Args:
            site: Three letter sitec code
            network: Network
            years: Year(s) of data required
        Returns:
            METData: METData object holding data and metadata
    """
    latitude, longitude, inlet_height, site_height = _get_site_loc(site=site, network=network)

    # Get the area to retrieve data for
    ecmwf_area = _get_ecmwf_area(site_lat=latitude, site_long=longitude)
    # Calculate the pressure at measurement height(s)
    measure_pressure = _get_site_pressure(inlet_height, site_height)
    # Calculate the ERA5 pressure levels required
    ecmwf_pressure_levels = _altitude_to_ecmwf_pressure(measure_pressure)

    if not isinstance(years, list):
        years = [years]
    else:
        years = sorted(years)

    # TODO - we might need to customise this further in the future to
    # request other types of weather data
    request = {
        "product_type": "reanalysis",
        "format": "netcdf",
        "variable": ["u_component_of_wind", "v_component_of_wind"],
        "pressure_level": ecmwf_pressure_levels,
        "year": [str(x) for x in years],
        "month": [str(x).zfill(2) for x in range(1, 13)],
        "day": [str(x).zfill(2) for x in range(1, 32)],
        "time": [f"{str(x).zfill(2)}:00" for x in range(0, 24)],
        "area": ecmwf_area,
    }

    cds_client = cdsapi.Client()
    dataset_name = "reanalysis-era5-pressure-levels"

    # Retrieve metadata from Copernicus about the dataset, this includes
    # the location of the data netCDF file.
    result = cds_client.retrieve(name=dataset_name, request=request)

    # Download the data itself
    dataset = _download_data(url=result.location)
    # dataset = xr.open_dataset("/home/gar/Documents/Devel/RSE/openghg/tests/data/request_return.nc")

    # We replace the date data with a start and end date here
    start_date = str(timestamp_tzaware(f"{years[0]}-1-1"))
    end_date = str(timestamp_tzaware(f"{years[-1]}-12-31"))

    metadata = {
        "product_type": request["product_type"],
        "format": request["format"],
        "variable": request["variable"],
        "pressure_level": request["pressure_level"],
        "area": request["area"],
        "site": site,
        "network": network,
        "start_date": start_date,
        "end_date": end_date,
    }

    return METData(data=dataset, metadata=metadata)


def _download_data(url: str) -> xr.Dataset:
    """ Retrieve data from the passed URL. This is used to retrieve data from
        the Copernicus data store.

        Args:
            url: URL string
        Returns:
            xarray.Dataset: NetCDF data retrieved
    """
    # If we get any of these codes we'll try again
    retriable_status_codes = [requests.codes.internal_server_error,
                            requests.codes.bad_gateway,
                            requests.codes.service_unavailable,
                            requests.codes.gateway_timeout,
                            requests.codes.too_many_requests,
                            requests.codes.request_timeout]

    timeout = 20  # seconds

    retry_strategy = Retry(
        total=3,
        status_forcelist=retriable_status_codes,
        method_whitelist=["HEAD", "GET", "OPTIONS"],
        backoff_factor=1
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)

    http = requests.Session()
    http.mount("https://", adapter)
    http.mount("http://", adapter)

    data = http.get(url, timeout=timeout).content

    try:
        dataset = xr.open_dataset(data)
    except ValueError:
        raise ValueError("Invalid data returned, cannot create dataset.")

    return dataset


def _two_closest_values(diff: np.ndarray) -> np.ndarray:
    """ Get location of two closest values in an array of differences.

        Args:
            diff: Numpy array of values
        Returns:
            np.ndarry: Numpy array of two closes values
    """
    return np.argpartition(np.abs(diff), 2)[:2]


def _get_site_loc(site: str, network: str) -> Tuple[str]:
    """ Extract site location data from site attributes file.

        Args:
            site: Site code
        Returns:
            dict: Dictionary of site data
    """
    from openghg.util import load_json

    network = network.upper()
    site = site.upper()

    site_info = load_json("acrg_site_info.json")

    try:
        site_data = site_info[site][network]

        latitude = site_data["latitude"]
        longitute = site_data["longitude"]
        inlet_height = site_data["height_name"]
        site_height = site_data["height_station_masl"]
    except KeyError as e:
        raise KeyError(f"Incorrect site or network : {e}")

    return latitude, longitute, inlet_height, site_height


def _get_ecmwf_area(site_lat: str, site_long: str) -> List:
    """ Find out the area required from ERA5. 

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


def _get_site_pressure(inlet_height: List, site_height: float) -> List[float]:
    """ Calculate the pressure levels required

        Args:
            inlet_height: Height(s) of inlets
            site_height: Height of site
        Returns:
            list: List of pressures
    """
    import re

    measured_pressure = []
    for h in inlet_height:
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


def _altitude_to_ecmwf_pressure(measure_pressure: np.ndarray) -> List:
    """ Find out what pressure levels are required from ERA5. 

        Args:
            measure_pressure: List of pressures
        Returns:
            list: List of desired pressures
    """
    # Available ERA5 pressure levels
    era5_pressure_levels = np.array([1, 2, 3, 5, 7, 10, 20, 30, 50, 70, 100, 125,
                                    150, 175, 200, 225, 250, 300, 350, 400, 450,
                                    500, 550, 600, 650, 700, 750, 775, 800, 825,
                                    850, 875, 900, 925, 950, 975, 1000])

    # Match pressure to ERA5 pressure levels
    ecwmf_pressure_indices = np.zeros(len(measure_pressure) * 2)

    for index, m in enumerate(measure_pressure):
        ecwmf_pressure_indices[(index * 2) : (index * 2 + 2)] = _two_closest_values(m - era5_pressure_levels)

    desired_era5_pressure = era5_pressure_levels[np.unique(ecwmf_pressure_indices).astype(int)]

    return desired_era5_pressure.astype(str).tolist()
