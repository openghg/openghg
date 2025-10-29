import os
import cdsapi  # type: ignore
import numpy as np
from openghg.util import _get_site_data  # , timestamp_tzaware
from openghg.util import _get_ecmwf_area, _altitude_to_ecmwf_pressure, _get_site_pressure
import pathlib
import xarray as xr

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


__all__ = ["pull_site_met", "check_cds_access", "_create_dummy_dataset", "retrieve_site_met"]


def check_cds_access() -> None:
    """
    Print instructions to access the Copernicus Data Store API and check that the user has access.
    (could be moved to utils._met.py?)
    """
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


def retrieve_site_met(
    site: str,
    network: str,
    years: str | list[str],
    months: str | list[str] | None = None,
    variables: list[str] | None = None,
    local_save_path: str | None = None,
    store: str | None = None,
) -> None:
    """
    Retrieve and store Met data from Copernicus Climate Data Store.

    See `pull_site_met` function for more details on the data.
    ...
    """
    from openghg.standardise import standardise_site_met

    all_months = [str(x).zfill(2) for x in range(1, 13)]
    if months is not None:
        if isinstance(months, str):
            months = [months]

        # check that all months are valid
        assert np.all(
            [month in all_months for month in months]
        ), "One of more of the months passed does not exist - pass them in format 'MM' eg '08' or '10' "

    filepaths = pull_site_met(
        site=site, network=network, years=years, months=months, variables=variables, save_path=local_save_path
    )

    met_source = "ecmwf"

    for filepath in filepaths:
        standardise_site_met(filepath, site=site, network=network, met_source=met_source, store=store)
        os.remove(filepath)  # remove the file after standardisation


def pull_site_met(
    site: str,
    network: str,
    years: str | list[str],
    months: str | list[str] | None = None,
    height: str | None = None,
    variables: list[str] | None = None,
    save_path: str | None = None,
    print_requests: bool = False,
) -> list:
    """Pull METData data and store on disc from ECMWF via the Copernicus Data Store API.
    This function currently on retrieves data from the "reanalysis-era5-pressure-levels"
    dataset but may be modified for other datasets in the future.
    Data is saved to save_path/Met_{site}_{inlet}_{network}_{year}{month}.nc by default.
    Args:
        site: Three letter sitec code
        network: Network
        years: Year(s) of data required
        months: Month(s) of data required. If None, all months in year(s) are downloaded.
        height: measurement inlet height (e.g. "10m"). If none, extracts meteorology for all heights at site and network.
        variables: List of variables to download
        save_path: path to save the data to. If none, saves to $HOME/metdata
    Returns:
        list of paths of the downloaded files
    """
    # add option to pass site height!
    # Note: passing month could lead to issues if downloading and standardising non-sequential months?

    # from openghg.dataobjects import METData
    default_variables = [
        "temperature",
        "relative_humidity",
        "specific_humidity",
        "u_component_of_wind",
        "v_component_of_wind",
        "vertical_velocity",
        "vorticity",
    ]

    if variables is None:
        variables = default_variables.copy()
    else:
        # print("Being able to extract variables is currently not implementing. Downloading default variables")
        variables = default_variables.copy()

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

        # the u- and v- component of wind are always downloaded
        if "u_component_of_wind" not in variables:
            variables.append("u_component_of_wind")
        if "v_component_of_wind" not in variables:
            variables.append("v_component_of_wind")

    latitude, longitude, site_height, inlet_heights = _get_site_data(site, network)

    if height is not None:
        if height in inlet_heights:
            inlet_heights = [height]
        else:
            raise ValueError(
                f"The height {height} is not one of the inlets for site {site} in network {network} (valid inlets are {inlet_heights})"
            )
    # Get the area to retrieve data for
    ecmwf_area = _get_ecmwf_area(site_lat=latitude, site_long=longitude)
    # Calculate the pressure at measurement height(s)

    # Note: need to test that this works fine for sites with multiple inlets!
    measure_pressure = _get_site_pressure(inlet_heights=inlet_heights, site_height=site_height)
    # Calculate the ERA5 pressure levels required
    ecmwf_pressure_levels = _altitude_to_ecmwf_pressure(measure_pressure=measure_pressure)
    formatted_pressure_levels = [str(x) for x in ecmwf_pressure_levels]

    if not isinstance(years, list):
        years = [years]
    else:
        years = sorted(years)

    default_save_path = os.path.join(os.getcwd().split("openghg")[0], "openghg/metdata")
    save_path = default_save_path if save_path is None else save_path
    assert os.path.isdir(
        save_path
    ), f"The save path {save_path} is not a directory. Please create it or pass a different save_path"

    dataset_savepaths: list[str] = []
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
    all_months = [str(x).zfill(2) for x in range(1, 13)]
    if months is None:
        months = all_months
    else:
        if isinstance(months, str):
            months = [months]

        assert np.all(
            [month in all_months for month in months]
        ), "One of more of the months passed does not exist - pass them in format 'MM' eg '08' or '10' "

    for year in years:
        for month in months:
            request = {
                "product_type": "reanalysis",
                "format": "netcdf",
                "variable": variables,
                "pressure_level": formatted_pressure_levels,
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
                f"Met_{site}_{network}_{year}{month}.nc",
            )
            dataset_savepaths.append(dataset_savepath)

            logger.info(f"Retrieving data for {site} and {month}/{year} to {dataset_savepath}")
            if print_requests:
                print(f"Requesting data with the following parameters:\n{request}\n")
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


def _create_dummy_dataset(request: dict, tmpdir: str) -> xr.Dataset:
    cds_client = cdsapi.Client()
    dataset_name = "reanalysis-era5-pressure-levels"
    _ = cds_client.retrieve(name=dataset_name, request=request, target=tmpdir)
    dataset = xr.open_dataset(tmpdir)
    return dataset
