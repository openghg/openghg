import logging
from pathlib import Path
import xarray as xr

from openghg.types import MetadataAndData
from openghg.util import timestamp_now, _get_site_data
from openghg.util import _get_site_pressure


logger = logging.getLogger("openghg.standardise.met")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


def parse_ecmwf(
    filepath: str | Path, site: str, network: str, chunks: dict | None = None
) -> list[MetadataAndData]:
    """
    Parse Met ECMWF data (typically downloaded from the Copernicus Climate Data Store).

    Args:
        filepath: Single filepath to netcdf file extracted from ECMWF/API, likely using retrieve_site_met() or pull_site_met()
        chunks: Chunks to use when opening and storing data
        site: Three letter code or name for site
        network: Network name
    Returns:
        list[MetadataAndData]: List of parsed data objects
    """
    print("parsing yay")
    with xr.open_dataset(filepath).chunk(chunks) as data:

        rename_coords = {"valid_time": "time", "latitude": "lat", "longitude": "lon"}

        data = data.rename(rename_coords)

        rename_vars = {
            "t": "temperature",
            "r": "relative_humidity",
            "q": "specific_humidity",
            "u": "u_wind",
            "v": "v_wind",
            "w": "vertical_velocity",
            "vo": "vorticity",
        }

        data = data.rename(rename_vars)

        _, _, site_height, inlet_heights = _get_site_data(site, network)
        inlet_pressures = _get_site_pressure(inlet_heights=inlet_heights, site_height=site_height)
        # ecmwf_pressure_levels = _altitude_to_ecmwf_pressure(measure_pressure=measure_pressure)

        data = data.assign_coords(
            {
                "inlet_height": (("inlet_height"), inlet_heights),
                "inlet_pressure": (("inlet_height"), inlet_pressures),
            }
        )

        metadata = {
            "author": "OpenGHG Cloud",
            "site": site,
            "network": network,
            "source": "ECMWF ERA5",
            "processed": str(timestamp_now()),
        }

        parsed_data = [MetadataAndData(metadata=metadata, data=data)]

        return parsed_data
