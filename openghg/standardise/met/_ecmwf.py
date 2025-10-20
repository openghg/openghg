import logging
from pathlib import Path
import xarray as xr

from openghg.types import MetadataAndData
from openghg.util import timestamp_now


logger = logging.getLogger("openghg.standardise.met")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


def parse_ecmwf(filepath: str | Path, chunks: dict | None = None) -> list[MetadataAndData]:
    """
    Parse Met ECMWF data (typically downloaded from the Copernicus Climate Data Store).

    Args:
        filepath: Single filepath to netcdf file
        chunks: Chunks to use when opening and storing data
    Returns:   
        list[MetadataAndData]: List of parsed data objects
    """

    with xr.open_dataset(filepath).chunk(chunks) as data:

        # TODO: Decide what internal variable names we want
        rename_coords = {"valid_time": "time", "latitude": "lat", "longitude": "lon"}

        data = data.rename(rename_coords)

        metadata = {"author": "OpenGHG Cloud", "processed": str(timestamp_now())}

        parsed_data = [MetadataAndData(metadata=metadata, data=data)]

        return parsed_data
