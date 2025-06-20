import logging
from pathlib import Path
import xarray as xr

from openghg.util import clean_string, timestamp_now, timestamp_tzaware
from pandas import Timestamp as pd_Timestamp

logger = logging.getLogger("openghg.standardise.eulerian_model")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


def parse_openghg(
    filepath: str | Path,
    model: str,
    species: str,
    start_date: str | None = None,
    end_date: str | None = None,
    setup: str | None = None,
    chunks: dict | None = None,
    **kwargs: str,
) -> dict:
    """Parse Eulerian model files

     Args:
         filepath: Path of Eulerian model species output
         model: Eulerian model name
         species: Species name
         start_date: Start date (inclusive) associated with model run
         end_date: End date (exclusive) associated with model run
         setup: Additional setup details for run
         chunks: Chunking schema to use when storing data. It expects a dictionary of dimension name and chunk size,
                 for example {"time": 100}. If None then a chunking schema will be set automatically by OpenGHG.
                 See documentation for guidance on chunking: https://docs.openghg.org/tutorials/local/Adding_data/Adding_ancillary_data.html#chunking.
                 To disable chunking pass in an empty dictionary.

    Returns:
         Dict : Dictionary of source_name : data, metadata, attr
    """

    filepath = Path(filepath)

    model = clean_string(model)
    species = clean_string(species)
    start_date = clean_string(start_date)
    end_date = clean_string(end_date)
    setup = clean_string(setup)

    with xr.open_dataset(filepath).chunk(chunks) as em_data:
        # Check necessary 4D coordinates are present and rename if necessary (for consistency)
        check_coords = {
            "time": ["time"],
            "lat": ["lat", "latitude"],
            "lon": ["lon", "longitude"],
            "lev": ["lev", "level", "layer", "sigma_level"],
        }
        for name, coord_options in check_coords.items():
            for coord in coord_options:
                if coord in em_data.coords:
                    break
            else:
                raise ValueError(f"Input data must contain one of '{coord_options}' co-ordinate")
            if name != coord:
                logger.info(f"Renaming co-ordinate '{coord}' to '{name}'")
                em_data = em_data.rename({coord: name})

        attrs = em_data.attrs

        # author_name = "OpenGHG Cloud"
        # em_data.attrs["author"] = author_name

        metadata = {}
        metadata.update(attrs)

        metadata["model"] = model
        metadata["species"] = species
        metadata["processed"] = str(timestamp_now())
        metadata["data_type"] = "eulerian_model"

        if start_date is None:
            if len(em_data["time"]) > 1:
                start_date = str(timestamp_tzaware(em_data.time[0].values))
            else:
                try:
                    start_date = attrs["simulation_start_date_and_time"]
                except KeyError:
                    raise Exception("Unable to derive start_date from data, please provide as an input.")
                else:
                    start_date = timestamp_tzaware(start_date)
                    start_date = str(start_date)

        if end_date is None:
            if len(em_data["time"]) > 1:
                end_date = str(timestamp_tzaware(em_data.time[-1].values))
            else:
                try:
                    end_date = attrs["simulation_end_date_and_time"]
                except KeyError:
                    raise Exception("Unable to derive `end_date` from data, please provide as an input.")
                else:
                    end_date = timestamp_tzaware(end_date)
                    end_date = str(end_date)

        date = str(pd_Timestamp(start_date).date())

        metadata["date"] = date
        metadata["start_date"] = start_date
        metadata["end_date"] = end_date

        metadata["max_longitude"] = round(float(em_data["lon"].max()), 5)
        metadata["min_longitude"] = round(float(em_data["lon"].min()), 5)
        metadata["max_latitude"] = round(float(em_data["lat"].max()), 5)
        metadata["min_latitude"] = round(float(em_data["lat"].min()), 5)

        history = metadata.get("history")
        if history is None:
            history = ""
        metadata["history"] = history + f" {str(timestamp_now())} Processed onto OpenGHG cloud"

        key = "_".join((model, species, date))

        eulerian_data: dict[str, dict] = {}
        eulerian_data[key] = {}
        eulerian_data[key]["data"] = em_data
        eulerian_data[key]["metadata"] = metadata

    return eulerian_data
