import logging
from pathlib import Path
import xarray as xr

from openghg.util import clean_string, timestamp_now, synonyms
from openghg.store import infer_date_range, update_zero_dim

logger = logging.getLogger("openghg.standardise.boundary_conditions")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


def parse_openghg(
    filepath: str | Path,
    species: str,
    bc_input: str,
    domain: str,
    period: str | None = None,
    continuous: bool = True,
    chunks: dict | None = None,
) -> dict:
    """
    Parses the boundary conditions file and adds data and metadata.
    Args:
        filepath: Path of boundary conditions file
        species: Species name
        bc_input: Input used to create boundary conditions. For example:
            - a model name such as "MOZART" or "CAMS"
            - a description such as "UniformAGAGE" (uniform values based on AGAGE average)
        domain: Region for boundary conditions
        chunks: Chunking schema to use when storing data. It expects a dictionary of dimension name and chunk size,
                for example {"time": 100}. If None then a chunking schema will be set automatically by OpenGHG.
                See documentation for guidance on chunking: https://docs.openghg.org/tutorials/local/Adding_data/Adding_ancillary_data.html#chunking.
                To disable chunking pass in an empty dictionary.
    Returns:
        Dict: Dictionary of "species_bc_input_domain" : data, metadata, attributes
    """
    species = clean_string(species)
    species = synonyms(species)
    bc_input = clean_string(bc_input)
    domain = clean_string(domain)

    filepath = Path(filepath)

    with xr.open_dataset(filepath).chunk(chunks) as bc_data:
        # Some attributes are numpy types we can't serialise to JSON so convert them
        # to their native types here
        attrs = {}
        for key, value in bc_data.attrs.items():
            try:
                attrs[key] = value.item()
            except AttributeError:
                attrs[key] = value

        author_name = "OpenGHG Cloud"
        bc_data.attrs["author"] = author_name

        metadata = {}
        metadata.update(attrs)

        metadata["species"] = species
        metadata["domain"] = domain
        metadata["bc_input"] = bc_input
        metadata["author"] = author_name
        metadata["processed"] = str(timestamp_now())

        # Check if time has 0-dimensions and, if so, expand this so time is 1D
        if "time" in bc_data.coords:
            bc_data = update_zero_dim(bc_data, dim="time")

        bc_time = bc_data["time"]

        start_date, end_date, period_str = infer_date_range(
            bc_time, filepath=filepath, period=period, continuous=continuous
        )

        metadata["start_date"] = str(start_date)
        metadata["end_date"] = str(end_date)

        metadata["max_longitude"] = round(float(bc_data["lon"].max()), 5)
        metadata["min_longitude"] = round(float(bc_data["lon"].min()), 5)
        metadata["max_latitude"] = round(float(bc_data["lat"].max()), 5)
        metadata["min_latitude"] = round(float(bc_data["lat"].min()), 5)
        metadata["min_height"] = round(float(bc_data["height"].min()), 5)
        metadata["max_height"] = round(float(bc_data["height"].max()), 5)
        metadata["input_filename"] = filepath.name

        metadata["time_period"] = period_str

        key = "_".join((species, bc_input, domain))

        boundary_conditions_data: dict[str, dict] = {key: {}}
        boundary_conditions_data[key]["data"] = bc_data
        boundary_conditions_data[key]["metadata"] = metadata

        return boundary_conditions_data
