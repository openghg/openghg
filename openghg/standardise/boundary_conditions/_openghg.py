import logging
from pathlib import Path
import xarray as xr

from openghg.util import clean_string, timestamp_now, synonyms, get_data
from openghg.store import infer_date_range, update_zero_dim
from openghg.standardise.boundary_conditions._units import normalise_boundary_condition_units

logger = logging.getLogger("openghg.standardise.boundary_conditions")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


def parse_openghg(
    filepath: str | Path | list[str] | list[Path] | None = None,
    species: str | None = None,
    bc_input: str | None = None,
    domain: str | None = None,
    period: str | None = None,
    continuous: bool = True,
    chunks: dict | None = None,
    data: xr.Dataset | None = None,
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
    if species is None or bc_input is None or domain is None:
        raise ValueError("`species`, `bc_input`, and `domain` must be specified.")

    species = clean_string(species)
    species = synonyms(species)
    bc_input = clean_string(bc_input)
    domain = clean_string(domain)

    if isinstance(filepath, list) and len(filepath) == 1:
        filepath = filepath[0]

    with get_data(
        dataset=data,
        filepath=filepath,
        realign_on_domain=domain,
        check_coords="time",
    ) as bc_data:
        bc_data = bc_data.chunk(chunks if chunks is not None else {})
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
        normalise_boundary_condition_units(bc_data, vmr_units=bc_data.attrs.get("units", "mol/mol"))

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

        # If filepath is a single file, the naming scheme of this file can be used
        # as one factor to try and determine the period.
        # If multiple files, this input isn't needed.
        if isinstance(filepath, (str, Path)):
            input_filepath = filepath
        else:
            input_filepath = None

        start_date, end_date, period_str = infer_date_range(
            bc_time, filepath=input_filepath, period=period, continuous=continuous
        )

        metadata["start_date"] = str(start_date)
        metadata["end_date"] = str(end_date)

        metadata["max_longitude"] = round(float(bc_data["lon"].max()), 5)
        metadata["min_longitude"] = round(float(bc_data["lon"].min()), 5)
        metadata["max_latitude"] = round(float(bc_data["lat"].max()), 5)
        metadata["min_latitude"] = round(float(bc_data["lat"].min()), 5)
        metadata["min_height"] = round(float(bc_data["height"].min()), 5)
        metadata["max_height"] = round(float(bc_data["height"].max()), 5)

        metadata["time_period"] = period_str

        key = "_".join((species, bc_input, domain))

        boundary_conditions_data: dict[str, dict] = {key: {}}
        boundary_conditions_data[key]["data"] = bc_data
        boundary_conditions_data[key]["metadata"] = metadata

        return boundary_conditions_data
