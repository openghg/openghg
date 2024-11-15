import logging
from pathlib import Path
from typing import Dict, Optional, Union
import xarray as xr

from openghg.standardise.meta import dataset_formatter
from openghg.util import clean_string, timestamp_now, synonyms
from openghg.store import update_zero_dim

logger = logging.getLogger("openghg.standardise.boundary_conditions")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


def parse_boundary_conditions(
    filepath: Union[str, Path],
    species: str,
    bc_input: str,
    domain: str,
    source_format: str = "boundary_conditions",
    chunks: Optional[Dict] = None,
):
    species = clean_string(species)
    species = synonyms(species)
    bc_input = clean_string(bc_input)
    domain = clean_string(domain)

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

            key = "_".join((species, bc_input, domain))

        boundary_conditions_data: dict[str, dict] = {}
        boundary_conditions_data[key] = {}
        boundary_conditions_data[key]["data"] = bc_data
        boundary_conditions_data[key]["metadata"] = metadata
        return boundary_conditions_data
