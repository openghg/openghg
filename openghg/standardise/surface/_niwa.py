from pathlib import Path
import logging
from typing import Optional
import xarray as xr
from openghg.standardise.meta import assign_attributes, dataset_formatter
from openghg.types import pathType
from openghg.util import load_internal_json, format_inlet, synonyms

logger = logging.getLogger("openghg.standardise.surface")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


def parse_niwa(
    filepath: pathType,
    site: Optional[str] = None,
    inlet: Optional[str] | None = None,
    network: str = "NIWA",
    species: str = "ch4",
    instrument: str | None = None,
    sampling_period: str | None = None,
    measurement_type: str | None = None,
    update_mismatch: str = "never",
) -> dict:
    """Reads NIWA data files and returns the UUIDS of the Datasources.
    It fetches data based on the site value as the original data has multiple station in one file itself.

    Args:
        filepath: Path of file to load
        site: Site name
        network: Network, defaults to NIWA
        inlet: Inlet height
        instrument: Instrument name
        sampling_period: Sampling period
        measurement_type: Type of measurement taken e.g."flask", "insitu"
        update_mismatch: This determines how mismatches between the internal data
            "attributes" and the supplied / derived "metadata" are handled.
            This includes the options:
              - "never" - don't update mismatches and raise an AttrMismatchError
              - "from_source" / "attributes" - update mismatches based on input data (e.g. data attributes)
              - "from_definition" / "metadata" - update mismatches based on associated data (e.g. site_info.json)
    Returns:
        dict: data, metadata and attributes keys
    """

    if sampling_period is None:
        sampling_period = "NOT_SET"

    filepath = Path(filepath)

    if site is not None:
        site_upper = site.upper()
    else:
        raise ValueError("Please specify site")
    network_upper = network.upper()

    attributes_data = load_internal_json(filename="attributes.json")["NIWA"]

    niwa_params = attributes_data

    site_data = xr.open_dataset(filepath).sel(station=site_upper)

    site_dataset = site_data.rename({"obs_time": "time"})

    inlet = format_inlet(inlet)
    species = synonyms(species=species)

    niwa_params["species"] = species

    attributes = niwa_params
    attributes["data_owner"] = attributes["global_attributes"]["data_owner"]
    attributes["data_owner_email"] = attributes["global_attributes"]["data_owner_email"]
    attributes["inlet_height_magl"] = inlet
    attributes["units"] = attributes["unit_species"][species.upper()]

    metadata = {
        "species": species,
        "sampling_period": str(sampling_period),
        "site": site_upper,
        "network": network_upper,
        "inlet": inlet,
        "data_type": "surface",
        "source_format": "niwa",
    }

    gas_data = {}

    gas_data[species] = {
        "metadata": metadata,
        "data": site_dataset,
        "attributes": attributes,
    }

    gas_data = dataset_formatter(data=gas_data)

    gas_data = assign_attributes(data=gas_data, site=site, network=network, update_mismatch=update_mismatch)

    return gas_data
