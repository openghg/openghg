from pathlib import Path
from typing import Dict, Optional

from openghg.types import pathType, optionalPathType


def parse_tmb(
    data_filepath: pathType,
    site: str = "TMB",
    network: str = "LGHG",
    inlet: Optional[str] = None,
    instrument: Optional[str] = None,
    sampling_period: Optional[str] = None,
    measurement_type: Optional[str] = None,
    update_mismatch: str = "never",
    site_filepath: optionalPathType = None,
    **kwargs: Dict,
) -> Dict:
    """Reads THAMESBARRIER data files and returns the UUIDS of the Datasources
    the processed data has been assigned to

    Args:
        data_filepath: Path of file to load
        site: Site name
        network: Network, defaults to LGHG
        inlet: Inlet height. Will be inferred if not specified
        instrument: Instrument name
        sampling_period: Sampling period
        measurement_type: Type of measurement taken e.g."flask", "insitu"
        update_mismatch: This determines how mismatches between the internal data
            "attributes" and the supplied / derived "metadata" are handled.
            This includes the options:
              - "never" - don't update mismatches and raise an AttrMismatchError
              - "from_source" / "attributes" - update mismatches based on input data (e.g. data attributes)
              - "from_definition" / "metadata" - update mismatches based on associated data (e.g. site_info.json)
        site_filepath: Alternative site info file (see openghg/openghg_defs repository for format).
            Otherwise will use the data stored within openghg_defs/data/site_info JSON file by default.
    Returns:
        list: UUIDs of Datasources data has been assigned to
    """
    from openghg.standardise.meta import assign_attributes
    from openghg.util import (
        clean_string,
        get_site_info,
        format_inlet,
        synonyms,
        load_internal_json,
    )
    from pandas import read_csv as pd_read_csv

    if sampling_period is None:
        sampling_period = "NOT_SET"

    data_filepath = Path(data_filepath)

    data = pd_read_csv(data_filepath, parse_dates={"time": [0]}, index_col=0)
    # Drop NaNs from the data
    data = data.dropna(axis="rows", how="all")
    # Drop a column if it's all NaNs
    data = data.dropna(axis="columns", how="all")

    rename_dict = {}
    if "Methane" in data.columns:
        rename_dict["Methane"] = "CH4"

    data = data.rename(columns=rename_dict)

    site_upper = site.upper()
    network_upper = network.upper()

    attributes_data = load_internal_json(filename="attributes.json")
    tb_params = attributes_data[site_upper]

    site_data = get_site_info()
    site_info = site_data[site_upper][network_upper]

    try:
        site_inlet_values = site_info["height"]
    except KeyError:
        raise ValueError(
            f"Unable to extract inlet height details for site '{site}'. Please input inlet value."
        )

    inlet = format_inlet(inlet)
    if inlet is None:
        inlet = site_inlet_values[0]  # Use first entry
        inlet = format_inlet(inlet)
    elif inlet not in site_inlet_values:
        print(f"WARNING: inlet value of '{inlet}' does not match to known inlet values")

    gas_data = {}

    for species_column in data.columns:
        processed_data = data.loc[:, [species_column]].sort_index().to_xarray()

        # Convert methane to ppb
        if species_column == "CH4":
            processed_data[species_column] *= 1000

        species = clean_string(species_column)
        species = synonyms(species, allow_new_species=True)

        # No averaging applied to raw obs, set variability to 0 to allow get_obs to calculate
        # when averaging
        processed_data["{} variability".format(species)] = processed_data[species_column] * 0.0

        site_attributes = tb_params["global_attributes"]
        site_attributes["inlet"] = inlet
        site_attributes["inlet_height_magl"] = format_inlet(inlet, key_name="inlet_height_magl")
        site_attributes["instrument"] = clean_string(tb_params["instrument"])
        # site_attributes["unit_species"] = tb_params["unit_species"]
        # site_attributes["calibration_scale"] = tb_params["scale"]

        attributes = site_attributes
        attributes["species"] = species

        metadata = {
            "species": species,
            "sampling_period": str(sampling_period),
            "site": site,
            "network": "LGHG",
            "inlet": inlet,
            "data_type": "surface",
            "source_format": "tmb",
        }

        # TODO: All attributes stored in the metadata?
        # metadata.update(attributes)

        gas_data[species] = {
            "metadata": metadata,
            "data": processed_data,
            "attributes": attributes,
        }

    gas_data = assign_attributes(
        data=gas_data, site=site, update_mismatch=update_mismatch, site_filepath=site_filepath
    )

    return gas_data
