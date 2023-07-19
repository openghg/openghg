from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

from openghg.standardise.meta import assign_attributes
from openghg.types import pathType
from openghg.util import clean_string, load_internal_json
from pandas import NaT, read_csv


def parse_npl(
    data_filepath: pathType,
    site: str = "NPL",
    network: str = "LGHG",
    inlet: Optional[str] = None,
    instrument: Optional[str] = None,
    sampling_period: Optional[str] = None,
    measurement_type: Optional[str] = None,
    update_mismatch: str = "never",
) -> Dict:
    """Reads NPL data files and returns the UUIDS of the Datasources
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
    Returns:
        list: UUIDs of Datasources data has been assigned to
    """
    from openghg.util import get_site_info, format_inlet, synonyms

    if sampling_period is None:
        sampling_period = "NOT_SET"

    data_filepath = Path(data_filepath)

    site_upper = site.upper()
    network_upper = network.upper()

    attributes_data = load_internal_json(filename="attributes.json")
    npl_params = attributes_data[site_upper]

    site_data = get_site_info()
    site_info = site_data[site_upper][network_upper]

    # mypy doesn't like NaT or NaNs - look into this
    def parser(date: str):  # type: ignore
        try:
            return datetime.strptime(str(date), "%d/%m/%Y %H:%M")
        except ValueError:
            return NaT

    data = read_csv(data_filepath, index_col=0, date_parser=parser)

    # Drop the NaT/NaNs
    data = data.loc[data.index.dropna()]

    # Rename columns
    rename_dict = {"Cal_CO2_dry": "CO2", "Cal_CH4_dry": "CH4"}

    data = data.rename(columns=rename_dict)
    data.index.name = "time"

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

        site_attributes = npl_params["global_attributes"]
        site_attributes["inlet"] = inlet
        site_attributes["inlet_height_magl"] = format_inlet(inlet, key_name="inlet_height_magl")
        site_attributes["instrument"] = npl_params["instrument"]

        attributes = site_attributes
        attributes["species"] = species

        metadata = {
            "species": species,
            "sampling_period": str(sampling_period),
            "site": "NPL",
            "network": "LGHG",
            "inlet": inlet,
            "data_type": "surface",
            "source_format": "npl",
        }

        # TODO - add in better metadata reading
        gas_data[species] = {
            "metadata": metadata,
            "data": processed_data,
            "attributes": attributes,
        }

    gas_data = assign_attributes(data=gas_data, site=site, network=network, update_mismatch=update_mismatch)

    return gas_data
