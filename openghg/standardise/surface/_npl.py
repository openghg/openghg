from pathlib import Path

from openghg.standardise.meta import assign_attributes, dataset_formatter
from openghg.types import pathType
from openghg.util import clean_string, load_internal_json
from pandas import read_csv


def parse_npl(
    filepath: pathType,
    site: str = "NPL",
    network: str = "LGHG",
    inlet: str | None = None,
    instrument: str | None = None,
    sampling_period: str | None = None,
    measurement_type: str | None = None,
    update_mismatch: str = "never",
) -> dict:
    """Reads NPL data files and returns the UUIDS of the Datasources
    the processed data has been assigned to

    Args:
        filepath: Path of file to load
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
    from openghg.util import format_inlet, get_site_info, synonyms

    if sampling_period is None:
        sampling_period = "NOT_SET"

    filepath = Path(filepath)

    site_upper = site.upper()
    network_upper = network.upper()

    attributes_data = load_internal_json(filename="attributes.json")
    npl_params = attributes_data[site_upper]

    site_data = get_site_info()
    site_info = site_data[site_upper][network_upper]

    data = read_csv(filepath, parse_dates={"time": [0]}, index_col=0, date_format="%d/%m/%Y %H:%M")

    # Drop the NaT/NaNs
    data = data.loc[data.index.dropna()]

    # Rename columns
    rename_dict = {"Cal_CO2_dry": "CO2", "Cal_CH4_dry": "CH4"}

    data = data.rename(columns=rename_dict)

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
        processed_data[f"{species} variability"] = processed_data[species_column] * 0.0

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

    gas_data = dataset_formatter(data=gas_data)

    gas_data = assign_attributes(data=gas_data, site=site, network=network, update_mismatch=update_mismatch)

    return gas_data
