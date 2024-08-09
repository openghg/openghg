from pathlib import Path
from typing import Dict, Optional, Union
import warnings


def parse_btt(
    data_filepath: Union[str, Path],
    site: Optional[str] = "BTT",
    network: Optional[str] = "LGHG",
    inlet: Optional[str] = None,
    instrument: Optional[str] = None,
    update_mismatch: str = "never",
) -> Dict:
    """Reads NPL data files and returns the UUIDS of the Datasources
    the processed data has been assigned to

    Args:
        data_filepath: Path of file to load
        site: Site name
        update_mismatch: This determines how mismatches between the internal data
            "attributes" and the supplied / derived "metadata" are handled.
            This includes the options:
              - "never" - don't update mismatches and raise an AttrMismatchError
              - "from_source" / "attributes" - update mismatches based on input data (e.g. data attributes)
              - "from_definition" / "metadata" - update mismatches based on associated data (e.g. site_info.json)
    Returns:
        dict: Dictionary of gas data
    """
    from numpy import nan as np_nan
    from openghg.standardise.meta import assign_attributes
    from openghg.util import clean_string, load_internal_json, get_site_info, format_inlet
    from pandas import Timestamp, isnull, read_csv, to_timedelta

    # TODO: Decide what to do about inputs which aren't use anywhere
    # at present - inlet, instrument, sampling_period, measurement_type

    data_filepath = Path(data_filepath)

    warnings.warn("This function will be removed in a future release", DeprecationWarning)
    site = "BTT"

    # Rename these columns
    rename_dict = {"co2.cal": "CO2", "ch4.cal.ppb": "CH4"}
    # We only want these species
    species_extract = ["CO2", "CH4"]
    # Take std-dev measurements from these columns for these species
    species_sd = {"CO2": "co2.sd.ppm", "CH4": "ch4.sd.ppb"}

    site_data = get_site_info()
    site_info = site_data[site][network]

    param_data = load_internal_json(filename="attributes.json")
    network_params = param_data["BTT"]
    site_attributes = network_params["global_attributes"]

    sampling_period = int(network_params["sampling_period"])
    sampling_period_seconds = str(sampling_period) + "s"

    metadata = {}
    metadata["site"] = site
    metadata["inlet"] = format_inlet(network_params["inlet"], key_name="inlet")
    metadata["instrument"] = network_params["instrument"]
    metadata["sampling_period"] = str(sampling_period)
    metadata["station_longitude"] = site_info["longitude"]
    metadata["station_latitude"] = site_info["latitude"]
    metadata["station_long_name"] = site_info["long_name"]
    metadata["data_type"] = "surface"
    metadata["source_format"] = "btt"

    attributes = network_params["global_attributes"]
    attributes["inlet_height_magl"] = format_inlet(network_params["inlet"], key_name="inlet_height_magl")
    attributes.update(metadata)

    data = read_csv(data_filepath)
    data["time"] = Timestamp("2019-01-01 00:00") + to_timedelta(data["DOY"] - 1, unit="D")
    data["time"] = data["time"].dt.round(sampling_period_seconds)
    data = data[~isnull(data.time)]

    data = data.rename(columns=rename_dict)
    data = data.set_index("time")

    gas_data = {}
    for species in species_extract:
        processed_data = data.loc[:, [species]].sort_index()
        # Create a variability column
        species_stddev_label = species_sd[species]
        processed_data[species][f"{species} variability"] = data[species_stddev_label]

        # Replace any values below zero with NaNs
        processed_data[processed_data < 0] = np_nan
        # Drop NaNs
        processed_data = processed_data.dropna()
        # Convert to a Dataset
        processed_data = processed_data.to_xarray()

        species_attributes = attributes.copy()
        species_attributes["species"] = clean_string(species)

        species_metadata = metadata.copy()
        species_metadata["species"] = clean_string(species)

        gas_data[species] = {
            "metadata": species_metadata,
            "data": processed_data,
            "attributes": site_attributes,
        }

    gas_data = assign_attributes(data=gas_data, site=site, network=network, update_mismatch=update_mismatch)

    return gas_data
