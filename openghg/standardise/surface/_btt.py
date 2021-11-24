from typing import Dict, Optional, Union
from pathlib import Path


def parse_btt(
    data_filepath: Union[str, Path],
    site: Optional[str] = "BTT",
    network: Optional[str] = "LGHG",
    inlet: Optional[str] = None,
    instrument: Optional[str] = None,
    sampling_period: Optional[str] = None,
    measurement_type: Optional[str] = None,
) -> Dict:
    """Reads NPL data files and returns the UUIDS of the Datasources
    the processed data has been assigned to

    Args:
        data_filepath: Path of file to load
        site: Site name
    Returns:
        dict: UUIDs of Datasources data has been assigned to
    """
    from openghg.retrieve import assign_attributes
    from pandas import read_csv, Timestamp, to_timedelta, isnull
    from numpy import nan as np_nan
    from openghg.util import clean_string, load_json

    # TODO: Decide what to do about inputs which aren't use anywhere
    # at present - inlet, instrument, sampling_period, measurement_type

    data_filepath = Path(data_filepath)

    site = "BTT"

    if sampling_period is None:
        sampling_period = "NOT_SET"

    # Rename these columns
    rename_dict = {"co2.cal": "CO2", "ch4.cal.ppb": "CH4"}
    # We only want these species
    species_extract = ["CO2", "CH4"]
    # Take std-dev measurements from these columns for these species
    species_sd = {"CO2": "co2.sd.ppm", "CH4": "ch4.sd.ppb"}

    param_data = load_json(filename="attributes.json")
    network_params = param_data["BTT"]

    sampling_period = network_params["sampling_period"]
    sampling_period_seconds = str(int(sampling_period)) + "s"

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

        site_attributes = network_params["global_attributes"]
        site_attributes["inlet_height_magl"] = network_params["inlet"]
        site_attributes["instrument"] = network_params["instrument"]
        site_attributes["sampling_period"] = sampling_period

        # TODO - add in better metadata reading
        metadata = {
            "species": clean_string(species),
            "sampling_period": str(sampling_period),
        }

        gas_data[species] = {
            "metadata": metadata,
            "data": processed_data,
            "attributes": site_attributes,
        }

    gas_data = assign_attributes(data=gas_data, site=site, network=network)

    return gas_data
