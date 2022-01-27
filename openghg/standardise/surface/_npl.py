from openghg.types import pathType
from typing import Dict
from pandas import read_csv, NaT
from datetime import datetime
from openghg.util import clean_string, load_json
from openghg.standardise.meta import assign_attributes
from pathlib import Path


def parse_npl(
    data_filepath: pathType,
    site: str = "NPL",
    network: str = "LGHG",
    inlet: str = None,
    instrument: str = None,
    sampling_period: str = None,
    measurement_type: str = None,
) -> Dict:
    """Reads NPL data files and returns the UUIDS of the Datasources
    the processed data has been assigned to

    Args:
        data_filepath: Path of file to load
        site: Site name
    Returns:
        list: UUIDs of Datasources data has been assigned to
    """

    if sampling_period is None:
        sampling_period = "NOT_SET"

    data_filepath = Path(data_filepath)

    site = "NPL"

    attributes_data = load_json(filename="attributes.json")
    npl_params = attributes_data["NPL"]

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

    if inlet is None:
        inlet = "NA"

    gas_data = {}
    for species in data.columns:
        processed_data = data.loc[:, [species]].sort_index().to_xarray()

        # Convert methane to ppb
        if species == "CH4":
            processed_data[species] *= 1000

        # No averaging applied to raw obs, set variability to 0 to allow get_obs to calculate
        # when averaging
        processed_data["{} variability".format(species)] = processed_data[species] * 0.0

        site_attributes = npl_params["global_attributes"]
        site_attributes["inlet_height_magl"] = npl_params["inlet"]
        site_attributes["instrument"] = npl_params["instrument"]

        metadata = {
            "species": clean_string(species),
            "sampling_period": str(sampling_period),
            "site": "NPL",
            "network": "LGHG",
            "inlet": inlet,
        }

        # TODO - add in better metadata reading
        gas_data[species] = {
            "metadata": metadata,
            "data": processed_data,
            "attributes": site_attributes,
        }

    gas_data = assign_attributes(data=gas_data, site=site, network=network)

    return gas_data
