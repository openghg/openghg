from typing import Dict, Optional
from pathlib import Path
from openghg.types import pathType


def parse_tmb(
    data_filepath: pathType,
    site: str = "TMB",
    network: Optional[str] = "LGHG",
    inlet: Optional[str] = None,
    instrument: Optional[str] = None,
    sampling_period: Optional[str] = None,
    measurement_type: Optional[str] = None,
) -> Dict:
    """Reads THAMESBARRIER data files and returns the UUIDS of the Datasources
    the processed data has been assigned to

    Args:
        data_filepath: Path of file to load
        site: Site name
    Returns:
        list: UUIDs of Datasources data has been assigned to
    """
    from openghg.standardise.meta import assign_attributes
    from pandas import read_csv as pd_read_csv
    from openghg.util import clean_string, load_json

    if sampling_period is None:
        sampling_period = "NOT_SET"

    data_filepath = Path(data_filepath)

    data = pd_read_csv(data_filepath, parse_dates=[0], infer_datetime_format=True, index_col=0)
    # Drop NaNs from the data
    data = data.dropna(axis="rows", how="all")
    # Drop a column if it's all NaNs
    data = data.dropna(axis="columns", how="all")

    rename_dict = {}
    if "Methane" in data.columns:
        rename_dict["Methane"] = "CH4"

    data = data.rename(columns=rename_dict)
    data.index.name = "time"

    tb_params = load_json(filename="attributes.json")["TMB"]

    gas_data = {}

    for species in data.columns:
        processed_data = data.loc[:, [species]].sort_index().to_xarray()

        # Convert methane to ppb
        if species == "CH4":
            processed_data[species] *= 1000

        # No averaging applied to raw obs, set variability to 0 to allow get_obs to calculate
        # when averaging
        processed_data["{} variability".format(species)] = processed_data[species] * 0.0

        site_attributes = tb_params["global_attributes"]
        site_attributes["inlet_height_magl"] = clean_string(tb_params["inlet"])
        site_attributes["instrument"] = clean_string(tb_params["instrument"])
        # site_attributes["inlet"] = clean_string(tb_params["inlet"])
        # site_attributes["unit_species"] = tb_params["unit_species"]
        # site_attributes["calibration_scale"] = tb_params["scale"]

        # All attributes stored in the metadata?
        metadata = {
            "species": clean_string(species),
            "site": site,
            "inlet": clean_string(tb_params["inlet"]),
            "network": "LGHG",
            "sampling_period": sampling_period,
        }
        metadata.update(site_attributes)

        gas_data[species] = {
            "metadata": metadata,
            "data": processed_data,
            "attributes": site_attributes,
        }

    gas_data = assign_attributes(data=gas_data, site=site)

    return gas_data
