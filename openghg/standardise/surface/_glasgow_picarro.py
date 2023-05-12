from pathlib import Path
from typing import Dict, Optional, Union
from warnings import warn

import pandas as pd
from addict import Dict as aDict


def parse_glasow_picarro(
    data_filepath: Union[str, Path],
    site: str,
    network: str,
    inlet: str,
    instrument: str = "picarro",
    sampling_period: Optional[str] = None,
    measurement_type: str = "surface",
    **kwargs: Dict,
) -> Dict:
    """Read the Glasgow Science Tower Picarro data

    Args:
        data_filepath: Path to data file
    Returns:
        dict: Dictionary of processed data
    """
    from openghg.util import format_inlet

    warn(message="Temporary function used to read Glasgow Science Tower Picarro data")

    df = pd.read_csv(data_filepath, index_col=[0], parse_dates=True)
    df = df.dropna(axis="rows", how="any")
    # We just want the concentration values for now
    species = ["co2", "ch4"]
    rename_cols = {f" {s}_C": s for s in species}
    df = df.rename(columns=rename_cols)

    site = "GST"
    long_site_name = "Glasgow Science Centre Tower"

    units = {"ch4": "ppb", "co2": "ppm"}

    if sampling_period is None:
        sampling_period = "NOT_SET"

    gas_data = aDict()
    for s in species:
        gas_data[s]["data"] = df[[s]].to_xarray()

        inlet = format_inlet("124m")

        gas_data[s]["metadata"] = {
            "species": s,
            "long_name": long_site_name,
            "latitude": 55.859238,
            "longitude": -4.296180,
            "network": "npl_picarro",
            "inlet": inlet,
            "sampling_period": sampling_period,
            "site": site,
            "instrument": "picarro",
            "units": units[s],
            "data_type": "surface",
        }

    # TODO - remove this once mypy stubs for addict are added
    to_return: Dict = gas_data.to_dict()

    return to_return
