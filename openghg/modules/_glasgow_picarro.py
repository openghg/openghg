import pandas as pd
from typing import Dict, Union
from pathlib import Path
from warnings import warn
from addict import Dict as aDict

__all__ = ["GLASGOWPICARRO"]


class GLASGOWPICARRO:
    def read_file(
        self,
        data_filepath: Union[str, Path],
        site: str,
        network: str,
        inlet: str,
        instrument: str = "picarro",
        sampling_period: str = "NA",
        measurement_type: str = "timeseries",
    ) -> Dict:
        """Read the Glasgow Science Tower Picarro data

        Args:
            data_filepath: Path to data file
        Returns:
            dict: Dictionary of processed data
        """
        warn(
            message="Temporary function used to read Glasgow Science Tower Picarro data"
        )

        df = pd.read_csv(data_filepath, index_col=[0], parse_dates=True)
        df = df.dropna(axis="rows", how="any")
        # We just want the concentration values for now
        species = ["co2", "ch4"]
        rename_cols = {f" {s}_C": s for s in species}
        df = df.rename(columns=rename_cols)

        site = "GST"
        long_site_name = "Glasgow Science Centre Tower"

        units = {"ch4": "ppb", "co2": "ppm"}

        gas_data = aDict()
        for s in species:
            gas_data[s]["data"] = df[[s]].to_xarray()

            gas_data[s]["metadata"] = {
                "species": s,
                "long_name": long_site_name,
                "latitude": 55.859238,
                "longitude": -4.296180,
                "network": "npl_picarro",
                "inlet": "124m",
                "sampling_period": "NA",
                "site": site,
                "instrument": "picarro",
                "units": units[s],
            }

        # TODO - remove this once mypy stubs for addict are added
        to_return: Dict = gas_data.to_dict()

        return to_return
