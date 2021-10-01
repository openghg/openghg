from typing import DefaultDict, Dict, Optional, Union
from pathlib import Path
from pandas import DataFrame

__all__ = ["BEACO2N"]


class BEACO2N:
    """Read BEACO2N data files"""

    def read_file(
        self,
        data_filepath: Union[str, Path],
        site: str,
        network: str,
        inlet: Optional[str] = None,
        instrument: Optional[str] = "shinyei",
        sampling_period: Optional[str] = None,
        measurement_type: Optional[str] = None,
    ) -> Dict:
        """Read BEACO2N data files

        Args:
            filepath: Data filepath
            site: Site name
        Returns:
            dict: Dictionary of data
        """
        import pandas as pd
        from numpy import nan as np_nan
        from openghg.util import load_json
        from collections import defaultdict
        from openghg.util import clean_string

        if sampling_period is None:
            sampling_period = "NOT_SET"

        datetime_columns = {"time": ["datetime"]}
        rename_cols = {
            "PM_ug/m3": "pm",
            "PM_ug/m3_QC_level": "pm_qc",
            "co2_ppm": "co2",
            "co2_ppm_QC_level": "co2_qc",
            "co_ppm": "co",
            "co_ppm_QC_level": "co_qc",
        }

        use_cols = [1, 5, 6, 7, 8, 9, 10]
        data_filepath = Path(data_filepath)

        try:
            data = pd.read_csv(
                data_filepath,
                index_col="time",
                parse_dates=datetime_columns,
                na_values=[-999.0, "1a"],
                usecols=use_cols,
            )
        except ValueError as e:
            raise ValueError(
                f"Unable to read data file, please make sure it is in the standard BEACO2N format.\nError: {e}"
            )

        beaco2n_site_data = load_json("beaco2n_site_data.json")

        try:
            site_metadata = beaco2n_site_data[site.upper()]
        except KeyError:
            raise ValueError(f"Site {site} not recognized.")

        site_metadata["comment"] = "Retrieved from http://beacon.berkeley.edu/"

        # Set all values below zero to NaN
        data[data < 0] = np_nan
        print(data.columns)
        data = data.rename(columns=rename_cols)

        measurement_types = ["pm", "co2"]
        units = {"pm": "ug/m3", "co2": "ppm"}

        print(data.columns)

        gas_data: DefaultDict[str, Dict[str, Union[DataFrame, Dict]]] = defaultdict(dict)
        for mt in measurement_types:
            m_data = data[[mt, f"{mt}_qc"]]
            m_data = m_data.dropna(axis="rows", how="any").to_xarray()

            species_metadata = {
                "units": units[mt],
                "site": clean_string(site),
                "species": clean_string(mt),
                "inlet": "NA",
                "network": "beaco2n",
                "sampling_period": str(sampling_period),
            }

            gas_data[mt]["data"] = m_data
            gas_data[mt]["metadata"] = species_metadata
            gas_data[mt]["attributes"] = site_metadata

        # TODO - add CF Compliant attributes?

        return gas_data
