from typing import Dict, Optional, Union
from pathlib import Path

__all__ = ["BEACO2N"]


class BEACO2N:
    """ Read BEACO2N data files

    """

    def read_file(self, filepath: Union[str, Path], site: Optional[str] = None) -> Dict:
        """ Read BEACO2N data files

            Args:
                filepath: Data filepath
                site: Site name
            Returns:
                dict: Dictionary of data
        """
        import pandas as pd
        import numpy as np

        datetime_columns = {"time": ["datetime"]}
        rename_cols = {"PM_ug/m3": "pm", "PM_ug/m3_QC_level": "pm_qc", "co2_ppm": "co2", "co2_ppm_QC_level": "co2_qc"}
        use_cols = [1, 5, 6, 7, 8]

        data = pd.read_csv(
            "Glen_Cove_Elementary_School.csv",
            index_col="time",
            parse_dates=datetime_columns,
            na_values=[-999.0, "1a"],
            usecols=use_cols,
        )
        
        # Set all non-zero values to be NaN
        data[data < 0] = np.nan
        data = data.rename(columns=rename_cols)

            #         combined_data[species] = {
            #     "metadata": species_metadata,
            #     "data": gas_data,
            #     "attributes": site_attributes,
            # }

        pm_data = data[["pm", "pm_qc"]]
        pm_data = pm_data.dropna(axis="rows", how="any").to_xarray()

        co2_data = data[["co2", "co2_qc"]]
        co2_data = co2_data.drop_na(axis="rows", how="any").to_xarray()

        gas_data = {}
        gas_data["co2"] = co2_data



