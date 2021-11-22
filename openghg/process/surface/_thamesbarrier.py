from openghg.store.base import BaseStore
from typing import Dict, Optional, Union
from pathlib import Path

__all__ = ["THAMESBARRIER"]


class THAMESBARRIER(BaseStore):
    """Class for retrieve Thames Barrier data"""

    def __init__(self) -> None:
        from openghg.util import load_json

        # Holds parameters used for writing attributes to Datasets
        self._tb_params = {}
        # Sampling period of  data in seconds
        self._sampling_period = "NA"

        data = load_json(filename="attributes.json")
        self._tb_params = data["TMB"]

    def read_file(
        self,
        data_filepath: Union[str, Path],
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
        from openghg.retrieve import assign_attributes
        from pandas import read_csv as pd_read_csv
        from openghg.util import clean_string

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

        gas_data = {}

        for species in data.columns:
            processed_data = data.loc[:, [species]].sort_index().to_xarray()

            # Convert methane to ppb
            if species == "CH4":
                processed_data[species] *= 1000

            # No averaging applied to raw obs, set variability to 0 to allow get_obs to calculate
            # when averaging
            processed_data["{} variability".format(species)] = processed_data[species] * 0.0

            site_attributes = self._tb_params["global_attributes"]
            site_attributes["inlet_height_magl"] = clean_string(self._tb_params["inlet"])
            site_attributes["instrument"] = clean_string(self._tb_params["instrument"])
            # site_attributes["inlet"] = clean_string(self._tb_params["inlet"])
            # site_attributes["unit_species"] = self._tb_params["unit_species"]
            # site_attributes["scale"] = self._tb_params["scale"]

            # All attributes stored in the metadata?
            metadata = {
                "species": clean_string(species),
                "site": site,
                "inlet": clean_string(self._tb_params["inlet"]),
                "network": "LGHG",
                "sampling_period": sampling_period
            }
            metadata.update(site_attributes)

            gas_data[species] = {
                "metadata": metadata,
                "data": processed_data,
                "attributes": site_attributes,
            }

        gas_data = assign_attributes(data=gas_data, site=site)

        return gas_data
