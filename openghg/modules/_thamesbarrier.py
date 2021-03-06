from openghg.modules import BaseModule
from typing import Dict, Optional, Union
from pathlib import Path

__all__ = ["THAMESBARRIER"]


class THAMESBARRIER(BaseModule):
    """ Class for processing THAMESBARRIER data

    """

    def __init__(self):
        from openghg.util import load_json

        # Holds parameters used for writing attributes to Datasets
        self._tb_params = {}
        # Sampling period of  data in seconds
        self._sampling_period = "NA"

        data = load_json(filename="attributes.json")
        self._tb_params = data["TMB"]

    def read_file(self, data_filepath: Union[str, Path], site: Optional[str] = None, network: Optional[str] = None) -> Dict:
        """ Reads THAMESBARRIER data files and returns the UUIDS of the Datasources
            the processed data has been assigned to

            Args:
                data_filepath: Path of file to load
                site: Site name
            Returns:
                list: UUIDs of Datasources data has been assigned to
        """
        from openghg.processing import assign_attributes

        data_filepath = Path(data_filepath)

        site = "TMB"

        gas_data = self.read_data(data_filepath=data_filepath)
        gas_data = assign_attributes(data=gas_data, site=site)

        return gas_data

    def read_data(self, data_filepath: Path) -> Dict:
        """ Separates the gases stored in the dataframe in
            separate dataframes and returns a dictionary of gases
            with an assigned UUID as gas:UUID and a list of the processed
            dataframes

            Args:
                data_filepath (pathlib.Path): Path of datafile
            Returns:
                dict: Dictionary containing attributes, data and metadata keys
        """
        from pandas import read_csv

        data = read_csv(data_filepath, parse_dates=[0], infer_datetime_format=True, index_col=0)
        # Drop NaNs from the data
        data = data.dropna(axis="rows", how="any")

        rename_dict = {}
        if "Methane" in data.columns:
            rename_dict["Methane"] = "CH4"

        data = data.rename(columns=rename_dict)
        data.index.name = "time"

        combined_data = {}

        for species in data.columns:
            processed_data = data.loc[:, [species]].sort_index().to_xarray()

            # Convert methane to ppb
            if species == "CH4":
                processed_data[species] *= 1000

            # No averaging applied to raw obs, set variability to 0 to allow get_obs to calculate
            # when averaging
            processed_data["{} variability".format(species)] = processed_data[species] * 0.0

            site_attributes = self._tb_params["global_attributes"]
            site_attributes["inlet_height_magl"] = self._tb_params["inlet"]
            site_attributes["instrument"] = self._tb_params["instrument"]

            metadata = {"species": species}

            # TODO - add in metadata reading
            combined_data[species] = {
                "metadata": metadata,
                "data": processed_data,
                "attributes": site_attributes,
            }

        return combined_data
