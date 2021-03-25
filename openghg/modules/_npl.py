from openghg.modules import BaseModule
from typing import Dict, Optional, Union
from pathlib import Path

__all__ = ["NPL"]


class NPL(BaseModule):
    """Class for processing National Physical Laboratory (NPL) data"""

    def __init__(self):
        from openghg.util import load_json

        # Sampling period of  data in seconds
        self._sampling_period = "NA"

        data = load_json(filename="attributes.json")
        self._params = data["NPL"]

    def read_file(self, data_filepath: Union[str, Path], site: Optional[str] = "NPL", network: Optional[str] = "LGHG") -> Dict:
        """Reads NPL data files and returns the UUIDS of the Datasources
        the processed data has been assigned to

        Args:
            data_filepath: Path of file to load
            site: Site name
        Returns:
            list: UUIDs of Datasources data has been assigned to
        """
        from openghg.processing import assign_attributes

        data_filepath = Path(data_filepath)

        site = "NPL"

        gas_data = self.read_data(data_filepath=data_filepath)
        gas_data = assign_attributes(data=gas_data, site=site, network=network)

        return gas_data

    def read_data(self, data_filepath: Path) -> Dict:
        """Separates the gases stored in the dataframe in
        separate dataframes and returns a dictionary of gases
        with an assigned UUID as gas:UUID and a list of the processed
        dataframes

        Args:
            data_filepath (pathlib.Path): Path of datafile
        Returns:
            dict: Dictionary containing attributes, data and metadata keys
        """
        from pandas import read_csv, NaT
        from datetime import datetime

        def parser(date):
            try:
                return datetime.strptime(str(date), "%d/%m/%Y %H:%M")
            except ValueError:
                return NaT

        data = read_csv(data_filepath, index_col=0, date_parser=parser)

        # Drop the NaT/NaNs
        data = data.loc[data.index.dropna()]
        # rename columns
        rename_dict = {"Cal_CO2_dry": "CO2", "Cal_CH4_dry": "CH4"}

        data = data.rename(columns=rename_dict)

        combined_data = {}
        for species in data.columns:
            processed_data = data.loc[:, [species]].sort_index().to_xarray()

            # Convert methane to ppb
            if species == "CH4":
                processed_data[species] *= 1000

            # No averaging applied to raw obs, set variability to 0 to allow get_obs to calculate
            # when averaging
            processed_data["{} variability".format(species)] = processed_data[species] * 0.0

            site_attributes = self._params["global_attributes"]
            site_attributes["inlet_height_magl"] = self._params["inlet"]
            site_attributes["instrument"] = self._params["instrument"]

            metadata = {"species": species}
            # TODO - add in better metadata reading
            combined_data[species] = {
                "metadata": metadata,
                "data": processed_data,
                "attributes": site_attributes,
            }

        return combined_data
