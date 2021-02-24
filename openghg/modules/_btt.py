from openghg.modules import BaseModule
from typing import Dict, Optional, Union
from pathlib import Path

__all__ = ["BTT"]


class BTT(BaseModule):
    """Class for processing National Physical Laboratory (NPL) data"""

    def __init__(self):
        from openghg.util import load_json

        # Sampling period of  data in seconds
        self._sampling_period = "NA"

        data = load_json(filename="attributes.json")
        self._params = data["BTT"]

    def read_file(
        self, data_filepath: Union[str, Path], site: Optional[str] = "BTT", network: Optional[str] = "LGHG"
    ) -> Dict:
        """Reads NPL data files and returns the UUIDS of the Datasources
        the processed data has been assigned to

        Args:
            data_filepath: Path of file to load
            site: Site name
        Returns:
            dict: UUIDs of Datasources data has been assigned to
        """
        from openghg.processing import assign_attributes

        data_filepath = Path(data_filepath)

        site = "BTT"

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
        from pandas import read_csv, Timestamp, to_timedelta, isnull
        from numpy import nan as np_nan

        # Rename these columns
        rename_dict = {"co2.cal": "CO2", "ch4.cal.ppb": "CH4"}
        # We only want these species
        species_extract = ["CO2", "CH4"]
        # Take std-dev measurements from these columns for these species
        species_sd = {"CO2": "co2.sd.ppm", "CH4": "ch4.sd.ppb"}

        data = read_csv(data_filepath)
        data["time"] = Timestamp("2019-01-01 00:00") + to_timedelta(data["DOY"] - 1, unit="D")
        data["time"] = data["time"].dt.round("30min")
        data = data[~isnull(data.time)]

        data = data.rename(columns=rename_dict)
        data = data.set_index("time")

        combined_data = {}
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

            site_attributes = self._params["global_attributes"]
            site_attributes["inlet_height_magl"] = self._params["inlet"]
            site_attributes["instrument"] = self._params["instrument"]

            # TODO - add in better metadata reading
            metadata = {"species": species}

            combined_data[species] = {
                "metadata": metadata,
                "data": processed_data,
                "attributes": site_attributes,
            }

        return combined_data
